// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/table_cache.h"

#include "db/filename.h"
#include "leveldb/env.h"
#include "leveldb/table.h"
#include "util/coding.h"

namespace leveldb {
bool directIO_of_RandomAccess = false;


static void DeleteEntry(const Slice& key, void* value) {
  TableAndFile* tf = reinterpret_cast<TableAndFile*>(value);
  delete tf->table;
  delete tf->file;
  delete tf;
}

static void DeleteEntry(void* arg1, void* arg2) {
   TableAndFile* tf = reinterpret_cast<TableAndFile*>(arg2);
    delete tf->table;
    delete tf->file;
    delete tf;
 }
 
 

static void UnrefEntry(void* arg1, void* arg2) {
  Cache* cache = reinterpret_cast<Cache*>(arg1);
  Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
  cache->Release(h);
}

TableCache::TableCache(const std::string& dbname,
                       const Options* options,
                       int entries)
    : env_(options->env),
      dbname_(dbname),
      options_(options),
      cache_(NewLRUCache(entries)) {
}

TableCache::~TableCache() {
  delete cache_;
}

Status TableCache::FindBufferedTable(uint64_t file_number, uint64_t file_size,
                             Cache::Handle** handle,TableAndFile *rtf){
    Status s;
    char buf[sizeof(file_number)];
    EncodeFixed64(buf, file_number);
    Slice key(buf, sizeof(buf));
    std::string fname = TableFileName(dbname_, file_number);
    RandomAccessFile* file = NULL;
    Table* table = NULL;
    s = env_->NewBufferedRandomAccessFile(fname, &file);
    if (!s.ok()) {
      std::string old_fname = SSTTableFileName(dbname_, file_number);
      if (env_->NewBufferedRandomAccessFile(old_fname, &file).ok()) {
        s = Status::OK();
      }
    }
    if (s.ok()) {
      s = Table::Open(*options_, file, file_size, &table);
    }
    if (!s.ok()) {
      assert(table == NULL);
      rtf->file = NULL;
      rtf->table = NULL;
      delete file;
      // We do not cache error results so that if the error is transient,
      // or somebody repairs the file, we recover automatically.
    } else {
      //TableAndFile* tf = new TableAndFile;
      rtf->file = file;
      rtf->table = table;
     
    }
    return s;
}

Status TableCache::FindTable(uint64_t file_number, uint64_t file_size,
                             Cache::Handle** handle) {
  Status s;
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  Slice key(buf, sizeof(buf));
  *handle = cache_->Lookup(key);
  if (*handle == NULL) {
    uint64_t start_micros = Env::Default()->NowMicros();
    std::string fname = TableFileName(dbname_, file_number);
    RandomAccessFile* file = NULL;
    Table* table = NULL;
    s = env_->NewRandomAccessFile(fname, &file,directIO_of_RandomAccess);
    if (!s.ok()) {
      std::string old_fname = SSTTableFileName(dbname_, file_number);
      if (env_->NewRandomAccessFile(old_fname, &file,directIO_of_RandomAccess).ok()) {
        s = Status::OK();
      }
    }
    if (s.ok()) {
      s = Table::Open(*options_, file, file_size, &table);
    }
    MeasureTime(Statistics::GetStatistics().get(),Tickers::OPEN_TABLE_TIME,Env::Default()->NowMicros() - start_micros);
    if (!s.ok()) {
      assert(table == NULL);
      delete file;
      // We do not cache error results so that if the error is transient,
      // or somebody repairs the file, we recover automatically.
    } else {
      TableAndFile* tf = new TableAndFile;
      tf->file = file;
      tf->table = table;
      *handle = cache_->Insert(key, tf, 1, &DeleteEntry);
    }
  }
  return s;
}


Iterator* TableCache::NewBufferedIterator(const ReadOptions& options,
                                  uint64_t file_number,
                                  uint64_t file_size,
                                  Table** tableptr) {
    if (tableptr != NULL) {
	*tableptr = NULL;
    }
    TableAndFile *rtf = new TableAndFile;
    Cache::Handle* handle = NULL;
    Status s = FindBufferedTable(file_number, file_size, &handle,rtf);
    if (!s.ok()) {
	return NewErrorIterator(s);
    }

    Table* table = rtf->table;
    Iterator* result = table->NewIterator(options);
    result->RegisterCleanup(&DeleteEntry,NULL,rtf);
    if (tableptr != NULL) {
	*tableptr = table;
    }
    return result;				
								     
}


Iterator* TableCache::NewIterator(const ReadOptions& options,
                                  uint64_t file_number,
                                  uint64_t file_size,
                                  Table** tableptr) {
  if (tableptr != NULL) {
    *tableptr = NULL;
  }

  Cache::Handle* handle = NULL;
  Status s = FindTable(file_number, file_size, &handle);
  if (!s.ok()) {
    return NewErrorIterator(s);
  }

  Table* table = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
  Iterator* result = table->NewIterator(options);
  result->RegisterCleanup(&UnrefEntry, cache_, handle);
  if (tableptr != NULL) {
    *tableptr = table;
  }
  return result;
}

Status TableCache::Get(const ReadOptions& options,
                       uint64_t file_number,
                       uint64_t file_size,
                       const Slice& k,
                       void* arg,
                       void (*saver)(void*, const Slice&, const Slice&)) {
  Cache::Handle* handle = NULL;
  uint64_t start_micros = Env::Default()->NowMicros();
  Status s = FindTable(file_number, file_size, &handle);
  MeasureTime(Statistics::GetStatistics().get(),Tickers::FINDTABLE,Env::Default()->NowMicros() - start_micros);
  if (s.ok()) {
    start_micros = Env::Default()->NowMicros();
    Table* t = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
    s = t->InternalGet(options, k, arg, saver);
    MeasureTime(Statistics::GetStatistics().get(),Tickers::INTERNALGET,Env::Default()->NowMicros() - start_micros);
    start_micros = Env::Default()->NowMicros();
    cache_->Release(handle);
    MeasureTime(Statistics::GetStatistics().get(),Tickers::RELEASE,Env::Default()->NowMicros() - start_micros);
  }
  return s;
}

void TableCache::Evict(uint64_t file_number) {
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  cache_->Erase(Slice(buf, sizeof(buf)));
}

}  // namespace leveldb
