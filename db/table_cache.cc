// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/table_cache.h"
#include "db/filename.h"
#include "util/coding.h"

namespace leveldb {
bool directIO_of_RandomAccess = false;


static void DeleteEntry(const Slice& key, void* value) {
  TableAndFile* tf = reinterpret_cast<TableAndFile*>(value);
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
      cache_(NewMultiQueue(entries,options->opEp_.lrus_num_,options->opEp_.base_num,options->opEp_.life_time,options->opEp_.force_shrink_ratio,options->opEp_.quick_shrink_ratio,options->opEp_.slow_shrink_ratio,options->opEp_.log_base)) {
}

TableCache::~TableCache() {
  delete cache_;
}

Status TableCache::FindTable(uint64_t file_number, uint64_t file_size,
                             Cache::Handle** handle,bool Get) {
  Status s;
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  Slice key(buf, sizeof(buf));
  *handle = cache_->Lookup(key,Get);
  if (*handle == NULL) {
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

    if (!s.ok()) {
      assert(table == NULL);
      delete file;
      // We do not cache error results so that if the error is transient,
      // or somebody repairs the file, we recover automatically.
    } else {
      TableAndFile* tf = new TableAndFile;
      tf->file = file;
      tf->table = table;
      size_t charge = 0;
      for(int i = 0 ; i < options_->opEp_.init_filter_nums ; i ++){
	  charge += FilterPolicy::bits_per_key_per_filter_[i];
      }
      *handle = cache_->Insert(key, tf,charge, &DeleteEntry,true);
    }
  }
  return s;
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

uint64_t TableCache::LookupFreCount(uint64_t file_number)
{
    char buf[sizeof(file_number)];
    EncodeFixed64(buf, file_number);
    Slice key(buf, sizeof(buf));
    return cache_->LookupFreCount(key);
}

void TableCache::SetFreCount(uint64_t file_number, uint64_t freCount)
{
    char buf[sizeof(file_number)];
    EncodeFixed64(buf, file_number);
    Slice key(buf, sizeof(buf));
    cache_->SetFreCount(key,freCount);
}


Status TableCache::Get(const ReadOptions& options,
                       uint64_t file_number,
                       uint64_t file_size,
                       const Slice& k,
                       void* arg,
                       void (*saver)(void*, const Slice&, const Slice&)) {
  Cache::Handle* handle = NULL;
  uint64_t start_micros = env_->NowMicros();
  Status s = FindTable(file_number, file_size, &handle,true);
  MeasureTime(Statistics::GetStatistics().get(),Tickers::FINDTABLE,Env::Default()->NowMicros() - start_micros);
  if (s.ok()) {
    start_micros = env_->NowMicros();
    Table* t = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
    s = t->InternalGet(options, k, arg, saver);
    MeasureTime(Statistics::GetStatistics().get(),Tickers::INTERNALGET,Env::Default()->NowMicros() - start_micros);
    start_micros = env_->NowMicros();
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

void TableCache::adjustFilters(uint64_t file_number, uint64_t file_size,int n)
{
    Cache::Handle* handle = NULL;
    Status s = FindTable(file_number, file_size, &handle);
    if (s.ok()) {
	Table* t = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
	if(n > 0){
	    t->AddFilters(n);
	}else{
	    t->RemoveFilters(-n);
	}
	cache_->Release(handle);
    }
}

size_t TableCache::GetTableCurrFiltersSize(uint64_t file_number,uint64_t file_size){
    Cache::Handle* handle = NULL;
    Status s = FindTable(file_number, file_size, &handle);
    if (s.ok()) {
      Table* t = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
      size_t res = t->getCurrFiltersSize();
      cache_->Release(handle);
      return res;
    }else{
      return 0;
    }
}

std::string TableCache::LRU_Status()
{
    return cache_->LRU_Status();
}


}  // namespace leveldb
