// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/table_cache.h"
#include "db/filename.h"
#include "util/coding.h"
#include <iostream>

namespace leveldb {
bool directIO_of_RandomAccess = false;


static void DeleteEntry(const Slice& key, void* value) {
  TableAndFile* tf = reinterpret_cast<TableAndFile*>(value);
  // if (--tf->refs == 0) {
    delete tf->table;
    delete tf->file;
    delete tf;
  // }
}

static void UnrefEntry(void* arg1, void* arg2) {
  Cache* cache = reinterpret_cast<Cache*>(arg1);
  Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
  cache->Release(h);
}

static void DeleteEntry(void* arg1, void* arg2) {
  TableAndFile* tf = reinterpret_cast<TableAndFile*>(arg2);
  delete tf->table;
  delete tf->file;
  delete tf;
}
TableCache::TableCache(const std::string& dbname,
                       const Options* options,
                       size_t entries)
    : env_(options->env),
      dbname_(dbname),
      options_(options),
      cache_(NewMultiQueue(entries,options->opEp_.lrus_num_,options->opEp_.base_num,options->opEp_.life_time,options->opEp_.force_shrink_ratio,options->opEp_.slow_shrink_ratio,options_->opEp_.change_ratio,options->opEp_.log_base,options->opEp_.slow_ratio)) {
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
    size_t *charge = NULL;
    if (s.ok()) {
      s = Table::Open(*options_, file, file_size, &table, charge,false);
    }
    delete [] charge;
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
                             Cache::Handle** handle,bool Get,bool isLevel0) {
  Status s;
  char buf[sizeof(file_number) + sizeof(uint32_t)];
  EncodeFixed64(buf, file_number);
  uint32_t *id_ = (uint32_t *) (buf + sizeof(file_number));
  *id_ = 0;
  Slice key(buf, sizeof(buf));
  
  uint64_t start_micros_l = Env::Default()->NowMicros();
  *handle = cache_->Lookup(key, false); //Get
  MeasureTime(Statistics::GetStatistics().get(),Tickers::FILTER_LOOKUP_TIME,Env::Default()->NowMicros() - start_micros_l);

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
    size_t *charge = NULL;

    if (s.ok()) {
      s = Table::Open(*options_, file, file_size, &table, charge, isLevel0);
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
      tf->refs = 0;
   //    for(int i = 0 ; i < table->getCurrFilterNum() ; i ++){
    // charge += FilterPolicy::bits_per_key_per_filter_[i];
   //    }
      // printf("table number %llu, regionNum %d\n", file_number, regionNum);fflush(stdout);

      int regionNum = table->getRegionNum();
      if (charge == NULL)
        charge = new size_t[regionNum]();
      for (int i = 0; i <= regionNum; i++) {
        *id_ = i;
        Slice key2(buf, sizeof(buf));
        if (i == 0)
          *handle = cache_->Insert(key2, tf,0, &DeleteEntry,true);        
        else
          cache_->Insert(key2, tf, charge[i - 1], &DeleteEntry,true);        

        tf->refs++;
      }
      delete [] charge;
    }
  }
  else {
      // std::cout << "cachetable lookup fid " << file_number << std::endl;
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
  Status s = FindTable(file_number, file_size, &handle,false,options.isLevel0);
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
                       void (*saver)(void*, const Slice&, const Slice&),uint64_t file_access_time) {
  Cache::Handle* handle = NULL;
  uint64_t start_micros = env_->NowMicros();
  // options_->opEp_.add_filter = file_access_time > cache_->GetLRUFreCount()?true:false;
  options_->opEp_.add_filter = !cache_->IsCacheFull();
  options.file_number = file_number;
  Status s = FindTable(file_number, file_size, &handle,true);
  MeasureTime(Statistics::GetStatistics().get(),Tickers::FINDTABLE,Env::Default()->NowMicros() - start_micros);
  if (s.ok()) {
    start_micros = env_->NowMicros();
    Table* t = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
    char buf[sizeof(file_number) + sizeof(uint32_t)];
    s = t->InternalGet(options, k, arg, saver, buf);
    Slice key(buf, sizeof(buf));
    Cache::Handle* cache_handle = cache_->Lookup(key, true);
    cache_->Release(cache_handle);

    MeasureTime(Statistics::GetStatistics().get(),Tickers::INTERNALGET,Env::Default()->NowMicros() - start_micros);
    start_micros = env_->NowMicros();
    cache_->Release(handle);
    MeasureTime(Statistics::GetStatistics().get(),Tickers::RELEASE,Env::Default()->NowMicros() - start_micros);
  }
  return s;
}

void TableCache::Evict(uint64_t file_number) {
  char buf[sizeof(file_number) + sizeof(uint32_t)];
  EncodeFixed64(buf, file_number);
  uint32_t *id_ = (uint32_t *) (buf + sizeof(file_number));
  *id_ = 0;
  cache_->Erase(Slice(buf, sizeof(buf)));
}

void TableCache::adjustFilters(uint64_t file_number, uint64_t file_size,int n)
{
    Cache::Handle* handle = NULL;
    Status s = FindTable(file_number, file_size, &handle);
    if (s.ok()) {
	Table* t = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
	if(n > 0){
	    t->AddFilters(n, 0);
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
