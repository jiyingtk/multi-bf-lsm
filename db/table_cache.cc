// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/table_cache.h"
#include "db/filename.h"
#include "util/coding.h"
#include "table/format.h"
#include <iostream>

extern bool multi_queue_init;

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
      cache_(NewMultiQueue(entries,options->opEp_.lrus_num_,options->opEp_.base_num,options->opEp_.life_time,options->opEp_.force_shrink_ratio,options->opEp_.slow_shrink_ratio,options_->opEp_.change_ratio,options->opEp_.log_base,options->opEp_.slow_ratio)), level0_freq(0) {
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
                             Cache::Handle** handle,bool Get,int file_level, TableMetaData *tableMetaData, std::vector<uint64_t> *input_0_numbers, std::vector<uint64_t> *input_1_numbers) {
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

    int *freqs = NULL, freq_count = 0;;
    int total_freq = 0;
    if (input_0_numbers != NULL) {
      std::vector<std::vector<Slice>>  region_keys_all;
      std::vector<int *> freqs_all;
      std::vector<int> freq_count_all;

      char buf2[sizeof(file_number) + sizeof(uint32_t)];

      int table_nums = (input_1_numbers == NULL) ? (*input_0_numbers).size() : (*input_0_numbers).size() + (*input_1_numbers).size();
      for (int i = 0; i < table_nums; i++) {
        uint64_t file_number_0;
        if (i < (*input_0_numbers).size())
          file_number_0 = (*input_0_numbers)[i];
        else
          file_number_0 = (*input_1_numbers)[i - (*input_0_numbers).size()];

        EncodeFixed64(buf2, file_number_0);
        uint32_t *id_ = (uint32_t *) (buf2 + sizeof(file_number));
        *id_ = 0;
        Slice key2(buf2, sizeof(buf2));
        Cache::Handle* p_0_handle = cache_->Lookup(key2, false); //Get
        if (p_0_handle != NULL) {
          Table* p_0_table = reinterpret_cast<TableAndFile*>(cache_->Value(p_0_handle))->table;

          std::vector<Slice> region_keys;
          p_0_table->getRegionKeyRanges(region_keys);
          region_keys_all.push_back(region_keys);
          freqs_all.push_back(p_0_table->freqs);
          freq_count_all.push_back(p_0_table->freq_count);
assert(p_0_table->freq_count == region_keys.size() - 1);
          cache_->Release(p_0_handle);
        }
        
      }

      std::vector<Slice> region_keys_cur;
      Table::getRegionKeyRangesByStr(options_, tableMetaData->index_data, region_keys_cur);
      freq_count = region_keys_cur.size() - 1;
      freqs = new int[freq_count]();

      std::vector<int> region_iters(region_keys_all.size());

      tableMetaData->region_num = freq_count;
      tableMetaData->load_filter_num = new int[freq_count]();

      for (int i = 0; i < freq_count; i++) {
        for (int j = 0; j < region_keys_all.size(); j++) {
          int cur_region = region_iters[j];
          if (cur_region >= freq_count_all[j])
            continue;
          Slice t_left_key = region_keys_all[j][cur_region];
          if (options_->comparator->Compare(region_keys_cur[i + 1], t_left_key) <= 0)
            continue;
          Slice t_right_key = region_keys_all[j][cur_region + 1];
          while (options_->comparator->Compare(t_right_key, region_keys_cur[i]) <= 0) {
            cur_region += 1;
            region_iters[j] += 1;
            if (cur_region >= freq_count_all[j])
              break;
            t_right_key = region_keys_all[j][cur_region + 1];
          }
          if (cur_region >= freq_count_all[j])
            continue;  

          freqs[i] += freqs_all[j][cur_region];
        }

        total_freq += freqs[i];
        // int filter_num = cache_->AllocFilterNums(freqs[i]);
        // tableMetaData->load_filter_num[i] = filter_num;
      }
      int filter_num = cache_->AllocFilterNums(total_freq);
      tableMetaData->load_filter_num[0] = filter_num;

    }


    size_t *charge = NULL;
    if (s.ok()) {
      s = Table::Open(*options_, file, file_size, &table, charge, file_level, tableMetaData);
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
        else {
          cache_->Insert(key2, tf, charge[i - 1], &DeleteEntry,true);
          if (file_level == 0)
            cache_->SetFreCount(key2, level0_freq);
          else if (freq_count != 0)
            cache_->SetFreCount(key2, total_freq);
            // cache_->SetFreCount(key2, freqs[i - 1]);
        }

        tf->refs++;
      }
      delete [] charge;
    }

    if (freqs)
      delete [] freqs;
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
                                  Table** tableptr, TableMetaData *tableMetaData_, std::vector<uint64_t> *input_0_numbers, std::vector<uint64_t> *input_1_numbers) {
  if (tableptr != NULL) {
    *tableptr = NULL;
  }
 
  Cache::Handle* handle = NULL;
  Status s = FindTable(file_number, file_size, &handle,false,options.file_level,tableMetaData_, input_0_numbers, input_1_numbers);
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

// uint64_t TableCache::getFreqCount(uint64_t file_number)
// {

// }

uint64_t TableCache::LookupFreCount(uint64_t file_number)
{
    char buf[sizeof(file_number) + sizeof(uint32_t)];
    EncodeFixed64(buf, file_number);
    uint32_t *id_ = (uint32_t *) (buf + sizeof(file_number));
    *id_ = 1;
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

void TableCache::SaveLevel0Freq(uint64_t file_number) {
  uint64_t freq = LookupFreCount(file_number);
  level0_freq = level0_freq < freq ? freq : level0_freq;
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
  options_->opEp_.add_filter = true;
  // options_->opEp_.add_filter = false;
  // options_->opEp_.add_filter = !cache_->IsCacheFull();
  options.file_number = file_number;
  Status s = FindTable(file_number, file_size, &handle,true, options.file_level);
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
  Slice key(buf, sizeof(buf));
  Cache::Handle* handle = cache_->Lookup(key, false);
  if (handle != NULL) {
    Table* t = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
    int regionNum = t->getRegionNum();
    cache_->Release(handle);
    for (int i = 1; i <= regionNum; i++) {
      *id_ = i;
      cache_->Erase(Slice(buf, sizeof(buf)));
    }

    *id_ = 0;
    cache_->Erase(Slice(buf, sizeof(buf)));
  }
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
