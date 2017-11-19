// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table.h"

#include "leveldb/cache.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include<list>
#include <boost/config/posix_features.hpp>

//filter memory space overhead
extern unsigned long long filter_mem_space;
extern unsigned long long filter_num;
namespace leveldb {

struct Table::Rep {
  ~Rep() {
    int curr_filter_num = filter->getCurrFiltersNum();
    delete filter;
    for(std::vector<const char *>::iterator filter_datas_iter=filter_datas.begin() ; filter_datas_iter != filter_datas.end() ; filter_datas_iter++){
	 delete [] (*filter_datas_iter);
	 BlockHandle filter_handle;
	 Slice v = filter_handles[--curr_filter_num];
	 if(!filter_handle.DecodeFrom(&v).ok()){
		assert(0);
		return ;
	 }
	 filter_mem_space -= (filter_handle.size()+kBlockTrailerSize) ;
	 filter_num--;
    }
    filter_handles.clear();   //followers delete meta will free space in handle slice
    delete index_block;
    if(meta){
	delete meta;
    }
  }

  Options options;
  Status status;
  RandomAccessFile* file;
  uint64_t cache_id;
  FilterBlockReader* filter;
 // const char* filter_data;
  std::vector<const char*> filter_datas; // be careful about vector memory overhead
  std::vector<Slice> filter_handles;    //use string instead of slice 
  BlockHandle metaindex_handle;  // Handle to metaindex_block: saved from footer
  Block* index_block;
  Block *meta=NULL;
};

Status Table::Open(const Options& options,
                   RandomAccessFile* file,
                   uint64_t size,
                   Table** table) {
  *table = NULL;
  if (size < Footer::kEncodedLength) {
    return Status::Corruption("file is too short to be an sstable");
  }

  char footer_space[Footer::kEncodedLength];
  Slice footer_input;
  Status s = file->Read(size - Footer::kEncodedLength, Footer::kEncodedLength,
                        &footer_input, footer_space);
  if (!s.ok()) return s;

  Footer footer;
  s = footer.DecodeFrom(&footer_input);
  if (!s.ok()) return s;

  // Read the index block
  BlockContents contents;
  Block* index_block = NULL;
  if (s.ok()) {
    ReadOptions opt;
    if (options.paranoid_checks) {
      opt.verify_checksums = true;
    }
    s = ReadBlock(file, opt, footer.index_handle(), &contents);
    if (s.ok()) {
      index_block = new Block(contents);
    }
  }

  if (s.ok()) {
    // We've successfully read the footer and the index block: we're
    // ready to serve requests.
    Rep* rep = new Table::Rep;
    rep->options = options;
    rep->file = file;
    rep->metaindex_handle = footer.metaindex_handle();
    rep->index_block = index_block;
    rep->cache_id = (options.block_cache ? options.block_cache->NewId() : 0);
    rep->filter_datas.clear();
    rep->filter = NULL;
    *table = new Table(rep);
    (*table)->ReadMeta(footer);
  } else {
    delete index_block;
  }

  return s;
}

void Table::ReadMeta(const Footer& footer) {
  if (rep_->options.filter_policy == NULL) {
    return;  // Do not need any metadata
  }

  // TODO(sanjay): Skip this if footer.metaindex_handle() size indicates
  // it is an empty block.
  ReadOptions opt;
  if (rep_->options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  BlockContents contents;
  if (!ReadBlock(rep_->file, opt, footer.metaindex_handle(), &contents).ok()) {
    // Do not propagate errors since meta info is not needed for operation
    return;
  }
  Block* meta = new Block(contents);
  rep_->meta = meta;
  Iterator* iter = meta->NewIterator(BytewiseComparator());
  char id[]={'1',0};
  std::string key = "filter."+std::string(id);
  key.append(rep_->options.filter_policy->Name());
  iter->Seek(key);
  if (iter->Valid() && iter->key() == Slice(key)) {
    //ReadFilter(iter->value());
  }else{
    fprintf(stderr,"filter iter is not valid\n");
  }
  while(iter->Valid()){
      rep_->filter_handles.push_back(iter->value());
      iter->Next();
   }
   ReadFilters(rep_->filter_handles,rep_->options.opEp_.init_filter_nums);
  delete iter;
 // delete meta;  //reserve meta index_block
}
void Table::ReadFilters(std::vector< Slice >& filter_handle_values,int n)
{
    Slice v;
    BlockHandle filter_handles[6];
    for(int i = 0 ;  i <  n ; i++){
	    v = filter_handle_values[i];
	    if(!filter_handles[i].DecodeFrom(&v).ok()){
		return;
	    }
    }
   ReadOptions opt;
   if (rep_->options.paranoid_checks) {
	opt.verify_checksums = true;
   }
   BlockContents blocks[6];
   uint64_t start_micros = Env::Default()->NowMicros();
   if (!ReadBlocks(rep_->file, opt, filter_handles, blocks,n).ok()) {
	return;
   }
   for(int i = 0 ; i < n ; i++){
	if (blocks[i].heap_allocated) {
	    rep_->filter_datas.push_back(blocks[i].data.data());     // Will need to delete later
	    filter_mem_space += blocks[i].data.size();
	    filter_num++;
	    int curr_filter_num = rep_->filter == NULL ? 0 : rep_->filter->getCurrFiltersNum();
	}
	if(rep_->filter == NULL){
		rep_->filter = new FilterBlockReader(rep_->options.filter_policy, blocks[i].data);
	}else{
		rep_->filter->AddFilter(blocks[i].data);
	}
   }
   MeasureTime(Statistics::GetStatistics().get(),Tickers::ADD_FILTER_TIME_0 + n,Env::Default()->NowMicros() - start_micros);
}

void Table::ReadFilter(const Slice& filter_handle_value) {
  Slice v = filter_handle_value;
  BlockHandle filter_handle;
  if (!filter_handle.DecodeFrom(&v).ok()) {
    return;
  }

  // We might want to unify with ReadBlock() if we start
  // requiring checksum verification in Table::Open.
  ReadOptions opt;
  if (rep_->options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  BlockContents block;
  uint64_t start_micros = Env::Default()->NowMicros();
  if (!ReadBlock(rep_->file, opt, filter_handle, &block).ok()) {
    return;
  }
  if (block.heap_allocated) {
    rep_->filter_datas.push_back(block.data.data());     // Will need to delete later
    filter_mem_space += block.data.size();
    filter_num++;
    int curr_filter_num = rep_->filter == NULL ? 0 : rep_->filter->getCurrFiltersNum();
    MeasureTime(Statistics::GetStatistics().get(),Tickers::ADD_FILTER_TIME_0 + curr_filter_num,Env::Default()->NowMicros() - start_micros);
  }
  if(rep_->filter == NULL){
	rep_->filter = new FilterBlockReader(rep_->options.filter_policy, block.data);
  }else{
	rep_->filter->AddFilter(block.data);
  }
}
 //TODO: read n block sequentially
size_t Table::AddFilters(int n)
{
    size_t delta = 0;
    if(rep_->filter == NULL){
	return 0;  //TODO rep_->filter = new FilterBlockReader(rep_->options.filter_policy, block.data);
    }
    int curr_filter_num = rep_->filter->getCurrFiltersNum();
    while(n--&&curr_filter_num < rep_->filter_handles.size()){  // avoid overhead of filters
        delta += FilterPolicy::bits_per_key_per_filter_[curr_filter_num];
	ReadFilter(rep_->filter_handles[curr_filter_num++]); 
    }
    return delta;
}

size_t Table::RemoveFilters(int n)
{
    int curr_filter_num = rep_->filter->getCurrFiltersNum();
    size_t delta = 0;
    if(n == -1){
	rep_->filter->RemoveFilters(curr_filter_num - 1);
	while(curr_filter_num >  1){
	    delete [] (rep_->filter_datas.back());
	    rep_->filter_datas.pop_back();
	    BlockHandle filter_handle;
	    Slice v = rep_->filter_handles[--curr_filter_num];
	    delta += FilterPolicy::bits_per_key_per_filter_[curr_filter_num];
	    if(!filter_handle.DecodeFrom(&v).ok()){
		assert(0);
		return 0;
	    }
	    filter_mem_space -= (filter_handle.size()+kBlockTrailerSize) ;
	    filter_num--;
	}
	return delta;
    }
    rep_->filter->RemoveFilters(n);
    while(n--&& curr_filter_num > 0){
	    delete [] (rep_->filter_datas.back());
	    rep_->filter_datas.pop_back();
	    BlockHandle filter_handle;
	    Slice v = rep_->filter_handles[--curr_filter_num];
	    delta += FilterPolicy::bits_per_key_per_filter_[curr_filter_num];
	    if(!filter_handle.DecodeFrom(&v).ok()){
		assert(0);
		return 0;
	    }
	    filter_mem_space -= (filter_handle.size()+kBlockTrailerSize) ;
	    filter_num--;
    }
    return delta;
}

int64_t Table::AdjustFilters(int n)
{
    int64_t delta = 0;
    uint64_t start_micros = Env::Default()->NowMicros();
   /* if(n < rep_->filter->getCurrFiltersNum()){
	delta = -static_cast<int64_t>(RemoveFilters(rep_->filter->getCurrFiltersNum() - n));
	MeasureTime(Statistics::GetStatistics().get(),Tickers::REMOVE_FILTER_TIME,Env::Default()->NowMicros() - start_micros);
    }else*/
    if(n > rep_->filter->getCurrFiltersNum()){ //only add when greater than
	delta =  AddFilters(n - rep_->filter->getCurrFiltersNum());
    }
    return delta;
}



size_t Table::getCurrFiltersSize(){
    int curr_filter_num = rep_->filter->getCurrFiltersNum();
    size_t table_filter_size = 0;
    while(curr_filter_num--){
      BlockHandle filter_handle;
      Slice v = rep_->filter_handles[curr_filter_num];
      if(!filter_handle.DecodeFrom(&v).ok()){
	assert(0);
	return 0;
      }
      table_filter_size += (filter_handle.size()+kBlockTrailerSize) ;
    }
    return table_filter_size;
}

Table::~Table() {
  delete rep_;
}

static void DeleteBlock(void* arg, void* ignored) {
  delete reinterpret_cast<Block*>(arg);
}

static void DeleteCachedBlock(const Slice& key, void* value) {
  Block* block = reinterpret_cast<Block*>(value);
  delete block;
}

static void ReleaseBlock(void* arg, void* h) {
  Cache* cache = reinterpret_cast<Cache*>(arg);
  Cache::Handle* handle = reinterpret_cast<Cache::Handle*>(h);
  cache->Release(handle);
}

// Convert an index iterator value (i.e., an encoded BlockHandle)
// into an iterator over the contents of the corresponding block.
Iterator* Table::BlockReader(void* arg,
                             const ReadOptions& options,
                             const Slice& index_value) {
  Table* table = reinterpret_cast<Table*>(arg);
  Cache* block_cache = table->rep_->options.block_cache;
  Block* block = NULL;
  Cache::Handle* cache_handle = NULL;

  BlockHandle handle;
  Slice input = index_value;
  Status s = handle.DecodeFrom(&input);
  // We intentionally allow extra stuff in index_value so that we
  // can add more features in the future.

  if (s.ok()) {
    BlockContents contents;
    if (block_cache != NULL) {
      char cache_key_buffer[16];
      EncodeFixed64(cache_key_buffer, table->rep_->cache_id);
      EncodeFixed64(cache_key_buffer+8, handle.offset());
      Slice key(cache_key_buffer, sizeof(cache_key_buffer));
      cache_handle = block_cache->Lookup(key);
      if (cache_handle != NULL) {
        block = reinterpret_cast<Block*>(block_cache->Value(cache_handle));
      } else {
        s = ReadBlock(table->rep_->file, options, handle, &contents);
        if (s.ok()) {
          block = new Block(contents);
          if (contents.cachable && options.fill_cache) {
            cache_handle = block_cache->Insert(
                key, block, block->size(), &DeleteCachedBlock);
          }
        }
      }
    } else {
      s = ReadBlock(table->rep_->file, options, handle, &contents);
      if (s.ok()) {
        block = new Block(contents);
      }
    }
  }

  Iterator* iter;
  if (block != NULL) {
    iter = block->NewIterator(table->rep_->options.comparator);
    if (cache_handle == NULL) {
      iter->RegisterCleanup(&DeleteBlock, block, NULL);
    } else {
      iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle);
    }
  } else {
    iter = NewErrorIterator(s);
  }
  return iter;
}

Iterator* Table::NewIterator(const ReadOptions& options) const {
  return NewTwoLevelIterator(
      rep_->index_block->NewIterator(rep_->options.comparator),
      &Table::BlockReader, const_cast<Table*>(this), options);
}

Status Table::InternalGet(const ReadOptions& options, const Slice& k,
                          void* arg,
                          void (*saver)(void*, const Slice&, const Slice&)) {
  Status s;
  Iterator* iiter = rep_->index_block->NewIterator(rep_->options.comparator);
  iiter->Seek(k);
  if (iiter->Valid()) {
    Slice handle_value = iiter->value();
    FilterBlockReader* filter = rep_->filter;
    BlockHandle handle;
    if (filter != NULL &&
        handle.DecodeFrom(&handle_value).ok() &&
        !filter->KeyMayMatch(handle.offset(), k)) {
      // Not found
    } else {
	uint64_t start_micros = Env::Default()->NowMicros();
	Iterator* block_iter = BlockReader(this, options, iiter->value());
	block_iter->Seek(k);
	if (block_iter->Valid()) {
	    (*saver)(arg, block_iter->key(), block_iter->value());
	}
	 MeasureTime(Statistics::GetStatistics().get(),Tickers::BLOCK_READ_TIME,Env::Default()->NowMicros() - start_micros);
	s = block_iter->status();
	options.read_file_nums++;
	delete block_iter;
    }
  }
  if (s.ok()) {
    s = iiter->status();
  }
  delete iiter;
  return s;
}


uint64_t Table::ApproximateOffsetOf(const Slice& key) const {
  Iterator* index_iter =
      rep_->index_block->NewIterator(rep_->options.comparator);
  index_iter->Seek(key);
  uint64_t result;
  if (index_iter->Valid()) {
    BlockHandle handle;
    Slice input = index_iter->value();
    Status s = handle.DecodeFrom(&input);
    if (s.ok()) {
      result = handle.offset();
    } else {
      // Strange: we can't decode the block handle in the index block.
      // We'll just return the offset of the metaindex block, which is
      // close to the whole file size for this case.
      result = rep_->metaindex_handle.offset();
    }
  } else {
    // key is past the last key in the file.  Approximate the offset
    // by returning the offset of the metaindex block (which is
    // right near the end of the file).
    result = rep_->metaindex_handle.offset();
  }
  delete index_iter;
  return result;
}

}  // namespace leveldb
