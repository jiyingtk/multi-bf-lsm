// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/filter_block.h"

#include "leveldb/filter_policy.h"
#include "util/coding.h"
#include <util/stop_watch.h>
#include <unistd.h>
#include <math.h>
#include<atomic>
#ifndef handle_error_en
#define handle_error_en(en, msg) \
  do { errno = en; perror(msg); exit(EXIT_FAILURE); } while (0)
#endif
namespace leveldb {

// See doc/table_format.md for an explanation of the filter block format.

// Original:Generate new filter every 2KB of data
//TODO: increase kFilterBaseLg
//FULL-filter
static const size_t kFilterBaseLg = 16;  //try every 64KB of data
static const size_t kFilterBase = 1 << kFilterBaseLg;

FilterBlockBuilder::FilterBlockBuilder(const FilterPolicy* policy)
    : policy_(policy) {
	results_.resize(policy->filterNums());
	filters_offsets_.resize(policy->filterNums());
}

void FilterBlockBuilder::StartBlock(uint64_t block_offset) {
  uint64_t filter_index = (block_offset / kFilterBase);
  std::list<std::vector<uint32_t>>::iterator filters_offsets_begin = filters_offsets_.begin();
  assert(filter_index >= filters_offsets_begin->size());
  while (filter_index > filters_offsets_begin->size()) {
    GenerateFilter();
  }
}

void FilterBlockBuilder::AddKey(const Slice& key) {
  Slice k = key;
  start_.push_back(keys_.size());
  keys_.append(k.data(), k.size());
}

std::list<std::string>& FilterBlockBuilder::Finish() {
  if (!start_.empty()) {
    GenerateFilter();
  }

  // Append array of per-filter offsets
  auto filters_offsets_iter = filters_offsets_.begin();
  for(auto results_iter = results_.begin() ; results_iter != results_.end() ; results_iter++){
//	const uint32_t array_offset = result_.size();
        const uint32_t array_offset = results_iter->size();
	for (size_t i = 0; i < filters_offsets_iter->size(); i++) {
	    PutFixed32(&(*results_iter), (*filters_offsets_iter)[i]);
	}
	
	PutFixed32(&(*results_iter), array_offset);
	results_iter->push_back(kFilterBaseLg);  // Save encoding parameter in result
	filters_offsets_iter++;
  }
  return results_;
}

std::string* FilterBlockBuilder::getOffsets(int which) {
  auto results_iter = results_.begin();
  
  for(int i = 0; results_iter != results_.end() ; results_iter++, i++){
    if (i == which)
      break;
  }

  Slice contents((*results_iter));
  size_t n = contents.size();
  if (n < 5) return new std::string();  // 1 byte for base_lg_ and 4 for start of offset array

  uint32_t last_word = DecodeFixed32(contents.data() + n - 5);
  if (last_word > n - 5) return new std::string();

  // char *buf = new char[n - last_word];
  // memcpy(buf, contents.data() + last_word, n - last_word);
  // std::string* dst = new std::string(buf, sizeof(buf));
  std::string* dst = new std::string(contents.data() + last_word, n - last_word);
  return dst;
}


void FilterBlockBuilder::GenerateFilter() {
  const size_t num_keys = start_.size();
  auto filters_offsets_iter = filters_offsets_.begin();
  auto results_iter = results_.begin();
  if (num_keys == 0) {
    // Fast path if there are no keys for this filter
    for(;filters_offsets_iter != filters_offsets_.end() ; filters_offsets_iter++){
	filters_offsets_iter->push_back(results_iter->size());
	results_iter++;
    }
    //filter_offsets_.push_back(result_.size());
    return;
  }

  // Make list of keys from flattened key structure
  start_.push_back(keys_.size());  // Simplify length computation
  tmp_keys_.resize(num_keys);
  for (size_t i = 0; i < num_keys; i++) {
    const char* base = keys_.data() + start_[i];
    size_t length = start_[i+1] - start_[i];
    tmp_keys_[i] = Slice(base, length);
  }

  // Generate filter for current set of keys and append to result_.
  //filter_offsets_.push_back(result_.size());
   for(;filters_offsets_iter!= filters_offsets_.end() ; filters_offsets_iter++){
	filters_offsets_iter->push_back(results_iter->size());
	results_iter++;
    }
    uint64_t start_micros = Env::Default()->NowMicros();
    policy_->CreateFilter(&tmp_keys_[0], static_cast<int>(num_keys), results_);
    MeasureTime(Statistics::GetStatistics().get(),Tickers::CREATE_FILTER_TIME,Env::Default()->NowMicros() - start_micros);
  tmp_keys_.clear();
  keys_.clear();
  start_.clear();
}
class FilterPolicy;


std::atomic<bool> FilterBlockReader::start_matches[8];
bool FilterBlockReader::matches[8];
// std::vector<uint32_t> *FilterBlockReader::filter_offsets = NULL;
std::vector<const char*> *FilterBlockReader::filter_datas = NULL;
int FilterBlockReader::filter_index(0);
bool FilterBlockReader::pthread_created(false);
bool FilterBlockReader::end_thread(false);
const FilterPolicy* FilterBlockReader::filter_policy(NULL);
Slice FilterBlockReader::filter_key;
pthread_t FilterBlockReader::pids_[8];

void *FilterBlockReader::KeyMayMatch_Thread(void* arg)
{
 //       int id = *(int*)(arg);
	// delete (int *)(arg);
	// uint32_t start,limit;
	// while(true){
	//     while(!start_matches[id]&&!end_thread){
	// 	sched_yield();
	//     }
	//     if(end_thread){
	// 	break;
	//      }
	//     start = DecodeFixed32((*filter_offsets)[id] + filter_index*4);
	//     limit = DecodeFixed32((*filter_offsets)[id] + filter_index*4 + 4);
	//     if (start <= limit && limit <= static_cast<size_t>((*filter_offsets)[id] - (*filter_datas)[id])) {
	// 	Slice filter = Slice((*filter_datas)[id] + start, limit - start);
	// 	 matches[id] = filter_policy->KeyMayMatch(filter_key,filter,id);
	//     } else if (start == limit) {
	// 	matches[id] = false;
	//     }
	//     start_matches[id] = false;
	// }
}


void FilterBlockReader::CreateThread(int filters_num,const leveldb::FilterPolicy *policy)
{
    int i = 0;
    char name_buf[24];
    int cpu_count =  sysconf(_SC_NPROCESSORS_CONF);
    filter_policy = policy;
    int base_cpu_id = 16;
    for(i = 0 ; i < filters_num ; i++){
	start_matches[i] = false;
	matches[i] = true;
    }
    for(i = 0 ; i < filters_num ; i++){
	int *temp_id = new int(i);
	cpu_set_t cpuset;
	CPU_ZERO(&cpuset);
	CPU_SET(base_cpu_id + i, &cpuset);
	if(pthread_create(pids_+i,NULL, FilterBlockReader::KeyMayMatch_Thread,(void*)(temp_id))!=0){
		  perror("create thread ");
	}
	snprintf(name_buf, sizeof name_buf, "filter_match:bg%d" ,i);
	name_buf[sizeof name_buf - 1] = '\0';
	pthread_setname_np(pids_[i], name_buf);
	if(base_cpu_id + filters_num < cpu_count ){
		int s = pthread_setaffinity_np(pids_[i], sizeof(cpu_set_t), &cpuset);
		if (s != 0){
		    handle_error_en(s, "pthread_setaffinity_np");
		  }
	}
    }
}

FilterBlockReader::FilterBlockReader(const FilterPolicy* policy,
                                     int regionNum, int regionFilters_, int base_lg, std::vector<std::vector<uint32_t>> *filter_offsets_)
    : policy_(policy), curr_num_of_filters_(0), regionFilters(regionFilters_), 
      num_((*filter_offsets_)[0].size()), num_regions(regionNum), filter_offsets(filter_offsets_),
      base_lg_(base_lg), max_num_of_filters_(policy->filterNums()){

  if (regionNum == 0) {
    handle_error_en(1, "filter block size is too large\n");
  }

  datas_.clear();
  for (int i = 0; i < num_regions; i++) {
    std::vector<const char *> region_data;
    region_data.clear();
    datas_.push_back(region_data);
  }
  curr_num_of_filters_regions_ = new int[num_regions]();

}

FilterBlockReader::~FilterBlockReader() {
  datas_.clear();
  delete [] curr_num_of_filters_regions_;
}

void FilterBlockReader::AddFilter(Slice &contents, int regionId)
{
  assert(1 + curr_num_of_filters_regions_[regionId] <= max_num_of_filters_);

  std::vector<const char *> &region_data = datas_[regionId];
  region_data.push_back(contents.data());

  curr_num_of_filters_regions_[regionId]++;
}

size_t FilterBlockReader::RemoveFilters(int n, int regionId)
{
  size_t delta = 0;
    // assert(n <= curr_num_of_filters_regions_[regionId]);	//at least 0 filters
  if (n > curr_num_of_filters_regions_[regionId]) {
    delta = 0;
    return delta;
  }

    while(n--){
	//const char *toBeDelete = datas_.back();
	//delete []toBeDelete;  // note: no need to be deleted, thus, FilterBlockReader is just a manager for filter blocks.

      size_t loc = regionId * regionFilters;

      int i = curr_num_of_filters_regions_[regionId] - 1;
      size_t data_size;
      if (regionId != num_regions - 1)
          data_size = (*filter_offsets)[i][loc + regionFilters] - (*filter_offsets)[i][loc];
      else
          data_size = (*filter_offsets)[i][(*filter_offsets)[i].size() - 1] - (*filter_offsets)[i][loc];

    	delta += data_size;
      datas_[regionId].pop_back();
    	// offsets_.pop_back();
    	curr_num_of_filters_regions_[regionId]--;
    }
  
  return delta;
}

//todo
double FilterBlockReader::getCurrFpr() {
  double ret = 0;
  int sum_bits = 0;
  for (int i = 0; i < curr_num_of_filters_; i++) {
    sum_bits += FilterPolicy::bits_per_key_per_filter_[i];
  }
  ret = pow(0.6185,sum_bits);
  return ret;
}

//todo
bool FilterBlockReader::KeyMayMatch(uint64_t block_offset, const Slice &key)
{
  uint64_t index = block_offset >> base_lg_;
  std::list<Slice> filters;
  int regionId = index / regionFilters;
  if (index < num_ && curr_num_of_filters_regions_[regionId] > 0)
  {
      for(int i = 0 ; i < curr_num_of_filters_regions_[regionId] ; i++)
      {
          uint32_t start = (*filter_offsets)[i][index];
          uint32_t limit = (*filter_offsets)[i][index + 1];
          int region_index = regionId * regionFilters;
          uint32_t region_start = start - (*filter_offsets)[i][region_index];
          if (start <= limit)
          {
              Slice filter = Slice(datas_[regionId][i] + region_start, limit - start);
              filters.push_back(filter);
          }
          else if (start >= limit)
          {
              // Empty filters do not match any keys
              printf("empty filters\n");
              return false;
          }
      }
      return policy_->KeyMayMatchFilters(key, filters);
  }
  return true;  // Errors are treated as potential matches
}


}
