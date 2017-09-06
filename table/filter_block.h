// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A filter block is stored near the end of a Table file.  It contains
// filters (e.g., bloom filters) for all data blocks in the table combined
// into a single filter block.

#ifndef STORAGE_LEVELDB_TABLE_FILTER_BLOCK_H_
#define STORAGE_LEVELDB_TABLE_FILTER_BLOCK_H_

#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>
#include "leveldb/slice.h"
#include "util/hash.h"
#include<list>
namespace leveldb {

class FilterPolicy;

// A FilterBlockBuilder is used to construct all of the filters for a
// particular Table.  It generates a single string which is stored as
// a special block in the Table.
//
// The sequence of calls to FilterBlockBuilder must match the regexp:
//      (StartBlock AddKey*)* Finish
class FilterBlockBuilder {
 public:
  explicit FilterBlockBuilder(const FilterPolicy*);

  void StartBlock(uint64_t block_offset);
  void AddKey(const Slice& key);
  std::list<std::string> &Finish();

 private:
  void GenerateFilter();

  const FilterPolicy* policy_;
  std::string keys_;              // Flattened key contents
  std::vector<size_t> start_;     // Starting index in keys_ of each key
  //std::string result_;            // Filter data computed so far
  std::list<std::string> results_;
  std::vector<Slice> tmp_keys_;   // policy_->CreateFilter() argument
 // std::vector<uint32_t> filter_offsets_;
  std::list<std::vector<uint32_t>> filters_offsets_;

  // No copying allowed
  FilterBlockBuilder(const FilterBlockBuilder&);
  void operator=(const FilterBlockBuilder&);
};

class FilterBlockReader {
 public:
 // REQUIRES: "contents" and *policy must stay live while *this is live.
  FilterBlockReader(const FilterPolicy* policy, const Slice& contents);
  bool KeyMayMatch(uint64_t block_offset, const Slice& key);
  void AddFilter(Slice& contents);
  void RemoveFilters(int n);
  inline int getCurrFiltersNum(){
    return curr_num_of_filters_;  
  }
 private:
  const FilterPolicy* policy_;
  std::vector<const char*> datas_;    // Pointer to filter data (at block-start)
  std::vector<const char*> offsets_;  // Pointer to beginning of offset array (at block-end)
  size_t num_;          // Number of entries in offset array
  size_t base_lg_;      // Encoding parameter (see kFilterBaseLg in .cc file)
  int max_num_of_filters_;
  int curr_num_of_filters_;
  void readFilters(const Slice& contents);
};

}

#endif  // STORAGE_LEVELDB_TABLE_FILTER_BLOCK_H_
