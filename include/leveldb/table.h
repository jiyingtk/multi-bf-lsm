// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_INCLUDE_TABLE_H_
#define STORAGE_LEVELDB_INCLUDE_TABLE_H_

#include <stdint.h>
#include <vector>

#include "leveldb/iterator.h"

namespace leveldb {

class Block;
class BlockHandle;
class Footer;
struct Options;
class RandomAccessFile;
struct ReadOptions;
class TableCache;
struct TableMetaData;

// A Table is a sorted map from strings to strings.  Tables are
// immutable and persistent.  A Table may be safely accessed from
// multiple threads without external synchronization.
class Table {
 public:
  // Attempt to open the table that is stored in bytes [0..file_size)
  // of "file", and read the metadata entries necessary to allow
  // retrieving data from the table.
  //
  // If successful, returns ok and sets "*table" to the newly opened
  // table.  The client should delete "*table" when no longer needed.
  // If there was an error while initializing the table, sets "*table"
  // to NULL and returns a non-ok status.  Does not take ownership of
  // "*source", but the client must ensure that "source" remains live
  // for the duration of the returned table's lifetime.
  //
  // *file must remain live while this Table is in use.
  static Status Open(const Options& options,
                     RandomAccessFile* file,
                     uint64_t file_size,
                     Table** table, size_t * &charge, int file_level = 1, TableMetaData *tableMetaData=NULL);

  ~Table();

  // Returns a new iterator over the table contents.
  // The result of NewIterator() is initially invalid (caller must
  // call one of the Seek methods on the iterator before using it).
  Iterator* NewIterator(const ReadOptions&) const;

  // Given a key, return an approximate byte offset in the file where
  // the data for that key begins (or would begin if the key were
  // present in the file).  The returned value is in terms of file
  // bytes, and so includes effects like compression of the underlying data.
  // E.g., the approximate offset of the last key in the table will
  // be close to the file length.
  uint64_t ApproximateOffsetOf(const Slice& key) const;
   size_t AddFilters(int n, int regionId);
   size_t* AddFilters(int n, int regionId_start, int regionId_end);
   size_t AdjustFilters(int n, int regionId = 0);
   size_t* AdjustFilters(int n, int regionId_start, int regionId_end, bool locked = false);
   size_t RemoveFilters(int n, int regionId = 0);
   size_t getCurrFiltersSize(int regionId = 0);
   int getCurrFilterNum(int regionId = 0, bool needLock = false);
   int getRegionNum();
   void setAccess(int regionId);
   bool isAccess(int regionId);
    void getRegionKeyRanges(std::vector<Slice> &region_keys);
   static void getRegionKeyRangesByStr(const Options *options, Slice &index_content, std::vector<Slice> &region_keys);

   static uint64_t LRU_Fre_Count;
  int freq_count;
  int *freqs;

 private:
  struct Rep;
  Rep* rep_;

  explicit Table(Rep* rep) : freq_count(0) { rep_ = rep; }
  static Iterator* BlockReader(void*, const ReadOptions&, const Slice&);

  // Calls (*handle_result)(arg, ...) with the entry found after a call
  // to Seek(key).  May not make such a call if filter policy says
  // that key is not present.
  friend class TableCache;
  Status InternalGet(
      const ReadOptions&, const Slice& key,
      void* arg,
      void (*handle_result)(void* arg, const Slice& k, const Slice& v), char* region_name);


  void ReadMeta(const Footer& footer, size_t * &charge, int add_filter_num=1, TableMetaData *tableMetaData=NULL); //int add_filter
  size_t ReadFilter(const Slice& filter_handle_value, int regionId);
  void ReadFilter(const Slice& filter_handle_value, int regionId_start, int regionId_end, size_t *delta);
  void ReadFilters(std::vector<Slice> &filter_handle_values, size_t * &charge, int n, TableMetaData *tableMetaData=NULL);
  // No copying allowed
  Table(const Table&);
  void operator=(const Table&);
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_TABLE_H_
