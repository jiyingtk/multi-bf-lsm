// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/format.h"

#include "leveldb/env.h"
#include "port/port.h"
#include "table/block.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include <iostream>

namespace leveldb {

void BlockHandle::EncodeTo(std::string* dst) const {
  // Sanity check that all fields have been set
  assert(offset_ != ~static_cast<uint64_t>(0));
  assert(size_ != ~static_cast<uint64_t>(0));
  PutVarint64(dst, offset_);
  PutVarint64(dst, size_);
}

Status BlockHandle::DecodeFrom(Slice* input) {
  if (GetVarint64(input, &offset_) &&
      GetVarint64(input, &size_)) {
    return Status::OK();
  } else {
    return Status::Corruption("bad block handle");
  }
}

void Footer::EncodeTo(std::string* dst) const {
  const size_t original_size = dst->size();
  metaindex_handle_.EncodeTo(dst);
  index_handle_.EncodeTo(dst);
  dst->resize(2 * BlockHandle::kMaxEncodedLength);  // Padding
  PutFixed32(dst, static_cast<uint32_t>(kTableMagicNumber & 0xffffffffu));
  PutFixed32(dst, static_cast<uint32_t>(kTableMagicNumber >> 32));
  assert(dst->size() == original_size + kEncodedLength);
  (void)original_size;  // Disable unused variable warning.
}

Status Footer::DecodeFrom(Slice* input) {
  const char* magic_ptr = input->data() + kEncodedLength - 8;
  const uint32_t magic_lo = DecodeFixed32(magic_ptr);
  const uint32_t magic_hi = DecodeFixed32(magic_ptr + 4);
  const uint64_t magic = ((static_cast<uint64_t>(magic_hi) << 32) |
                          (static_cast<uint64_t>(magic_lo)));
  if (magic != kTableMagicNumber) {
    return Status::Corruption("not an sstable (bad magic number)");
  }

  Status result = metaindex_handle_.DecodeFrom(input);
  if (result.ok()) {
    result = index_handle_.DecodeFrom(input);
  }
  if (result.ok()) {
    // We skip over any leftover data (just padding for now) in "input"
    const char* end = magic_ptr + 8;
    *input = Slice(end, input->data() + input->size() - end);
  }
  return result;
}

Status ReadBlocks(RandomAccessFile* file,
                 const ReadOptions& options,
                 const BlockHandle* handles_ptr,
                 BlockContents* results,int n) {
    int i;
    size_t lens[32],sum_lens=0;
    char *bufs[32];
    for(i = 0 ; i <  n ; ++i){
	results[i].data = Slice();
	results[i].cachable = false;
	results[i].heap_allocated = false;
	bufs[i] = new char[handles_ptr[i].size() + kBlockTrailerSize];
	lens[i] = handles_ptr[i].size() + kBlockTrailerSize;
	sum_lens += handles_ptr[i].size();
    }
    Slice contents[32];
    Status s = file->Reads(handles_ptr[0].offset(), sum_lens + n*kBlockTrailerSize, contents, bufs,lens,n);
    if (!s.ok()) {
  std::cout << "ReadBlocks read failed, n " << n << " offset " << handles_ptr[0].offset() << " len0 " << lens[0] << " status " << s.ToString() << std::endl;

	 for(i = 0 ; i <  n ; ++i){
		delete [] bufs[i];
	 }
	return s;
    }
    size_t contents_size = 0;
    for(i = 0 ; i < n ; i++){
	contents_size += contents[i].size();
    }
    if (contents_size != sum_lens + n*kBlockTrailerSize) {
	    for(i = 0 ; i <  n ; ++i){
		delete [] bufs[i];
	    }
	    return Status::Corruption("truncated block read");
    }
    const char* data = contents[0].data();    // Pointer to where Read put the data
    bool cache_flag = (data == bufs[0]);
    for(i = 0 ; i < n ; i++){
	 data = contents[i].data();
	 switch (data[handles_ptr[i].size()]) {
	    case kNoCompression:
		if (!cache_flag) {
		    // File implementation gave us pointer to some other data.
		    // Use it directly under the assumption that it will be live
		    // while the file is open.
		    delete[] bufs[i];
		    results[i].data = Slice(data, handles_ptr[i].size());
		    results[i].heap_allocated = false;
		    results[i].cachable = false;  // Do not double-cache
		} else {
		    results[i].data = Slice(bufs[i], handles_ptr[i].size());
		    results[i].heap_allocated = true;
		    results[i].cachable = true;
		}
		// Ok
		break;
	    case kSnappyCompression: {
		 size_t ulength = 0;
		 if (!port::Snappy_GetUncompressedLength(data, handles_ptr[i].size(), &ulength)) {
			delete[] bufs[i];
			return Status::Corruption("corrupted compressed block contents");
		  }
		  char* ubuf = new char[ulength];
		  if (!port::Snappy_Uncompress(data, handles_ptr[i].size(), ubuf)) {
			delete[] bufs[i];
			delete[] ubuf;
			return Status::Corruption("corrupted compressed block contents");
		    }
		    delete[] bufs[i];
		    results[i].data = Slice(ubuf, ulength);
		    results[i].heap_allocated = true;
		    results[i].cachable = true;
		    break;
	    }
	    default:{
		    delete [] bufs[i];
	    }
	    return Status::Corruption("bad block type");
	}
    }
    return Status::OK();
}

Status ReadBlock(RandomAccessFile* file,
                 const ReadOptions& options,
                 const BlockHandle& handle,
                 BlockContents* result) {
  result->data = Slice();
  result->cachable = false;
  result->heap_allocated = false;

  // Read the block contents as well as the type/crc footer.
  // See table_builder.cc for the code that built this structure.
  uint64_t start_micros = Env::Default()->NowMicros();
  size_t n = static_cast<size_t>(handle.size());
  char* buf = new char[n + kBlockTrailerSize];
  MeasureTime(Statistics::GetStatistics().get(),Tickers::READ_BLOCK_NEW_TIME,Env::Default()->NowMicros() - start_micros);
  Slice contents;
  start_micros = Env::Default()->NowMicros();
  Status s = file->Read(handle.offset(), n + kBlockTrailerSize, &contents, buf);
  MeasureTime(Statistics::GetStatistics().get(),Tickers::READ_BLOCK_FILE_READ_TIME,Env::Default()->NowMicros() - start_micros);
  if (!s.ok()) {
    delete[] buf;
    return s;
  }
  if (contents.size() != n + kBlockTrailerSize) {
    delete[] buf;
    return Status::Corruption("truncated block read");
  }

  // Check the crc of the type and the block contents
  const char* data = contents.data();    // Pointer to where Read put the data
  if (options.verify_checksums) {
    const uint32_t crc = crc32c::Unmask(DecodeFixed32(data + n + 1));
    const uint32_t actual = crc32c::Value(data, n + 1);
    if (actual != crc) {
      delete[] buf;
      s = Status::Corruption("block checksum mismatch");
      return s;
    }
  }

  switch (data[n]) {
    case kNoCompression:
      if (data != buf) {
        // File implementation gave us pointer to some other data.
        // Use it directly under the assumption that it will be live
        // while the file is open.
        delete[] buf;
        result->data = Slice(data, n);
        result->heap_allocated = false;
        result->cachable = false;  // Do not double-cache
      } else {
        result->data = Slice(buf, n);
        result->heap_allocated = true;
        result->cachable = true;
      }

      // Ok
      break;
    case kSnappyCompression: {
      size_t ulength = 0;
      if (!port::Snappy_GetUncompressedLength(data, n, &ulength)) {
        delete[] buf;
        return Status::Corruption("corrupted compressed block contents");
      }
      char* ubuf = new char[ulength];
      if (!port::Snappy_Uncompress(data, n, ubuf)) {
        delete[] buf;
        delete[] ubuf;
        return Status::Corruption("corrupted compressed block contents");
      }
      delete[] buf;
      result->data = Slice(ubuf, ulength);
      result->heap_allocated = true;
      result->cachable = true;
      break;
    }
    default:
      delete[] buf;
      return Status::Corruption("bad block type");
  }

  return Status::OK();
}

}  // namespace leveldb
