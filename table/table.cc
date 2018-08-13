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
#include <iostream>
#include <list>
//#include <boost/config/detail/posix_features.hpp>
#include <boost/config/posix_features.hpp>


//filter memory space overhead
extern unsigned long long filter_mem_space;
extern unsigned long long filter_num;
extern bool multi_queue_init;
namespace leveldb
{

    struct Table::Rep
    {
        ~Rep()
        {
            if(filter != NULL)
            {
                // int curr_filter_num = filter->getCurrFiltersNum();
                delete filter;
                for(std::vector<std::vector<Slice>>::iterator filter_datas_iter = filter_datas.begin() ; filter_datas_iter != filter_datas.end() ; filter_datas_iter++)
                {
                    // delete [] (*filter_datas_iter);
                    for (std::vector<Slice>::iterator sub_iter = (*filter_datas_iter).begin(); sub_iter != (*filter_datas_iter).end(); sub_iter++)
                    {
                        delete [] ((*sub_iter).data());
                    }
                    (*filter_datas_iter).clear();
//todo
                    // BlockHandle filter_handle;
                    // Slice v = filter_handles[--curr_filter_num];
                    // if(!filter_handle.DecodeFrom(&v).ok())
                    // {
                    //     assert(0);
                    //     return ;
                    // }
                    // filter_mem_space -= (filter_handle.size() + kBlockTrailerSize) ;
                    filter_num--;
                }
            }
            filter_handles.clear();   //followers delete meta will free space in handle slice
            delete index_block;
            if(meta)
            {
                delete meta;
            }
        }

        Options options;
        Status status;
        RandomAccessFile *file;
        uint64_t cache_id;
        FilterBlockReader *filter;
        // const char* filter_data;
        std::vector<std::vector<Slice>> filter_datas; // be careful about vector memory overhead
        std::vector<Slice> filter_handles;    //use string instead of slice
        BlockHandle metaindex_handle;  // Handle to metaindex_block: saved from footer
        Block *index_block;
        Block *meta = NULL;
        std::vector<uint32_t> offsets_;
        size_t base_lg_;
    };

    Status Table::Open(const Options &options,
                       RandomAccessFile *file,
                       uint64_t size,
                       Table **table, size_t * &charge, int file_level, TableMetaData *tableMetaData)
    {
        *table = NULL;
        if (size < Footer::kEncodedLength)
        {
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
        Block *index_block = NULL;
        if (s.ok())
        {
            ReadOptions opt;
            if (options.paranoid_checks)
            {
                opt.verify_checksums = true;
            }
            s = ReadBlock(file, opt, footer.index_handle(), &contents);
            if (s.ok())
            {
                index_block = new Block(contents);
            }
        }

        if (s.ok())
        {
            // We've successfully read the footer and the index block: we're
            // ready to serve requests.
            Rep *rep = new Table::Rep;
            rep->options = options;
            rep->file = file;
            rep->metaindex_handle = footer.metaindex_handle();
            rep->index_block = index_block;
            rep->cache_id = (options.block_cache ? options.block_cache->NewId() : 0);
            rep->filter_datas.clear();
            rep->filter = NULL;
            *table = new Table(rep);
            //   if(isLevel0){
            // (*table)->ReadMeta(footer,options.opEp_.init_filter_nums);//4
            //   }else{
            // (*table)->ReadMeta(footer,options.opEp_.add_filter?1:0);
            //   }

            int add_filter_num = options.opEp_.add_filter ? rep->options.opEp_.init_filter_nums : 0;
            // if (file_level > 3)
            //     add_filter_num = 0;
            if (tableMetaData != NULL)
                add_filter_num = tableMetaData->load_filter_num[0];
            if (file_level == 0)
                add_filter_num = 4;
            (*table)->ReadMeta(footer, charge, add_filter_num, tableMetaData);


            Iterator *iiter = rep->index_block->NewIterator(rep->options.comparator);
            iiter->SeekToLast();
            Slice handle_value = iiter->value();
            BlockHandle handle;
            handle.DecodeFrom(&handle_value);
            uint64_t max_offset = (handle.offset() & ~(1 << options.opEp_.kFilterBaseLg - 1));
            (*table)->freq_count = (max_offset + options.opEp_.kFilterBaseLg + options.opEp_.freq_divide_size - 1) / options.opEp_.freq_divide_size;
            (*table)->freqs = new int[(*table)->freq_count]();
            delete iiter;

        }
        else
        {
            delete index_block;
        }

        return s;
    }

    void Table::ReadMeta(const Footer &footer, size_t * &charge, int add_filter_num, TableMetaData *tableMetaData)
    {
        if (rep_->options.filter_policy == NULL)   //allow add_filter in init_phase
        {
            return;  // Do not need any metadata
        }

        // TODO(sanjay): Skip this if footer.metaindex_handle() size indicates
        // it is an empty block.
        ReadOptions opt;
        if (rep_->options.paranoid_checks)
        {
            opt.verify_checksums = true;
        }
        BlockContents contents;
        if (!ReadBlock(rep_->file, opt, footer.metaindex_handle(), &contents).ok())
        {
            // Do not propagate errors since meta info is not needed for operation
            std::cout << "ReadMeta footer failed" << std::endl;
            return;
        }
        Block *meta = new Block(contents);
        rep_->meta = meta;
        Iterator *iter = meta->NewIterator(BytewiseComparator());

        std::string key_off = "filter.0offsets";
        iter->Seek(key_off);
        if (iter->Valid() && iter->key() == Slice(key_off))
        {
            Slice offsets = iter->value();
            rep_->offsets_.clear();
            const char *contents = offsets.data();
            size_t n = offsets.size();
            rep_->base_lg_ = contents[n - 1];
            if (rep_->options.opEp_.kFilterBaseLg != rep_->base_lg_) {
                // std::cerr << "Options kFilterBaseLg " << rep_->options.opEp_.kFilterBaseLg << ", table kFilterBaseLg " << rep_->base_lg_ << std::endl;
                // exit(1);
            }
            size_t num_ = (n - 5) / 4;
            for (int i = 0; i <= num_; i++)
            {
                uint32_t start = DecodeFixed32(contents + i * 4);
                rep_->offsets_.push_back(start);
            }

            int regionNum = getRegionNum();
            for (int j = 0; j < regionNum; j++)
            {
                std::vector<Slice> region_filter;
                region_filter.clear();
                rep_->filter_datas.push_back(region_filter);
            }
        }

        char id[] = {'1', 0};
        std::string key = "filter." + std::string(id);
        key.append(rep_->options.filter_policy->Name());
        iter->Seek(key);
        if (iter->Valid() && iter->key() == Slice(key))
        {

        }
        else
        {
            std::cout << "ReadMeta filter iter is not valid" << std::endl;

            fprintf(stderr, "filter iter is not valid\n");
        }
        while(iter->Valid())
        {
            rep_->filter_handles.push_back(iter->value());
            iter->Next();
        }
        if(add_filter_num > 0)
        {
            //     fprintf(stderr,"level 0 add %d filters\n",add_filter_num);
            ReadFilters(rep_->filter_handles, charge, add_filter_num, tableMetaData);
        }
        // else if(multi_queue_init)
        // {
        //     ReadFilters(rep_->filter_handles, rep_->options.opEp_.init_filter_nums);
        // }
        // else if(add_filter_num)
        // {
        //     ReadFilters(rep_->filter_handles, 1);
        // }
        delete iter;
        // delete meta;  //reserve meta index_block
    }
    void Table::ReadFilters(std::vector< Slice > &filter_handle_values, size_t * &charge, int n, TableMetaData *tableMetaData)
    {
        Slice v;
        BlockHandle filter_handles[32];
        for(int i = 0 ;  i <  n ; i++)
        {
            v = filter_handle_values[i];
            if(!filter_handles[i].DecodeFrom(&v).ok())
            {
                return;
            }
        }
        ReadOptions opt;
        if (rep_->options.paranoid_checks)
        {
            opt.verify_checksums = true;
        }
        BlockContents blocks[32];
        uint64_t start_micros = Env::Default()->NowMicros();
        if (tableMetaData != NULL) {
            int i;
            for (i = 0; i < n; i++) {
                blocks[i].data = tableMetaData->filter_data[i];
                blocks[i].heap_allocated = true;
                blocks[i].cachable = true;
            }
            for (; i < tableMetaData->filter_num; i++) {
                delete [] tableMetaData->filter_data[i].data();
            }
        }
        else if (!ReadBlocks(rep_->file, opt, filter_handles, blocks, n).ok())
        {
            return;
        }
        int regionNum = getRegionNum();
        charge = new size_t[regionNum]();
        for(int i = 0 ; i < n ; i++)
        {
            if (blocks[i].heap_allocated)
            {
                size_t regionUnit = rep_->options.opEp_.region_divide_size;
                size_t regionOffset = regionUnit / (1 << rep_->base_lg_);
                for (int j = 0; j < regionNum; j++)
                {
                    std::vector<Slice> &region_filter = rep_->filter_datas[j];
                    size_t loc = j * regionOffset;
                    const char *start = blocks[i].data.data() + rep_->offsets_[loc];
                    size_t data_size;
                    if (j != regionNum - 1)
                        data_size = rep_->offsets_[loc + regionOffset] - rep_->offsets_[loc];
                    else
                        data_size = rep_->offsets_[rep_->offsets_.size() - 1] - rep_->offsets_[loc];
                    char *new_data = new char[data_size];
                    memcpy(new_data, start, data_size);
                    Slice filter_region(new_data, data_size);
                    region_filter.push_back(filter_region);
                    // if (j == regionNum - 1) {
                    //     assert(rep_->offsets_[loc] + data_size + 4 * rep_->offsets_.size() == blocks[i].data.size() - 1);
                    // }
                    charge[j] += data_size;
                }
                delete [] blocks[i].data.data();


                // rep_->filter_datas.push_back(blocks[i].data.data());     // Will need to delete later
                // filter_mem_space += blocks[i].data.size();

                filter_mem_space += rep_->offsets_[rep_->offsets_.size() - 1] + rep_->offsets_.size() * 4;
                filter_num++;
            }
            if(rep_->filter == NULL)
            {
                rep_->filter = new FilterBlockReader(rep_->options.filter_policy, regionNum, rep_->options.opEp_.region_divide_size / (1 << rep_->base_lg_), rep_->base_lg_, &rep_->offsets_);
            }
            for (int j = 0; j < regionNum; j++)
            {
                rep_->filter->AddFilter(rep_->filter_datas[j][i], j);
            }

        }
        MeasureTime(Statistics::GetStatistics().get(), Tickers::ADD_FILTER_TIME_0 + n, Env::Default()->NowMicros() - start_micros);
    }
    int Table::getCurrFilterNum(int regionId)
    {
        if(rep_->filter == NULL || regionId < 0)
        {
            return 0;
        }
        return rep_->filter->getCurrFiltersNum(regionId);
    }

    int Table::getRegionNum()
    {
        if(rep_->offsets_.size() == 0)
        {
            return 0;
        }
        size_t data_size = (rep_->offsets_.size() - 1) * (1 << rep_->base_lg_);
        size_t regionFilters = rep_->options.opEp_.region_divide_size;
        return (data_size + regionFilters - 1) / regionFilters;
    }


    size_t Table::ReadFilter(const Slice &filter_handle_value, int regionId)
    {
        Slice v = filter_handle_value;
        BlockHandle filter_handle;
        if (!filter_handle.DecodeFrom(&v).ok())
        {
            return 0;
        }

        // We might want to unify with ReadBlock() if we start
        // requiring checksum verification in Table::Open.
        ReadOptions opt;
        if (rep_->options.paranoid_checks)
        {
            opt.verify_checksums = true;
        }

        BlockContents block;
        uint64_t start_micros = Env::Default()->NowMicros();

        int regionNum = getRegionNum();
        size_t regionUnit = rep_->options.opEp_.region_divide_size;
        size_t regionOffset = regionUnit / (1 << rep_->base_lg_);
        size_t loc = regionId * regionOffset;

        size_t data_size;
        if (regionId != regionNum - 1)
            data_size = rep_->offsets_[loc + regionOffset] - rep_->offsets_[loc];
        else
            data_size = rep_->offsets_[rep_->offsets_.size() - 1] - rep_->offsets_[loc];

        Slice contents;
        char *buf = new char[data_size];
        size_t offset = filter_handle.offset() + rep_->offsets_[loc];

        Status s = rep_->file->Read(offset, data_size, &contents, buf);

        // if (!ReadBlock(rep_->file, opt, filter_handle, &block).ok()) {
        //   return;
        // }
        if (!s.ok())
        {
            delete[] buf;
            return 0;
        }

        if (true || block.heap_allocated)
        {
            std::vector<Slice> &region_filter = rep_->filter_datas[regionId];
            region_filter.push_back(contents);

            filter_mem_space += data_size;
            filter_num++;
            int curr_filter_num = rep_->filter->getCurrFiltersNum(regionId);
            MeasureTime(Statistics::GetStatistics().get(), Tickers::ADD_FILTER_TIME_0 + curr_filter_num + 1, Env::Default()->NowMicros() - start_micros);
        }
        if(rep_->filter == NULL)
        {
            rep_->filter = new FilterBlockReader(rep_->options.filter_policy, regionNum, rep_->options.opEp_.region_divide_size / (1 << rep_->base_lg_), rep_->base_lg_, &rep_->offsets_);
        }
        rep_->filter->AddFilter(contents, regionId);

        return data_size;
    }
    //TODO: read n block sequentially
    size_t Table::AddFilters(int n, int regionId)
    {
        size_t delta = 0;
        if(rep_->filter == NULL)
        {
            // ReadFilters(rep_->filter_handles, n);
            // while (n--)
            //     delta += ReadFilter(rep_->filter_handles, regionId);
            // // for(int i = 0 ;  i <  n ; i++)
            // // {
            // //     delta += FilterPolicy::bits_per_key_per_filter_[i];
            // // }
            // return delta;
            int regionNum = getRegionNum();
            rep_->filter = new FilterBlockReader(rep_->options.filter_policy, regionNum, rep_->options.opEp_.region_divide_size / (1 << rep_->base_lg_), rep_->base_lg_, &rep_->offsets_);

        }
        int curr_filter_num = rep_->filter->getCurrFiltersNum(regionId);
        while(n-- && curr_filter_num < rep_->filter_handles.size()) // avoid overhead of filters
        {
            // delta += FilterPolicy::bits_per_key_per_filter_[curr_filter_num];
            delta += ReadFilter(rep_->filter_handles[curr_filter_num++], regionId);
        }
        return delta;
    }
//todo
    size_t Table::RemoveFilters(int n, int regionId)
    {
        if(rep_->filter == NULL)
        {
            return 0;
        }
        int curr_filter_num = rep_->filter->getCurrFiltersNum(regionId);
        size_t delta = 0;
        if(n == -1)
        {
            delta += rep_->filter->RemoveFilters(curr_filter_num - 1, regionId);
            while(curr_filter_num >  1)
            {
                delete [] (rep_->filter_datas[regionId].back().data());
                rep_->filter_datas[regionId].pop_back();
                // BlockHandle filter_handle;
                Slice v = rep_->filter_handles[--curr_filter_num];
                delta += FilterPolicy::bits_per_key_per_filter_[curr_filter_num];
                // if(!filter_handle.DecodeFrom(&v).ok())
                // {
                //     assert(0);
                //     return 0;
                // }
                // filter_mem_space -= (filter_handle.size() + kBlockTrailerSize) ;
                // filter_num--;
            }
            return delta;
        }
        delta += rep_->filter->RemoveFilters(n, regionId);
        while(n-- && curr_filter_num > 0)
        {
            delete [] (rep_->filter_datas[regionId].back().data());
            rep_->filter_datas[regionId].pop_back();
            // BlockHandle filter_handle;
            Slice v = rep_->filter_handles[--curr_filter_num];
            // delta += FilterPolicy::bits_per_key_per_filter_[curr_filter_num];
            // if(!filter_handle.DecodeFrom(&v).ok())
            // {
            //     assert(0);
            //     return 0;
            // }
            // filter_mem_space -= (filter_handle.size() + kBlockTrailerSize) ;
            filter_num--;
        }
        return delta;
    }

    int64_t Table::AdjustFilters(int n, int regionId)
    {
        int64_t delta = 0;
        uint64_t start_micros = Env::Default()->NowMicros();
        /* if(n < rep_->filter->getCurrFiltersNum()){
        delta = -static_cast<int64_t>(RemoveFilters(rep_->filter->getCurrFiltersNum() - n));
        MeasureTime(Statistics::GetStatistics().get(),Tickers::REMOVE_FILTER_TIME,Env::Default()->NowMicros() - start_micros);
         }else*/
        assert(n >= 0);
        if(n > 0)
        {
            if(rep_->filter == NULL)
            {
                delta =  AddFilters(n, regionId);
            }
            else if(n > rep_->filter->getCurrFiltersNum(regionId))  //only add when greater than
            {
                delta =  AddFilters(n - rep_->filter->getCurrFiltersNum(regionId), regionId);
            }
        }
        return delta;
    }


    //todo
    size_t Table::getCurrFiltersSize(int regionId)
    {
        if(rep_->filter == NULL || regionId < 0)
        {
            return 0;
        }
        size_t totalSize = 0;
        for (int i = 0; i < rep_->filter->getCurrFiltersNum(regionId); i++) {
            totalSize += rep_->filter_datas[regionId][i].size();
        }
        return totalSize;
    }

    Table::~Table()
    {
        delete rep_;
        if (freq_count != 0) delete freqs;
    }

    static void DeleteBlock(void *arg, void *ignored)
    {
        delete reinterpret_cast<Block *>(arg);
    }

    static void DeleteCachedBlock(const Slice &key, void *value)
    {
        Block *block = reinterpret_cast<Block *>(value);
        delete block;
    }

    static void ReleaseBlock(void *arg, void *h)
    {
        Cache *cache = reinterpret_cast<Cache *>(arg);
        Cache::Handle *handle = reinterpret_cast<Cache::Handle *>(h);
        cache->Release(handle);
    }

    // Convert an index iterator value (i.e., an encoded BlockHandle)
    // into an iterator over the contents of the corresponding block.
    Iterator *Table::BlockReader(void *arg,
                                 const ReadOptions &options,
                                 const Slice &index_value)
    {
        Table *table = reinterpret_cast<Table *>(arg);
        Cache *block_cache = table->rep_->options.block_cache;
        Block *block = NULL;
        Cache::Handle *cache_handle = NULL;

        BlockHandle handle;
        Slice input = index_value;
        Status s = handle.DecodeFrom(&input);
        // We intentionally allow extra stuff in index_value so that we
        // can add more features in the future.
        uint64_t start_micros = Env::Default()->NowMicros();
        if (s.ok())
        {
            BlockContents contents;
            if (block_cache != NULL)
            {
                char cache_key_buffer[16];
                EncodeFixed64(cache_key_buffer, table->rep_->cache_id);
                EncodeFixed64(cache_key_buffer + 8, handle.offset());
                Slice key(cache_key_buffer, sizeof(cache_key_buffer));
                cache_handle = block_cache->Lookup(key);
                if (cache_handle != NULL)
                {
                    block = reinterpret_cast<Block *>(block_cache->Value(cache_handle));
                    MeasureTime(Statistics::GetStatistics().get(), Tickers::BLOCKREADER_CACHE_TIME, Env::Default()->NowMicros() - start_micros);
                }
                else
                {
                    s = ReadBlock(table->rep_->file, options, handle, &contents);
                    if (s.ok())
                    {
                        block = new Block(contents);
                        if (contents.cachable && options.fill_cache)
                        {
                            cache_handle = block_cache->Insert(
                                               key, block, block->size(), &DeleteCachedBlock);
                        }
                    }
                    MeasureTime(Statistics::GetStatistics().get(), Tickers::BLOCKREADER_NOCACHE_TIME, Env::Default()->NowMicros() - start_micros);
                }
            }
            else
            {
                s = ReadBlock(table->rep_->file, options, handle, &contents);
                MeasureTime(Statistics::GetStatistics().get(), Tickers::BLOCKREADER_NOCACHE_TIME, Env::Default()->NowMicros() - start_micros);
                if (s.ok())
                {
                    block = new Block(contents);
                }
            }
        }

        Iterator *iter;
        if (block != NULL)
        {
            iter = block->NewIterator(table->rep_->options.comparator);
            if (cache_handle == NULL)
            {
                iter->RegisterCleanup(&DeleteBlock, block, NULL);
            }
            else
            {
                iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle);
            }
        }
        else
        {
            iter = NewErrorIterator(s);
        }
        return iter;
    }

    Iterator *Table::NewIterator(const ReadOptions &options) const
    {
        return NewTwoLevelIterator(
                   rep_->index_block->NewIterator(rep_->options.comparator),
                   &Table::BlockReader, const_cast<Table *>(this), options);
    }

    void Table::getRegionKeyRangesByStr(const Options *options, Slice &index_content, std::vector<Slice> &region_keys) {
        BlockContents contents;
        contents.data = index_content;
        contents.cachable = true;
        contents.heap_allocated = true;
        
        Block *index_block = new Block(contents);

        Iterator *iiter = index_block->NewIterator(options->comparator);
        iiter->SeekToLast();
        Slice handle_value = iiter->value();
        BlockHandle handle;
        handle.DecodeFrom(&handle_value);
        uint64_t max_offset = (handle.offset() & ~(1 << options->opEp_.kFilterBaseLg - 1));
        int count = (max_offset + options->opEp_.kFilterBaseLg + options->opEp_.freq_divide_size - 1) / options->opEp_.freq_divide_size;
        for (int i = 0; i < count; i++) {
            uint64_t cur_offset = i * options->opEp_.freq_divide_size;
            iiter->SeekByValue(cur_offset);
            region_keys.push_back(iiter->key().copy());
        }
        iiter->SeekToLast();
        region_keys.push_back(iiter->key().copy());
        delete iiter;
    }

    void Table::getRegionKeyRanges(std::vector<Slice> &region_keys) {
        Iterator *iiter = rep_->index_block->NewIterator(rep_->options.comparator);
        iiter->SeekToLast();
        Slice handle_value = iiter->value();
        BlockHandle handle;
        handle.DecodeFrom(&handle_value);
        uint64_t max_offset = (handle.offset() & ~(1 << rep_->options.opEp_.kFilterBaseLg - 1));
        int count = (max_offset + rep_->options.opEp_.kFilterBaseLg + rep_->options.opEp_.freq_divide_size - 1) / rep_->options.opEp_.freq_divide_size;
        for (int i = 0; i < count; i++) {
            uint64_t cur_offset = i * rep_->options.opEp_.freq_divide_size;
            iiter->SeekByValue(cur_offset);
            region_keys.push_back(iiter->key().copy());
        }
        iiter->SeekToLast();
        region_keys.push_back(iiter->key().copy());
        delete iiter;
    }

    Status Table::InternalGet(const ReadOptions &options, const Slice &k,
                              void *arg,
                              void (*saver)(void *, const Slice &, const Slice &), char *region_name)
    {
        Status s;
        Iterator *iiter = rep_->index_block->NewIterator(rep_->options.comparator);

        iiter->Seek(k);
        uint64_t start_micros = Env::Default()->NowMicros();
        if (iiter->Valid())
        {
            options.access_file_nums++;

            Slice handle_value = iiter->value();
            FilterBlockReader *filter = rep_->filter;
            BlockHandle handle;
            Status ds = handle.DecodeFrom(&handle_value);

            EncodeFixed64(region_name, options.file_number);
            uint32_t *id_ = (uint32_t *) (region_name + sizeof(uint64_t));
            *id_ = handle.offset() / rep_->options.opEp_.region_divide_size + 1;

            if (multi_queue_init && getCurrFilterNum(*id_ - 1) == 0) {
            // if (getCurrFilterNum(*id_ - 1) == 0) {
                AddFilters(rep_->options.opEp_.init_filter_nums, *id_ - 1);
            }


            if (filter != NULL &&
                    ds.ok() &&
                    !filter->KeyMayMatch(handle.offset(), k))
            {
                // Not found
                MeasureTime(Statistics::GetStatistics().get(), Tickers::FILTER_MATCHES_TIME, Env::Default()->NowMicros() - start_micros);
            }
            else
            {
                start_micros = Env::Default()->NowMicros();
                Iterator *block_iter = BlockReader(this, options, iiter->value());
                block_iter->Seek(k);
                if (block_iter->Valid())
                {
                    (*saver)(arg, block_iter->key(), block_iter->value());
                }

                MeasureTime(Statistics::GetStatistics().get(), Tickers::BLOCK_READ_TIME, Env::Default()->NowMicros() - start_micros);
                s = block_iter->status();
                options.read_file_nums++;
                delete block_iter;
            }

            if (filter == NULL)
                options.total_fpr += 1;
            else
                options.total_fpr += filter->getCurrFpr();
            if (ds.ok())
            {
                uint64_t offset = handle.offset();
                uint64_t which = offset / rep_->options.opEp_.freq_divide_size;
                if (which < freq_count)
                    freqs[which]++;
                else
                {
                    std::cout << "error freq overflow, which " << which << " count " << freq_count << " offset " << offset << " size " << handle.size() << std::endl;
                }

            }
        }
        if (s.ok())
        {
            s = iiter->status();
        }
        delete iiter;
        return s;
    }


    uint64_t Table::ApproximateOffsetOf(const Slice &key) const
    {
        Iterator *index_iter =
            rep_->index_block->NewIterator(rep_->options.comparator);
        index_iter->Seek(key);
        uint64_t result;
        if (index_iter->Valid())
        {
            BlockHandle handle;
            Slice input = index_iter->value();
            Status s = handle.DecodeFrom(&input);
            if (s.ok())
            {
                result = handle.offset();
            }
            else
            {
                // Strange: we can't decode the block handle in the index block.
                // We'll just return the offset of the metaindex block, which is
                // close to the whole file size for this case.
                result = rep_->metaindex_handle.offset();
            }
        }
        else
        {
            // key is past the last key in the file.  Approximate the offset
            // by returning the offset of the metaindex block (which is
            // right near the end of the file).
            result = rep_->metaindex_handle.offset();
        }
        delete index_iter;
        return result;
    }
    uint64_t Table::LRU_Fre_Count = 0;
}  // namespace leveldb
