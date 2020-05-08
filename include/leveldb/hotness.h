
#ifndef STORAGE_LEVELDB_INCLUDE_HOTNESS_H_
#define STORAGE_LEVELDB_INCLUDE_HOTNESS_H_

#include <stdint.h>
#include <iostream>

namespace leveldb
{
struct HotnessInfos
{
    HotnessInfos() : freqs{}, bf_nums{}, used_num(0)
    {}

    bool Check(uint64_t freq, int bf_num);

    void Insert(uint64_t freq, int bf_num) {
        freqs[used_num] = freq;
        bf_nums[used_num] = bf_num;
        used_num++;
    }

    static const int MaxLength = 32;
    uint64_t freqs[MaxLength];
    int bf_nums[MaxLength];
    int used_num;
};
} // namespace leveldb

#endif