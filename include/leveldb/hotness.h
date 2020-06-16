
#ifndef STORAGE_LEVELDB_INCLUDE_HOTNESS_H_
#define STORAGE_LEVELDB_INCLUDE_HOTNESS_H_

#include <stdint.h>
#include <cstring>
#include <iostream>

namespace leveldb
{
struct HotnessInfos
{
    HotnessInfos() : freqs{}, bf_nums{}, handle_keys{}, used_num(0)
    {}

    bool Check(uint64_t freq, int bf_num);

    void Insert(char *key, uint64_t freq, int bf_num) {
        memcpy(handle_keys[used_num], key, 12);
        freqs[used_num] = freq;
        bf_nums[used_num] = bf_num;
        used_num++;
    }

    static const int MaxLength = 32;
    uint64_t freqs[MaxLength];
    int bf_nums[MaxLength];
    char handle_keys[MaxLength][12];
    int used_num;
};
} // namespace leveldb

#endif