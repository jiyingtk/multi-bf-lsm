#include "leveldb/hotness.h"

namespace leveldb {
bool HotnessInfos::Check(uint64_t freq, int bf_num) {
  int i;
  for (i = 0; i < used_num; i++) {
    // if (freqs[i] < freq) {
    //     std::cout << "[hotness check]: failed in frequence, i " << i << ",
    //     freq " << freqs[i] << ", cur_freq " << freq << std::endl; return
    //     false;
    // }
    if (bf_nums[i] < bf_num) {
      // std::cout << "[hotness check]: failed in bf_num, i " << i << ", bf_num
      // " << bf_nums[i] << ", used_num" << used_num << ", cur_bf_num " <<
      // bf_num << std::endl;
      return false;
    }
  }
  return true;
}
}  // namespace leveldb
