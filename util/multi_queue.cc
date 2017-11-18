// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <math.h>  
#include <iostream>
#include "leveldb/cache.h"
#include "port/port.h"
#include "util/hash.h"
#include "util/mutexlock.h"
#include "db/table_cache.h"
using namespace std;
namespace leveldb {

namespace{
    struct LRUQueueCache;
    struct LRUQueueHandle {
    void* value;
    void (*deleter)(const Slice&, void* value);
    LRUQueueHandle* next_hash;
    LRUQueueHandle* next;
    LRUQueueHandle* prev;
    size_t charge;      // TODO(opt): Only allow uint32_t?
    size_t key_length;
    bool in_cache;      // Whether entry is in the cache.
    uint32_t refs;      // References, including cache reference, if present.
    uint32_t hash;      // Hash of key(); used for fast sharding and comparisons
    uint64_t fre_count;   //frequency count 
    uint64_t expire_time; //expire_time = current_time_ + life_time_
    uint16_t queue_id;   // queue id
    bool type;			//"true” represents  tableandfile
    char key_data[1];   // Beginning of key

    Slice key() const {
      // For cheaper lookups, we allow a temporary Handle object
      // to store a pointer to a key in "value".
      if (next == this) {
	return *(reinterpret_cast<Slice*>(value));
      } else {
	return Slice(key_data, key_length);
      }
    }
};

class HandleTable {   // a list store LRUQueueHandle's address , don't care queue id
 public:
  HandleTable() : length_(0), elems_(0), list_(NULL) { Resize(); }
  ~HandleTable() { delete[] list_; }

  LRUQueueHandle* Lookup(const Slice& key, uint32_t hash) {
    return *FindPointer(key, hash);
  }

  LRUQueueHandle* Insert(LRUQueueHandle* h) {
    LRUQueueHandle** ptr = FindPointer(h->key(), h->hash);
    LRUQueueHandle* old = *ptr;
    h->next_hash = (old == NULL ? NULL : old->next_hash);
    *ptr = h;
    if (old == NULL) {
      ++elems_;
      if (elems_ > length_) {
        // Since each cache entry is fairly large, we aim for a small
        // average linked list length (<= 1).
        Resize();
      }
    }
    return old;
  }

  LRUQueueHandle* Remove(const Slice& key, uint32_t hash) {
    LRUQueueHandle** ptr = FindPointer(key, hash);
    LRUQueueHandle* result = *ptr;
    if (result != NULL) {
      *ptr = result->next_hash;
      --elems_;
    }
    return result;
  }

 private:
  // The table consists of an array of buckets where each bucket is
  // a linked list of cache entries that hash into the bucket.
  uint32_t length_;
  uint32_t elems_;
  LRUQueueHandle** list_;

  // Return a pointer to slot that points to a cache entry that
  // matches key/hash.  If there is no such cache entry, return a
  // pointer to the trailing slot in the corresponding linked list.
  LRUQueueHandle** FindPointer(const Slice& key, uint32_t hash) {
    LRUQueueHandle** ptr = &list_[hash & (length_ - 1)];
    while (*ptr != NULL &&
           ((*ptr)->hash != hash || key != (*ptr)->key())) {
      ptr = &(*ptr)->next_hash;
    }
    return ptr;
  }

  void Resize() {
    uint32_t new_length = 4;
    while (new_length < elems_) {
      new_length *= 2;
    }
    LRUQueueHandle** new_list = new LRUQueueHandle*[new_length];
    memset(new_list, 0, sizeof(new_list[0]) * new_length);
    uint32_t count = 0;
    for (uint32_t i = 0; i < length_; i++) {
      LRUQueueHandle* h = list_[i];
      while (h != NULL) {
        LRUQueueHandle* next = h->next_hash;
        uint32_t hash = h->hash;
        LRUQueueHandle** ptr = &new_list[hash & (new_length - 1)];
        h->next_hash = *ptr;
        *ptr = h;
        h = next;
        count++;
      }
    }
    assert(elems_ == count);
    delete[] list_;
    list_ = new_list;
    length_ = new_length;
  }  
};

// class LRUQueueCache {
//  public:
//   LRUQueueCache();
//   ~LRUQueueCache();

//   // Separate from constructor so caller can easily make an array of LRUCache
//   void SetCapacity(size_t capacity) { capacity_ = capacity; }

//   // Like Cache methods, but with an extra "hash" parameter.
//   Cache::Handle* Insert(const Slice& key, uint32_t hash,
//                         void* value, size_t charge,
//                         void (*deleter)(const Slice& key, void* value));
//   Cache::Handle* Lookup(const Slice& key, uint32_t hash);
//   void Release(Cache::Handle* handle);
//   void Erase(const Slice& key, uint32_t hash);
//   void Prune();
//   size_t TotalCharge() const {
//     MutexLock l(&mutex_);
//     return usage_;
//   }
//   friend class LRUQueueCache;
//  private:
  
//   bool FinishErase(LRUQueueHandle* e);

//   // Initialized before use.
//   size_t capacity_;

//   // mutex_ protects the following state.
//   mutable port::Mutex mutex_;
//   size_t usage_;

//   // Dummy head of LRU list.
//   // lru.prev is newest entry, lru.next is oldest entry.
//   // Entries have refs==1 and in_cache==true.
//   LRUQueueHandle lru_;

//   // Dummy head of in-use list.
//   // Entries are in use by clients, and have refs >= 2 and in_cache==true.

  
// };

// LRUQueueCache::LRUQueueCache()
//     : usage_(0) {
//   // Make empty circular linked lists.
//   lru_.next = &lru_;
//   lru_.prev = &lru_;
// }

// LRUQueueCache::~LRUQueueCache() { // TODO : deconstructor in MultiQueue
//  // assert(in_use_.next == &in_use_);  // Error if caller has an unreleased handle //TODO: MultiQueue
//   for (LRUQueueHandle* e = lru_.next; e != &lru_; ) {
//     LRUQueueHandle* next = e->next;
//     assert(e->in_cache);
//     e->in_cache = false;
//     assert(e->refs == 1);  // Invariant of lru_ list.
//     Unref(e);
//     e = next;
//   }
// }


class MultiQueue:public Cache{
    int lrus_num_;
    size_t *charges_;
    std::atomic<uint64_t> last_id_;
    mutable leveldb::SpinMutex mutex_;  //for hashtable ,usage_ and e
    // Dummy head of in-use list.
    LRUQueueHandle in_use_;
    //Dummy heads of LRU list.
    LRUQueueHandle *lrus_;
    std::vector<size_t> lru_lens_;
    atomic<size_t> sum_lru_len;
    HandleTable table_;
    size_t capacity_;
    std::atomic<size_t> usage_;
    std::atomic<bool> shrinking_;  //shrinking usage?
    uint64_t current_time_;
    uint64_t life_time_;  
    int base_num_ ;
    std::atomic<bool> shutting_down_;
    double slow_shrink_ratio;
    double quick_shrink_ratio;
    double force_shrink_ratio;
    const double log_base;
public:
    MultiQueue(size_t capacity,int lrus_num = 1,int base_num=64,uint64_t life_time=50,double fr=1.1,double qr=0.99,double sr=.95,int lg_b=3);
    ~MultiQueue();
    void Ref(LRUQueueHandle *e,bool addFreCount=false);
    void Unref(LRUQueueHandle* e) ;
    void LRU_Remove(LRUQueueHandle* e);
    void LRU_Append(LRUQueueHandle* list, LRUQueueHandle* e);
    Cache::Handle* Lookup(const Slice& key, uint32_t hash,bool Get);
    Cache::Handle* Lookup(const Slice& key);
    Cache::Handle* Lookup(const Slice& key,bool Get);
    uint64_t LookupFreCount(const Slice &key);
    void SetFreCount(const Slice &key,uint64_t freCount);
    void Release(Cache::Handle* handle);
    virtual Handle* Insert(const Slice& key, void* value, size_t charge,
                         void (*deleter)(const Slice& key, void* value)) ;
    virtual Handle* Insert(const Slice& key, void* value, size_t charge,
                         void (*deleter)(const Slice& key, void* value),bool type) ;
    
    virtual void *Value(Handle *handle);
    virtual void Erase(const Slice &key);
    virtual uint64_t NewId();
    virtual size_t TotalCharge() const;
    //charge must >= 0
    Cache::Handle* Insert(const Slice& key, uint32_t hash, void* value, size_t charge,
      void (*deleter)(const Slice& key, void* value),bool type) ;  
      
     bool FinishErase(LRUQueueHandle* e);  
     void Erase(const Slice& key, uint32_t hash);
     void Prune(){} //do nothing
     bool ShrinkLRU(int k,int64_t remove_charge[],bool force=false);  
     int Queue_Num(uint64_t fre_count);
     uint64_t Num_Queue(int queue_id);
     std::string LRU_Status();
     void inline addCurrentTime(){
	 ++current_time_;
    }
     static inline uint32_t HashSlice(const Slice& s) {
	    return Hash(s.data(), s.size(), 0);
     }
     static void BGShrinkUsage(void *mq);
     void MayBeShrinkUsage();
     void BackgroudShrinkUsage();
     void SlowShrinking();
     void QuickShrinking();
     void ForceShrinking();
     static uint64_t base_fre_counts[10];
      static Env* mq_env;
};

MultiQueue::MultiQueue(size_t capacity,int lrus_num,int base_num,uint64_t life_time,double fr,double qr,double sr,int lg_b):capacity_(capacity),lrus_num_(lrus_num),base_num_(base_num),life_time_(life_time),shrinking_(false)
,force_shrink_ratio(fr),quick_shrink_ratio(qr),slow_shrink_ratio(sr),sum_lru_len(0),log_base(log(lg_b))
{
    //TODO: declare outside  class  in_use and lrus parent must be Initialized,avoid lock crush
      uint64_t base_sum = lg_b;
      in_use_.next = &in_use_;
      in_use_.prev = &in_use_;
      in_use_.queue_id = lrus_num;
      lrus_ = new LRUQueueHandle[lrus_num];
      lru_lens_.resize(lrus_num);
      for(int i = 0 ; i  < lrus_num ; ++i){
	lrus_[i].next = &lrus_[i];
	lrus_[i].prev = &lrus_[i];
	lrus_[i].queue_id = i;
	lru_lens_[i] = 0;
	base_fre_counts[i] = base_sum + 1 ;
	base_sum = base_sum*lg_b;
      }
      base_fre_counts[0] = base_num / 2;
      current_time_ = 0;
      cout<<"Multi-Queue Capacity:"<<capacity_<<endl;
      for(int i = 0 ; i  < lrus_num ; ++i){
	cout<<"Base "<< i <<" fre count: "<<base_fre_counts[i]<<endl;
      }

}

std::string MultiQueue::LRU_Status()
{
    mutex_.lock();
    int count = 0;
    char buf[300];
    std::string value;
    for(int i = 0 ; i < lrus_num_ ;  i++){
	count = 0;
	for (LRUQueueHandle* e = lrus_[i].next; e != &lrus_[i]; ) {
	    count++;
	    LRUQueueHandle* next = e->next;
	    e = next;
	}
	snprintf(buf,sizeof(buf),"lru %d count %d lru_lens_count:%lu \n",i,count,lru_lens_[i]);
	value.append(buf);
    }
    mutex_.unlock();
    return value;
}

MultiQueue::~MultiQueue()
{
  assert(in_use_.next == &in_use_);  // Error if caller has an unreleased handle
  shutting_down_ = true;
  mutex_.lock();
  for(int i = 0 ; i < lrus_num_ ;  i++){
    for (LRUQueueHandle* e = lrus_[i].next; e != &lrus_[i]; ) {
	LRUQueueHandle* next = e->next;
	assert(e->in_cache);
	e->in_cache = false;
	assert(e->refs == 1);  // Invariant of lru_ list.
	Unref(e);
	e = next;
    }
  }
  mutex_.unlock();
  delete []lrus_;
}
#define ln4 1.38629436
#define ln3 1.09861229
int MultiQueue::Queue_Num(uint64_t fre_count)
{
    if(fre_count <= base_num_){
	return 0;
    }
    return std::max(1,std::min(lrus_num_-1,static_cast<int>(log(fre_count)/log_base) - 1));
}


inline uint64_t MultiQueue::Num_Queue(int queue_id)
{
    return base_fre_counts[queue_id];
}

void MultiQueue::Ref(LRUQueueHandle* e,bool addFreCount)
{
     //mutex_.assert_held();
      if (e->refs == 1 && e->in_cache) {  // If on lru_ list, move to in_use_ list.
	  LRU_Remove(e);
	  LRU_Append(&in_use_, e);
	  --lru_lens_[e->queue_id];
	  --sum_lru_len;
	}
	e->refs++;
	if(addFreCount){
	    if(e->expire_time > current_time_ ){ //not expired
		++e->fre_count;
	    }else if(e->expire_time < current_time_){   //expired
		e->fre_count = Num_Queue(e->queue_id);
	    }
	}
	e->expire_time = current_time_ + life_time_;
}

void MultiQueue::Unref(LRUQueueHandle* e)
{
	//mutex_.assert_held();
	 assert(e->refs > 0);
	 e->refs--;
	 if (e->refs == 0) { // Deallocate.
		assert(!e->in_cache);
		(*e->deleter)(e->key(), e->value);
		free(e);
	  } else if (e->in_cache && e->refs == 1) {  // note:No longer in use; move to lru_ list.
		LRU_Remove(e);
		int qn = Queue_Num(e->fre_count);
		if(qn != e->queue_id && e->type){ 
		    leveldb::TableAndFile *tf = reinterpret_cast<leveldb::TableAndFile *>(e->value);
		    mutex_.unlock();
		    int64_t delta_charge = tf->table->AdjustFilters(qn+1);  // not in lru list, so need to worry will be catched by ShrinkUsage
		    mutex_.lock();
		    e->charge += delta_charge;
		    usage_ += delta_charge;
		    MayBeShrinkUsage();   
		 }
		LRU_Append(&lrus_[qn], e);   
		++lru_lens_[qn];
		++sum_lru_len;
	  }
	  mutex_.unlock();

}

void MultiQueue::LRU_Remove(LRUQueueHandle* e)
{
	  e->next->prev = e->prev;
	  e->prev->next = e->next;
}

void MultiQueue::LRU_Append(LRUQueueHandle* list, LRUQueueHandle* e)
{
	  // Make "e" newest entry by inserting just before *list
	   e->next = list;
	   e->prev = list->prev;
	   e->prev->next = e;
	   e->next->prev = e;
	   //if append to in_use , no need to remember queue_id,thus in_use_mutex used independently
	   e->queue_id = list->queue_id == lrus_num_ ? e->queue_id : list->queue_id; 
}

Cache::Handle* MultiQueue::Lookup(const Slice& key, uint32_t hash,bool Get)
{
   mutex_.lock();
   LRUQueueHandle* e = table_.Lookup(key, hash);
   if (e != NULL) {
      if(e->in_cache && e->refs == 1){
	Ref(e,Get);
      }else{
	Ref(e,Get); //on in-use list or not in cache in the short time 
      }
    }
    mutex_.unlock();
    return reinterpret_cast<Cache::Handle*>(e);
}

uint64_t MultiQueue::LookupFreCount(const Slice& key)
{
    const uint32_t hash = HashSlice(key);
    mutex_.lock();
    LRUQueueHandle* e = table_.Lookup(key, hash);
    if (e != NULL) {
	return e->fre_count;
    }
    mutex_.unlock();
    return 0;
}

void MultiQueue::SetFreCount(const Slice &key,uint64_t freCount){
     const uint32_t hash = HashSlice(key);
     mutex_.lock();
     LRUQueueHandle* e = table_.Lookup(key, hash);  
     if (e != NULL) {
	e->fre_count = freCount;
     }
     mutex_.unlock();
 
}

Cache::Handle *MultiQueue::Lookup(const Slice& key,bool Get){
      const uint32_t hash = HashSlice(key);
       return  Lookup(key, hash,Get);
}

Cache::Handle *MultiQueue::Lookup(const Slice& key){
      const uint32_t hash = HashSlice(key);
      return  Lookup(key, hash,false);
}
void MultiQueue::Release(Cache::Handle* handle) {
  auto lru_queue_handle = reinterpret_cast<LRUQueueHandle*>(handle);
  mutex_.lock();
  Unref(lru_queue_handle);
}

void MultiQueue::MayBeShrinkUsage(){
    //mutex_.assertheld
    if (shrinking_) {
	// Already scheduled
    } else if (shutting_down_) {
	// DB is being deleted; no more background compactions
    } else if(usage_ > capacity_*slow_shrink_ratio) {
	shrinking_ = true;
	mq_env->Schedule(&MultiQueue::BGShrinkUsage, this);
	mutex_.unlock();
	while(usage_ > capacity_*force_shrink_ratio);
	mutex_.lock();
    }
}

void MultiQueue::BGShrinkUsage(void *mq){
     reinterpret_cast<MultiQueue*>(mq)->BackgroudShrinkUsage();
}

inline bool MultiQueue::ShrinkLRU(int k,int64_t remove_charge[],bool force)
{
    //mutex_.assertHeld
    int64_t removed_usage = 0;
    if(k == 0){
	  while(lrus_[0].next != &lrus_[0] && removed_usage < remove_charge[0]){
		LRUQueueHandle *old = lrus_[0].next;
		if(force||old->expire_time < current_time_){
		    auto old_handle = table_.Remove(old->key(), old->hash);
		    uint64_t start_micros = Env::Default()->NowMicros();
		    removed_usage += old_handle->charge;
		    bool erased = FinishErase(old_handle);
		    if(force){
			MeasureTime(Statistics::GetStatistics().get(),Tickers::REMOVE_HEAD_FILTER_TIME_0,Env::Default()->NowMicros() - start_micros);
		    }else{
			MeasureTime(Statistics::GetStatistics().get(),Tickers::REMOVE_EXPIRED_FILTER_TIME_0,Env::Default()->NowMicros() - start_micros);
		    }
		    if (!erased) {  // to avoid unused variable when compiled NDEBUG
			assert(erased);
		    }
		}else{
		    return false;
		}
	    }
	    return true;
    }else{
	while (usage_ > capacity_ && k < lrus_num_) {
	    while(lrus_[k].next != &lrus_[k]  && removed_usage < remove_charge[k]){
		LRUQueueHandle* old = lrus_[k].next;
		assert(old->refs == 1);
		//TODO: old->fre_count = queue_id
		if(force||old->expire_time < current_time_){
		    if(old->type){
			leveldb::TableAndFile *tf = reinterpret_cast<leveldb::TableAndFile *>(old->value);
			uint64_t start_micros = Env::Default()->NowMicros();
			size_t delta_charge = tf->table->RemoveFilters(1);
			if(force){
			    MeasureTime(Statistics::GetStatistics().get(),Tickers::REMOVE_HEAD_FILTER_TIME_0+k,Env::Default()->NowMicros() - start_micros);
			}else{
			    MeasureTime(Statistics::GetStatistics().get(),Tickers::REMOVE_EXPIRED_FILTER_TIME_0+k,Env::Default()->NowMicros() - start_micros);
			}
			old->charge -= delta_charge;
			usage_ -= delta_charge;
			removed_usage += delta_charge;
			old->fre_count = Num_Queue(k-1);   // also decrease fre count
		    }
		    --lru_lens_[k];
		    LRU_Remove(old);
		    ++lru_lens_[k-1];
		    LRU_Append(&lrus_[k-1],old);	
		}else{
		    break;
		}
	    }
	    if(removed_usage >= remove_charge[k] ){
		remove_charge[k] = remove_charge[k]*1.05;
	    }
	    removed_usage = 0;
	    k++;
	}
    }
    return false;
}


void MultiQueue::SlowShrinking(){
    int64_t remove_charge = (usage_ - capacity_*slow_shrink_ratio);
    const int interval = 8;
    if(remove_charge < 0){
      return ;
    }
    remove_charge = (remove_charge + 7) / 8;
    for(int i = 0 ; i < interval  && usage_ > capacity_*slow_shrink_ratio ; i++){
	    mutex_.lock();
	    if(!ShrinkLRU(0,&remove_charge)){
		mutex_.unlock();
		break;
	    }
	    mutex_.unlock();
	    remove_charge = remove_charge*1.05;
	    for (uint32_t tries = 0; tries < 200; ++tries);
    }
    
}

void MultiQueue::QuickShrinking()
{
	int64_t remove_charges[8];
	int64_t overflow_charge = usage_ - capacity_*quick_shrink_ratio;
	int interval = 8;
	if(overflow_charge < 0){
	  return;
	}
	bool shrinkLRU0_flag = true;
	for(int i = 0 ; i < lrus_num_ ; i++){
	    remove_charges[i] = overflow_charge * (lru_lens_[i]*1.0/sum_lru_len);
	}
	for(int i = 0 ; i < 1  && usage_ > capacity_*quick_shrink_ratio ; i++){
	    mutex_.lock();
	    if(shrinkLRU0_flag){
		shrinkLRU0_flag = ShrinkLRU(0,&overflow_charge);
	    }
	    //	    ShrinkLRU(1,remove_charges);
	    mutex_.unlock();
	    if(shrinkLRU0_flag){
		remove_charges[0] = remove_charges[0]*1.05;
	    }
	    for (uint32_t tries = 0; tries < 100; ++tries);
	}
}

void MultiQueue::ForceShrinking()
{
	int64_t remove_charges[8];
	int64_t overflow_charge = usage_ - capacity_;
	for(int i = 0 ; i < lrus_num_ ; i++){
	    remove_charges[i] = overflow_charge * (lru_lens_[i]*1.0/sum_lru_len);
	}
	while(usage_ > capacity_ * force_shrink_ratio){
	     mutex_.lock();
	     ShrinkLRU(0,remove_charges,true);
	     ShrinkLRU(1,remove_charges,true);
	     mutex_.unlock();
	}
}

void MultiQueue::BackgroudShrinkUsage()
{
    if(shutting_down_ || usage_ < capacity_ * slow_shrink_ratio){
	return;
    }
    uint64_t start_micros = Env::Default()->NowMicros();
    if(usage_ > capacity_*force_shrink_ratio){
	ForceShrinking();
	MeasureTime(Statistics::GetStatistics().get(),Tickers::FORCE_SHRINKING,Env::Default()->NowMicros() - start_micros);
    }else if(usage_ < capacity_*quick_shrink_ratio){
	SlowShrinking();
	MeasureTime(Statistics::GetStatistics().get(),Tickers::SLOW_SHRINKING,Env::Default()->NowMicros() - start_micros);
    }else{
	SlowShrinking();  //also call SlowShrinking
	//QuickShrinking();
	MeasureTime(Statistics::GetStatistics().get(),Tickers::QUICK_SHRINKING,Env::Default()->NowMicros() - start_micros);
    }
    shrinking_ = false;
}

Cache::Handle* MultiQueue::Insert(const Slice& key, void* value, size_t charge, void (*deleter)(const Slice& key, void* value)){
    const uint32_t hash = HashSlice(key);
    return Insert(key,hash,value,charge,deleter,false);
}

Cache::Handle* MultiQueue::Insert(const Slice& key, void* value, size_t charge, void (*deleter)(const Slice& key, void* value), bool type)
{
    const uint32_t hash = HashSlice(key);
    return Insert(key, hash, value,charge, deleter, type);
}


Cache::Handle* MultiQueue::Insert(const Slice& key, uint32_t hash, void* value, size_t charge, void (*deleter)(const Slice& key, void* value),bool type)
{
  //MutexLock l(&mutex_);

  LRUQueueHandle* e = reinterpret_cast<LRUQueueHandle*>(
      malloc(sizeof(LRUQueueHandle)-1 + key.size()));
  e->value = value;
  e->deleter = deleter;
  e->charge = charge;
  e->key_length = key.size();
  e->hash = hash;
  e->in_cache = false;
  e->refs = 1;  // for the returned handle.
  e->type = type;
  e->fre_count = 0;
  e->queue_id = 0;   //new entry always have 1 filter and in lru list 0
  e->expire_time = current_time_ + life_time_;
  memcpy(e->key_data, key.data(), key.size());
  
  if (capacity_ > 0) {
    e->refs++;  // for the cache's reference.
    e->fre_count++; //for the first access
    e->in_cache = true;
    mutex_.lock();
    LRU_Append(&in_use_, e);

    usage_ += charge;
    auto redun_handle =  table_.Insert(e);
    if(redun_handle != NULL){
      FinishErase(redun_handle);
    }
    mutex_.unlock();
    
  } // else don't cache.  (Tests use capacity_==0 to turn off caching.)
  mutex_.lock();
  MayBeShrinkUsage();
  mutex_.unlock();
  return reinterpret_cast<Cache::Handle*>(e);
}

bool MultiQueue::FinishErase(LRUQueueHandle* e) {
  if (e != NULL) {
    assert(e->in_cache);
    LRU_Remove(e);
    if(e->refs == 1){    //means remove from LRU
	--lru_lens_[e->queue_id];
	--sum_lru_len;
    }
    e->in_cache = false;
    usage_ -= e->charge;
    Unref(e);
  }
  return e != NULL;
}

void MultiQueue::Erase(const Slice& key, uint32_t hash)
{
    mutex_.lock();
    auto obsolete_handle = table_.Remove(key,hash);
    if(obsolete_handle != NULL){
      FinishErase(obsolete_handle);
    }
    mutex_.unlock();
   
}

void MultiQueue::Erase(const Slice& key)
{
     const uint32_t hash = HashSlice(key);
     Erase(key,hash);
}

uint64_t MultiQueue::NewId()
{
    return ++(last_id_);
}

size_t MultiQueue::TotalCharge() const
{
    return usage_;
}

void* MultiQueue::Value(Cache::Handle* handle)
{
    return reinterpret_cast<LRUQueueHandle*>(handle)->value;
}



};
Env* MultiQueue::mq_env;
uint64_t MultiQueue::base_fre_counts[10]={4,10,28,82,243,730};


Cache* NewMultiQueue(size_t capacity,int lrus_num,int base_num,uint64_t life_time,double force_shrink_ratio,double quick_shrink_ratio,double slow_shrink_ratio,int lg_b){
    MultiQueue::mq_env = newEnv();
    return new MultiQueue(capacity,lrus_num,base_num,life_time,force_shrink_ratio,quick_shrink_ratio,slow_shrink_ratio,lg_b);
}

};

