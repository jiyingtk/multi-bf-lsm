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
    mutable leveldb::SpinMutex mutex_;  //for whole MultiQueue e.g. Lookup
    mutable leveldb::SpinMutex in_use_mutex_; // in_use_'s mutex;
    mutable leveldb::SpinMutex *lru_mutexs_;  //  [0,lru_num-1] for lrus_
    // Dummy head of in-use list.
    LRUQueueHandle in_use_;
    //Dummy heads of LRU list.
    LRUQueueHandle *lrus_;
    HandleTable table_;
    size_t capacity_;
    size_t usage_;
    uint64_t current_time_;
    uint64_t life_time_;  
    int base_num_ ;
public:
    MultiQueue(size_t capacity,int lrus_num = 1,int base_num=64,uint64_t life_time=50);
    ~MultiQueue();
    void Ref(LRUQueueHandle *e,bool addFreCount=false);
    void Unref(LRUQueueHandle* e) ;
    void LRU_Remove(LRUQueueHandle* e);
    void LRU_Append(LRUQueueHandle* list, LRUQueueHandle* e);
    Cache::Handle* Lookup(const Slice& key, uint32_t hash,bool Get);
    Cache::Handle* Lookup(const Slice& key);
    Cache::Handle* Lookup(const Slice& key,bool Get);
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
     void ShrinkUsage();
     int Queue_Num(uint64_t fre_count);
     uint64_t Num_Queue(int queue_id);
     std::string LRU_Status();
     static inline uint32_t HashSlice(const Slice& s) {
	    return Hash(s.data(), s.size(), 0);
     }
};

MultiQueue::MultiQueue(size_t capacity,int lrus_num,int base_num,uint64_t life_time):capacity_(capacity),lrus_num_(lrus_num),base_num_(base_num),life_time_(life_time)
{
    //TODO: declare outside  class  in_use and lrus parent must be Initialized,avoid lock crush
      in_use_.next = &in_use_;
      in_use_.prev = &in_use_;
      in_use_.queue_id = lrus_num;
      lrus_ = new LRUQueueHandle[lrus_num];
      lru_mutexs_ = new leveldb::SpinMutex[lrus_num];
      for(int i = 0 ; i  < lrus_num ; ++i){
	lrus_[i].next = &lrus_[i];
	lrus_[i].prev = &lrus_[i];
	lrus_[i].queue_id = i;
      }
      current_time_ = 0;
      cout<<"Multi-Queue Capacity:"<<capacity_<<endl;
}

std::string MultiQueue::LRU_Status()
{
    mutex_.lock();
    int count = 0;
    char buf[300];
    std::string value;
    for(int i = 0 ; i < lrus_num_ ;  i++){
	count = 0;
	lru_mutexs_[i].lock();
	for (LRUQueueHandle* e = lrus_[i].next; e != &lrus_[i]; ) {
	    count++;
	    LRUQueueHandle* next = e->next;
	    e = next;
	}
	lru_mutexs_[i].unlock();
	snprintf(buf,sizeof(buf),"lru %d count %d \n",i,count);
	value.append(buf);
    }
    mutex_.unlock();
    return value;
}

MultiQueue::~MultiQueue()
{
  assert(in_use_.next == &in_use_);  // Error if caller has an unreleased handle
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
  delete []lrus_;
  delete []lru_mutexs_;
}
#define ln4 1.38629436

int MultiQueue::Queue_Num(uint64_t fre_count)
{
    if(fre_count <= base_num_){
	return 0;
    }
    return std::min(lrus_num_-1,static_cast<int>(log(fre_count)/1.38629436) - 1);
}


uint64_t MultiQueue::Num_Queue(int queue_id)
{
    if(queue_id == 0){
	return 0;
    }
    return 2 << queue_id + base_num_;
}

void MultiQueue::Ref(LRUQueueHandle* e,bool addFreCount)
{
      if (e->refs == 1 && e->in_cache) {  // If on lru_ list, move to in_use_ list.
	  LRU_Remove(e);
	  in_use_mutex_.lock();
	  LRU_Append(&in_use_, e);
	  in_use_mutex_.unlock();
	}
	e->refs++;
	if(addFreCount){
	    ++e->fre_count;
	    ++current_time_;
	    e->expire_time = current_time_ + life_time_;
	}
}

void MultiQueue::Unref(LRUQueueHandle* e)
{
	 assert(e->refs > 0);
	 e->refs--;
	 if (e->refs == 0) { // Deallocate.
	    assert(!e->in_cache);
	    (*e->deleter)(e->key(), e->value);
	    free(e);
	  } else if (e->in_cache && e->refs == 1) {  // note:No longer in use; move to lru_ list.
	    LRU_Remove(e);
	    //TODO: mutex   note: avoid deadlock with origin list mutex
	    int qn = Queue_Num(e->fre_count);
	    if(qn != e->queue_id && e->type){
	      leveldb::TableAndFile *tf = reinterpret_cast<leveldb::TableAndFile *>(e->value);
	      int64_t delta_charge = tf->table->AdjustFilters(qn+1);
	      e->charge += delta_charge;
	      usage_ += delta_charge;
	      ShrinkUsage();
	    }
	    SpinMutexLock l(lru_mutexs_+qn);
	    LRU_Append(&lrus_[qn], e);   
	    
	  }
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
   mutex_.unlock();
   if (e != NULL) {
      if(e->in_cache && e->refs == 1){
	SpinMutexLock l1(lru_mutexs_ + e->queue_id);
	Ref(e,Get);
      }else{
	in_use_mutex_.lock();
	Ref(e,Get);
	in_use_mutex_.unlock();
      }
    }
    return reinterpret_cast<Cache::Handle*>(e);
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
  SpinMutexLock(lru_mutexs_ + lru_queue_handle->queue_id);
  Unref(lru_queue_handle);
}

void MultiQueue::ShrinkUsage()
{
    int k = 1;
    if(usage_ > capacity_){
	if(lrus_[0].next != &lrus_[0]){
	    LRUQueueHandle *old = lrus_[0].next;
	    assert(old->refs == 1);
	    if(old->expire_time < current_time_){
		mutex_.lock();
		SpinMutexLock l0(lru_mutexs_);
		uint64_t start_micros = Env::Default()->NowMicros();
		bool erased = FinishErase(table_.Remove(old->key(), old->hash));
		MeasureTime(Statistics::GetStatistics().get(),Tickers::REMOVE_EXPIRED_FILTER_TIME_0,Env::Default()->NowMicros() - start_micros);
		mutex_.unlock();
		if (!erased) {  // to avoid unused variable when compiled NDEBUG
		    assert(erased);
		}
	    }
	}
    }

    while (usage_ > capacity_ && k < lrus_num_) {
	if(lrus_[k].next != &lrus_[k]){
	    LRUQueueHandle* old = lrus_[k].next;
	    assert(old->refs == 1);
	    //TODO: old->fre_count = queue_id
	    if(old->expire_time < current_time_){
		 if(old->type){
		    leveldb::TableAndFile *tf = reinterpret_cast<leveldb::TableAndFile *>(old->value);
		    uint64_t start_micros = Env::Default()->NowMicros();
		    size_t delta_charge = tf->table->RemoveFilters(1);
		    MeasureTime(Statistics::GetStatistics().get(),Tickers::REMOVE_EXPIRED_FILTER_TIME_0+k,Env::Default()->NowMicros() - start_micros);
		    old->charge -= delta_charge;
		    usage_ -= delta_charge;
		}
		SpinMutexLock lr(lru_mutexs_ + k);
		LRU_Remove(old);
		SpinMutexLock la(lru_mutexs_ + k-1);
		LRU_Append(&lrus_[k-1],old);
	    }
	}
	k++;
    }
    while (usage_ > capacity_){
	    for(k = lrus_num_ - 1 ; k > 0  && usage_ > capacity_; k--){
		if(lrus_[k].next != &lrus_[k]){
		    LRUQueueHandle* old = lrus_[k].next;
		    assert(old->refs == 1);
		    if(old->type){
			leveldb::TableAndFile *tf = reinterpret_cast<leveldb::TableAndFile *>(old->value);
			uint64_t start_micros = Env::Default()->NowMicros();
			size_t delta_charge = tf->table->RemoveFilters(1);
			MeasureTime(Statistics::GetStatistics().get(),Tickers::REMOVE_HEAD_FILTER_TIME_0+k,Env::Default()->NowMicros() - start_micros);
			old->charge -= delta_charge;
			usage_ -= delta_charge;
		    }
		    SpinMutexLock lr(lru_mutexs_ + k);
		    LRU_Remove(old);
		    SpinMutexLock la(lru_mutexs_ + k-1);
		    LRU_Append(&lrus_[k-1],old);
		}
	    }
	     if(usage_ > capacity_){
		if(lrus_[0].next != &lrus_[0]){
		    LRUQueueHandle *old = lrus_[0].next;
		    assert(old->refs == 1);
		    mutex_.lock();
		    SpinMutexLock lr(lru_mutexs_);
		    uint64_t start_micros = Env::Default()->NowMicros();
		    bool erased = FinishErase(table_.Remove(old->key(), old->hash));
		    MeasureTime(Statistics::GetStatistics().get(),Tickers::REMOVE_HEAD_FILTER_TIME_0,Env::Default()->NowMicros() - start_micros);
		    mutex_.unlock();
		    if (!erased) {  // to avoid unused variable when compiled NDEBUG
		      assert(erased);
		    }
		}
	     }
    }
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
    in_use_mutex_.lock();
    LRU_Append(&in_use_, e);
    in_use_mutex_.unlock();
    
    usage_ += charge;
    mutex_.lock();
    auto redun_handle =  table_.Insert(e);
    mutex_.unlock();
    if(redun_handle != NULL){
      SpinMutexLock l2(lru_mutexs_ + redun_handle->queue_id);
      FinishErase(redun_handle);
    }
  } // else don't cache.  (Tests use capacity_==0 to turn off caching.)
  
  ShrinkUsage();
  /*
  }*/
  //TODO:adjustUsage(); 
  // according to e->type do something in adjustUsage , if e is tableandfile , adjustfilter should return charge.

  return reinterpret_cast<Cache::Handle*>(e);
}

bool MultiQueue::FinishErase(LRUQueueHandle* e) {
  if (e != NULL) {
    assert(e->in_cache);
    LRU_Remove(e);
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
    mutex_.unlock();
    if(obsolete_handle != NULL){
      SpinMutexLock l1(lru_mutexs_ + obsolete_handle->queue_id);
      FinishErase(obsolete_handle);
    }
   
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

Cache* NewMultiQueue(size_t capacity,int lrus_num,int base_num,uint64_t life_time){
    return new MultiQueue(capacity,lrus_num,base_num,life_time);
}

};

