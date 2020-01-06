// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <iostream>
#include <queue>
#include <unordered_map>
#include <boost/iterator/iterator_concepts.hpp>
#include <thread>
#include <chrono>
#include <condition_variable>
#include "leveldb/cache.h"
#include "port/port.h"
#include "util/hash.h"
#include "util/coding.h"
#include "util/mutexlock.h"
#include "db/table_cache.h"
using namespace std;
#define handle_error_en(en, msg) \
  do { errno = en; perror(msg); exit(EXIT_FAILURE); } while (0)
  
bool multi_queue_init = true;

namespace leveldb
{
    namespace multiqueue_ns
    {
        struct LRUQueueHandle
        {
            void *value;
            void (*deleter)(const Slice &, void *value, const bool realDelete);
            LRUQueueHandle *next_hash;
            LRUQueueHandle *next;
            LRUQueueHandle *prev;
            size_t charge;      // TODO(opt): Only allow uint32_t?
            size_t key_length;
            bool in_cache;      // Whether entry is in the cache.
            uint32_t refs;      // References, including cache reference, if present.
            uint32_t hash;      // Hash of key(); used for fast sharding and comparisons
            uint64_t fre_count;   //frequency count
            uint64_t expire_time; //expire_time = current_time_ + life_time_
            uint16_t queue_id;   // queue id  start from 0  and 0 means 0 filter
            uint32_t value_id;
            uint32_t value_refs;
            bool is_mark;
            LRUQueueHandle *table_handle, *prev_region, *next_region;

            char key_data[1];   // Beginning of key

            Slice key() const
            {
                // For cheaper lookups, we allow a temporary Handle object
                // to store a pointer to a key in "value".
                if (next == this)
                {
                    return *(reinterpret_cast<Slice *>(value));
                }
                else
                {
                    return Slice(key_data, key_length);
                }
            }
        };

        class HandleTable     // a list store LRUQueueHandle's address , don't care queue id
        {
        public:
            HandleTable() : length_(0), elems_(0), list_(NULL)
            {
                Resize();
            }
            ~HandleTable()
            {
                delete[] list_;
            }

            LRUQueueHandle *Lookup(const Slice &key, uint32_t hash)
            {
                return *FindPointer(key, hash);
            }

            LRUQueueHandle *Insert(LRUQueueHandle *h)
            {
                LRUQueueHandle **ptr = FindPointer(h->key(), h->hash);
                LRUQueueHandle *old = *ptr;
                h->next_hash = (old == NULL ? NULL : old->next_hash);
                *ptr = h;
                if (old == NULL)
                {
                    ++elems_;
                    if (elems_ > length_)
                    {
                        // Since each cache entry is fairly large, we aim for a small
                        // average linked list length (<= 1).
                        Resize();
                    }
                }
                return old;
            }

            LRUQueueHandle *Remove(const Slice &key, uint32_t hash)
            {
                LRUQueueHandle **ptr = FindPointer(key, hash);
                LRUQueueHandle *result = *ptr;
                if (result != NULL)
                {
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
            LRUQueueHandle **list_;

            // Return a pointer to slot that points to a cache entry that
            // matches key/hash.  If there is no such cache entry, return a
            // pointer to the trailing slot in the corresponding linked list.
            LRUQueueHandle **FindPointer(const Slice &key, uint32_t hash)
            {
                LRUQueueHandle **ptr = &list_[hash & (length_ - 1)];
                while (*ptr != NULL &&
                        ((*ptr)->hash != hash || key != (*ptr)->key()))
                {
                    ptr = &(*ptr)->next_hash;
                }
                return ptr;
            }

            void Resize()
            {
                uint32_t new_length = 131072;
                while (new_length < elems_)
                {
                    new_length *= 2;
                }
                LRUQueueHandle **new_list = new LRUQueueHandle*[new_length];
                memset(new_list, 0, sizeof(new_list[0]) * new_length);
                uint32_t count = 0;
                for (uint32_t i = 0; i < length_; i++)
                {
                    LRUQueueHandle *h = list_[i];
                    while (h != NULL)
                    {
                        LRUQueueHandle *next = h->next_hash;
                        uint32_t hash = h->hash;
                        LRUQueueHandle **ptr = &new_list[hash & (new_length - 1)];
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

        struct HandlePair {
            uint32_t table_id;
            uint32_t region_id;
            uint64_t freq;
            uint32_t filter_num;
        };

        class MultiQueue: public Cache
        {
            const std::string dbname_;
            int insert_count;
            int lrus_num_;
            size_t *charges_;
            std::atomic<uint64_t> last_id_;
            mutable leveldb::SpinMutex mutex_;  //for hashtable ,usage_ and e
            // Dummy head of in-use list.
            LRUQueueHandle in_use_;
            //Dummy heads of LRU list.
            LRUQueueHandle *lrus_;
            std::vector<size_t> lru_lens_;
            std::vector<size_t> sum_freqs_;
            atomic<size_t> sum_lru_len;
            HandleTable table_;
            size_t capacity_;
            std::atomic<size_t> usage_;
            uint64_t current_time_;
            uint64_t life_time_;
            std::atomic<bool> shutting_down_;
            double change_ratio_;
            double expection_;
            std::vector<double> fps;
            std::vector<size_t> bits_per_key_per_filter_, bits_per_key_per_filter_sum;  //begin from 0 bits
            bool need_adjust;
            uint64_t dynamic_merge_counter[2];
            bool cache_use_real_size_;
            int counters[16];
            FILE * freq_info_diff_file;
            bool should_recovery_hotness_;
            std::unordered_map<uint64_t, HandlePair> freq_info_map;
        public:
            static pthread_t pids_[16];
            static int bg_thread_nums;
            queue<HandlePair> bg_queue;
            std::mutex queue_mutex;
            std::condition_variable_any queue_con;
            std::mutex backup_mutex;
            std::condition_variable backup_cv;

            MultiQueue(const std::string &dbname, size_t capacity, int lrus_num = 1, uint64_t life_time = 20000, double cr = 0.0001, bool cache_use_real_size = true, bool should_recovery_hotness = false);
            ~MultiQueue();
            void backup_node_hotness(LRUQueueHandle *e, bool shouldBackup = false);
            void backup_all();
            void restore_all();

            void Ref(LRUQueueHandle *e, bool addFreCount = false);
            void Unref(LRUQueueHandle *e) ;
            void LRU_Remove(LRUQueueHandle *e);
            void LRU_Append(LRUQueueHandle *list, LRUQueueHandle *e);
            virtual Cache::Handle *Lookup(const Slice &key) override;
            virtual Cache::Handle *Lookup(const Slice &key, bool Get) override;
            uint64_t LookupFreCount(const Slice &key);
            void SetFreCount(const Slice &key, uint64_t freCount);
            virtual int AllocFilterNums(int freq) override;

            void Release(Cache::Handle *handle);
            virtual Handle *Insert(const Slice &key, void *value, size_t charge,
                                   void (*deleter)(const Slice &key, void *value, const bool realDelete)) override;

            virtual void *Value(Handle *handle) override;
            virtual void Erase(const Slice &key) override;
            virtual uint64_t NewId() override;
            virtual bool IsCacheFull() const override ;
            virtual size_t TotalCharge() const override;

            bool FinishErase(LRUQueueHandle *e);
            void Erase(const Slice &key, uint32_t hash);
            void Prune() {} //do nothing
            bool ShrinkLRU(int k, int64_t remove_charge[], bool force = false);

            uint64_t Num_Queue(int queue_id, uint64_t fre_count);
            std::string LRU_Status();
            virtual void inline addCurrentTime() override
            {
                ++current_time_;
            }
            static inline uint32_t HashSlice(const Slice &s)
            {
                return Hash(s.data(), s.size(), 0);
            }
            void MayBeShrinkUsage();
            void RecomputeExp(LRUQueueHandle *e);
            double FalsePositive(LRUQueueHandle *e);
            static void * BackThreadAll(void *args);
            static void * BackThreadDiff(void *args);
        };
        pthread_t MultiQueue::pids_[16];
        int MultiQueue::bg_thread_nums = 1;


        MultiQueue::MultiQueue(const std::string &dbname, size_t capacity, int lrus_num, uint64_t life_time, double change_ratio, bool cache_use_real_size, bool should_recovery_hotness): dbname_(dbname), capacity_(capacity), lrus_num_(lrus_num), life_time_(life_time)
            , change_ratio_(change_ratio), sum_lru_len(0), expection_(0), usage_(0), shutting_down_(false), insert_count(0), need_adjust(true), cache_use_real_size_(cache_use_real_size), should_recovery_hotness_(should_recovery_hotness)
        {
            //TODO: declare outside  class  in_use and lrus parent must be Initialized,avoid lock crush
            in_use_.next = &in_use_;
            in_use_.prev = &in_use_;
            in_use_.queue_id = lrus_num; //lrus_num = filter_num + 1
            lrus_ = new LRUQueueHandle[lrus_num];
            lru_lens_.resize(lrus_num);
            sum_freqs_.resize(lrus_num);
            for(int i = 0 ; i  < lrus_num ; ++i)
            {
                lrus_[i].next = &lrus_[i];
                lrus_[i].prev = &lrus_[i];
                lrus_[i].queue_id = i;
                lru_lens_[i] = 0;
                sum_freqs_[i] = 0;
            }
            current_time_ = 0;
            cout << "Multi-Queue Capacity:" << capacity_ << endl;
            int sum_bits = 0;
            fps.push_back(1.0); // 0 filters
            bits_per_key_per_filter_.push_back(0);
            bits_per_key_per_filter_sum.push_back(0);
            for(int i = 1 ; i  < lrus_num ; ++i)
            {
                sum_bits += FilterPolicy::bits_per_key_per_filter_[i - 1];
                fps.push_back( pow(0.6185, sum_bits) );
                bits_per_key_per_filter_.push_back(FilterPolicy::bits_per_key_per_filter_[i - 1]);
                bits_per_key_per_filter_sum.push_back(sum_bits);
            }

            dynamic_merge_counter[0] = dynamic_merge_counter[1] = 0;

            for (int i = 0; i < 16; i++)counters[i] = 0;

            if (should_recovery_hotness_)
                restore_all();

            char name_buf[100];
            snprintf(name_buf, sizeof(name_buf), "/freq_info_diff");
            std::string freq_info_fn = dbname_ + name_buf;
            freq_info_diff_file = fopen(freq_info_fn.data(), "wb");

            bg_thread_nums = 2;
            int i;
            for (i = 0; i < bg_thread_nums; i++) {
                if(i == 0 && pthread_create(&pids_[i], NULL, MultiQueue::BackThreadAll, this) != 0)
                {
                    perror("create thread ");
                }
                if(i == 1 && pthread_create(&pids_[i], NULL, MultiQueue::BackThreadDiff, this) != 0)
                {
                    perror("create thread ");
                }
                snprintf(name_buf, sizeof name_buf, "MultiQueue:freq_info%d", i);
                name_buf[sizeof name_buf - 1] = '\0';
                pthread_setname_np(pids_[i], name_buf);
            }

        }

        MultiQueue::~MultiQueue()
        {
            assert(in_use_.next == &in_use_);  // Error if caller has an unreleased handle
            shutting_down_ = true;
            queue_con.notify_all();
            for (int i = 0; i < bg_thread_nums; i++) {
                pthread_join(pids_[i], NULL);
            }
            
            mutex_.lock();
            fprintf(stderr, "optimized expection_ is %lf\n", expection_);
            for(int i = 0 ; i < lrus_num_ ;  i++)
            {
                for (LRUQueueHandle *e = lrus_[i].next; e != &lrus_[i]; )
                {
                    LRUQueueHandle *next = e->next;
                    assert(e->in_cache);
                    e->in_cache = false;
                    assert(e->refs == 1);  // Invariant of lru_ list.
                    Unref(e);
                    e = next;
                }
            }
            double avg_merge_region_nums = dynamic_merge_counter[0] == 0 ? 0 : dynamic_merge_counter[1] / dynamic_merge_counter[0];
            fprintf(stderr, "multi_queue_init is %s, expection_ is %lf, avg_merge_region_nums is %lf\n", multi_queue_init ? "true" : "false", expection_, avg_merge_region_nums);
            for (int i = 0; i < 16; i++)
                fprintf(stderr, "counters[%d] = %d\n", i, counters[i]);
            mutex_.unlock();
            delete []lrus_;
        }

        inline void save_freq_info(FILE* fp, HandlePair *hp) {
            //fixed encoding
            // fwrite(hp, sizeof(*hp), 1, fp);

            //varied encoding
            std::string s;
            char tmp[1];
            s.append(tmp, 1);
            PutVarint32(&s, hp->table_id);
            PutVarint32(&s, hp->region_id);
            PutVarint64(&s, hp->freq);
            PutVarint32(&s, hp->filter_num);
            int count = s.length() - 1;
            s[0] = count;
            fwrite(s.data(), s.length(), 1, fp);
        }

        inline bool restore_freq_info(FILE* fp, std::unordered_map<uint64_t, HandlePair>& map) {
            HandlePair hp;

            //fixed decoding
            // if (fread(&hp, sizeof(hp), 1, fp) != 1)
            //     return true;

            //varied decoding
            char buf[32];
            if (fread(buf, 1, 1, fp) != 1)
                return true;
            int read_count = buf[0];
            if (fread(buf, 1, read_count, fp) != read_count)
                return true;

            const char *p = buf, *limit = buf + 32;
            p = GetVarint32Ptr(p, limit, &hp.table_id);
            p = GetVarint32Ptr(p, limit, &hp.region_id);
            p = GetVarint64Ptr(p, limit, &hp.freq);
            uint32_t filter_num;
            p = GetVarint32Ptr(p, limit, &filter_num);
            hp.filter_num = filter_num;

            uint64_t key;
            char *key_ptr = (char*) &key;
            memcpy(key_ptr, &hp.table_id, sizeof(uint32_t));
            memcpy(key_ptr + sizeof(uint32_t), &hp.region_id, sizeof(uint32_t));
            map[key] = hp;
            return false;
        }

        void * MultiQueue::BackThreadAll(void *arg) {
            MultiQueue *mq = (MultiQueue *)arg;
            while(!mq->shutting_down_) {
                std::unique_lock<std::mutex> lk(mq->backup_mutex);
                std::chrono::seconds ten(10);
                mq->backup_cv.wait_for(lk, ten);
                
                if (mq->shutting_down_) {
                    break;
                }

                uint64_t start_micros = Env::Default()->NowMicros();
                mq->backup_all();
                MeasureTime(Statistics::GetStatistics().get(), Tickers::BACKUP_TIME, Env::Default()->NowMicros() - start_micros);
            }

            return NULL;
        }

        void * MultiQueue::BackThreadDiff(void *arg) {
            MultiQueue *mq = (MultiQueue *)arg;
            while(!mq->shutting_down_) {
                mq->queue_mutex.lock();
                while (!mq->shutting_down_ && mq->bg_queue.empty())
                    mq->queue_con.wait(mq->queue_mutex);

                if (mq->shutting_down_) {
                    mq->queue_mutex.unlock();
                    break;
                }

                HandlePair hp = mq->bg_queue.front();
                mq->bg_queue.pop();
                mq->queue_mutex.unlock();

                save_freq_info(mq->freq_info_diff_file, &hp);
            }

            return NULL;
        }

        void MultiQueue::backup_all() {
            queue<HandlePair> q;

            mutex_.lock();
            for(int i = 0 ; i < lrus_num_; i++)
            {
                for (LRUQueueHandle *e = lrus_[i].next; e != &lrus_[i]; )
                {
                    if (e->value_id != 0) {
                        leveldb::TableAndFile *tf = reinterpret_cast<leveldb::TableAndFile *>(e->value);
                        HandlePair hp = {(uint32_t) tf->file_number, e->value_id, e->fre_count, e->queue_id};
                        q.push(hp);
                    }

                    LRUQueueHandle *next = e->next;
                    e = next;
                }
            }
            mutex_.unlock();

            char name_buf[100];
            snprintf(name_buf, sizeof(name_buf), "/freq_info");
            std::string freq_info_fn = dbname_ + name_buf;
            FILE *fp = fopen(freq_info_fn.data(), "wb");

            while (!q.empty()) {
                HandlePair hp = q.front();
                q.pop();
                save_freq_info(fp, &hp);
            }

            fclose(fp);
        }

        void MultiQueue::restore_all() {
            fprintf(stderr, "restore hotness info of multiqueue\n");
            char name_buf[100];
            snprintf(name_buf, sizeof(name_buf), "/freq_info");
            std::string freq_info_fn = dbname_ + name_buf;

            FILE *fp = fopen(freq_info_fn.data(), "rb");
            while (!restore_freq_info(fp, freq_info_map));
            fclose(fp);
        }

        inline void MultiQueue::backup_node_hotness(LRUQueueHandle *e, bool shouldBackup) {
            if (!shouldBackup)
                return;
                
            leveldb::TableAndFile *tf = reinterpret_cast<leveldb::TableAndFile *>(e->value);

            HandlePair hp = {(uint32_t) tf->file_number, e->value_id, e->fre_count, e->queue_id};
            queue_mutex.lock();
            bg_queue.push(hp);
            queue_con.notify_all();
            queue_mutex.unlock();
        }

        std::string MultiQueue::LRU_Status()
        {
            mutex_.lock();
            int count = 0;
            char buf[1024];
            std::string value;
            for(int i = 0 ; i < lrus_num_ ;  i++)
            {
                count = 0;
                for (LRUQueueHandle *e = lrus_[i].next; e != &lrus_[i]; )
                {
                    count++;
                    LRUQueueHandle *next = e->next;
                    e = next;
                }
                snprintf(buf, sizeof(buf), "lru %d count %d lru_lens_count:%lu \n", i, count, lru_lens_[i]);
                value.append(buf);
            }
            snprintf(buf, sizeof(buf), "MQ state: capacity: %zu usage: %zu, expection: %lf, insert_count %d\n", capacity_, usage_.load(std::memory_order_relaxed), expection_, insert_count);
            value.append(buf);
            mutex_.unlock();
            return value;
        }

        inline uint64_t MultiQueue::Num_Queue(int queue_id, uint64_t fre_count)
        {
            if(&lrus_[queue_id] == lrus_[queue_id].next)
            {
                return fre_count >> 1;
            }
            else
            {
                LRUQueueHandle *lru_handle = lrus_[queue_id].next;
                LRUQueueHandle *mru_handle = lrus_[queue_id].prev;
                return (lru_handle->fre_count + mru_handle->fre_count) / 2;
            }
        }

        inline double MultiQueue::FalsePositive(LRUQueueHandle *e)
        {
            leveldb::TableAndFile *tf = reinterpret_cast<leveldb::TableAndFile *>(e->value);
            return fps[tf->table->getCurrFilterNum(e->value_id - 1)];
        }

        void MultiQueue::RecomputeExp(LRUQueueHandle *e)
        {
            if(multi_queue_init || (e->queue_id + 1) == lrus_num_)
            {
                ++e->fre_count;
                sum_freqs_[e->queue_id]++;
                expection_ += FalsePositive(e);
            }
            else
            {
                uint64_t start_micros = Env::Default()->NowMicros();
                double now_expection  = expection_ + FalsePositive(e) ;
                ++e->fre_count;
                double min_expection = now_expection, change_expection;
                const double new_expection = expection_ - (e->fre_count - 1) * FalsePositive(e) + e->fre_count * fps[e->queue_id + 1]; //TODO: OPTIMIZE
                int need_bits = usage_ - capacity_;// + bits_per_key_per_filter_[e->queue_id + 1];
                if (e->queue_id == 0) {
                    need_bits += lrus_[1].next->charge;
                }
                else {
                    need_bits += e->charge / bits_per_key_per_filter_sum[e->queue_id] * bits_per_key_per_filter_[e->queue_id + 1];
                }
                int remove_bits, min_i = -1 ;
                if(need_bits > 0)
                {
                    counters[2]++;
                    for(int i = 1 ; i < lrus_num_ ; i++)
                    {
                        remove_bits = 0;
                        change_expection =  new_expection;
                        LRUQueueHandle *old = lrus_[i].next;
                        while(old != &lrus_[i] && remove_bits < need_bits)
                        {
                            if(old->expire_time < current_time_)  // expired
                            {
                                // remove_bits += bits_per_key_per_filter_[i];
                                remove_bits += old->charge / bits_per_key_per_filter_sum[old->queue_id] * bits_per_key_per_filter_[i];
                                change_expection += (old->fre_count * fps[i - 1] - old->fre_count * FalsePositive(old));
                            }
                            else
                            {
                                break;
                            }
                            old = old->next;
                        }
                        if(remove_bits >= need_bits && change_expection < min_expection)
                        {
                            min_expection = change_expection;
                            min_i = i;
                        }
                    }
                    if (min_i != -1)
                        counters[10]++;
                    if(min_i != -1 && now_expection - min_expection > now_expection * change_ratio_)
                    {
                        counters[3]++;
                        assert(now_expection > min_expection);
                        remove_bits = 0;
                        while(lrus_[min_i].next != &lrus_[min_i] && remove_bits < need_bits)
                        {
                            LRUQueueHandle *old = lrus_[min_i].next;
                            leveldb::TableAndFile *tf = reinterpret_cast<leveldb::TableAndFile *>(old->value);
                            // remove_bits += bits_per_key_per_filter_[min_i];
                            remove_bits += old->charge / bits_per_key_per_filter_sum[old->queue_id] * bits_per_key_per_filter_[min_i];
                            size_t delta_charge = tf->table->RemoveFilters(1, old->value_id - 1);
                            usage_ -= delta_charge;
                            MeasureTime(Statistics::GetStatistics().get(), Tickers::REMOVE_EXPIRED_FILTER_TIME_0 + min_i, Env::Default()->NowMicros() - start_micros);
                            --lru_lens_[min_i];
                            sum_freqs_[min_i] -= old->fre_count;

                            LRU_Remove(old);
                            ++lru_lens_[min_i - 1];
                            sum_freqs_[min_i - 1] += old->fre_count;
                            LRU_Append(&lrus_[min_i - 1], old);

                            backup_node_hotness(old);
                        }

                        sum_freqs_[e->queue_id] -= e->fre_count;
                        sum_freqs_[e->queue_id + 1] += e->fre_count;
                        ++e->queue_id;
                        expection_ = min_expection;
                    }
                    else
                    {
                        counters[4]++;
                        expection_ = now_expection;
                    }
                }
                else
                {
                    counters[5]++;
                    if(now_expection - new_expection > now_expection * change_ratio_)
                    {
                        counters[6]++;

                        sum_freqs_[e->queue_id] -= e->fre_count;
                        sum_freqs_[e->queue_id + 1] += e->fre_count;
                        ++e->queue_id;
                        expection_ = new_expection;
                    }
                    else
                    {
                        counters[7]++;
                        expection_ = now_expection;
                    }
                }
            }
        }

        void MultiQueue::Ref(LRUQueueHandle *e, bool addFreCount)
        {
            if (e->refs == 1 && e->in_cache)    // If on lru_ list, move to in_use_ list.
            {
                LRU_Remove(e);
                LRU_Append(&in_use_, e);
                --lru_lens_[e->queue_id];
                --sum_lru_len;
            }
            e->refs++;

            counters[0]++;

            if(addFreCount)
            {
                leveldb::TableAndFile *tf = reinterpret_cast<leveldb::TableAndFile *>(e->value);
                tf->table->setAccess(e->value_id - 1);
        
                if(e->expire_time > current_time_ )  //not expired
                {
                    counters[1]++;
                    RecomputeExp(e);
                }
            }
            e->expire_time = current_time_ + life_time_;
        }

        void MultiQueue::Unref(LRUQueueHandle *e)
        {
            assert(e->refs > 0);
            e->refs--;
            if (e->refs == 0)   // Deallocate.
            {
deallocate:
                assert(!e->in_cache);
                expection_ -= e->fre_count * fps[e->queue_id];

                leveldb::TableAndFile *tf = reinterpret_cast<leveldb::TableAndFile *>(e->value);

                if (e->value_id == 0) {
                    Slice key(e->key_data, sizeof(uint64_t));
                    (*e->deleter)(key, e->value, true);
                }
                free(e);
            }
            else if (e->in_cache && e->refs == 1)      // note:No longer in use; move to lru_ list.
            {
                leveldb::TableAndFile *tf = reinterpret_cast<leveldb::TableAndFile *>(e->value);
                if(e->value_id > 0 && tf->table->getCurrFilterNum(e->value_id - 1) < e->queue_id)   //only add;
                {
                    ++e->refs;
                    size_t delta_charge = tf->table->AdjustFilters(e->queue_id, e->value_id - 1);  // not in lru list, so need to worry will be catched by ShrinkUsage
                    --e->refs;
                    if(e->refs == 0)
                    {
                        goto deallocate;
                    }
                    e->charge += delta_charge;
                    usage_ += delta_charge;

                    backup_node_hotness(e);

                    MayBeShrinkUsage();
                }
                else if (e->value_id > 0 && tf->table->getCurrFilterNum(e->value_id - 1) > e->queue_id) {
                    e->queue_id = tf->table->getCurrFilterNum(e->value_id - 1);
                    size_t new_charge;
                    if (cache_use_real_size_)
                        new_charge = tf->table->getCurrFiltersSize(e->value_id - 1);
                    else
                        new_charge = bits_per_key_per_filter_sum[e->queue_id];
                    usage_ -= e->charge;
                    e->charge = new_charge;
                    usage_ += e->charge;

                    backup_node_hotness(e);

                    MayBeShrinkUsage();
                }
                LRU_Remove(e);
                LRU_Append(&lrus_[e->queue_id], e);
                ++lru_lens_[e->queue_id];
                ++sum_lru_len;
            }
        }

        void MultiQueue::LRU_Remove(LRUQueueHandle *e)
        {
            e->next->prev = e->prev;
            e->prev->next = e->next;
        }

        void MultiQueue::LRU_Append(LRUQueueHandle *list, LRUQueueHandle *e)
        {
            // Make "e" newest entry by inserting just before *list
            e->next = list;
            e->prev = list->prev;
            e->prev->next = e;
            e->next->prev = e;
            //if append to in_use , no need to remember queue_id,thus in_use_mutex used independently
            e->queue_id = list->queue_id == lrus_num_ ? e->queue_id : list->queue_id;
        }

        uint64_t MultiQueue::LookupFreCount(const Slice &key)
        {
            const uint32_t hash = HashSlice(key);
            mutex_.lock();
            LRUQueueHandle *e = table_.Lookup(key, hash);
            if (e != NULL)
            {
                mutex_.unlock();
                return e->fre_count;
            }
            mutex_.unlock();
            return 0;
        }

        void MultiQueue::SetFreCount(const Slice &key, uint64_t freCount)
        {
            const uint32_t hash = HashSlice(key);
            mutex_.lock();
            LRUQueueHandle *e = table_.Lookup(key, hash);
            if (e != NULL)
            {
                expection_ -= e->fre_count * FalsePositive(e);
                e->fre_count = freCount;
                expection_ += e->fre_count * FalsePositive(e);
            }
            mutex_.unlock();

        }



        Cache::Handle *MultiQueue::Lookup(const Slice &key)
        {
            return Lookup(key, false);
        }

        Cache::Handle *MultiQueue::Lookup(const Slice &key, bool addFreq)
        {
            const uint32_t hash = HashSlice(key);
            mutex_.lock();
            LRUQueueHandle *e = table_.Lookup(key, hash);
            if (e != NULL)
            {
                if(e->in_cache && e->refs == 1)
                {
                    Ref(e, addFreq);
                }
                else
                {
                    Ref(e, addFreq); //on in-use list or not in cache in the short time
                }
            }
            mutex_.unlock();
            return reinterpret_cast<Cache::Handle *>(e);
        }

        void MultiQueue::Release(Cache::Handle *handle)
        {
            auto lru_queue_handle = reinterpret_cast<LRUQueueHandle *>(handle);
            mutex_.lock();
            Unref(lru_queue_handle);
            mutex_.unlock();
        }

        void MultiQueue::MayBeShrinkUsage()
        {
            if(usage_ >= capacity_)
            {
                multi_queue_init = false;
                int64_t overflow_charge = usage_ - capacity_;
                if(!ShrinkLRU(lrus_num_ - 1, &overflow_charge, false))
                {
                    overflow_charge = usage_ - capacity_;
                    ShrinkLRU(1, &overflow_charge, true);
                }
            }
        }

        inline bool MultiQueue::ShrinkLRU(int k, int64_t remove_charge[], bool force)
        {
            int64_t removed_usage = 0;
            if(!force)
            {
                counters[8]++;
                while (usage_ > capacity_ && k >= 1)
                {
                    while(lrus_[k].next != &lrus_[k]  && removed_usage < remove_charge[0])
                    {
                        LRUQueueHandle *old = lrus_[k].next;

                        if(old->refs == 1 && old->expire_time < current_time_)
                        {
                            leveldb::TableAndFile *tf = reinterpret_cast<leveldb::TableAndFile *>(old->value);
                            uint64_t start_micros = Env::Default()->NowMicros();
                            size_t delta_charge = tf->table->RemoveFilters(1, old->value_id - 1);
                            MeasureTime(Statistics::GetStatistics().get(), Tickers::REMOVE_EXPIRED_FILTER_TIME_0 + k, Env::Default()->NowMicros() - start_micros);
                            old->charge -= delta_charge;
                            usage_ -= delta_charge;
                            removed_usage += delta_charge;
                            expection_ -= old->fre_count * fps[k];
                            old->fre_count = Num_Queue(k - 1, old->fre_count); // also decrease fre count
                            expection_ += old->fre_count * fps[k - 1];
                            --lru_lens_[k];
                            sum_freqs_[k] -= old->fre_count;
                            LRU_Remove(old);
                            ++lru_lens_[k - 1];
                            sum_freqs_[k - 1] += old->fre_count;
                            LRU_Append(&lrus_[k - 1], old);

                            backup_node_hotness(old);
                        }
                        else
                        {
                            break;
                        }
                    }
                    k--;
                }
                if(removed_usage >= remove_charge[0])
                {
                    return true;
                }
                return false;
            }
            else
            {
                counters[9]++;
                size_t max_lru_lens = 0, max_i = -1;
                for(int i = 1 ; i < lrus_num_ ; i++)
                {
                    if(lru_lens_[i] > max_lru_lens)
                    {
                        max_lru_lens = lru_lens_[i];
                        max_i = i;
                    }
                }
                if(max_i != -1)
                {
                    k = max_i;
                    while(removed_usage < remove_charge[0])
                    {
                        uint64_t start_micros = Env::Default()->NowMicros();
                        LRUQueueHandle *old = lrus_[k].next;

                        if (old == &lrus_[k]) {
                            printf("error! max lru is empty!\n");
                            int i = 1;
                            while (i) {
                                int b = 0;
                                b += 1;
                            }
                        }
                        leveldb::TableAndFile *tf = reinterpret_cast<leveldb::TableAndFile *>(old->value);
                        size_t delta_charge = tf->table->RemoveFilters(1, old->value_id - 1); //  remove 1 filter
                        old->charge -= delta_charge;
                        usage_ -= delta_charge;
                        removed_usage += delta_charge;
                        expection_ -= old->fre_count * fps[k];
                        old->fre_count = Num_Queue(k - 1, old->fre_count); // also decrease fre count
                        expection_ += old->fre_count * fps[k - 1];
                        --lru_lens_[k];
                        sum_freqs_[k] -= old->fre_count;
                        LRU_Remove(old);
                        LRU_Append(&lrus_[k - 1], old);
                        ++lru_lens_[k - 1];
                        sum_freqs_[k - 1] += old->fre_count;
                        MeasureTime(Statistics::GetStatistics().get(), Tickers::REMOVE_HEAD_FILTER_TIME_0 + k, Env::Default()->NowMicros() - start_micros);
                        backup_node_hotness(old);
                    }
                }
                return true;
            }
        }

        int MultiQueue::AllocFilterNums(int freq) {
            int i = 1;
            int last_ = 1;
            while(i < lrus_num_)
            {
                if(lrus_[i].next != &lrus_[i])
                {
                    if (freq < sum_freqs_[i] / lru_lens_[i])
                        return i;
                    last_ = i;
                }
                ++i;
            }
            // return last_ + 1 < lrus_num_ ? last_ + 1 : last_;
            return last_;
        }

        inline bool MultiQueue::IsCacheFull() const
        {
            return usage_ >= capacity_;
        }

        Cache::Handle *MultiQueue::Insert(const Slice &key, void *value, size_t charge, void (*deleter)(const Slice &key, void *value, const bool realDelete))
        {
            const uint32_t hash = HashSlice(key);
            mutex_.lock();
            insert_count++;

            LRUQueueHandle *e = reinterpret_cast<LRUQueueHandle *>(
                                    malloc(sizeof(LRUQueueHandle) - 1 + key.size()));
            e->value = value;
            e->deleter = deleter;
            e->charge = charge;
            e->key_length = key.size();
            e->hash = hash;
            e->in_cache = false;
            e->refs = 1;  // for the returned handle.
            e->fre_count = 0;
            e->is_mark = false;
            leveldb::TableAndFile *tf = reinterpret_cast<leveldb::TableAndFile *>(e->value);
            uint32_t *regionId_ = (uint32_t *) (key.data() + sizeof(uint64_t));
            e->value_id = *regionId_;
            e->queue_id = tf->table->getCurrFilterNum(e->value_id - 1) ;   //

            e->expire_time = current_time_ + life_time_;
            memcpy(e->key_data, key.data(), key.size());

            if (e->value_id == 0)
            {
                e->queue_id = 0;
                e->refs++;
                e->value_refs = 0;
                e->in_cache = true;
                e->table_handle = e;
                // mutex_.lock();
                LRU_Append(&in_use_, e);
                auto redun_handle = table_.Insert(e);
                if(redun_handle != NULL)
                {
                    FinishErase(redun_handle);
                }
                // mutex_.unlock();

            }
            else if (capacity_ > 0 && e->value_id > 0)
            {
                char buf[sizeof(uint64_t) + sizeof(uint32_t)];
                memcpy(buf, e->key_data, sizeof(uint64_t));
                regionId_ = (uint32_t *) (buf + sizeof(uint64_t));
                *regionId_ = 0;
                Slice key_(buf, sizeof(buf));
                uint32_t hash_ = HashSlice(key_);
                LRUQueueHandle *table_handle = table_.Lookup(key_, hash_);
                assert(table_handle);
                table_handle->value_refs++;
                e->table_handle = table_handle;
                if (e->value_id == 1)
                    e->prev_region = NULL;
                else {
                    *regionId_ = e->value_id - 1;
                    Slice key2(buf, sizeof(buf));
                    hash_ = HashSlice(key2);
                    LRUQueueHandle *prev_region = table_.Lookup(key2, hash_);
                    e->prev_region = prev_region;
                    prev_region->next_region = e;
                }
                e->next_region = NULL;

                e->in_cache = true;

                if (should_recovery_hotness_) {
                    uint64_t key_tmp;
                    char *key_ptr = (char*) &key_tmp;
                    uint32_t table_id = tf->file_number;
                    uint32_t region_id = e->value_id;
                    memcpy(key_ptr, &table_id, sizeof(uint32_t));
                    memcpy(key_ptr + sizeof(uint32_t), &region_id, sizeof(uint32_t));
                    if (freq_info_map.find(key_tmp) != freq_info_map.end()) {
                        auto hp = freq_info_map[key_tmp];

                        e->queue_id = hp.filter_num;
                        e->charge = bits_per_key_per_filter_sum[e->queue_id];
                        e->fre_count = hp.freq;
                        expection_ += e->fre_count * fps[e->queue_id];

                        tf->table->AdjustFilters(e->queue_id, e->value_id - 1);
                    }
                }

                LRU_Append(&lrus_[e->queue_id], e);
                ++lru_lens_[e->queue_id];
                ++sum_lru_len;

                usage_ += e->charge;
                auto redun_handle = table_.Insert(e);
                if(redun_handle != NULL)
                {
                    FinishErase(redun_handle);
                }
                // mutex_.unlock();

            } // else don't cache.  (Tests use capacity_==0 to turn off caching.)
            
            MayBeShrinkUsage();
            mutex_.unlock();
            return reinterpret_cast<Cache::Handle *>(e);
        }

        bool MultiQueue::FinishErase(LRUQueueHandle *e)
        {
            if (e != NULL)
            {
                assert(e->in_cache);
                LRU_Remove(e);
                if(e->refs == 1)     //means remove from LRU
                {
                    --lru_lens_[e->queue_id];
                    sum_freqs_[e->queue_id] -= e->fre_count;
                    --sum_lru_len;
                }
                e->in_cache = false;
                usage_ -= e->charge;
                Unref(e);
            }
            return e != NULL;
        }

        void MultiQueue::Erase(const Slice &key, uint32_t hash)
        {
            mutex_.lock();
            auto obsolete_handle = table_.Remove(key, hash);
            if(obsolete_handle != NULL)
            {
                FinishErase(obsolete_handle);
            }
            mutex_.unlock();

        }

        void MultiQueue::Erase(const Slice &key)
        {
            const uint32_t hash = HashSlice(key);
            Erase(key, hash);
        }

        size_t MultiQueue::TotalCharge() const {
            mutex_.lock();
            size_t usage = usage_;
            mutex_.unlock();
            return usage;
        }
        
        uint64_t MultiQueue::NewId()
        {
            return ++(last_id_);
        }

        void *MultiQueue::Value(Cache::Handle *handle)
        {
            return reinterpret_cast<LRUQueueHandle *>(handle)->value;
        }

    };

    Cache *NewMultiQueue(const std::string& dbname, size_t capacity, int lrus_num, uint64_t life_time, double change_ratio, bool cache_use_real_size, bool should_recovery_hotness)
    {
        return new multiqueue_ns::MultiQueue(dbname, capacity, lrus_num, life_time, change_ratio, cache_use_real_size, should_recovery_hotness);
    }

};

