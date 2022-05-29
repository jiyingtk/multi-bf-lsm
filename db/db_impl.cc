// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_impl.h"

#include <stdint.h>
#include <stdio.h>
#include <table/filter_block.h>

#include <algorithm>
#include <set>
#include <string>
#include <vector>

#include "db/builder.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/statistics.h"
#include "leveldb/status.h"
#include "leveldb/table.h"
#include "leveldb/table_builder.h"
#include "port/port.h"
#include "table/block.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"
#include "util/mutexlock.h"
#include "util/stop_watch.h"
unsigned long long filter_mem_space = 0;
unsigned long long filter_num = 0;
unsigned long long table_meta_space = 0;
unsigned long long block_cache_hit = 0, bc_hit_prev = 0;
unsigned long long block_cache_count = 0, bc_count_prev = 0;
namespace leveldb {

const int kNumNonTableCacheFiles = 10;

// Information kept for every waiting writer
struct DBImpl::Writer {
  Status status;
  WriteBatch* batch;
  bool sync;
  bool done;
  port::CondVar cv;

  explicit Writer(port::Mutex* mu) : cv(mu) {}
};

struct DBImpl::CompactionState {
  Compaction* const compaction;

  // Sequence numbers < smallest_snapshot are not significant since we
  // will never have to service a snapshot below smallest_snapshot.
  // Therefore if we have seen a sequence number S <= smallest_snapshot,
  // we can drop all entries for the same key with sequence numbers < S.
  SequenceNumber smallest_snapshot;

  // Files produced by compaction
  struct Output {
    uint64_t number;
    uint64_t file_size;
    InternalKey smallest, largest;
  };
  std::vector<Output> outputs;

  // State kept for output being generated
  WritableFile* outfile;
  TableBuilder* builder;

  uint64_t total_bytes;

  Output* current_output() { return &outputs[outputs.size() - 1]; }

  explicit CompactionState(Compaction* c)
      : compaction(c), outfile(NULL), builder(NULL), total_bytes(0) {}
};

// Fix user-supplied options to be reasonable
template <class T, class V>
static void ClipToRange(T* ptr, V minvalue, V maxvalue) {
  if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
  if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
}
Options SanitizeOptions(const std::string& dbname,
                        const InternalKeyComparator* icmp,
                        const InternalFilterPolicy* ipolicy,
                        const Options& src) {
  Options result = src;
  result.comparator = icmp;
  result.filter_policy = (src.filter_policy != NULL) ? ipolicy : NULL;
  ClipToRange(&result.max_open_files, 64 + kNumNonTableCacheFiles, 200000);
  ClipToRange(&result.write_buffer_size, 64 << 10, 1 << 30);
  ClipToRange(&result.max_file_size, 1 << 20, 1 << 30);
  ClipToRange(&result.block_size, 1 << 10, 4 << 20);
  if (result.info_log == NULL) {
    // Open a log file in the same directory as the db
    src.env->CreateDir(dbname);  // In case it does not exist
    src.env->RenameFile(InfoLogFileName(dbname), OldInfoLogFileName(dbname));
    Status s = src.env->NewLogger(InfoLogFileName(dbname), &result.info_log);
    if (!s.ok()) {
      // No place suitable for logging
      result.info_log = NULL;
    }
  }
  if (result.block_cache == NULL) {
    // result.block_cache = NewLRUCache(8 << 20);
    result.block_cache = NULL;
  }

  char name_buf[100];
  snprintf(name_buf, sizeof(name_buf), "/freq_info");
  std::string freq_info_fn = dbname + name_buf;
  // result.opEp_.should_recovery_hotness = (access(freq_info_fn.c_str(), F_OK)
  // != -1) && !result.opEp_.cache_use_real_size;
  fprintf(stderr, "should recovery hotness: %s\n",
          (result.opEp_.should_recovery_hotness ? "True" : "False"));

#ifdef Micros_Stat
  fprintf(stderr, "duration unit: us\n");
#else
  fprintf(stderr, "duration unit: ns\n");
#endif

  return result;
}

DBImpl::DBImpl(const Options& raw_options, const std::string& dbname)
    : env_(raw_options.env),
      internal_comparator_(raw_options.comparator),
      internal_filter_policy_(raw_options.filter_policy),
      options_(SanitizeOptions(dbname, &internal_comparator_,
                               &internal_filter_policy_, raw_options)),
      owns_info_log_(options_.info_log != raw_options.info_log),
      owns_cache_(options_.block_cache != raw_options.block_cache),
      dbname_(dbname),
      statis_(options_.opEp_.stats_.get()),
      db_lock_(NULL),
      shutting_down_(NULL),
      bg_cv_(&mutex_),
      mem_(NULL),
      imm_(NULL),
      logfile_(NULL),
      logfile_number_(0),
      log_(NULL),
      seed_(0),
      tmp_batch_(new WriteBatch),
      bg_compaction_scheduled_(false),
      manual_compaction_(NULL) {
  has_imm_.Release_Store(NULL);

  // Reserve ten files or so for other uses and give the rest to TableCache.
  // const size_t table_cache_size = (size_t)((options_.max_open_files -
  // kNumNonTableCacheFiles)*options_.opEp_.filter_capacity_ratio / 8 * 64 *
  // 10000);
  size_t table_cache_size;
  if (options_.opEp_.cache_use_real_size) {
    table_cache_size =
        (size_t)((options_.max_open_files - kNumNonTableCacheFiles) *
                 options_.opEp_.filter_capacity_ratio / 8 *
                 options_.max_file_size / options_.opEp_.key_value_size);
  } else {
    if (options_.max_file_size > options_.opEp_.region_divide_size)
      table_cache_size =
          (size_t)((options_.max_open_files - kNumNonTableCacheFiles) *
                   options_.opEp_.filter_capacity_ratio *
                   options_.max_file_size / options_.opEp_.region_divide_size);
    else
      table_cache_size =
          (size_t)((options_.max_open_files - kNumNonTableCacheFiles) *
                   options_.opEp_.filter_capacity_ratio);
  }

  // if (options_.opEp_.useLRUCache) {
  //   table_cache_size = options_.max_open_files - kNumNonTableCacheFiles;
  // }

  table_cache_ = new TableCache(dbname_, &options_, table_cache_size);

  versions_ =
      new VersionSet(dbname_, &options_, table_cache_, &internal_comparator_);
  leveldb::directIO_of_RandomAccess = options_.opEp_.no_cache_io_;

  printf("DBImpl l0sizeraito %lf\n", options_.opEp_.l0_base_ratio);
  fp_reqs = fp_io = fp_nums = read_nums = filter_info = 0;
  fp_sum = 0;
  last_fp = 0;
  fp_calc_fpr_str.clear();
  filter_info_str.clear();
  fp_access_file_str.clear();
  fp_real_fpr_str.clear();
  fp_real_io_str.clear();

  for (int i = 0; i < config::kNumLevels; i++) get_latency_str[i].clear();
}

void DBImpl::untilCompactionEnds() {
  std::string preValue, afterValue;
  int count = 0;
  const int countMAX = 24000;
  this->GetProperty("leveldb.num-files", &afterValue);
  // std::cout<<afterValue<<std::endl;
  // std::cout<<preValue<<std::endl;
  while (preValue.compare(afterValue) != 0 && count < countMAX) {
    preValue = afterValue;
    sleep(120);
    this->GetProperty("leveldb.num-files", &afterValue);
    count++;
  }
  std::cout << "--- untilCompactionEnds will output ------------" << std::endl;
  if (count == countMAX) {
    fprintf(stderr, "Compaction is still running!\n");
  } else {
    fprintf(stderr, "no compaction!\n");
  }
  std::cout
      << "\n--------------above are untilCompactionEnds output--------------\n"
      << std::endl;
  std::string stat_str;
  if (count > 3) {
    this->GetProperty("leveldb.stats", &stat_str);
    std::cout << stat_str << std::endl;
  }
}

DBImpl::~DBImpl() {
  // Wait for background work to finish
  //  untilCompactionEnds();
  mutex_.Lock();
  shutting_down_.Release_Store(this);  // Any non-NULL value is ok
  while (bg_compaction_scheduled_) {
    bg_cv_.Wait();
  }
  mutex_.Unlock();

  if (db_lock_ != NULL) {
    env_->UnlockFile(db_lock_);
  }

  delete versions_;
  if (mem_ != NULL) mem_->Unref();
  if (imm_ != NULL) imm_->Unref();
  delete tmp_batch_;
  delete log_;
  delete logfile_;
  delete table_cache_;

  if (owns_info_log_) {
    delete options_.info_log;
  }
  if (owns_cache_) {
    delete options_.block_cache;
  }
  // FilterBlockReader::end_thread = true;
  // AlignedBuffer::FreeAlignedBuffers();
}

Status DBImpl::NewDB() {
  VersionEdit new_db;
  new_db.SetComparatorName(user_comparator()->Name());
  new_db.SetLogNumber(0);
  new_db.SetNextFile(2);
  new_db.SetLastSequence(0);

  const std::string manifest = DescriptorFileName(dbname_, 1);
  WritableFile* file;
  Status s = env_->NewWritableFile(manifest, &file);
  if (!s.ok()) {
    return s;
  }
  {
    log::Writer log(file);
    std::string record;
    new_db.EncodeTo(&record);
    s = log.AddRecord(record);
    if (s.ok()) {
      s = file->Close();
    }
  }
  delete file;
  if (s.ok()) {
    // Make "CURRENT" file that points to the new manifest file.
    s = SetCurrentFile(env_, dbname_, 1);
  } else {
    env_->DeleteFile(manifest);
  }
  return s;
}

void DBImpl::MaybeIgnoreError(Status* s) const {
  if (s->ok() || options_.paranoid_checks) {
    // No change needed
  } else {
    Log(options_.info_log, "Ignoring error %s", s->ToString().c_str());
    *s = Status::OK();
  }
}

void DBImpl::DeleteObsoleteFiles() {
  if (!bg_error_.ok()) {
    // After a background error, we don't know whether a new version may
    // or may not have been committed, so we cannot safely garbage collect.
    return;
  }

  // Make a set of all of the live files
  std::set<uint64_t> live = pending_outputs_;
  versions_->AddLiveFiles(&live);

  std::vector<std::string> filenames;
  env_->GetChildren(dbname_, &filenames);  // Ignoring errors on purpose
  uint64_t number;
  FileType type;
  for (size_t i = 0; i < filenames.size(); i++) {
    if (ParseFileName(filenames[i], &number, &type)) {
      bool keep = true;
      switch (type) {
        case kLogFile:
          keep = ((number >= versions_->LogNumber()) ||
                  (number == versions_->PrevLogNumber()));
          break;
        case kDescriptorFile:
          // Keep my manifest file, and any newer incarnations'
          // (in case there is a race that allows other incarnations)
          keep = (number >= versions_->ManifestFileNumber());
          break;
        case kTableFile:
          keep = (live.find(number) != live.end());
          break;
        case kTempFile:
          // Any temp files that are currently being written to must
          // be recorded in pending_outputs_, which is inserted into "live"
          keep = (live.find(number) != live.end());
          break;
        case kCurrentFile:
        case kDBLockFile:
        case kInfoLogFile:
          keep = true;
          break;
      }

      if (!keep) {
        if (type == kTableFile) {
          table_cache_->Evict(number);
        }
        Log(options_.info_log, "Delete type=%d #%lld\n", int(type),
            static_cast<unsigned long long>(number));
        env_->DeleteFile(dbname_ + "/" + filenames[i]);
      }
    }
  }
}

Status DBImpl::Recover(VersionEdit* edit, bool* save_manifest) {
  mutex_.AssertHeld();

  // Ignore error from CreateDir since the creation of the DB is
  // committed only when the descriptor is created, and this directory
  // may already exist from a previous failed creation attempt.
  env_->CreateDir(dbname_);
  assert(db_lock_ == NULL);
  Status s = env_->LockFile(LockFileName(dbname_), &db_lock_);
  if (!s.ok()) {
    return s;
  }

  if (!env_->FileExists(CurrentFileName(dbname_))) {
    if (options_.create_if_missing) {
      s = NewDB();
      if (!s.ok()) {
        return s;
      }
    } else {
      return Status::InvalidArgument(
          dbname_, "does not exist (create_if_missing is false)");
    }
  } else {
    if (options_.error_if_exists) {
      return Status::InvalidArgument(dbname_,
                                     "exists (error_if_exists is true)");
    }
  }

  s = versions_->Recover(save_manifest);
  if (!s.ok()) {
    return s;
  }
  SequenceNumber max_sequence(0);

  // Recover from all newer log files than the ones named in the
  // descriptor (new log files may have been added by the previous
  // incarnation without registering them in the descriptor).
  //
  // Note that PrevLogNumber() is no longer used, but we pay
  // attention to it in case we are recovering a database
  // produced by an older version of leveldb.
  const uint64_t min_log = versions_->LogNumber();
  const uint64_t prev_log = versions_->PrevLogNumber();
  std::vector<std::string> filenames;
  s = env_->GetChildren(dbname_, &filenames);
  if (!s.ok()) {
    return s;
  }
  std::set<uint64_t> expected;
  versions_->AddLiveFiles(&expected);
  uint64_t number;
  FileType type;
  std::vector<uint64_t> logs;
  for (size_t i = 0; i < filenames.size(); i++) {
    if (ParseFileName(filenames[i], &number, &type)) {
      expected.erase(number);
      if (type == kLogFile && ((number >= min_log) || (number == prev_log)))
        logs.push_back(number);
    }
  }
  if (!expected.empty()) {
    char buf[50];
    snprintf(buf, sizeof(buf), "%d missing files; e.g.",
             static_cast<int>(expected.size()));
    return Status::Corruption(buf, TableFileName(dbname_, *(expected.begin())));
  }

  // Recover in the order in which the logs were generated
  std::sort(logs.begin(), logs.end());
  for (size_t i = 0; i < logs.size(); i++) {
    s = RecoverLogFile(logs[i], (i == logs.size() - 1), save_manifest, edit,
                       &max_sequence);
    if (!s.ok()) {
      return s;
    }

    // The previous incarnation may not have written any MANIFEST
    // records after allocating this log number.  So we manually
    // update the file number allocation counter in VersionSet.
    versions_->MarkFileNumberUsed(logs[i]);
  }

  if (versions_->LastSequence() < max_sequence) {
    versions_->SetLastSequence(max_sequence);
  }

  return Status::OK();
}

Status DBImpl::RecoverLogFile(uint64_t log_number, bool last_log,
                              bool* save_manifest, VersionEdit* edit,
                              SequenceNumber* max_sequence) {
  struct LogReporter : public log::Reader::Reporter {
    Env* env;
    Logger* info_log;
    const char* fname;
    Status* status;  // NULL if options_.paranoid_checks==false
    virtual void Corruption(size_t bytes, const Status& s) {
      Log(info_log, "%s%s: dropping %d bytes; %s",
          (this->status == NULL ? "(ignoring error) " : ""), fname,
          static_cast<int>(bytes), s.ToString().c_str());
      if (this->status != NULL && this->status->ok()) *this->status = s;
    }
  };

  mutex_.AssertHeld();

  // Open the log file
  std::string fname = LogFileName(dbname_, log_number);
  SequentialFile* file;
  Status status = env_->NewSequentialFile(fname, &file);
  if (!status.ok()) {
    MaybeIgnoreError(&status);
    return status;
  }

  // Create the log reader.
  LogReporter reporter;
  reporter.env = env_;
  reporter.info_log = options_.info_log;
  reporter.fname = fname.c_str();
  reporter.status = (options_.paranoid_checks ? &status : NULL);
  // We intentionally make log::Reader do checksumming even if
  // paranoid_checks==false so that corruptions cause entire commits
  // to be skipped instead of propagating bad information (like overly
  // large sequence numbers).
  log::Reader reader(file, &reporter, true /*checksum*/, 0 /*initial_offset*/);
  Log(options_.info_log, "Recovering log #%llu",
      (unsigned long long)log_number);

  // Read all the records and add to a memtable
  std::string scratch;
  Slice record;
  WriteBatch batch;
  int compactions = 0;
  MemTable* mem = NULL;
  while (reader.ReadRecord(&record, &scratch) && status.ok()) {
    if (record.size() < 12) {
      reporter.Corruption(record.size(),
                          Status::Corruption("log record too small"));
      continue;
    }
    WriteBatchInternal::SetContents(&batch, record);

    if (mem == NULL) {
      mem = new MemTable(internal_comparator_);
      mem->Ref();
    }
    status = WriteBatchInternal::InsertInto(&batch, mem);
    MaybeIgnoreError(&status);
    if (!status.ok()) {
      break;
    }
    const SequenceNumber last_seq = WriteBatchInternal::Sequence(&batch) +
                                    WriteBatchInternal::Count(&batch) - 1;
    if (last_seq > *max_sequence) {
      *max_sequence = last_seq;
    }

    if (mem->ApproximateMemoryUsage() > options_.write_buffer_size) {
      compactions++;
      *save_manifest = true;
      status = WriteLevel0Table(mem, edit, NULL);
      mem->Unref();
      mem = NULL;
      if (!status.ok()) {
        // Reflect errors immediately so that conditions like full
        // file-systems cause the DB::Open() to fail.
        break;
      }
    }
  }

  delete file;

  // See if we should keep reusing the last log file.
  if (status.ok() && options_.reuse_logs && last_log && compactions == 0) {
    assert(logfile_ == NULL);
    assert(log_ == NULL);
    assert(mem_ == NULL);
    uint64_t lfile_size;
    if (env_->GetFileSize(fname, &lfile_size).ok() &&
        env_->NewAppendableFile(fname, &logfile_).ok()) {
      Log(options_.info_log, "Reusing old log %s \n", fname.c_str());
      log_ = new log::Writer(logfile_, lfile_size);
      logfile_number_ = log_number;
      if (mem != NULL) {
        mem_ = mem;
        mem = NULL;
      } else {
        // mem can be NULL if lognum exists but was empty.
        mem_ = new MemTable(internal_comparator_);
        mem_->Ref();
      }
    }
  }

  if (mem != NULL) {
    // mem did not get reused; compact it.
    if (status.ok()) {
      *save_manifest = true;
      status = WriteLevel0Table(mem, edit, NULL);
    }
    mem->Unref();
  }

  return status;
}

Status DBImpl::WriteLevel0Table(MemTable* mem, VersionEdit* edit,
                                Version* base) {
  mutex_.AssertHeld();
  const uint64_t start_micros = env_->NowMicros();
  FileMetaData meta;
  meta.number = versions_->NewFileNumber();
  pending_outputs_.insert(meta.number);
  Iterator* iter = mem->NewIterator();
  Log(options_.info_log, "Level-0 table #%llu: started",
      (unsigned long long)meta.number);

  Status s;
  {
    mutex_.Unlock();
    s = BuildTable(dbname_, env_, options_, table_cache_, iter, &meta, true);
    mutex_.Lock();
  }

  Log(options_.info_log, "Level-0 table #%llu: %lld bytes %s",
      (unsigned long long)meta.number, (unsigned long long)meta.file_size,
      s.ToString().c_str());
  delete iter;
  pending_outputs_.erase(meta.number);

  // Note that if file_size is zero, the file has been deleted and
  // should not be added to the manifest.
  int level = 0;
  if (s.ok() && meta.file_size > 0) {
    const Slice min_user_key = meta.smallest.user_key();
    const Slice max_user_key = meta.largest.user_key();
    if (base != NULL) {
      level = base->PickLevelForMemTableOutput(min_user_key, max_user_key);
    }
    edit->AddFile(level, meta.number, meta.file_size, meta.smallest,
                  meta.largest);
  }

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros;
  stats.bytes_written = meta.file_size;
  stats_[level].Add(stats);
  return s;
}

void DBImpl::CompactMemTable() {
  mutex_.AssertHeld();
  assert(imm_ != NULL);

  // Save the contents of the memtable as a new Table
  VersionEdit edit;
  Version* base = versions_->current();
  base->Ref();
  Status s = WriteLevel0Table(imm_, &edit, base);
  base->Unref();

  if (s.ok() && shutting_down_.Acquire_Load()) {
    s = Status::IOError("Deleting DB during memtable compaction");
  }

  // Replace immutable memtable with the generated Table
  if (s.ok()) {
    edit.SetPrevLogNumber(0);
    edit.SetLogNumber(logfile_number_);  // Earlier logs no longer needed
    s = versions_->LogAndApply(&edit, &mutex_);
  }

  if (s.ok()) {
    // Commit to the new state
    imm_->Unref();
    imm_ = NULL;
    has_imm_.Release_Store(NULL);
    DeleteObsoleteFiles();
  } else {
    RecordBackgroundError(s);
  }
}

void DBImpl::CompactRange(const Slice* begin, const Slice* end) {
  int max_level_with_files = 1;
  {
    MutexLock l(&mutex_);
    Version* base = versions_->current();
    for (int level = 1; level < config::kNumLevels; level++) {
      if (base->OverlapInLevel(level, begin, end)) {
        max_level_with_files = level;
      }
    }
  }
  TEST_CompactMemTable();  // TODO(sanjay): Skip if memtable does not overlap
  for (int level = 0; level < max_level_with_files; level++) {
    TEST_CompactRange(level, begin, end);
  }
}

void DBImpl::TEST_CompactRange(int level, const Slice* begin,
                               const Slice* end) {
  assert(level >= 0);
  assert(level + 1 < config::kNumLevels);

  InternalKey begin_storage, end_storage;

  ManualCompaction manual;
  manual.level = level;
  manual.done = false;
  if (begin == NULL) {
    manual.begin = NULL;
  } else {
    begin_storage = InternalKey(*begin, kMaxSequenceNumber, kValueTypeForSeek);
    manual.begin = &begin_storage;
  }
  if (end == NULL) {
    manual.end = NULL;
  } else {
    end_storage = InternalKey(*end, 0, static_cast<ValueType>(0));
    manual.end = &end_storage;
  }

  MutexLock l(&mutex_);
  while (!manual.done && !shutting_down_.Acquire_Load() && bg_error_.ok()) {
    if (manual_compaction_ == NULL) {  // Idle
      manual_compaction_ = &manual;
      MaybeScheduleCompaction();
    } else {  // Running either my compaction or another compaction.
      bg_cv_.Wait();
    }
  }
  if (manual_compaction_ == &manual) {
    // Cancel my manual compaction since we aborted early for some reason.
    manual_compaction_ = NULL;
  }
}

Status DBImpl::TEST_CompactMemTable() {
  // NULL batch means just wait for earlier writes to be done
  Status s = Write(WriteOptions(), NULL);
  if (s.ok()) {
    // Wait until the compaction completes
    MutexLock l(&mutex_);
    while (imm_ != NULL && bg_error_.ok()) {
      bg_cv_.Wait();
    }
    if (imm_ != NULL) {
      s = bg_error_;
    }
  }
  return s;
}

void DBImpl::RecordBackgroundError(const Status& s) {
  mutex_.AssertHeld();
  if (bg_error_.ok()) {
    bg_error_ = s;
    bg_cv_.SignalAll();
  }
}

void DBImpl::MaybeScheduleCompaction() {
  mutex_.AssertHeld();
  if (bg_compaction_scheduled_) {
    // Already scheduled
  } else if (shutting_down_.Acquire_Load()) {
    // DB is being deleted; no more background compactions
  } else if (!bg_error_.ok()) {
    // Already got an error; no more changes
  } else if (imm_ == NULL && manual_compaction_ == NULL &&
             !versions_->NeedsCompaction()) {
    // No work to be done
  } else {
    bg_compaction_scheduled_ = true;
    env_->Schedule(&DBImpl::BGWork, this);
  }
}

void DBImpl::BGWork(void* db) {
  reinterpret_cast<DBImpl*>(db)->BackgroundCall();
}

void DBImpl::BackgroundCall() {
  MutexLock l(&mutex_);
  assert(bg_compaction_scheduled_);
  if (shutting_down_.Acquire_Load()) {
    // No more background work when shutting down.
  } else if (!bg_error_.ok()) {
    // No more background work after a background error.
  } else {
    BackgroundCompaction();
  }

  bg_compaction_scheduled_ = false;

  // Previous compaction may have produced too many files in a level,
  // so reschedule another compaction if needed.
  MaybeScheduleCompaction();
  bg_cv_.SignalAll();
}

void DBImpl::BackgroundCompaction() {
  mutex_.AssertHeld();

  if (imm_ != NULL) {
    CompactMemTable();
    return;
  }

  Compaction* c;
  bool is_manual = (manual_compaction_ != NULL);
  InternalKey manual_end;
  if (is_manual) {
    ManualCompaction* m = manual_compaction_;
    c = versions_->CompactRange(m->level, m->begin, m->end);
    m->done = (c == NULL);
    if (c != NULL) {
      manual_end = c->input(0, c->num_input_files(0) - 1)->largest;
    }
    Log(options_.info_log,
        "Manual compaction at level-%d from %s .. %s; will stop at %s\n",
        m->level, (m->begin ? m->begin->DebugString().c_str() : "(begin)"),
        (m->end ? m->end->DebugString().c_str() : "(end)"),
        (m->done ? "(end)" : manual_end.DebugString().c_str()));
  } else {
    c = versions_->PickCompaction();
  }

  Status status;
  if (c == NULL) {
    // Nothing to do
  } else if (false && !is_manual &&
             c->IsTrivialMove()) {  // disable trivial move
    // Move file to next level
    assert(c->num_input_files(0) == 1);
    FileMetaData* f = c->input(0, 0);
    c->edit()->DeleteFile(c->level(), f->number);
    c->edit()->AddFile(c->level() + 1, f->number, f->file_size, f->smallest,
                       f->largest);
    status = versions_->LogAndApply(c->edit(), &mutex_);
    if (!status.ok()) {
      RecordBackgroundError(status);
    }
    VersionSet::LevelSummaryStorage tmp;
    Log(options_.info_log, "Moved #%lld to level-%d %lld bytes %s: %s\n",
        static_cast<unsigned long long>(f->number), c->level() + 1,
        static_cast<unsigned long long>(f->file_size),
        status.ToString().c_str(), versions_->LevelSummary(&tmp));
  } else {
    CompactionState* compact = new CompactionState(c);
    status = DoCompactionWork(compact);
    if (!status.ok()) {
      RecordBackgroundError(status);
    }

    if (compact->compaction->level() == 0) {
      for (int i = 0; i < compact->compaction->num_input_files(0); i++)
        table_cache_->SaveLevel0Freq(compact->compaction->input(0, i)->number);
    }

    CleanupCompaction(compact);
    c->ReleaseInputs();
    DeleteObsoleteFiles();
  }
  delete c;

  if (status.ok()) {
    // Done
  } else if (shutting_down_.Acquire_Load()) {
    // Ignore compaction errors found during shutting down
  } else {
    Log(options_.info_log, "Compaction error: %s", status.ToString().c_str());
  }

  if (is_manual) {
    ManualCompaction* m = manual_compaction_;
    if (!status.ok()) {
      m->done = true;
    }
    if (!m->done) {
      // We only compacted part of the requested range.  Update *m
      // to the range that is left to be compacted.
      m->tmp_storage = manual_end;
      m->begin = &m->tmp_storage;
    }
    manual_compaction_ = NULL;
  }
}
void DBImpl::KeepFreCount(CompactionState* compact) {
  uint64_t sum_fre_count = 0, bundle_fre_count = 0;
  uint64_t old_bags[30], new_bags[30];
  int which = 0, num_bags;
  int i;
  num_bags = compact->compaction->num_input_files(0);
  mutex_.Unlock();
  for (i = 0; i < num_bags; i++) {
    sum_fre_count +=
        table_cache_->LookupFreCount(compact->compaction->input(0, i)->number);
  }
  sum_fre_count = sum_fre_count / num_bags;
  num_bags = compact->compaction->num_input_files(1);
  for (i = 0; i < num_bags; i++) {
    old_bags[i] =
        table_cache_->LookupFreCount(compact->compaction->input(1, i)->number) +
        sum_fre_count;
  }
  size_t output_size = compact->outputs.size();
  int curr_ball_num = output_size, need_ball_num = num_bags,
      full_ball_num = output_size;
  int curr_id = 0;
  uint64_t new_fre_count;
  std::vector<leveldb::DBImpl::CompactionState::Output>::iterator iter =
      compact->outputs.begin();
  for (i = 0; i < output_size; i++) {
    if (curr_ball_num >= need_ball_num) {
      new_fre_count = old_bags[curr_id] * (need_ball_num * 1.0 / full_ball_num);
    } else {
      new_fre_count =
          old_bags[curr_id] * (curr_ball_num * 1.0 / full_ball_num) +
          old_bags[curr_id + 1] *
              ((need_ball_num - curr_ball_num) * 1.0 / full_ball_num);
    }
    curr_ball_num -= need_ball_num;
    if (curr_ball_num <= 0) {
      curr_ball_num += full_ball_num;
      ++curr_id;
    }
    table_cache_->SetFreCount(iter->number, new_fre_count);
    iter++;
  }
  mutex_.Lock();
}
void DBImpl::CleanupCompaction(CompactionState* compact) {
  mutex_.AssertHeld();

  if (options_.opEp_.setFreCountInCompaction) {
    uint64_t start_micros = Env::Default()->NowMicros();
    KeepFreCount(compact);
    MeasureTime(Statistics::GetStatistics().get(),
                Tickers::SET_FRE_COUNT_IN_COMPACTION_TIME,
                Env::Default()->NowMicros() - start_micros);
  }
  if (compact->builder != NULL) {
    // May happen if we get a shutdown call in the middle of compaction
    compact->builder->Abandon();
    delete compact->builder;
  } else {
    assert(compact->outfile == NULL);
  }
  delete compact->outfile;
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    pending_outputs_.erase(out.number);
  }
  delete compact;
}

Status DBImpl::OpenCompactionOutputFile(CompactionState* compact) {
  assert(compact != NULL);
  assert(compact->builder == NULL);
  uint64_t file_number;
  {
    mutex_.Lock();
    file_number = versions_->NewFileNumber();
    pending_outputs_.insert(file_number);
    CompactionState::Output out;
    out.number = file_number;
    out.smallest.Clear();
    out.largest.Clear();
    compact->outputs.push_back(out);
    mutex_.Unlock();
  }

  // Make the output file
  std::string fname = TableFileName(dbname_, file_number);
  Status s = env_->NewWritableFile(fname, &compact->outfile);
  if (s.ok()) {
    compact->builder = new TableBuilder(options_, compact->outfile);
  }
  return s;
}

Status DBImpl::FinishCompactionOutputFile(CompactionState* compact,
                                          Iterator* input) {
  assert(compact != NULL);
  assert(compact->outfile != NULL);
  assert(compact->builder != NULL);

  const uint64_t output_number = compact->current_output()->number;
  assert(output_number != 0);

  // Check for iterator errors
  Status s = input->status();
  const uint64_t current_entries = compact->builder->NumEntries();
  TableMetaData* tableMetaData_ = NULL;
  if (s.ok()) {
    s = compact->builder->Finish();
    tableMetaData_ = compact->builder->tableMetaData_;
  } else {
    compact->builder->Abandon();
  }
  const uint64_t current_bytes = compact->builder->FileSize();
  compact->current_output()->file_size = current_bytes;
  compact->total_bytes += current_bytes;
  delete compact->builder;
  compact->builder = NULL;

  // Finish and check for file errors
  if (s.ok()) {
    uint64_t start_micros = Env::Default()->NowMicros();
    s = compact->outfile->Sync();
    MeasureTime(Statistics::GetStatistics().get(), Tickers::SYNC_TIME,
                Env::Default()->NowMicros() - start_micros);
  }
  if (s.ok()) {
    s = compact->outfile->Close();
  }
  delete compact->outfile;
  compact->outfile = NULL;

  std::vector<uint64_t>*input_0_numbers, *input_1_numbers;
  input_0_numbers = new std::vector<uint64_t>();
  input_1_numbers = new std::vector<uint64_t>();
  for (int i = 0; i < compact->compaction->num_input_files(0); i++)
    input_0_numbers->push_back(compact->compaction->input(0, i)->number);
  for (int i = 0; i < compact->compaction->num_input_files(1); i++)
    input_1_numbers->push_back(compact->compaction->input(1, i)->number);

  if (s.ok() && current_entries > 0) {
    // Verify that the table is usable
    leveldb::ReadOptions ro;
    ro.file_level = compact->compaction->level() + 1;
    Iterator* iter = table_cache_->NewIterator(
        ro, output_number, current_bytes, NULL, tableMetaData_, input_0_numbers,
        input_1_numbers);
    delete input_0_numbers;
    delete input_1_numbers;

    s = iter->status();
    delete iter;
    if (s.ok()) {
      if (tableMetaData_ != NULL) delete tableMetaData_;
      Log(options_.info_log, "Generated table #%llu@%d: %lld keys, %lld bytes",
          (unsigned long long)output_number, compact->compaction->level(),
          (unsigned long long)current_entries,
          (unsigned long long)current_bytes);
    }
  }
  return s;
}

Status DBImpl::InstallCompactionResults(CompactionState* compact) {
  mutex_.AssertHeld();
  Log(options_.info_log, "Compacted %d@%d + %d@%d files => %lld bytes",
      compact->compaction->num_input_files(0), compact->compaction->level(),
      compact->compaction->num_input_files(1), compact->compaction->level() + 1,
      static_cast<long long>(compact->total_bytes));

  // Add compaction outputs
  compact->compaction->AddInputDeletions(compact->compaction->edit());
  const int level = compact->compaction->level();
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    compact->compaction->edit()->AddFile(level + 1, out.number, out.file_size,
                                         out.smallest, out.largest);
  }
  return versions_->LogAndApply(compact->compaction->edit(), &mutex_);
}

Status DBImpl::DoCompactionWork(CompactionState* compact) {
  const uint64_t start_micros = env_->NowMicros();
  int64_t imm_micros = 0;  // Micros spent doing imm_ compactions

  Log(options_.info_log, "Compacting %d@%d + %d@%d files",
      compact->compaction->num_input_files(0), compact->compaction->level(),
      compact->compaction->num_input_files(1),
      compact->compaction->level() + 1);

  assert(versions_->NumLevelFiles(compact->compaction->level()) > 0);
  assert(compact->builder == NULL);
  assert(compact->outfile == NULL);
  if (snapshots_.empty()) {
    compact->smallest_snapshot = versions_->LastSequence();
  } else {
    compact->smallest_snapshot = snapshots_.oldest()->number_;
  }

  // Release mutex while we're actually doing the compaction work
  mutex_.Unlock();

  Iterator* input = versions_->MakeInputIterator(compact->compaction);
  input->SeekToFirst();
  Status status;
  ParsedInternalKey ikey;
  std::string current_user_key;
  bool has_current_user_key = false;
  SequenceNumber last_sequence_for_key = kMaxSequenceNumber;
  for (; input->Valid() && !shutting_down_.Acquire_Load();) {
    // Prioritize immutable compaction work
    if (has_imm_.NoBarrier_Load() != NULL) {
      const uint64_t imm_start = env_->NowMicros();
      mutex_.Lock();
      if (imm_ != NULL) {
        CompactMemTable();
        bg_cv_.SignalAll();  // Wakeup MakeRoomForWrite() if necessary
      }
      mutex_.Unlock();
      imm_micros += (env_->NowMicros() - imm_start);
    }

    Slice key = input->key();
    if (compact->compaction->ShouldStopBefore(key) &&
        compact->builder != NULL) {
      status = FinishCompactionOutputFile(compact, input);
      if (!status.ok()) {
        break;
      }
    }

    // Handle key/value, add to state, etc.
    bool drop = false;
    if (!ParseInternalKey(key, &ikey)) {
      // Do not hide error keys
      current_user_key.clear();
      has_current_user_key = false;
      last_sequence_for_key = kMaxSequenceNumber;
    } else {
      if (!has_current_user_key ||
          user_comparator()->Compare(ikey.user_key, Slice(current_user_key)) !=
              0) {
        // First occurrence of this user key
        current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
        has_current_user_key = true;
        last_sequence_for_key = kMaxSequenceNumber;
      }

      if (last_sequence_for_key <= compact->smallest_snapshot) {
        // Hidden by an newer entry for same user key
        drop = true;  // (A)
      } else if (ikey.type == kTypeDeletion &&
                 ikey.sequence <= compact->smallest_snapshot &&
                 compact->compaction->IsBaseLevelForKey(ikey.user_key)) {
        // For this user key:
        // (1) there is no data in higher levels
        // (2) data in lower levels will have larger sequence numbers
        // (3) data in layers that are being compacted here and have
        //     smaller sequence numbers will be dropped in the next
        //     few iterations of this loop (by rule (A) above).
        // Therefore this deletion marker is obsolete and can be dropped.
        drop = true;
      }

      last_sequence_for_key = ikey.sequence;
    }
#if 0
    Log(options_.info_log,
        "  Compact: %s, seq %d, type: %d %d, drop: %d, is_base: %d, "
        "%d smallest_snapshot: %d",
        ikey.user_key.ToString().c_str(),
        (int)ikey.sequence, ikey.type, kTypeValue, drop,
        compact->compaction->IsBaseLevelForKey(ikey.user_key),
        (int)last_sequence_for_key, (int)compact->smallest_snapshot);
#endif

    if (!drop) {
      // Open output file if necessary
      if (compact->builder == NULL) {
        status = OpenCompactionOutputFile(compact);
        if (!status.ok()) {
          break;
        }
      }
      if (compact->builder->NumEntries() == 0) {
        compact->current_output()->smallest.DecodeFrom(key);
      }
      compact->current_output()->largest.DecodeFrom(key);
      compact->builder->Add(key, input->value());

      // Close output file if it is big enough
      if (compact->builder->FileSize() >=
          compact->compaction->MaxOutputFileSize()) {
        status = FinishCompactionOutputFile(compact, input);
        if (!status.ok()) {
          break;
        }
      }
    }

    input->Next();
  }

  if (status.ok() && shutting_down_.Acquire_Load()) {
    status = Status::IOError("Deleting DB during compaction");
  }
  if (status.ok() && compact->builder != NULL) {
    status = FinishCompactionOutputFile(compact, input);
  }
  if (status.ok()) {
    status = input->status();
  }
  delete input;
  input = NULL;

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros - imm_micros;
  MeasureTime(Statistics::GetStatistics().get(), Tickers::COMPACTION_TIME,
              Env::Default()->NowMicros() - start_micros);
  for (int which = 0; which < 2; which++) {
    for (int i = 0; i < compact->compaction->num_input_files(which); i++) {
      stats.bytes_read += compact->compaction->input(which, i)->file_size;
    }
  }
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    stats.bytes_written += compact->outputs[i].file_size;
  }

  mutex_.Lock();
  stats_[compact->compaction->level() + 1].Add(stats);

  if (status.ok()) {
    status = InstallCompactionResults(compact);
  }
  if (!status.ok()) {
    RecordBackgroundError(status);
  }
  VersionSet::LevelSummaryStorage tmp;
  Log(options_.info_log, "compacted to: %s", versions_->LevelSummary(&tmp));
  return status;
}

namespace {
struct IterState {
  port::Mutex* mu;
  Version* version;
  MemTable* mem;
  MemTable* imm;
};

static void CleanupIteratorState(void* arg1, void* arg2) {
  IterState* state = reinterpret_cast<IterState*>(arg1);
  state->mu->Lock();
  state->mem->Unref();
  if (state->imm != NULL) state->imm->Unref();
  state->version->Unref();
  state->mu->Unlock();
  delete state;
}
}  // namespace

Iterator* DBImpl::NewInternalIterator(const ReadOptions& options,
                                      SequenceNumber* latest_snapshot,
                                      uint32_t* seed) {
  IterState* cleanup = new IterState;
  mutex_.Lock();
  *latest_snapshot = versions_->LastSequence();

  // Collect together all needed child iterators
  std::vector<Iterator*> list;
  list.push_back(mem_->NewIterator());
  mem_->Ref();
  if (imm_ != NULL) {
    list.push_back(imm_->NewIterator());
    imm_->Ref();
  }
  versions_->current()->AddIterators(options, &list);
  Iterator* internal_iter =
      NewMergingIterator(&internal_comparator_, &list[0], list.size());
  versions_->current()->Ref();

  cleanup->mu = &mutex_;
  cleanup->mem = mem_;
  cleanup->imm = imm_;
  cleanup->version = versions_->current();
  internal_iter->RegisterCleanup(CleanupIteratorState, cleanup, NULL);

  *seed = ++seed_;
  mutex_.Unlock();
  return internal_iter;
}

Iterator* DBImpl::TEST_NewInternalIterator() {
  SequenceNumber ignored;
  uint32_t ignored_seed;
  return NewInternalIterator(ReadOptions(), &ignored, &ignored_seed);
}

int64_t DBImpl::TEST_MaxNextLevelOverlappingBytes() {
  MutexLock l(&mutex_);
  return versions_->MaxNextLevelOverlappingBytes();
}

Status DBImpl::Get(const ReadOptions& options, const Slice& key,
                   std::string* value) {
  Status s;
  MutexLock l(&mutex_);
  SequenceNumber snapshot;
  if (options.snapshot != NULL) {
    snapshot = reinterpret_cast<const SnapshotImpl*>(options.snapshot)->number_;
  } else {
    snapshot = versions_->LastSequence();
  }

  MemTable* mem = mem_;
  MemTable* imm = imm_;
  Version* current = versions_->current();
  mem->Ref();
  if (imm != NULL) imm->Ref();
  current->Ref();

  bool have_stat_update = false;
  Version::GetStats stats;
  std::allocator<StopWatch> alloc_stop_watch;
  auto p = alloc_stop_watch.allocate(1);
  alloc_stop_watch.construct(p, env_, statis_, MEM_READ_TIME);
  // Unlock while reading from files and memtables

  uint64_t fp_reqs_ = 0, filter_info_ = 0, read_nums_ = 0, fp_nums_ = 0,
           fp_io_ = 0;
  // double fp_sum_ = 0;
  {
    mutex_.Unlock();
    // First look in the memtable, then in the immutable memtable (if any).
    LookupKey lkey(key, snapshot);
    if (mem->Get(lkey, value, &s)) {
      // Done
      alloc_stop_watch.destroy(p);
    } else if (imm != NULL && imm->Get(lkey, value, &s)) {
      // Done
      p->setHistType(IMMEM_READ_TIME);
      alloc_stop_watch.destroy(p);
    } else {
      uint64_t start_micros = Env::Default()->NowMicros();

      s = current->Get(options, lkey, value, &stats);
      //      if(s.IsNotFound()){
      if (options.read_file_nums < 10)
        p->setHistType(options.read_file_nums + READ_0_TIME);
      else
        p->setHistType(10 + READ_0_TIME);

      alloc_stop_watch.destroy(p);
      //      }
      have_stat_update = true;

      // if(s.IsNotFound()){
      //   fp_sum += options.total_fpr;
      //   fp_reqs++;
      //   fp_nums += options.read_file_nums;
      // }

      fp_reqs_ = __sync_add_and_fetch(&fp_reqs, 1);
      // fp_sum_ = __sync_add_and_fetch(&fp_sum, options.total_fpr);
      fp_sum += options.total_fpr;
      filter_info_ = __sync_add_and_fetch(&filter_info, options.filter_info);
      read_nums_ = __sync_add_and_fetch(&read_nums, options.access_file_nums);
      fp_nums_ = __sync_add_and_fetch(&fp_nums, options.read_file_nums);
      fp_io_ = __sync_add_and_fetch(&fp_io, options.read_file_nums);

      if (!s.IsNotFound()) {
        fp_nums_ = __sync_sub_and_fetch(&fp_nums, 1);
      }
      // #ifndef MULTI_THREAD_MODE
      //   else {
      //     uint64_t during = Env::Default()->NowMicros() - start_micros;
      //     char buf[32];
      //     snprintf(buf, sizeof(buf), "%lu,", during);

      //     int which = options.access_compacted_file_nums >=
      //     config::kNumLevels ? config::kNumLevels - 1 :
      //     options.access_compacted_file_nums;
      //     get_latency_str[which].append(buf);
      //   }
      // #endif
    }
    alloc_stop_watch.deallocate(p, 1);
    mutex_.Lock();
  }

  if (have_stat_update && current->UpdateStats(stats)) {
    MaybeScheduleCompaction();
  }
  mem->Unref();
  if (imm != NULL) imm->Unref();
  current->Unref();

  // table_cache_->TurnOnAdjustment();
  // table_cache_->TurnOffAdjustment();

  if (fp_reqs_ != 0 && fp_reqs_ % options_.opEp_.fp_stat_num == 0) {
    char buf[32];
    // snprintf(buf, sizeof(buf), "%lu,", fp_reqs);
    snprintf(buf, sizeof(buf), "%.5lf,", (double)(1.0 * fp_sum / fp_reqs_));
    fp_calc_fpr_str.append(buf);
    snprintf(buf, sizeof(buf), "%.5lf,",
             (double)(1.0 * filter_info_ / fp_reqs_));
    filter_info_str.append(buf);
    snprintf(buf, sizeof(buf), "%.5lf,", (double)(1.0 * read_nums_ / fp_reqs_));
    fp_access_file_str.append(buf);
    snprintf(buf, sizeof(buf), "%.5lf,",
             (double)(1.0 * fp_nums_ / options_.opEp_.fp_stat_num));
    fp_real_fpr_str.append(buf);
    snprintf(buf, sizeof(buf), "%.5lf,", (double)(1.0 * fp_io_ / fp_reqs_));
    fp_real_io_str.append(buf);

    // double cur_fp = (double)(1.0*fp_nums_/options_.opEp_.fp_stat_num);
    // if (last_fp != 0) {
    //   double fp_diff = (cur_fp - last_fp) / last_fp;
    //   if (fp_diff < 0.1 && fp_diff > -0.1)
    //     table_cache_->TurnOffAdjustment();
    //   else
    //     table_cache_->TurnOnAdjustment();
    // }
    // last_fp = cur_fp;

    fp_nums = 0;
  }

  return s;
}

Iterator* DBImpl::NewIterator(const ReadOptions& options) {
  SequenceNumber latest_snapshot;
  uint32_t seed;
  Iterator* iter = NewInternalIterator(options, &latest_snapshot, &seed);
  return NewDBIterator(
      this, user_comparator(), iter,
      (options.snapshot != NULL
           ? reinterpret_cast<const SnapshotImpl*>(options.snapshot)->number_
           : latest_snapshot),
      seed);
}

void DBImpl::RecordReadSample(Slice key) {
  MutexLock l(&mutex_);
  if (versions_->current()->RecordReadSample(key)) {
    MaybeScheduleCompaction();
  }
}

const Snapshot* DBImpl::GetSnapshot() {
  MutexLock l(&mutex_);
  return snapshots_.New(versions_->LastSequence());
}

void DBImpl::ReleaseSnapshot(const Snapshot* s) {
  MutexLock l(&mutex_);
  snapshots_.Delete(reinterpret_cast<const SnapshotImpl*>(s));
}

// Convenience methods
Status DBImpl::Put(const WriteOptions& o, const Slice& key, const Slice& val) {
  return DB::Put(o, key, val);
}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
  return DB::Delete(options, key);
}

Status DBImpl::Write(const WriteOptions& options, WriteBatch* my_batch) {
  Writer w(&mutex_);
  w.batch = my_batch;
  w.sync = options.sync;
  w.done = false;

  MutexLock l(&mutex_);
  writers_.push_back(&w);
  while (!w.done && &w != writers_.front()) {
    w.cv.Wait();
  }
  if (w.done) {
    return w.status;
  }

  // May temporarily unlock and wait.
  uint64_t start_micros = Env::Default()->NowMicros();
  Status status = MakeRoomForWrite(my_batch == NULL);
  MeasureTime(Statistics::GetStatistics().get(), Tickers::MAKE_ROOM_FOR_WRITE,
              Env::Default()->NowMicros() - start_micros);
  uint64_t last_sequence = versions_->LastSequence();
  Writer* last_writer = &w;
  if (status.ok() && my_batch != NULL) {  // NULL batch is for compactions
    WriteBatch* updates = BuildBatchGroup(&last_writer);
    WriteBatchInternal::SetSequence(updates, last_sequence + 1);
    last_sequence += WriteBatchInternal::Count(updates);

    // Add to log and apply to memtable.  We can release the lock
    // during this phase since &w is currently responsible for logging
    // and protects against concurrent loggers and concurrent writes
    // into mem_.
    {
      start_micros = Env::Default()->NowMicros();
      mutex_.Unlock();
      status = log_->AddRecord(WriteBatchInternal::Contents(updates));
      bool sync_error = false;
      if (status.ok() && options.sync) {
        status = logfile_->Sync();
        if (!status.ok()) {
          sync_error = true;
        }
      }
      MeasureTime(Statistics::GetStatistics().get(), Tickers::WRITE_TO_LOG,
                  Env::Default()->NowMicros() - start_micros);
      start_micros = Env::Default()->NowMicros();
      if (status.ok()) {
        status = WriteBatchInternal::InsertInto(updates, mem_);
      }
      MeasureTime(Statistics::GetStatistics().get(), Tickers::WRITE_TO_MEMTABLE,
                  Env::Default()->NowMicros() - start_micros);
      mutex_.Lock();
      if (sync_error) {
        // The state of the log file is indeterminate: the log record we
        // just added may or may not show up when the DB is re-opened.
        // So we force the DB into a mode where all future writes fail.
        RecordBackgroundError(status);
      }
    }
    if (updates == tmp_batch_) tmp_batch_->Clear();

    versions_->SetLastSequence(last_sequence);
  }

  while (true) {
    Writer* ready = writers_.front();
    writers_.pop_front();
    if (ready != &w) {
      ready->status = status;
      ready->done = true;
      ready->cv.Signal();
    }
    if (ready == last_writer) break;
  }

  // Notify new head of write queue
  if (!writers_.empty()) {
    writers_.front()->cv.Signal();
  }

  return status;
}

// REQUIRES: Writer list must be non-empty
// REQUIRES: First writer must have a non-NULL batch
WriteBatch* DBImpl::BuildBatchGroup(Writer** last_writer) {
  assert(!writers_.empty());
  Writer* first = writers_.front();
  WriteBatch* result = first->batch;
  assert(result != NULL);

  size_t size = WriteBatchInternal::ByteSize(first->batch);

  // Allow the group to grow up to a maximum size, but if the
  // original write is small, limit the growth so we do not slow
  // down the small write too much.
  size_t max_size = 1 << 20;
  if (size <= (128 << 10)) {
    max_size = size + (128 << 10);
  }

  *last_writer = first;
  std::deque<Writer*>::iterator iter = writers_.begin();
  ++iter;  // Advance past "first"
  for (; iter != writers_.end(); ++iter) {
    Writer* w = *iter;
    if (w->sync && !first->sync) {
      // Do not include a sync write into a batch handled by a non-sync write.
      break;
    }

    if (w->batch != NULL) {
      size += WriteBatchInternal::ByteSize(w->batch);
      if (size > max_size) {
        // Do not make batch too big
        break;
      }

      // Append to *result
      if (result == first->batch) {
        // Switch to temporary batch instead of disturbing caller's batch
        result = tmp_batch_;
        assert(WriteBatchInternal::Count(result) == 0);
        WriteBatchInternal::Append(result, first->batch);
      }
      WriteBatchInternal::Append(result, w->batch);
    }
    *last_writer = w;
  }
  return result;
}

// REQUIRES: mutex_ is held
// REQUIRES: this thread is currently at the front of the writer queue
Status DBImpl::MakeRoomForWrite(bool force) {
  mutex_.AssertHeld();
  assert(!writers_.empty());
  bool allow_delay = !force;
  Status s;
  while (true) {
    uint64_t start_micros = Env::Default()->NowMicros();
    if (!bg_error_.ok()) {
      // Yield previous error
      s = bg_error_;
      break;
    } else if (allow_delay && versions_->NumLevelFiles(0) >=
                                  config::kL0_SlowdownWritesTrigger) {
      // We are getting close to hitting a hard limit on the number of
      // L0 files.  Rather than delaying a single write by several
      // seconds when we hit the hard limit, start delaying each
      // individual write by 1ms to reduce latency variance.  Also,
      // this delay hands over some CPU to the compaction thread in
      // case it is sharing the same core as the writer.
      mutex_.Unlock();
      env_->SleepForMicroseconds(1000);
      allow_delay = false;  // Do not delay a single write more than once
      MeasureTime(Statistics::GetStatistics().get(), Tickers::SLOW_DOWN_WRITE,
                  Env::Default()->NowMicros() - start_micros);
      MeasureTime(Statistics::GetStatistics().get(), Tickers::WAIT_TIME,
                  Env::Default()->NowMicros() - start_micros);
      mutex_.Lock();
    } else if (!force &&
               (mem_->ApproximateMemoryUsage() <= options_.write_buffer_size)) {
      // There is room in current memtable
      break;
    } else if (imm_ != NULL) {
      // We have filled up the current memtable, but the previous
      // one is still being compacted, so we wait.
      Log(options_.info_log, "Current memtable full; waiting...\n");
      bg_cv_.Wait();
      MeasureTime(Statistics::GetStatistics().get(), Tickers::WAIT_TIME,
                  Env::Default()->NowMicros() - start_micros);
    } else if (versions_->NumLevelFiles(0) >= config::kL0_StopWritesTrigger) {
      // There are too many level-0 files.
      Log(options_.info_log, "Too many L0 files; waiting...\n");
      bg_cv_.Wait();
      MeasureTime(Statistics::GetStatistics().get(), Tickers::WAIT_TIME,
                  Env::Default()->NowMicros() - start_micros);
      MeasureTime(Statistics::GetStatistics().get(), Tickers::STOP_WRITE,
                  Env::Default()->NowMicros() - start_micros);
    } else {
      // Attempt to switch to a new memtable and trigger compaction of old
      assert(versions_->PrevLogNumber() == 0);
      uint64_t new_log_number = versions_->NewFileNumber();
      WritableFile* lfile = NULL;
      s = env_->NewWritableFile(LogFileName(dbname_, new_log_number), &lfile);
      if (!s.ok()) {
        // Avoid chewing through file number space in a tight loop.
        versions_->ReuseFileNumber(new_log_number);
        break;
      }
      delete log_;
      delete logfile_;
      logfile_ = lfile;
      logfile_number_ = new_log_number;
      log_ = new log::Writer(lfile);
      imm_ = mem_;
      has_imm_.Release_Store(imm_);
      mem_ = new MemTable(internal_comparator_);
      mem_->Ref();
      force = false;  // Do not force another compaction if have room
      MaybeScheduleCompaction();
    }
  }
  return s;
}

bool DBImpl::GetProperty(const Slice& property, std::string* value) {
  value->clear();

  MutexLock l(&mutex_);
  Slice in = property;
  Slice prefix("leveldb.");
  if (!in.starts_with(prefix)) return false;
  in.remove_prefix(prefix.size());

  if (in.starts_with("num-files-at-level")) {
    in.remove_prefix(strlen("num-files-at-level"));
    uint64_t level;
    bool ok = ConsumeDecimalNumber(&in, &level) && in.empty();
    if (!ok || level >= config::kNumLevels) {
      return false;
    } else {
      char buf[100];
      snprintf(buf, sizeof(buf), "%d",
               versions_->NumLevelFiles(static_cast<int>(level)));
      *value = buf;
      return true;
    }
  } else if (in == "stats") {
    char buf[200];
    uint64_t stats_sum = 0;
    snprintf(buf, sizeof(buf),
             "                               Compactions\n"
             "Level  Files Size(MB) Time(sec) Read(MB) Write(MB)\n"
             "--------------------------------------------------\n");
    value->append(buf);
    for (int level = 0; level < config::kNumLevels; level++) {
      int files = versions_->NumLevelFiles(level);
      if (stats_[level].micros > 0 || files > 0) {
        snprintf(buf, sizeof(buf), "%3d %8d %8.0f %9.0f %8.0f %9.0f\n", level,
                 files, versions_->NumLevelBytes(level) / 1048576.0,
                 stats_[level].micros / 1e6,
                 stats_[level].bytes_read / 1048576.0,
                 stats_[level].bytes_written / 1048576.0);
        value->append(buf);
        stats_sum += stats_[level].micros;
      }
    }
    unsigned long long tmp_hit = block_cache_hit - bc_hit_prev,
                       tmp_count = block_cache_count - bc_count_prev;
    bc_hit_prev = block_cache_hit;
    bc_count_prev = block_cache_count;
    snprintf(buf, sizeof(buf),
             "filter mem space overhead:%llu filter_num:%llu table meta mem "
             "overhead:%llu \n",
             filter_mem_space, filter_num, table_meta_space);
    value->append(buf);
    snprintf(
        buf, sizeof(buf),
        "block cache count:%llu hit:%llu hit rate:%lf, phase hit rate:%lf \n",
        block_cache_count, block_cache_hit,
        block_cache_count == 0 ? 0.0
                               : block_cache_hit * 1.0 / block_cache_count,
        tmp_count == 0 ? 0 : tmp_hit * 1.0 / tmp_count);
    value->append(buf);
    if (statis_) {
      if (stats_sum != 0) {
        snprintf(buf, sizeof(buf),
                 "create filters time / compaction time = %.3lf%% write "
                 "filters time / compaction time = %.3lf%%\n",
                 statis_->GetTickerHistogram(Tickers::CREATE_FILTER_TIME) *
                     1.0 / stats_sum * 100,
                 statis_->GetTickerHistogram(Tickers::WRITE_FILTER_TIME) * 1.0 /
                     stats_sum * 100);
        value->append(buf);
        if (statis_->getTickerCount(Tickers::CREATE_FILTER_TIME) != 0) {
          snprintf(
              buf, sizeof(buf),
              "average create filters time  = %.3lf average filter lock time = "
              "%.3lf average filter wait time = %.3lf \n",
              statis_->GetTickerHistogram(Tickers::CREATE_FILTER_TIME) * 1.0 /
                  statis_->getTickerCount(Tickers::CREATE_FILTER_TIME),
              statis_->GetTickerHistogram(Tickers::FILTER_LOCK_TIME) * 1.0 /
                  statis_->getTickerCount(Tickers::FILTER_LOCK_TIME),
              statis_->GetTickerHistogram(Tickers::FILTER_WAIT_TIME) * 1.0 /
                  statis_->getTickerCount(Tickers::FILTER_WAIT_TIME));
          value->append(buf);
          snprintf(buf, sizeof(buf),
                   "average sync time  = %.3lf sync count = %lu \n",
                   statis_->GetTickerHistogram(Tickers::SYNC_TIME) * 1.0 /
                       statis_->getTickerCount(Tickers::SYNC_TIME),
                   statis_->getTickerCount(Tickers::SYNC_TIME));
          value->append(buf);
          snprintf(
              buf, sizeof(buf),
              "average child create filter time  = %.3lf average child other "
              "time = %.3lf \n",
              statis_->GetTickerHistogram(Tickers::CHILD_CREATE_FILTER_TIME) *
                  1.0 /
                  statis_->getTickerCount(Tickers::CHILD_CREATE_FILTER_TIME),
              statis_->getTickerCount(Tickers::CHILD_FILTER_OTHER_TIME) * 1.0 /
                  statis_->getTickerCount(Tickers::CHILD_FILTER_OTHER_TIME));
          value->append(buf);
        }
      }
      if (true) {
        int i;
        value->append(" Remove Expired Filter \n");
        for (i = Tickers::REMOVE_EXPIRED_FILTER_TIME_0;
             i <= Tickers::REMOVE_EXPIRED_FILTER_TIME_6; i++) {
          if (statis_->getTickerCount(i) == 0) {
            continue;
          }
          snprintf(buf, sizeof(buf),
                   "Remove LRU%d expired filter count: %lu time: %lu \n",
                   i - Tickers::REMOVE_EXPIRED_FILTER_TIME_0,
                   statis_->getTickerCount(i), statis_->GetTickerHistogram(i));
          value->append(buf);
        }
        value->append(" Remove Head Filter \n");
        for (i = Tickers::REMOVE_HEAD_FILTER_TIME_0;
             i <= Tickers::REMOVE_HEAD_FILTER_TIME_6; i++) {
          if (statis_->getTickerCount(i) == 0) {
            continue;
          }
          snprintf(buf, sizeof(buf),
                   "Remove LRU%d head filter count: %lu time: %lu \n",
                   i - Tickers::REMOVE_HEAD_FILTER_TIME_0,
                   statis_->getTickerCount(i), statis_->GetTickerHistogram(i));
          value->append(buf);
        }
      }
      if (statis_->getTickerCount(Tickers::REMOVE_FILTER_TIME) != 0) {
        int i = Tickers::REMOVE_FILTER_TIME;
        snprintf(buf, sizeof(buf), "Remove filter count: %lu time: %lu \n",
                 statis_->getTickerCount(i), statis_->GetTickerHistogram(i));
        value->append(buf);
      }
      if (statis_->getTickerCount(Tickers::ADD_FILTER_TIME_0) != 0 ||
          statis_->getTickerCount(Tickers::ADD_FILTER_TIME_0 +
                                  options_.opEp_.init_filter_nums) != 0) {
        int i;
        uint64_t totalAddFilterCount = 0;
        uint64_t totalAddFilterTime = 0;
        value->append(" ADD Filter Time \n");
        for (i = Tickers::ADD_FILTER_TIME_0; i <= Tickers::ADD_FILTER_TIME_7;
             i++) {
          if (statis_->getTickerCount(i) == 0) {
            continue;
          }
          snprintf(buf, sizeof(buf),
                   "add filter to %d filter(s) count: %lu time: %lu  average "
                   "time: %.3lf\n",
                   i - Tickers::ADD_FILTER_TIME_0, statis_->getTickerCount(i),
                   statis_->GetTickerHistogram(i),
                   statis_->GetTickerHistogram(i) * 1.0 /
                       statis_->getTickerCount(i));
          value->append(buf);
          totalAddFilterTime += statis_->GetTickerHistogram(i);
          totalAddFilterCount += statis_->getTickerCount(i);
        }
        snprintf(buf, sizeof(buf),
                 "total add filter count: %lu latency: %.2lf  ",
                 totalAddFilterCount,
                 totalAddFilterTime * 1.0 / totalAddFilterCount);
        value->append(buf);
      }
      value->append(
          statis_->ToString(Tickers::SET_FRE_COUNT_IN_COMPACTION_TIME,
                            Tickers::SET_FRE_COUNT_IN_COMPACTION_TIME));
      value->append(
          statis_->ToString(Tickers::FINDTABLE, Tickers::OPEN_TABLE_TIME));
      if (statis_->getTickerCount(Tickers::SLOW_DOWN_WRITE) != 0) {
        value->append(
            statis_->ToString(Tickers::SLOW_DOWN_WRITE, Tickers::WAIT_TIME));
      }
      if (statis_->getTickerCount(Tickers::WRITE_TO_MEMTABLE) != 0) {
        value->append(statis_->ToString(Tickers::WRITE_TO_MEMTABLE,
                                        Tickers::COMPACTION_READ_TIME));
      }
      value->append(table_cache_->LRU_Status());
      value->append(printStatistics());
      if (statis_->getTickerCount(Tickers::MQ_LOOKUP_TIME) != 0) {
        value->append(statis_->ToString(Tickers::MQ_LOOKUP_TIME,
                                        Tickers::MQ_LOOKUP_TIME));
      }
      value->append(
          statis_->ToString(Tickers::MQ_BACKUP_TIME, Tickers::MQ_BACKUP_TIME));
      statis_->reset();
    }
    return true;
  } else if (in == "sstables") {
    *value = versions_->current()->DebugString();
    return true;
  } else if (in == "approximate-memory-usage") {
    size_t total_usage = options_.block_cache->TotalCharge();
    if (mem_) {
      total_usage += mem_->ApproximateMemoryUsage();
    }
    if (imm_) {
      total_usage += imm_->ApproximateMemoryUsage();
    }
    char buf[50];
    snprintf(buf, sizeof(buf), "%llu",
             static_cast<unsigned long long>(total_usage));
    value->append(buf);
    return true;
  } else if (in.starts_with("files-access-frequencies")) {
    in.remove_prefix(strlen("files-access-frequencies"));
    uint64_t level;
    bool ok = ConsumeDecimalNumber(&in, &level) && in.empty();
    if (!ok || level >= config::kNumLevels) {
      return false;
    } else {
      versions_->printTables(level, value);
      return true;
    }
  } else if (in.starts_with("files-extra-infos")) {
    in.remove_prefix(strlen("files-extra-infos"));
    uint64_t level;
    bool ok = ConsumeDecimalNumber(&in, &level) && in.empty();
    if (!ok || level >= config::kNumLevels) {
      return false;
    } else {
      // versions_->printTableExtraInfos(level,value);
      value->append(get_latency_str[level]);
      return true;
    }
  } else if (in == "num-files") {
    for (int level = 0; level < config::kNumLevels; level++) {
      char buf[100];
      snprintf(buf, sizeof(buf), "%d",
               versions_->NumLevelFiles(static_cast<int>(level)));
      value->append(buf);
    }
    return true;
  } else if (in == "fp-stat-calc_fpr") {
    value->append(fp_calc_fpr_str);
    return true;
  } else if (in == "filter-info") {
    value->append(filter_info_str);
    return true;
  } else if (in == "fp-stat-access_file") {
    value->append(fp_access_file_str);
    return true;
  } else if (in == "fp-stat-real_fpr") {
    value->append(fp_real_fpr_str);
    return true;
  } else if (in == "fp-stat-real_io") {
    value->append(fp_real_io_str);
    return true;
  } else if (in.starts_with("file_filter_size")) {
    in.remove_prefix(strlen("file_filter_size"));
    uint64_t level;
    bool ok = ConsumeDecimalNumber(&in, &level) && in.empty();
    if (!ok || level >= config::kNumLevels) {
      return false;
    } else {
      versions_->printTables(level, value, "file_filter_size");
      return true;
    }
  }
  return false;
}

void DBImpl::GetApproximateSizes(const Range* range, int n, uint64_t* sizes) {
  // TODO(opt): better implementation
  Version* v;
  {
    MutexLock l(&mutex_);
    versions_->current()->Ref();
    v = versions_->current();
  }

  for (int i = 0; i < n; i++) {
    // Convert user_key into a corresponding internal key.
    InternalKey k1(range[i].start, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey k2(range[i].limit, kMaxSequenceNumber, kValueTypeForSeek);
    uint64_t start = versions_->ApproximateOffsetOf(v, k1);
    uint64_t limit = versions_->ApproximateOffsetOf(v, k2);
    sizes[i] = (limit >= start ? limit - start : 0);
  }

  {
    MutexLock l(&mutex_);
    v->Unref();
  }
}
std::string DBImpl::printStatistics() {
  if (statis_) {
    return statis_->ToString();
  }
  return std::string();
}

// Default implementations of convenience methods that subclasses of DB
// can call if they wish
Status DB::Put(const WriteOptions& opt, const Slice& key, const Slice& value) {
  WriteBatch batch;
  batch.Put(key, value);
  return Write(opt, &batch);
}

Status DB::Delete(const WriteOptions& opt, const Slice& key) {
  WriteBatch batch;
  batch.Delete(key);
  return Write(opt, &batch);
}

DB::~DB() {}

Status DB::Open(const Options& options, const std::string& dbname, DB** dbptr) {
  *dbptr = NULL;

  DBImpl* impl = new DBImpl(options, dbname);
  impl->mutex_.Lock();
  VersionEdit edit;
  // Recover handles create_if_missing, error_if_exists
  bool save_manifest = false;
  Status s = impl->Recover(&edit, &save_manifest);
  if (s.ok() && impl->mem_ == NULL) {
    // Create new log and a corresponding memtable.
    uint64_t new_log_number = impl->versions_->NewFileNumber();
    WritableFile* lfile;
    s = options.env->NewWritableFile(LogFileName(dbname, new_log_number),
                                     &lfile);
    if (s.ok()) {
      edit.SetLogNumber(new_log_number);
      impl->logfile_ = lfile;
      impl->logfile_number_ = new_log_number;
      impl->log_ = new log::Writer(lfile);
      impl->mem_ = new MemTable(impl->internal_comparator_);
      impl->mem_->Ref();
    }
  }
  if (s.ok() && save_manifest) {
    edit.SetPrevLogNumber(0);  // No older logs needed after recovery.
    edit.SetLogNumber(impl->logfile_number_);
    s = impl->versions_->LogAndApply(&edit, &impl->mutex_);
  }
  if (s.ok()) {
    impl->DeleteObsoleteFiles();
    impl->MaybeScheduleCompaction();
  }
  impl->mutex_.Unlock();
  if (s.ok()) {
    assert(impl->mem_ != NULL);
    impl->mutex_.Lock();
    if (options.opEp_.findAllTable) {
      impl->versions_->findAllTables();
    }
    impl->mutex_.Unlock();
    *dbptr = impl;
  } else {
    delete impl;
  }
  return s;
}

void DBImpl::DoSomeThing(void* arg) {
  char* thing_str = static_cast<char*>(arg);
  char adjust_filter_str[] = "adjust_filter";
  if (strncmp(thing_str, adjust_filter_str, strlen(adjust_filter_str)) == 0) {
    adjustFilter();
  }
}

void DBImpl::adjustFilter() {
  MutexLock l(&mutex_);
  versions_->adjustFilter();
}

Snapshot::~Snapshot() {}

Status DestroyDB(const std::string& dbname, const Options& options) {
  Env* env = options.env;
  std::vector<std::string> filenames;
  // Ignore error in case directory does not exist
  env->GetChildren(dbname, &filenames);
  if (filenames.empty()) {
    return Status::OK();
  }

  FileLock* lock;
  const std::string lockname = LockFileName(dbname);
  Status result = env->LockFile(lockname, &lock);
  if (result.ok()) {
    uint64_t number;
    FileType type;
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type) &&
          type != kDBLockFile) {  // Lock file will be deleted at end
        Status del = env->DeleteFile(dbname + "/" + filenames[i]);
        if (result.ok() && !del.ok()) {
          result = del;
        }
      }
    }
    env->UnlockFile(lock);  // Ignore error since state is already gone
    env->DeleteFile(lockname);
    env->DeleteDir(dbname);  // Ignore error in case dir contains other files
  }
  return result;
}

}  // namespace leveldb
