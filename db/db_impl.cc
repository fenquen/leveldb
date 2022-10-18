// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_impl.h"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdio>
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

namespace leveldb {

    const int kNumNonTableCacheFiles = 10;

// Information kept for every waiting writer
    struct DBImpl::Writer {
        explicit Writer(port::Mutex *mutex) : batch(nullptr),
                                              sync(false),
                                              done(false),
                                              condVar(mutex) {}

        Status status;
        WriteBatch *batch;
        bool sync;
        bool done;
        port::CondVar condVar;
    };

    struct DBImpl::CompactionState {
        // Files produced by compaction
        struct Output {
            uint64_t number;
            uint64_t file_size;
            InternalKey smallest, largest;
        };

        Output *current_output() { return &outputs[outputs.size() - 1]; }

        explicit CompactionState(Compaction *c)
                : compaction(c),
                  smallest_snapshot(0),
                  outfile(nullptr),
                  builder(nullptr),
                  total_bytes(0) {}

        Compaction *const compaction;

        // Sequence numbers < smallest_snapshot are not significant since we
        // will never have to service a snapshot below smallest_snapshot.
        // Therefore if we have seen a sequence number S <= smallest_snapshot,
        // we can drop all entries for the same key with sequence numbers < S.
        SequenceNumber smallest_snapshot;

        std::vector<Output> outputs;

        // State kept for output being generated
        WritableFile *outfile;
        TableBuilder *builder;

        uint64_t total_bytes;
    };

// Fix user-supplied options to be reasonable
    template<class T, class V>
    static void ClipToRange(T *ptr, V minvalue, V maxvalue) {
        if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
        if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
    }

    Options SanitizeOptions(const std::string &dbname,
                            const InternalKeyComparator *icmp,
                            const InternalFilterPolicy *ipolicy,
                            const Options &src) {
        Options result = src;

        result.comparator = icmp;
        result.filterPolicy = (src.filterPolicy != nullptr) ? ipolicy : nullptr;

        ClipToRange(&result.maxOpenFiles, 64 + kNumNonTableCacheFiles, 50000);
        ClipToRange(&result.writeBufferSize, 64 << 10, 1 << 30);
        ClipToRange(&result.maxFileSize, 1 << 20, 1 << 30);
        ClipToRange(&result.blockSize, 1 << 10, 4 << 20);

        if (result.logger == nullptr) {
            // Open a log file in the same directory as the db
            src.env->CreateDir(dbname);  // In case it does not exist
            src.env->RenameFile(InfoLogFileName(dbname), OldInfoLogFileName(dbname));
            Status s = src.env->NewLogger(InfoLogFileName(dbname), &result.logger);
            if (!s.ok()) {  // No place suitable for logging
                result.logger = nullptr;
            }
        }

        if (result.blockCache == nullptr) {
            result.blockCache = NewLRUCache(8 << 20);
        }

        return result;
    }

    static int TableCacheSize(const Options &sanitized_options) {
        // Reserve ten files or so for other uses and give the rest to TableCache.
        return sanitized_options.maxOpenFiles - kNumNonTableCacheFiles;
    }

    DBImpl::DBImpl(const Options &raw_options, const std::string &dbname)
            : env_(raw_options.env),
              internalKeyComparator(raw_options.comparator),
              internal_filter_policy_(raw_options.filterPolicy),
              options_(SanitizeOptions(dbname,
                                       &internalKeyComparator,
                                       &internal_filter_policy_,
                                       raw_options)),
              owns_info_log_(options_.logger != raw_options.logger),
              owns_cache_(options_.blockCache != raw_options.blockCache),
              dbname_(dbname),
              tableCache(new TableCache(dbname_,
                                        options_,
                                        TableCacheSize(options_))),
              db_lock_(nullptr),
              shutting_down_(false),
              background_work_finished_signal_(&mutex),
              memTable_(nullptr),
              immutableMemTable(nullptr),
              has_imm_(false),
              logfile_(nullptr),
              logfile_number_(0),
              log_(nullptr),
              seed_(0),
              tmp_batch_(new WriteBatch),
              background_compaction_scheduled_(false),
              manual_compaction_(nullptr),
              versionSet(new VersionSet(dbname_,
                                        &options_,
                                        tableCache,
                                        &internalKeyComparator)) {}

    DBImpl::~DBImpl() {
        // Wait for background work to finish.
        mutex.Lock();
        shutting_down_.store(true, std::memory_order_release);
        while (background_compaction_scheduled_) {
            background_work_finished_signal_.Wait();
        }
        mutex.Unlock();

        if (db_lock_ != nullptr) {
            env_->UnlockFile(db_lock_);
        }

        delete versionSet;
        if (memTable_ != nullptr) memTable_->Unref();
        if (immutableMemTable != nullptr) immutableMemTable->Unref();
        delete tmp_batch_;
        delete log_;
        delete logfile_;
        delete tableCache;

        if (owns_info_log_) {
            delete options_.logger;
        }
        if (owns_cache_) {
            delete options_.blockCache;
        }
    }

    Status DBImpl::NewDB() {
        VersionEdit versionEdit;
        versionEdit.SetComparatorName(user_comparator()->Name());
        versionEdit.SetLogNumber(0);
        versionEdit.SetNextFile(2);
        versionEdit.SetLastSequence(0);

        // dbname/MANIFEST-number
        const std::string manifestFilePath = DescriptorFileName(dbname_, 1);
        WritableFile *manifestFileWritable;
        Status status = env_->NewWritableFile(manifestFilePath, &manifestFileWritable);
        if (!status.ok()) {
            return status;
        }

        // 向manifestFile写入data,然后调用sync
        {
            log::Writer manifestFileWriter(manifestFileWritable);

            std::string record;
            versionEdit.EncodeTo(&record);

            status = manifestFileWriter.AddRecord(record);
            if (status.ok()) {
                status = manifestFileWritable->Sync();
            }

            if (status.ok()) {
                status = manifestFileWritable->Close();
            }
        }

        delete manifestFileWritable;

        if (status.ok()) {
            // Make current file point to the new manifestFile
            // descNumber 和 DescriptorFileName () 用到的相同
            status = SetCurrentFile(env_, dbname_, 1);
        } else {
            env_->RemoveFile(manifestFilePath);
        }

        return status;
    }

    void DBImpl::MaybeIgnoreError(Status *s) const {
        if (s->ok() || options_.paranoid_checks) {
            // No change needed
        } else {
            Log(options_.logger, "Ignoring error %s", s->ToString().c_str());
            *s = Status::OK();
        }
    }

    void DBImpl::RemoveObsoleteFiles() {
        mutex.AssertHeld();

        if (!bg_error_.ok()) {
            // After a background error, we don't know whether a new version may
            // or may not have been committed, so we cannot safely garbage collect.
            return;
        }

        // Make a set of all of the live files
        std::set<uint64_t> live = pendingOutputFileNumberArr;
        versionSet->AddLiveFiles(&live);

        std::vector<std::string> filenames;
        env_->GetChildren(dbname_, &filenames);  // Ignoring errors on purpose
        uint64_t number;
        FileType type;
        std::vector<std::string> files_to_delete;
        for (std::string &filename: filenames) {
            if (ParseFileName(filename, &number, &type)) {
                bool keep = true;
                switch (type) {
                    case kLogFile:
                        keep = ((number >= versionSet->LogNumber()) ||
                                (number == versionSet->PrevLogNumber()));
                        break;
                    case kDescriptorFile:
                        // Keep my manifest file, and any newer incarnations'
                        // (in case there is a race that allows other incarnations)
                        keep = (number >= versionSet->ManifestFileNumber());
                        break;
                    case kTableFile:
                        keep = (live.find(number) != live.end());
                        break;
                    case kTempFile:
                        // Any temp files that are currently being written to must
                        // be recorded in pendingOutputFileNumberArr, which is inserted into "live"
                        keep = (live.find(number) != live.end());
                        break;
                    case kCurrentFile:
                    case kDBLockFile:
                    case kInfoLogFile:
                        keep = true;
                        break;
                }

                if (!keep) {
                    files_to_delete.push_back(std::move(filename));
                    if (type == kTableFile) {
                        tableCache->Evict(number);
                    }
                    Log(options_.logger, "Delete type=%d #%lld\n", static_cast<int>(type),
                        static_cast<unsigned long long>(number));
                }
            }
        }

        // While deleting all files unblock other threads. All files being deleted
        // have unique names which will not collide with newly created files and
        // are therefore safe to delete while allowing other threads to proceed.
        mutex.Unlock();
        for (const std::string &filename: files_to_delete) {
            env_->RemoveFile(dbname_ + "/" + filename);
        }
        mutex.Lock();
    }

    Status DBImpl::Recover(VersionEdit *versionEdit, bool *save_manifest) {
        mutex.AssertHeld();

        // Ignore error from CreateDir since the creation of the DB is
        // committed only when the descriptor is created, and this directory
        // may already exist from a previous failed creation attempt.
        env_->CreateDir(dbname_);

        assert(db_lock_ == nullptr);
        Status status = env_->LockFile(LockFileName(dbname_), &db_lock_);
        if (!status.ok()) {
            return status;
        }

        if (!env_->FileExists(CurrentFileName(dbname_))) {
            if (options_.create_if_missing) {
                Log(options_.logger, "Creating DB %status since it was missing", dbname_.c_str());
                status = NewDB();
                if (!status.ok()) {
                    return status;
                }
            } else {
                return Status::InvalidArgument(dbname_, "does not exist (create_if_missing is false)");
            }
        } else {
            // 不允许已存在
            if (options_.error_if_exists) {
                return Status::InvalidArgument(dbname_, "exists (error_if_exists is true)");
            }
        }

        status = versionSet->Recover(save_manifest);
        if (!status.ok()) {
            return status;
        }

        SequenceNumber maxSequence = 0;

        // Recover from all newer log files than the ones named in the
        // descriptor (new log files may have been added by the previous
        // incarnation without registering them in the descriptor).
        //
        // Note that PrevLogNumber() is no longer used, but we pay
        // attention to it in case we are recovering a database produced by an older version of leveldb.
        std::vector<std::string> childFileNameVec;
        status = env_->GetChildren(dbname_, &childFileNameVec);
        if (!status.ok()) {
            return status;
        }

        // log是wal log
        const uint64_t minLogNum = versionSet->LogNumber();
        const uint64_t prevLogNum = versionSet->PrevLogNumber();

        std::set<uint64_t> missingFileNums;
        versionSet->AddLiveFiles(&missingFileNums);

        std::vector<uint64_t> logFileNumVec;

        for (auto &childFileName: childFileNameVec) {
            uint64_t number;
            FileType fileType;
            if (ParseFileName(childFileName, &number, &fileType)) {
                missingFileNums.erase(number);

                if (fileType == kLogFile && ((number >= minLogNum) || (number == prevLogNum))) {
                    logFileNumVec.push_back(number);
                }
            }
        }

        // 有缺少的
        if (!missingFileNums.empty()) {
            char buf[50];
            std::snprintf(buf,
                          sizeof(buf),
                          "%d missing files; e.g.",
                          static_cast<int>(missingFileNums.size()));

            return Status::Corruption(buf, TableFileName(dbname_, *(missingFileNums.begin())));
        }

        // 依log的顺序recover
        std::sort(logFileNumVec.begin(), logFileNumVec.end());

        // 挨个应用 wal
        for (size_t i = 0; i < logFileNumVec.size(); i++) {
            status = RecoverLogFile(logFileNumVec[i],
                                    (i == logFileNumVec.size() - 1),
                                    save_manifest,
                                    versionEdit,
                                    &maxSequence);

            if (!status.ok()) {
                return status;
            }

            // The previous incarnation may not have written any MANIFEST
            // records after allocating this log number.  So we manually
            // update the file number allocation counter in VersionSet.
            versionSet->MarkFileNumberUsed(logFileNumVec[i]);
        }

        if (versionSet->LastSequence() < maxSequence) {
            versionSet->SetLastSequence(maxSequence);
        }

        return Status::OK();
    }

    Status DBImpl::RecoverLogFile(uint64_t logNumber,
                                  bool lastLog,
                                  bool *save_manifest,
                                  VersionEdit *versionEdit,
                                  SequenceNumber *maxSequence) {

        struct LogReporter : public log::Reader::Reporter {
            Env *env;
            Logger *info_log;
            const char *fname;
            Status *status;  // null if options_.paranoid_checks==false

            void Corruption(size_t bytes, const Status &s) override {
                Log(info_log, "%s%s: dropping %d bytes; %s",
                    (this->status == nullptr ? "(ignoring error) " : ""), fname,
                    static_cast<int>(bytes), s.ToString().c_str());
                if (this->status != nullptr && this->status->ok()) *this->status = s;
            }
        };

        mutex.AssertHeld();

        // open the log file
        // dbname/number.log
        std::string logFilePath = LogFileName(dbname_, logNumber);

        SequentialFile *logFileSequential;
        Status status = env_->NewSequentialFile(logFilePath, &logFileSequential);
        if (!status.ok()) {
            MaybeIgnoreError(&status);
            return status;
        }

        LogReporter logReporter;
        logReporter.env = env_;
        logReporter.info_log = options_.logger;
        logReporter.fname = logFilePath.c_str();
        logReporter.status = (options_.paranoid_checks ? &status : nullptr);

        // We intentionally make log::Reader do checksumming even if
        // paranoid_checks==false so that corruptions cause entire commits
        // to be skipped instead of propagating bad information (like overly
        // large sequence numbers).
        log::Reader logFileReader(logFileSequential,
                                  &logReporter,
                                  true,
                                  0);

        Log(options_.logger,
            "Recovering log #%llu",
            (unsigned long long) logNumber);

        // Read all the records and add to memTable memtable
        std::string scratch;
        Slice dest;
        WriteBatch writeBatch;
        int compactions = 0;
        MemTable *memTable = nullptr;

        while (logFileReader.ReadRecord(&dest, &scratch) && status.ok()) {
            if (dest.size() < 12) {
                logReporter.Corruption(dest.size(), Status::Corruption("log dest too small"));
                continue;
            }

            WriteBatchInternal::SetContents(&writeBatch, dest);

            if (memTable == nullptr) {
                memTable = new MemTable(internalKeyComparator);
                memTable->Ref();
            }

            // 把 writeBatch内容 insert到 memTable_
            status = WriteBatchInternal::InsertInto(&writeBatch, memTable);
            MaybeIgnoreError(&status);
            if (!status.ok()) {
                break;
            }

            const SequenceNumber lastSeq = WriteBatchInternal::Sequence(&writeBatch) +
                                           WriteBatchInternal::Count(&writeBatch) - 1;
            if (lastSeq > *maxSequence) {
                *maxSequence = lastSeq;
            }

            // 越过了buffer大小 要落地了
            if (memTable->ApproximateMemoryUsage() > options_.writeBufferSize) {
                compactions++;
                *save_manifest = true;
                status = WriteLevel0Table(memTable, versionEdit, nullptr);
                memTable->Unref();
                memTable = nullptr;

                if (!status.ok()) {
                    // Reflect errors immediately so that conditions like full
                    // logFileSequential-systems cause the DB::Open() to fail.
                    break;
                }
            }
        }

        delete logFileSequential;

        // See if we should keep reusing the last log logFileSequential.
        if (status.ok() && options_.reuseLogs && lastLog && compactions == 0) {
            assert(logfile_ == nullptr);
            assert(log_ == nullptr);
            assert(memTable_ == nullptr);

            uint64_t lfile_size;

            if (env_->GetFileSize(logFilePath, &lfile_size).ok() &&
                env_->NewAppendableFile(logFilePath, &logfile_).ok()) {

                Log(options_.logger, "Reusing old log %s \n", logFilePath.c_str());

                log_ = new log::Writer(logfile_, lfile_size);
                logfile_number_ = logNumber;
                if (memTable != nullptr) {
                    memTable_ = memTable;
                    memTable = nullptr;
                } else {
                    // memTable_ can be nullptr if lognum exists but was empty.
                    memTable_ = new MemTable(internalKeyComparator);
                    memTable_->Ref();
                }
            }
        }

        if (memTable != nullptr) {
            // memTable_ did not get reused; compact it.
            if (status.ok()) {
                *save_manifest = true;
                status = WriteLevel0Table(memTable, versionEdit, nullptr);
            }
            memTable->Unref();
        }

        return status;
    }

    Status DBImpl::WriteLevel0Table(MemTable *memTable, VersionEdit *edit, Version *base) {
        mutex.AssertHeld();

        const uint64_t startMicros = env_->NowMicros();

        FileMetaData fileMetaData;
        fileMetaData.number = versionSet->NewFileNumber();
        pendingOutputFileNumberArr.insert(fileMetaData.number);

        Iterator *iterator = memTable->NewIterator();

        Log(options_.logger,"Level-0 table #%llu: started", (unsigned long long) fileMetaData.number);

        Status status;
        {
            mutex.Unlock();
            status = BuildTable(dbname_,
                                env_,
                                options_,
                                tableCache,
                                iterator,
                                &fileMetaData);
            mutex.Lock();
        }

        Log(options_.logger,
            "Level-0 table #%llu: %lld bytes %status",
            (unsigned long long) fileMetaData.number,
            (unsigned long long) fileMetaData.file_size,
            status.ToString().c_str());

        delete iterator;
        pendingOutputFileNumberArr.erase(fileMetaData.number);

        // Note that if file_size is zero, the file has been deleted and should not be added to the manifest.
        int level = 0;
        if (status.ok() && fileMetaData.file_size > 0) {
            const Slice min_user_key = fileMetaData.smallest.user_key();
            const Slice max_user_key = fileMetaData.largest.user_key();
            if (base != nullptr) {
                level = base->PickLevelForMemTableOutput(min_user_key, max_user_key);
            }

            edit->AddFile(level,
                          fileMetaData.number,
                          fileMetaData.file_size,
                          fileMetaData.smallest,
                          fileMetaData.largest);
        }

        CompactionStats compactionStats;
        compactionStats.micros = env_->NowMicros() - startMicros;
        compactionStats.bytes_written = fileMetaData.file_size;
        compactionStatArr[level].Add(compactionStats);
        return status;
    }

    void DBImpl::CompactMemTable() {
        mutex.AssertHeld();
        assert(immutableMemTable != nullptr);

        // Save the contents of the memtable as a new Table
        VersionEdit edit;
        Version *base = versionSet->current();
        base->Ref();
        Status s = WriteLevel0Table(immutableMemTable, &edit, base);
        base->Unref();

        if (s.ok() && shutting_down_.load(std::memory_order_acquire)) {
            s = Status::IOError("Deleting DB during memtable compaction");
        }

        // Replace immutable memtable with the generated Table
        if (s.ok()) {
            edit.SetPrevLogNumber(0);
            edit.SetLogNumber(logfile_number_);  // Earlier logs no longer needed
            s = versionSet->LogAndApply(&edit, &mutex);
        }

        if (s.ok()) {
            // Commit to the new state
            immutableMemTable->Unref();
            immutableMemTable = nullptr;
            has_imm_.store(false, std::memory_order_release);
            RemoveObsoleteFiles();
        } else {
            RecordBackgroundError(s);
        }
    }

    void DBImpl::CompactRange(const Slice *begin, const Slice *end) {
        int max_level_with_files = 1;
        {
            MutexLock l(&mutex);
            Version *base = versionSet->current();
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

    void DBImpl::TEST_CompactRange(int level, const Slice *begin,
                                   const Slice *end) {
        assert(level >= 0);
        assert(level + 1 < config::kNumLevels);

        InternalKey begin_storage, end_storage;

        ManualCompaction manual;
        manual.level = level;
        manual.done = false;
        if (begin == nullptr) {
            manual.begin = nullptr;
        } else {
            begin_storage = InternalKey(*begin, kMaxSequenceNumber, kValueTypeForSeek);
            manual.begin = &begin_storage;
        }
        if (end == nullptr) {
            manual.end = nullptr;
        } else {
            end_storage = InternalKey(*end, 0, static_cast<ValueType>(0));
            manual.end = &end_storage;
        }

        MutexLock l(&mutex);
        while (!manual.done && !shutting_down_.load(std::memory_order_acquire) &&
               bg_error_.ok()) {
            if (manual_compaction_ == nullptr) {  // Idle
                manual_compaction_ = &manual;
                MaybeScheduleCompaction();
            } else {  // Running either my compaction or another compaction.
                background_work_finished_signal_.Wait();
            }
        }
        if (manual_compaction_ == &manual) {
            // Cancel my manual compaction since we aborted early for some reason.
            manual_compaction_ = nullptr;
        }
    }

    Status DBImpl::TEST_CompactMemTable() {
        // nullptr batch means just wait for earlier writes to be done
        Status s = Write(WriteOptions(), nullptr);
        if (s.ok()) {
            // Wait until the compaction completes
            MutexLock l(&mutex);
            while (immutableMemTable != nullptr && bg_error_.ok()) {
                background_work_finished_signal_.Wait();
            }
            if (immutableMemTable != nullptr) {
                s = bg_error_;
            }
        }
        return s;
    }

    void DBImpl::RecordBackgroundError(const Status &s) {
        mutex.AssertHeld();
        if (bg_error_.ok()) {
            bg_error_ = s;
            background_work_finished_signal_.SignalAll();
        }
    }

    void DBImpl::MaybeScheduleCompaction() {
        mutex.AssertHeld();
        if (background_compaction_scheduled_) {
            // Already scheduled
        } else if (shutting_down_.load(std::memory_order_acquire)) {
            // DB is being deleted; no more background compactions
        } else if (!bg_error_.ok()) {
            // Already got an error; no more changes
        } else if (immutableMemTable == nullptr && manual_compaction_ == nullptr &&
                   !versionSet->NeedsCompaction()) {
            // No work to be done
        } else {
            background_compaction_scheduled_ = true;
            env_->Schedule(&DBImpl::BGWork, this);
        }
    }

    void DBImpl::BGWork(void *db) {
        reinterpret_cast<DBImpl *>(db)->BackgroundCall();
    }

    void DBImpl::BackgroundCall() {
        MutexLock l(&mutex);
        assert(background_compaction_scheduled_);
        if (shutting_down_.load(std::memory_order_acquire)) {
            // No more background work when shutting down.
        } else if (!bg_error_.ok()) {
            // No more background work after a background error.
        } else {
            BackgroundCompaction();
        }

        background_compaction_scheduled_ = false;

        // Previous compaction may have produced too many files in a level,
        // so reschedule another compaction if needed.
        MaybeScheduleCompaction();
        background_work_finished_signal_.SignalAll();
    }

    void DBImpl::BackgroundCompaction() {
        mutex.AssertHeld();

        if (immutableMemTable != nullptr) {
            CompactMemTable();
            return;
        }

        Compaction *c;
        bool is_manual = (manual_compaction_ != nullptr);
        InternalKey manual_end;
        if (is_manual) {
            ManualCompaction *m = manual_compaction_;
            c = versionSet->CompactRange(m->level, m->begin, m->end);
            m->done = (c == nullptr);
            if (c != nullptr) {
                manual_end = c->input(0, c->num_input_files(0) - 1)->largest;
            }
            Log(options_.logger,
                "Manual compaction at level-%d from %s .. %s; will stop at %s\n",
                m->level, (m->begin ? m->begin->DebugString().c_str() : "(begin)"),
                (m->end ? m->end->DebugString().c_str() : "(end)"),
                (m->done ? "(end)" : manual_end.DebugString().c_str()));
        } else {
            c = versionSet->PickCompaction();
        }

        Status status;
        if (c == nullptr) {
            // Nothing to do
        } else if (!is_manual && c->IsTrivialMove()) {
            // Move file to next level
            assert(c->num_input_files(0) == 1);
            FileMetaData *f = c->input(0, 0);
            c->edit()->RemoveFile(c->level(), f->number);
            c->edit()->AddFile(c->level() + 1, f->number, f->file_size, f->smallest,
                               f->largest);
            status = versionSet->LogAndApply(c->edit(), &mutex);
            if (!status.ok()) {
                RecordBackgroundError(status);
            }
            VersionSet::LevelSummaryStorage tmp;
            Log(options_.logger, "Moved #%lld to level-%d %lld bytes %s: %s\n",
                static_cast<unsigned long long>(f->number), c->level() + 1,
                static_cast<unsigned long long>(f->file_size),
                status.ToString().c_str(), versionSet->LevelSummary(&tmp));
        } else {
            CompactionState *compact = new CompactionState(c);
            status = DoCompactionWork(compact);
            if (!status.ok()) {
                RecordBackgroundError(status);
            }
            CleanupCompaction(compact);
            c->ReleaseInputs();
            RemoveObsoleteFiles();
        }
        delete c;

        if (status.ok()) {
            // Done
        } else if (shutting_down_.load(std::memory_order_acquire)) {
            // Ignore compaction errors found during shutting down
        } else {
            Log(options_.logger, "Compaction error: %s", status.ToString().c_str());
        }

        if (is_manual) {
            ManualCompaction *m = manual_compaction_;
            if (!status.ok()) {
                m->done = true;
            }
            if (!m->done) {
                // We only compacted part of the requested range.  Update *m
                // to the range that is left to be compacted.
                m->tmp_storage = manual_end;
                m->begin = &m->tmp_storage;
            }
            manual_compaction_ = nullptr;
        }
    }

    void DBImpl::CleanupCompaction(CompactionState *compact) {
        mutex.AssertHeld();
        if (compact->builder != nullptr) {
            // May happen if we get a shutdown call in the middle of compaction
            compact->builder->Abandon();
            delete compact->builder;
        } else {
            assert(compact->outfile == nullptr);
        }
        delete compact->outfile;
        for (size_t i = 0; i < compact->outputs.size(); i++) {
            const CompactionState::Output &out = compact->outputs[i];
            pendingOutputFileNumberArr.erase(out.number);
        }
        delete compact;
    }

    Status DBImpl::OpenCompactionOutputFile(CompactionState *compact) {
        assert(compact != nullptr);
        assert(compact->builder == nullptr);
        uint64_t file_number;
        {
            mutex.Lock();
            file_number = versionSet->NewFileNumber();
            pendingOutputFileNumberArr.insert(file_number);
            CompactionState::Output out;
            out.number = file_number;
            out.smallest.Clear();
            out.largest.Clear();
            compact->outputs.push_back(out);
            mutex.Unlock();
        }

        // Make the output file
        std::string fname = TableFileName(dbname_, file_number);
        Status s = env_->NewWritableFile(fname, &compact->outfile);
        if (s.ok()) {
            compact->builder = new TableBuilder(options_, compact->outfile);
        }
        return s;
    }

    Status DBImpl::FinishCompactionOutputFile(CompactionState *compact,
                                              Iterator *input) {
        assert(compact != nullptr);
        assert(compact->outfile != nullptr);
        assert(compact->builder != nullptr);

        const uint64_t output_number = compact->current_output()->number;
        assert(output_number != 0);

        // Check for iterator errors
        Status s = input->status();
        const uint64_t current_entries = compact->builder->NumEntries();
        if (s.ok()) {
            s = compact->builder->Finish();
        } else {
            compact->builder->Abandon();
        }
        const uint64_t current_bytes = compact->builder->FileSize();
        compact->current_output()->file_size = current_bytes;
        compact->total_bytes += current_bytes;
        delete compact->builder;
        compact->builder = nullptr;

        // Finish and check for file errors
        if (s.ok()) {
            s = compact->outfile->Sync();
        }
        if (s.ok()) {
            s = compact->outfile->Close();
        }
        delete compact->outfile;
        compact->outfile = nullptr;

        if (s.ok() && current_entries > 0) {
            // Verify that the table is usable
            Iterator *iter =
                    tableCache->NewIterator(ReadOptions(), output_number, current_bytes);
            s = iter->status();
            delete iter;
            if (s.ok()) {
                Log(options_.logger, "Generated table #%llu@%d: %lld keys, %lld bytes",
                    (unsigned long long) output_number, compact->compaction->level(),
                    (unsigned long long) current_entries,
                    (unsigned long long) current_bytes);
            }
        }
        return s;
    }

    Status DBImpl::InstallCompactionResults(CompactionState *compact) {
        mutex.AssertHeld();
        Log(options_.logger, "Compacted %d@%d + %d@%d files => %lld bytes",
            compact->compaction->num_input_files(0), compact->compaction->level(),
            compact->compaction->num_input_files(1), compact->compaction->level() + 1,
            static_cast<long long>(compact->total_bytes));

        // Add compaction outputs
        compact->compaction->AddInputDeletions(compact->compaction->edit());
        const int level = compact->compaction->level();
        for (size_t i = 0; i < compact->outputs.size(); i++) {
            const CompactionState::Output &out = compact->outputs[i];
            compact->compaction->edit()->AddFile(level + 1, out.number, out.file_size,
                                                 out.smallest, out.largest);
        }
        return versionSet->LogAndApply(compact->compaction->edit(), &mutex);
    }

    Status DBImpl::DoCompactionWork(CompactionState *compact) {
        const uint64_t start_micros = env_->NowMicros();
        int64_t imm_micros = 0;  // Micros spent doing immutableMemTable compactions

        Log(options_.logger, "Compacting %d@%d + %d@%d files",
            compact->compaction->num_input_files(0), compact->compaction->level(),
            compact->compaction->num_input_files(1),
            compact->compaction->level() + 1);

        assert(versionSet->NumLevelFiles(compact->compaction->level()) > 0);
        assert(compact->builder == nullptr);
        assert(compact->outfile == nullptr);
        if (snapshots_.empty()) {
            compact->smallest_snapshot = versionSet->LastSequence();
        } else {
            compact->smallest_snapshot = snapshots_.oldest()->sequence_number();
        }

        Iterator *input = versionSet->MakeInputIterator(compact->compaction);

        // Release mutex while we're actually doing the compaction work
        mutex.Unlock();

        input->SeekToFirst();
        Status status;
        ParsedInternalKey ikey;
        std::string current_user_key;
        bool has_current_user_key = false;
        SequenceNumber last_sequence_for_key = kMaxSequenceNumber;
        while (input->Valid() && !shutting_down_.load(std::memory_order_acquire)) {
            // Prioritize immutable compaction work
            if (has_imm_.load(std::memory_order_relaxed)) {
                const uint64_t imm_start = env_->NowMicros();
                mutex.Lock();
                if (immutableMemTable != nullptr) {
                    CompactMemTable();
                    // Wake up MakeRoomForWrite() if necessary.
                    background_work_finished_signal_.SignalAll();
                }
                mutex.Unlock();
                imm_micros += (env_->NowMicros() - imm_start);
            }

            Slice key = input->key();
            if (compact->compaction->ShouldStopBefore(key) &&
                compact->builder != nullptr) {
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
                        user_comparator()->Compare(ikey.userKey, Slice(current_user_key)) !=
                        0) {
                    // First occurrence of this user key
                    current_user_key.assign(ikey.userKey.data(), ikey.userKey.size());
                    has_current_user_key = true;
                    last_sequence_for_key = kMaxSequenceNumber;
                }

                if (last_sequence_for_key <= compact->smallest_snapshot) {
                    // Hidden by an newer entry for same user key
                    drop = true;  // (A)
                } else if (ikey.type == kTypeDeletion &&
                           ikey.sequence <= compact->smallest_snapshot &&
                           compact->compaction->IsBaseLevelForKey(ikey.userKey)) {
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
            Log(options_.logger,
                "  Compact: %s, seq %d, type: %d %d, drop: %d, is_base: %d, "
                "%d smallest_snapshot: %d",
                ikey.userKey.ToString().c_str(),
                (int)ikey.sequence, ikey.type, kTypeValue, drop,
                compact->compaction->IsBaseLevelForKey(ikey.userKey),
                (int)last_sequence_for_key, (int)compact->smallest_snapshot);
#endif

            if (!drop) {
                // Open output file if necessary
                if (compact->builder == nullptr) {
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

        if (status.ok() && shutting_down_.load(std::memory_order_acquire)) {
            status = Status::IOError("Deleting DB during compaction");
        }
        if (status.ok() && compact->builder != nullptr) {
            status = FinishCompactionOutputFile(compact, input);
        }
        if (status.ok()) {
            status = input->status();
        }
        delete input;
        input = nullptr;

        CompactionStats stats;
        stats.micros = env_->NowMicros() - start_micros - imm_micros;
        for (int which = 0; which < 2; which++) {
            for (int i = 0; i < compact->compaction->num_input_files(which); i++) {
                stats.bytes_read += compact->compaction->input(which, i)->file_size;
            }
        }
        for (size_t i = 0; i < compact->outputs.size(); i++) {
            stats.bytes_written += compact->outputs[i].file_size;
        }

        mutex.Lock();
        compactionStatArr[compact->compaction->level() + 1].Add(stats);

        if (status.ok()) {
            status = InstallCompactionResults(compact);
        }
        if (!status.ok()) {
            RecordBackgroundError(status);
        }
        VersionSet::LevelSummaryStorage tmp;
        Log(options_.logger, "compacted to: %s", versionSet->LevelSummary(&tmp));
        return status;
    }

    namespace {

        struct IterState {
            port::Mutex *const mu;
            Version *const version GUARDED_BY(mu);
            MemTable *const mem GUARDED_BY(mu);
            MemTable *const imm GUARDED_BY(mu);

            IterState(port::Mutex *mutex, MemTable *mem, MemTable *imm, Version *version)
                    : mu(mutex), version(version), mem(mem), imm(imm) {}
        };

        static void CleanupIteratorState(void *arg1, void *arg2) {
            IterState *state = reinterpret_cast<IterState *>(arg1);
            state->mu->Lock();
            state->mem->Unref();
            if (state->imm != nullptr) state->imm->Unref();
            state->version->Unref();
            state->mu->Unlock();
            delete state;
        }

    }  // anonymous namespace

    Iterator *DBImpl::NewInternalIterator(const ReadOptions &options,
                                          SequenceNumber *latest_snapshot,
                                          uint32_t *seed) {
        mutex.Lock();
        *latest_snapshot = versionSet->LastSequence();

        // Collect together all needed child iterators
        std::vector<Iterator *> list;
        list.push_back(memTable_->NewIterator());
        memTable_->Ref();
        if (immutableMemTable != nullptr) {
            list.push_back(immutableMemTable->NewIterator());
            immutableMemTable->Ref();
        }
        versionSet->current()->AddIterators(options, &list);
        Iterator *internal_iter =
                NewMergingIterator(&internalKeyComparator, &list[0], list.size());
        versionSet->current()->Ref();

        IterState *cleanup = new IterState(&mutex, memTable_, immutableMemTable, versionSet->current());
        internal_iter->RegisterCleanup(CleanupIteratorState, cleanup, nullptr);

        *seed = ++seed_;
        mutex.Unlock();
        return internal_iter;
    }

    Iterator *DBImpl::TEST_NewInternalIterator() {
        SequenceNumber ignored;
        uint32_t ignored_seed;
        return NewInternalIterator(ReadOptions(), &ignored, &ignored_seed);
    }

    int64_t DBImpl::TEST_MaxNextLevelOverlappingBytes() {
        MutexLock l(&mutex);
        return versionSet->MaxNextLevelOverlappingBytes();
    }

    Status DBImpl::Get(const ReadOptions &options, const Slice &key,
                       std::string *value) {
        Status s;
        MutexLock l(&mutex);
        SequenceNumber snapshot;
        if (options.snapshot != nullptr) {
            snapshot =
                    static_cast<const SnapshotImpl *>(options.snapshot)->sequence_number();
        } else {
            snapshot = versionSet->LastSequence();
        }

        MemTable *mem = memTable_;
        MemTable *imm = immutableMemTable;
        Version *current = versionSet->current();
        mem->Ref();
        if (imm != nullptr) imm->Ref();
        current->Ref();

        bool have_stat_update = false;
        Version::GetStats stats;

        // Unlock while reading from files and memtables
        {
            mutex.Unlock();
            // First look in the memtable, then in the immutable memtable (if any).
            LookupKey lkey(key, snapshot);
            if (mem->Get(lkey, value, &s)) {
                // Done
            } else if (imm != nullptr && imm->Get(lkey, value, &s)) {
                // Done
            } else {
                s = current->Get(options, lkey, value, &stats);
                have_stat_update = true;
            }
            mutex.Lock();
        }

        if (have_stat_update && current->UpdateStats(stats)) {
            MaybeScheduleCompaction();
        }
        mem->Unref();
        if (imm != nullptr) imm->Unref();
        current->Unref();
        return s;
    }

    Iterator *DBImpl::NewIterator(const ReadOptions &options) {
        SequenceNumber latest_snapshot;
        uint32_t seed;
        Iterator *iter = NewInternalIterator(options, &latest_snapshot, &seed);
        return NewDBIterator(this, user_comparator(), iter,
                             (options.snapshot != nullptr
                              ? static_cast<const SnapshotImpl *>(options.snapshot)
                                      ->sequence_number()
                              : latest_snapshot),
                             seed);
    }

    void DBImpl::RecordReadSample(Slice key) {
        MutexLock l(&mutex);
        if (versionSet->current()->RecordReadSample(key)) {
            MaybeScheduleCompaction();
        }
    }

    const Snapshot *DBImpl::GetSnapshot() {
        MutexLock l(&mutex);
        return snapshots_.New(versionSet->LastSequence());
    }

    void DBImpl::ReleaseSnapshot(const Snapshot *snapshot) {
        MutexLock l(&mutex);
        snapshots_.Delete(static_cast<const SnapshotImpl *>(snapshot));
    }

// Convenience methods
    Status DBImpl::Put(const WriteOptions &o, const Slice &key, const Slice &val) {
        return DB::Put(o, key, val);
    }

    Status DBImpl::Delete(const WriteOptions &options, const Slice &key) {
        return DB::Delete(options, key);
    }

    Status DBImpl::Write(const WriteOptions &writeOptions, WriteBatch *writeBatch) {
        Writer writer(&mutex);
        writer.batch = writeBatch;
        writer.sync = writeOptions.sync;
        writer.done = false;

        MutexLock mutexLock(&mutex);
        writerDeque.push_back(&writer);
        while (!writer.done && &writer != writerDeque.front()) {
            writer.condVar.Wait();
        }
        if (writer.done) {
            return writer.status;
        }

        // May temporarily unlock and wait.
        Status status = MakeRoomForWrite(writeBatch == nullptr);
        uint64_t last_sequence = versionSet->LastSequence();
        Writer *last_writer = &writer;
        if (status.ok() && writeBatch != nullptr) {  // nullptr batch is for compactions
            WriteBatch *write_batch = BuildBatchGroup(&last_writer);
            WriteBatchInternal::SetSequence(write_batch, last_sequence + 1);
            last_sequence += WriteBatchInternal::Count(write_batch);

            // Add to log and apply to memtable.  We can release the lock
            // during this phase since &writer is currently responsible for logging
            // and protects against concurrent loggers and concurrent writes
            // into memTable_.
            {
                mutex.Unlock();

                // 写wal
                status = log_->AddRecord(WriteBatchInternal::Contents(write_batch));
                bool sync_error = false;
                if (status.ok() && writeOptions.sync) {
                    status = logfile_->Sync();
                    if (!status.ok()) {
                        sync_error = true;
                    }
                }
                if (status.ok()) {
                    status = WriteBatchInternal::InsertInto(write_batch, memTable_);
                }

                mutex.Lock();
                if (sync_error) {
                    // The state of the log file is indeterminate: the log record we
                    // just added may or may not show up when the DB is re-opened.
                    // So we force the DB into a mode where all future writes fail.
                    RecordBackgroundError(status);
                }
            }
            if (write_batch == tmp_batch_) tmp_batch_->Clear();

            versionSet->SetLastSequence(last_sequence);
        }

        while (true) {
            Writer *ready = writerDeque.front();
            writerDeque.pop_front();
            if (ready != &writer) {
                ready->status = status;
                ready->done = true;
                ready->condVar.Signal();
            }
            if (ready == last_writer) break;
        }

        // Notify new head of write queue
        if (!writerDeque.empty()) {
            writerDeque.front()->condVar.Signal();
        }

        return status;
    }

// REQUIRES: Writer list must be non-empty
// REQUIRES: First writer must have a non-null batch
    WriteBatch *DBImpl::BuildBatchGroup(Writer **last_writer) {
        mutex.AssertHeld();
        assert(!writerDeque.empty());
        Writer *first = writerDeque.front();
        WriteBatch *result = first->batch;
        assert(result != nullptr);

        size_t size = WriteBatchInternal::ByteSize(first->batch);

        // Allow the group to grow up to a maximum size, but if the
        // original write is small, limit the growth so we do not slow
        // down the small write too much.
        size_t max_size = 1 << 20;
        if (size <= (128 << 10)) {
            max_size = size + (128 << 10);
        }

        *last_writer = first;
        std::deque<Writer *>::iterator iter = writerDeque.begin();
        ++iter;  // Advance past "first"
        for (; iter != writerDeque.end(); ++iter) {
            Writer *w = *iter;
            if (w->sync && !first->sync) {
                // Do not include a sync write into a batch handled by a non-sync write.
                break;
            }

            if (w->batch != nullptr) {
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

// REQUIRES: mutex is held
// REQUIRES: this thread is currently at the front of the writer queue
    Status DBImpl::MakeRoomForWrite(bool force) {
        mutex.AssertHeld();
        assert(!writerDeque.empty());
        bool allow_delay = !force;
        Status s;
        while (true) {
            if (!bg_error_.ok()) {
                // Yield previous error
                s = bg_error_;
                break;
            } else if (allow_delay && versionSet->NumLevelFiles(0) >=
                                      config::kL0_SlowdownWritesTrigger) {
                // We are getting close to hitting a hard limit on the number of
                // L0 files.  Rather than delaying a single write by several
                // seconds when we hit the hard limit, start delaying each
                // individual write by 1ms to reduce latency variance.  Also,
                // this delay hands over some CPU to the compaction thread in
                // case it is sharing the same core as the writer.
                mutex.Unlock();
                env_->SleepForMicroseconds(1000);
                allow_delay = false;  // Do not delay a single write more than once
                mutex.Lock();
            } else if (!force &&
                       (memTable_->ApproximateMemoryUsage() <= options_.writeBufferSize)) {
                // There is room in current memtable
                break;
            } else if (immutableMemTable != nullptr) {
                // We have filled up the current memtable, but the previous
                // one is still being compacted, so we wait.
                Log(options_.logger, "Current memtable full; waiting...\n");
                background_work_finished_signal_.Wait();
            } else if (versionSet->NumLevelFiles(0) >= config::kL0_StopWritesTrigger) {
                // There are too many level-0 files.
                Log(options_.logger, "Too many L0 files; waiting...\n");
                background_work_finished_signal_.Wait();
            } else {
                // Attempt to switch to a new memtable and trigger compaction of old
                assert(versionSet->PrevLogNumber() == 0);
                uint64_t new_log_number = versionSet->NewFileNumber();
                WritableFile *lfile = nullptr;
                s = env_->NewWritableFile(LogFileName(dbname_, new_log_number), &lfile);
                if (!s.ok()) {
                    // Avoid chewing through file number space in a tight loop.
                    versionSet->ReuseFileNumber(new_log_number);
                    break;
                }
                delete log_;
                delete logfile_;
                logfile_ = lfile;
                logfile_number_ = new_log_number;
                log_ = new log::Writer(lfile);
                immutableMemTable = memTable_;
                has_imm_.store(true, std::memory_order_release);
                memTable_ = new MemTable(internalKeyComparator);
                memTable_->Ref();
                force = false;  // Do not force another compaction if have room
                MaybeScheduleCompaction();
            }
        }
        return s;
    }

    bool DBImpl::GetProperty(const Slice &property, std::string *value) {
        value->clear();

        MutexLock l(&mutex);
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
                std::snprintf(buf, sizeof(buf), "%d",
                              versionSet->NumLevelFiles(static_cast<int>(level)));
                *value = buf;
                return true;
            }
        } else if (in == "stats") {
            char buf[200];
            std::snprintf(buf, sizeof(buf),
                          "                               Compactions\n"
                          "Level  Files Size(MB) Time(sec) Read(MB) Write(MB)\n"
                          "--------------------------------------------------\n");
            value->append(buf);
            for (int level = 0; level < config::kNumLevels; level++) {
                int files = versionSet->NumLevelFiles(level);
                if (compactionStatArr[level].micros > 0 || files > 0) {
                    std::snprintf(buf, sizeof(buf), "%3d %8d %8.0f %9.0f %8.0f %9.0f\n",
                                  level, files, versionSet->NumLevelBytes(level) / 1048576.0,
                                  compactionStatArr[level].micros / 1e6,
                                  compactionStatArr[level].bytes_read / 1048576.0,
                                  compactionStatArr[level].bytes_written / 1048576.0);
                    value->append(buf);
                }
            }
            return true;
        } else if (in == "sstables") {
            *value = versionSet->current()->DebugString();
            return true;
        } else if (in == "approximate-memory-usage") {
            size_t total_usage = options_.blockCache->TotalCharge();
            if (memTable_) {
                total_usage += memTable_->ApproximateMemoryUsage();
            }
            if (immutableMemTable) {
                total_usage += immutableMemTable->ApproximateMemoryUsage();
            }
            char buf[50];
            std::snprintf(buf, sizeof(buf), "%llu",
                          static_cast<unsigned long long>(total_usage));
            value->append(buf);
            return true;
        }

        return false;
    }

    void DBImpl::GetApproximateSizes(const Range *range, int n, uint64_t *sizes) {
        // TODO(opt): better implementation
        MutexLock l(&mutex);
        Version *v = versionSet->current();
        v->Ref();

        for (int i = 0; i < n; i++) {
            // Convert userKey into a corresponding internal key.
            InternalKey k1(range[i].start, kMaxSequenceNumber, kValueTypeForSeek);
            InternalKey k2(range[i].limit, kMaxSequenceNumber, kValueTypeForSeek);
            uint64_t start = versionSet->ApproximateOffsetOf(v, k1);
            uint64_t limit = versionSet->ApproximateOffsetOf(v, k2);
            sizes[i] = (limit >= start ? limit - start : 0);
        }

        v->Unref();
    }

    // Default implementations of convenience methods that subclasses of DB can call if they wish
    Status DB::Put(const WriteOptions &opt, const Slice &key, const Slice &value) {
        WriteBatch batch;
        batch.Put(key, value);
        return Write(opt, &batch);
    }

    Status DB::Delete(const WriteOptions &opt, const Slice &key) {
        WriteBatch batch;
        batch.Delete(key);
        return Write(opt, &batch);
    }

    DB::~DB() = default;

    Status DB::Open(const Options &options, const std::string &dbname, DB **db) {
        *db = nullptr;

        DBImpl *dbImpl = new DBImpl(options, dbname);

        dbImpl->mutex.Lock();

        VersionEdit versionEdit;
        bool saveManifest = false;
        Status status = dbImpl->Recover(&versionEdit, &saveManifest);
        if (status.ok() && dbImpl->memTable_ == nullptr) {
            // Create new log and a corresponding memtable.
            uint64_t new_log_number = dbImpl->versionSet->NewFileNumber();
            WritableFile *lfile;
            status = options.env->NewWritableFile(LogFileName(dbname, new_log_number), &lfile);
            if (status.ok()) {
                versionEdit.SetLogNumber(new_log_number);
                dbImpl->logfile_ = lfile;
                dbImpl->logfile_number_ = new_log_number;
                dbImpl->log_ = new log::Writer(lfile);
                dbImpl->memTable_ = new MemTable(dbImpl->internalKeyComparator);
                dbImpl->memTable_->Ref();
            }
        }

        if (status.ok() && saveManifest) {
            versionEdit.SetPrevLogNumber(0);  // No older logs needed after recovery.
            versionEdit.SetLogNumber(dbImpl->logfile_number_);
            status = dbImpl->versionSet->LogAndApply(&versionEdit, &dbImpl->mutex);
        }

        if (status.ok()) {
            dbImpl->RemoveObsoleteFiles();
            dbImpl->MaybeScheduleCompaction();
        }

        dbImpl->mutex.Unlock();

        if (status.ok()) {
            assert(dbImpl->memTable_ != nullptr);
            *db = dbImpl;
        } else {
            delete dbImpl;
        }

        return status;
    }

    Snapshot::~Snapshot() = default;

    Status DestroyDB(const std::string &dbname, const Options &options) {
        Env *env = options.env;
        std::vector<std::string> filenames;
        Status result = env->GetChildren(dbname, &filenames);
        if (!result.ok()) {
            // Ignore error in case directory does not exist
            return Status::OK();
        }

        FileLock *lock;
        const std::string lockname = LockFileName(dbname);
        result = env->LockFile(lockname, &lock);
        if (result.ok()) {
            uint64_t number;
            FileType type;
            for (size_t i = 0; i < filenames.size(); i++) {
                if (ParseFileName(filenames[i], &number, &type) &&
                    type != kDBLockFile) {  // Lock file will be deleted at end
                    Status del = env->RemoveFile(dbname + "/" + filenames[i]);
                    if (result.ok() && !del.ok()) {
                        result = del;
                    }
                }
            }
            env->UnlockFile(lock);  // Ignore error since state is already gone
            env->RemoveFile(lockname);
            env->RemoveDir(dbname);  // Ignore error in case dir contains other files
        }
        return result;
    }

}  // namespace leveldb
