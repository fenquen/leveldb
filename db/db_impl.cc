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
        explicit Writer(port::Mutex *mutex) : writeBatch_(nullptr),
                                              sync(false),
                                              done(false),
                                              condVar(mutex) {}

        Status status;
        WriteBatch *writeBatch_;
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
              hasImmutableMemTable_(false),
              logFileWritable_(nullptr),
              logFileNumber_(0),
              logWriter_(nullptr),
              seed_(0),
              tmpWriteBatch_(new WriteBatch),
              backgroundCompactionScheduled_(false),
              manualCompaction_(nullptr),
              versionSet(new VersionSet(dbname_,
                                        &options_,
                                        tableCache,
                                        &internalKeyComparator)) {}

    DBImpl::~DBImpl() {
        // Wait for background work to finish.
        mutex.Lock();
        shutting_down_.store(true, std::memory_order_release);
        while (backgroundCompactionScheduled_) {
            background_work_finished_signal_.Wait();
        }
        mutex.Unlock();

        if (db_lock_ != nullptr) {
            env_->UnlockFile(db_lock_);
        }

        delete versionSet;
        if (memTable_ != nullptr) memTable_->Unref();
        if (immutableMemTable != nullptr) immutableMemTable->Unref();
        delete tmpWriteBatch_;
        delete logWriter_;
        delete logFileWritable_;
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
        // 以下的2个是来自versionEdit
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
            status = this->RecoverLogFile(logFileNumVec[i],
                                          (i == logFileNumVec.size() - 1),
                                          save_manifest,
                                          versionEdit,
                                          &maxSequence);

            if (!status.ok()) {
                return status;
            }

            // the previous incarnation may not have written any MANIFEST records after allocating this log number
            // manually update the file number allocation counter in VersionSet.
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

        // read all the records and add to memTable memtable
        std::string scratch;
        Slice dest;
        WriteBatch writeBatch;
        // 发生compaction趟数
        int compactionRound = 0;

        // 当调用该函数的时候都要新生成memTable,下边的逻辑有可能会把该memTable变为dbImpl的memTable
        MemTable *memTable = nullptr;

        // log文件中的logRecord的payLoad是writeBatch内容
        while (logFileReader.ReadRecord(&dest, &scratch) && status.ok()) {
            if (dest.size() < WriteBatch::HEADER_LEN) { // 12 是 writeBatch的数据的header大小
                logReporter.Corruption(dest.size(),
                                       Status::Corruption("log dest too small"));
                continue;
            }

            // 得到的成果(log record 中保存的)是之前writeBatch的内容数据 现把这些的数据在还原到writeBatch
            WriteBatchInternal::SetContents(&writeBatch, dest);

            if (memTable == nullptr) {
                memTable = new MemTable(internalKeyComparator);
                memTable->Ref();
            }

            // 把 writeBatch内容 insert到 memTable_
            status = WriteBatchInternal::InsertIntoMemTable(&writeBatch, memTable);
            MaybeIgnoreError(&status);
            if (!status.ok()) {
                break;
            }

            // sequence
            const SequenceNumber lastSeq = WriteBatchInternal::Sequence(&writeBatch) +
                                           WriteBatchInternal::Count(&writeBatch) - 1;
            if (lastSeq > *maxSequence) {
                *maxSequence = lastSeq;
            }

            // 越过了buffer大小 要落地了
            if (memTable->ApproximateMemoryUsage() > options_.writeBufferSize) {
                compactionRound++;
                *save_manifest = true;
                status = this->WriteLevel0Table(memTable, versionEdit, nullptr);
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

        // 这边的逻辑会决定要必要把函数新生成的memTable注入到dbImple
        // See if we should keep reusing the last log logFileSequential
        if (status.ok() && options_.reuseLogs && lastLog && compactionRound == 0) {
            assert(logFileWritable_ == nullptr);
            assert(logWriter_ == nullptr);
            assert(memTable_ == nullptr);

            uint64_t logFileSize;

            if (env_->GetFileSize(logFilePath, &logFileSize).ok() &&
                env_->NewAppendableFile(logFilePath, &logFileWritable_).ok()) {

                Log(options_.logger, "reuse old log %s \n", logFilePath.c_str());

                logWriter_ = new log::Writer(logFileWritable_, logFileSize);
                logFileNumber_ = logNumber;

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

    Status DBImpl::WriteLevel0Table(MemTable *memTable,
                                    VersionEdit *versionEdit,
                                    Version *baseVersion) {
        mutex.AssertHeld();

        const uint64_t startMicros = env_->NowMicros();

        FileMetaData fileMetaData;
        fileMetaData.number = versionSet->NewFileNumber();
        pendingOutputFileNumberArr.insert(fileMetaData.number);

        Iterator *iterator = memTable->NewIterator();

        Log(options_.logger, "Level-0 table #%llu: started", (unsigned long long) fileMetaData.number);

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
            (unsigned long long) fileMetaData.fileSize_,
            status.ToString().c_str());

        delete iterator;
        pendingOutputFileNumberArr.erase(fileMetaData.number);

        // Note that if fileSize_ is zero, the file has been deleted and should not be added to the manifest.
        int level = 0;
        if (status.ok() && fileMetaData.fileSize_ > 0) {
            const Slice min_user_key = fileMetaData.smallestInternalKey_.user_key();
            const Slice max_user_key = fileMetaData.largestInternalKey_.user_key();
            if (baseVersion != nullptr) {
                level = baseVersion->PickLevelForMemTableOutput(min_user_key, max_user_key);
            }

            versionEdit->AddFile(level,
                                 fileMetaData.number,
                                 fileMetaData.fileSize_,
                                 fileMetaData.smallestInternalKey_,
                                 fileMetaData.largestInternalKey_);
        }

        CompactionStats compactionStats;
        compactionStats.micros = env_->NowMicros() - startMicros;
        compactionStats.bytes_written = fileMetaData.fileSize_;
        compactionStatArr[level].Add(compactionStats);

        return status;
    }

    void DBImpl::CompactMemTable() {
        mutex.AssertHeld();
        assert(immutableMemTable != nullptr);

        // save the content of the immutableMemTable as a new Table
        VersionEdit versionEdit;

        Version *currentVersion = versionSet->current();
        currentVersion->Ref();
        Status status = this->WriteLevel0Table(immutableMemTable, &versionEdit, currentVersion);
        currentVersion->Unref();

        if (status.ok() && shutting_down_.load(std::memory_order_acquire)) {
            status = Status::IOError("Deleting DB during memtable compaction");
        }

        // replace immutable memtable with the generated Table
        if (status.ok()) {
            versionEdit.SetPrevLogNumber(0);
            versionEdit.SetLogNumber(logFileNumber_);  // Earlier logs no longer needed
            status = versionSet->LogAndApply(&versionEdit, &mutex);
        }

        if (status.ok()) {
            // Commit to the new state
            immutableMemTable->Unref();
            immutableMemTable = nullptr;
            hasImmutableMemTable_.store(false, std::memory_order_release);
            RemoveObsoleteFiles();
        } else {
            RecordBackgroundError(status);
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
            if (manualCompaction_ == nullptr) {  // Idle
                manualCompaction_ = &manual;
                MaybeScheduleCompaction();
            } else {  // Running either my compaction or another compaction.
                background_work_finished_signal_.Wait();
            }
        }
        if (manualCompaction_ == &manual) {
            // Cancel my manual compaction since we aborted early for some reason.
            manualCompaction_ = nullptr;
        }
    }

    Status DBImpl::TEST_CompactMemTable() {
        // nullptr writeBatch_ means just wait for earlier writes to be done
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

        // already scheduled
        if (backgroundCompactionScheduled_) {
            return;
        }

        // DB is being deleted; no more background compactions
        if (shutting_down_.load(std::memory_order_acquire)) {
            return;
        }

        // Already got an error; no more changes
        if (!bg_error_.ok()) {
            return;
        }

        // No work to be done
        if (immutableMemTable == nullptr &&
            manualCompaction_ == nullptr &&
            !versionSet->NeedsCompaction()) {
            return;
        }

        backgroundCompactionScheduled_ = true;
        env_->Schedule(&DBImpl::BGWork, this);
    }

    void DBImpl::BGWork(void *db) {
        reinterpret_cast<DBImpl *>(db)->BackgroundCall();
    }

    void DBImpl::BackgroundCall() {
        MutexLock mutexLock(&mutex);
        assert(backgroundCompactionScheduled_);

        // No more background work when shutting down.
        if (shutting_down_.load(std::memory_order_acquire)) {

        } else if (!bg_error_.ok()) {  // No more background work after a background error.

        } else {
            this->BackgroundCompaction();
        }

        backgroundCompactionScheduled_ = false;

        // previous round may have produced too many files in a level,reschedule another compaction if needed.
        this->MaybeScheduleCompaction();
        background_work_finished_signal_.SignalAll();
    }

    void DBImpl::BackgroundCompaction() {
        mutex.AssertHeld();

        if (immutableMemTable != nullptr) {
            this->CompactMemTable();
            return;
        }

        Compaction *compaction;
        bool isManual = (manualCompaction_ != nullptr);
        InternalKey manualEnd;

        if (isManual) {
            ManualCompaction *m = manualCompaction_;
            compaction = versionSet->CompactRange(m->level, m->begin, m->end);
            m->done = (compaction == nullptr);
            if (compaction != nullptr) {
                manualEnd = compaction->input(0, compaction->num_input_files(0) - 1)->largestInternalKey_;
            }

            Log(options_.logger,
                "Manual compaction at level-%d from %s .. %s; will stop at %s\n",
                m->level, (m->begin ? m->begin->DebugString().c_str() : "(begin)"),
                (m->end ? m->end->DebugString().c_str() : "(end)"),
                (m->done ? "(end)" : manualEnd.DebugString().c_str()));
        } else {
            compaction = versionSet->PickCompaction();
        }

        Status status;

        if (compaction == nullptr) {
            // Nothing to do
        } else if (!isManual && compaction->IsTrivialMove()) {// Move file to next level
            assert(compaction->num_input_files(0) == 1);

            FileMetaData *fileMetaData = compaction->input(0, 0);

            compaction->edit()->RemoveFile(compaction->level(), fileMetaData->number);

            compaction->edit()->AddFile(compaction->level() + 1,
                                        fileMetaData->number,
                                        fileMetaData->fileSize_,
                                        fileMetaData->smallestInternalKey_,
                                        fileMetaData->largestInternalKey_);

            status = versionSet->LogAndApply(compaction->edit(), &mutex);
            if (!status.ok()) {
                this->RecordBackgroundError(status);
            }

            VersionSet::LevelSummaryStorage tmp{};

            Log(options_.logger, "Moved #%lld to level-%d %lld bytes %s: %s\n",
                static_cast<unsigned long long>(fileMetaData->number), compaction->level() + 1,
                static_cast<unsigned long long>(fileMetaData->fileSize_),
                status.ToString().c_str(), versionSet->LevelSummary(&tmp));
        } else {
            auto *compactionState = new CompactionState(compaction);
            status = DoCompactionWork(compactionState);
            if (!status.ok()) {
                RecordBackgroundError(status);
            }

            CleanupCompaction(compactionState);
            compaction->ReleaseInputs();
            RemoveObsoleteFiles();
        }
        delete compaction;

        if (status.ok()) {
            // Done
        } else if (shutting_down_.load(std::memory_order_acquire)) {
            // Ignore compaction errors found during shutting down
        } else {
            Log(options_.logger, "Compaction error: %s", status.ToString().c_str());
        }

        if (isManual) {
            ManualCompaction *manualCompaction = manualCompaction_;

            if (!status.ok()) {
                manualCompaction->done = true;
            }

            if (!manualCompaction->done) {
                // We only compacted part of the requested range.  Update *manualCompaction
                // to the range that is left to be compacted.
                manualCompaction->tmp_storage = manualEnd;
                manualCompaction->begin = &manualCompaction->tmp_storage;
            }

            manualCompaction_ = nullptr;
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
            if (hasImmutableMemTable_.load(std::memory_order_relaxed)) {
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
                stats.bytes_read += compact->compaction->input(which, i)->fileSize_;
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

    // Convenience method
    Status DBImpl::Put(const WriteOptions &o, const Slice &key, const Slice &val) {
        return DB::Put(o, key, val);
    }

    Status DBImpl::Delete(const WriteOptions &options, const Slice &key) {
        return DB::Delete(options, key);
    }

    // 先wal 再 memTable
    Status DBImpl::Write(const WriteOptions &writeOptions, WriteBatch *writeBatch) {
        Writer writer(&mutex);
        writer.writeBatch_ = writeBatch;
        writer.sync = writeOptions.sync;
        writer.done = false;

        MutexLock mutexLock(&mutex);
        writerDeque_.push_back(&writer);
        while (!writer.done && &writer != writerDeque_.front()) {
            writer.condVar.Wait();
        }
        if (writer.done) {
            return writer.status;
        }

        // May temporarily unlock and wait.
        Status status = this->MakeRoomForWrite(writeBatch == nullptr);
        uint64_t lastSequence = versionSet->LastSequence();
        // 暂时设置 不定是真的
        Writer *lastWriter = &writer;
        if (status.ok() && writeBatch != nullptr) {  // nullptr writeBatch_ is for compactions
            WriteBatch *mergedWriteBatch = this->BuildBatchGroup(&lastWriter);

            WriteBatchInternal::SetSequence(mergedWriteBatch, lastSequence + 1);
            lastSequence += WriteBatchInternal::Count(mergedWriteBatch);

            // Add to log and apply to memtable
            // We can release the lock since &writer is currently responsible for logging
            // and protects against concurrent loggers and concurrent writes
            // into memTable_
            {
                mutex.Unlock();

                // 写wal
                status = logWriter_->AddRecord(WriteBatchInternal::Contents(mergedWriteBatch));

                bool syncError = false;
                if (status.ok() && writeOptions.sync) {
                    status = logFileWritable_->Sync();
                    if (!status.ok()) {
                        syncError = true;
                    }
                }

                // 写memTable
                if (status.ok()) {
                    status = WriteBatchInternal::InsertIntoMemTable(mergedWriteBatch, memTable_);
                }

                mutex.Lock();

                if (syncError) {
                    // The state of the log file is indeterminate: the log record we
                    // just added may or may not show up when the DB is re-opened.
                    // So we force the DB into a mode where all future writes fail.
                    RecordBackgroundError(status);
                }
            }

            if (mergedWriteBatch == tmpWriteBatch_) {
                tmpWriteBatch_->Clear();
            }

            versionSet->SetLastSequence(lastSequence);
        }

        while (true) {
            Writer *ready = writerDeque_.front();
            writerDeque_.pop_front();

            // batch化注入相同的status
            if (ready != &writer) {
                ready->status = status;
                ready->done = true;
                ready->condVar.Signal();
            }

            if (ready == lastWriter) {
                break;
            }
        }

        // Notify new head of write queue
        if (!writerDeque_.empty()) {
            writerDeque_.front()->condVar.Signal();
        }

        return status;
    }

    // 本质是把writeDeque_中全部的writeBatch融合为1个
    // REQUIRES: Writer list must be non-empty
    // REQUIRES: First writer must have a non-null writeBatch_
    WriteBatch *DBImpl::BuildBatchGroup(Writer **lastWriter) {
        mutex.AssertHeld();
        assert(!writerDeque_.empty());

        Writer *frontWriter = writerDeque_.front();

        WriteBatch *result = frontWriter->writeBatch_;
        assert(result != nullptr);

        size_t size = WriteBatchInternal::ByteSize(frontWriter->writeBatch_);

        // Allow the group to grow up to a maximum size, but if the
        // original write is small, limit the growth so we do not slow down the small write too much.
        size_t maxSize = 1 << 20;
        if (size <= (128 << 10)) {
            maxSize = size + (128 << 10);
        }

        *lastWriter = frontWriter;

        auto iterator = writerDeque_.begin();

        // 越过当前的这个 "frontWriter"
        ++iterator;

        // 遍历
        for (; iterator != writerDeque_.end(); ++iterator) {
            Writer *writer = *iterator;

            // 领头的writer的sync和当前的不相同 break
            // do not include a sync write into a writeBatch_ handled by a non-sync write.
            if (writer->sync && !frontWriter->sync) {
                break;
            }

            if (writer->writeBatch_ != nullptr) {
                size += WriteBatchInternal::ByteSize(writer->writeBatch_);

                // 越过了maxSize了 break
                // do not make writeBatch_ too big
                if (size > maxSize) {
                    break;
                }

                // 只会成立了1趟
                if (result == frontWriter->writeBatch_) {
                    // 切换到dbImpl自己的tmpWriteBatch_ 不去扰乱 caller's writeBatch
                    result = tmpWriteBatch_;
                    assert(WriteBatchInternal::Count(result) == 0);
                    WriteBatchInternal::Append(result, frontWriter->writeBatch_);
                }

                WriteBatchInternal::Append(result, writer->writeBatch_);
            }

            *lastWriter = writer;
        }

        return result;
    }

    // REQUIRES: mutex is held
    // REQUIRES: this thread is currently at the front of the writer queue
    Status DBImpl::MakeRoomForWrite(bool force) {
        mutex.AssertHeld();
        assert(!writerDeque_.empty());
        bool allowDelay = !force;

        Status status;

        while (true) {
            // yield previous error
            if (!bg_error_.ok()) {
                status = bg_error_;
                break;
            } else if (allowDelay && versionSet->NumLevelFiles(0) >= config::kL0_SlowdownWritesTrigger) {
                // We are getting close to hitting a hard limit on the number of
                // L0 files.  Rather than delaying a single write by several
                // seconds when we hit the hard limit, start delaying each
                // individual write by 1ms to reduce latency variance.  Also,
                // this delay hands over some CPU to the compaction thread in
                // case it is sharing the same core as the writer.
                mutex.Unlock();
                env_->SleepForMicroseconds(1000);
                // 只允许delay1趟
                allowDelay = false;
                mutex.Lock();
            } else if (!force && (memTable_->ApproximateMemoryUsage() <= options_.writeBufferSize)) {
                // there is room in current memtable
                break;
            } else if (immutableMemTable != nullptr) {
                // 当前的memtable写满, but the previous one is still being compacted, so we wait.
                Log(options_.logger, "Current memtable full; waiting...\n");
                background_work_finished_signal_.Wait();
            } else if (versionSet->NumLevelFiles(0) >= config::kL0_StopWritesTrigger) {
                // There are too many level-0 file
                Log(options_.logger, "Too many L0 files; waiting...\n");
                background_work_finished_signal_.Wait();
            } else { // 切换到了new memtable and trigger compaction of old
                assert(versionSet->PrevLogNumber() == 0);

                uint64_t newLogNumber = versionSet->NewFileNumber();
                WritableFile *newLogFileWritable = nullptr;
                status = env_->NewWritableFile(LogFileName(dbname_, newLogNumber),
                                               &newLogFileWritable);
                if (!status.ok()) {
                    // avoid chewing through file number space in a tight loop.
                    versionSet->ReuseFileNumber(newLogNumber);
                    break;
                }

                delete logWriter_;
                delete logFileWritable_;

                logFileWritable_ = newLogFileWritable;
                logFileNumber_ = newLogNumber;
                logWriter_ = new log::Writer(newLogFileWritable);

                immutableMemTable = memTable_;
                hasImmutableMemTable_.store(true, std::memory_order_release);
                memTable_ = new MemTable(internalKeyComparator);
                memTable_->Ref();

                // do not force another compaction if have room
                force = false;

                this->MaybeScheduleCompaction();
            }
        }

        return status;
    }

    bool DBImpl::GetProperty(const Slice &property, std::string *value) {
        value->clear();

        MutexLock l(&mutex);
        Slice in = property;
        Slice prefix("leveldb.");
        if (!in.starts_with(prefix))
            return false;
        in.remove_prefix(prefix.size());

        if (in.starts_with("num-files-at-level")) {
            in.remove_prefix(strlen("num-files-at-level"));
            uint64_t level;
            bool ok = ConsumeDecimalNumber(&in, &level) && in.empty();
            if (!ok || level >= config::kNumLevels) {
                return false;
            }

            char buf[100];
            std::snprintf(buf, sizeof(buf), "%d",
                          versionSet->NumLevelFiles(static_cast<int>(level)));
            *value = buf;
            return true;
        }

        if (in == "stats") {
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
        }

        if (in == "sstables") {
            *value = versionSet->current()->DebugString();
            return true;
        }

        if (in == "approximate-memory-usage") {
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
    Status DB::Put(const WriteOptions &writeOptions,
                   const Slice &key,
                   const Slice &value) {
        WriteBatch writeBatch;
        writeBatch.Put(key, value);
        return Write(writeOptions, &writeBatch);
    }

    Status DB::Delete(const WriteOptions &opt, const Slice &key) {
        WriteBatch batch;
        batch.Delete(key);
        return Write(opt, &batch);
    }

    DB::~DB() = default;

    Status DB::Open(const Options &options, const std::string &dbname, DB **db) {
        *db = nullptr;

        auto *dbImpl = new DBImpl(options, dbname);

        dbImpl->mutex.Lock();

        VersionEdit versionEdit;
        bool saveManifest = false;
        Status status = dbImpl->Recover(&versionEdit, &saveManifest);
        if (status.ok() && dbImpl->memTable_ == nullptr) {
            // 生成 logFile 和 相应的memTable.
            uint64_t newLogFileNumber = dbImpl->versionSet->NewFileNumber();

            WritableFile *logFileWritable;
            status = options.env->NewWritableFile(LogFileName(dbname, newLogFileNumber),
                                                  &logFileWritable);
            if (status.ok()) {
                versionEdit.SetLogNumber(newLogFileNumber);
                dbImpl->logFileWritable_ = logFileWritable;
                dbImpl->logFileNumber_ = newLogFileNumber;
                dbImpl->logWriter_ = new log::Writer(logFileWritable);
                dbImpl->memTable_ = new MemTable(dbImpl->internalKeyComparator);
                dbImpl->memTable_->Ref();
            }
        }

        if (status.ok() && saveManifest) {
            versionEdit.SetPrevLogNumber(0);  // No older logs needed after recovery.
            versionEdit.SetLogNumber(dbImpl->logFileNumber_);
            status = dbImpl->versionSet->LogAndApply(&versionEdit, &dbImpl->mutex);
        }

        if (status.ok()) {
            dbImpl->RemoveObsoleteFiles();

            // 启动后台的compact线程
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
            for (auto &filename: filenames) {
                if (ParseFileName(filename, &number, &type) &&
                    type != kDBLockFile) {  // Lock file will be deleted at end
                    Status del = env->RemoveFile(dbname + "/" + filename);
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
