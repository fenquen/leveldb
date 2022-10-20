// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_DB_IMPL_H_
#define STORAGE_LEVELDB_DB_DB_IMPL_H_

#include <atomic>
#include <deque>
#include <set>
#include <string>

#include "db/dbformat.h"
#include "db/log_writer.h"
#include "db/snapshot.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "port/port.h"
#include "port/thread_annotations.h"

namespace leveldb {

    class MemTable;

    class TableCache;

    class Version;

    class VersionEdit;

    class VersionSet;

    class DBImpl : public DB {
    public:
        DBImpl(const Options &options, const std::string &dbname);

        DBImpl(const DBImpl &) = delete;

        DBImpl &operator=(const DBImpl &) = delete;

        ~DBImpl() override;

        // Implementations of the DB interface
        Status Put(const WriteOptions &, const Slice &key, const Slice &value) override;

        Status Delete(const WriteOptions &, const Slice &key) override;

        Status Write(const WriteOptions &writeOptions, WriteBatch *writeBatch) override;

        Status Get(const ReadOptions &options, const Slice &key, std::string *value) override;

        Iterator *NewIterator(const ReadOptions &) override;

        const Snapshot *GetSnapshot() override;

        void ReleaseSnapshot(const Snapshot *snapshot) override;

        bool GetProperty(const Slice &property, std::string *value) override;

        void GetApproximateSizes(const Range *range, int n, uint64_t *sizes) override;

        void CompactRange(const Slice *begin, const Slice *end) override;

        // Extra methods (for testing) that are not in the public DB interface

        // Compact any files in the named level that overlap [*begin,*end]
        void TEST_CompactRange(int level, const Slice *begin, const Slice *end);

        // Force current memtable contents to be compacted.
        Status TEST_CompactMemTable();

        // Return an internal iterator over the current state of the database.
        // The keys of this iterator are internal keys (see format.h).
        // The returned iterator should be deleted when no longer needed.
        Iterator *TEST_NewInternalIterator();

        // Return the maximum overlapping data (in bytes) at next level for any
        // file at a level >= 1.
        int64_t TEST_MaxNextLevelOverlappingBytes();

        // Record a sample of bytes read at the specified internal key.
        // Samples are taken approximately once every config::kReadBytesPeriod
        // bytes.
        void RecordReadSample(Slice key);

    private:
        friend class DB;

        struct CompactionState;
        struct Writer;

        // Information for a manual compaction
        struct ManualCompaction {
            int level;
            bool done;
            const InternalKey *begin;  // null means beginning of key range
            const InternalKey *end;    // null means end of key range
            InternalKey tmp_storage;   // Used to keep track of compaction progress
        };

        // Per level compaction stats.  compactionStatArr[level] stores the stats for
        // compactions that produced data for the specified "level".
        struct CompactionStats {
            CompactionStats() : micros(0), bytes_read(0), bytes_written(0) {}

            void Add(const CompactionStats &c) {
                this->micros += c.micros;
                this->bytes_read += c.bytes_read;
                this->bytes_written += c.bytes_written;
            }

            int64_t micros;
            int64_t bytes_read;
            int64_t bytes_written;
        };

        Iterator *NewInternalIterator(const ReadOptions &,
                                      SequenceNumber *latest_snapshot,
                                      uint32_t *seed);

        Status NewDB();

        // Recover the descriptor from persistent storage.  May do a significant
        // amount of work to recover recently logged updates.  Any changes to
        // be made to the descriptor are added to *versionEdit.
        Status Recover(VersionEdit *versionEdit, bool *save_manifest)
        EXCLUSIVE_LOCKS_REQUIRED(mutex);

        void MaybeIgnoreError(Status *s) const;

        // Delete any unneeded files and stale in-memory entries.
        void RemoveObsoleteFiles() EXCLUSIVE_LOCKS_REQUIRED(mutex);

        // Compact the in-memory write buffer to disk.  Switches to a new
        // log-file/memtable and writes a new descriptor iff successful.
        // Errors are recorded in bg_error_.
        void CompactMemTable() EXCLUSIVE_LOCKS_REQUIRED(mutex);

        Status RecoverLogFile(uint64_t logNumber, bool lastLog, bool *save_manifest,
                              VersionEdit *versionEdit, SequenceNumber *maxSequence)
        EXCLUSIVE_LOCKS_REQUIRED(mutex);

        Status WriteLevel0Table(MemTable *memTable, VersionEdit *versionEdit, Version *baseVersion)
        EXCLUSIVE_LOCKS_REQUIRED(mutex);

        Status MakeRoomForWrite(bool force /* compact even if there is room? */)
        EXCLUSIVE_LOCKS_REQUIRED(mutex);

        WriteBatch *BuildBatchGroup(Writer **lastWriter)EXCLUSIVE_LOCKS_REQUIRED(mutex);

        // 以下都和 compaction 的对应
        void RecordBackgroundError(const Status &s);

        void MaybeScheduleCompaction() EXCLUSIVE_LOCKS_REQUIRED(mutex);

        static void BGWork(void *db);

        void BackgroundCall();

        void BackgroundCompaction() EXCLUSIVE_LOCKS_REQUIRED(mutex);

        void CleanupCompaction(CompactionState *compact) EXCLUSIVE_LOCKS_REQUIRED(mutex);

        Status DoCompactionWork(CompactionState *compact) EXCLUSIVE_LOCKS_REQUIRED(mutex);

        Status OpenCompactionOutputFile(CompactionState *compact);

        Status FinishCompactionOutputFile(CompactionState *compact, Iterator *input);

        Status InstallCompactionResults(CompactionState *compact) EXCLUSIVE_LOCKS_REQUIRED(mutex);

        const Comparator *user_comparator() const {
            return internalKeyComparator.user_comparator();
        }

        // Constant after construction
        Env *const env_;
        const InternalKeyComparator internalKeyComparator;
        const InternalFilterPolicy internal_filter_policy_;
        const Options options_;  // options_.internalKeyComparator == &internalKeyComparator
        const bool owns_info_log_;
        const bool owns_cache_;
        const std::string dbname_; // 是dir的path

        // tableCache provides its own synchronization
        TableCache *const tableCache;

        // Lock over the persistent DB state.  Non-null iff successfully acquired.
        FileLock *db_lock_;

        // below is protected by mutex
        port::Mutex mutex;
        std::atomic<bool> shutting_down_;
        port::CondVar background_work_finished_signal_ GUARDED_BY(mutex);
        MemTable *memTable_;
        MemTable *immutableMemTable GUARDED_BY(mutex);  // Memtable being compacted
        std::atomic<bool> hasImmutableMemTable_;         // So bg thread can detect non-null immutableMemTable

        // 以下的是绑定的都在同个logFile上
        WritableFile *logFileWritable_; // 当前持有的logFile
        uint64_t logFileNumber_ GUARDED_BY(mutex);
        log::Writer *logWriter_;

        uint32_t seed_ GUARDED_BY(mutex);  // For sampling.

        // Queue of writers.
        std::deque<Writer *> writerDeque_ GUARDED_BY(mutex);
        WriteBatch *tmpWriteBatch_ GUARDED_BY(mutex);

        SnapshotList snapshots_ GUARDED_BY(mutex);

        // Set of table files to protect from deletion because they are part of ongoing compactions.
        std::set<uint64_t> pendingOutputFileNumberArr GUARDED_BY(mutex);

        // Has a background compaction been scheduled or is running?
        bool backgroundCompactionScheduled_ GUARDED_BY(mutex);

        ManualCompaction *manualCompaction_ GUARDED_BY(mutex); // 手动测试用的 实际用不到

        VersionSet *const versionSet GUARDED_BY(mutex);

        // Have we encountered a background error in paranoid mode?
        Status bg_error_ GUARDED_BY(mutex);

        CompactionStats compactionStatArr[config::kNumLevels] GUARDED_BY(mutex);
    };

    // Sanitize db options.  The caller should delete result.logger if it is not equal to src.logger.
    Options SanitizeOptions(const std::string &db,
                            const InternalKeyComparator *icmp,
                            const InternalFilterPolicy *ipolicy,
                            const Options &src);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_DB_IMPL_H_
