// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// The representation of a DBImpl consists of a set of Versions.  The
// newest version is called "current".  Older versions may be kept
// around to provide a consistent view to live iterators.
//
// Each Version keeps track of a set of Table files per level.  The
// entire set of versions is maintained in a VersionSet.
//
// Version,VersionSet are thread-compatible, but require external
// synchronization on all accesses.

#ifndef STORAGE_LEVELDB_DB_VERSION_SET_H_
#define STORAGE_LEVELDB_DB_VERSION_SET_H_

#include <map>
#include <set>
#include <vector>

#include "db/dbformat.h"
#include "db/version_edit.h"
#include "port/port.h"
#include "port/thread_annotations.h"

namespace leveldb {

    namespace log {
        class Writer;
    }

    class Compaction;

    class Iterator;

    class MemTable;

    class TableBuilder;

    class TableCache;

    class Version;

    class VersionSet;

    class WritableFile;

// Return the smallestInternalKey_ index i such that files[i]->largestInternalKey_ >= key.
// Return files.size() if there is no such file.
// REQUIRES: "files" contains a sorted list of non-overlapping files.
    int FindFile(const InternalKeyComparator &internalKeyComparator,
                 const std::vector<FileMetaData *> &files, const Slice &key);

// Returns true iff some file in "fileMetaDataVec" overlaps the user key range
// [*smallestInternalKey_,*largestInternalKey_].
// smallestInternalKey_==nullptr represents a key smaller than all keys in the DB.
// largestInternalKey_==nullptr represents a key largestInternalKey_ than all keys in the DB.
// REQUIRES: If disjoint_sorted_files, fileMetaDataVec[] contains disjoint ranges
//           in sorted order.
    bool SomeFileOverlapsRange(const InternalKeyComparator &icmp,
                               bool disjoint_sorted_files,
                               const std::vector<FileMetaData *> &fileMetaDataVec,
                               const Slice *smallest_user_key,
                               const Slice *largest_user_key);

    class Version {
    public:
        // Lookup the value for key.  If found, store it in *val and
        // return OK.  Else return a non-OK status.  Fills *stats.
        // REQUIRES: lock is not held
        struct GetStats {
            FileMetaData *seek_file;
            int seek_file_level;
        };

        // Append to *iters a sequence of iterators that will
        // yield the contents of this Version when merged together.
        // REQUIRES: This version has been saved (see VersionSet::SaveTo)
        void AddIterators(const ReadOptions &, std::vector<Iterator *> *iters);

        Status Get(const ReadOptions &,
                   const LookupKey &lookupKey,
                   std::string *val,
                   GetStats *stats);

        // Adds "stats" into the current state.  Returns true if a new
        // compaction_ may need to be triggered, false otherwise.
        // REQUIRES: lock is held
        bool UpdateStats(const GetStats &stats);

        // Record a sample of bytes read at the specified internal key.
        // Samples are taken approximately once every config::kReadBytesPeriod
        // bytes.  Returns true if a new compaction_ may need to be triggered.
        // REQUIRES: lock is held
        bool RecordReadSample(Slice key);

        // Reference count management (so Versions do not disappear out from
        // under live iterators)
        void Ref();

        void Unref();

        void GetOverlappingInputs(int level,
                                  const InternalKey *begin,  // nullptr means before all keys
                                  const InternalKey *end,    // nullptr means after all keys
                                  std::vector<FileMetaData *> *inputFileMetaDataVec);

        // Returns true iff some file in the specified level overlaps
        // some part of [*smallest_user_key,*largest_user_key].
        // smallest_user_key==nullptr represents a key smaller than all the DB's keys.
        // largest_user_key==nullptr represents a key largestInternalKey_ than all the DB's keys.
        bool OverlapInLevel(int level,
                            const Slice *smallest_user_key,
                            const Slice *largest_user_key);

        // Return the level at which we should place a new memtable compaction_
        // result that covers the range [smallest_user_key,largest_user_key].
        int PickLevelForMemTableOutput(const Slice &smallest_user_key,
                                       const Slice &largest_user_key);

        int NumFiles(int level) const {
            return fileMetaDataVecArr[level].size();
        }

        // Return a human readable string that describes this version's contents.
        std::string DebugString() const;

    private:
        friend class Compaction;

        friend class VersionSet;

        class LevelFileNumIterator;

        explicit Version(VersionSet *versionSet) : belongingVersionSet(versionSet),
                                                   next_(this),
                                                   prev_(this),
                                                   refs_(0),
                                                   file_to_compact_(nullptr),
                                                   fileToCompactLevel_(-1),
                                                   compactionScore_(-1),
                                                   compactionLevel_(-1) {}

        Version(const Version &) = delete;

        Version &operator=(const Version &) = delete;

        ~Version();

        Iterator *NewConcatenatingIterator(const ReadOptions &, int level) const;

        // Call func(arg, level, f) for every file that overlaps userKey in
        // order from newest to oldest.  If an invocation of func returns
        // false, makes no more calls.
        //
        // REQUIRES: user portion of internalKey == userKey.
        void ForEachOverlapping(Slice userKey, Slice internalKey, void *arg,
                                bool (*func)(void *, int, FileMetaData *));

        VersionSet *belongingVersionSet;  // VersionSet to which this Version belongs
        Version *next_;     // Next version in linked list
        Version *prev_;     // Previous version in linked list
        int refs_;          // Number of live refs to this version

        // list of files per level
        std::vector<FileMetaData *> fileMetaDataVecArr[config::kNumLevels];

        // next file to compact based on seek stats.
        FileMetaData *file_to_compact_;
        int fileToCompactLevel_;

        // Level that should be compacted next and its compaction_ score.
        // Score < 1 means compaction_ is not strictly needed.  These fields are initialized by Finalize().
        double compactionScore_;
        int compactionLevel_;
    };

    class VersionSet {
    public:
        VersionSet(std::string dbname,
                   const Options *options,
                   TableCache *tableCache,
                   const InternalKeyComparator *);

        VersionSet(const VersionSet &) = delete;

        VersionSet &operator=(const VersionSet &) = delete;

        ~VersionSet();

        // Apply *versionEdit to the current version to form a new descriptor that
        // is both saved to persistent state and installed as the new
        // current version.  Will release *mutex while actually writing to the file.
        // REQUIRES: *mutex is held on entry.
        // REQUIRES: no other thread concurrently calls LogAndApply()
        Status LogAndApply(VersionEdit *versionEdit, port::Mutex *mutex) EXCLUSIVE_LOCKS_REQUIRED(mu);

        // Recover the last saved descriptor from persistent storage.
        Status Recover(bool *save_manifest);

        // Return the current version.
        Version *current() const {
            return currentVersion_;
        }

        // return the current manifest file number
        uint64_t ManifestFileNumber() const {
            return manifest_file_number_;
        }

        // Allocate and return a new file number
        uint64_t NewFileNumber() {
            return next_file_number_++;
        }

        // Arrange to reuse "file_number" unless a newer file number has
        // already been allocated.
        // REQUIRES: "file_number" was returned by a call to NewFileNumber().
        void ReuseFileNumber(uint64_t file_number) {
            if (next_file_number_ == file_number + 1) {
                next_file_number_ = file_number;
            }
        }

        // Return the number of Table files at the specified level.
        int NumLevelFiles(int level) const;

        // Return the combined file size of all files at the specified level.
        int64_t NumLevelBytes(int level) const;

        // Return the last sequence number.
        uint64_t LastSequence() const { return last_sequence_; }

        // Set the last sequence number to s.
        void SetLastSequence(uint64_t s) {
            assert(s >= last_sequence_);
            last_sequence_ = s;
        }

        // Mark the specified file number as used.
        void MarkFileNumberUsed(uint64_t number);

        // Return the current log file number.
        uint64_t LogNumber() const {
            return log_number_;
        }

        // Return the log file number for the log file that is currently
        // being compacted, or zero if there is no such log file.
        uint64_t PrevLogNumber() const { return prev_log_number_; }

        // Pick level and inputs for a new compaction_.
        // Returns nullptr if there is no compaction_ to be done.
        // Otherwise returns a pointer to a heap-allocated object that
        // describes the compaction_.  Caller should delete the result.
        Compaction *PickCompaction();

        // Return a compaction_ object for compacting the range [begin,end] in
        // the specified level.  Returns nullptr if there is nothing in that
        // level that overlaps the specified range.  Caller should delete
        // the result.
        Compaction *CompactRange(int level, const InternalKey *begin,
                                 const InternalKey *end);

        // Return the maximum overlapping data (in bytes) at next level for any
        // file at a level >= 1.
        int64_t MaxNextLevelOverlappingBytes();

        Iterator *MakeInputIterator(Compaction *compaction);

        // Returns true iff some level needs a compaction_.
        bool NeedsCompaction() const {
            Version *v = currentVersion_;
            return (v->compactionScore_ >= 1) || (v->file_to_compact_ != nullptr);
        }

        // Add all files listed in any live version to *live.
        // May also mutate some internal state.
        void AddLiveFiles(std::set<uint64_t> *live);

        // Return the approximate offset in the database of the data for
        // "key" as of version "v".
        uint64_t ApproximateOffsetOf(Version *v, const InternalKey &key);

        // Return a human-readable short (single-line) summary of the number
        // of files per level.  Uses *scratch as backing store.
        struct LevelSummaryStorage {
            char buffer[100];
        };

        const char *LevelSummary(LevelSummaryStorage *scratch) const;

    private:
        class Builder;

        friend class Compaction;

        friend class Version;

        bool ReuseManifest(const std::string &dscname, const std::string &dscbase);

        // 决定要compact哪个level上的ldb
        void Finalize(Version *version);

        void GetRange(const std::vector<FileMetaData *> &inputFileMetaDataVec, InternalKey *smallest,
                      InternalKey *largest);

        void GetRange2(const std::vector<FileMetaData *> &input1,
                       const std::vector<FileMetaData *> &input2,
                       InternalKey *smallest, InternalKey *largest);

        void SetupOtherInputs(Compaction *compaction);

        // save current contents to *writer
        Status WriteSnapshot(log::Writer *writer);

        void AppendVersion(Version *version);

        Env *const env_;
        const std::string dbname_;
        const Options *const options_;
        TableCache *const table_cache_;
        const InternalKeyComparator internalKeyComparator_;

        // 以下的5个是versionEdit的
        uint64_t manifest_file_number_;
        uint64_t next_file_number_; // manifest log ldb 文件公用的计数器
        uint64_t last_sequence_;
        uint64_t log_number_;
        uint64_t prev_log_number_;  // 0 or backing store for memtable being compacted

        // 对应的是manifest
        WritableFile *descriptorFile_;
        // 对应的是manifest的writer
        log::Writer *descriptorLog_;

        Version dummy_versions_;  // Head of circular doubly-linked list of versions.
        Version *currentVersion_; // == dummy_versions_.prev_

        // per-level key at which the next compaction_ at the level should start.
        // Either an empty string, or a valid InternalKey.
        std::string compactPointer_[config::kNumLevels];
    };

    // information about a compaction_.
    class Compaction {
    public:
        ~Compaction();

        // Return the level that is being compacted.  Inputs from "level"
        // and "level+1" will be merged to produce a set of "level+1" files.
        int level() const {
            return level_;
        }

        // Return the object that holds the edits to the descriptor done by this compaction_.
        VersionEdit *edit() {
            return &versionEdit_;
        }

        // "which" must be either 0 or 1
        int num_input_files(int which) const {
            return inputFileMetaDataVecArr_[which].size();
        }

        // Return the ith input file at "level()+which" ("which" must be 0 or 1).
        FileMetaData *input(int which, int i) const {
            return inputFileMetaDataVecArr_[which][i];
        }

        // Maximum size of files to build during this compaction_.
        uint64_t MaxOutputFileSize() const {
            return max_output_file_size_;
        }

        // Is this a trivial compaction_ that can be implemented by just
        // moving a single input file to the next level (no merging or splitting)
        bool IsTrivialMove() const;

        // Add all inputs to this compaction_ as delete operations to *edit.
        void AddInputDeletions(VersionEdit *edit);

        // true if the information we have available guarantees that
        // the compaction_ is producing data in "level+1" for which no data exists in levels greater than "level+1"
        // a
        // userKey不会落在level+2和以上的fileMeta
        bool IsBaseLevelForKey(const Slice &userKey);

        bool ShouldStopBefore(const Slice &internalKey);

        // release the input version for the compaction_, once the compaction_ is successful.
        void ReleaseInputs();

    private:
        friend class Version;

        friend class VersionSet;

        Compaction(const Options *options, int level);

        int level_;
        uint64_t max_output_file_size_;
        Version *inputVersion_;
        VersionEdit versionEdit_;

        // each compaction_ reads inputs from "level_" and "level_+1" 两套的input
        std::vector<FileMetaData *> inputFileMetaDataVecArr_[2];

        // State used to check for number of overlapping grandparent files
        // (parent == level_ + 1, grandparent == level_ + 2)
        std::vector<FileMetaData *> grandparentFileMetaVac_;
        size_t grandparentIndex_;  // Index in grandparent_starts_
        bool seenKey_;             // Some output key has been seen
        int64_t overlappedByteLen_;  // Bytes of overlap between current output and grandparent files

        // 用来 IsBaseLevelForKey()
        // 记录了对应的version的fileMetaVecArr各个level上的vec的index
        // level_ptrs_ holds indices into inputVersion_->levelStateArr: our state
        // is that we are positioned at one of the file ranges for each
        // higher level than the ones involved in this compaction_ (i.e. for all L >= level_ + 2).
        size_t level_ptrs_[config::kNumLevels];
    };

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_VERSION_SET_H_
