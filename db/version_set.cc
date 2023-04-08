// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_set.h"

#include <algorithm>
#include <cstdio>

#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "leveldb/env.h"
#include "leveldb/table_builder.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"

namespace leveldb {

    static size_t TargetFileSize(const Options *options) {
        return options->maxFileSize;
    }

// Maximum bytes of overlaps in grandparent (i.e., level+2) before we
// stop building a single file in a level->level+1 compaction_.
    static int64_t MaxGrandParentOverlapBytes(const Options *options) {
        return 10 * TargetFileSize(options);
    }

    // Maximum number of bytes in all compacted files.  We avoid expanding
    // the lower level file set of a compaction_ if it would make the
    // total compaction_ cover more than this many bytes.
    static int64_t ExpandedCompactionByteSizeLimit(const Options *options) {
        return 25 * TargetFileSize(options);
    }

    // Note: the result for level zero is not really used since we set
    // the level-0 compaction_ threshold based on number of files.
    static double MaxBytesForLevel(const Options *options, int level) {
        // result for both level 0 and level 1
        double result = 10. * 1048576.0;
        while (level > 1) {
            result *= 10;
            level--;
        }
        return result;
    }

    static uint64_t MaxFileSizeForLevel(const Options *options, int level) {
        // We could vary per level to reduce number of files?
        return TargetFileSize(options);
    }

    static int64_t TotalFileSize(const std::vector<FileMetaData *> &fileMetaDataVec) {
        int64_t sum = 0;
        for (auto fileMetaData: fileMetaDataVec) {
            sum += fileMetaData->fileSize_;
        }
        return sum;
    }

    Version::~Version() {
        assert(refs_ == 0);

        // remove from linked list
        prev_->next_ = next_;
        next_->prev_ = prev_;

        // drop references to files
        for (auto &level: fileMetaDataVecArr) {
            for (auto f: level) {
                assert(f->refs > 0);
                f->refs--;
                if (f->refs <= 0) {
                    delete f;
                }
            }
        }
    }

    int FindFile(const InternalKeyComparator &internalKeyComparator,
                 const std::vector<FileMetaData *> &files,
                 const Slice &key) {
        uint32_t left = 0;
        uint32_t right = files.size();
        while (left < right) {
            uint32_t mid = (left + right) / 2;
            const FileMetaData *f = files[mid];
            if (internalKeyComparator.InternalKeyComparator::Compare(f->largestInternalKey_.Encode(), key) < 0) {
                // Key at "mid.largestInternalKey_" is < "target".  Therefore all
                // files at or before "mid" are uninteresting.
                left = mid + 1;
            } else {
                // Key at "mid.largestInternalKey_" is >= "target".  Therefore all files
                // after "mid" are uninteresting.
                right = mid;
            }
        }
        return right;
    }

    static bool AfterFile(const Comparator *ucmp, const Slice *user_key,
                          const FileMetaData *f) {
        // null userKey occurs before all keys and is therefore never after *f
        return (user_key != nullptr &&
                ucmp->Compare(*user_key, f->largestInternalKey_.userKey()) > 0);
    }

    static bool BeforeFile(const Comparator *ucmp, const Slice *user_key,
                           const FileMetaData *f) {
        // null userKey occurs after all keys and is therefore never before *f
        return (user_key != nullptr &&
                ucmp->Compare(*user_key, f->smallestInternalKey_.userKey()) < 0);
    }

    bool SomeFileOverlapsRange(const InternalKeyComparator &icmp,
                               bool disjoint_sorted_files,
                               const std::vector<FileMetaData *> &fileMetaDataVec,
                               const Slice *smallest_user_key,
                               const Slice *largest_user_key) {
        const Comparator *ucmp = icmp.user_comparator();
        if (!disjoint_sorted_files) {
            // Need to check against all fileMetaDataVec
            for (auto fileMetaData: fileMetaDataVec) {
                if (AfterFile(ucmp, smallest_user_key, fileMetaData) ||
                    BeforeFile(ucmp, largest_user_key, fileMetaData)) {
                    // No overlap
                    continue;
                }

                return true;  // Overlap
            }

            return false;
        }

        // Binary search over file list
        uint32_t index = 0;
        if (smallest_user_key != nullptr) {
            // Find the earliest possible internal key for smallest_user_key
            InternalKey small_key(*smallest_user_key, kMaxSequenceNumber, kValueTypeForSeek);
            index = FindFile(icmp, fileMetaDataVec, small_key.Encode());
        }

        if (index >= fileMetaDataVec.size()) {
            // beginning of range is after all fileMetaDataVec, so no overlap.
            return false;
        }

        return !BeforeFile(ucmp, largest_user_key, fileMetaDataVec[index]);
    }

    // An internal iterator.
    // For a given version/level pair, yields information about the files in the level.
    // For a given entry, key() is the largestInternalKey_ key that occurs in the file, and value() is an
    // 16-byte value containing the file number and file size, both
    // encoded using EncodeFixed64.
    class Version::LevelFileNumIterator : public Iterator {
    public:
        LevelFileNumIterator(const InternalKeyComparator &icmp,
                             const std::vector<FileMetaData *> *flist)
                : icmp_(icmp), flist_(flist), index_(flist->size()) {  // Marks as invalid
        }

        bool Valid() const override { return index_ < flist_->size(); }

        void Seek(const Slice &target) override {
            index_ = FindFile(icmp_, *flist_, target);
        }

        void SeekToFirst() override { index_ = 0; }

        void SeekToLast() override {
            index_ = flist_->empty() ? 0 : flist_->size() - 1;
        }

        void Next() override {
            assert(Valid());
            index_++;
        }

        void Prev() override {
            assert(Valid());
            if (index_ == 0) {
                index_ = flist_->size();  // Marks as invalid
            } else {
                index_--;
            }
        }

        Slice key() const override {
            assert(Valid());
            return (*flist_)[index_]->largestInternalKey_.Encode();
        }

        Slice value() const override {
            assert(Valid());
            EncodeFixed64(value_buf_, (*flist_)[index_]->number);
            EncodeFixed64(value_buf_ + 8, (*flist_)[index_]->fileSize_);
            return Slice(value_buf_, sizeof(value_buf_));
        }

        Status status() const override { return Status::OK(); }

    private:
        const InternalKeyComparator icmp_;
        const std::vector<FileMetaData *> *const flist_;
        uint32_t index_;

        // Backing store for value().  Holds the file number and size.
        mutable char value_buf_[16];
    };

    static Iterator *GetFileIterator(void *arg, const ReadOptions &options,
                                     const Slice &file_value) {
        TableCache *cache = reinterpret_cast<TableCache *>(arg);
        if (file_value.size() != 16) {
            return NewErrorIterator(
                    Status::Corruption("FileReader invoked with unexpected value"));
        } else {
            return cache->NewIterator(options, DecodeFixed64(file_value.data()),
                                      DecodeFixed64(file_value.data() + 8));
        }
    }

    Iterator *Version::NewConcatenatingIterator(const ReadOptions &options,
                                                int level) const {
        return NewTwoLevelIterator(
                new LevelFileNumIterator(belongingVersionSet->internalKeyComparator_, &fileMetaDataVecArr[level]),
                &GetFileIterator,
                belongingVersionSet->table_cache_, options);
    }

    void Version::AddIterators(const ReadOptions &options,
                               std::vector<Iterator *> *iters) {
        // Merge all level zero files together since they may overlap
        for (size_t i = 0; i < fileMetaDataVecArr[0].size(); i++) {
            iters->push_back(belongingVersionSet->table_cache_->NewIterator(
                    options, fileMetaDataVecArr[0][i]->number, fileMetaDataVecArr[0][i]->fileSize_));
        }

        // For levels > 0, we can use a concatenating iterator that sequentially
        // walks through the non-overlapping files in the level, opening them
        // lazily.
        for (int level = 1; level < config::kNumLevels; level++) {
            if (!fileMetaDataVecArr[level].empty()) {
                iters->push_back(NewConcatenatingIterator(options, level));
            }
        }
    }

    // Callback from TableCache::Get()
    namespace {
        enum SaverState {
            kNotFound,
            kFound,
            kDeleted,
            kCorrupt,
        };
        struct Saver {
            SaverState state;
            const Comparator *ucmp;
            Slice user_key;
            std::string *value;
        };
    }  // namespace

    static void SaveValue(void *arg, const Slice &internalKey, const Slice &value) {
        auto *saver = reinterpret_cast<Saver *>(arg);
        ParsedInternalKey parsed_key;
        if (!ParseInternalKey(internalKey, &parsed_key)) {
            saver->state = kCorrupt;
        } else {
            if (saver->ucmp->Compare(parsed_key.userKey, saver->user_key) == 0) {
                saver->state = (parsed_key.type == kTypeValue) ? kFound : kDeleted;
                if (saver->state == kFound) {
                    saver->value->assign(value.data(), value.size());
                }
            }
        }
    }

    // 比较number
    static bool NewestFirst(FileMetaData *a, FileMetaData *b) {
        return a->number > b->number;
    }

    void Version::ForEachOverlapping(Slice userKey,
                                     Slice internalKey,
                                     void *arg,
                                     bool (*func)(void * , int level, FileMetaData *)) {
        const Comparator *userComparator = belongingVersionSet->internalKeyComparator_.user_comparator();

        // 搜索 level-0 in order from newest to oldest.
        std::vector<FileMetaData *> tmp;
        tmp.reserve(fileMetaDataVecArr[0].size());

        // level0
        for (auto fileMeta : fileMetaDataVecArr[0]) {
            if (userComparator->Compare(userKey, fileMeta->smallestInternalKey_.userKey()) >= 0 &&
                userComparator->Compare(userKey, fileMeta->largestInternalKey_.userKey()) <= 0) {
                tmp.push_back(fileMeta);
            }
        }

        if (!tmp.empty()) {
            std::sort(tmp.begin(), tmp.end(), NewestFirst);

            for (auto & fileMeta : tmp) {
                if (!func(arg, 0, fileMeta)) {
                    return;
                }
            }
        }

        // 搜索其它的level
        for (int level = 1; level < config::kNumLevels; level++) {
            size_t num_files = fileMetaDataVecArr[level].size();
            if (num_files == 0) continue;

            // Binary search to find earliest index whose largestInternalKey_ key >= internalKey.
            uint32_t index = FindFile(belongingVersionSet->internalKeyComparator_, fileMetaDataVecArr[level],
                                      internalKey);
            if (index < num_files) {
                FileMetaData *f = fileMetaDataVecArr[level][index];
                if (userComparator->Compare(userKey, f->smallestInternalKey_.userKey()) < 0) {
                    // All of "f" is past any data for userKey
                } else {
                    if (!func(arg, level, f)) {
                        return;
                    }
                }
            }
        }
    }


    Status Version::Get(const ReadOptions &readOptions,
                        const LookupKey &lookupKey,
                        std::string *value,
                        GetStats *stats) {
        stats->seek_file = nullptr;
        stats->seek_file_level = -1;

        struct State {
            Saver saver;
            GetStats *stats;
            const ReadOptions *options;
            Slice ikey;
            FileMetaData *last_file_read;
            int last_file_read_level;

            VersionSet *vset;
            Status s;
            bool found;

            // true说明还要再去寻找
            static bool Match(void *arg, int level, FileMetaData *fileMeta) {
                auto *state = reinterpret_cast<State *>(arg);

                if (state->stats->seek_file == nullptr &&
                    state->last_file_read != nullptr) {
                    // We have had more than one seek for this read.  Charge the 1st file.
                    state->stats->seek_file = state->last_file_read;
                    state->stats->seek_file_level = state->last_file_read_level;
                }

                state->last_file_read = fileMeta;
                state->last_file_read_level = level;

                state->s = state->vset->table_cache_->Get(*state->options,
                                                          fileMeta->number,
                                                          fileMeta->fileSize_,
                                                          state->ikey,
                                                          &state->saver,
                                                          SaveValue);
                if (!state->s.ok()) {
                    state->found = true;
                    return false;
                }

                switch (state->saver.state) {
                    case kNotFound:
                        return true;  // Keep searching in other files
                    case kFound:
                        state->found = true;
                        return false;
                    case kDeleted:
                        return false;
                    case kCorrupt:
                        state->s = Status::Corruption("corrupted key for ", state->saver.user_key);
                        state->found = true;
                        return false;
                }

                // Not reached. Added to avoid false compilation warnings of
                // "control reaches end of non-void function".
                return false;
            }
        };

        State state;
        state.found = false;
        state.stats = stats;
        state.last_file_read = nullptr;
        state.last_file_read_level = -1;

        state.options = &readOptions;
        state.ikey = lookupKey.internal_key();
        state.vset = belongingVersionSet;

        state.saver.state = kNotFound;
        state.saver.ucmp = belongingVersionSet->internalKeyComparator_.user_comparator();
        state.saver.user_key = lookupKey.user_key();
        state.saver.value = value;

        ForEachOverlapping(state.saver.user_key,
                           state.ikey,
                           &state,
                           &State::Match);

        return state.found ? state.s : Status::NotFound(Slice());
    }

    bool Version::UpdateStats(const GetStats &stats) {
        FileMetaData *fileMetaData = stats.seek_file;
        if (fileMetaData != nullptr) {
            fileMetaData->allowedSeeks_--;

            if (fileMetaData->allowedSeeks_ <= 0 && file_to_compact_ == nullptr) {
                file_to_compact_ = fileMetaData;
                fileToCompactLevel_ = stats.seek_file_level;
                return true;
            }
        }

        return false;
    }

    bool Version::RecordReadSample(Slice internal_key) {
        ParsedInternalKey ikey;
        if (!ParseInternalKey(internal_key, &ikey)) {
            return false;
        }

        struct State {
            GetStats stats;  // Holds first matching file
            int matches;

            static bool Match(void *arg, int level, FileMetaData *f) {
                State *state = reinterpret_cast<State *>(arg);
                state->matches++;
                if (state->matches == 1) {
                    // Remember first match.
                    state->stats.seek_file = f;
                    state->stats.seek_file_level = level;
                }
                // We can stop iterating once we have a second match.
                return state->matches < 2;
            }
        };

        State state;
        state.matches = 0;
        ForEachOverlapping(ikey.userKey, internal_key, &state, &State::Match);

        // Must have at least two matches since we want to merge across
        // files. But what if we have a single file that contains many
        // overwrites and deletions?  Should we have another mechanism for
        // finding such files?
        if (state.matches >= 2) {
            // 1MB cost is about 1 seek (see comment in Builder::Apply).
            return UpdateStats(state.stats);
        }
        return false;
    }

    void Version::Ref() {
        ++refs_;
    }

    void Version::Unref() {
        assert(this != &belongingVersionSet->dummy_versions_);
        assert(refs_ >= 1);
        --refs_;
        if (refs_ == 0) {
            delete this;
        }
    }

    bool Version::OverlapInLevel(int level,
                                 const Slice *smallest_user_key,
                                 const Slice *largest_user_key) {
        return SomeFileOverlapsRange(belongingVersionSet->internalKeyComparator_,
                                     level > 0,
                                     fileMetaDataVecArr[level],
                                     smallest_user_key,
                                     largest_user_key);
    }

    int Version::PickLevelForMemTableOutput(const Slice &smallest_user_key,
                                            const Slice &largest_user_key) {
        int level = 0;
        if (!OverlapInLevel(0, &smallest_user_key, &largest_user_key)) {
            // Push to next level if there is no overlap in next level,
            // and the #bytes overlapping in the level after that are limited.
            InternalKey start(smallest_user_key, kMaxSequenceNumber, kValueTypeForSeek);
            InternalKey limit(largest_user_key, 0, static_cast<ValueType>(0));
            std::vector<FileMetaData *> overlaps;

            while (level < config::kMaxMemCompactLevel) {
                if (OverlapInLevel(level + 1, &smallest_user_key, &largest_user_key)) {
                    break;
                }

                if (level + 2 < config::kNumLevels) {
                    // Check that file does not overlap too many grandparent bytes.
                    GetOverlappingInputs(level + 2, &start, &limit, &overlaps);
                    const int64_t sum = TotalFileSize(overlaps);
                    if (sum > MaxGrandParentOverlapBytes(belongingVersionSet->options_)) {
                        break;
                    }
                }

                level++;
            }
        }

        return level;
    }

    // Store in "*inputFileMetaDataVec" all files in "level" that overlap [begin,end]
    void Version::GetOverlappingInputs(int level,
                                       const InternalKey *begin,
                                       const InternalKey *end,
                                       std::vector<FileMetaData *> *inputFileMetaDataVec) {
        assert(level >= 0);
        assert(level < config::kNumLevels);

        inputFileMetaDataVec->clear();

        // userKey是和internalKey相对的
        Slice userKeyBegin;
        Slice userKeyEnd;

        if (begin != nullptr) {
            userKeyBegin = begin->userKey();
        }

        if (end != nullptr) {
            userKeyEnd = end->userKey();
        }

        const Comparator *userKeyComparator = belongingVersionSet->internalKeyComparator_.user_comparator();

        for (size_t i = 0; i < fileMetaDataVecArr[level].size();) {
            FileMetaData *fileMetaData = fileMetaDataVecArr[level][i++];

            const Slice userKeyBeginInFile = fileMetaData->smallestInternalKey_.userKey();
            const Slice userKeyEndInFile = fileMetaData->largestInternalKey_.userKey();

            // ldb的终点 < userKeyBegin 说明没有overlapping
            if (begin != nullptr && userKeyComparator->Compare(userKeyEndInFile, userKeyBegin) < 0) {
                continue;
            }

            // ldb的起点 > userKeyEnd 说明没有overlapping
            if (end != nullptr && userKeyComparator->Compare(userKeyBeginInFile, userKeyEnd) > 0) {
                continue;
            }

            inputFileMetaDataVec->push_back(fileMetaData);

            // Level-0 files may overlap each other,
            // if the newly added file has expanded the range , restart search.
            if (level == 0) {

                // userKeyBeginInFile  userKeyBegin
                if (begin != nullptr &&
                    userKeyComparator->Compare(userKeyBeginInFile, userKeyBegin) < 0) {
                    userKeyBegin = userKeyBeginInFile;

                    // restart
                    inputFileMetaDataVec->clear();
                    i = 0;
                } else if (end != nullptr &&
                           userKeyComparator->Compare(userKeyEndInFile, userKeyEnd) > 0) { //userKeyEnd userKeyEndInFile
                    userKeyEnd = userKeyEndInFile;

                    // restart
                    inputFileMetaDataVec->clear();
                    i = 0;
                }
            }
        }
    }

    std::string Version::DebugString() const {
        std::string r;
        for (int level = 0; level < config::kNumLevels; level++) {
            // E.g.,
            //   --- level 1 ---
            //   17:123['a' .. 'd']
            //   20:43['e' .. 'g']
            r.append("--- level ");
            AppendNumberTo(&r, level);
            r.append(" ---\n");
            const std::vector<FileMetaData *> &files = fileMetaDataVecArr[level];
            for (size_t i = 0; i < files.size(); i++) {
                r.push_back(' ');
                AppendNumberTo(&r, files[i]->number);
                r.push_back(':');
                AppendNumberTo(&r, files[i]->fileSize_);
                r.append("[");
                r.append(files[i]->smallestInternalKey_.DebugString());
                r.append(" .. ");
                r.append(files[i]->largestInternalKey_.DebugString());
                r.append("]\n");
            }
        }
        return r;
    }

    // A helper class so we can efficiently apply a whole sequence
    // of edits to a particular state without creating intermediate
    // Versions that contain full copies of the intermediate state.
    class VersionSet::Builder {
    private:
        // helper to sort by v->fileMetaDataVecArr[file_number].smallestInternalKey_
        struct BySmallestKey {
            const InternalKeyComparator *internalKeyComparator;

            bool operator()(FileMetaData *f1, FileMetaData *f2) const {
                int r = internalKeyComparator->Compare(f1->smallestInternalKey_, f2->smallestInternalKey_);
                if (r != 0) {
                    return (r < 0);
                }

                // Break ties by file number
                return (f1->number < f2->number);
            }
        };

        typedef std::set<FileMetaData *, BySmallestKey> FileSet;
        struct LevelState {
            std::set<uint64_t> deletedFileNumbers;
            FileSet *addedFileMetaDataSet_;
        };

        VersionSet *versionSet_;
        Version *baseVersion_;
        LevelState levelStateArr[config::kNumLevels];

    public:
        // initialize a tableBuilder_ with the files from *baseVersion and other info from *belongingVersionSet
        Builder(VersionSet *versionSet, Version *baseVersion) : versionSet_(versionSet),
                                                                baseVersion_(baseVersion) {
            baseVersion_->Ref();
            BySmallestKey bySmallestKey{};
            bySmallestKey.internalKeyComparator = &versionSet->internalKeyComparator_;
            for (auto &levelState: levelStateArr) {
                levelState.addedFileMetaDataSet_ = new FileSet(bySmallestKey);
            }
        }

        ~Builder() {
            for (auto &level: levelStateArr) {
                const FileSet *added = level.addedFileMetaDataSet_;
                std::vector<FileMetaData *> to_unref;
                to_unref.reserve(added->size());
                for (auto it: *added) {
                    to_unref.push_back(it);
                }
                delete added;
                for (auto f: to_unref) {
                    f->refs--;
                    if (f->refs <= 0) {
                        delete f;
                    }
                }
            }
            baseVersion_->Unref();
        }

        // 吸收 versionEdit 主要是对各level对应的levelState的
        void Apply(VersionEdit *versionEdit) {
            // update compactPointer_
            for (auto &level_internalKey: versionEdit->compact_pointers_) {
                const int level = level_internalKey.first;
                versionSet_->compactPointer_[level] = level_internalKey.second.Encode().ToString();
            }

            // delete files
            for (const auto &pair: versionEdit->deletedLevelFileNumberSet_) {
                const int level = pair.first;
                const uint64_t fileNumber = pair.second;
                levelStateArr[level].deletedFileNumbers.insert(fileNumber);
            }

            // new files
            for (auto &pair: versionEdit->addedLevelFileMetaDataVec_) {
                const int level = pair.first;

                auto *fileMetaData = new FileMetaData(pair.second);
                fileMetaData->refs = 1;

                // arrange to automatically compact this file after a certain number of seeks.  Let's assume:
                //   (1) One seek costs 10ms
                //   (2) Writing or reading 1MB costs 10ms (100MB/s)
                //   (3) A compaction_ of 1MB does 25MB of IO:
                //         1MB read from this level
                //         10-12MB read from next level (boundaries may be misaligned)
                //         10-12MB written to next level
                // This implies that 25 seeks cost the same as the compaction_ of 1MB of data.
                // I.e., one seek costs approximately the same as the compaction_ of 40KB of data
                // We are a little conservative and allow approximately one seek for every 16KB
                // of data before triggering a compaction_.
                fileMetaData->allowedSeeks_ = static_cast<int>((fileMetaData->fileSize_ / 16384U));
                if (fileMetaData->allowedSeeks_ < 100) {
                    fileMetaData->allowedSeeks_ = 100;
                }

                levelStateArr[level].deletedFileNumbers.erase(fileMetaData->number);
                levelStateArr[level].addedFileMetaDataSet_->insert(fileMetaData);
            }
        }

        // save the current state into version
        void SaveTo(Version *version) {
            BySmallestKey bySmallestKey{};
            bySmallestKey.internalKeyComparator = &versionSet_->internalKeyComparator_;

            // 遍历了各个level
            for (int level = 0; level < config::kNumLevels; level++) {
                // Merge the set of added files with the set of pre-existing files.
                // drop any deleted files.  Store the result in *version.
                const std::vector<FileMetaData *> &fileMetaDataVec = baseVersion_->fileMetaDataVecArr[level];

                auto baseIter = fileMetaDataVec.begin();
                auto baseEnd = fileMetaDataVec.end();

                const FileSet *addedFileMetaDataSet = levelStateArr[level].addedFileMetaDataSet_;

                version->fileMetaDataVecArr[level].reserve(fileMetaDataVec.size() + addedFileMetaDataSet->size());

                // 遍历了各个level上的新add的
                for (const auto &addedFileMetaData: *addedFileMetaDataSet) {
                    // add all smaller files listed in baseVersion_
                    auto upperBound = std::upper_bound(baseIter,
                                                       baseEnd,
                                                       addedFileMetaData,
                                                       bySmallestKey);

                    // 把baseVersion_中比它小的都add
                    for (; baseIter != upperBound; ++baseIter) {
                        MaybeAddFile(version, level, *baseIter);
                    }

                    // add 它的本身
                    MaybeAddFile(version, level, addedFileMetaData);
                }

                // 添加剩下的比较大的
                for (; baseIter != baseEnd; ++baseIter) {
                    MaybeAddFile(version, level, *baseIter);
                }

#ifndef NDEBUG
                if (level == 0) {
                    continue;
                }

                // 确保是没有overlap 的 when level > 0
                for (uint32_t i = 1; i < version->fileMetaDataVecArr[level].size(); i++) {
                    const InternalKey &prevEnd = version->fileMetaDataVecArr[level][i - 1]->largestInternalKey_;
                    const InternalKey &thisBegin = version->fileMetaDataVecArr[level][i]->smallestInternalKey_;

                    if (versionSet_->internalKeyComparator_.Compare(prevEnd, thisBegin) >= 0) {
                        std::fprintf(stderr,
                                     "overlapping ranges in same level %s vs. %s\n",
                                     prevEnd.DebugString().c_str(),
                                     thisBegin.DebugString().c_str());

                        ::abort();
                    }
                }
#endif
            }
        }

        // 添加到了 version->fileMetaDataVecArr[level]
        void MaybeAddFile(Version *version, int level, FileMetaData *fileMetaData) {
            // deleted
            if (levelStateArr[level].deletedFileNumbers.count(fileMetaData->number) > 0) {
                return;
            }

            std::vector<FileMetaData *> *fileMetaDataVec = &version->fileMetaDataVecArr[level];

            // 不能 overlap 当 level
            if (level > 0 && !fileMetaDataVec->empty()) {
                assert(versionSet_->internalKeyComparator_.Compare(
                        (*fileMetaDataVec)[fileMetaDataVec->size() - 1]->largestInternalKey_,
                        fileMetaData->smallestInternalKey_) < 0);
            }

            fileMetaData->refs++;
            fileMetaDataVec->push_back(fileMetaData);
        }
    };

    VersionSet::VersionSet(std::string dbname,
                           const Options *options,
                           TableCache *tableCache,
                           const InternalKeyComparator *cmp) : env_(options->env),
                                                               dbname_(std::move(dbname)),
                                                               options_(options),
                                                               table_cache_(tableCache),
                                                               internalKeyComparator_(*cmp),
                                                               next_file_number_(2),
                                                               manifest_file_number_(0),  // Filled by Recover()
                                                               last_sequence_(0),
                                                               log_number_(0),
                                                               prev_log_number_(0),
                                                               descriptorFile_(nullptr),
                                                               descriptorLog_(nullptr),
                                                               dummy_versions_(this),
                                                               currentVersion_(nullptr) {
        AppendVersion(new Version(this));
    }

    VersionSet::~VersionSet() {
        currentVersion_->Unref();
        assert(dummy_versions_.next_ == &dummy_versions_);  // List must be empty
        delete descriptorLog_;
        delete descriptorFile_;
    }

    // 把version添加到链表,make version current
    void VersionSet::AppendVersion(Version *version) {
        assert(version->refs_ == 0);
        assert(version != currentVersion_);

        if (currentVersion_ != nullptr) {
            currentVersion_->Unref();
        }

        currentVersion_ = version;
        version->Ref();

        // append to linked list 变成如下
        // oldPrev - version - dummy
        version->prev_ = dummy_versions_.prev_;
        version->next_ = &dummy_versions_;
        version->prev_->next_ = version;
        version->next_->prev_ = version;
    }

    Status VersionSet::LogAndApply(VersionEdit *versionEdit, port::Mutex *mutex) {
        if (versionEdit->has_log_number_) {
            assert(versionEdit->log_number_ >= log_number_);
            assert(versionEdit->log_number_ < next_file_number_);
        } else {
            versionEdit->SetLogNumber(log_number_);
        }

        if (!versionEdit->has_prev_log_number_) {
            versionEdit->SetPrevLogNumber(prev_log_number_);
        }

        versionEdit->SetNextFile(next_file_number_);
        versionEdit->SetLastSequence(last_sequence_);

        // 生成新version
        auto *version = new Version(this);
        {
            Builder builder(this, currentVersion_);
            // builder吸收versionEdit
            builder.Apply(versionEdit);
            // 当前的version融合versionEdit落地生成新的version,其实是配置新version的fileMetaDataVecArr
            builder.SaveTo(version);
        }
        Finalize(version);

        // Initialize new descriptor log file if necessary by creating
        // a temporary file that contains a snapshot of the current version.
        std::string newManifestFilePath;
        Status status;
        if (descriptorLog_ == nullptr) {
            // No reason to unlock *mutex here since we only hit this path in the
            // first call to LogAndApply (when opening the database).
            assert(descriptorFile_ == nullptr);

            versionEdit->SetNextFile(next_file_number_);

            newManifestFilePath = DescriptorFileName(dbname_, manifest_file_number_);

            status = env_->NewWritableFile(newManifestFilePath, &descriptorFile_);
            if (status.ok()) {
                descriptorLog_ = new log::Writer(descriptorFile_);
                // 对这个manifest文件的起点写东西
                status = this->WriteSnapshot(descriptorLog_);
            }
        }

        // Unlock during expensive MANIFEST log write
        // versionEdit落地到manifest
        {
            mutex->Unlock();

            if (status.ok()) {
                std::string record;
                versionEdit->EncodeTo(&record);

                // record依照log的格式写到manifest
                status = descriptorLog_->AddRecord(record);
                if (status.ok()) {
                    status = descriptorFile_->Sync();
                } else {
                    Log(options_->logger, "MANIFEST write: %status\n", status.ToString().c_str());
                }
            }

            // If we just created a new descriptor file, install it by writing a
            // new CURRENT file that points to it.
            if (status.ok() && !newManifestFilePath.empty()) {
                status = leveldb::SetCurrentFile(env_, dbname_, manifest_file_number_);
            }

            mutex->Lock();
        }

        // 该version纳入versionSet
        if (status.ok()) {
            this->AppendVersion(version);
            log_number_ = versionEdit->log_number_;
            prev_log_number_ = versionEdit->prev_log_number_;
        } else {
            delete version;

            if (!newManifestFilePath.empty()) {
                delete descriptorLog_;
                delete descriptorFile_;
                descriptorLog_ = nullptr;
                descriptorFile_ = nullptr;
                env_->RemoveFile(newManifestFilePath);
            }
        }

        return status;
    }

    Status VersionSet::Recover(bool *save_manifest) {
        struct LogReporter : public log::Reader::Reporter {
            Status *status0;

            void Corruption(size_t bytes, const Status &s) override {
                if (this->status0->ok()) {
                    *this->status0 = s;
                }
            }
        };

        // 读取 "CURRENT" manifestFile, which contains 之前 SetCurrentFile() 写入的当前使用的 manifest file的名字
        std::string currentFileContentStr;
        Status status = ReadFileToString(env_,
                                         CurrentFileName(dbname_),
                                         &currentFileContentStr);
        if (!status.ok()) {
            return status;
        }

        // SetCurrentFile() 写入的当前用的 manifest 的名字 会以\n收尾
        if (currentFileContentStr.empty() || currentFileContentStr[currentFileContentStr.size() - 1] != '\n') {
            return Status::Corruption("CURRENT manifestFile does not end with newline");
        }

        // 内容的是 MANIFEST-number
        currentFileContentStr.resize(currentFileContentStr.size() - 1);

        // manifest文件保存的是versionEdit序列化后的内容
        std::string manifestFilePath = dbname_ + "/" + currentFileContentStr;

        SequentialFile *manifestFile;
        status = env_->NewSequentialFile(manifestFilePath, &manifestFile);
        if (!status.ok()) {
            if (status.IsNotFound()) {
                return Status::Corruption("CURRENT points to a non-existent manifest file", status.ToString());
            }

            return status;
        }

        bool haveLogNumber = false;
        bool havePrevLogNumber = false;
        bool haveNextFile = false;
        bool haveLastSequence = false;
        uint64_t nextManifestFileNumber = 0;
        uint64_t lastSequence = 0;
        uint64_t logNumber = 0;
        uint64_t prevLogNumber = 0;
        Builder builder(this, currentVersion_);
        int readRecordCount = 0;

        // 读取 manifest
        {
            LogReporter logReporter;
            logReporter.status0 = &status;

            log::Reader manifestReader(manifestFile, &logReporter, true, 0);

            Slice dest;
            std::string scratch;
            // manifest文件中的logRecord的payload是versionEdit
            while (manifestReader.ReadRecord(&dest, &scratch) && status.ok()) { // 读取裸的byte
                ++readRecordCount;

                // versionEdit的是增量变化
                VersionEdit versionEdit;
                status = versionEdit.DecodeFrom(dest); // 还原 versionEdit
                if (status.ok()) {
                    if (versionEdit.has_comparator_ &&
                        versionEdit.comparatorName_ != internalKeyComparator_.user_comparator()->Name()) {
                        status = Status::InvalidArgument(
                                versionEdit.comparatorName_ + " does not match existing internalKeyComparator ",
                                internalKeyComparator_.user_comparator()->Name());
                    }
                }

                if (status.ok()) {
                    builder.Apply(&versionEdit); // builder吸收versionEdit
                }

                if (versionEdit.has_log_number_) {
                    logNumber = versionEdit.log_number_;
                    haveLogNumber = true;
                }
                if (versionEdit.has_prev_log_number_) {
                    prevLogNumber = versionEdit.prev_log_number_;
                    havePrevLogNumber = true;
                }
                if (versionEdit.has_next_file_number_) {
                    nextManifestFileNumber = versionEdit.next_file_number_;
                    haveNextFile = true;
                }
                if (versionEdit.has_last_sequence_) {
                    lastSequence = versionEdit.last_sequence_;
                    haveLastSequence = true;
                }
            }
        }
        delete manifestFile;
        manifestFile = nullptr;

        if (status.ok()) {
            if (!haveNextFile) {
                status = Status::Corruption("no meta-nextfile entry in descriptor");
            } else if (!haveLogNumber) {
                status = Status::Corruption("no meta-lognumber entry in descriptor");
            } else if (!haveLastSequence) {
                status = Status::Corruption("no last-sequence-number entry in descriptor");
            }

            if (!havePrevLogNumber) {
                prevLogNumber = 0;
            }

            MarkFileNumberUsed(prevLogNumber);
            MarkFileNumberUsed(logNumber);
        }

        if (status.ok()) {
            auto *version = new Version(this);
            // 把增量落地到version
            builder.SaveTo(version);
            // install recovered version
            Finalize(version);
            AppendVersion(version);

            // versionEdit上读取到的移动到versionSet
            manifest_file_number_ = nextManifestFileNumber;
            next_file_number_ = nextManifestFileNumber + 1;
            last_sequence_ = lastSequence;
            log_number_ = logNumber;
            prev_log_number_ = prevLogNumber;

            // See if we can reuse the existing MANIFEST manifestFile.
            if (ReuseManifest(manifestFilePath, currentFileContentStr)) {
                // No need to save new manifest
            } else {
                *save_manifest = true;
            }
        } else {
            std::string error = status.ToString();
            Log(options_->logger,
                "error recovering version set with %d records: %status",
                readRecordCount, error.c_str());
        }

        return status;
    }

    bool VersionSet::ReuseManifest(const std::string &dscname,
                                   const std::string &dscbase) {
        if (!options_->reuseLogs) {
            return false;
        }
        FileType manifest_type;
        uint64_t manifest_number;
        uint64_t manifest_size;
        if (!ParseFileName(dscbase, &manifest_number, &manifest_type) ||
            manifest_type != kDescriptorFile ||
            !env_->GetFileSize(dscname, &manifest_size).ok() ||
            // Make new compacted MANIFEST if old one is too big
            manifest_size >= TargetFileSize(options_)) {
            return false;
        }

        assert(descriptorFile_ == nullptr);
        assert(descriptorLog_ == nullptr);
        Status r = env_->NewAppendableFile(dscname, &descriptorFile_);
        if (!r.ok()) {
            Log(options_->logger, "Reuse MANIFEST: %s\n", r.ToString().c_str());
            assert(descriptorFile_ == nullptr);
            return false;
        }

        Log(options_->logger, "Reusing MANIFEST %s\n", dscname.c_str());
        descriptorLog_ = new log::Writer(descriptorFile_, manifest_size);
        manifest_file_number_ = manifest_number;
        return true;
    }

    void VersionSet::MarkFileNumberUsed(uint64_t number) {
        if (next_file_number_ <= number) {
            next_file_number_ = number + 1;
        }
    }

    void VersionSet::Finalize(Version *version) {
        // 数量大的要用来 next compaction_
        int bestLevel = -1;
        double bestScore = -1;

        for (int level = 0; level < config::kNumLevels - 1; level++) {
            double score;

            if (level == 0) {
                // We treat level-0 specially by bounding 文件数量 instead of byte数量 for two reasons:
                //
                // (1) With larger write-buffer sizes, it is nice not to do too many level-0 compactions.
                //
                // (2) The files in level-0 are merged on every read and
                // therefore we wish to avoid too many files when the individual
                // file size is small (perhaps because of a small write-buffer
                // setting, or very high compression ratios, or lots of
                // overwrites/deletions).
                score = version->fileMetaDataVecArr[level].size() /
                        static_cast<double>(config::level0_CompactionTrigger);
            } else {
                // Compute the ratio of current size to size limit.
                const uint64_t levelByteCount = TotalFileSize(version->fileMetaDataVecArr[level]);
                score = static_cast<double>(levelByteCount) / MaxBytesForLevel(options_, level);
            }

            if (score > bestScore) {
                bestLevel = level;
                bestScore = score;
            }
        }

        version->compactionLevel_ = bestLevel;
        version->compactionScore_ = bestScore;
    }

    // 落地当前时间点的versionSet本身和currentVersion信息
    Status VersionSet::WriteSnapshot(log::Writer *writer) {
        // TODO: Break up into multiple records to reduce memory usage on recovery?

        VersionEdit versionEdit;

        // setComparatorName
        versionEdit.SetComparatorName(internalKeyComparator_.user_comparator()->Name());

        // 遍历level setCompactPointer
        for (int level = 0; level < config::kNumLevels; level++) {
            if (compactPointer_[level].empty()) {
                continue;
            }

            InternalKey internalKey;
            internalKey.DecodeFrom(compactPointer_[level]);

            versionEdit.SetCompactPointer(level, internalKey);
        }

        // 遍历level addFile
        for (int level = 0; level < config::kNumLevels; level++) {
            const std::vector<FileMetaData *> &fileMetaDataVec = currentVersion_->fileMetaDataVecArr[level];
            for (auto fileMetaData: fileMetaDataVec) {
                versionEdit.AddFile(level,
                                    fileMetaData->number,
                                    fileMetaData->fileSize_,
                                    fileMetaData->smallestInternalKey_,
                                    fileMetaData->largestInternalKey_);
            }
        }

        std::string record;
        versionEdit.EncodeTo(&record);
        return writer->AddRecord(record);
    }

    int VersionSet::NumLevelFiles(int level) const {
        assert(level >= 0);
        assert(level < config::kNumLevels);
        return currentVersion_->fileMetaDataVecArr[level].size();
    }

    const char *VersionSet::LevelSummary(LevelSummaryStorage *scratch) const {
        // Update code if kNumLevels changes
        static_assert(config::kNumLevels == 7, "");
        std::snprintf(
                scratch->buffer, sizeof(scratch->buffer), "files[ %d %d %d %d %d %d %d ]",
                int(currentVersion_->fileMetaDataVecArr[0].size()), int(currentVersion_->fileMetaDataVecArr[1].size()),
                int(currentVersion_->fileMetaDataVecArr[2].size()), int(currentVersion_->fileMetaDataVecArr[3].size()),
                int(currentVersion_->fileMetaDataVecArr[4].size()), int(currentVersion_->fileMetaDataVecArr[5].size()),
                int(currentVersion_->fileMetaDataVecArr[6].size()));
        return scratch->buffer;
    }

    uint64_t VersionSet::ApproximateOffsetOf(Version *v, const InternalKey &ikey) {
        uint64_t result = 0;
        for (int level = 0; level < config::kNumLevels; level++) {
            const std::vector<FileMetaData *> &files = v->fileMetaDataVecArr[level];
            for (auto file: files) {
                if (internalKeyComparator_.Compare(file->largestInternalKey_, ikey) <= 0) {
                    // Entire file is before "ikey", so just add the file size
                    result += file->fileSize_;
                } else if (internalKeyComparator_.Compare(file->smallestInternalKey_, ikey) > 0) {
                    // Entire file is after "ikey", so ignore
                    if (level > 0) {
                        // Files other than level 0 are sorted by meta->smallestInternalKey_, so
                        // no further files in this level will contain data for
                        // "ikey".
                        break;
                    }
                } else {
                    // "ikey" falls in the range for this table.  Add the
                    // approximate offset of "ikey" within the table.
                    Table *tableptr;
                    Iterator *iter = table_cache_->NewIterator(
                            ReadOptions(), file->number, file->fileSize_, &tableptr);
                    if (tableptr != nullptr) {
                        result += tableptr->ApproximateOffsetOf(ikey.Encode());
                    }
                    delete iter;
                }
            }
        }
        return result;
    }

    void VersionSet::AddLiveFiles(std::set<uint64_t> *live) {
        for (Version *v = dummy_versions_.next_; v != &dummy_versions_; v = v->next_) {
            for (auto &fileMetaDataVec: v->fileMetaDataVecArr) {
                for (auto &fileMetaData: fileMetaDataVec) {
                    live->insert(fileMetaData->number);
                }
            }
        }
    }

    int64_t VersionSet::NumLevelBytes(int level) const {
        assert(level >= 0);
        assert(level < config::kNumLevels);
        return TotalFileSize(currentVersion_->fileMetaDataVecArr[level]);
    }

    int64_t VersionSet::MaxNextLevelOverlappingBytes() {
        int64_t result = 0;
        std::vector<FileMetaData *> overlaps;
        for (int level = 1; level < config::kNumLevels - 1; level++) {
            for (size_t i = 0; i < currentVersion_->fileMetaDataVecArr[level].size(); i++) {
                const FileMetaData *f = currentVersion_->fileMetaDataVecArr[level][i];
                currentVersion_->GetOverlappingInputs(level + 1, &f->smallestInternalKey_, &f->largestInternalKey_,
                                                      &overlaps);
                const int64_t sum = TotalFileSize(overlaps);
                if (sum > result) {
                    result = sum;
                }
            }
        }
        return result;
    }

    // 得到 inputFileMetaDataVec  最小的 smallestInternalKey_ 和最大的 largestInternalKey_
    // REQUIRES: inputFileMetaDataVec is not empty
    void VersionSet::GetRange(const std::vector<FileMetaData *> &inputFileMetaDataVec,
                              InternalKey *smallest,
                              InternalKey *largest) {
        assert(!inputFileMetaDataVec.empty());

        smallest->Clear();
        largest->Clear();

        for (size_t i = 0; i < inputFileMetaDataVec.size(); i++) {
            FileMetaData *inputFileMetaData = inputFileMetaDataVec[i];

            if (i == 0) {
                *smallest = inputFileMetaData->smallestInternalKey_;
                *largest = inputFileMetaData->largestInternalKey_;
                continue;
            }

            if (internalKeyComparator_.Compare(inputFileMetaData->smallestInternalKey_, *smallest) < 0) {
                *smallest = inputFileMetaData->smallestInternalKey_;
            }

            if (internalKeyComparator_.Compare(inputFileMetaData->largestInternalKey_, *largest) > 0) {
                *largest = inputFileMetaData->largestInternalKey_;
            }
        }
    }

    // Stores the minimal range that covers all entries in input1 and input2
    // in *smallestInternalKey_, *largestInternalKey_.
    // REQUIRES: inputs is not empty
    void VersionSet::GetRange2(const std::vector<FileMetaData *> &input1,
                               const std::vector<FileMetaData *> &input2,
                               InternalKey *smallest,
                               InternalKey *largest) {
        std::vector<FileMetaData *> all = input1;
        all.insert(all.end(), input2.begin(), input2.end());
        GetRange(all, smallest, largest);
    }

    // 生成 an iterator that read compaction的 inputFileMetaDataVecArr_
    Iterator *VersionSet::MakeInputIterator(Compaction *compaction) {
        ReadOptions readOptions;
        readOptions.verifyChecksums_ = options_->paranoid_checks;
        readOptions.fillCache_ = false;

        // Level-0 files have to be merged together.
        // For other levels,we will make a concatenating iterator per level.
        // TODO(opt): use concatenating iterator for level-0 if there is no overlap
        const int arraySize = (compaction->level() == 0 ? compaction->inputFileMetaDataVecArr_[0].size() + 1 : 2);
        auto **iteratorPtrArr = new Iterator *[arraySize];
        int num = 0;

        for (int which = 0; which < 2; which++) {
            if (compaction->inputFileMetaDataVecArr_[which].empty()) {
                continue;
            }

            if (compaction->level() + which == 0) {
                const std::vector<FileMetaData *> &fileMetaDataVec = compaction->inputFileMetaDataVecArr_[which];

                for (auto fileMetaData: fileMetaDataVec) {
                    iteratorPtrArr[num++] = table_cache_->NewIterator(readOptions,
                                                                      fileMetaData->number,
                                                                      fileMetaData->fileSize_);
                }
            } else {
                // Create concatenating iterator for the files from this level
                iteratorPtrArr[num++] = NewTwoLevelIterator(
                        new Version::LevelFileNumIterator(internalKeyComparator_,
                                                          &compaction->inputFileMetaDataVecArr_[which]),
                        &GetFileIterator, table_cache_,
                        readOptions);
            }
        }

        assert(num <= arraySize);

        Iterator *result = NewMergingIterator(&internalKeyComparator_, iteratorPtrArr, num);

        delete[] iteratorPtrArr;

        return result;
    }

    Compaction *VersionSet::PickCompaction() {
        Compaction *compaction;
        int level;

        // We 更倾向 compactions triggered by too much data in a level 相比 the compactions triggered by seeks.
        const bool sizeCompaction = (currentVersion_->compactionScore_ >= 1);
        // 因为seek的趟数太多需要compaction
        const bool seekCompaction = (currentVersion_->file_to_compact_ != nullptr);

        if (sizeCompaction) { // 优先
            // compactLevel和compactScore是1起的
            level = currentVersion_->compactionLevel_;

            assert(level >= 0);
            assert(level + 1 < config::kNumLevels);

            compaction = new Compaction(options_, level);

            // Pick the first file that comes after compactPointer_[level]
            for (size_t i = 0; i < currentVersion_->fileMetaDataVecArr[level].size(); i++) {
                FileMetaData *fileMetaData = currentVersion_->fileMetaDataVecArr[level][i];

                if (compactPointer_[level].empty() ||
                    internalKeyComparator_.Compare(fileMetaData->largestInternalKey_.Encode(),
                                                   compactPointer_[level]) > 0) {
                    compaction->inputFileMetaDataVecArr_[0].push_back(fileMetaData);
                    break;
                }
            }

            if (compaction->inputFileMetaDataVecArr_[0].empty()) {
                // wrap-around to the beginning of the key space
                compaction->inputFileMetaDataVecArr_[0].push_back(currentVersion_->fileMetaDataVecArr[level][0]);
            }
        } else if (seekCompaction) { // 直接是针对的某个文件
            level = currentVersion_->fileToCompactLevel_;
            compaction = new Compaction(options_, level);
            compaction->inputFileMetaDataVecArr_[0].push_back(currentVersion_->file_to_compact_);
        } else {
            return nullptr;
        }

        compaction->inputVersion_ = currentVersion_;
        compaction->inputVersion_->Ref();

        // level 0 上的各个ldb文件可能在key上会重叠,提取全部的 overlapping ones
        if (level == 0) {
            InternalKey smallest;
            InternalKey largest;

            // 得到 inputFileMetaDataVec  最小的 smallestInternalKey_ 和最大的 largestInternalKey_
            this->GetRange(compaction->inputFileMetaDataVecArr_[0], &smallest, &largest);

            // Note that the next call will discard the file we placed in
            // compaction_->inputFileMetaDataVecArr_[0] earlier and replace it with an overlapping set
            // which will include the picked file.
            currentVersion_->GetOverlappingInputs(0,
                                                  &smallest,
                                                  &largest,
                                                  &compaction->inputFileMetaDataVecArr_[0]);

            assert(!compaction->inputFileMetaDataVecArr_[0].empty());
        }

        this->SetupOtherInputs(compaction);

        return compaction;
    }

    // 在各meta的最大的key中挑选最大的
    bool FindLargestKey(const InternalKeyComparator &internalKeyComparator,
                        const std::vector<FileMetaData *> &files,
                        InternalKey *largest_key) {
        if (files.empty()) {
            return false;
        }
        *largest_key = files[0]->largestInternalKey_;
        for (size_t i = 1; i < files.size(); ++i) {
            FileMetaData *f = files[i];
            if (internalKeyComparator.Compare(f->largestInternalKey_, *largest_key) > 0) {
                *largest_key = f->largestInternalKey_;
            }
        }
        return true;
    }

    // find minimum file b2=(l2, u2) in level file for which l2 > u1 and userKey(l2) = userKey(u1)
    // 得到类似实现std::upper_bound效果 比largestKey大的最小的
    FileMetaData *FindSmallestBoundaryFile(const InternalKeyComparator &internalKeyComparator,
                                           const std::vector<FileMetaData *> &versionLevelFileMetaVec,
                                           const InternalKey &largestKey) {
        const Comparator *userComparator = internalKeyComparator.user_comparator();

        FileMetaData *smallestBoundaryFileMeta = nullptr;

        for (auto versionLevelFileMeta: versionLevelFileMetaVec) {

            // 要比largestKey大
            if (internalKeyComparator.Compare(versionLevelFileMeta->smallestInternalKey_,
                                              largestKey) > 0 &&
                userComparator->Compare(versionLevelFileMeta->smallestInternalKey_.userKey(),
                                        largestKey.userKey()) == 0) {

                // 纯粹的比小相对简单
                if (smallestBoundaryFileMeta == nullptr ||
                    internalKeyComparator.Compare(versionLevelFileMeta->smallestInternalKey_,
                                                  smallestBoundaryFileMeta->smallestInternalKey_) < 0) {

                    smallestBoundaryFileMeta = versionLevelFileMeta;
                }
            }
        }

        return smallestBoundaryFileMeta;
    }

    // Extracts the largestInternalKey_ file b1 from |compactionFiles| and then searches for a
    // b2 in |versionLevelFileMetaVec| for which userKey(u1) = userKey(l2). If it finds such a
    // file b2 (known as a boundary file) it adds it to |compactionFiles| and then
    // searches again using this new upper bound.
    //
    // If there are two blocks, b1=(l1, u1) and b2=(l2, u2) and
    // userKey(u1) = userKey(l2), and if we compact b1 but not b2 then a
    // subsequent get operation will yield an incorrect result because it will
    // return the record from b2 in level i rather than from b1 because it searches
    // level by level for records matching the supplied user key.
    //
    // parameters:
    //   in     versionLevelFileMetaVec:      List of files to search for boundary files.
    //   in/out compactionFiles: List of files to extend by adding boundary files.
    void AddBoundaryInputs(const InternalKeyComparator &internalKeyComparator,
                           const std::vector<FileMetaData *> &versionLevelFileMetaVec,
                           std::vector<FileMetaData *> *compactionFiles) {
        InternalKey largestKeyCompact;

        if (!FindLargestKey(internalKeyComparator, *compactionFiles, &largestKeyCompact)) {
            return;
        }

        while (true) {
            // 到 versionLevelFileMetaVec 寻找 比largest大的smallestInternalKey_当中的最小的
            FileMetaData *smallestBoundaryFileMeta = FindSmallestBoundaryFile(internalKeyComparator,
                                                                              versionLevelFileMetaVec,
                                                                              largestKeyCompact);

            if (smallestBoundaryFileMeta == nullptr) {
                return;
            }

            compactionFiles->push_back(smallestBoundaryFileMeta);

            largestKeyCompact = smallestBoundaryFileMeta->largestInternalKey_;
        }
    }

    void VersionSet::SetupOtherInputs(Compaction *compaction) {
        const int compactLevel = compaction->level();

        // 得到了各个的边界的meta，成果是装到compaction->inputFileMetaDataVecArr_[0]
        AddBoundaryInputs(internalKeyComparator_,
                          currentVersion_->fileMetaDataVecArr[compactLevel],
                          &compaction->inputFileMetaDataVecArr_[0]);

        // 得到 compactLevel+1 上的overlap 装入到 compaction->inputFileMetaDataVecArr_[1]
        InternalKey smallest;
        InternalKey largest;
        GetRange(compaction->inputFileMetaDataVecArr_[0], &smallest, &largest);
        currentVersion_->GetOverlappingInputs(compactLevel + 1,
                                              &smallest,
                                              &largest,
                                              &compaction->inputFileMetaDataVecArr_[1]);

        // 得到那两个的数组整体维度上的最小最大
        InternalKey all_start;
        InternalKey all_limit;
        GetRange2(compaction->inputFileMetaDataVecArr_[0],
                  compaction->inputFileMetaDataVecArr_[1],
                  &all_start,
                  &all_limit);

        // see if can grow number of inputs in "compactLevel" without changing number of "compactLevel+1"
        // level 和 level+1有key重复
        if (!compaction->inputFileMetaDataVecArr_[1].empty()) {
            std::vector<FileMetaData *> expanded0;
            currentVersion_->GetOverlappingInputs(compactLevel,
                                                  &all_start,
                                                  &all_limit,
                                                  &expanded0);

            AddBoundaryInputs(internalKeyComparator_,
                              currentVersion_->fileMetaDataVecArr[compactLevel],
                              &expanded0);

            const int64_t input0Size = TotalFileSize(compaction->inputFileMetaDataVecArr_[0]);
            const int64_t input1Size = TotalFileSize(compaction->inputFileMetaDataVecArr_[1]);
            const int64_t expanded0Size = TotalFileSize(expanded0);

            if (expanded0.size() > compaction->inputFileMetaDataVecArr_[0].size() &&
                input1Size + expanded0Size < ExpandedCompactionByteSizeLimit(options_)) {

                InternalKey new_start;
                InternalKey new_limit;
                GetRange(expanded0, &new_start, &new_limit);
                std::vector<FileMetaData *> expanded1;
                currentVersion_->GetOverlappingInputs(compactLevel + 1,
                                                      &new_start,
                                                      &new_limit,
                                                      &expanded1);

                if (expanded1.size() == compaction->inputFileMetaDataVecArr_[1].size()) {
                    Log(options_->logger,
                        "Expanding@%d %d+%d (%ld+%ld bytes) to %d+%d (%ld+%ld bytes)\n",
                        compactLevel,
                        int(compaction->inputFileMetaDataVecArr_[0].size()),
                        int(compaction->inputFileMetaDataVecArr_[1].size()),
                        long(input0Size),
                        long(input1Size),
                        int(expanded0.size()),
                        int(expanded1.size()),
                        long(expanded0Size),
                        long(input1Size));

                    smallest = new_start;
                    largest = new_limit;
                    compaction->inputFileMetaDataVecArr_[0] = expanded0;
                    compaction->inputFileMetaDataVecArr_[1] = expanded1;

                    GetRange2(compaction->inputFileMetaDataVecArr_[0],
                              compaction->inputFileMetaDataVecArr_[1],
                              &all_start,
                              &all_limit);
                }
            }
        }

        // Compute the set of grandparent that overlap this compaction_
        // (parent 是 compactLevel+1; grandparent 是 compactLevel+2)
        if (compactLevel + 2 < config::kNumLevels) {
            currentVersion_->GetOverlappingInputs(compactLevel + 2,
                                                  &all_start,
                                                  &all_limit,
                                                  &compaction->grandparentFileMetaVac_);
        }

        // Update the place where we will do the next compaction_ for this compactLevel.
        // We update this immediately instead of waiting for the VersionEdit
        // to be applied so that if the compaction_ fails, we will try a different
        // key range next time.
        compactPointer_[compactLevel] = largest.Encode().ToString();
        compaction->versionEdit_.SetCompactPointer(compactLevel, largest);
    }

    Compaction *VersionSet::CompactRange(int level,
                                         const InternalKey *begin,
                                         const InternalKey *end) {
        std::vector<FileMetaData *> inputs;
        currentVersion_->GetOverlappingInputs(level, begin, end, &inputs);
        if (inputs.empty()) {
            return nullptr;
        }

        // Avoid compacting too much in one shot in case the range is large.
        // But we cannot do this for level-0 since level-0 files can overlap
        // and we must not pick one file and drop another older file if the
        // two files overlap.
        if (level > 0) {
            const uint64_t limit = MaxFileSizeForLevel(options_, level);
            uint64_t total = 0;
            for (size_t i = 0; i < inputs.size(); i++) {
                uint64_t s = inputs[i]->fileSize_;
                total += s;
                if (total >= limit) {
                    inputs.resize(i + 1);
                    break;
                }
            }
        }

        Compaction *c = new Compaction(options_, level);
        c->inputVersion_ = currentVersion_;
        c->inputVersion_->Ref();
        c->inputFileMetaDataVecArr_[0] = inputs;
        SetupOtherInputs(c);
        return c;
    }

    Compaction::Compaction(const Options *options, int level) : level_(level),
                                                                max_output_file_size_(
                                                                        MaxFileSizeForLevel(options, level)),
                                                                inputVersion_(nullptr),
                                                                grandparentIndex_(0),
                                                                seenKey_(false),
                                                                overlappedByteLen_(0) {
        for (unsigned long &level_ptr: level_ptrs_) {
            level_ptr = 0;
        }
    }

    Compaction::~Compaction() {
        if (inputVersion_ != nullptr) {
            inputVersion_->Unref();
        }
    }

    bool Compaction::IsTrivialMove() const {
        const VersionSet *belongingVersionSet = inputVersion_->belongingVersionSet;
        // Avoid a move if there is lots of overlapping grandparent data.
        // Otherwise, the move could create a parent file that will require
        // a very expensive merge later on.
        return (num_input_files(0) == 1 &&
                num_input_files(1) == 0 &&
                TotalFileSize(grandparentFileMetaVac_) <= MaxGrandParentOverlapBytes(belongingVersionSet->options_));
    }

    void Compaction::AddInputDeletions(VersionEdit *edit) {
        for (int which = 0; which < 2; which++) {
            for (size_t i = 0; i < inputFileMetaDataVecArr_[which].size(); i++) {
                edit->RemoveFile(level_ + which, inputFileMetaDataVecArr_[which][i]->number);
            }
        }
    }

    bool Compaction::IsBaseLevelForKey(const Slice &userKey) {
        const Comparator *userComparator = inputVersion_->belongingVersionSet->internalKeyComparator_.user_comparator();
        for (int level = level_ + 2; level < config::kNumLevels; level++) {
            const std::vector<FileMetaData *> &fileMetaVec = inputVersion_->fileMetaDataVecArr[level];

            while (level_ptrs_[level] < fileMetaVec.size()) {
                FileMetaData *fileMeta = fileMetaVec[level_ptrs_[level]];

                // we've advanced far enough
                if (userComparator->Compare(userKey, fileMeta->largestInternalKey_.userKey()) <= 0) {

                    // 落到了fileMeta
                    if (userComparator->Compare(userKey, fileMeta->smallestInternalKey_.userKey()) >= 0) {
                        return false;
                    }

                    // 到下1个level
                    break;
                }

                level_ptrs_[level]++;
            }
        }

        return true;
    }

    // true if we should stop building the current output before handle "internalKey"
    bool Compaction::ShouldStopBefore(const Slice &internalKey) {
        const VersionSet *versionSet = inputVersion_->belongingVersionSet;
        const InternalKeyComparator *internalKeyComparator = &versionSet->internalKeyComparator_;

        // 遍历 to find earliest grandparent file that contains key.
        while (grandparentIndex_ < grandparentFileMetaVac_.size() &&
               internalKeyComparator->Compare(internalKey,
                                              grandparentFileMetaVac_[grandparentIndex_]->largestInternalKey_.Encode()) > 0) {
            if (seenKey_) {
                overlappedByteLen_ += grandparentFileMetaVac_[grandparentIndex_]->fileSize_;
            }

            grandparentIndex_++;
        }

        seenKey_ = true;

        // too much overlap for current output; start new output
        if (overlappedByteLen_ > MaxGrandParentOverlapBytes(versionSet->options_)) {
            overlappedByteLen_ = 0;
            return true;
        }

        return false;
    }

    void Compaction::ReleaseInputs() {
        if (inputVersion_ == nullptr) {
            return;
        }

        inputVersion_->Unref();
        inputVersion_ = nullptr;
    }

}  // namespace leveldb
