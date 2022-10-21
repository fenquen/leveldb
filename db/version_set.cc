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
// stop building a single file in a level->level+1 compaction.
    static int64_t MaxGrandParentOverlapBytes(const Options *options) {
        return 10 * TargetFileSize(options);
    }

// Maximum number of bytes in all compacted files.  We avoid expanding
// the lower level file set of a compaction if it would make the
// total compaction cover more than this many bytes.
    static int64_t ExpandedCompactionByteSizeLimit(const Options *options) {
        return 25 * TargetFileSize(options);
    }

    // Note: the result for level zero is not really used since we set
    // the level-0 compaction threshold based on number of files.
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
        for (auto & level : fileMetaDataVecArr) {
            for (auto f : level) {
                assert(f->refs > 0);
                f->refs--;
                if (f->refs <= 0) {
                    delete f;
                }
            }
        }
    }

    int FindFile(const InternalKeyComparator &icmp,
                 const std::vector<FileMetaData *> &files, const Slice &key) {
        uint32_t left = 0;
        uint32_t right = files.size();
        while (left < right) {
            uint32_t mid = (left + right) / 2;
            const FileMetaData *f = files[mid];
            if (icmp.InternalKeyComparator::Compare(f->largestInternalKey_.Encode(), key) < 0) {
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
                ucmp->Compare(*user_key, f->largestInternalKey_.user_key()) > 0);
    }

    static bool BeforeFile(const Comparator *ucmp, const Slice *user_key,
                           const FileMetaData *f) {
        // null userKey occurs after all keys and is therefore never before *f
        return (user_key != nullptr &&
                ucmp->Compare(*user_key, f->smallestInternalKey_.user_key()) < 0);
    }

    bool SomeFileOverlapsRange(const InternalKeyComparator &icmp,
                               bool disjoint_sorted_files,
                               const std::vector<FileMetaData *> &files,
                               const Slice *smallest_user_key,
                               const Slice *largest_user_key) {
        const Comparator *ucmp = icmp.user_comparator();
        if (!disjoint_sorted_files) {
            // Need to check against all files
            for (size_t i = 0; i < files.size(); i++) {
                const FileMetaData *f = files[i];
                if (AfterFile(ucmp, smallest_user_key, f) ||
                    BeforeFile(ucmp, largest_user_key, f)) {
                    // No overlap
                } else {
                    return true;  // Overlap
                }
            }
            return false;
        }

        // Binary search over file list
        uint32_t index = 0;
        if (smallest_user_key != nullptr) {
            // Find the earliest possible internal key for smallest_user_key
            InternalKey small_key(*smallest_user_key, kMaxSequenceNumber,
                                  kValueTypeForSeek);
            index = FindFile(icmp, files, small_key.Encode());
        }

        if (index >= files.size()) {
            // beginning of range is after all files, so no overlap.
            return false;
        }

        return !BeforeFile(ucmp, largest_user_key, files[index]);
    }

// An internal iterator.  For a given version/level pair, yields
// information about the files in the level.  For a given entry, key()
// is the largestInternalKey_ key that occurs in the file, and value() is an
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
    static void SaveValue(void *arg, const Slice &ikey, const Slice &v) {
        Saver *s = reinterpret_cast<Saver *>(arg);
        ParsedInternalKey parsed_key;
        if (!ParseInternalKey(ikey, &parsed_key)) {
            s->state = kCorrupt;
        } else {
            if (s->ucmp->Compare(parsed_key.userKey, s->user_key) == 0) {
                s->state = (parsed_key.type == kTypeValue) ? kFound : kDeleted;
                if (s->state == kFound) {
                    s->value->assign(v.data(), v.size());
                }
            }
        }
    }

    static bool NewestFirst(FileMetaData *a, FileMetaData *b) {
        return a->number > b->number;
    }

    void Version::ForEachOverlapping(Slice user_key, Slice internal_key, void *arg,
                                     bool (*func)(void *, int, FileMetaData *)) {
        const Comparator *ucmp = belongingVersionSet->internalKeyComparator_.user_comparator();

        // Search level-0 in order from newest to oldest.
        std::vector<FileMetaData *> tmp;
        tmp.reserve(fileMetaDataVecArr[0].size());
        for (uint32_t i = 0; i < fileMetaDataVecArr[0].size(); i++) {
            FileMetaData *f = fileMetaDataVecArr[0][i];
            if (ucmp->Compare(user_key, f->smallestInternalKey_.user_key()) >= 0 &&
                ucmp->Compare(user_key, f->largestInternalKey_.user_key()) <= 0) {
                tmp.push_back(f);
            }
        }
        if (!tmp.empty()) {
            std::sort(tmp.begin(), tmp.end(), NewestFirst);
            for (uint32_t i = 0; i < tmp.size(); i++) {
                if (!(*func)(arg, 0, tmp[i])) {
                    return;
                }
            }
        }

        // Search other levels.
        for (int level = 1; level < config::kNumLevels; level++) {
            size_t num_files = fileMetaDataVecArr[level].size();
            if (num_files == 0) continue;

            // Binary search to find earliest index whose largestInternalKey_ key >= internal_key.
            uint32_t index = FindFile(belongingVersionSet->internalKeyComparator_, fileMetaDataVecArr[level],
                                      internal_key);
            if (index < num_files) {
                FileMetaData *f = fileMetaDataVecArr[level][index];
                if (ucmp->Compare(user_key, f->smallestInternalKey_.user_key()) < 0) {
                    // All of "f" is past any data for userKey
                } else {
                    if (!(*func)(arg, level, f)) {
                        return;
                    }
                }
            }
        }
    }

    Status Version::Get(const ReadOptions &options, const LookupKey &k,
                        std::string *value, GetStats *stats) {
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

            static bool Match(void *arg, int level, FileMetaData *f) {
                State *state = reinterpret_cast<State *>(arg);

                if (state->stats->seek_file == nullptr &&
                    state->last_file_read != nullptr) {
                    // We have had more than one seek for this read.  Charge the 1st file.
                    state->stats->seek_file = state->last_file_read;
                    state->stats->seek_file_level = state->last_file_read_level;
                }

                state->last_file_read = f;
                state->last_file_read_level = level;

                state->s = state->vset->table_cache_->Get(*state->options, f->number,
                                                          f->fileSize_, state->ikey,
                                                          &state->saver, SaveValue);
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
                        state->s =
                                Status::Corruption("corrupted key for ", state->saver.user_key);
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

        state.options = &options;
        state.ikey = k.internal_key();
        state.vset = belongingVersionSet;

        state.saver.state = kNotFound;
        state.saver.ucmp = belongingVersionSet->internalKeyComparator_.user_comparator();
        state.saver.user_key = k.user_key();
        state.saver.value = value;

        ForEachOverlapping(state.saver.user_key, state.ikey, &state, &State::Match);

        return state.found ? state.s : Status::NotFound(Slice());
    }

    bool Version::UpdateStats(const GetStats &stats) {
        FileMetaData *f = stats.seek_file;
        if (f != nullptr) {
            f->allowedSeeks_--;
            if (f->allowedSeeks_ <= 0 && file_to_compact_ == nullptr) {
                file_to_compact_ = f;
                file_to_compact_level_ = stats.seek_file_level;
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

    bool Version::OverlapInLevel(int level, const Slice *smallest_user_key,
                                 const Slice *largest_user_key) {
        return SomeFileOverlapsRange(belongingVersionSet->internalKeyComparator_, (level > 0),
                                     fileMetaDataVecArr[level],
                                     smallest_user_key, largest_user_key);
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

// Store in "*inputs" all files in "level" that overlap [begin,end]
    void Version::GetOverlappingInputs(int level, const InternalKey *begin,
                                       const InternalKey *end,
                                       std::vector<FileMetaData *> *inputs) {
        assert(level >= 0);
        assert(level < config::kNumLevels);
        inputs->clear();
        Slice user_begin, user_end;
        if (begin != nullptr) {
            user_begin = begin->user_key();
        }
        if (end != nullptr) {
            user_end = end->user_key();
        }
        const Comparator *user_cmp = belongingVersionSet->internalKeyComparator_.user_comparator();
        for (size_t i = 0; i < fileMetaDataVecArr[level].size();) {
            FileMetaData *f = fileMetaDataVecArr[level][i++];
            const Slice file_start = f->smallestInternalKey_.user_key();
            const Slice file_limit = f->largestInternalKey_.user_key();
            if (begin != nullptr && user_cmp->Compare(file_limit, user_begin) < 0) {
                // "f" is completely before specified range; skip it
            } else if (end != nullptr && user_cmp->Compare(file_start, user_end) > 0) {
                // "f" is completely after specified range; skip it
            } else {
                inputs->push_back(f);
                if (level == 0) {
                    // Level-0 files may overlap each other.  So check if the newly
                    // added file has expanded the range.  If so, restart search.
                    if (begin != nullptr && user_cmp->Compare(file_start, user_begin) < 0) {
                        user_begin = file_start;
                        inputs->clear();
                        i = 0;
                    } else if (end != nullptr &&
                               user_cmp->Compare(file_limit, user_end) > 0) {
                        user_end = file_limit;
                        inputs->clear();
                        i = 0;
                    }
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
        // initialize a builder with the files from *baseVersion and other info from *belongingVersionSet
        Builder(VersionSet *versionSet, Version *baseVersion) : versionSet_(versionSet),
                                                                baseVersion_(baseVersion) {
            baseVersion_->Ref();
            BySmallestKey cmp;
            cmp.internalKeyComparator = &versionSet->internalKeyComparator_;
            for (int level = 0; level < config::kNumLevels; level++) {
                levelStateArr[level].addedFileMetaDataSet_ = new FileSet(cmp);
            }
        }

        ~Builder() {
            for (int level = 0; level < config::kNumLevels; level++) {
                const FileSet *added = levelStateArr[level].addedFileMetaDataSet_;
                std::vector<FileMetaData *> to_unref;
                to_unref.reserve(added->size());
                for (FileSet::const_iterator it = added->begin(); it != added->end(); ++it) {
                    to_unref.push_back(*it);
                }
                delete added;
                for (uint32_t i = 0; i < to_unref.size(); i++) {
                    FileMetaData *f = to_unref[i];
                    f->refs--;
                    if (f->refs <= 0) {
                        delete f;
                    }
                }
            }
            baseVersion_->Unref();
        }

        // apply all of the edits in *versionEdit to the current state.
        void Apply(VersionEdit *versionEdit) {
            // update compaction pointers
            for (auto &pair: versionEdit->compact_pointers_) {
                const int level = pair.first;
                versionSet_->compact_pointer_[level] = pair.second.Encode().ToString();
            }

            // delete files
            for (const auto &pair: versionEdit->deletedLevelFileNumberSet_) {
                const int level = pair.first;
                const uint64_t fileNumber = pair.second;
                levelStateArr[level].deletedFileNumbers.insert(fileNumber);
            }

            // add new files
            for (auto &pair: versionEdit->addedLevelFileMetaDataVec_) {
                const int level = pair.first;
                auto *fileMetaData = new FileMetaData(pair.second);
                fileMetaData->refs = 1;

                // arrange to automatically compact this file after a certain number of seeks.  Let's assume:
                //   (1) One seek costs 10ms
                //   (2) Writing or reading 1MB costs 10ms (100MB/s)
                //   (3) A compaction of 1MB does 25MB of IO:
                //         1MB read from this level
                //         10-12MB read from next level (boundaries may be misaligned)
                //         10-12MB written to next level
                // This implies that 25 seeks cost the same as the compaction
                // of 1MB of data.  I.e., one seek costs approximately the
                // same as the compaction of 40KB of data.  We are a little
                // conservative and allow approximately one seek for every 16KB
                // of data before triggering a compaction.
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

            for (int level = 0; level < config::kNumLevels; level++) {
                // Merge the set of added files with the set of pre-existing files.
                // drop any deleted files.  Store the result in *version.
                const std::vector<FileMetaData *> &baseVersionFileMetaDataVec = baseVersion_->fileMetaDataVecArr[level];

                auto baseIter = baseVersionFileMetaDataVec.begin();
                auto baseEnd = baseVersionFileMetaDataVec.end();

                const FileSet *addedFileMetaDataSet = levelStateArr[level].addedFileMetaDataSet_;
                version->fileMetaDataVecArr[level].reserve(
                        baseVersionFileMetaDataVec.size() + addedFileMetaDataSet->size());

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

                // add remaining base files
                for (; baseIter != baseEnd; ++baseIter) {
                    MaybeAddFile(version, level, *baseIter);
                }

#ifndef NDEBUG
                // make sure there is no overlap when levels > 0
                if (level > 0) {
                    for (uint32_t i = 1; i < version->fileMetaDataVecArr[level].size(); i++) {
                        const InternalKey &prevEnd = version->fileMetaDataVecArr[level][i - 1]->largestInternalKey_;
                        const InternalKey &thisBegin = version->fileMetaDataVecArr[level][i]->smallestInternalKey_;

                        if (versionSet_->internalKeyComparator_.Compare(prevEnd, thisBegin) >= 0) {
                            std::fprintf(stderr,
                                         "overlapping ranges in same level %s vs. %s\n",
                                         prevEnd.DebugString().c_str(),
                                         thisBegin.DebugString().c_str());

                            std::abort();
                        }
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

            if (level > 0 && !fileMetaDataVec->empty()) {
                // Must not overlap
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
                                                               descriptor_file_(nullptr),
                                                               descriptor_log_(nullptr),
                                                               dummy_versions_(this),
                                                               currentVersion_(nullptr) {
        AppendVersion(new Version(this));
    }

    VersionSet::~VersionSet() {
        currentVersion_->Unref();
        assert(dummy_versions_.next_ == &dummy_versions_);  // List must be empty
        delete descriptor_log_;
        delete descriptor_file_;
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

        // append to linked list
        // oldPrev - version - dummy
        version->prev_ = dummy_versions_.prev_;
        version->next_ = &dummy_versions_;
        version->prev_->next_ = version;
        version->next_->prev_ = version;
    }

    Status VersionSet::LogAndApply(VersionEdit *edit, port::Mutex *mu) {
        if (edit->has_log_number_) {
            assert(edit->log_number_ >= log_number_);
            assert(edit->log_number_ < next_file_number_);
        } else {
            edit->SetLogNumber(log_number_);
        }

        if (!edit->has_prev_log_number_) {
            edit->SetPrevLogNumber(prev_log_number_);
        }

        edit->SetNextFile(next_file_number_);
        edit->SetLastSequence(last_sequence_);

        Version *v = new Version(this);
        {
            Builder builder(this, currentVersion_);
            builder.Apply(edit);
            builder.SaveTo(v);
        }
        Finalize(v);

        // Initialize new descriptor log file if necessary by creating
        // a temporary file that contains a snapshot of the current version.
        std::string new_manifest_file;
        Status s;
        if (descriptor_log_ == nullptr) {
            // No reason to unlock *mu here since we only hit this path in the
            // first call to LogAndApply (when opening the database).
            assert(descriptor_file_ == nullptr);
            new_manifest_file = DescriptorFileName(dbname_, manifest_file_number_);
            edit->SetNextFile(next_file_number_);
            s = env_->NewWritableFile(new_manifest_file, &descriptor_file_);
            if (s.ok()) {
                descriptor_log_ = new log::Writer(descriptor_file_);
                s = WriteSnapshot(descriptor_log_);
            }
        }

        // Unlock during expensive MANIFEST log write
        {
            mu->Unlock();

            // Write new record to MANIFEST log
            if (s.ok()) {
                std::string record;
                edit->EncodeTo(&record);
                s = descriptor_log_->AddRecord(record);
                if (s.ok()) {
                    s = descriptor_file_->Sync();
                }
                if (!s.ok()) {
                    Log(options_->logger, "MANIFEST write: %s\n", s.ToString().c_str());
                }
            }

            // If we just created a new descriptor file, install it by writing a
            // new CURRENT file that points to it.
            if (s.ok() && !new_manifest_file.empty()) {
                s = SetCurrentFile(env_, dbname_, manifest_file_number_);
            }

            mu->Lock();
        }

        // Install the new version
        if (s.ok()) {
            AppendVersion(v);
            log_number_ = edit->log_number_;
            prev_log_number_ = edit->prev_log_number_;
        } else {
            delete v;

            if (!new_manifest_file.empty()) {
                delete descriptor_log_;
                delete descriptor_file_;
                descriptor_log_ = nullptr;
                descriptor_file_ = nullptr;
                env_->RemoveFile(new_manifest_file);
            }
        }
        return s;
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

        // 读取 "CURRENT" sequentialFileManifest, which contains 之前 SetCurrentFile() 写入的当前使用的 manifest file的名字
        std::string currentFileContentStr;
        Status status = ReadFileToString(env_,
                                         CurrentFileName(dbname_),
                                         &currentFileContentStr);
        if (!status.ok()) {
            return status;
        }

        // SetCurrentFile() 写入的当前用的 manifest 的名字 会以\n收尾
        if (currentFileContentStr.empty() || currentFileContentStr[currentFileContentStr.size() - 1] != '\n') {
            return Status::Corruption("CURRENT sequentialFileManifest does not end with newline");
        }

        // MANIFEST-number
        currentFileContentStr.resize(currentFileContentStr.size() - 1);

        // manifest文件保存的是VersionEdit序列化后的内容
        std::string manifestFilePath = dbname_ + "/" + currentFileContentStr;

        SequentialFile *sequentialFileManifest;
        status = env_->NewSequentialFile(manifestFilePath, &sequentialFileManifest);
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
        uint64_t nextFileNumber = 0;
        uint64_t lastSequence = 0;
        uint64_t logNumber = 0;
        uint64_t prevLogNumber = 0;
        Builder builder(this, currentVersion_);
        int readRecordCount = 0;

        // 读取 manifest
        {
            LogReporter logReporter;
            logReporter.status0 = &status;

            log::Reader manifestReader(sequentialFileManifest,
                                       &logReporter,
                                       true,
                                       0 /*initial_offset*/);
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
                        versionEdit.comparator_ != internalKeyComparator_.user_comparator()->Name()) {
                        status = Status::InvalidArgument(
                                versionEdit.comparator_ + " does not match existing internalKeyComparator ",
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
                    nextFileNumber = versionEdit.next_file_number_;
                    haveNextFile = true;
                }
                if (versionEdit.has_last_sequence_) {
                    lastSequence = versionEdit.last_sequence_;
                    haveLastSequence = true;
                }
            }
        }
        delete sequentialFileManifest;
        sequentialFileManifest = nullptr;

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
            Version *version = new Version(this);
            // 把增量落地到version
            builder.SaveTo(version);
            // install recovered version
            Finalize(version);
            AppendVersion(version);

            // versionEdit上读取到的移动到versionSet
            manifest_file_number_ = nextFileNumber;
            next_file_number_ = nextFileNumber + 1;
            last_sequence_ = lastSequence;
            log_number_ = logNumber;
            prev_log_number_ = prevLogNumber;

            // See if we can reuse the existing MANIFEST sequentialFileManifest.
            if (ReuseManifest(manifestFilePath, currentFileContentStr)) {
                // No need to save new manifest
            } else {
                *save_manifest = true;
            }
        } else {
            std::string error = status.ToString();
            Log(options_->logger,
                "Error recovering version set with %d records: %status",
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

        assert(descriptor_file_ == nullptr);
        assert(descriptor_log_ == nullptr);
        Status r = env_->NewAppendableFile(dscname, &descriptor_file_);
        if (!r.ok()) {
            Log(options_->logger, "Reuse MANIFEST: %s\n", r.ToString().c_str());
            assert(descriptor_file_ == nullptr);
            return false;
        }

        Log(options_->logger, "Reusing MANIFEST %s\n", dscname.c_str());
        descriptor_log_ = new log::Writer(descriptor_file_, manifest_size);
        manifest_file_number_ = manifest_number;
        return true;
    }

    void VersionSet::MarkFileNumberUsed(uint64_t number) {
        if (next_file_number_ <= number) {
            next_file_number_ = number + 1;
        }
    }

    void VersionSet::Finalize(Version *version) {
        // precomputed best level for next compaction
        int bestLevel = -1;
        double bestScore = -1;

        for (int level = 0; level < config::kNumLevels - 1; level++) {
            double score;

            if (level == 0) {
                // We treat level-0 specially by bounding the number of files
                // instead of number of bytes for two reasons:
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

        version->compaction_level_ = bestLevel;
        version->compaction_score_ = bestScore;
    }

    Status VersionSet::WriteSnapshot(log::Writer *log) {
        // TODO: Break up into multiple records to reduce memory usage on recovery?

        // Save metadata
        VersionEdit edit;
        edit.SetComparatorName(internalKeyComparator_.user_comparator()->Name());

        // Save compaction pointers
        for (int level = 0; level < config::kNumLevels; level++) {
            if (!compact_pointer_[level].empty()) {
                InternalKey key;
                key.DecodeFrom(compact_pointer_[level]);
                edit.SetCompactPointer(level, key);
            }
        }

        // Save files
        for (int level = 0; level < config::kNumLevels; level++) {
            const std::vector<FileMetaData *> &files = currentVersion_->fileMetaDataVecArr[level];
            for (size_t i = 0; i < files.size(); i++) {
                const FileMetaData *f = files[i];
                edit.AddFile(level, f->number, f->fileSize_, f->smallestInternalKey_, f->largestInternalKey_);
            }
        }

        std::string record;
        edit.EncodeTo(&record);
        return log->AddRecord(record);
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
            for (size_t i = 0; i < files.size(); i++) {
                if (internalKeyComparator_.Compare(files[i]->largestInternalKey_, ikey) <= 0) {
                    // Entire file is before "ikey", so just add the file size
                    result += files[i]->fileSize_;
                } else if (internalKeyComparator_.Compare(files[i]->smallestInternalKey_, ikey) > 0) {
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
                            ReadOptions(), files[i]->number, files[i]->fileSize_, &tableptr);
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

// Stores the minimal range that covers all entries in inputs in
// *smallestInternalKey_, *largestInternalKey_.
// REQUIRES: inputs is not empty
    void VersionSet::GetRange(const std::vector<FileMetaData *> &inputs,
                              InternalKey *smallest, InternalKey *largest) {
        assert(!inputs.empty());
        smallest->Clear();
        largest->Clear();
        for (size_t i = 0; i < inputs.size(); i++) {
            FileMetaData *f = inputs[i];
            if (i == 0) {
                *smallest = f->smallestInternalKey_;
                *largest = f->largestInternalKey_;
            } else {
                if (internalKeyComparator_.Compare(f->smallestInternalKey_, *smallest) < 0) {
                    *smallest = f->smallestInternalKey_;
                }
                if (internalKeyComparator_.Compare(f->largestInternalKey_, *largest) > 0) {
                    *largest = f->largestInternalKey_;
                }
            }
        }
    }

// Stores the minimal range that covers all entries in inputs1 and inputs2
// in *smallestInternalKey_, *largestInternalKey_.
// REQUIRES: inputs is not empty
    void VersionSet::GetRange2(const std::vector<FileMetaData *> &inputs1,
                               const std::vector<FileMetaData *> &inputs2,
                               InternalKey *smallest, InternalKey *largest) {
        std::vector<FileMetaData *> all = inputs1;
        all.insert(all.end(), inputs2.begin(), inputs2.end());
        GetRange(all, smallest, largest);
    }

    Iterator *VersionSet::MakeInputIterator(Compaction *c) {
        ReadOptions options;
        options.verify_checksums = options_->paranoid_checks;
        options.fill_cache = false;

        // Level-0 files have to be merged together.  For other levels,
        // we will make a concatenating iterator per level.
        // TODO(opt): use concatenating iterator for level-0 if there is no overlap
        const int space = (c->level() == 0 ? c->inputs_[0].size() + 1 : 2);
        Iterator **list = new Iterator *[space];
        int num = 0;
        for (int which = 0; which < 2; which++) {
            if (!c->inputs_[which].empty()) {
                if (c->level() + which == 0) {
                    const std::vector<FileMetaData *> &files = c->inputs_[which];
                    for (size_t i = 0; i < files.size(); i++) {
                        list[num++] = table_cache_->NewIterator(options, files[i]->number,
                                                                files[i]->fileSize_);
                    }
                } else {
                    // Create concatenating iterator for the files from this level
                    list[num++] = NewTwoLevelIterator(
                            new Version::LevelFileNumIterator(internalKeyComparator_, &c->inputs_[which]),
                            &GetFileIterator, table_cache_, options);
                }
            }
        }
        assert(num <= space);
        Iterator *result = NewMergingIterator(&internalKeyComparator_, list, num);
        delete[] list;
        return result;
    }

    Compaction *VersionSet::PickCompaction() {
        Compaction *compaction;
        int level;

        // We prefer compactions triggered by too much data in a level over
        // the compactions triggered by seeks.
        const bool size_compaction = (currentVersion_->compaction_score_ >= 1);
        const bool seek_compaction = (currentVersion_->file_to_compact_ != nullptr);
        if (size_compaction) {
            level = currentVersion_->compaction_level_;
            assert(level >= 0);
            assert(level + 1 < config::kNumLevels);
            compaction = new Compaction(options_, level);

            // Pick the first file that comes after compact_pointer_[level]
            for (size_t i = 0; i < currentVersion_->fileMetaDataVecArr[level].size(); i++) {
                FileMetaData *f = currentVersion_->fileMetaDataVecArr[level][i];
                if (compact_pointer_[level].empty() ||
                    internalKeyComparator_.Compare(f->largestInternalKey_.Encode(), compact_pointer_[level]) > 0) {
                    compaction->inputs_[0].push_back(f);
                    break;
                }
            }
            if (compaction->inputs_[0].empty()) {
                // Wrap-around to the beginning of the key space
                compaction->inputs_[0].push_back(currentVersion_->fileMetaDataVecArr[level][0]);
            }
        } else if (seek_compaction) {
            level = currentVersion_->file_to_compact_level_;
            compaction = new Compaction(options_, level);
            compaction->inputs_[0].push_back(currentVersion_->file_to_compact_);
        } else {
            return nullptr;
        }

        compaction->input_version_ = currentVersion_;
        compaction->input_version_->Ref();

        // Files in level 0 may overlap each other, so pick up all overlapping ones
        if (level == 0) {
            InternalKey smallest, largest;
            GetRange(compaction->inputs_[0], &smallest, &largest);
            // Note that the next call will discard the file we placed in
            // compaction->inputs_[0] earlier and replace it with an overlapping set
            // which will include the picked file.
            currentVersion_->GetOverlappingInputs(0, &smallest, &largest, &compaction->inputs_[0]);
            assert(!compaction->inputs_[0].empty());
        }

        SetupOtherInputs(compaction);

        return compaction;
    }

// Finds the largestInternalKey_ key in a vector of files. Returns true if files it not
// empty.
    bool FindLargestKey(const InternalKeyComparator &icmp,
                        const std::vector<FileMetaData *> &files,
                        InternalKey *largest_key) {
        if (files.empty()) {
            return false;
        }
        *largest_key = files[0]->largestInternalKey_;
        for (size_t i = 1; i < files.size(); ++i) {
            FileMetaData *f = files[i];
            if (icmp.Compare(f->largestInternalKey_, *largest_key) > 0) {
                *largest_key = f->largestInternalKey_;
            }
        }
        return true;
    }

// Finds minimum file b2=(l2, u2) in level file for which l2 > u1 and
// userKey(l2) = userKey(u1)
    FileMetaData *FindSmallestBoundaryFile(
            const InternalKeyComparator &icmp,
            const std::vector<FileMetaData *> &level_files,
            const InternalKey &largest_key) {
        const Comparator *user_cmp = icmp.user_comparator();
        FileMetaData *smallest_boundary_file = nullptr;
        for (size_t i = 0; i < level_files.size(); ++i) {
            FileMetaData *f = level_files[i];
            if (icmp.Compare(f->smallestInternalKey_, largest_key) > 0 &&
                user_cmp->Compare(f->smallestInternalKey_.user_key(), largest_key.user_key()) ==
                0) {
                if (smallest_boundary_file == nullptr ||
                    icmp.Compare(f->smallestInternalKey_, smallest_boundary_file->smallestInternalKey_) < 0) {
                    smallest_boundary_file = f;
                }
            }
        }
        return smallest_boundary_file;
    }

// Extracts the largestInternalKey_ file b1 from |compaction_files| and then searches for a
// b2 in |level_files| for which userKey(u1) = userKey(l2). If it finds such a
// file b2 (known as a boundary file) it adds it to |compaction_files| and then
// searches again using this new upper bound.
//
// If there are two blocks, b1=(l1, u1) and b2=(l2, u2) and
// userKey(u1) = userKey(l2), and if we compact b1 but not b2 then a
// subsequent get operation will yield an incorrect result because it will
// return the record from b2 in level i rather than from b1 because it searches
// level by level for records matching the supplied user key.
//
// parameters:
//   in     level_files:      List of files to search for boundary files.
//   in/out compaction_files: List of files to extend by adding boundary files.
    void AddBoundaryInputs(const InternalKeyComparator &icmp,
                           const std::vector<FileMetaData *> &level_files,
                           std::vector<FileMetaData *> *compaction_files) {
        InternalKey largest_key;

        // Quick return if compaction_files is empty.
        if (!FindLargestKey(icmp, *compaction_files, &largest_key)) {
            return;
        }

        bool continue_searching = true;
        while (continue_searching) {
            FileMetaData *smallest_boundary_file =
                    FindSmallestBoundaryFile(icmp, level_files, largest_key);

            // If a boundary file was found advance largest_key, otherwise we're done.
            if (smallest_boundary_file != NULL) {
                compaction_files->push_back(smallest_boundary_file);
                largest_key = smallest_boundary_file->largestInternalKey_;
            } else {
                continue_searching = false;
            }
        }
    }

    void VersionSet::SetupOtherInputs(Compaction *c) {
        const int level = c->level();
        InternalKey smallest, largest;

        AddBoundaryInputs(internalKeyComparator_, currentVersion_->fileMetaDataVecArr[level], &c->inputs_[0]);
        GetRange(c->inputs_[0], &smallest, &largest);

        currentVersion_->GetOverlappingInputs(level + 1, &smallest, &largest,
                                              &c->inputs_[1]);

        // Get entire range covered by compaction
        InternalKey all_start, all_limit;
        GetRange2(c->inputs_[0], c->inputs_[1], &all_start, &all_limit);

        // See if we can grow the number of inputs in "level" without
        // changing the number of "level+1" files we pick up.
        if (!c->inputs_[1].empty()) {
            std::vector<FileMetaData *> expanded0;
            currentVersion_->GetOverlappingInputs(level, &all_start, &all_limit, &expanded0);
            AddBoundaryInputs(internalKeyComparator_, currentVersion_->fileMetaDataVecArr[level], &expanded0);
            const int64_t inputs0_size = TotalFileSize(c->inputs_[0]);
            const int64_t inputs1_size = TotalFileSize(c->inputs_[1]);
            const int64_t expanded0_size = TotalFileSize(expanded0);
            if (expanded0.size() > c->inputs_[0].size() &&
                inputs1_size + expanded0_size <
                ExpandedCompactionByteSizeLimit(options_)) {
                InternalKey new_start, new_limit;
                GetRange(expanded0, &new_start, &new_limit);
                std::vector<FileMetaData *> expanded1;
                currentVersion_->GetOverlappingInputs(level + 1, &new_start, &new_limit,
                                                      &expanded1);
                if (expanded1.size() == c->inputs_[1].size()) {
                    Log(options_->logger,
                        "Expanding@%d %d+%d (%ld+%ld bytes) to %d+%d (%ld+%ld bytes)\n",
                        level, int(c->inputs_[0].size()), int(c->inputs_[1].size()),
                        long(inputs0_size), long(inputs1_size), int(expanded0.size()),
                        int(expanded1.size()), long(expanded0_size), long(inputs1_size));
                    smallest = new_start;
                    largest = new_limit;
                    c->inputs_[0] = expanded0;
                    c->inputs_[1] = expanded1;
                    GetRange2(c->inputs_[0], c->inputs_[1], &all_start, &all_limit);
                }
            }
        }

        // Compute the set of grandparent files that overlap this compaction
        // (parent == level+1; grandparent == level+2)
        if (level + 2 < config::kNumLevels) {
            currentVersion_->GetOverlappingInputs(level + 2, &all_start, &all_limit,
                                                  &c->grandparents_);
        }

        // Update the place where we will do the next compaction for this level.
        // We update this immediately instead of waiting for the VersionEdit
        // to be applied so that if the compaction fails, we will try a different
        // key range next time.
        compact_pointer_[level] = largest.Encode().ToString();
        c->edit_.SetCompactPointer(level, largest);
    }

    Compaction *VersionSet::CompactRange(int level, const InternalKey *begin,
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
        c->input_version_ = currentVersion_;
        c->input_version_->Ref();
        c->inputs_[0] = inputs;
        SetupOtherInputs(c);
        return c;
    }

    Compaction::Compaction(const Options *options, int level)
            : level_(level),
              max_output_file_size_(MaxFileSizeForLevel(options, level)),
              input_version_(nullptr),
              grandparent_index_(0),
              seen_key_(false),
              overlapped_bytes_(0) {
        for (int i = 0; i < config::kNumLevels; i++) {
            level_ptrs_[i] = 0;
        }
    }

    Compaction::~Compaction() {
        if (input_version_ != nullptr) {
            input_version_->Unref();
        }
    }

    bool Compaction::IsTrivialMove() const {
        const VersionSet *belongingVersionSet = input_version_->belongingVersionSet;
        // Avoid a move if there is lots of overlapping grandparent data.
        // Otherwise, the move could create a parent file that will require
        // a very expensive merge later on.
        return (num_input_files(0) == 1 &&
                num_input_files(1) == 0 &&
                TotalFileSize(grandparents_) <= MaxGrandParentOverlapBytes(belongingVersionSet->options_));
    }

    void Compaction::AddInputDeletions(VersionEdit *edit) {
        for (int which = 0; which < 2; which++) {
            for (size_t i = 0; i < inputs_[which].size(); i++) {
                edit->RemoveFile(level_ + which, inputs_[which][i]->number);
            }
        }
    }

    bool Compaction::IsBaseLevelForKey(const Slice &user_key) {
        // Maybe use binary search to find right entry instead of linear search?
        const Comparator *user_cmp = input_version_->belongingVersionSet->internalKeyComparator_.user_comparator();
        for (int lvl = level_ + 2; lvl < config::kNumLevels; lvl++) {
            const std::vector<FileMetaData *> &files = input_version_->fileMetaDataVecArr[lvl];
            while (level_ptrs_[lvl] < files.size()) {
                FileMetaData *f = files[level_ptrs_[lvl]];
                if (user_cmp->Compare(user_key, f->largestInternalKey_.user_key()) <= 0) {
                    // We've advanced far enough
                    if (user_cmp->Compare(user_key, f->smallestInternalKey_.user_key()) >= 0) {
                        // Key falls in this file's range, so definitely not base level
                        return false;
                    }
                    break;
                }
                level_ptrs_[lvl]++;
            }
        }
        return true;
    }

    bool Compaction::ShouldStopBefore(const Slice &internal_key) {
        const VersionSet *vset = input_version_->belongingVersionSet;
        // Scan to find earliest grandparent file that contains key.
        const InternalKeyComparator *icmp = &vset->internalKeyComparator_;
        while (grandparent_index_ < grandparents_.size() &&
                icmp->Compare(internal_key,
                             grandparents_[grandparent_index_]->largestInternalKey_.Encode()) >
                0) {
            if (seen_key_) {
                overlapped_bytes_ += grandparents_[grandparent_index_]->fileSize_;
            }
            grandparent_index_++;
        }
        seen_key_ = true;

        if (overlapped_bytes_ > MaxGrandParentOverlapBytes(vset->options_)) {
            // Too much overlap for current output; start new output
            overlapped_bytes_ = 0;
            return true;
        } else {
            return false;
        }
    }

    void Compaction::ReleaseInputs() {
        if (input_version_ != nullptr) {
            input_version_->Unref();
            input_version_ = nullptr;
        }
    }

}  // namespace leveldb
