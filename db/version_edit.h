// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_VERSION_EDIT_H_
#define STORAGE_LEVELDB_DB_VERSION_EDIT_H_

#include <set>
#include <utility>
#include <vector>

#include "db/dbformat.h"

namespace leveldb {

    class VersionSet;

    struct FileMetaData {
        FileMetaData() : refs(0),
                         allowedSeeks_(1 << 30),
                         fileSize_(0) {

        }

        int refs;
        int allowedSeeks_;  // seeks allowed until compaction_
        uint64_t number;
        uint64_t fileSize_;    // file size in byte
        InternalKey smallestInternalKey_;  // Smallest internal key served by table
        InternalKey largestInternalKey_;   // Largest internal key served by table
    };

    class VersionEdit {
    public:
        VersionEdit() {
            Clear();
        }

        ~VersionEdit() = default;

        void Clear();

        void SetComparatorName(const Slice &name) {
            has_comparator_ = true;
            comparatorName_ = name.ToString();
        }

        void SetLogNumber(uint64_t num) {
            has_log_number_ = true;
            log_number_ = num;
        }

        void SetPrevLogNumber(uint64_t num) {
            has_prev_log_number_ = true;
            prev_log_number_ = num;
        }

        void SetNextFile(uint64_t num) {
            has_next_file_number_ = true;
            next_file_number_ = num;
        }

        void SetLastSequence(SequenceNumber seq) {
            has_last_sequence_ = true;
            last_sequence_ = seq;
        }

        void SetCompactPointer(int level, const InternalKey &key) {
            compact_pointers_.push_back(std::make_pair(level, key));
        }

        // 增加1条fileMetaData记录
        // Add the specified file at the specified number.
        // REQUIRES: This version has not been saved (see VersionSet::SaveTo)
        // REQUIRES: "smallestInternalKey_" and "largestInternalKey_" are smallestInternalKey_ and largestInternalKey_ keys in file
        void AddFile(int level,
                     uint64_t file,
                     uint64_t file_size,
                     const InternalKey &smallest,
                     const InternalKey &largest) {
            FileMetaData fileMetaData;
            fileMetaData.number = file;
            fileMetaData.fileSize_ = file_size;
            fileMetaData.smallestInternalKey_ = smallest;
            fileMetaData.largestInternalKey_ = largest;
            addedLevelFileMetaDataVec_.emplace_back(level, fileMetaData);
        }

        // Delete the specified "file" from the specified "level".
        void RemoveFile(int level, uint64_t file) {
            deletedLevelFileNumberSet_.insert(std::make_pair(level, file));
        }

        void EncodeTo(std::string *dst) const;

        Status DecodeFrom(const Slice &src);

        std::string DebugString() const;

    private:
        friend class VersionSet;

        std::string comparatorName_;
        uint64_t log_number_; // earlier logs no longer needed
        uint64_t prev_log_number_; // 0 no older logs needed after recovery
        uint64_t next_file_number_;
        SequenceNumber last_sequence_;
        bool has_comparator_;
        bool has_log_number_;
        bool has_prev_log_number_;
        bool has_next_file_number_;
        bool has_last_sequence_;

        // level_internalKey
        std::vector<std::pair<int, InternalKey>> compact_pointers_;

        // 干掉的的 level_fileNumber
        // typedef std::set<std::pair<int, uint64_t>> DeletedFileSet;
        std::set<std::pair<int, uint64_t>> deletedLevelFileNumberSet_;

        // 新增的
        std::vector<std::pair<int, FileMetaData>> addedLevelFileMetaDataVec_;
    };

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_VERSION_EDIT_H_
