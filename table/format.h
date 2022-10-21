// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_TABLE_FORMAT_H_
#define STORAGE_LEVELDB_TABLE_FORMAT_H_

#include <cstdint>
#include <string>

#include "leveldb/slice.h"
#include "leveldb/status.h"
#include "leveldb/table_builder.h"

namespace leveldb {

    class Block;

    class RandomAccessFile;

    struct ReadOptions;

    // a pointer to the extent of a file that stores a data block or a meta block.
    class BlockHandle {
    public:
        // Maximum encoding length of a BlockHandle
        enum {
            kMaxEncodedLength = 10 + 10
        };

        BlockHandle();

        void EncodeTo(std::string *dst) const;

        Status DecodeFrom(Slice *input);

        // The offset of the block in the file.
        uint64_t offset() const {
            return offset_;
        }

        void set_offset(uint64_t offset) {
            offset_ = offset;
        }

        // The size of the stored block
        uint64_t size() const {
            return size_;
        }

        void set_size(uint64_t size) {
            size_ = size;
        }

    private:
        uint64_t offset_;
        uint64_t size_;
    };

    // Footer encapsulates the fixed information stored at the tail  end of every table file.
    class Footer {
    public:
        // Encoded length of a Footer.  Note that the serialization of a
        // Footer will always occupy exactly this many bytes.  It consists
        // of two block handles and a magic number.
        enum {
            ENCODED_LEN = 2 * BlockHandle::kMaxEncodedLength + 8
        };

        Footer() = default;

        const BlockHandle &metaindex_handle() const {
            return metaIndexHandle_;
        }

        void set_metaindex_handle(const BlockHandle &h) {
            metaIndexHandle_ = h;
        }

        const BlockHandle &index_handle() const {
            return indexHandle_;
        }

        void set_index_handle(const BlockHandle &h) {
            indexHandle_ = h;
        }

        void EncodeTo(std::string *dst) const;

        Status DecodeFrom(Slice *src);

    private:
        BlockHandle metaIndexHandle_;
        BlockHandle indexHandle_;
    };

    // kTableMagicNumber was picked by running echo http://code.google.com/p/leveldb/ | sha1sum and taking the leading 64 bits.
    static const uint64_t kTableMagicNumber = 0xdb4775248b80fb57ull;

    // 1 字节 compressionType + 4字节 crc
    static const size_t BLOCK_TRAILER_SIZE = 5;

    struct BlockContents {
        Slice data;           // Actual contents of data
        bool cachable;        // true if data can be cached
        bool allocatedFromHeap_;  // true if caller should delete[] data.data()
    };

    // Read the block identified by "blockHandle" from "ldbFile".  On failure
    // return non-OK.  On success fill *result and return OK.
    Status ReadBlock(RandomAccessFile *ldbFile, const ReadOptions &readOptions,
                     const BlockHandle &blockHandle, BlockContents *result);

    // Implementation details follow.  Client should ignore,
    inline BlockHandle::BlockHandle() : offset_(~static_cast<uint64_t>(0)),
                                        size_(~static_cast<uint64_t>(0)) {}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_FORMAT_H_
