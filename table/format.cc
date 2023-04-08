// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/format.h"

#include "leveldb/env.h"
#include "port/port.h"
#include "table/block.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {

    void BlockHandle::EncodeTo(std::string *dst) const {
        // Sanity check that all fields have been set
        assert(offset_ != ~static_cast<uint64_t>(0));
        assert(size_ != ~static_cast<uint64_t>(0));
        PutVarint64(dst, offset_);
        PutVarint64(dst, size_);
    }

    Status BlockHandle::DecodeFrom(Slice *input) {
        if (GetVarint64(input, &offset_) && GetVarint64(input, &size_)) {
            return Status::OK();
        }

        return Status::Corruption("bad block handle");
    }

    void Footer::EncodeTo(std::string *dst) const {
        const size_t original_size = dst->size();
        metaIndexHandle_.EncodeTo(dst);
        indexHandle_.EncodeTo(dst);
        dst->resize(2 * BlockHandle::kMaxEncodedLength);  // Padding
        PutFixed32(dst, static_cast<uint32_t>(kTableMagicNumber & 0xffffffffu));
        PutFixed32(dst, static_cast<uint32_t>(kTableMagicNumber >> 32));
        assert(dst->size() == original_size + ENCODED_LEN);
        (void) original_size;  // Disable unused variable warning.
    }

    // 格式 https://blog.csdn.net/xxb249/article/details/94559781
    Status Footer::DecodeFrom(Slice *src) {
        const char *magicPos = src->data() + ENCODED_LEN - 8;
        // 小端模式
        const uint32_t magicLow = DecodeFixed32(magicPos);
        const uint32_t magicHigh = DecodeFixed32(magicPos + 4);
        const uint64_t magic = ((static_cast<uint64_t>(magicHigh) << 32) | (static_cast<uint64_t>(magicLow)));
        if (magic != kTableMagicNumber) {
            return Status::Corruption("not an sstable (bad magic number)");
        }

        Status result = metaIndexHandle_.DecodeFrom(src);
        if (result.ok()) {
            result = indexHandle_.DecodeFrom(src);
        }

        if (result.ok()) {
            // We skip over any leftover data (just padding for now) in "src"
            const char *end = magicPos + 8;
            *src = Slice(end, src->data() + src->size() - end);
        }

        return result;
    }

    Status ReadBlock(RandomAccessFile *ldbFile,
                     const ReadOptions &readOptions,
                     const BlockHandle &blockHandle,
                     BlockContents *result) {
        result->data = Slice();
        result->cachable = false;
        result->allocatedFromHeap_ = false;

        // Read the block contents as well as the type/crc footer.
        // See table_builder.cc for the code that built this structure.
        auto n = static_cast<size_t>(blockHandle.size());
        char *buf = new char[n + BLOCK_TRAILER_SIZE];
        Slice contents;
        Status status = ldbFile->Read(blockHandle.offset(), n + BLOCK_TRAILER_SIZE, &contents, buf);
        if (!status.ok()) {
            delete[] buf;
            return status;
        }

        if (contents.size() != n + BLOCK_TRAILER_SIZE) {
            delete[] buf;
            return Status::Corruption("truncated block read");
        }

        // Check the crc of the type and the block contents
        const char *data = contents.data();  // Pointer to where Read put the data

        if (readOptions.verifyChecksums_) {
            const uint32_t crc = crc32c::Unmask(DecodeFixed32(data + n + 1));
            const uint32_t actual = crc32c::Value(data, n + 1);
            if (actual != crc) {
                delete[] buf;
                status = Status::Corruption("block checksum mismatch");
                return status;
            }
        }

        switch (data[n]) {
            case kNoCompression:
                if (data != buf) {
                    // File implementation gave us pointer to some other data.
                    // Use it directly under the assumption that it will be live
                    // while the ldbFile is open.
                    delete[] buf;
                    result->data = Slice(data, n);
                    result->allocatedFromHeap_ = false;
                    result->cachable = false;  // Do not double-cache
                } else {
                    result->data = Slice(buf, n);
                    result->allocatedFromHeap_ = true;
                    result->cachable = true;
                }

                // Ok
                break;
            case kSnappyCompression: {
                size_t ulength = 0;
                if (!port::Snappy_GetUncompressedLength(data, n, &ulength)) {
                    delete[] buf;
                    return Status::Corruption("corrupted compressed block contents");
                }
                char *ubuf = new char[ulength];
                if (!port::Snappy_Uncompress(data, n, ubuf)) {
                    delete[] buf;
                    delete[] ubuf;
                    return Status::Corruption("corrupted compressed block contents");
                }
                delete[] buf;
                result->data = Slice(ubuf, ulength);
                result->allocatedFromHeap_ = true;
                result->cachable = true;
                break;
            }
            default:
                delete[] buf;
                return Status::Corruption("bad block type");
        }

        return Status::OK();
    }

}  // namespace leveldb
