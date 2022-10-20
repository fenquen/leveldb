// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// BlockBuilder generates blocks where keys are prefix-compressed:
//
// When we store a key, we drop the prefix shared with the previous
// string.  This helps reduce the space requirement significantly.
// Furthermore, once every K keys, we do not apply the prefix
// compression and store the entire key.  We call this a "restart
// point".  The tail end of the block stores the offsets of all of the
// restart points, and can be used to do a binary search when looking
// for a particular key.  Values are stored as-is (without compression)
// immediately following the corresponding key.
//
// An entry for a particular key-value pair has the form:
//     shared_bytes: varint32
//     unshared_bytes: varint32
//     value_length: varint32
//     key_delta: char[unshared_bytes]
//     value: char[value_length]
// shared_bytes == 0 for restart points.
//
// The trailer of the block has the form:
//     restarts: uint32[num_restarts]
//     num_restarts: uint32
// restarts[i] contains the offset within the block of the ith restart point.

#include "table/block_builder.h"

#include <algorithm>
#include <cassert>

#include "leveldb/comparator.h"
#include "leveldb/options.h"
#include "util/coding.h"

namespace leveldb {

    BlockBuilder::BlockBuilder(const Options *options) : options_(options),
                                                         restartPointVec_(),
                                                         counter_(0),
                                                         finished_(false) {
        assert(options->blockRestartInterval >= 1);
        restartPointVec_.push_back(0);  // First restart point is at offset 0
    }

    void BlockBuilder::Reset() {
        buffer_.clear();
        restartPointVec_.clear();
        restartPointVec_.push_back(0);  // First restart point is at offset 0
        counter_ = 0;
        finished_ = false;
        lastKey_.clear();
    }

    size_t BlockBuilder::CurrentSizeEstimate() const {
        return (buffer_.size() +                       // Raw data buffer
                restartPointVec_.size() * sizeof(uint32_t) +  // Restart array
                sizeof(uint32_t));                     // Restart array length
    }

    Slice BlockBuilder::Finish() {
        // Append restart array
        for (unsigned int restart : restartPointVec_) {
            PutFixed32(&buffer_, restart);
        }
        PutFixed32(&buffer_, restartPointVec_.size());
        finished_ = true;
        return Slice(buffer_);
    }

    // https://blog.csdn.net/xxb249/article/details/94559781
    void BlockBuilder::Add(const Slice &key, const Slice &value) {
        Slice lastKeyPiece(lastKey_);

        assert(!finished_);
        assert(counter_ <= options_->blockRestartInterval);
        assert(buffer_.empty() || options_->comparator->Compare(key, lastKeyPiece) > 0);

        // 和前1个的key相比相同的byte数量
        // 例如 前1个是 hello 后1个是helloaa 那么相同的数量是5
        size_t sharedByteLen = 0;
        if (counter_ < options_->blockRestartInterval) {
            // See how much sharing to do with previous string
            const size_t min_length = std::min(lastKeyPiece.size(), key.size());
            while ((sharedByteLen < min_length) && (lastKeyPiece[sharedByteLen] == key[sharedByteLen])) {
                sharedByteLen++;
            }
        } else {
            // Restart compression
            restartPointVec_.push_back(buffer_.size());
            counter_ = 0;
        }

        const size_t nonSharedByteLen = key.size() - sharedByteLen;

        // key值相同部分长度
        PutVarint32(&buffer_, sharedByteLen);
        // key值不同部分长度
        PutVarint32(&buffer_, nonSharedByteLen);
        // value长度
        PutVarint32(&buffer_, value.size());
        // key不同内容
        buffer_.append(key.data() + sharedByteLen, nonSharedByteLen);
        // value内容
        buffer_.append(value.data(), value.size());

        // Update state
        lastKey_.resize(sharedByteLen);
        lastKey_.append(key.data() + sharedByteLen, nonSharedByteLen);
        assert(Slice(lastKey_) == key);
        counter_++;
    }

}  // namespace leveldb
