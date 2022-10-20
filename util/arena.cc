// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/arena.h"

namespace leveldb {

    static const int kBlockSize = 4096;

    Arena::Arena()
            : alloc_ptr_(nullptr), allocateByteCoutRemaining(0), memory_usage_(0) {}

    Arena::~Arena() {
        for (size_t i = 0; i < blockVec.size(); i++) {
            delete[] blockVec[i];
        }
    }

    char *Arena::AllocateFallback(size_t bytes) {
        // Object is more than a quarter of our block size.
        // Allocate it separately to avoid wasting too much space in leftover byte
        if (bytes > kBlockSize / 4) {
            return AllocateNewBlock(bytes);
        }

        // We waste the remaining space in the current block.
        alloc_ptr_ = AllocateNewBlock(kBlockSize);
        allocateByteCoutRemaining = kBlockSize;

        char *result = alloc_ptr_;
        alloc_ptr_ += bytes;
        allocateByteCoutRemaining -= bytes;
        return result;
    }

    char *Arena::AllocateAligned(size_t bytes) {
        const int align = (sizeof(void *) > 8) ? sizeof(void *) : 8;
        static_assert((align & (align - 1)) == 0, "Pointer size should be a power of 2");

        // 效果 alloc_ptr_ % align
        size_t mod = reinterpret_cast<uintptr_t>(alloc_ptr_) & (align - 1);
        size_t padding = (mod == 0 ? 0 : align - mod);
        size_t needed = bytes + padding;

        char *result;
        if (needed <= allocateByteCoutRemaining) {
            result = alloc_ptr_ + padding; // 把padding忽视掉
            alloc_ptr_ += needed;
            allocateByteCoutRemaining -= needed;
        } else {
            // AllocateFallback always returned aligned memory
            result = AllocateFallback(bytes);
        }

        assert((reinterpret_cast<uintptr_t>(result) & (align - 1)) == 0);

        return result;
    }

    char *Arena::AllocateNewBlock(size_t block_bytes) {
        char *result = new char[block_bytes];
        blockVec.push_back(result);
        memory_usage_.fetch_add(block_bytes + sizeof(char *), std::memory_order_relaxed);
        return result;
    }

}  // namespace leveldb
