// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Thread-safe (provides internal synchronization)

#ifndef STORAGE_LEVELDB_DB_TABLE_CACHE_H_
#define STORAGE_LEVELDB_DB_TABLE_CACHE_H_

#include <cstdint>
#include <string>

#include "db/dbformat.h"
#include "leveldb/cache.h"
#include "leveldb/table.h"
#include "port/port.h"

namespace leveldb {

    class Env;

    // 内部有包含 cache
    class TableCache {
    public:
        TableCache(const std::string &dbname,
                   const Options &options,
                   int entries);

        ~TableCache();

        // 返回的是 table对象上的iterator
        //
        // Return an iterator for the specified file number (the corresponding
        // file length must be exactly "fileSize_" bytes).  If "tableptr" is
        // non-null, also sets "*tableptr" to point to the Table object
        // underlying the returned iterator, or to nullptr if no Table object
        // underlies the returned iterator.  The returned "*tableptr" object is owned
        // by the cache and should not be deleted, and is valid for as long as the
        // returned iterator is live.
        Iterator *NewIterator(const ReadOptions &readOptions,
                              uint64_t fileNumber,
                              uint64_t fileSize,
                              Table **tableptr = nullptr);

        // If a seek to internal key "internalKey" in specified file finds an entry,
        // call (*resultHandler)(arg, found_key, found_value).
        Status Get(const ReadOptions &readOptions,
                   uint64_t fileNumber,
                   uint64_t fileSize,
                   const Slice &internalKey,
                   void *arg,
                   void (*resultHandler)(void *, const Slice &, const Slice &));

        // Evict any entry for the specified file number
        void Evict(uint64_t file_number);

    private:
        Status FindTable(uint64_t fileNumber,
                         uint64_t fileSize,
                         Cache::Handle **);

        Env *const env_;
        const std::string dbname_;
        const Options &options_;
        // key是fileNumber value是TableAndFile
        Cache *cache_;
    };

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_TABLE_CACHE_H_
