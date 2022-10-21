// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/table_cache.h"

#include "db/filename.h"
#include "leveldb/env.h"
#include "leveldb/table.h"
#include "util/coding.h"

namespace leveldb {

    struct TableAndFile {
        RandomAccessFile *file;
        Table *table;
    };

    static void DeleteEntry(const Slice &key, void *value) {
        TableAndFile *tf = reinterpret_cast<TableAndFile *>(value);
        delete tf->table;
        delete tf->file;
        delete tf;
    }

    static void UnrefEntry(void *arg1, void *arg2) {
        Cache *cache = reinterpret_cast<Cache *>(arg1);
        Cache::Handle *h = reinterpret_cast<Cache::Handle *>(arg2);
        cache->Release(h);
    }

    TableCache::TableCache(const std::string &dbname,
                           const Options &options,
                           int entries) : env_(options.env),
                                          dbname_(dbname),
                                          options_(options),
                                          cache_(NewLRUCache(entries)) {

    }

    TableCache::~TableCache() {
        delete cache_;
    }

    // 得到table都要过cache
    Status TableCache::FindTable(uint64_t fileNumber, uint64_t fileSize, Cache::Handle **handle) {
        Status status;

        char buf[sizeof(fileNumber)];
        EncodeFixed64(buf, fileNumber);
        Slice key(buf, sizeof(buf));

        *handle = cache_->Lookup(key);
        if (*handle == nullptr) {
            std::string tableFilePath = TableFileName(dbname_, fileNumber);

            RandomAccessFile *randomAccessFile = nullptr;
            Table *table = nullptr;

            // 得到对应的lbd文件
            status = env_->NewRandomAccessFile(tableFilePath, &randomAccessFile);
            if (!status.ok()) {
                // 老版本中后缀不是ldb而是那个广为流传的sst
                std::string tableFilePathSST = SSTTableFileName(dbname_, fileNumber);
                if (env_->NewRandomAccessFile(tableFilePathSST, &randomAccessFile).ok()) {
                    status = Status::OK();
                }
            }

            if (status.ok()) {
                status = Table::Open(options_, randomAccessFile, fileSize, &table);
            }

            if (!status.ok()) {
                assert(table == nullptr);
                delete randomAccessFile;
                // We do not cache error results so that if the error is transient,
                // or somebody repairs the randomAccessFile, we recover automatically.
            } else {
                auto *tableAndFile = new TableAndFile();
                tableAndFile->file = randomAccessFile;
                tableAndFile->table = table;
                *handle = cache_->Insert(key, tableAndFile, 1, &DeleteEntry);
            }
        }

        return status;
    }

    Iterator *TableCache::NewIterator(const ReadOptions &readOptions,
                                      uint64_t fileNumber,
                                      uint64_t fileSize,
                                      Table **tableptr) {
        if (tableptr != nullptr) {
            *tableptr = nullptr;
        }

        Cache::Handle *handle = nullptr;
        Status status = this->FindTable(fileNumber, fileSize, &handle);
        if (!status.ok()) {
            return NewErrorIterator(status);
        }

        Table *table = reinterpret_cast<TableAndFile *>(cache_->Value(handle))->table;

        Iterator *iterator = table->NewIterator(readOptions);
        iterator->RegisterCleanup(&UnrefEntry, cache_, handle);

        if (tableptr != nullptr) {
            *tableptr = table;
        }

        return iterator;
    }

    Status TableCache::Get(const ReadOptions &options, uint64_t file_number,
                           uint64_t file_size, const Slice &k, void *arg,
                           void (*handle_result)(void *, const Slice &,
                                                 const Slice &)) {
        Cache::Handle *handle = nullptr;
        Status s = FindTable(file_number, file_size, &handle);
        if (s.ok()) {
            Table *t = reinterpret_cast<TableAndFile *>(cache_->Value(handle))->table;
            s = t->InternalGet(options, k, arg, handle_result);
            cache_->Release(handle);
        }
        return s;
    }

    void TableCache::Evict(uint64_t file_number) {
        char buf[sizeof(file_number)];
        EncodeFixed64(buf, file_number);
        cache_->Erase(Slice(buf, sizeof(buf)));
    }

}  // namespace leveldb
