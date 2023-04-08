// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/table_cache.h"

#include "db/filename.h"
#include "leveldb/env.h"
#include "leveldb/table.h"
#include "util/coding.h"

namespace leveldb {

    // table对象和对应的ldb
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
    // 本质是得到table and file
    Status TableCache::FindTable(uint64_t fileNumber, uint64_t fileSize, Cache::Handle **handle) {
        Status status;

        char buf[sizeof(fileNumber)];
        EncodeFixed64(buf, fileNumber);
        Slice key(buf, sizeof(buf));

        // lruHandle
        *handle = cache_->Lookup(key);
        if (*handle == nullptr) {
            // 得到对应的ldb文件,也便是table对应的文件
            std::string ldbFilePath = TableFileName(dbname_, fileNumber);
            RandomAccessFile *ldbFile = nullptr;
            status = env_->NewRandomAccessFile(ldbFilePath, &ldbFile);
            if (!status.ok()) {
                // 老版本中后缀不是ldb而是那个广为流传的sst
                std::string tableFilePathSST = SSTTableFileName(dbname_, fileNumber);
                if (env_->NewRandomAccessFile(tableFilePathSST, &ldbFile).ok()) {
                    status = Status::OK();
                }
            }

            // 得到table对象
            Table *table = nullptr;
            if (status.ok()) {
                status = Table::Open(options_, ldbFile, fileSize, &table);
            }

            if (!status.ok()) {
                assert(table == nullptr);
                delete ldbFile;
                // We do not cache error results so that if the error is transient,
                // or somebody repairs the ldbFile, we recover automatically.
            } else {
                auto *tableAndFile = new TableAndFile();
                tableAndFile->file = ldbFile;
                tableAndFile->table = table;
                // 读取到的tableAndFile添加到cache的
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

    Status TableCache::Get(const ReadOptions &readOptions,
                           uint64_t fileNumber,
                           uint64_t fileSize,
                           const Slice &internalKey,
                           void *arg,  // saver
                           void (*resultHandler)(void *, const Slice &internalKey, const Slice &value)) {
        Cache::Handle *handle = nullptr;
        Status status = this->FindTable(fileNumber, fileSize, &handle);
        if (status.ok()) {
            Table *table = reinterpret_cast<TableAndFile *>(cache_->Value(handle))->table;
            status = table->InternalGet(readOptions, internalKey, arg, resultHandler);
            cache_->Release(handle);
        }
        return status;
    }

    void TableCache::Evict(uint64_t file_number) {
        char buf[sizeof(file_number)];
        EncodeFixed64(buf, file_number);
        cache_->Erase(Slice(buf, sizeof(buf)));
    }

}  // namespace leveldb
