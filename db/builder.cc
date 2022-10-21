// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/builder.h"

#include "db/dbformat.h"
#include "db/filename.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"

namespace leveldb {

    // 生成sst
    Status BuildTable(const std::string &dbname,
                      Env *env,
                      const Options &options,
                      TableCache *tableCache,
                      Iterator *memTableIter,
                      FileMetaData *fileMetaData) {
        fileMetaData->fileSize_ = 0;
        memTableIter->SeekToFirst();
        std::string ldbFilePath = TableFileName(dbname, fileMetaData->number);

        Status status;
        if (memTableIter->Valid()) {
            WritableFile *ldbFile;
            status = env->NewWritableFile(ldbFilePath, &ldbFile);
            if (!status.ok()) {
                return status;
            }

            auto *tableBuilder = new TableBuilder(options, ldbFile);

            fileMetaData->smallestInternalKey_.DecodeFrom(memTableIter->key());

            // 遍历
            Slice key;
            for (; memTableIter->Valid(); memTableIter->Next()) {
                key = memTableIter->key();
                tableBuilder->Add(key, memTableIter->value());
            }

            if (!key.empty()) {
                fileMetaData->largestInternalKey_.DecodeFrom(key);
            }

            // 收尾
            status = tableBuilder->Finish();
            if (status.ok()) {
                fileMetaData->fileSize_ = tableBuilder->FileSize();
                assert(fileMetaData->fileSize_ > 0);
            }

            delete tableBuilder;

            // Finish and check for ldbFile errors
            if (status.ok()) {
                status = ldbFile->Sync();
            }
            if (status.ok()) {
                status = ldbFile->Close();
            }
            delete ldbFile;
            ldbFile = nullptr;

            if (status.ok()) {
                // verify that the table is usable 也有刷缓存的用途
                Iterator *iterator = tableCache->NewIterator(ReadOptions(),
                                                             fileMetaData->number,
                                                             fileMetaData->fileSize_);
                status = iterator->status();
                delete iterator;
            }
        }

        // Check for input memTableIter errors
        if (!memTableIter->status().ok()) {
            status = memTableIter->status();
        }

        if (status.ok() && fileMetaData->fileSize_ > 0) {
            // Keep it
        } else {
            env->RemoveFile(ldbFilePath);
        }

        return status;
    }

}