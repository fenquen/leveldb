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
                      Iterator *iterator,
                      FileMetaData *fileMetaData) {
        fileMetaData->file_size = 0;
        iterator->SeekToFirst();
        std::string ldbFileName = TableFileName(dbname, fileMetaData->number);

        Status status;
        if (iterator->Valid()) {
            WritableFile *ldbFile;
            status = env->NewWritableFile(ldbFileName, &ldbFile);
            if (!status.ok()) {
                return status;
            }

            auto *tableBuilder = new TableBuilder(options, ldbFile);
            fileMetaData->smallest.DecodeFrom(iterator->key());

            // 遍历
            Slice key;
            for (; iterator->Valid(); iterator->Next()) {
                key = iterator->key();
                tableBuilder->Add(key, iterator->value());
            }

            if (!key.empty()) {
                fileMetaData->largest.DecodeFrom(key);
            }

            // Finish and check for tableBuilder errors
            status = tableBuilder->Finish();
            if (status.ok()) {
                fileMetaData->file_size = tableBuilder->FileSize();
                assert(fileMetaData->file_size > 0);
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
                // Verify that the table is usable
                Iterator *it = tableCache->NewIterator(ReadOptions(),
                                                       fileMetaData->number,
                                                       fileMetaData->file_size);
                status = it->status();
                delete it;
            }
        }

        // Check for input iterator errors
        if (!iterator->status().ok()) {
            status = iterator->status();
        }

        if (status.ok() && fileMetaData->file_size > 0) {
            // Keep it
        } else {
            env->RemoveFile(ldbFileName);
        }

        return status;
    }

}