// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_WRITE_BATCH_INTERNAL_H_
#define STORAGE_LEVELDB_DB_WRITE_BATCH_INTERNAL_H_

#include "db/dbformat.h"
#include "leveldb/write_batch.h"

namespace leveldb {

    class MemTable;

// WriteBatchInternal provides static methods for manipulating a
// WriteBatch that we don't want in the public WriteBatch interface.
    class WriteBatchInternal {
    public:
        // Return the number of entries in the writeBatch.
        static int Count(const WriteBatch *writeBatch);

        // Set the count for the number of entries in the writeBatch_.
        static void SetCount(WriteBatch *writeBatch, int n);

        // Return the sequence number for the start of this writeBatch.
        static SequenceNumber Sequence(const WriteBatch *writeBatch);

        // Store the specified number as the sequence number for the start of
        // this writeBatch_.
        static void SetSequence(WriteBatch *batch, SequenceNumber seq);

        static Slice Contents(const WriteBatch *batch) {
            return Slice(batch->rep_);
        }

        static size_t ByteSize(const WriteBatch *batch) { return batch->rep_.size(); }

        static void SetContents(WriteBatch *writeBatch, const Slice &contents);

        static Status InsertIntoMemTable(const WriteBatch *writeBatch, MemTable *memtable);

        static void Append(WriteBatch *dest, const WriteBatch *src);
    };

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_WRITE_BATCH_INTERNAL_H_
