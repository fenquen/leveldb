// WriteBatch holds a collection of updates to apply atomically to a DB.
//
// The updates are applied in the order in which they are added
// to the WriteBatch.  For example, the value of "key" will be "v3"
// after the following batch is written:
//
//    batch.Put("key", "v1");
//    batch.Delete("key");
//    batch.Put("key", "v2");
//    batch.Put("key", "v3");
//
// Multiple threads can invoke const methods on a WriteBatch without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same WriteBatch must use
// external synchronization.

#ifndef STORAGE_LEVELDB_INCLUDE_WRITE_BATCH_H_
#define STORAGE_LEVELDB_INCLUDE_WRITE_BATCH_H_

#include <string>

#include "leveldb/export.h"
#include "leveldb/status.h"

namespace leveldb {
    class Slice;

    // writeBatch数据的格式,要是多个action在1个batch中共享相同的sequence
    // WriteBatch::rep_ :=
    //            sequence: fixed64
    //            count: fixed32
    //            data: record[count]
    //        record :=
    //            kTypeValue varstring varstring         |
    //            kTypeDeletion varstring
    //        varstring :=
    //           len: varint32
    //           data: uint8[len]
    class LEVELDB_EXPORT WriteBatch {
    public:
        class LEVELDB_EXPORT Handler {
        public:
            virtual ~Handler();

            virtual void Put(const Slice &key, const Slice &value) = 0;

            virtual void Delete(const Slice &key) = 0;
        };

        WriteBatch();

        // Intentionally copyable.
        WriteBatch(const WriteBatch &) = default;

        WriteBatch &operator=(const WriteBatch &) = default;

        ~WriteBatch();

        // Store the mapping "key->value" in the database.
        void Put(const Slice &key, const Slice &value);

        // If the database contains a mapping for "key", erase it.  Else do nothing.
        void Delete(const Slice &key);

        // Clear all updates buffered in this batch.
        void Clear();

        // The size of the database changes caused by this batch.
        //
        // This number is tied to implementation details, and may change across
        // releases. It is intended for LevelDB usage metrics.
        size_t ApproximateSize() const;

        // Copies the operations in "source" to this batch.
        //
        // This runs in O(source size) time. However, the constant factor is better
        // than calling Iterate() over the source batch with a Handler that replicates
        // the operations into this batch.
        void Append(const WriteBatch &source);

        // Support for iterating over the contents of a batch.
        Status Iterate(Handler *handler) const;

    private:
        friend class WriteBatchInternal;

        std::string rep_;  // See comment in write_batch.cc for the format of rep_
    };

}

#endif
