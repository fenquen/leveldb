// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_MEMTABLE_H_
#define STORAGE_LEVELDB_DB_MEMTABLE_H_

#include <string>

#include "db/dbformat.h"
#include "db/skiplist.h"
#include "leveldb/db.h"
#include "util/arena.h"

namespace leveldb {

    class InternalKeyComparator;

    class MemTableIterator;

    class MemTable {
    public:
        // MemTables are reference counted.  The initial reference count
        // is zero and the caller must call Ref() at least once.
        explicit MemTable(const InternalKeyComparator &internalKeyComparator);

        MemTable(const MemTable &) = delete;

        MemTable &operator=(const MemTable &) = delete;

        // increase reference count.
        void Ref() { ++refs_; }

        // Drop reference count.  Delete if no more references exist.
        void Unref() {
            --refs_;
            assert(refs_ >= 0);
            if (refs_ <= 0) {
                delete this;
            }
        }

        // Returns an estimate of the number of bytes of data in use by this
        // data structure. It is safe to call when MemTable is being modified.
        size_t ApproximateMemoryUsage();

        // Return an iterator that yields the contents of the memtable.
        //
        // The caller must ensure that the underlying MemTable remains live
        // while the returned iterator is live.  The keys returned by this
        // iterator are internal keys encoded by AppendInternalKey in the
        // db/format.{h,cc} module.
        Iterator *NewIterator();

        // Add an entry into memtable that maps key to value at the
        // specified sequence number and with the specified valueType.
        // Typically value will be empty if valueType==kTypeDeletion.
        void Add(SequenceNumber sequenceNumber,
                 ValueType valueType,
                 const Slice &key,
                 const Slice &value);

        // If memtable contains a value for key, store it in *value and return true.
        // If memtable contains a deletion for key, store a NotFound() error
        // in *status and return true.
        // Else, return false.
        bool Get(const LookupKey &key, std::string *value, Status *s);

    private:
        friend class MemTableIterator;

        friend class MemTableBackwardIterator;

        struct KeyComparator {
            const InternalKeyComparator internalKeyComparator;

            explicit KeyComparator(const InternalKeyComparator &c) : internalKeyComparator(c) {}

            int operator()(const char *a, const char *b) const;
        };

        typedef SkipList<const char *, KeyComparator> Table;

        ~MemTable();  // private since only Unref() should be used to delete it

        KeyComparator keyComparator;
        int refs_;
        Arena arena;
        Table table;
    };

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_MEMTABLE_H_
