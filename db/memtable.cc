// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/memtable.h"
#include "db/dbformat.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "util/coding.h"

namespace leveldb {

    static Slice GetLengthPrefixedSlice(const char *data) {
        uint32_t len;
        const char *p = data;
        p = GetVarint32Ptr(p, p + 5, &len);  // +5: we assume "p" is not corrupted
        return {p, len};
    }

    MemTable::MemTable(const InternalKeyComparator &internalKeyComparator) : keyComparator(internalKeyComparator),
                                                                             refs_(0),
                                                                             table(keyComparator, &arena) {}

    MemTable::~MemTable() {
        assert(refs_ == 0);
    }

    size_t MemTable::ApproximateMemoryUsage() {
        return arena.MemoryUsage();
    }

    int MemTable::KeyComparator::operator()(const char *aptr,
                                            const char *bptr) const {
        // Internal keys are encoded as length-prefixed strings.
        Slice a = GetLengthPrefixedSlice(aptr);
        Slice b = GetLengthPrefixedSlice(bptr);
        return internalKeyComparator.Compare(a, b);
    }

    // Encode a suitable internal key target for "target" and return it.
    // Uses *scratch as scratch space, and the returned pointer will point
    // into this scratch space.
    static const char *EncodeKey(std::string *scratch, const Slice &target) {
        scratch->clear();
        PutVarint32(scratch, target.size());
        scratch->append(target.data(), target.size());
        return scratch->data();
    }

    class MemTableIterator : public Iterator {
    public:
        explicit MemTableIterator(MemTable::Table *table) : iterator_(table) {}

        MemTableIterator(const MemTableIterator &) = delete;

        MemTableIterator &operator=(const MemTableIterator &) = delete;

        ~MemTableIterator() override = default;

        bool Valid() const override {
            return iterator_.Valid();
        }

        void Seek(const Slice &k) override {
            iterator_.Seek(EncodeKey(&tmp_, k));
        }

        void SeekToFirst() override { iterator_.SeekToFirst(); }

        void SeekToLast() override { iterator_.SeekToLast(); }

        void Next() override { iterator_.Next(); }

        void Prev() override { iterator_.Prev(); }

        Slice key() const override { return GetLengthPrefixedSlice(iterator_.key()); }

        Slice value() const override {
            Slice key_slice = GetLengthPrefixedSlice(iterator_.key());
            return GetLengthPrefixedSlice(key_slice.data() + key_slice.size());
        }

        Status status() const override { return Status::OK(); }

    private:
        MemTable::Table::Iterator iterator_;
        std::string tmp_;  // For passing to EncodeKey
    };

    Iterator *MemTable::NewIterator() {
        return new MemTableIterator(&table);
    }

    void MemTable::Add(SequenceNumber sequenceNumber,
                       ValueType valueType,
                       const Slice &key,
                       const Slice &value) {
        // Format of an entry is concatenation of:
        //  keySize     : varint32 of internal_key.size()
        //  key bytes    : char[internal_key.size()]
        //  value_size   : varint32 of value.size()
        //  value bytes  : char[value.size()]
        size_t keySize = key.size();
        size_t internalKeySize = keySize + 8;

        size_t valSize = value.size();

        const size_t encodedLen = VarintLength(internalKeySize) + internalKeySize +
                                  VarintLength(valSize) + valSize;

        char *allocated = arena.Allocate(encodedLen);

        // 写 internalKeySize
        char *p = EncodeVarint32(allocated, internalKeySize);

        // 写 原始的key
        ::memcpy(p, key.data(), keySize);
        p += keySize;

        // 写 internalKey后边多的8字节
        // 对应上了internalKey格式,相比原始的key末尾多了8字节的(sequenceNumber << 8) | valueType
        EncodeFixed64(p, (sequenceNumber << 8) | valueType);
        p += 8;

        // 写 valueSize
        p = EncodeVarint32(p, valSize);
        // 写 value
        std::memcpy(p, value.data(), valSize);

        assert(p + valSize == allocated + encodedLen);

        // skipList
        table.Insert(allocated);
    }

    bool MemTable::Get(const LookupKey &key, std::string *value, Status *s) {
        Slice memkey = key.memtable_key();
        Table::Iterator iter(&table);
        iter.Seek(memkey.data());
        if (iter.Valid()) {
            // entry format is:
            //    klength  varint32
            //    userkey  char[klength]
            //    tag      uint64
            //    vlength  varint32
            //    value    char[vlength]
            // Check that it belongs to same user key.  We do not check the
            // sequence number since the Seek() call above should have skipped
            // all entries with overly large sequence numbers.
            const char *entry = iter.key();
            uint32_t key_length;
            const char *key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length);
            if (keyComparator.internalKeyComparator.user_comparator()->Compare(
                    Slice(key_ptr, key_length - 8), key.user_key()) == 0) {
                // Correct user key
                const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
                switch (static_cast<ValueType>(tag & 0xff)) {
                    case kTypeValue: {
                        Slice v = GetLengthPrefixedSlice(key_ptr + key_length);
                        value->assign(v.data(), v.size());
                        return true;
                    }
                    case kTypeDeletion:
                        *s = Status::NotFound(Slice());
                        return true;
                }
            }
        }
        return false;
    }

}  // namespace leveldb
