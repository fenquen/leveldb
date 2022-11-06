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
        //  key byte    : char[internal_key.size()]
        //  value_size   : varint32 of value.size()
        //  value byte  : char[value.size()]
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

    bool MemTable::Get(const LookupKey &lookupKey,
                       std::string *value,
                       Status *status) {

        Slice memtableKey = lookupKey.memtableKey();
        Table::Iterator iterator(&table);

        iterator.Seek(memtableKey.data());

        if (iterator.Valid()) {
            // entry format is:
            //    internalKeyLen  varint32
            //    userkey      char[internalKeyLen-8]
            //    valueType      uint64
            //    vlength  varint32
            //    value    char[vlength]
            // Check that it belongs to same user lookupKey.  We do not check the
            // sequence number since the Seek() call above should have skipped
            // all entries with overly large sequence numbers
            const char *entry = iterator.key();
            uint32_t internalKeyLen;
            const char *internalKeyPos = GetVarint32Ptr(entry, entry + 5, &internalKeyLen);
            if (keyComparator.internalKeyComparator.user_comparator()->Compare(Slice(internalKeyPos, internalKeyLen - 8), lookupKey.user_key()) == 0) {
                // 更正 user lookupKey
                const uint64_t valueType = DecodeFixed64(internalKeyPos + internalKeyLen - 8);
                switch (static_cast<ValueType>(valueType & 0xff)) {
                    case kTypeValue: {
                        Slice v = GetLengthPrefixedSlice(internalKeyPos + internalKeyLen);
                        value->assign(v.data(), v.size());
                        return true;
                    }
                    case kTypeDeletion:
                        *status = Status::NotFound(Slice());
                        return true;
                }
            }
        }

        return false;
    }

}  // namespace leveldb
