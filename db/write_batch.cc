// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/write_batch.h"

#include "db/dbformat.h"
#include "db/memtable.h"
#include "db/write_batch_internal.h"
#include "leveldb/db.h"
#include "util/coding.h"

namespace leveldb {
    // 8-byte sequence number, 4-byte count.
    static const size_t WRITE_BATCH_HEADER_LEN = 12;

    WriteBatch::WriteBatch() {
        Clear();
    }

    WriteBatch::~WriteBatch() = default;

    WriteBatch::Handler::~Handler() = default;

    void WriteBatch::Clear() {
        rep_.clear();
        rep_.resize(WRITE_BATCH_HEADER_LEN);
    }

    size_t WriteBatch::ApproximateSize() const { return rep_.size(); }

    Status WriteBatch::Iterate(Handler *handler) const {
        Slice input(rep_);
        if (input.size() < WRITE_BATCH_HEADER_LEN) {
            return Status::Corruption("malformed WriteBatch (too small)");
        }

        input.remove_prefix(WRITE_BATCH_HEADER_LEN);

        Slice key, value;
        int found = 0;
        while (!input.empty()) {
            found++;
            char valueType = input[0];
            input.remove_prefix(1);
            switch (valueType) {
                case kTypeValue:
                    if (GetLengthPrefixedSlice(&input, &key) &&
                        GetLengthPrefixedSlice(&input, &value)) {
                        handler->Put(key, value);
                    } else {
                        return Status::Corruption("bad WriteBatch Put");
                    }

                    break;
                case kTypeDeletion:
                    if (GetLengthPrefixedSlice(&input, &key)) {
                        handler->Delete(key);
                    } else {
                        return Status::Corruption("bad WriteBatch Delete");
                    }
                    break;
                default:
                    return Status::Corruption("unknown WriteBatch valueType");
            }
        }

        if (found != WriteBatchInternal::Count(this)) {
            return Status::Corruption("WriteBatch has wrong count");
        }

        return Status::OK();
    }

    // 由 writeBatch数据的格式可知 打头的8字节后边的4字节是
    int WriteBatchInternal::Count(const WriteBatch *writeBatch) {
        return DecodeFixed32(writeBatch->rep_.data() + 8);
    }

    void WriteBatchInternal::SetCount(WriteBatch *b, int n) {
        EncodeFixed32(&b->rep_[8], n);
    }

    // 由 writeBatch数据的格式可知 大头的8字节是sequence
    SequenceNumber WriteBatchInternal::Sequence(const WriteBatch *writeBatch) {
        return SequenceNumber(DecodeFixed64(writeBatch->rep_.data()));
    }

    void WriteBatchInternal::SetSequence(WriteBatch *b, SequenceNumber seq) {
        EncodeFixed64(&b->rep_[0], seq);
    }

    void WriteBatch::Put(const Slice &key, const Slice &value) {
        WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);
        rep_.push_back(static_cast<char>(kTypeValue));
        PutLengthPrefixedSlice(&rep_, key);
        PutLengthPrefixedSlice(&rep_, value);
    }

    void WriteBatch::Delete(const Slice &key) {
        WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);
        rep_.push_back(static_cast<char>(kTypeDeletion));
        PutLengthPrefixedSlice(&rep_, key);
    }

    void WriteBatch::Append(const WriteBatch &source) {
        WriteBatchInternal::Append(this, &source);
    }

    namespace {
        class MemTableInserter : public WriteBatch::Handler {
        public:
            SequenceNumber sequenceNumber;
            MemTable *memTable;

            void Put(const Slice &key, const Slice &value) override {
                memTable->Add(sequenceNumber, kTypeValue, key, value);
                sequenceNumber++;
            }

            void Delete(const Slice &key) override {
                memTable->Add(sequenceNumber, kTypeDeletion, key, Slice());
                sequenceNumber++;
            }
        };
    }  // namespace

    // 用 writeBatch内容 insert到 memTable_
    Status WriteBatchInternal::InsertIntoMemTable(const WriteBatch *writeBatch, MemTable *memtable) {
        MemTableInserter memTableInserter;
        memTableInserter.sequenceNumber = WriteBatchInternal::Sequence(writeBatch);
        memTableInserter.memTable = memtable;
        return writeBatch->Iterate(&memTableInserter);
    }

    void WriteBatchInternal::SetContents(WriteBatch *writeBatch, const Slice &contents) {
        assert(contents.size() >= WRITE_BATCH_HEADER_LEN);
        writeBatch->rep_.assign(contents.data(), contents.size());
    }

    // src的前12个字节(WRITE_BATCH_HEADER_LEN)会去掉然后融入到dest,dest的count也要相应的更改
    void WriteBatchInternal::Append(WriteBatch *dest, const WriteBatch *src) {
        SetCount(dest, Count(dest) + Count(src));
        assert(src->rep_.size() >= WRITE_BATCH_HEADER_LEN);

        dest->rep_.append(src->rep_.data() + WRITE_BATCH_HEADER_LEN,
                          src->rep_.size() - WRITE_BATCH_HEADER_LEN);
    }

}  // namespace leveldb
