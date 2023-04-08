// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table_builder.h"

#include <cassert>

#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block_builder.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {

    struct TableBuilder::Rep {
        Rep(const Options &options, WritableFile *writableFile)
                : options(options),
                  indexBlockOptions(options),
                  writableFile(writableFile),
                  offset(0),
                  dataBlockBuilder_(&options),
                  indexBlockBuilder_(&indexBlockOptions),
                  entryCount(0),
                  closed(false),
                  filterBlockBuilder(options.filterPolicy == nullptr ? nullptr : new FilterBlockBuilder(options.filterPolicy)),
                  pendingIndexBlockEntry_(false) {
            indexBlockOptions.blockRestartInterval = 1;
        }

        Options options;
        Options indexBlockOptions;
        WritableFile *writableFile;
        uint64_t offset;
        Status status;
        BlockBuilder dataBlockBuilder_;
        BlockBuilder indexBlockBuilder_;
        std::string lastKey;
        int64_t entryCount;
        bool closed;  // 调用 Finish() or Abandon() be true
        FilterBlockBuilder *filterBlockBuilder;

        // https://blog.csdn.net/xxb249/article/details/94559781
        // 对应的因该是data index block
        // We do not emit the index entry for a block until we have seen the
        // first key for the next data block.  This allows us to use shorter
        // keys in the index block.  For example, consider a block boundary
        // between the keys "the quick brown fox" and "the who".  We can use
        // "the r" as the key for the index block entry since it is >= all
        // entries in the first block and < all entries in subsequent blocks.
        //
        // Invariant: r->pendingIndexBlockEntry_ is true only if dataBlockBuilder_ is empty.
        bool pendingIndexBlockEntry_;

        // Handle to add to index block
        BlockHandle pendingIndexBlockHandle;

        // 保存压缩了之后的data
        std::string compressedOutput_;
    };

    TableBuilder::TableBuilder(const Options &options,
                               WritableFile *writableFile) : rep_(new Rep(options, writableFile)) {
        if (rep_->filterBlockBuilder != nullptr) {
            rep_->filterBlockBuilder->StartBlock(0);
        }
    }

    TableBuilder::~TableBuilder() {
        assert(rep_->closed);  // Catch errors where caller forgot to call Finish()
        delete rep_->filterBlockBuilder;
        delete rep_;
    }

    Status TableBuilder::ChangeOptions(const Options &options) {
        // Note: if more fields are added to Options, update
        // this function to catch changes that should not be allowed to
        // change in the middle of building a Table.
        if (options.comparator != rep_->options.comparator) {
            return Status::InvalidArgument("changing internalKeyComparator while building table");
        }

        // Note that any live BlockBuilders point to rep_->options and therefore
        // will automatically pick up the updated options.
        rep_->options = options;
        rep_->indexBlockOptions = options;
        rep_->indexBlockOptions.blockRestartInterval = 1;
        return Status::OK();
    }

    void TableBuilder::Add(const Slice &key, const Slice &value) {
        Rep *rep = rep_;
        assert(!rep->closed);

        if (!ok()) {
            return;
        }

        if (rep->entryCount > 0) {
            assert(rep->options.comparator->Compare(key, Slice(rep->lastKey)) > 0);
        }

        if (rep->pendingIndexBlockEntry_) {
            assert(rep->dataBlockBuilder_.empty());

            rep->options.comparator->FindShortestSeparator(&rep->lastKey, key);

            std::string handleEncoding;
            rep->pendingIndexBlockHandle.EncodeTo(&handleEncoding);
            rep->indexBlockBuilder_.Add(rep->lastKey, Slice(handleEncoding));

            rep->pendingIndexBlockEntry_ = false;
        }

        if (rep->filterBlockBuilder != nullptr) {
            rep->filterBlockBuilder->AddKey(key);
        }

        rep->lastKey.assign(key.data(), key.size());
        rep->entryCount++;
        rep->dataBlockBuilder_.Add(key, value);

        const size_t estimatedBlockSize = rep->dataBlockBuilder_.CurrentSizeEstimate();
        if (estimatedBlockSize >= rep->options.blockSize) {
            this->Flush();
        }
    }

    void TableBuilder::Flush() {
        Rep *rep = rep_;
        assert(!rep->closed);

        if (!rep->status.ok()) {
            return;
        }

        if (rep->dataBlockBuilder_.empty()) {
            return;
        }

        assert(!rep->pendingIndexBlockEntry_);

        // 写到了底下的writable的buffer
        this->WriteBlock(&rep->dataBlockBuilder_, &rep->pendingIndexBlockHandle);

        if (ok()) {
            // https://blog.csdn.net/xxb249/article/details/94559781
            // 说的 dataBlock后边是 metaIndex 和 dataIndex 故而 pendingIndexEntry_是true说的过去
            rep->pendingIndexBlockEntry_ = true;
            rep->status = rep->writableFile->Flush(); // writableFile本身也有buffer,也需要flush()
        }

        if (rep->filterBlockBuilder != nullptr) {
            rep->filterBlockBuilder->StartBlock(rep->offset);
        }
    }

    // 单个的block格式
    //    block_data: uint8[n]
    //    type: uint8
    //    crc: uint32
    void TableBuilder::WriteBlock(BlockBuilder *blockBuilder, BlockHandle *blockHandle) {
        assert(ok());

        Rep *rep = rep_;
        Slice rawContent = blockBuilder->Finish();

        Slice blockContent;
        CompressionType compressionType = rep->options.compression;
        // TODO(postrelease): Support more compression options: zlib?
        switch (compressionType) {
            case kNoCompression:
                blockContent = rawContent;
                break;
            case kSnappyCompression: {
                std::string *compressedOutput = &rep->compressedOutput_;
                if (port::Snappy_Compress(rawContent.data(), rawContent.size(), compressedOutput) &&
                    compressedOutput->size() < rawContent.size() - (rawContent.size() / 8u)) {
                    blockContent = *compressedOutput;
                } else {
                    // snappy not supported, or compressedOutput less than 12.5%, store uncompressed form
                    blockContent = rawContent;
                    compressionType = kNoCompression;
                }
                break;
            }
        }

        this->WriteRawBlock(blockContent, compressionType, blockHandle);

        rep->compressedOutput_.clear();
        blockBuilder->Reset();
    }

    void TableBuilder::WriteRawBlock(const Slice &blockContent,
                                     CompressionType compressionType,
                                     BlockHandle *blockHandle) {
        Rep *rep = rep_;

        blockHandle->set_offset(rep->offset);
        blockHandle->set_size(blockContent.size());

        rep->status = rep->writableFile->Append(blockContent);

        if (rep->status.ok()) {
            char trailer[BLOCK_TRAILER_SIZE];

            // compressionType
            trailer[0] = compressionType;

            // crc
            uint32_t crc = crc32c::Value(blockContent.data(), blockContent.size());
            crc = crc32c::Extend(crc, trailer, 1);  // Extend crc to cover block compressionType
            EncodeFixed32(trailer + 1, crc32c::Mask(crc));

            rep->status = rep->writableFile->Append(Slice(trailer, BLOCK_TRAILER_SIZE));
            if (rep->status.ok()) {
                rep->offset += blockContent.size() + BLOCK_TRAILER_SIZE;
            }
        }
    }

    Status TableBuilder::status() const {
        return rep_->status;
    }

    // ldb格式 https://blog.csdn.net/xxb249/article/details/94559781
    Status TableBuilder::Finish() {
        Rep *rep = rep_;

        // dataBlockBuilder_的内容落地
        this->Flush();

        assert(!rep->closed);
        rep->closed = true;

        BlockHandle filterBlockHandle;
        BlockHandle metaIndexBlockHandle;
        BlockHandle indexBlockHandle;

        // Write filter block
        if (ok() && rep->filterBlockBuilder != nullptr) {
            this->WriteRawBlock(rep->filterBlockBuilder->Finish(), kNoCompression, &filterBlockHandle);
        }

        // 落地 meta index block
        if (ok()) {
            BlockBuilder metaIndexBlockBuilder(&rep->options);
            if (rep->filterBlockBuilder != nullptr) {
                // Add mapping from "filter.Name" to location of filter data
                std::string key = "filter.";
                key.append(rep->options.filterPolicy->Name());
                std::string handle_encoding;
                filterBlockHandle.EncodeTo(&handle_encoding);
                metaIndexBlockBuilder.Add(key, handle_encoding);
            }

            // TODO(postrelease): Add stats and other meta blocks
            WriteBlock(&metaIndexBlockBuilder, &metaIndexBlockHandle);
        }

        // 落地 index block
        if (ok()) {
            if (rep->pendingIndexBlockEntry_) {
                rep->options.comparator->FindShortSuccessor(&rep->lastKey);
                std::string handle_encoding;
                rep->pendingIndexBlockHandle.EncodeTo(&handle_encoding);
                rep->indexBlockBuilder_.Add(rep->lastKey, Slice(handle_encoding));
                rep->pendingIndexBlockEntry_ = false;
            }

            WriteBlock(&rep->indexBlockBuilder_, &indexBlockHandle);
        }

        // Write footer
        if (ok()) {
            Footer footer;
            footer.set_metaindex_handle(metaIndexBlockHandle);
            footer.set_index_handle(indexBlockHandle);
            std::string footer_encoding;
            footer.EncodeTo(&footer_encoding);
            rep->status = rep->writableFile->Append(footer_encoding);
            if (rep->status.ok()) {
                rep->offset += footer_encoding.size();
            }
        }

        return rep->status;
    }

    void TableBuilder::Abandon() {
        Rep *r = rep_;
        assert(!r->closed);
        r->closed = true;
    }

    uint64_t TableBuilder::NumEntries() const { return rep_->entryCount; }

    uint64_t TableBuilder::FileSize() const { return rep_->offset; }

}  // namespace leveldb
