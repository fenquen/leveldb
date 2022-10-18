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
        Rep(const Options &options, WritableFile *f)
                : options(options),
                  indexBlockOptions(options),
                  writableFile(f),
                  offset(0),
                  dataBlock(&options),
                  indexBlock(&indexBlockOptions),
                  numEntries(0),
                  closed(false),
                  filterBlockBuilder(
                          options.filterPolicy == nullptr ? nullptr : new FilterBlockBuilder(options.filterPolicy)),
                  pending_index_entry(false) {
            indexBlockOptions.blockRestartInterval = 1;
        }

        Options options;
        Options indexBlockOptions;
        WritableFile *writableFile;
        uint64_t offset;
        Status status;
        BlockBuilder dataBlock;
        BlockBuilder indexBlock;
        std::string lastKey;
        int64_t numEntries;
        bool closed;  // Either Finish() or Abandon() has been called.
        FilterBlockBuilder *filterBlockBuilder;

        // We do not emit the index entry for a block until we have seen the
        // first key for the next data block.  This allows us to use shorter
        // keys in the index block.  For example, consider a block boundary
        // between the keys "the quick brown fox" and "the who".  We can use
        // "the r" as the key for the index block entry since it is >= all
        // entries in the first block and < all entries in subsequent blocks.
        //
        // Invariant: r->pending_index_entry is true only if dataBlock is empty.
        bool pending_index_entry;

        // Handle to add to index block
        BlockHandle pending_handle;

        std::string compressed_output;
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

        if (rep->numEntries > 0) {
            assert(rep->options.comparator->Compare(key, Slice(rep->lastKey)) > 0);
        }

        if (rep->pending_index_entry) {
            assert(rep->dataBlock.empty());
            rep->options.comparator->FindShortestSeparator(&rep->lastKey, key);
            std::string handle_encoding;
            rep->pending_handle.EncodeTo(&handle_encoding);
            rep->indexBlock.Add(rep->lastKey, Slice(handle_encoding));
            rep->pending_index_entry = false;
        }

        if (rep->filterBlockBuilder != nullptr) {
            rep->filterBlockBuilder->AddKey(key);
        }

        rep->lastKey.assign(key.data(), key.size());
        rep->numEntries++;
        rep->dataBlock.Add(key, value);

        const size_t estimatedBlockSize = rep->dataBlock.CurrentSizeEstimate();
        if (estimatedBlockSize >= rep->options.blockSize) {
            Flush();
        }
    }

    void TableBuilder::Flush() {
        Rep *rep = rep_;
        assert(!rep->closed);

        if (!rep->status.ok()) {
            return;
        }

        if (rep->dataBlock.empty()) {
            return;
        }

        assert(!rep->pending_index_entry);
        WriteBlock(&rep->dataBlock, &rep->pending_handle);
        if (ok()) {
            rep->pending_index_entry = true;
            rep->status = rep->writableFile->Flush();
        }
        if (rep->filterBlockBuilder != nullptr) {
            rep->filterBlockBuilder->StartBlock(rep->offset);
        }
    }

    void TableBuilder::WriteBlock(BlockBuilder *block, BlockHandle *handle) {
        // File format contains a sequence of blocks where each block has:
        //    block_data: uint8[n]
        //    type: uint8
        //    crc: uint32
        assert(ok());
        Rep *r = rep_;
        Slice raw = block->Finish();

        Slice block_contents;
        CompressionType type = r->options.compression;
        // TODO(postrelease): Support more compression options: zlib?
        switch (type) {
            case kNoCompression:
                block_contents = raw;
                break;

            case kSnappyCompression: {
                std::string *compressed = &r->compressed_output;
                if (port::Snappy_Compress(raw.data(), raw.size(), compressed) &&
                    compressed->size() < raw.size() - (raw.size() / 8u)) {
                    block_contents = *compressed;
                } else {
                    // Snappy not supported, or compressed less than 12.5%, so just
                    // store uncompressed form
                    block_contents = raw;
                    type = kNoCompression;
                }
                break;
            }
        }
        WriteRawBlock(block_contents, type, handle);
        r->compressed_output.clear();
        block->Reset();
    }

    void TableBuilder::WriteRawBlock(const Slice &block_contents,
                                     CompressionType type, BlockHandle *handle) {
        Rep *r = rep_;
        handle->set_offset(r->offset);
        handle->set_size(block_contents.size());
        r->status = r->writableFile->Append(block_contents);
        if (r->status.ok()) {
            char trailer[kBlockTrailerSize];
            trailer[0] = type;
            uint32_t crc = crc32c::Value(block_contents.data(), block_contents.size());
            crc = crc32c::Extend(crc, trailer, 1);  // Extend crc to cover block type
            EncodeFixed32(trailer + 1, crc32c::Mask(crc));
            r->status = r->writableFile->Append(Slice(trailer, kBlockTrailerSize));
            if (r->status.ok()) {
                r->offset += block_contents.size() + kBlockTrailerSize;
            }
        }
    }

    Status TableBuilder::status() const {
        return rep_->status;
    }

    Status TableBuilder::Finish() {
        Rep *r = rep_;
        Flush();
        assert(!r->closed);
        r->closed = true;

        BlockHandle filter_block_handle, metaindex_block_handle, index_block_handle;

        // Write filter block
        if (ok() && r->filterBlockBuilder != nullptr) {
            WriteRawBlock(r->filterBlockBuilder->Finish(), kNoCompression,
                          &filter_block_handle);
        }

        // Write metaindex block
        if (ok()) {
            BlockBuilder meta_index_block(&r->options);
            if (r->filterBlockBuilder != nullptr) {
                // Add mapping from "filter.Name" to location of filter data
                std::string key = "filter.";
                key.append(r->options.filterPolicy->Name());
                std::string handle_encoding;
                filter_block_handle.EncodeTo(&handle_encoding);
                meta_index_block.Add(key, handle_encoding);
            }

            // TODO(postrelease): Add stats and other meta blocks
            WriteBlock(&meta_index_block, &metaindex_block_handle);
        }

        // Write index block
        if (ok()) {
            if (r->pending_index_entry) {
                r->options.comparator->FindShortSuccessor(&r->lastKey);
                std::string handle_encoding;
                r->pending_handle.EncodeTo(&handle_encoding);
                r->indexBlock.Add(r->lastKey, Slice(handle_encoding));
                r->pending_index_entry = false;
            }
            WriteBlock(&r->indexBlock, &index_block_handle);
        }

        // Write footer
        if (ok()) {
            Footer footer;
            footer.set_metaindex_handle(metaindex_block_handle);
            footer.set_index_handle(index_block_handle);
            std::string footer_encoding;
            footer.EncodeTo(&footer_encoding);
            r->status = r->writableFile->Append(footer_encoding);
            if (r->status.ok()) {
                r->offset += footer_encoding.size();
            }
        }
        return r->status;
    }

    void TableBuilder::Abandon() {
        Rep *r = rep_;
        assert(!r->closed);
        r->closed = true;
    }

    uint64_t TableBuilder::NumEntries() const { return rep_->numEntries; }

    uint64_t TableBuilder::FileSize() const { return rep_->offset; }

}  // namespace leveldb
