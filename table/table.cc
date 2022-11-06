// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table.h"

#include "leveldb/cache.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"

namespace leveldb {

    struct Table::Rep {
        ~Rep() {
            delete filter;
            delete[] filter_data;
            delete index_block;
        }

        Options options;
        Status status;
        RandomAccessFile *file;
        uint64_t cache_id;
        FilterBlockReader *filter;
        const char *filter_data;

        BlockHandle metaindex_handle;  // Handle to metaindex_block: saved from footer
        Block *index_block;
    };

    Status Table::Open(const Options &options,
                       RandomAccessFile *ldbFile,
                       uint64_t ldbFileSize,
                       Table **table) {

        *table = nullptr;
        if (ldbFileSize < Footer::ENCODED_LEN) {
            return Status::Corruption("ldbFile is too short to be an sstable");
        }

        char footerSpace[Footer::ENCODED_LEN];
        Slice footerInput;

        Status status = ldbFile->Read(ldbFileSize - Footer::ENCODED_LEN,
                                      Footer::ENCODED_LEN,
                                      &footerInput,
                                      footerSpace);
        if (!status.ok()) {
            return status;
        }

        // 反序列footer
        Footer footer;
        status = footer.DecodeFrom(&footerInput);
        if (!status.ok()) {
            return status;
        }

        // read the index block
        // 相应的格式 https://blog.csdn.net/xxb249/article/details/94559781
        BlockContents indexBlockContents;
        ReadOptions readOptions;
        if (options.paranoid_checks) {
            readOptions.verifyChecksums_ = true;
        }
        status = ReadBlock(ldbFile,
                           readOptions,
                           footer.index_handle(),
                           &indexBlockContents);

        // We've successfully read the footer and the index block: we're ready to serve request
        if (status.ok()) {
            auto *indexBlock = new Block(indexBlockContents);
            Rep *rep = new Table::Rep();
            rep->options = options;
            rep->file = ldbFile;
            rep->metaindex_handle = footer.metaindex_handle();
            rep->index_block = indexBlock;
            rep->cache_id = (options.blockCache ? options.blockCache->NewId() : 0);
            rep->filter_data = nullptr;
            rep->filter = nullptr;
            *table = new Table(rep);
            (*table)->ReadMeta(footer);
        }

        return status;
    }

    void Table::ReadMeta(const Footer &footer) {
        if (rep_->options.filterPolicy == nullptr) {
            return;  // Do not need any metadata
        }

        // TODO(sanjay): Skip this if footer.metaindex_handle() size indicates
        // it is an empty block.
        ReadOptions opt;
        if (rep_->options.paranoid_checks) {
            opt.verifyChecksums_ = true;
        }
        BlockContents contents;
        if (!ReadBlock(rep_->file, opt, footer.metaindex_handle(), &contents).ok()) {
            // Do not propagate errors since meta info is not needed for operation
            return;
        }
        Block *meta = new Block(contents);

        Iterator *iter = meta->NewIterator(BytewiseComparator());
        std::string key = "filter.";
        key.append(rep_->options.filterPolicy->Name());
        iter->Seek(key);
        if (iter->Valid() && iter->key() == Slice(key)) {
            ReadFilter(iter->value());
        }
        delete iter;
        delete meta;
    }

    void Table::ReadFilter(const Slice &filter_handle_value) {
        Slice v = filter_handle_value;
        BlockHandle filter_handle;
        if (!filter_handle.DecodeFrom(&v).ok()) {
            return;
        }

        // We might want to unify with ReadBlock() if we start
        // requiring checksum verification in Table::Open.
        ReadOptions opt;
        if (rep_->options.paranoid_checks) {
            opt.verifyChecksums_ = true;
        }
        BlockContents block;
        if (!ReadBlock(rep_->file, opt, filter_handle, &block).ok()) {
            return;
        }
        if (block.allocatedFromHeap_) {
            rep_->filter_data = block.data.data();  // Will need to delete later
        }
        rep_->filter = new FilterBlockReader(rep_->options.filterPolicy, block.data);
    }

    Table::~Table() { delete rep_; }

    static void DeleteBlock(void *arg, void *ignored) {
        delete reinterpret_cast<Block *>(arg);
    }

    static void DeleteCachedBlock(const Slice &key, void *value) {
        Block *block = reinterpret_cast<Block *>(value);
        delete block;
    }

    static void ReleaseBlock(void *arg, void *h) {
        Cache *cache = reinterpret_cast<Cache *>(arg);
        Cache::Handle *handle = reinterpret_cast<Cache::Handle *>(h);
        cache->Release(handle);
    }

// Convert an index iterator value (i.e., an encoded BlockHandle)
// into an iterator over the contents of the corresponding block.
    Iterator *Table::BlockReader(void *arg, const ReadOptions &options,
                                 const Slice &index_value) {
        Table *table = reinterpret_cast<Table *>(arg);
        Cache *block_cache = table->rep_->options.blockCache;
        Block *block = nullptr;
        Cache::Handle *cache_handle = nullptr;

        BlockHandle handle;
        Slice input = index_value;
        Status s = handle.DecodeFrom(&input);
        // We intentionally allow extra stuff in index_value so that we
        // can add more features in the future.

        if (s.ok()) {
            BlockContents contents;
            if (block_cache != nullptr) {
                char cache_key_buffer[16];
                EncodeFixed64(cache_key_buffer, table->rep_->cache_id);
                EncodeFixed64(cache_key_buffer + 8, handle.offset());
                Slice key(cache_key_buffer, sizeof(cache_key_buffer));
                cache_handle = block_cache->Lookup(key);
                if (cache_handle != nullptr) {
                    block = reinterpret_cast<Block *>(block_cache->Value(cache_handle));
                } else {
                    s = ReadBlock(table->rep_->file, options, handle, &contents);
                    if (s.ok()) {
                        block = new Block(contents);
                        if (contents.cachable && options.fillCache_) {
                            cache_handle = block_cache->Insert(key, block, block->size(),
                                                               &DeleteCachedBlock);
                        }
                    }
                }
            } else {
                s = ReadBlock(table->rep_->file, options, handle, &contents);
                if (s.ok()) {
                    block = new Block(contents);
                }
            }
        }

        Iterator *iter;
        if (block != nullptr) {
            iter = block->NewIterator(table->rep_->options.comparator);
            if (cache_handle == nullptr) {
                iter->RegisterCleanup(&DeleteBlock, block, nullptr);
            } else {
                iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle);
            }
        } else {
            iter = NewErrorIterator(s);
        }
        return iter;
    }

    Iterator *Table::NewIterator(const ReadOptions &options) const {
        return NewTwoLevelIterator(rep_->index_block->NewIterator(rep_->options.comparator),
                                   &Table::BlockReader,
                                   const_cast<Table *>(this), options);
    }

    Status Table::InternalGet(const ReadOptions &options,
                              const Slice &k, void *arg,
                              void (*handler)(void *arg, const Slice &key, const Slice &value)) {
        Status s;
        Iterator *iiter = rep_->index_block->NewIterator(rep_->options.comparator);
        iiter->Seek(k);
        if (iiter->Valid()) {
            Slice handle_value = iiter->value();
            FilterBlockReader *filter = rep_->filter;
            BlockHandle handle;
            if (filter != nullptr && handle.DecodeFrom(&handle_value).ok() &&
                !filter->KeyMayMatch(handle.offset(), k)) {
                // Not found
            } else {
                Iterator *block_iter = BlockReader(this, options, iiter->value());
                block_iter->Seek(k);
                if (block_iter->Valid()) {
                    handler(arg, block_iter->key(), block_iter->value());
                }
                s = block_iter->status();
                delete block_iter;
            }
        }
        if (s.ok()) {
            s = iiter->status();
        }
        delete iiter;
        return s;
    }

    uint64_t Table::ApproximateOffsetOf(const Slice &key) const {
        Iterator *index_iter =
                rep_->index_block->NewIterator(rep_->options.comparator);
        index_iter->Seek(key);
        uint64_t result;
        if (index_iter->Valid()) {
            BlockHandle handle;
            Slice input = index_iter->value();
            Status s = handle.DecodeFrom(&input);
            if (s.ok()) {
                result = handle.offset();
            } else {
                // Strange: we can't decode the block handle in the index block.
                // We'll just return the offset of the metaindex block, which is
                // close to the whole file size for this case.
                result = rep_->metaindex_handle.offset();
            }
        } else {
            // key is past the last key in the file.  Approximate the offset
            // by returning the offset of the metaindex block (which is
            // right near the end of the file).
            result = rep_->metaindex_handle.offset();
        }
        delete index_iter;
        return result;
    }

}  // namespace leveldb
