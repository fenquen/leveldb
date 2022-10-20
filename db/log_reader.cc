// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/log_reader.h"

#include <cstdio>

#include "leveldb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {
    namespace log {

        Reader::Reporter::~Reporter() = default;

        Reader::Reader(SequentialFile *file,
                       Reporter *reporter,
                       bool useCheckSum,
                       uint64_t initial_offset) : file_(file),
                                                  reporter_(reporter),
                                                  checksum_(useCheckSum),
                                                  backing_store_(new char[LOG_BLOCK_LEN]),
                                                  buffer_(),
                                                  eof_(false),
                                                  last_record_offset_(0),
                                                  bufferEndPos(0),
                                                  initial_offset_(initial_offset),
                                                  resyncing_(initial_offset > 0) {}

        Reader::~Reader() { delete[] backing_store_; }

        bool Reader::SkipToInitialBlock() {
            const size_t offset_in_block = initial_offset_ % LOG_BLOCK_LEN;
            uint64_t block_start_location = initial_offset_ - offset_in_block;

            // Don't search a block if we'd be in the trailer
            if (offset_in_block > LOG_BLOCK_LEN - 6) {
                block_start_location += LOG_BLOCK_LEN;
            }

            bufferEndPos = block_start_location;

            // Skip to start of first block that can contain the initial record
            if (block_start_location > 0) {
                Status skip_status = file_->Skip(block_start_location);
                if (!skip_status.ok()) {
                    ReportDrop(block_start_location, skip_status);
                    return false;
                }
            }

            return true;
        }

        // 读取的文件的内容是1条条log record构成
        bool Reader::ReadRecord(Slice *dest, std::string *scratch) {
            if (last_record_offset_ < initial_offset_) {
                if (!SkipToInitialBlock()) {
                    return false;
                }
            }

            scratch->clear();
            dest->clear();

            bool inFragmentedRecord = false;
            // Record offset of the logical dest that we're reading
            // 0 is a dummy value to make compilers happy
            uint64_t prospectiveRecordOffset = 0;

            Slice fragment;
            while (true) {
                // 得到的是去掉LOG_HEADER_LEN个字节后的1条log record的纯数据
                const unsigned int recordType = this->ReadPhysicalRecord(&fragment);

                // ReadPhysicalRecord may have only had an empty trailer remaining in its
                // internal buffer. Calculate the offset of the next physical dest now
                // that it has returned, properly accounting for its header size.
                uint64_t physicalRecordOffset = bufferEndPos - buffer_.size() - LOG_HEADER_LEN - fragment.size();

                if (resyncing_) {
                    if (recordType == kMiddleType) {
                        continue;
                    }

                    if (recordType == kLastType) {
                        resyncing_ = false;
                        continue;
                    }

                    resyncing_ = false;
                }

                switch (recordType) {
                    case kFullType:
                        if (inFragmentedRecord) {
                            // Handle bug in earlier versions of log::Writer where
                            // it could emit an empty kFirstType dest at the tail end
                            // of a block followed by a kFullType or kFirstType dest
                            // at the beginning of the next block.
                            if (!scratch->empty()) {
                                ReportCorruption(scratch->size(), "partial dest without end(1)");
                            }
                        }
                        prospectiveRecordOffset = physicalRecordOffset;
                        scratch->clear();
                        *dest = fragment;
                        last_record_offset_ = prospectiveRecordOffset;
                        return true;
                    case kFirstType:
                        if (inFragmentedRecord) {
                            // Handle bug in earlier versions of log::Writer where
                            // it could emit an empty kFirstType dest at the tail end
                            // of a block followed by a kFullType or kFirstType dest
                            // at the beginning of the next block.
                            if (!scratch->empty()) {
                                ReportCorruption(scratch->size(), "partial dest without end(2)");
                            }
                        }
                        prospectiveRecordOffset = physicalRecordOffset;
                        scratch->assign(fragment.data(), fragment.size());
                        inFragmentedRecord = true;
                        break;
                    case kMiddleType:
                        if (!inFragmentedRecord) {
                            ReportCorruption(fragment.size(),
                                             "missing start of fragmented dest(1)");
                        } else {
                            scratch->append(fragment.data(), fragment.size());
                        }
                        break;

                    case kLastType:
                        if (!inFragmentedRecord) {
                            ReportCorruption(fragment.size(),"missing start of fragmented dest(2)");
                        } else {
                            scratch->append(fragment.data(), fragment.size());
                            *dest = Slice(*scratch);
                            last_record_offset_ = prospectiveRecordOffset;
                            return true;
                        }
                        break;
                    case kEof:
                        if (inFragmentedRecord) {
                            // This can be caused by the writer dying immediately after
                            // writing a physical dest but before completing the next; don't
                            // treat it as a corruption, just ignore the entire logical dest.
                            scratch->clear();
                        }

                        return false;
                    case kBadRecord:
                        if (inFragmentedRecord) {
                            ReportCorruption(scratch->size(), "error in middle of dest");
                            inFragmentedRecord = false;
                            scratch->clear();
                        }
                        break;
                    default: {
                        char buf[40];
                        std::snprintf(buf, sizeof(buf), "unknown dest type %u", recordType);
                        ReportCorruption(
                                (fragment.size() + (inFragmentedRecord ? scratch->size() : 0)),
                                buf);
                        inFragmentedRecord = false;
                        scratch->clear();
                        break;
                    }
                }
            }

            return false;
        }

        uint64_t Reader::LastRecordOffset() { return last_record_offset_; }

        void Reader::ReportCorruption(uint64_t bytes, const char *reason) {
            ReportDrop(bytes, Status::Corruption(reason));
        }

        void Reader::ReportDrop(uint64_t bytes, const Status &reason) {
            if (reporter_ != nullptr &&
                bufferEndPos - buffer_.size() - bytes >= initial_offset_) {
                reporter_->Corruption(static_cast<size_t>(bytes), reason);
            }
        }

        // 裸的读取log/manifest文件文件的内容,文件中的每条record对应1个versionEdit
        // 需要的话会校验crc,得到的是去掉LOG_HEADER_LEN字节后的1条log record 纯的数据
        unsigned int Reader::ReadPhysicalRecord(Slice *dest) {
            while (true) {
                // 当前读取到的长度 < 7字节的LOG_HEADER_LEN
                if (buffer_.size() < LOG_HEADER_LEN) {
                    if (eof_) {
                        // Note that if buffer_ is non-empty, we have a truncated header at the
                        // end of the file, which can be caused by the writer crashing in the
                        // middle of writing the header. Instead of considering this an error,
                        // just report EOF.
                        buffer_.clear();
                        return kEof;
                    }

                    // Last read was a full read, so this is a trailer to skip
                    buffer_.clear();
                    Status status = file_->Read(LOG_BLOCK_LEN, &buffer_, backing_store_);
                    bufferEndPos += buffer_.size();

                    if (!status.ok()) {
                        buffer_.clear();
                        ReportDrop(LOG_BLOCK_LEN, status);
                        eof_ = true;
                        return kEof;
                    }

                    if (buffer_.size() < LOG_BLOCK_LEN) {
                        eof_ = true;
                    }

                    continue;
                }

                // Parse the header: 4 byte crc + 2 byte length +1 byte type
                const char *header = buffer_.data();

                // length对应的2byte 小端模式
                const uint32_t a = static_cast<uint32_t>(header[4]) & 0xff;
                const uint32_t b = static_cast<uint32_t>(header[5]) & 0xff;
                const uint32_t length = a | (b << 8);

                // type对应的1byte
                const unsigned int type = header[6];

                // 长度对不上
                if (LOG_HEADER_LEN + length > buffer_.size()) {
                    size_t drop_size = buffer_.size();
                    buffer_.clear();

                    // 是有错误了
                    if (!eof_) {
                        ReportCorruption(drop_size, "bad record length");
                        return kBadRecord;
                    }

                    // If the end of the file has been reached without reading |length| bytes
                    // of payload, assume the writer died in the middle of writing the record.
                    // Don't report a corruption.
                    return kEof;
                }

                if (type == kZeroType && length == 0) {
                    // Skip zero length record without reporting any drops since
                    // such records are produced by the mmap based writing code in
                    // env_posix.cc that preallocates file regions.
                    buffer_.clear();
                    return kBadRecord;
                }

                // crc
                if (checksum_) {
                    uint32_t expected_crc = crc32c::Unmask(DecodeFixed32(header));
                    uint32_t actual_crc = crc32c::Value(header + 6, 1 + length); // 说明crc计算的是 1字节的type+后边的data
                    if (actual_crc != expected_crc) {
                        // Drop the rest of the buffer since "length" itself may have
                        // been corrupted and if we trust it, we could find some
                        // fragment of a real log record that just happens to look
                        // like a valid log record.
                        size_t drop_size = buffer_.size();
                        buffer_.clear();
                        ReportCorruption(drop_size, "checksum mismatch");
                        return kBadRecord;
                    }
                }

                buffer_.remove_prefix(LOG_HEADER_LEN + length);

                // Skip physical record that started before initial_offset_
                if (bufferEndPos - buffer_.size() - LOG_HEADER_LEN - length < initial_offset_) {
                    dest->clear();
                    return kBadRecord;
                }

                *dest = Slice(header + LOG_HEADER_LEN, length);
                return type;
            }
        }

    }  // namespace log
}  // namespace leveldb
