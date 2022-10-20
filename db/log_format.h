// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Log format information shared by reader and writer.
// See ../doc/log_format.md for more detail.

#ifndef STORAGE_LEVELDB_DB_LOG_FORMAT_H_
#define STORAGE_LEVELDB_DB_LOG_FORMAT_H_

namespace leveldb {
    namespace log {

        enum RecordType {
            // reserved for pre allocated files
            kZeroType = 0,

            kFullType = 1,

            // for fragments
            kFirstType = 2,
            kMiddleType = 3,
            kLastType = 4
        };
        static const int kMaxRecordType = kLastType;

        static const int LOG_BLOCK_LEN = 32768;

// Header is checksum (4 bytes), length (2 bytes), type (1 byte).
        static const int LOG_HEADER_LEN = 4 + 2 + 1;

    }  // namespace log
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_LOG_FORMAT_H_
