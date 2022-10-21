// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// File names used by DB code

#ifndef STORAGE_LEVELDB_DB_FILENAME_H_
#define STORAGE_LEVELDB_DB_FILENAME_H_

#include <cstdint>
#include <string>

#include "leveldb/slice.h"
#include "leveldb/status.h"
#include "port/port.h"

namespace leveldb {

    class Env;

    enum FileType {
        kLogFile,
        kDBLockFile,
        kTableFile,
        kDescriptorFile,
        kCurrentFile,
        kTempFile,
        kInfoLogFile  // Either the current one, or an old one
    };

    // dbname/number.log
    std::string LogFileName(const std::string &dbname, uint64_t number);

// Return the name of the sstable with the specified number
// in the db named by "dbname".  The result will be prefixed with
// "dbname".
    std::string TableFileName(const std::string &dbname, uint64_t number);

    // 后来的leveldb中sst文件改名为了ldb 这里是为了向前兼容
    std::string SSTTableFileName(const std::string &dbname, uint64_t number);

// dbname/MANIFEST-number
    std::string DescriptorFileName(const std::string &dbname, uint64_t number);

// dbname + "/CURRENT"
    std::string CurrentFileName(const std::string &dbname);

// Return the name of the lock file for the db named by
// "dbname".  The result will be prefixed with "dbname".
    std::string LockFileName(const std::string &dbname);

// Return the name of a temporary file owned by the db named "dbname".
// The result will be prefixed with "dbname".
    std::string TempFileName(const std::string &dbname, uint64_t number);

// Return the name of the info log file for "dbname".
    std::string InfoLogFileName(const std::string &dbname);

// Return the name of the old info log file for "dbname".
    std::string OldInfoLogFileName(const std::string &dbname);

// If filename is a leveldb file, store the type of the file in *type.
// The number encoded in the filename is stored in *number.  If the
// filename was successfully parsed, returns true.  Else return false.
    bool ParseFileName(const std::string &filename, uint64_t *number, FileType *type);

    Status SetCurrentFile(Env *env, const std::string &dbname, uint64_t descriptorNumber);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_FILENAME_H_
