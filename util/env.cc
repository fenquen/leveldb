// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/env.h"

#include <cstdarg>

// This workaround can be removed when leveldb::Env::DeleteFile is removed.
// See env.h for justification.
#if defined(_WIN32) && defined(LEVELDB_DELETEFILE_UNDEFINED)
#undef DeleteFile
#endif

namespace leveldb {

    Env::Env() = default;

    Env::~Env() = default;

    Status Env::NewAppendableFile(const std::string &fname, WritableFile **result) {
        return Status::NotSupported("NewAppendableFile", fname);
    }

    Status Env::RemoveDir(const std::string &dirname) { return DeleteDir(dirname); }

    Status Env::DeleteDir(const std::string &dirname) { return RemoveDir(dirname); }

    Status Env::RemoveFile(const std::string &fname) { return DeleteFile(fname); }

    Status Env::DeleteFile(const std::string &fname) { return RemoveFile(fname); }

    SequentialFile::~SequentialFile() = default;

    RandomAccessFile::~RandomAccessFile() = default;

    WritableFile::~WritableFile() = default;

    Logger::~Logger() = default;

    FileLock::~FileLock() = default;

    void Log(Logger *info_log, const char *format, ...) {
        if (info_log == nullptr) {
            return;
        }

        std::va_list vaList;
        va_start(vaList, format);
        info_log->Logv(format, vaList);
        va_end(vaList);
    }

    static Status DoWriteStringToFile(Env *env, const Slice &data,
                                      const std::string &fname, bool should_sync) {
        WritableFile *file;
        Status s = env->NewWritableFile(fname, &file);
        if (!s.ok()) {
            return s;
        }
        s = file->Append(data);
        if (s.ok() && should_sync) {
            s = file->Sync();
        }
        if (s.ok()) {
            s = file->Close();
        }
        delete file;  // Will auto-close if we did not close above
        if (!s.ok()) {
            env->RemoveFile(fname);
        }
        return s;
    }

    Status WriteStringToFile(Env *env, const Slice &data,
                             const std::string &fname) {
        return DoWriteStringToFile(env, data, fname, false);
    }

    Status WriteStringToFileSync(Env *env, const Slice &data,
                                 const std::string &fname) {
        return DoWriteStringToFile(env, data, fname, true);
    }

    Status ReadFileToString(Env *env, const std::string &filePath, std::string *dest) {
        dest->clear();

        SequentialFile *sequentialFile;
        Status status = env->NewSequentialFile(filePath, &sequentialFile);
        if (!status.ok()) {
            return status;
        }

        static const int BUFFER_SIZE = 8192;

        char *buffer = new char[BUFFER_SIZE];
        while (true) {
            Slice slice;

            status = sequentialFile->Read(BUFFER_SIZE, &slice, buffer);
            if (!status.ok()) {
                break;
            }

            dest->append(slice.data(), slice.size());

            if (slice.empty()) {
                break;
            }
        }

        delete[] buffer;
        delete sequentialFile;

        return status;
    }

    EnvWrapper::~EnvWrapper() {}

}  // namespace leveldb
