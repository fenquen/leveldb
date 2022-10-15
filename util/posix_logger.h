// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Logger implementation that can be shared by all environments
// where enough posix functionality is available.

#ifndef STORAGE_LEVELDB_UTIL_POSIX_LOGGER_H_
#define STORAGE_LEVELDB_UTIL_POSIX_LOGGER_H_

#include <sys/time.h>

#include <cassert>
#include <cstdarg>
#include <cstdio>
#include <ctime>
#include <sstream>
#include <thread>

#include "leveldb/env.h"

namespace leveldb {

    class PosixLogger final : public Logger {
    public:
        // Creates a logger that writes to the given file.
        // The PosixLogger instance takes ownership of the file handle.
        explicit PosixLogger(std::FILE *fp) : fp_(fp) {
            assert(fp != nullptr);
        }

        ~PosixLogger() override {
            std::fclose(fp_);
        }

        void Logv(const char *format, std::va_list arguments) override {
            // Record the time as close to the Logv() call as possible.
            struct ::timeval nowTimeval{};
            ::gettimeofday(&nowTimeval, nullptr);

            const std::time_t nowSeconds = nowTimeval.tv_sec;
            struct std::tm nowComponents{};
            ::localtime_r(&nowSeconds, &nowComponents);

            // record the thread ID.
            constexpr const int threadIdStrMaxLen = 32;
            std::ostringstream thread_stream;
            thread_stream << std::this_thread::get_id();
            std::string threadIdStr = thread_stream.str();
            if (threadIdStr.size() > threadIdStrMaxLen) {
                threadIdStr.resize(threadIdStrMaxLen);
            }

            // We first attempt to print into a stack-allocated buffer. If this attempt
            // fails, we make a second attempt with a dynamically allocated buffer.
            constexpr const int stackBufferSize = 512;
            char stackBuffer[stackBufferSize];
            static_assert(sizeof(stackBuffer) == static_cast<size_t>(stackBufferSize),
                          "sizeof(char) is expected to be 1 in C++");

            int dynamicBufferSize = 0;  // Computed in the first iteration.
            for (int iteration = 0; iteration < 2; ++iteration) {
                const int bufferSize = (iteration == 0) ? stackBufferSize : dynamicBufferSize;
                char *const buffer = (iteration == 0) ? stackBuffer : new char[dynamicBufferSize];

                // Print the header into the buffer.
                int bufferOffset = ::snprintf(buffer,
                                              bufferSize,
                                              "%04d/%02d/%02d-%02d:%02d:%02d.%06d %s ",
                                              nowComponents.tm_year + 1900,
                                              nowComponents.tm_mon + 1,
                                              nowComponents.tm_mday,
                                              nowComponents.tm_hour, nowComponents.tm_min,
                                              nowComponents.tm_sec,
                                              static_cast<int>(nowTimeval.tv_usec),
                                              threadIdStr.c_str());

                // The header can be at most 28 characters (10 date + 15 time +
                // 3 delimiters) plus the thread ID, which should fit comfortably into the
                // static buffer.
                assert(bufferOffset <= 28 + threadIdStrMaxLen);
                static_assert(28 + threadIdStrMaxLen < stackBufferSize,
                              "stack-allocated buffer may not fit the message header");
                assert(bufferOffset < bufferSize);

                // Print the message into the buffer.
                std::va_list arguments_copy;
                va_copy(arguments_copy, arguments);

                bufferOffset += ::vsnprintf(buffer + bufferOffset,
                                            bufferSize - bufferOffset,
                                            format,
                                            arguments_copy);
                va_end(arguments_copy);

                // The code below may append a newline at the end of the buffer, which
                // requires an extra character.
                if (bufferOffset >= bufferSize - 1) {
                    // The message did not fit into the buffer.
                    if (iteration == 0) {
                        // Re-run the loop and use a dynamically-allocated buffer. The buffer
                        // will be large enough for the log message, an extra newline and a null terminator.
                        dynamicBufferSize = bufferOffset + 2;
                        continue;
                    }

                    // The dynamically-allocated buffer was incorrectly sized. This should
                    // not happen, assuming a correct implementation of std::(v)snprintf.
                    // Fail in tests, recover by truncating the log message in production.
                    assert(false);
                    bufferOffset = bufferSize - 1;
                }

                // Add a newline if necessary.
                if (buffer[bufferOffset - 1] != '\n') {
                    buffer[bufferOffset] = '\n';
                    ++bufferOffset;
                }

                assert(bufferOffset <= bufferSize);
                std::fwrite(buffer, 1, bufferOffset, fp_);
                std::fflush(fp_);

                if (iteration != 0) {
                    delete[] buffer;
                }

                break;
            }
        }

    private:
        std::FILE *const fp_;
    };
}

#endif  // STORAGE_LEVELDB_UTIL_POSIX_LOGGER_H_