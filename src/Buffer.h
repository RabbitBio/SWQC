/*
  This file is a part of DSRC software distributed under GNU GPL 2 licence.
  The homepage of the DSRC project is http://sun.aei.polsl.pl/dsrc

  Authors: Lucas Roguski and Sebastian Deorowicz

  Version: 2.00
  Modified by Zekun Yin
  zekun.yin@mail.sdu.edu.cn
*/

#ifndef BUFFER_H
#define BUFFER_H

#include <algorithm>
#include <memory>
#include "Globals.h"
#include "utils.h"

namespace rabbit {

    namespace core {

#define USE_64BIT_MEMORY 1

/**
 * @brief: buffer to store chunk data
 */
        class Buffer {
        public:
            Buffer(uint64 size_) {
                owns_buffer = true;

#if (USE_64BIT_MEMORY)
                uint64 size64 = size_ / 8;
                if (size64 * 8 < size_) size64 += 1;
                buffer = size64 ? new uint64[size64] : NULL;
#else
                buffer = size_ ? new byte[size_] : NULL;
#endif
                size = size_;
            }

            ~Buffer() { if (owns_buffer) delete[] buffer; }

            // An optional lease keeps decoded storage alive through QC and writing.
            void SetView(byte *data, uint64 bytes, std::shared_ptr<void> owner = {}) {
                if (owns_buffer) delete[] buffer;
                view_owner = std::move(owner);
#if (USE_64BIT_MEMORY)
                buffer = reinterpret_cast<uint64 *>(data);
#else
                buffer = data;
#endif
                size = bytes;
                owns_buffer = false;
            }

            void ResetView() {
                if (!owns_buffer) {
                    buffer = NULL;
                    size = 0;
                    owns_buffer = true;
                    view_owner.reset();
                }
            }

            uint64 Size() const { return size; }

            /// return the pointer of buffer
            byte *Pointer() const { return (byte *) buffer; }

#if (USE_64BIT_MEMORY)

            uint64 *Pointer64() const { return buffer; }

#endif

            /// resize the buffer
            void Extend(uint64 size_, bool copy_ = false) {
                if (!owns_buffer) {
                    uint64 bytes = std::max(size, size_);
#if (USE_64BIT_MEMORY)
                    uint64 *p = new uint64[(bytes + 7) / 8];
#else
                    byte *p = new byte[bytes];
#endif
                    if (copy_) std::copy(Pointer(), Pointer() + size, (byte *)p);
                    buffer = p;
                    size = bytes;
                    owns_buffer = true;
                    view_owner.reset();
                    return;
                }
#if (USE_64BIT_MEMORY)
                uint64 size64 = size / 8;
                if (size64 * 8 < size) size64 += 1;

                uint64 newSize64 = size_ / 8;
                if (newSize64 * 8 < size_) newSize64 += 1;

                if (size > size_) return;

                if (size64 == newSize64) {
                    size = size_;
                    return;
                }

                uint64 *p = new uint64[newSize64];

                if (copy_) std::copy(buffer, buffer + size64, p);
#else
                if (size > size_) return;

                byte *p = new byte[size_];

                if (copy_) std::copy(buffer, buffer + size, p);

#endif
                delete[] buffer;

                buffer = p;
                size = size_;
            }

            void Swap(Buffer &b) {
                TSwap(b.buffer, buffer);
                TSwap(b.size, size);
                TSwap(b.owns_buffer, owns_buffer);
                view_owner.swap(b.view_owner);
            }

        private:
            Buffer(const Buffer &) {}

            Buffer &operator=(const Buffer &) { return *this; }

#if (USE_64BIT_MEMORY)
            uint64 *buffer;
#else
            byte *buffer;
#endif
            uint64 size;
            bool owns_buffer;
            std::shared_ptr<void> view_owner;
        };

/**
 * @brief: rabbitio chunk data wapper
 */
        struct DataChunk {
            /// default swap buffer size
            static const uint64 DefaultBufferSize = 1 << 20;  // 1 << 22

            /// chunk data
            Buffer data;
            /// chunk size
            uint64 size;
            /// list to matain all sequence chunk in one part
            DataChunk *next = NULL;  //[haoz:]added for all seqchunk in one part

            DataChunk(const uint64 bufferSize_ = DefaultBufferSize) : data(bufferSize_), size(0) {}

            void Reset() {
                data.ResetView();
                size = 0;
                next = NULL;
            }
        };

        struct DataPairChunk {
            DataChunk *left_part;
            DataChunk *right_part;
        };

    }  // namespace core

}  // namespace rabbit

#endif  // BUFFER_H
