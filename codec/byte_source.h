#ifndef SWQC_BYTE_SOURCE_H
#define SWQC_BYTE_SOURCE_H
#include <algorithm>
#include <cstdio>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <sys/stat.h>
#include <vector>
#include "read_ahead.h"

// Borrowed memory must outlive the reader. No filesystem calls in memory mode.
class SwqcByteSource {
public:
    SwqcByteSource(const char *path, const std::vector<unsigned char> *memory = nullptr)
        : memory_(memory), file_(nullptr, &fclose) {
        if (memory_) size_ = memory_->size();
        else {
            file_.reset(fopen(path, "rb"));
            struct stat st;
            if (!file_ || fstat(fileno(file_.get()), &st) || st.st_size < 0)
                throw std::runtime_error("cannot open/stat input");
            size_ = st.st_size;
        }
    }
    size_t size() const { return size_; }
    size_t tell() const { return position_; }
    void enable_prefetch(size_t bytes) {
        if (bytes && !memory_ && !prefetch_)
            prefetch_.reset(new SwqcReadAhead(fileno(file_.get()), size_, bytes));
    }
    SwqcReadAhead::Stats prefetch_stats() {
        return prefetch_ ? prefetch_->stats() : SwqcReadAhead::Stats{};
    }
    void finish_prefetch() { if (prefetch_) prefetch_->stop(); }
    void seek(size_t pos) {
        if (pos > size_ || (!memory_ && !prefetch_ && fseeko(file_.get(), (off_t)pos, SEEK_SET)))
            throw std::runtime_error("input seek failed");
        position_ = pos;
    }
    size_t read(void *out, size_t bytes) {
        bytes = std::min(bytes, size_ - position_);
        size_t n;
        if (memory_) {
            if (bytes) memcpy(out, memory_->data() + position_, bytes);
            n = bytes;
        } else if (prefetch_) {
            prefetch_->read(out, bytes, position_);
            n = bytes;
        } else {
            n = fread(out, 1, bytes, file_.get());
            if (n != bytes) throw std::runtime_error("input read failed or file changed");
        }
        position_ += n;
        return n;
    }
    unsigned byte() {
        if (position_ == size_) throw std::runtime_error("truncated input");
        if (memory_) return (*memory_)[position_++];
        if (prefetch_) {
            unsigned char c;
            read(&c, 1);
            return c;
        }
        int c = fgetc(file_.get());
        if (c == EOF) throw std::runtime_error("truncated input");
        ++position_;
        return (unsigned)c;
    }
    static std::vector<unsigned char> preload(const char *path, size_t limit) {
        SwqcByteSource input(path);
        if (input.size() > limit) throw std::runtime_error("paired input exceeds preload limit");
        std::vector<unsigned char> data(input.size());
        input.read(data.data(), data.size());
        return data;
    }
private:
    const std::vector<unsigned char> *memory_;
    std::unique_ptr<FILE, int (*)(FILE *)> file_;
    std::unique_ptr<SwqcReadAhead> prefetch_;
    size_t size_ = 0, position_ = 0;
};
#endif
