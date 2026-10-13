#pragma once
#include "FileReader.hpp"
#include <cstring>
#include <memory>

namespace rapidgzip {
// SWQC adaptation: the host owns immutable input until the CPE batch joins.
class CpeMemoryFileReader final : public FileReader {
public:
    CpeMemoryFileReader(const char *data, size_t size) : data_(data), size_(size) {}
    std::unique_ptr<CpeMemoryFileReader> cloneMemory() const {
        auto copy = std::make_unique<CpeMemoryFileReader>(data_, size_);
        copy->position_ = position_;
        copy->closed_ = closed_;
        return copy;
    }
    void close() override { closed_ = true; }
    bool closed() const override { return closed_; }
    bool eof() const override { return position_ == size_; }
    bool fail() const override { return false; }
    void clearerr() override {}
    bool seekable() const override { return true; }
    int fileno() const override { return -1; }
    std::optional<size_t> size() const override { return size_; }
    size_t tell() const override { return position_; }
    size_t seek(long long offset, int origin = SEEK_SET) override {
        position_ = effectiveOffsetFor(*this, offset, origin);
        return position_;
    }
    size_t read(char *output, size_t count) override {
        count = std::min(count, size_ - position_);
        if (output && count) std::memcpy(output, data_ + position_, count);
        position_ += count;
        return count;
    }
protected:
    UniqueFileReader cloneRaw() const override { return cloneMemory(); }
private:
    const char *data_;
    size_t size_, position_ = 0;
    bool closed_ = false;
};
}
