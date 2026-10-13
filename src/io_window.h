#ifndef SWQC_IO_WINDOW_H
#define SWQC_IO_WINDOW_H
#include <mpi.h>
#include <algorithm>
#include <cerrno>
#include <climits>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <memory>
#include <time.h>
#include <unistd.h>
#include "swqc_host_io.h"
#include "output_fingerprint.h"

inline double swqc_io_clock() {
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return t.tv_sec + t.tv_nsec * 1e-9;
}

inline void swqc_io_fail(const char *message) {
    fprintf(stderr, "[SWQC I/O] %s (errno=%d)\n", message, errno);
    MPI_Abort(MPI_COMM_WORLD, 1);
    abort();
}

inline size_t swqc_window_bytes(const char *name) {
    const char *value = getenv(name);
    if (!value) return 0;
    char *end;
    long mib = strtol(value, &end, 10);
    if (!*value || *end || mib < 0 || mib > 256) swqc_io_fail("invalid I/O window size");
    return (size_t)mib << 20;
}

inline bool swqc_posix_output() {
    const char *value = getenv("SWQC_POSIX_OUTPUT");
    return value && strcmp(value, "1") == 0;
}

inline bool swqc_discard_output() {
    const char *value = getenv("SWQC_DISCARD_OUTPUT");
    return value && strcmp(value, "1") == 0;
}

inline bool swqc_output_fingerprint() {
    const char *value = getenv("SWQC_OUTPUT_FINGERPRINT");
    return value && strcmp(value, "1") == 0;
}

/* One producer owns this cache. Retain the overlap needed to align the next
 * record; never read past the rank's contiguous stripe just to fill the cache. */
class SwqcReadWindow {
    int fd_;
    size_t capacity_, used_;
    int64_t begin_;
    std::unique_ptr<char[]> buffer_;
    const char *memory_ = NULL;
    bool borrow_ = false;
    int64_t memory_begin_ = 0, memory_end_ = 0;
public:
    double io_s = 0, move_s = 0, memory_s = 0;
    uint64_t calls = 0, bytes = 0;
    SwqcReadWindow(int fd, size_t capacity)
        : fd_(fd), capacity_(capacity), used_(0), begin_(0), buffer_(new char[capacity]) {}

    void set_memory_view(const char *data, int64_t begin, int64_t end, bool borrow = false) {
        if (!data || begin < 0 || end < begin) swqc_io_fail("invalid preloaded view");
        memory_ = data;
        borrow_ = borrow;
        memory_begin_ = begin;
        memory_end_ = end;
        begin_ = begin;
        used_ = 0;
        if (borrow_) used_ = (size_t)(end - begin);
    }

    void ensure(int64_t begin, int64_t end, int64_t limit) {
        if (begin < 0 || end < begin || end > limit || (uint64_t)(end - begin) > capacity_)
            swqc_io_fail("invalid input window bounds");
        if (borrow_) {
            if (begin < memory_begin_ || end > memory_end_)
                swqc_io_fail("borrowed input outside preloaded stripe");
            return;
        }
        if (begin >= begin_ && end <= begin_ + (int64_t)used_) return;
        double t = swqc_io_clock();
        size_t keep = 0;
        if (begin >= begin_ && begin < begin_ + (int64_t)used_) {
            keep = used_ - (size_t)(begin - begin_);
            swqc_host_memmove(buffer_.get(), buffer_.get() + (begin - begin_), keep);
        }
        move_s += swqc_io_clock() - t;
        begin_ = begin;
        used_ = keep;
        size_t target = (size_t)std::min<int64_t>(capacity_, limit - begin);
        t = swqc_io_clock();
        if (used_ < target) {
            if (memory_) {
                if (begin_ + (int64_t)used_ < memory_begin_ ||
                    begin_ + (int64_t)target > memory_end_)
                    swqc_io_fail("input window outside preloaded stripe");
                swqc_host_memcpy(buffer_.get() + used_, memory_ + (begin_ + used_ - memory_begin_), target - used_);
            } else {
                unsigned long read_calls;
                int error = swqc_host_pread(fd_, buffer_.get() + used_, target - used_,
                                            begin_ + used_, &read_calls);
                if (error) { errno = error; swqc_io_fail("short input window read"); }
                calls += read_calls;
                bytes += target - used_;
            }
            used_ = target;
        }
        if (memory_) memory_s += swqc_io_clock() - t;
        else io_s += swqc_io_clock() - t;
    }

    const char *at(int64_t offset) const {
        if (offset < begin_ || offset > begin_ + (int64_t)used_)
            swqc_io_fail("input window offset outside cache");
        return (borrow_ ? memory_ : buffer_.get()) + (offset - begin_);
    }

    bool line(int64_t &pos, const char *&data, size_t &length) const {
        data = at(pos);
        size_t available = used_ - (size_t)(pos - begin_);
        if (!available) return false;
        const char *eol = (const char *)memchr(data, '\n', available);
        if (!eol) swqc_io_fail("record exceeds input window or missing newline");
        length = (size_t)(eol - data);
        if (length && data[length - 1] == '\r') --length;
        pos += eol - data + 1;
        return true;
    }
};

/* A buffer may span pieces only when their assigned file offsets are adjacent.
 * Flush before a gap, so buffering never changes the ordered file layout. */
class SwqcWriteWindow {
    MPI_File file_;
    int fd_ = -1;
    size_t capacity_, used_;
    MPI_Offset begin_;
    std::unique_ptr<char[]> buffer_;
    bool discard_ = false, fingerprint_enabled_ = false;

    void write(const char *data, size_t size, MPI_Offset offset) {
        while (size) {
            int block = (int)std::min<size_t>(size, INT_MAX);
            MPI_Status status;
            double t = swqc_io_clock();
            int written = 0;
            if (discard_) {
                written = block;
            } else if (fd_ >= 0) {
                ssize_t n;
                do { n = pwrite(fd_, data, block, (off_t)offset); }
                while (n < 0 && errno == EINTR);
                if (n <= 0) swqc_io_fail("failed POSIX output write");
                written = (int)n;
            } else {
                int result = MPI_File_write_at(file_, offset, data, block, MPI_BYTE, &status);
                if (result != MPI_SUCCESS || MPI_Get_count(&status, MPI_BYTE, &written) != MPI_SUCCESS || written != block)
                    swqc_io_fail("short MPI output write");
            }
            if (!discard_) io_s += swqc_io_clock() - t;
            ++calls;
            bytes += written;
            size -= written;
            data += written;
            offset += written;
        }
    }
public:
    SwqcOutputFingerprint fingerprint;
    double io_s = 0, copy_s = 0;
    uint64_t calls = 0, bytes = 0;
    SwqcWriteWindow(MPI_File file, size_t capacity, bool discard = false, bool fingerprint_enabled = false)
        : file_(file), capacity_(capacity), used_(0), begin_(0),
          buffer_(capacity ? new char[capacity] : NULL), discard_(discard),
          fingerprint_enabled_(fingerprint_enabled) {}

    SwqcWriteWindow(const char *path, size_t capacity)
        : SwqcWriteWindow(MPI_FILE_NULL, capacity) {
        static_assert(sizeof(off_t) >= 8, "output offsets must be 64 bit");
        fd_ = open(path, O_WRONLY);
        if (fd_ < 0) swqc_io_fail("cannot open POSIX output");
    }

    void close() {
        flush();
        if (fd_ >= 0) {
            int result = ::close(fd_);
            fd_ = -1;
            if (result != 0) swqc_io_fail("cannot close POSIX output");
        }
    }

    ~SwqcWriteWindow() { if (fd_ >= 0) ::close(fd_); }

    void flush() {
        if (!used_) return;
        write(buffer_.get(), used_, begin_);
        used_ = 0;
    }

    void append(const char *data, size_t size, MPI_Offset offset) {
        if (fingerprint_enabled_ && !fingerprint.append(data, size, offset))
            swqc_io_fail("output fingerprint offset overflow");
        if (!capacity_) { write(data, size, offset); return; }
        if (used_ && offset != begin_ + (MPI_Offset)used_) flush();
        while (size) {
            if (!used_) begin_ = offset;
            size_t part = std::min(size, capacity_ - used_);
            double t = swqc_io_clock();
            swqc_host_memcpy(buffer_.get() + used_, data, part);
            copy_s += swqc_io_clock() - t;
            used_ += part;
            size -= part;
            data += part;
            offset += part;
            if (used_ == capacity_) flush();
        }
    }
};
#endif
