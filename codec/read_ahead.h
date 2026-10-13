#ifndef SWQC_READ_AHEAD_H
#define SWQC_READ_AHEAD_H
#include <algorithm>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <exception>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <unistd.h>

// Four bounded compressed-byte windows retain speculative decode lookbehind.
// Only the I/O worker calls pread; it
// never enters MPI or uses CPEs. The caller owns the fd until this is destroyed.
class SwqcReadAhead {
public:
    struct Stats { uint64_t bytes = 0, calls = 0; double io_s = 0; };
    SwqcReadAhead(int fd, size_t size, size_t window) : fd_(fd), size_(size), window_(window) {
        if (!window_) throw std::invalid_argument("zero read-ahead window");
        for (auto &s : slots_) s.data.reset(new unsigned char[window_]);
        worker_ = std::thread(&SwqcReadAhead::work, this);
    }
    ~SwqcReadAhead() { stop(); }
    void stop() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            stopped_ = true;
        }
        changed_.notify_all();
        if (worker_.joinable()) worker_.join();
    }
    Stats stats() {
        std::lock_guard<std::mutex> lock(mutex_);
        return stats_;
    }
    void read(void *out, size_t bytes, size_t offset) {
        if (offset > size_ || bytes > size_ - offset)
            throw std::out_of_range("read-ahead input range");
        auto *target = static_cast<unsigned char *>(out);
        while (bytes) {
            size_t base = offset / window_ * window_;
            std::unique_lock<std::mutex> lock(mutex_);
            if (stopped_) throw std::runtime_error("read-ahead already stopped");
            if (error_) std::rethrow_exception(error_);
            int found = find(base);
            if (found < 0) {
                changed_.wait(lock, [&] { return active_ < 0 || error_; });
                if (error_) std::rethrow_exception(error_);
                found = victim(-1);
                request(found, base);
            }
            changed_.wait(lock, [&] { return slots_[found].ready || error_; });
            if (error_) std::rethrow_exception(error_);
            size_t n = std::min(bytes, slots_[found].size - (offset - base));
            if (!n) throw std::runtime_error("read-ahead made no progress");
            const unsigned char *source = slots_[found].data.get() + offset - base;
            slots_[found].used = ++clock_;
            size_t next = base + window_;
            if (next < size_ && find(next) < 0 && active_ < 0) request(victim(found), next);
            lock.unlock();
            // Only this caller schedules replacements. The worker uses the
            // other slot until this copy completes and the next read starts.
            std::memcpy(target, source, n);
            target += n; bytes -= n; offset += n;
        }
    }
private:
    struct Slot {
        std::unique_ptr<unsigned char[]> data;
        size_t base = 0, size = 0;
        uint64_t used = 0;
        bool valid = false, ready = false;
    } slots_[4];
    int fd_, active_ = -1;
    uint64_t clock_ = 0;
    size_t size_, window_;
    bool stopped_ = false;
    Stats stats_;
    std::exception_ptr error_;
    std::mutex mutex_;
    std::condition_variable changed_;
    std::thread worker_;
    int find(size_t base) const {
        for (int i = 0; i < 4; ++i)
            if (slots_[i].valid && slots_[i].base == base) return i;
        return -1;
    }
    int victim(int pinned) const {
        int selected = -1;
        for (int i = 0; i < 4; ++i) {
            if (i == pinned) continue;
            if (!slots_[i].valid) return i;
            if (selected < 0 || slots_[i].used < slots_[selected].used) selected = i;
        }
        return selected;
    }
    void request(int slot, size_t base) {
        slots_[slot].base = base;
        slots_[slot].size = std::min(window_, size_ - base);
        slots_[slot].valid = true;
        slots_[slot].ready = false;
        slots_[slot].used = ++clock_;
        active_ = slot;
        changed_.notify_all();
    }
    void work() {
        for (;;) {
            std::unique_lock<std::mutex> lock(mutex_);
            changed_.wait(lock, [&] { return stopped_ || active_ >= 0; });
            if (stopped_) return;
            Slot &s = slots_[active_];
            lock.unlock();
            size_t done = 0, calls = 0;
            auto begin = std::chrono::steady_clock::now();
            std::exception_ptr failure;
            try {
                while (done < s.size) {
                    ++calls;
                    ssize_t n = pread(fd_, s.data.get() + done, s.size - done, (off_t)(s.base + done));
                    if (n < 0 && errno == EINTR) continue;
                    if (n <= 0) throw std::runtime_error("compressed pread failed or input changed");
                    done += n;
                }
            } catch (...) { failure = std::current_exception(); }
            double elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - begin).count();
            lock.lock();
            stats_.io_s += elapsed; stats_.calls += calls; stats_.bytes += done;
            error_ = failure;
            s.ready = !failure;
            active_ = -1;
            changed_.notify_all();
            if (failure) return;
        }
    }
};
#endif
