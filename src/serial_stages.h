#ifndef SWQC_SERIAL_STAGES_H
#define SWQC_SERIAL_STAGES_H
#include "swqc_log.h"
#include <condition_variable>
#include <mutex>
#include <chrono>
#include <cstdio>

/* The existing pipeline threads keep their resource ownership, but only one
 * stage may execute. MPI barriers at stage boundaries align all ranks. */
class SwqcSerialStages {
    enum Phase { READER, PROCESSOR, WRITER };
    std::mutex mutex_;
    std::condition_variable ready_;
    Phase phase_ = READER;
    bool enabled_ = false, closed_ = false, final_ = false;
    unsigned long rounds_ = 0;
    double started_ = 0, seconds_[3] = {0, 0, 0};
    double read_fence_ = 0;

    static double clock() {
        using namespace std::chrono;
        return duration<double>(steady_clock::now().time_since_epoch()).count();
    }
    bool wait(Phase phase) {
        if (!enabled_) return true;
        std::unique_lock<std::mutex> lock(mutex_);
        ready_.wait(lock, [&] { return closed_ || phase_ == phase; });
        if (closed_) return false;
        started_ = clock();
        return true;
    }
    void finish(Phase phase, Phase next, bool final) {
        if (!enabled_) return;
        std::lock_guard<std::mutex> lock(mutex_);
        seconds_[phase] += clock() - started_;
        if (phase == PROCESSOR) final_ = final;
        if (phase == WRITER) {
            ++rounds_;
            closed_ = final_;
        }
        phase_ = next;
        ready_.notify_all();
    }
public:
    void enable() { enabled_ = true; }
    bool enabled() const { return enabled_; }
    bool reader() { return wait(READER); }
    void read_done() { finish(READER, PROCESSOR, false); }
    bool processor() { return wait(PROCESSOR); }
    void processor_ready() {
        if (!enabled_) return;
        double now = clock();
        read_fence_ += now - started_;
        started_ = now;
    }
    void processed(bool final) { finish(PROCESSOR, WRITER, final); }
    bool writer() { return wait(WRITER); }
    void written() { finish(WRITER, READER, false); }
    void report(int rank) const {
        if (enabled_)
            swqc_log_fprintf(stderr, "[SWQC serial stages] rank=%d rounds=%lu reader_s=%.6f processor_s=%.6f writer_s=%.6f read_fence_s=%.6f closed=%d\n",
                    rank, rounds_, seconds_[READER], seconds_[PROCESSOR],
                    seconds_[WRITER], read_fence_, (int)closed_);
    }
};
#endif
