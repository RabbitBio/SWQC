#ifndef SWQC_ORDERED_INPUT_H
#define SWQC_ORDERED_INPUT_H
#include "FastxChunk.h"
#include "io_window.h"
#include "swqc_log.h"
#include "replay_input.h"
#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <algorithm>
#include <memory>
#include <sys/stat.h>
#include <vector>
#include <utility>

/* Fixed byte stripes, aligned forward to complete FASTQ records. Assign each
 * rank one CPE_COUNT-wide stripe per round. Existing round/rank output prefixes
 * then preserve input order without a second pass or a data gather at rank 0.
 * Only plain, four-line, byte-aligned paired FASTQ is accepted. */
class OrderedPairReader {
    FILE *input_[2], *boundary_[2];
    int rank_, ranks_;
    int64_t size_, ordinal_, chunk_bytes_;
    std::unique_ptr<SwqcReadWindow> windows_[2];
    double boundary_s_ = 0, allocation_s_ = 0, copy_s_ = 0, direct_read_s_ = 0;
    uint64_t logical_bytes_ = 0, chunks_ = 0;
    struct PreloadedStripe {
        int64_t begin;
        std::vector<char> mates[2];
    };
    std::vector<PreloadedStripe> preloaded_;
    bool memory_input_ = false;
    bool borrow_input_ = false;
    size_t memory_stripe_ = size_t(-1);
    std::unique_ptr<SwqcReplayInput> replay_;
    std::vector<rabbit::fq::FastqDataPairChunk *> replay_batch_;
    std::vector<uint64_t> replay_ids_;
    size_t replay_position_ = 0;
    double replay_generate_s_ = 0;
    bool profile_rounds_ = false;

    rabbit::fq::FastqDataPairChunk *next_replay(rabbit::fq::FastqDataPool *pool) {
        if (replay_position_ == replay_batch_.size()) {
            replay_batch_.clear(); replay_ids_.clear(); replay_position_ = 0;
            double t = swqc_io_clock();
            for (int i = 0; i < SWQC_CPE_COUNT; ++i) {
                uint64_t id = (ordinal_ / SWQC_CPE_COUNT * ranks_ + rank_) * SWQC_CPE_COUNT
                            + ordinal_ % SWQC_CPE_COUNT;
                if (id >= replay_->chunks()) break;
                ++ordinal_;
                const auto &c = replay_->chunk(id);
                auto *p = new rabbit::fq::FastqDataPairChunk;
                pool->Acquire(p->left_part); pool->Acquire(p->right_part);
                rabbit::fq::FastqDataChunk *parts[2] = {p->left_part, p->right_part};
                for (int m = 0; m < 2; ++m) {
                    if (parts[m]->data.Size() < c.data[m].size() + 1)
                        parts[m]->data.Extend(c.data[m].size() + 1);
                    parts[m]->size = c.data[m].size() - 1;
                    logical_bytes_ += c.data[m].size();
                }
                replay_batch_.push_back(p); replay_ids_.push_back(id); ++chunks_;
            }
            allocation_s_ += swqc_io_clock() - t;
            t = swqc_io_clock();
            swqc_host_for(replay_batch_.size(), [](void *opaque, int, size_t begin, size_t end) {
                auto &r = *static_cast<OrderedPairReader *>(opaque);
                for (size_t i = begin; i < end; ++i) {
                    auto *p = r.replay_batch_[i];
                    r.replay_->materialize(r.replay_ids_[i], (char *)p->left_part->data.Pointer(),
                                          (char *)p->right_part->data.Pointer());
                }
            }, this);
            replay_generate_s_ += swqc_io_clock() - t;
            if (profile_rounds_ && !replay_ids_.empty()) {
                uint64_t pairs = 0, bytes = 0, bases = 0, low = 0, anchors = 0;
                for (uint64_t id : replay_ids_) {
                    const auto &c = replay_->chunk(id);
                    pairs += c.names.size(); bytes += c.data[0].size() + c.data[1].size();
                    bases += c.bases; low += c.low_quality_bases; anchors += c.anchor_pairs;
                }
                swqc_log_fprintf(stderr, "[SWQC replay round] rank=%d round=%llu first_chunk=%llu chunks=%zu pairs=%llu bytes=%llu low_q20=%.6f anchor_fraction=%.6f\n",
                    rank_, (unsigned long long)(replay_ids_.front() / (SWQC_CPE_COUNT * uint64_t(ranks_))),
                    (unsigned long long)replay_ids_.front(), replay_ids_.size(),
                    (unsigned long long)pairs, (unsigned long long)bytes,
                    bases ? double(low) / bases : 0.0, pairs ? double(anchors) / pairs : 0.0);
            }
        }
        return replay_position_ < replay_batch_.size() ? replay_batch_[replay_position_++] : NULL;
    }

    int64_t align_window(const SwqcReadWindow &window, int64_t nominal) {
        if (nominal <= 0) return 0;
        if (nominal >= size_) return size_;
        int64_t pos = nominal;
        const char *s;
        size_t n;
        if (*window.at(nominal - 1) != '\n' && !window.line(pos, s, n)) return size_;
        while (pos < size_) {
            int64_t candidate = pos;
            if (!window.line(pos, s, n)) return size_;
            int64_t after_header = pos;
            if (n && s[0] == '@') {
                if (!window.line(pos, s, n)) break;
                size_t bases = n;
                bool valid = bases != 0;
                for (size_t i = 0; valid && i < n; ++i)
                    valid = (s[i] >= 'A' && s[i] <= 'Z') || (s[i] >= 'a' && s[i] <= 'z') || s[i] == '.';
                if (!window.line(pos, s, n)) break;
                valid = valid && n && s[0] == '+';
                if (!window.line(pos, s, n)) break;
                if (valid && n == bases) return candidate;
            }
            pos = after_header;
            if (pos - nominal > chunk_bytes_) fail("record exceeds chunk size or invalid FASTQ");
        }
        return size_;
    }

    static void fail(const char *reason) {
        fprintf(stderr, "[SWQC ordered input] %s\n", reason);
        MPI_Abort(MPI_COMM_WORLD, 1);
        abort();
    }

    static size_t content_length(const char *s, ssize_t n) {
        if (n < 0) return 0;
        while (n && (s[n - 1] == '\n' || s[n - 1] == '\r')) --n;
        return (size_t)n;
    }

    int64_t align(FILE *file, int64_t nominal) {
        if (nominal <= 0) return 0;
        if (nominal >= size_) return size_;
        if (fseeko(file, nominal - 1, SEEK_SET)) fail("boundary seek failed");
        char *line[4] = {NULL, NULL, NULL, NULL};
        size_t cap[4] = {0, 0, 0, 0};
        ssize_t len[4];
        if (fgetc(file) != '\n') getline(&line[0], &cap[0], file);
        int64_t result = size_;
        for (;;) {
            int64_t candidate = ftello(file);
            len[0] = getline(&line[0], &cap[0], file);
            if (len[0] < 0) break;
            int64_t after_header = ftello(file);
            if (line[0][0] == '@') {
                for (int i = 1; i < 4; ++i) len[i] = getline(&line[i], &cap[i], file);
                size_t bases = content_length(line[1], len[1]);
                bool valid = bases && len[2] > 0 && line[2][0] == '+' &&
                    content_length(line[3], len[3]) == bases;
                for (size_t i = 0; valid && i < bases; ++i) {
                    char c = line[1][i];
                    valid = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '.';
                }
                if (valid) { result = candidate; break; }
            }
            if (fseeko(file, after_header, SEEK_SET)) fail("boundary rewind failed");
            if (after_header - nominal > chunk_bytes_) fail("record exceeds chunk size or invalid FASTQ");
        }
        if (ferror(file)) fail("boundary read failed");
        for (int i = 0; i < 4; ++i) free(line[i]);
        return result;
    }

public:
    void replay(const std::string &r1, const std::string &r2, uint64_t pairs,
                uint64_t copies, size_t limit_bytes, const std::string &layout = "source",
                size_t block_chunks = 32, uint64_t seed = 1) {
        if (ordinal_ || memory_input_ || replay_) fail("replay requires unused input");
        double t = swqc_io_clock();
        replay_.reset(new SwqcReplayInput(r1, r2, pairs, copies, chunk_bytes_, limit_bytes,
                                         layout, block_chunks, seed));
        swqc_log_fprintf(stderr, "[SWQC replay layout] rank=%d layout=%s block_chunks=%zu seed=%llu\n",
                         rank_, layout.c_str(), block_chunks, (unsigned long long)seed);
        swqc_log_fprintf(stderr, "[SWQC replay input] rank=%d seed_pairs=%llu copies=%llu seed_bytes=%llu logical_pairs=%llu logical_bytes=%llu seed_chunks=%llu logical_chunks=%llu load_s=%.6f\n",
            rank_, (unsigned long long)pairs, (unsigned long long)copies,
            (unsigned long long)replay_->bytes(), (unsigned long long)(pairs * copies),
            (unsigned long long)(replay_->bytes() * copies),
            (unsigned long long)replay_->seed_chunks(), (unsigned long long)replay_->chunks(),
            swqc_io_clock() - t);
    }
    void preload(size_t limit_bytes, bool borrow = false) {
        if (!windows_[0] || ordinal_ || memory_input_) fail("preload requires unused windowed input");
        double started = swqc_io_clock();
        const int64_t stripe = int64_t(SWQC_CPE_COUNT) * chunk_bytes_;
        const int64_t step = int64_t(ranks_) * stripe;
        size_t total = 0;
        for (int64_t base = int64_t(rank_) * stripe; base < size_;) {
            int64_t begin = std::max<int64_t>(0, base - 1);
            int64_t length = std::min(size_ - base, stripe + chunk_bytes_) + (base - begin);
            if (uint64_t(length) > (limit_bytes - total) / 2)
                fail("rank preload exceeds explicit memory limit");
            total += size_t(length) * 2;
            PreloadedStripe part;
            part.begin = begin;
            preloaded_.push_back(std::move(part));
            if (step >= size_ - base) break;
            base += step;
        }
        for (size_t n = 0; n < preloaded_.size(); ++n) {
            auto &part = preloaded_[n];
            int64_t base = (int64_t(n) * ranks_ + rank_) * stripe;
            size_t length = size_t(std::min(size_ - base, stripe + chunk_bytes_) + (base - part.begin));
            for (int i = 0; i < 2; ++i) {
                part.mates[i].resize(length + 1);
                unsigned long calls;
                int error = swqc_host_pread(fileno(input_[i]), part.mates[i].data(), length, part.begin, &calls);
                if (error) { errno = error; fail("preload read failed"); }
            }
        }
        memory_input_ = true;
        borrow_input_ = borrow;
        swqc_log_fprintf(stderr, "[SWQC input ownership] rank=%d borrowed=%d\n", rank_, int(borrow_input_));
        swqc_log_fprintf(stderr, "[SWQC input preload] rank=%d bytes=%zu stripes=%zu seconds=%.6f mode=full_real_input\n",
                rank_, total, preloaded_.size(), swqc_io_clock() - started);
    }

    OrderedPairReader(const std::string &r1, const std::string &r2,
                      int rank, int ranks, int64_t chunk_bytes, size_t window_bytes = 0)
        : rank_(rank), ranks_(ranks), ordinal_(0), chunk_bytes_(chunk_bytes) {
        const char *profile = getenv("SWQC_PROFILE_ROUNDS");
        profile_rounds_ = profile && strcmp(profile, "1") == 0;
        const char *paths[2] = {r1.c_str(), r2.c_str()};
        struct stat st[2];
        for (int i = 0; i < 2; ++i) {
            if (stat(paths[i], &st[i]) || !S_ISREG(st[i].st_mode)) fail("input is not a regular file");
            input_[i] = fopen(paths[i], "rb");
            boundary_[i] = fopen(paths[i], "rb");
            if (!input_[i] || !boundary_[i]) fail("cannot open FASTQ");
        }
        if (st[0].st_size != st[1].st_size) fail("mates must have identical byte boundaries");
        size_ = st[0].st_size;
        if (window_bytes) {
            if (window_bytes < (uint64_t)chunk_bytes_ * 2 + 1)
                fail("input window must exceed two nominal chunks");
            for (int i = 0; i < 2; ++i)
                windows_[i].reset(new SwqcReadWindow(fileno(input_[i]), window_bytes));
        }
    }

    ~OrderedPairReader() {
        if (replay_)
            swqc_log_fprintf(stderr, "[SWQC replay generation] rank=%d chunks=%llu allocation_s=%.6f generate_s=%.6f workers=%d\n",
                rank_, (unsigned long long)chunks_, allocation_s_, replay_generate_s_, swqc_host_threads());
        // The owner is destroyed only after both pipeline threads have joined.
        // Release disjoint payload allocations on the available MPEs, retaining
        // the same measured lifetime as the ordinary vector destructor.
        double release_started = swqc_io_clock();
        swqc_host_for(preloaded_.size() * 2, [](void *opaque, int, size_t begin, size_t end) {
            auto &parts = *static_cast<std::vector<PreloadedStripe> *>(opaque);
            for (size_t i = begin; i < end; ++i)
                std::vector<char>().swap(parts[i / 2].mates[i % 2]);
        }, &preloaded_);
        preloaded_.clear();
        const double release_s = swqc_io_clock() - release_started;
        swqc_log_fprintf(stderr, "[SWQC input release] rank=%d workers=%d seconds=%.6f\n",
                rank_, swqc_host_threads(), release_s);
        double io = direct_read_s_, move = 0, memory = 0;
        uint64_t calls = 0, bytes = 0;
        for (int i = 0; i < 2; ++i) if (windows_[i]) {
            io += windows_[i]->io_s; move += windows_[i]->move_s;
            memory += windows_[i]->memory_s;
            calls += windows_[i]->calls; bytes += windows_[i]->bytes;
        }
        swqc_log_fprintf(stderr, "[SWQC input detail] rank=%d cached=%d chunks=%llu logical_bytes=%llu read_bytes=%llu pread_calls=%llu io_s=%.6f boundary_s=%.6f allocation_s=%.6f copy_s=%.6f cache_move_s=%.6f memory_input=%d memory_fill_s=%.6f\n",
                rank_, windows_[0] ? 1 : 0, (unsigned long long)chunks_,
                (unsigned long long)logical_bytes_, (unsigned long long)bytes,
                (unsigned long long)calls, io, boundary_s_, allocation_s_, copy_s_, move, int(memory_input_), memory);
        for (int i = 0; i < 2; ++i) { fclose(input_[i]); fclose(boundary_[i]); }
    }

    rabbit::fq::FastqDataPairChunk *next(rabbit::fq::FastqDataPool *pool) {
        if (replay_) return next_replay(pool);
        const int64_t id = (ordinal_ / SWQC_CPE_COUNT * ranks_ + rank_) * SWQC_CPE_COUNT
                         + ordinal_ % SWQC_CPE_COUNT;
        ++ordinal_;
        if (id > size_ / chunk_bytes_ || id * chunk_bytes_ >= size_) return NULL;
        int64_t nominal = id * chunk_bytes_;
        int64_t next = std::min(size_, (id + 1) * chunk_bytes_);
        if (windows_[0]) {
            if (memory_input_) {
                size_t stripe_index = size_t((ordinal_ - 1) / SWQC_CPE_COUNT);
                if (stripe_index >= preloaded_.size()) fail("missing preloaded stripe");
                if (stripe_index != memory_stripe_) {
                    auto &part = preloaded_[stripe_index];
                    for (int i = 0; i < 2; ++i)
                        windows_[i]->set_memory_view(part.mates[i].data(), part.begin,
                                                    part.begin + part.mates[i].size() - 1,
                                                    borrow_input_);
                    memory_stripe_ = stripe_index;
                }
            }
            int64_t stripe_end = (id / SWQC_CPE_COUNT + 1) * SWQC_CPE_COUNT * chunk_bytes_;
            int64_t limit = std::min(size_, stripe_end + chunk_bytes_);
            for (int i = 0; i < 2; ++i)
                windows_[i]->ensure(std::max<int64_t>(0, nominal - 1),
                                    std::min(size_, next + chunk_bytes_), limit);
        }
        double t = swqc_io_clock();
        int64_t begin = windows_[0] ? align_window(*windows_[0], nominal) : align(boundary_[0], nominal);
        int64_t end = windows_[0] ? align_window(*windows_[0], next) : align(boundary_[0], next);
        if (begin == size_) return NULL;
        if (begin >= end || end - begin > 2 * chunk_bytes_) fail("unsupported oversized FASTQ record");
        int64_t begin2 = windows_[1] ? align_window(*windows_[1], nominal) : align(boundary_[1], nominal);
        int64_t end2 = windows_[1] ? align_window(*windows_[1], next) : align(boundary_[1], next);
        if (begin2 != begin || end2 != end)
            fail("mate byte boundaries differ");
        boundary_s_ += swqc_io_clock() - t;
        t = swqc_io_clock();
        rabbit::fq::FastqDataPairChunk *pair = new rabbit::fq::FastqDataPairChunk;
        allocation_s_ += swqc_io_clock() - t;
        rabbit::fq::FastqDataChunk *parts[2];
        for (int i = 0; i < 2; ++i) {
            t = swqc_io_clock();
            pool->Acquire(parts[i]);
            size_t bytes = (size_t)(end - begin);
            if (borrow_input_) {
                parts[i]->data.SetView((unsigned char *)windows_[i]->at(begin), bytes + 1);
            } else if (parts[i]->data.Size() < bytes + 1) {
                parts[i]->data.Extend(bytes + 1);
            }
            allocation_s_ += swqc_io_clock() - t;
            unsigned char *data = parts[i]->data.Pointer();
            t = swqc_io_clock();
            if (borrow_input_) {
                // The preload owner outlives both processing and serialization.
            } else if (windows_[i]) {
                swqc_host_memcpy(data, windows_[i]->at(begin), bytes);
                copy_s_ += swqc_io_clock() - t;
            } else {
                if (fseeko(input_[i], begin, SEEK_SET) || fread(data, 1, bytes, input_[i]) != bytes)
                    fail("short FASTQ read");
                direct_read_s_ += swqc_io_clock() - t;
            }
            if (!borrow_input_) data[bytes] = 0;
            if (data[bytes - 1] != '\n') fail("FASTQ must end with a newline");
            parts[i]->size = bytes - 1;
            if (bytes > 1 && data[bytes - 2] == '\r') --parts[i]->size;
        }
        pair->left_part = parts[0];
        pair->right_part = parts[1];
        logical_bytes_ += 2 * (end - begin);
        ++chunks_;
        return pair;
    }
};
#endif
