#include "rapid_reader.h"
#include "gzip_stream.h"
#include "byte_source.h"
#include "crc_combine.h"
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <stdexcept>
#include <sys/stat.h>
#include <zlib.h>

namespace {
double now() {
    return std::chrono::duration<double>(std::chrono::steady_clock::now().time_since_epoch()).count();
}
const size_t GUARD = 128U << 10;
const size_t HISTORY = 32768;
const size_t MAX_SYMBOL_CAPACITY = size_t(256) << 20;
}

struct SwqcRapidReader::Impl {
    SwqcByteSource file;
    SwqcGzipMetrics &metrics;
    uint64_t file_size = 0, cursor = 0, member_size = 0;
    uint32_t crc = 0;
    size_t chunk;
    size_t capacity;
    unsigned workers;
    size_t symbol_budget = size_t(2048) << 20;
    bool finished = false;
    bool views = false;
    std::shared_ptr<unsigned char> view_output;
    SwqcLineIndex view_lines;
    size_t view_size = 0, view_offset = 0;
    std::shared_ptr<std::atomic<size_t>> live_views{new std::atomic<size_t>(0)};
    std::vector<unsigned char> history;
    std::vector<std::unique_ptr<uint16_t[]>> output;
    std::vector<std::unique_ptr<uint32_t[]>> line_counts;
    std::vector<size_t> lengths;
    size_t buffer = 0, offset = 0;
    z_stream fallback{};
    bool fallback_active = false;
    std::vector<unsigned char> fallback_input;

    Impl(const char *path, size_t size, unsigned n, SwqcGzipMetrics &m,
         const std::vector<unsigned char> *memory, bool borrow)
        : file(path, memory), metrics(m), chunk(size),
          capacity(std::min(MAX_SYMBOL_CAPACITY, size * 8 + 65536)), workers(n), views(borrow) {
        if (const char *budget = std::getenv("SWQC_GZIP_SYMBOL_BUDGET_MIB")) {
            char *end = nullptr;
            unsigned long mib = std::strtoul(budget, &end, 10);
            if (!*budget || *end || mib < 512 || mib > 2048)
                throw std::runtime_error("gzip symbol budget must be 512..2048 MiB");
            symbol_budget = size_t(mib) << 20;
        }
        file_size = file.size();
        metrics.input_bytes = file_size;
        metrics.memory_source = memory != nullptr;
        if (!file_size) throw std::runtime_error("empty gzip input");
        header(0, false);
        size_t prefetch_mib = 64;
        if (const char *setting = std::getenv("SWQC_GZIP_PREFETCH_MIB")) {
            char *end = nullptr;
            unsigned long value = std::strtoul(setting, &end, 10);
            if (!*setting || *end || value > 256)
                throw std::runtime_error("gzip prefetch must be 0..256 MiB");
            prefetch_mib = value;
        }
        file.enable_prefetch(prefetch_mib << 20);
    }
    ~Impl() {
        if (fallback_active) inflateEnd(&fallback);
    }
    void allocate_view(size_t bytes) {
        auto live = live_views;
        unsigned char *data = new unsigned char[bytes];
        size_t total = live->fetch_add(bytes, std::memory_order_relaxed) + bytes;
        view_output = std::shared_ptr<unsigned char>(data,
            [live, bytes](unsigned char *p) {
                delete[] p;
                live->fetch_sub(bytes, std::memory_order_relaxed);
            });
        metrics.peak_view_bytes = std::max<uint64_t>(metrics.peak_view_bytes, total);
        view_size = bytes;
    }
    void update_prefetch_metrics() {
        if (finished) file.finish_prefetch();
        const auto io = file.prefetch_stats();
        metrics.prefetch_io_s = io.io_s;
        metrics.prefetch_bytes = io.bytes;
        metrics.prefetch_calls = io.calls;
    }
    unsigned byte() {
        return file.byte();
    }
    void seek(uint64_t pos) {
        file.seek(pos);
    }
    void header(uint64_t pos, bool allow_end) {
        seek(pos);
        if (allow_end && pos == file_size) { finished = true; return; }
        unsigned first = byte();
        if (allow_end && first == 0) {
            for (uint64_t i = pos + 1; i < file_size; ++i)
                if (byte()) throw std::runtime_error("nonzero bytes after gzip padding");
            finished = true;
            return;
        }
        uint32_t hcrc = 0;
        auto take = [&]() { unsigned char c = (unsigned char)byte(); hcrc = crc32(hcrc, &c, 1); return (unsigned)c; };
        unsigned char f = (unsigned char)first;
        hcrc = crc32(hcrc, &f, 1);
        if (first != 31 || take() != 139 || take() != 8) throw std::runtime_error("invalid gzip header");
        unsigned flags = take();
        if (flags & 0xe0) throw std::runtime_error("reserved gzip header flags");
        for (int i = 0; i < 6; ++i) take();
        if (flags & 4) {
            unsigned len = take(); len |= take() << 8;
            for (unsigned i = 0; i < len; ++i) take();
        }
        for (unsigned flag : {8U, 16U}) if (flags & flag) while (take()) {}
        if (flags & 2) {
            unsigned stored = byte(); stored |= byte() << 8;
            if (stored != (hcrc & 65535)) throw std::runtime_error("gzip header CRC mismatch");
        }
        cursor = (uint64_t)file.tell() * 8;
        history.clear(); crc = 0; member_size = 0;
    }
    void trailer() {
        seek((cursor + 7) / 8);
        uint32_t stored_crc = 0, stored_size = 0;
        for (unsigned i = 0; i < 4; ++i) stored_crc |= byte() << (8 * i);
        for (unsigned i = 0; i < 4; ++i) stored_size |= byte() << (8 * i);
        if (stored_crc != crc || stored_size != (uint32_t)member_size)
            throw std::runtime_error("gzip member CRC/length mismatch");
        header(file.tell(), true);
    }
    void launch(std::vector<swqc_codec_job> &jobs) {
        swqc_codec_batch batch{jobs.data(), (unsigned)jobs.size(), workers, 0, 0};
        double t = now(); swqc_codec_launch(&batch); double elapsed = now() - t;
        metrics.launch_s += elapsed;
        metrics.kernel_s += batch.run_s; metrics.queue_s += batch.wait_s;
        if (jobs.front().operation == SWQC_CODEC_DISCOVER) {
            for (const auto &job : jobs) {
                ++metrics.state_jobs;
                metrics.state_ldm_jobs += job.state_in_ldm != 0;
                metrics.state_bytes = std::max<uint64_t>(metrics.state_bytes, job.state_bytes);
            }
            metrics.discover_s += elapsed;
            metrics.discover_run_s += batch.run_s;
            metrics.discover_wait_s += batch.wait_s;
        } else {
            metrics.resolve_s += elapsed;
            metrics.resolve_run_s += batch.run_s;
            metrics.resolve_wait_s += batch.wait_s;
        }
        metrics.jobs += jobs.size();
    }
    std::vector<unsigned char> update_history(const uint16_t *symbols, size_t count,
                                              const unsigned char *previous, size_t previous_size) {
        const size_t keep = count < HISTORY ? std::min(previous_size, HISTORY - count) : 0;
        std::vector<unsigned char> tail(keep + std::min(count, HISTORY));
        if (count < HISTORY) {
            if (keep) std::memcpy(tail.data(), previous + previous_size - keep, keep);
        }
        size_t out = keep;
        for (size_t i = count > HISTORY ? count - HISTORY : 0; i < count; ++i) {
            unsigned symbol = symbols[i];
            if (symbol >= 256) {
                if (symbol < 65536U - previous_size) throw std::runtime_error("invalid gzip backward reference");
                symbol = previous[symbol - (65536U - previous_size)];
            }
            tail[out++] = (unsigned char)symbol;
        }
        return tail;
    }
    void begin_fallback() {
        // Use bounded serial decoding if discovery still fails after the
        // capacity retries. Do not create a full-file index or retain its output.
        seek(cursor / 8);
        if (inflateInit2(&fallback, -15) != Z_OK) throw std::runtime_error("cannot initialize gzip fallback");
        fallback_active = true;
        if (cursor % 8 && inflatePrime(&fallback, 8 - cursor % 8, byte() >> (cursor % 8)) != Z_OK)
            throw std::runtime_error("cannot prime gzip fallback");
        if (!history.empty() && inflateSetDictionary(&fallback, history.data(), history.size()) != Z_OK)
            throw std::runtime_error("cannot set gzip fallback window");
        fallback_input.resize(1U << 20);
    }
    void fill_fallback() {
        unsigned char *data;
        if (views) {
            allocate_view(1U << 20);
            data = view_output.get();
        } else {
            output.emplace_back(new uint16_t[(1U << 20) / 2]);
            line_counts.emplace_back(nullptr);
            data = reinterpret_cast<unsigned char *>(output.back().get());
        }
        fallback.next_out = data; fallback.avail_out = 1U << 20;
        int result = Z_OK;
        double t = now();
        while (fallback.avail_out && result != Z_STREAM_END) {
            if (!fallback.avail_in) {
                fallback.avail_in = file.read(fallback_input.data(), fallback_input.size());
                fallback.next_in = fallback_input.data();
                if (!fallback.avail_in) throw std::runtime_error("truncated gzip fallback");
            }
            uInt before_in = fallback.avail_in, before_out = fallback.avail_out;
            result = inflate(&fallback, Z_NO_FLUSH);
            if ((result != Z_OK && result != Z_STREAM_END) ||
                (result == Z_OK && before_in == fallback.avail_in && before_out == fallback.avail_out))
                throw std::runtime_error("invalid gzip fallback stream");
        }
        metrics.serial_s += now() - t;
        size_t n = (1U << 20) - fallback.avail_out;
        if (views) {
            view_size = n;
            const double index_start = now();
            view_lines.append(data, n);
            metrics.line_index_s += now() - index_start;
        } else lengths.push_back(n);
        metrics.fallback_bytes += n; member_size += n; crc = crc32(crc, data, n);
        if (result == Z_STREAM_END) {
            cursor = ((uint64_t)file.tell() - fallback.avail_in) * 8;
            inflateEnd(&fallback); fallback_active = false; fallback = z_stream{};
            trailer();
        }
    }
    void fill() {
        output.clear(); lengths.clear(); line_counts.clear(); buffer = offset = 0;
        view_output.reset(); view_lines.clear(); view_size = view_offset = 0;
        if (finished) return;
        if (fallback_active) { fill_fallback(); return; }
        double t = now();
        size_t count = std::min<size_t>(workers, symbol_budget / (capacity * sizeof(uint16_t)));
        const uint64_t base = cursor / 8;
        const size_t size = (size_t)std::min<uint64_t>(file_size - base, count * chunk + (1U << 20));
        std::vector<unsigned char> input(size + GUARD, 0);
        metrics.input_prepare_s += now() - t;
        t = now();
        seek(base);
        if (file.read(input.data(), size) != size) throw std::runtime_error("gzip read failed");
        metrics.io_s += now() - t;
        metrics.source_read_bytes += size;
        ++metrics.rapid_batches;
        std::vector<swqc_codec_job> jobs;
        std::vector<size_t> bases;
        std::vector<std::unique_ptr<uint16_t[]>> symbols;
        std::vector<std::unique_ptr<unsigned char[]>> tails;
        for (;;) {
            t = now();
            // Release the previous attempt BEFORE enlarging its buffers. The
            // byte budget is shared by the whole batch, not by each CPE.
            jobs.clear(); bases.clear(); symbols.clear(); tails.clear();
            count = std::min<size_t>(workers, symbol_budget / (capacity * sizeof(uint16_t)));
            for (size_t i = 0; i < count && i * chunk < size; ++i) {
                size_t begin = i * chunk;
                symbols.emplace_back(new uint16_t[capacity]);
                tails.emplace_back(new unsigned char[HISTORY]);
                swqc_codec_job j{};
                j.input = input.data() + begin;
                j.logical_size = std::min(size - begin, chunk + (1U << 20));
                j.input_size = j.logical_size + GUARD;
                j.symbols = symbols.back().get(); j.output_capacity = capacity;
                j.tail = tails.back().get();
                j.start_bit = i ? 0 : cursor % 8; j.stop_bit = chunk * 8;
                j.known_start = i == 0;
                if (!i) { j.dictionary = history.data(); j.dictionary_size = history.size(); }
                j.operation = SWQC_CODEC_DISCOVER;
                bases.push_back(begin); jobs.push_back(j);
            }
            metrics.symbol_capacity = std::max<uint64_t>(metrics.symbol_capacity, capacity);
            metrics.peak_symbol_bytes = std::max<uint64_t>(metrics.peak_symbol_bytes,
                jobs.size() * capacity * sizeof(uint16_t));
            metrics.task_build_s += now() - t;
            launch(jobs);
            // Only the verified starting block can teach us the required
            // capacity. Speculative false positives must not inflate the pool.
            if (!jobs.front().output_full || capacity == MAX_SYMBOL_CAPACITY) break;
            capacity = std::min(MAX_SYMBOL_CAPACITY, capacity * 2);
            ++metrics.capacity_retries;
            // Reuse compressed input and keep the learned capacity for the
            // following batches (and concatenated gzip members).
        }
        std::vector<swqc_codec_job> accepted;
        std::vector<std::vector<unsigned char>> windows;
        windows.reserve(jobs.size());
        const unsigned char *window = history.data();
        size_t window_size = history.size();
        bool last = false;
        t = now();
        for (;;) {
            size_t found = jobs.size();
            uint64_t relative = cursor - base * 8;
            for (size_t i = 0; i < jobs.size(); ++i) {
                const auto &j = jobs[i];
                if (j.status == Z_OK && relative >= bases[i] * 8 + j.start_bit &&
                    relative <= bases[i] * 8 + j.start_max_bit) { found = i; break; }
            }
            if (found == jobs.size()) break;
            auto j = jobs[found];
            j.dictionary = window; j.dictionary_size = window_size;
            if (j.tail_known) { window = j.tail; window_size = HISTORY; }
            else {
                windows.push_back(update_history(symbols[found].get(), j.output_size, window, window_size));
                window = windows.back().data(); window_size = windows.back().size();
            }
            output.push_back(std::move(symbols[found])); lengths.push_back(j.output_size);
            j.symbols = output.back().get(); j.output = reinterpret_cast<unsigned char *>(output.back().get());
            line_counts.emplace_back(new uint32_t[(j.output_size + SwqcLineIndex::PAGE - 1) / SwqcLineIndex::PAGE]);
            j.line_counts = line_counts.back().get();
            j.operation = SWQC_CODEC_RESOLVE;
            accepted.push_back(j);
            cursor = base * 8 + bases[found] * 8 + j.end_bit;
            jobs[found].status = Z_DATA_ERROR;
            ++metrics.accepted_jobs;
            last = j.final_block;
            if (last) break;
        }
        metrics.repair_s += now() - t;
        if (accepted.empty()) {
            begin_fallback(); fill_fallback(); return;
        }
        if (views) {
            t = now();
            size_t bytes = 0;
            for (const auto &j : accepted) bytes += j.output_size;
            allocate_view(bytes);
            size_t pos = 0;
            for (auto &j : accepted) {
                j.output = view_output.get() + pos;
                pos += j.output_size;
            }
            metrics.task_build_s += now() - t;
        }
        launch(accepted); metrics.repair_jobs += accepted.size();
        t = now();
        if (window_size && window != history.data()) history.assign(window, window + window_size);
        static const SwqcCrcCombine combine;
        for (const auto &j : accepted) {
            if (j.status != Z_OK) throw std::runtime_error("gzip window repair failed");
            crc = combine.combine(crc, j.crc, j.output_size);
            member_size += j.output_size;
        }
        if (last) trailer();
        metrics.validate_s += now() - t;
        if (views) {
            t = now();
            for (const auto &j : accepted)
                view_lines.append(j.output, j.output_size, j.line_counts);
            metrics.line_index_s += now() - t;
            output.clear(); lengths.clear(); line_counts.clear();
        }
    }
};

SwqcRapidReader::SwqcRapidReader(const char *path, size_t chunk, unsigned workers, SwqcGzipMetrics &m,
                               const std::vector<unsigned char> *memory, bool views)
    : impl_(new Impl(path, chunk, workers, m, memory, views)) {}
SwqcRapidReader::~SwqcRapidReader() = default;
bool SwqcRapidReader::eof() const {
    if (impl_->views) return impl_->finished && impl_->view_offset == impl_->view_size;
    return impl_->finished && impl_->buffer == impl_->output.size();
}
bool SwqcRapidReader::peek(size_t maximum, SwqcDecodedView &view) {
    auto &s = *impl_;
    if (!s.views || !maximum) throw std::logic_error("invalid decoded view request");
    view.owner.reset(); view.data = nullptr; view.size = 0; view.lines.clear();
    while (s.view_offset == s.view_size && !s.finished) s.fill();
    s.update_prefetch_metrics();
    if (s.view_offset == s.view_size) return false;
    view.owner = s.view_output;
    view.data = s.view_output.get() + s.view_offset;
    view.size = std::min(maximum, s.view_size - s.view_offset);
    const double t = now();
    view.lines.append_slice(s.view_lines, view.data, s.view_offset, view.size);
    s.metrics.line_index_s += now() - t;
    return true;
}
void SwqcRapidReader::consume(size_t bytes) {
    auto &s = *impl_;
    if (!s.views || bytes > s.view_size - s.view_offset)
        throw std::out_of_range("decoded view consumption");
    s.view_offset += bytes;
    s.metrics.view_bytes += bytes;
}
size_t SwqcRapidReader::read(void *out, size_t bytes, SwqcLineIndex *lines) {
    auto &s = *impl_;
    if (s.views) throw std::logic_error("use peek/consume for borrowed gzip input");
    auto *target = static_cast<unsigned char *>(out);
    size_t copied = 0;
    while (copied < bytes) {
        if (s.buffer == s.output.size()) {
            if (s.finished) break;
            s.fill();
            if (s.output.empty()) continue;
        }
        size_t n = std::min(bytes - copied, s.lengths[s.buffer] - s.offset);
        const double t = now();
        std::memcpy(target + copied, reinterpret_cast<const unsigned char *>(s.output[s.buffer].get()) + s.offset, n);
        s.metrics.copy_s += now() - t;
        if (lines) {
            const double index_start = now();
            lines->append(target + copied, n, s.line_counts[s.buffer].get(), s.offset);
            s.metrics.line_index_s += now() - index_start;
        }
        s.metrics.copy_bytes += n;
        ++s.metrics.copy_calls;
        copied += n; s.offset += n;
        if (s.offset == s.lengths[s.buffer]) { ++s.buffer; s.offset = 0; }
    }
    s.update_prefetch_metrics();
    return copied;
}
