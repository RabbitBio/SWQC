#include "gzip_pipeline.h"
#include "swqc_runtime.h"
#include "swqc_log.h"
#include "globalMutex.h"
#include "../codec/byte_source.h"
#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <stdexcept>
#include <chrono>

static double reader_clock() {
    return std::chrono::duration<double>(std::chrono::steady_clock::now().time_since_epoch()).count();
}

extern "C" void slave_swqc_codec_worker(void *);
extern "C" void swqc_codec_launch(swqc_codec_batch *batch) {
    swqc_runtime_run_profile((void *)slave_swqc_codec_worker, batch, &batch->run_s, &batch->wait_s);
}
bool swqc_standard_gzip() {
    const char *s = getenv("SWQC_STANDARD_GZIP");
    return s && strcmp(s, "1") == 0;
}
bool swqc_gzip_zero_copy() {
    const char *s = getenv("SWQC_GZIP_ZERO_COPY");
    return !s || strcmp(s, "0") != 0;
}
unsigned swqc_gzip_workers() {
    const char *s = getenv("SWQC_GZIP_WORKERS");
    return s ? (unsigned)atoi(s) : SWQC_CPE_COUNT;
}
size_t swqc_gzip_chunk_bytes() {
    const char *s = getenv("SWQC_GZIP_CHUNK_MIB");
    return size_t(s ? atoi(s) : 1) << 20;
}
void swqc_gzip_report(const char *phase, const char *path, const SwqcGzipMetrics &m) {
    swqc_log_fprintf(stderr, "[SWQC gzip pipeline] phase=%s file=%s input_bytes=%llu output_bytes=%llu jobs=%llu read_s=%.6f io_s=%.6f cpe_wait_and_run_s=%.6f serial_s=%.6f window_s=%.6f discover_s=%.6f resolve_s=%.6f kernel_s=%.6f queue_s=%.6f accepted=%llu repair_jobs=%llu fallback_bytes=%llu\n",
        phase, path, (unsigned long long)m.input_bytes, (unsigned long long)m.output_bytes,
        (unsigned long long)m.jobs, m.read_s, m.io_s, m.launch_s, m.serial_s, m.repair_s,
        m.discover_s, m.resolve_s, m.kernel_s, m.queue_s,
        (unsigned long long)m.accepted_jobs, (unsigned long long)m.repair_jobs,
        (unsigned long long)m.fallback_bytes);
    if (m.rapid_batches) {
        // Partition read_s without counting the CPE launch/wait twice. The
        // residual includes temporary-buffer destruction and reader control.
        const double launch_overhead = m.launch_s - m.kernel_s - m.queue_s;
        const double other = m.read_s - (m.io_s + m.input_prepare_s + m.task_build_s +
            m.launch_s + m.repair_s + m.copy_s + m.line_index_s + m.validate_s + m.serial_s);
        swqc_log_fprintf(stderr, "[SWQC gzip phases] file=%s source=%s api_s=%.9f input_read_s=%.9f input_prepare_s=%.9f task_build_s=%.9f discover_run_s=%.9f discover_wait_s=%.9f link_s=%.9f resolve_run_s=%.9f resolve_wait_s=%.9f copy_s=%.9f launch_overhead_s=%.9f validate_s=%.9f fallback_s=%.9f other_s=%.9f batches=%llu source_read_bytes=%llu copy_calls=%llu copy_bytes=%llu\n",
            path, m.memory_source ? "memory" : "file", m.read_s, m.io_s,
            m.input_prepare_s, m.task_build_s, m.discover_run_s, m.discover_wait_s,
            m.repair_s, m.resolve_run_s, m.resolve_wait_s, m.copy_s,
            launch_overhead, m.validate_s, m.serial_s, other,
            (unsigned long long)m.rapid_batches, (unsigned long long)m.source_read_bytes,
            (unsigned long long)m.copy_calls, (unsigned long long)m.copy_bytes);
        swqc_log_fprintf(stderr, "[SWQC gzip capacity] file=%s retries=%llu max_symbols_per_job=%llu peak_symbol_bytes=%llu\n",
            path, (unsigned long long)m.capacity_retries,
            (unsigned long long)m.symbol_capacity, (unsigned long long)m.peak_symbol_bytes);
        swqc_log_fprintf(stderr, "[SWQC gzip line index] file=%s host_s=%.9f page_bytes=%d\n",
            path, m.line_index_s, int(SwqcLineIndex::PAGE));
        swqc_log_fprintf(stderr, "[SWQC gzip prefetch] file=%s syscall_s=%.9f source_bytes=%llu calls=%llu\n",
            path, m.prefetch_io_s, (unsigned long long)m.prefetch_bytes, (unsigned long long)m.prefetch_calls);
        swqc_log_fprintf(stderr, "[SWQC gzip decoder state] file=%s bytes=%llu jobs=%llu ldm_jobs=%llu\n",
            path, (unsigned long long)m.state_bytes, (unsigned long long)m.state_jobs,
            (unsigned long long)m.state_ldm_jobs);
        swqc_log_fprintf(stderr, "[SWQC gzip views] file=%s consumed_bytes=%llu peak_live_bytes=%llu\n",
            path, (unsigned long long)m.view_bytes, (unsigned long long)m.peak_view_bytes);
    }
}

struct SwqcGzipPairReader::Input {
    std::string path;
    std::vector<unsigned char> memory;
    std::unique_ptr<SwqcGzipReader> gzip;
    std::unique_ptr<SwqcByteSource> file;
    std::vector<char> tail;
    SwqcLineIndex lines;
    bool eof = false;
    bool borrowed = false;
    SwqcDecodedView view;
    size_t size = 0;
    uint64_t refills = 0, source_bytes = 0, carry_bytes = 0;
    double refill_s = 0, boundary_s = 0, carry_s = 0;
    Input(const std::string &name, bool preload, size_t &limit) : path(name) {
        if (preload) {
            memory = SwqcByteSource::preload(name.c_str(), limit);
            limit -= memory.size();
        }
        const auto *source = preload ? &memory : nullptr;
        if (swqc_gzip_path(name)) {
            const char *backend = getenv("SWQC_GZIP_READER");
            const char *chunk = getenv("SWQC_GZIP_READ_CHUNK_KIB");
            gzip.reset(new SwqcGzipReader(name.c_str(), size_t(chunk ? atoi(chunk) : 256) << 10,
                                         swqc_gzip_workers(), backend ? backend : "auto", source,
                                         swqc_gzip_zero_copy()));
        }
        else {
            file.reset(new SwqcByteSource(name.c_str(), source));
        }
    }
    ~Input() {
        if (gzip) swqc_gzip_report("inflate", path.c_str(), gzip->metrics());
    }
    void consume_view(size_t bytes) {
        gzip->consume(bytes);
        source_bytes += bytes;
    }
    void fill_view(rabbit::fq::FastqDataChunk *part) {
        double started = reader_clock();
        borrowed = false;
        size = 0;
        lines.clear();
        ++refills;
        if (!gzip->peek(BLOCK_SIZE, view)) {
            eof = true;
            refill_s += reader_clock() - started;
            return;
        }
        if (view.lines.lines() >= 4) {
            part->data.SetView(view.data, view.size, view.owner);
            size = view.size;
            lines = std::move(view.lines);
            borrowed = true;
            refill_s += reader_clock() - started;
            return;
        }
        // Only a record crossing a decode-batch boundary needs a bridge copy.
        part->data.Extend(BLOCK_SIZE + 2);
        auto *data = part->data.Pointer();
        for (;;) {
            size_t take = view.size;
            const size_t need = 4 - lines.lines();
            if (view.lines.lines() >= need)
                take = view.lines.end_of_line(view.data, need);
            if (take > BLOCK_SIZE - size)
                throw std::runtime_error("FASTQ record exceeds input block: " + path);
            double copy_start = reader_clock();
            memcpy(data + size, view.data, take);
            lines.append(data + size, take);
            size += take;
            carry_bytes += take;
            carry_s += reader_clock() - copy_start;
            consume_view(take);
            if (lines.lines() == 4) break;
            if (!gzip->peek(BLOCK_SIZE, view)) {
                eof = true;
                if (size && data[size - 1] != '\n') {
                    data[size++] = '\n';
                    lines.append(data + size - 1, 1);
                }
                if (lines.lines() != 4)
                    throw std::runtime_error("truncated FASTQ record: " + path);
                break;
            }
        }
        view.owner.reset();
        refill_s += reader_clock() - started;
    }
    void fill(rabbit::fq::FastqDataChunk *part) {
        if (gzip && gzip->supports_views()) {
            fill_view(part);
            return;
        }
        // Decode directly into the pool allocation that QC will retain. Only
        // the unconsumed suffix crosses chunk boundaries; no per-record copy.
        if (part->data.Size() < BLOCK_SIZE + 2) part->data.Extend(BLOCK_SIZE + 2);
        char *data = reinterpret_cast<char *>(part->data.Pointer());
        double started = reader_clock();
        size = tail.size();
        if (size) memcpy(data, tail.data(), size);
        lines.clear();
        lines.append(reinterpret_cast<unsigned char *>(data), size);
        carry_s += reader_clock() - started;
        tail.clear();
        started = reader_clock();
        while (size < BLOCK_SIZE && !eof) {
            size_t n = gzip ? gzip->read(data + size, BLOCK_SIZE - size, &lines)
                            : file->read(data + size, BLOCK_SIZE - size);
            if (!gzip) lines.append(reinterpret_cast<unsigned char *>(data + size), n);
            ++refills; source_bytes += n; size += n;
            if (!n) eof = true;
        }
        refill_s += reader_clock() - started;
        started = reader_clock();
        if (eof && size && data[size - 1] != '\n') {
            data[size++] = '\n';
            lines.append(reinterpret_cast<unsigned char *>(data + size - 1), 1);
        }
        boundary_s += reader_clock() - started;
        if (eof && (lines.lines() % 4 || (size && (!lines.lines() ||
            lines.end_of_line(reinterpret_cast<unsigned char *>(data), lines.lines()) != size))))
            throw std::runtime_error("truncated FASTQ record: " + path);
        if (!eof && lines.lines() < 4)
            throw std::runtime_error("FASTQ record exceeds input block: " + path);
    }
    size_t finish(rabbit::fq::FastqDataChunk *part, size_t records) {
        char *data = reinterpret_cast<char *>(part->data.Pointer());
        double started = reader_clock();
        const size_t used = lines.end_of_line(reinterpret_cast<unsigned char *>(data), records * 4);
        boundary_s += reader_clock() - started;
        if (gzip && gzip->supports_views()) {
            if (borrowed) consume_view(used);
            view.owner.reset();
            part->size = used - 1;
            return used;
        }
        started = reader_clock();
        tail.assign(data + used, data + size);
        carry_bytes += tail.size();
        carry_s += reader_clock() - started;
        data[used] = 0;
        part->size = used - 1;
        return used;
    }
};
SwqcGzipPairReader::SwqcGzipPairReader(const std::string &r1, const std::string &r2) {
    const char *setting = getenv("SWQC_PRELOAD_INPUT");
    const bool preload = setting && strcmp(setting, "1") == 0;
    const char *bound = getenv("SWQC_PRELOAD_LIMIT_MIB");
    size_t limit = size_t(bound ? strtoul(bound, nullptr, 10) : 16384) << 20;
    const auto start = std::chrono::steady_clock::now();
    input_[0].reset(new Input(r1, preload, limit));
    input_[1].reset(new Input(r2, preload, limit));
    if (preload) {
        double seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
        swqc_log_fprintf(stderr, "[SWQC gzip preload] bytes=%llu seconds=%.6f compressed=%d outside_pipeline=1\n",
            (unsigned long long)(input_[0]->memory.size() + input_[1]->memory.size()), seconds,
            int(swqc_gzip_path(r1)));
    }
}
SwqcGzipPairReader::~SwqcGzipPairReader() {
    swqc_log_fprintf(stderr, "[SWQC reader bulk] chunks=%llu pairs=%llu bytes=%llu refill_s=%.6f boundary_s=%.6f carry_s=%.6f acquire_s=%.6f carry_bytes=%llu\n",
        (unsigned long long)chunks_, (unsigned long long)pairs_, (unsigned long long)bytes_,
        input_[0]->refill_s + input_[1]->refill_s,
        input_[0]->boundary_s + input_[1]->boundary_s,
        input_[0]->carry_s + input_[1]->carry_s, acquire_s_,
        (unsigned long long)(input_[0]->carry_bytes + input_[1]->carry_bytes));
    for (int m = 0; m < 2; ++m)
        swqc_log_fprintf(stderr, "[SWQC reader source] mate=%d refills=%llu source_bytes=%llu\n",
            m + 1,
            (unsigned long long)input_[m]->refills, (unsigned long long)input_[m]->source_bytes);
}

rabbit::fq::FastqDataPairChunk *SwqcGzipPairReader::next(rabbit::fq::FastqDataPool *pool) {
    rabbit::fq::FastqDataChunk *parts[2] = {nullptr, nullptr};
    try {
        double started = reader_clock();
        pool->Acquire(parts[0]); pool->Acquire(parts[1]);
        acquire_s_ += reader_clock() - started;
        for (int m = 0; m < 2; ++m) input_[m]->fill(parts[m]);
        size_t records = std::min(input_[0]->lines.lines() / 4, input_[1]->lines.lines() / 4);
        if (!records) {
            if (input_[0]->lines.lines() || input_[1]->lines.lines())
                throw std::runtime_error("paired FASTQ record counts differ");
            pool->Release(parts[0]); pool->Release(parts[1]);
            return nullptr;
        }
        std::unique_ptr<rabbit::fq::FastqDataPairChunk> pair(new rabbit::fq::FastqDataPairChunk);
        for (int m = 0; m < 2; ++m) bytes_ += input_[m]->finish(parts[m], records);
        pair->left_part = parts[0]; pair->right_part = parts[1];
        pairs_ += records; ++chunks_;
        return pair.release();
    } catch (...) {
        for (auto *part : parts) if (part) pool->Release(part);
        throw;
    }
}
