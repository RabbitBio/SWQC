#ifndef SWQC_GZIP_STREAM_H
#define SWQC_GZIP_STREAM_H
#include "codec_job.h"
#include "line_index.h"
#include "decoded_view.h"
#include <cstdio>
#include <memory>
#include <string>
#include <vector>

struct SwqcGzipMetrics {
    double io_s = 0, launch_s = 0;
    double serial_s = 0, repair_s = 0, read_s = 0;
    double discover_s = 0, resolve_s = 0;
    double kernel_s = 0, queue_s = 0;
    // Exclusive rapid-reader phases; launch times above remain inclusive.
    double input_prepare_s = 0, task_build_s = 0, copy_s = 0;
    double line_index_s = 0;
    double prefetch_io_s = 0;
    uint64_t prefetch_bytes = 0, prefetch_calls = 0;
    uint64_t state_jobs = 0, state_ldm_jobs = 0, state_bytes = 0;
    double discover_run_s = 0, discover_wait_s = 0;
    double resolve_run_s = 0, resolve_wait_s = 0, validate_s = 0;
    uint64_t source_read_bytes = 0, copy_bytes = 0, copy_calls = 0;
    uint64_t rapid_batches = 0;
    uint64_t view_bytes = 0, peak_view_bytes = 0;
    uint64_t capacity_retries = 0, symbol_capacity = 0, peak_symbol_bytes = 0;
    bool memory_source = false;
    uint64_t input_bytes = 0, output_bytes = 0, jobs = 0;
    uint64_t accepted_jobs = 0, repair_jobs = 0, fallback_bytes = 0;
};

// Bounded batches; the complete uncompressed file is never retained.
class SwqcGzipReader {
public:
    SwqcGzipReader(const char *path, size_t chunk_bytes, unsigned workers,
                  const std::string &backend = "auto",
                  const std::vector<unsigned char> *memory = nullptr, bool views = false);
    ~SwqcGzipReader();
    size_t read(void *destination, size_t bytes, SwqcLineIndex *lines = nullptr);
    bool eof() const;
    bool supports_views() const;
    bool peek(size_t maximum, SwqcDecodedView &view);
    void consume(size_t bytes);
    const SwqcGzipMetrics &metrics() const { return metrics_; }
private:
    SwqcGzipReader(const SwqcGzipReader &) = delete;
    SwqcGzipReader &operator=(const SwqcGzipReader &) = delete;
    struct Impl;
    std::unique_ptr<Impl> impl_;
    SwqcGzipMetrics metrics_;
};

class SwqcGzipWriter {
public:
    SwqcGzipWriter(const char *path, size_t chunk_bytes, unsigned workers, int level);
    ~SwqcGzipWriter();
    void append(const void *data, size_t bytes);
    void finish();
    const SwqcGzipMetrics &metrics() const { return metrics_; }
private:
    SwqcGzipWriter(const SwqcGzipWriter &) = delete;
    SwqcGzipWriter &operator=(const SwqcGzipWriter &) = delete;
    void flush();
    void write(const void *data, size_t bytes);
    std::string path_;
    FILE *file_ = nullptr;
    bool complete_ = false;
    size_t chunk_bytes_, batch_chunks_;
    unsigned workers_;
    int level_;
    uint32_t crc_ = 0;
    std::vector<unsigned char> previous_;
    std::vector<std::vector<unsigned char>> input_;
    SwqcGzipMetrics metrics_;
};
#endif
