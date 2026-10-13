#include "gzip_stream.h"
#include "byte_source.h"
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <stdexcept>
#include <vector>

// Exercise arbitrary QC queue fragment boundaries, including zero-length writes.
int main(int argc, char **argv) {
    if (argc != 4 && argc != 5) return 2;
    try {
        swqc_codec_init();
        const char *backend = getenv("SWQC_GZIP_READER");
        const char *read_kib = getenv("SWQC_GZIP_READ_CHUNK_KIB");
        const bool preload = getenv("SWQC_TEST_MEMORY") != nullptr;
        const bool views = getenv("SWQC_TEST_VIEWS") != nullptr;
        std::vector<unsigned char> memory;
        if (preload) memory = SwqcByteSource::preload(argv[1], size_t(1) << 30);
        SwqcGzipReader input(preload ? "/nonexistent-swqc-memory-input" : argv[1], size_t(read_kib ? atoi(read_kib) : 1024) << 10,
                             (unsigned)atoi(argv[3]), backend ? backend : "auto", preload ? &memory : nullptr, views);
        SwqcGzipWriter output(argv[2], 1U << 20, (unsigned)atoi(argv[3]),
                             argc == 5 ? atoi(argv[4]) : 6);
        std::vector<unsigned char> buffer(4U << 20);
        const size_t sizes[] = {1, 7, 4093, (1U << 20) + 117, (4U << 20) - 1};
        size_t i = 0;
        while (!input.eof()) {
            SwqcLineIndex lines;
            SwqcDecodedView view;
            const size_t requested = sizes[i++ % 5];
            size_t n;
            unsigned char *data = buffer.data();
            if (input.supports_views()) {
                input.peek(requested, view);
                n = view.size;
                data = view.data;
                lines = std::move(view.lines);
                input.consume(n);
            } else n = input.read(data, requested, &lines);
            size_t count = 0;
            for (size_t pos = 0; pos < n; ++pos) {
                if (data[pos] == '\n' && lines.end_of_line(data, ++count) != pos + 1)
                    throw std::runtime_error("newline metadata position mismatch");
            }
            if (count != lines.lines()) throw std::runtime_error("newline metadata count mismatch");
            output.append(nullptr, 0);
            output.append(data, n);
        }
        const auto &m = input.metrics();
        if (m.rapid_batches) {
            const double measured = m.io_s + m.input_prepare_s + m.task_build_s +
                m.launch_s + m.repair_s + m.copy_s + m.validate_s + m.serial_s;
            if ((input.supports_views() ? m.view_bytes : m.copy_bytes) != m.output_bytes ||
                measured > m.read_s + 1e-6 ||
                m.discover_run_s + m.resolve_run_s > m.kernel_s + 1e-6 ||
                m.discover_wait_s + m.resolve_wait_s > m.queue_s + 1e-6)
                throw std::runtime_error("rapid phase accounting mismatch");
        }
        output.finish(); output.finish();
        swqc_codec_finish();
        printf("stream roundtrip: bytes=%llu read_jobs=%llu write_jobs=%llu accepted=%llu fallback_bytes=%llu capacity_retries=%llu max_symbols=%llu peak_symbol_bytes=%llu\n",
            (unsigned long long)input.metrics().output_bytes,
            (unsigned long long)input.metrics().jobs,
            (unsigned long long)output.metrics().jobs,
            (unsigned long long)input.metrics().accepted_jobs,
            (unsigned long long)input.metrics().fallback_bytes,
            (unsigned long long)m.capacity_retries, (unsigned long long)m.symbol_capacity,
            (unsigned long long)m.peak_symbol_bytes);
        return 0;
    } catch (const std::exception &e) {
        fprintf(stderr, "stream error: %s\n", e.what()); return 1;
    }
}
