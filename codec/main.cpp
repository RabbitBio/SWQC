#include "gzip_stream.h"
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <memory>
#include <stdexcept>
#include <unistd.h>
#include <vector>

namespace {
double seconds() {
    return std::chrono::duration<double>(std::chrono::steady_clock::now().time_since_epoch()).count();
}
struct Output {
    std::string path;
    FILE *file = nullptr;
    bool complete = false;
    explicit Output(const char *name) : path(name) {
        int fd = open(name, O_WRONLY | O_CREAT | O_EXCL, 0666);
        if (fd < 0) throw std::runtime_error("output must not already exist");
        file = fdopen(fd, "wb");
        if (!file) { close(fd); unlink(name); throw std::runtime_error("fdopen failed"); }
    }
    ~Output() {
        if (file) fclose(file);
        if (!complete) unlink(path.c_str());
    }
    void write(const void *data, size_t bytes) {
        if (bytes && fwrite(data, 1, bytes, file) != bytes) throw std::runtime_error("output write failed");
    }
    void finish() {
        FILE *p = file; file = nullptr;
        if (fclose(p)) throw std::runtime_error("output close failed");
        complete = true;
    }
};
struct Backend {
    Backend() { swqc_codec_init(); }
    ~Backend() { swqc_codec_finish(); }
};
SwqcGzipMetrics decompress(const char *source, const char *destination, size_t chunk, unsigned workers) {
    SwqcGzipReader input(source, chunk, workers);
    Output output(destination);
    std::vector<unsigned char> buffer(1U << 20);
    double write_s = 0;
    while (!input.eof()) {
        const size_t bytes = input.read(buffer.data(), buffer.size());
        const double start = seconds();
        output.write(buffer.data(), bytes);
        write_s += seconds() - start;
    }
    output.finish();
    auto metrics = input.metrics();
    metrics.io_s += write_s;
    return metrics;
}
SwqcGzipMetrics compress(const char *source, const char *destination, size_t chunk,
                         unsigned workers, int level) {
    std::unique_ptr<FILE, decltype(&fclose)> input(fopen(source, "rb"), fclose);
    if (!input) throw std::runtime_error(std::string("cannot open input: ") + strerror(errno));
    SwqcGzipWriter output(destination, chunk, workers, level);
    std::vector<unsigned char> buffer(1U << 20);
    double read_s = 0;
    for (;;) {
        const double start = seconds();
        const size_t bytes = fread(buffer.data(), 1, buffer.size(), input.get());
        if (ferror(input.get())) throw std::runtime_error("input read failed");
        read_s += seconds() - start;
        if (!bytes) break;
        output.append(buffer.data(), bytes);
    }
    output.finish();
    auto metrics = output.metrics();
    metrics.io_s += read_s;
    return metrics;
}
unsigned number(const char *text, unsigned lo, unsigned hi) {
    char *end = nullptr;
    errno = 0;
    const unsigned long value = strtoul(text, &end, 10);
    if (errno || !*text || *end || value < lo || value > hi) throw std::runtime_error("invalid numeric option");
    return (unsigned)value;
}
}
int main(int argc, char **argv) {
    try {
        if (argc < 4 || (strcmp(argv[1], "c") && strcmp(argv[1], "d"))) {
            fprintf(stderr, "Usage: %s c|d INPUT OUTPUT [--workers 1..64] [--chunk-mib 1..64] [--level 1..9]\n", argv[0]);
            return 2;
        }
        unsigned workers = 64, chunk = 1, level = 6;
        for (int i = 4; i < argc; i += 2) {
            if (i + 1 == argc) throw std::runtime_error("option needs a value");
            if (!strcmp(argv[i], "--workers")) workers = number(argv[i+1], 1, 64);
            else if (!strcmp(argv[i], "--chunk-mib")) chunk = number(argv[i+1], 1, 64);
            else if (!strcmp(argv[i], "--level")) level = number(argv[i+1], 1, 9);
            else throw std::runtime_error("unknown option");
        }
        Backend backend;
        const double start = seconds();
        const auto metrics = *argv[1] == 'd'
            ? decompress(argv[2], argv[3], size_t(chunk) << 20, workers)
            : compress(argv[2], argv[3], size_t(chunk) << 20, workers, level);
        printf("[SWQC gzip] backend=%s mode=%c workers=%u input_bytes=%llu output_bytes=%llu jobs=%llu io_s=%.6f kernel_s=%.6f total_s=%.6f\n",
#ifdef SWQC_CODEC_SUNWAY
            "cpe",
#else
            "host_serial_reference",
#endif
            *argv[1], workers, (unsigned long long)metrics.input_bytes,
            (unsigned long long)metrics.output_bytes, (unsigned long long)metrics.jobs,
            metrics.io_s, metrics.kernel_s, seconds() - start);
        return 0;
    } catch (const std::exception& e) {
        fprintf(stderr, "[SWQC gzip error] %s\n", e.what());
        return 1;
    }
}
