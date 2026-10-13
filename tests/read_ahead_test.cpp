#include "../codec/byte_source.h"
#include <cassert>
#include <random>
#include <unistd.h>

int main() {
    char path[] = "/tmp/swqc-read-ahead-XXXXXX";
    int fd = mkstemp(path);
    assert(fd >= 0);
    std::vector<unsigned char> expected((4U << 20) + 79), actual(300001);
    std::mt19937 random(51);
    for (auto &c : expected) c = random();
    size_t written = 0;
    while (written < expected.size()) {
        ssize_t n = write(fd, expected.data() + written, expected.size() - written);
        assert(n > 0); written += n;
    }
    close(fd);
    for (size_t window : {4096U, 65536U, 1U << 20}) {
        SwqcByteSource source(path);
        source.enable_prefetch(window);
        for (size_t pos = 0; pos < expected.size();) {
            size_t n = source.read(actual.data(), actual.size());
            assert(n && !memcmp(actual.data(), expected.data() + pos, n));
            pos += n;
        }
        assert(source.read(actual.data(), 1) == 0);
        for (unsigned i = 0; i < 80; ++i) {
            size_t pos = random() % expected.size();
            source.seek(pos);
            assert(source.byte() == expected[pos++]);
            size_t n = source.read(actual.data(), random() % actual.size());
            assert(!memcmp(actual.data(), expected.data() + pos, n));
        }
        bool rejected = false;
        try { source.seek(expected.size() + 1); }
        catch (const std::runtime_error &) { rejected = true; }
        assert(rejected);
        source.finish_prefetch();
        auto stats = source.prefetch_stats();
        assert(stats.calls && stats.bytes >= expected.size());
    }
    // Reader destruction must join a pending syscall before closing its fd.
    for (int i = 0; i < 16; ++i) {
        SwqcByteSource source(path);
        source.enable_prefetch(65536);
        assert(source.byte() == expected[0]);
    }
    {
        SwqcByteSource source(path);
        source.enable_prefetch(65536);
        assert(truncate(path, 0) == 0);
        bool rejected = false;
        try { source.byte(); }
        catch (const std::runtime_error &) { rejected = true; }
        assert(rejected);
    }
    unlink(path);
    puts("READ_AHEAD_PASS: sequential, cross-window, seek, EOF, destruction, I/O errors");
}
