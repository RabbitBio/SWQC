#include "swqc_host_io.h"
#include <cassert>
#include <cstring>
#include <thread>
#include <vector>
#include <unistd.h>
#include <cstdio>
#include <cstdlib>

static void copies(int caller)
{
    const size_t guard = 64;
    for (size_t size : {size_t(0), size_t(7), size_t(524287), size_t(524288),
                       size_t(1048589), size_t(4194304)}) {
        std::vector<char> source(size + 2 * guard), target(source.size(), 'Z');
        for (size_t i = 0; i < source.size(); ++i) source[i] = char((i * 17 + caller) % 251);
        for (int n = 0; n < 4; ++n) {
            swqc_host_memcpy(target.data() + guard, source.data() + guard, size);
            assert(!memcmp(target.data() + guard, source.data() + guard, size));
            for (size_t i = 0; i < guard; ++i) {
                assert(target[i] == 'Z');
                assert(target[guard + size + i] == 'Z');
            }
        }
    }
}

int main()
{
    for (bool helpers : {false, true}) {
        if (helpers) assert(swqc_host_memory_init(6) == 0);
        assert(swqc_host_threads() == (helpers ? 6 : 1));
        for (size_t count : {size_t(0), size_t(1), size_t(5), size_t(6), size_t(7), size_t(384)}) {
            std::vector<int> values(count, 0);
            swqc_host_for(count, [](void *p, int worker, size_t begin, size_t end) {
                auto &v = *static_cast<std::vector<int> *>(p);
                assert(worker >= 0 && worker < 6 && begin < end && end <= v.size());
                for (size_t i = begin; i < end; ++i) ++v[i];
            }, &values);
            for (int v : values) assert(v == 1);
        }
        std::thread a(copies, 1), b(copies, 2), c(copies, 3);
        a.join(); b.join(); c.join();
        const size_t length = 1UL << 20;
        for (size_t delta : {size_t(1), length / 2, length, length + 1}) {
            for (bool right : {false, true}) {
                std::vector<char> actual(4 * length), expected;
                for (size_t i = 0; i < actual.size(); ++i) actual[i] = char(i * 7 % 251);
                expected = actual;
                size_t src = right ? 64 : 64 + delta, dst = right ? 64 + delta : 64;
                memmove(expected.data() + dst, expected.data() + src, length);
                swqc_host_memmove(actual.data() + dst, actual.data() + src, length);
                assert(actual == expected);
            }
        }
        char path[] = "/tmp/swqc-memory-read-XXXXXX";
        int fd = mkstemp(path);
        assert(fd >= 0);
        std::vector<char> bytes(length, 'R'), output(length, 'Z');
        assert(write(fd, bytes.data(), bytes.size()) == (ssize_t)bytes.size());
        unsigned long calls = 0;
        assert(swqc_host_pread(fd, output.data(), output.size(), 0, &calls) == 0);
        assert(output == bytes && calls == 1);
        close(fd); unlink(path);
        if (helpers) swqc_host_io_destroy();
    }
    assert(swqc_host_io_init(6) == 0);
    copies(4);
    swqc_host_io_destroy();
    puts("PASS: serial/parallel copies, concurrent callers, guard bytes, overlapping moves, copy-only reads and worker reuse");
}
