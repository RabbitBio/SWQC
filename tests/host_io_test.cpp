#include "swqc_host_io.h"
#include <cassert>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

int main() {
    char path[] = "/tmp/swqc-host-io-XXXXXX";
    int fd = mkstemp(path);
    assert(fd >= 0);
    std::string expected(7 * 1024 * 1024 + 113, 'x');
    for (size_t i = 0; i < expected.size(); ++i) expected[i] = char(i * 37 % 251);
    assert(write(fd, expected.data(), expected.size()) == (ssize_t)expected.size());
    for (int threads : {1, 6, 6}) {
        assert(swqc_host_io_init(threads) == 0);
        auto check = [&]() {
            for (size_t n : {size_t(0), size_t(71), size_t(1024 * 1024), expected.size() - 13}) {
                std::vector<char> actual(n + 32, '!');
                unsigned long calls;
                assert(swqc_host_pread(fd, actual.data() + 16, n, 13, &calls) == 0);
                assert(std::string(actual.data() + 16, n) == expected.substr(13, n));
                assert(std::string(actual.data(), 16) == std::string(16, '!'));
                assert(std::string(actual.data() + n + 16, 16) == std::string(16, '!'));
            }
        };
        std::thread other(check);
        check();
        other.join();
        char byte;
        unsigned long calls;
        assert(swqc_host_pread(fd, &byte, 1, expected.size(), &calls) == EIO);
        assert(swqc_host_pread(-1, &byte, 1, 0, &calls) == EBADF);
        swqc_host_io_destroy();
    }
    close(fd);
    unlink(path);
    puts("PASS: serial/parallel reads, offsets, guards, concurrent callers, EOF, teardown/reinit");
}
