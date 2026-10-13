#include "../codec/crc_combine.h"
#include <cassert>
#include <cstdio>
#include <random>
#include <vector>
int main() {
    SwqcCrcCombine combine;
    std::mt19937 rng(29);
    for (unsigned i = 0; i < 2000; ++i) {
        uint32_t a = rng(), b = rng();
        uint64_t n = ((uint64_t(rng()) << 32) | rng()) & ((uint64_t(1) << 62) - 1);
        assert(combine.combine(a, b, n) == crc32_combine64(a, b, n));
    }
    std::vector<unsigned char> data(800003);
    for (auto &c : data) c = rng();
    uint32_t crc = 0;
    for (size_t pos = 0; pos < data.size();) {
        size_t n = std::min(size_t(1 + rng() % 30000), data.size() - pos);
        crc = combine.combine(crc, crc32(0, data.data() + pos, n), n);
        pos += n;
    }
    assert(crc == crc32(0, data.data(), data.size()));
    assert(combine.combine(crc, 0, 0) == crc);
    puts("CRC_COMBINE_PASS: zlib equivalence, 64-bit lengths, fragmented input");
}
