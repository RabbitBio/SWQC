#ifndef SWQC_CRC_COMBINE_H
#define SWQC_CRC_COMBINE_H
#include <cstdint>
#include <zlib.h>

// Cache powers of zlib's one-byte CRC shift. Older system zlib reconstructs
// these matrices for every combine; a streaming reader combines thousands.
class SwqcCrcCombine {
public:
    SwqcCrcCombine() {
        for (unsigned i = 0; i < 32; ++i) powers_[0][i] = crc32_combine(uint32_t(1) << i, 0, 1);
        for (unsigned n = 1; n < 64; ++n)
            for (unsigned i = 0; i < 32; ++i)
                powers_[n][i] = apply(powers_[n - 1], powers_[n - 1][i]);
    }
    uint32_t combine(uint32_t first, uint32_t second, uint64_t bytes) const {
        if (!bytes) return first;
        for (unsigned n = 0; bytes; ++n, bytes >>= 1)
            if (bytes & 1) first = apply(powers_[n], first);
        return first ^ second;
    }
private:
    uint32_t powers_[64][32];
    static uint32_t apply(const uint32_t *matrix, uint32_t bits) {
        uint32_t value = 0;
        for (unsigned i = 0; bits; ++i, bits >>= 1)
            if (bits & 1) value ^= matrix[i];
        return value;
    }
};
#endif
