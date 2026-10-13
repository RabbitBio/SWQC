#ifndef SWQC_FINGERPRINT_H
#define SWQC_FINGERPRINT_H
#include <cstddef>
#include <cstdint>
#include <limits>

/* Identical host/CPE position-sensitive diagnostic, not a full checksum. */
class SwqcOutputFingerprint {
    static uint64_t mix(uint64_t x) {
        x = (x ^ (x >> 30)) * uint64_t(0xbf58476d1ce4e5b9ULL);
        x = (x ^ (x >> 27)) * uint64_t(0x94d049bb133111ebULL);
        return x ^ (x >> 31);
    }
public:
    enum { STRIDE = 4096 };
    uint64_t bytes = 0, samples = 0, sum64 = 0, xor64 = 0, end_offset = 0;

    bool append(const char *data, size_t size, int64_t offset) {
        if (offset < 0 || size > uint64_t(std::numeric_limits<int64_t>::max()) - uint64_t(offset) ||
            size > std::numeric_limits<uint64_t>::max() - bytes) return false;
        if (!size) return true;
        uint64_t begin = uint64_t(offset), end = begin + size;
        bytes += size;
        if (end > end_offset) end_offset = end;
        size_t skip = (STRIDE - begin % STRIDE) % STRIDE;
        for (size_t i = skip; i < size; i += STRIDE) {
            uint64_t h = mix(mix(begin + i) ^ static_cast<unsigned char>(data[i]));
            sum64 += h;
            xor64 ^= h;
            ++samples;
        }
        return true;
    }

    bool combine(const SwqcOutputFingerprint &part) {
        const uint64_t limit = std::numeric_limits<uint64_t>::max();
        if (part.bytes > limit - bytes || part.samples > limit - samples) return false;
        bytes += part.bytes;
        samples += part.samples;
        sum64 += part.sum64;
        xor64 ^= part.xor64;
        if (part.end_offset > end_offset) end_offset = part.end_offset;
        return true;
    }
};

struct SwqcOutputSpan {
    const char *data;
    size_t bytes;
    int64_t offset;
};

struct SwqcFingerprintTask {
    const SwqcOutputSpan *mates[2];
    SwqcOutputFingerprint (*results)[2];
    int *errors;
};
#endif
