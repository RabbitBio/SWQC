#ifndef SWQC_LDM_ARENA_H
#define SWQC_LDM_ARENA_H
#include <cstddef>
#include <cstdint>
#include <cstring>

/* A portable core shared by the CPE allocator and native lifetime tests.
 * Metadata and payload use the same LDM backing; no per-request heap metadata. */
struct SwqcLdmArena {
    enum { UNIT = 64, SLOTS = SWQC_LDM_POOL_BYTES / UNIT, WORDS = (SLOTS + 63) / 64 };
    unsigned char *base;
    uint64_t occupied[WORDS];
    uint16_t lengths[SLOTS];
    size_t live, peak;
    void init(void *payload) {
        base = static_cast<unsigned char *>(payload);
        std::memset(occupied, 0, sizeof(occupied));
        std::memset(lengths, 0, sizeof(lengths));
        live = peak = 0;
    }
    void *allocate(size_t bytes) {
        if (!bytes) bytes = 1;
        if (bytes > SWQC_LDM_POOL_BYTES) return NULL;
        const unsigned count = (bytes + UNIT - 1) / UNIT;
        unsigned run = 0;
        for (unsigned i = 0; i < SLOTS; ++i) {
            if (occupied[i / 64] & (uint64_t(1) << (i % 64))) { run = 0; continue; }
            if (++run < count) continue;
            unsigned start = i + 1 - count;
            for (unsigned j = start; j <= i; ++j)
                occupied[j / 64] |= uint64_t(1) << (j % 64);
            lengths[start] = count;
            live += count * UNIT;
            if (live > peak) peak = live;
            return base + start * UNIT;
        }
        return NULL;
    }
    bool contains(const void *p) const {
        uintptr_t address = reinterpret_cast<uintptr_t>(p);
        uintptr_t begin = reinterpret_cast<uintptr_t>(base);
        return address >= begin && address - begin < SWQC_LDM_POOL_BYTES;
    }
    bool release(void *p) {
        uintptr_t offset = reinterpret_cast<uintptr_t>(p) - reinterpret_cast<uintptr_t>(base);
        if (offset >= SWQC_LDM_POOL_BYTES || offset % UNIT) return false;
        unsigned start = offset / UNIT, count = lengths[start];
        if (!count || start + count > SLOTS) return false;
        for (unsigned j = start; j < start + count; ++j) {
            if (!(occupied[j / 64] & (uint64_t(1) << (j % 64)))) return false;
        }
        for (unsigned j = start; j < start + count; ++j)
            occupied[j / 64] &= ~(uint64_t(1) << (j % 64));
        lengths[start] = 0;
        live -= count * UNIT;
        return true;
    }
};
#endif
