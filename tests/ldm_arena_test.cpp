#include "swqc_config.h"
#include "swqc_ldm_arena.h"
#include <cassert>
#include <cstdio>
#include <limits>
#include <random>
#include <vector>

int main() {
    alignas(64) unsigned char payload[SWQC_LDM_POOL_BYTES];
    SwqcLdmArena arena;
    arena.init(payload);
    assert(!arena.allocate(std::numeric_limits<size_t>::max()));
    void *full = arena.allocate(SWQC_LDM_POOL_BYTES);
    assert(full == payload && !arena.allocate(1));
    assert(!arena.release(payload + 1));
    assert(arena.release(full) && !arena.release(full));
    struct Block { void *p; size_t n; unsigned char byte; };
    std::vector<Block> blocks;
    std::mt19937 random(17);
    for (int i = 0; i < 20000; ++i) {
        if (!blocks.empty() && random() % 2) {
            size_t index = random() % blocks.size();
            Block b = blocks[index];
            for (size_t j = 0; j < b.n; ++j) assert(static_cast<unsigned char *>(b.p)[j] == b.byte);
            assert(arena.release(b.p));
            blocks.erase(blocks.begin() + index);
        } else {
            size_t n = random() % 2048 + 1;
            void *p = arena.allocate(n);
            if (p) {
                assert(reinterpret_cast<uintptr_t>(p) % 64 == 0 && arena.contains(p));
                unsigned char byte = random() % 255;
                memset(p, byte, n);
                blocks.push_back({p, n, byte});
            }
        }
    }
    for (auto b : blocks) assert(arena.release(b.p));
    assert(arena.live == 0 && arena.peak <= SWQC_LDM_POOL_BYTES);
    full = arena.allocate(SWQC_LDM_POOL_BYTES);
    assert(full && arena.release(full));
    puts("PASS: LDM arena alignment, exhaustion, holes, data preservation and complete reclamation");
}
