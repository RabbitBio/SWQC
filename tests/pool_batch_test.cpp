#include "DataPool.h"
#include <thread>
#include <vector>
#include <cassert>
#include <cstdio>

struct Item {
    explicit Item(unsigned int) : generation(0) {}
    unsigned int generation;
    void Reset() { ++generation; }
};

int main() {
    for (int size : {1, 16, 768}) {
        rabbit::core::TDataPool<Item> pool(size, 0);
        std::vector<Item *> held(size);
        std::vector<const Item *> release(size);
        for (int i=0; i<size; ++i) { pool.Acquire(held[i]); release[i]=held[i]; }
        std::thread reader([&] {
            for (int round=0; round<100; ++round) {
                for (int i=0; i<size; ++i) pool.Acquire(held[i]);
                for (int i=0; i<size; ++i) release[i]=held[i];
                pool.ReleaseBatch(release.data(), release.size());
            }
        });
        // Publish the initial full pool without racing the reader's vectors.
        std::vector<const Item *> initial(release);
        pool.ReleaseBatch(initial.data(), initial.size());
        reader.join();
        assert(pool.partNum == 0);
        pool.ReleaseBatch(nullptr, 0);
    }
    puts("batch pool release/wakeup/reuse PASS");
}
