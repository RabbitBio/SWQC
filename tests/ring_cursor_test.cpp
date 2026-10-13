#include "ring_cursor.h"
#include <thread>
#include <vector>
#include <cstdio>

int main() {
    for (int capacity : {1, 8, 63, 512, 3072}) {
        std::vector<unsigned long> slots(capacity);
        std::atomic_int read(0), write(0), count(0);
        const unsigned long items = (1UL << 20) + 100;
        std::thread producer([&] {
            for (unsigned long i = 0; i < items; ++i) {
                while (count.load() == capacity) std::this_thread::yield();
                slots[swqc_next_ring_slot(write, capacity)] = i;
                ++count;
            }
        });
        for (unsigned long i = 0; i < items; ++i) {
            while (!count.load()) std::this_thread::yield();
            assert(slots[swqc_next_ring_slot(read, capacity)] == i);
            --count;
        }
        producer.join();
        assert(count == 0 && read == items % capacity && write == items % capacity);
    }
    puts("bounded queue wrap/order PASS");
}
