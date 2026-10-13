#ifndef SWQC_RING_CURSOR_H
#define SWQC_RING_CURSOR_H
#include <atomic>
#include <cassert>

/* The queue's count publishes data; each cursor has one active owner. */
inline int swqc_next_ring_slot(std::atomic_int &cursor, int capacity) {
    int slot = cursor.load(std::memory_order_relaxed);
    assert(capacity > 0 && slot >= 0 && slot < capacity);
    cursor.store(slot + 1 == capacity ? 0 : slot + 1, std::memory_order_relaxed);
    return slot;
}
#endif
