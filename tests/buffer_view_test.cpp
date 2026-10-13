#include "Buffer.h"
#include "DataPool.h"
#include <cassert>
#include <cstring>
#include <cstddef>

extern "C" size_t cpe_buffer_size();
extern "C" size_t cpe_chunk_size();
extern "C" size_t cpe_chunk_length_offset();
extern "C" size_t cpe_chunk_next_offset();

int main() {
    using rabbit::core::Buffer;
    using rabbit::core::DataChunk;
    assert(sizeof(Buffer) == cpe_buffer_size());
    assert(sizeof(DataChunk) == cpe_chunk_size());
    assert(offsetof(DataChunk, size) == cpe_chunk_length_offset());
    assert(offsetof(DataChunk, next) == cpe_chunk_next_offset());
    std::shared_ptr<unsigned char> payload(new unsigned char[128], std::default_delete<unsigned char[]>());
    std::weak_ptr<unsigned char> lifetime = payload;
    memcpy(payload.get(), "borrowed", 9);
    rabbit::core::TDataPool<DataChunk> pool(2, 0);
    DataChunk *a, *b;
    pool.Acquire(a); pool.Acquire(b);
    a->data.SetView(payload.get(), 9, payload);
    b->data.SetView(payload.get(), 9, payload);
    payload.reset();
    assert(!lifetime.expired());
    pool.Release(a);
    assert(!lifetime.expired() && strcmp((char *)b->data.Pointer(), "borrowed") == 0);
    b->data.Extend(16, true);
    assert(lifetime.expired() && strcmp((char *)b->data.Pointer(), "borrowed") == 0);
    pool.Release(b);
    pool.Acquire(a);
    payload.reset(new unsigned char[128], std::default_delete<unsigned char[]>());
    lifetime = payload;
    a->data.SetView(payload.get() + 1, 16, payload);
    payload.reset();
    { Buffer other(0); a->data.Swap(other); assert(!lifetime.expired()); }
    assert(lifetime.expired());
    pool.Release(a);
    puts("BUFFER_VIEW_TEST_PASS: leases, pool release, unaligned slices, extend, swap");
}
