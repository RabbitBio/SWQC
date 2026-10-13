#include "../slave/Buffer.h"
#include <cstddef>

extern "C" size_t cpe_buffer_size() { return sizeof(rabbit::core::Buffer); }
extern "C" size_t cpe_chunk_size() { return sizeof(rabbit::core::DataChunk); }
extern "C" size_t cpe_chunk_length_offset() { return offsetof(rabbit::core::DataChunk, size); }
extern "C" size_t cpe_chunk_next_offset() { return offsetof(rabbit::core::DataChunk, next); }
