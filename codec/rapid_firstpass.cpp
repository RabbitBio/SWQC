#include "codec_job.h"
#include "rapid_state.h"
#include <algorithm>
#include <cstring>
#include <limits>
#include <memory>
#include <filereader/CpeMemory.hpp>
#include <rapidgzip/gzip/deflate.hpp>
#include <rapidgzip/blockfinder/DynamicHuffman.hpp>
#include <rapidgzip/blockfinder/Uncompressed.hpp>
#include <zlib.h>
#ifdef deflate
#undef deflate
#endif

extern "C" void swqc_rapid_discover(swqc_codec_job *job)
{
    using namespace rapidgzip;
    job->status = Z_DATA_ERROR;
    job->output_size = 0;
    job->final_block = 0;
    job->output_full = 0;
    try {
        gzip::BitReader bits(std::make_unique<CpeMemoryFileReader>(
            reinterpret_cast<const char *>(job->input), job->input_size));
        SwqcRapidState<deflate::Block<false>> block;
        job->state_bytes = sizeof(deflate::Block<false>);
        job->state_in_ldm = block.in_ldm();
        const size_t end = job->logical_size * 8;
        const size_t none = std::numeric_limits<size_t>::max();
        size_t search = job->start_bit;
        while (search < end) {
            size_t start = search, start_max = search;
            if (!job->known_start) {
                const size_t limit = std::min(job->stop_bit, end);
                bits.seekTo(search);
                const auto dynamic = blockfinder::seekToNonFinalDynamicDeflateBlock(bits, limit);
                bits.seekTo(search);
                const auto stored = blockfinder::seekToNonFinalUncompressedDeflateBlock(bits, limit);
                start = std::min(dynamic, stored.first);
                if (start == none || start >= limit) return;
                if (start == stored.first) start_max = stored.second;
                else start_max = start;
            }
            bits.seekTo(start);
            if (job->known_start)
                block->reset(VectorView<uint8_t>(job->dictionary, job->dictionary_size));
            else
                block->reset();
            size_t produced = 0;
            bool valid = true;
            for (;;) {
                if (bits.tell() >= end || block->readHeader(bits) != Error::NONE) {
                    valid = false;
                    break;
                }
                while (!block->eob()) {
                    // Input has a 128 KiB guard. It prevents EOF exceptions in
                    // speculative decoding; logical bounds remain authoritative.
                    if (bits.tell() >= end) { valid = false; break; }
                    if (produced + 65536 > job->output_capacity) {
                        job->output_full = 1;
                        job->status = Z_BUF_ERROR;
                        return;
                    }
                    const auto decoded = block->read(bits, 32768);
                    if (decoded.second != Error::NONE || bits.tell() > end) {
                        valid = false;
                        break;
                    }
                    for (const auto &v : decoded.first.dataWithMarkers) {
                        if (!v.empty()) std::memcpy(job->symbols + produced, v.data(), v.size() * 2);
                        produced += v.size();
                    }
                    for (const auto &v : decoded.first.data) {
                        for (size_t i = 0; i < v.size(); ++i) job->symbols[produced + i] = v[i];
                        produced += v.size();
                    }
                    if (!decoded.first.size() && !block->eob()) { valid = false; break; }
                }
                if (!valid) break;
                if (block->eos()) break;
                if (bits.tell() >= job->stop_bit) {
                    // Match the block finder's non-final dynamic/stored candidates.
                    const auto next = bits.peek<3>();
                    if (next == 0 || next == 4) break;
                }
            }
            if (valid && bits.tell() <= end) {
                job->start_bit = start;
                job->start_max_bit = start_max;
                job->end_bit = bits.tell();
                job->output_size = produced;
                job->tail_known = job->tail && produced >= 32768;
                if (job->tail_known) {
                    for (size_t i = 0; i < 32768; ++i) {
                        const auto value = job->symbols[produced - 32768 + i];
                        if (value >= 256) { job->tail_known = 0; break; }
                        job->tail[i] = static_cast<unsigned char>(value);
                    }
                }
                job->final_block = block->eos();
                job->status = Z_OK;
                return;
            }
            if (job->known_start) { job->status = Z_BUF_ERROR; return; }
            search = start_max + 1;
        }
    } catch (const std::bad_alloc&) {
        job->status = Z_MEM_ERROR;
    } catch (...) {
        job->status = Z_DATA_ERROR;
    }
}
