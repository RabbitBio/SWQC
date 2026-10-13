#include "codec_job.h"
#include <limits.h>
#include <string.h>
#include <zlib.h>
#ifdef SWQC_CODEC_RAPID
extern void swqc_rapid_discover(swqc_codec_job *job);
#endif

void swqc_codec_execute(swqc_codec_job *job)
{
    z_stream stream;
    int result;
    memset(&stream, 0, sizeof(stream));
    job->status = Z_STREAM_ERROR;
    if (job->operation == SWQC_CODEC_RESOLVE) {
        size_t i;
        if (!job->symbols || !job->output || job->dictionary_size > 32768 ||
            job->output_size > job->output_capacity || job->output_size > UINT_MAX) return;
        for (i = 0; i < job->output_size;) {
            size_t page = i / 4096;
            size_t end = job->output_size - i < 4096 ? job->output_size : i + 4096;
            uint32_t lines = 0;
            for (; i < end; ++i) {
                unsigned symbol = job->symbols[i];
                if (symbol >= 256) {
                    if (symbol < 65536U - job->dictionary_size || !job->dictionary) {
                        job->status = Z_DATA_ERROR;
                        return;
                    }
                    symbol = job->dictionary[symbol - (65536U - job->dictionary_size)];
                }
                job->output[i] = (unsigned char)symbol;
                lines += symbol == '\n';
            }
            if (job->line_counts) job->line_counts[page] = lines;
        }
        job->crc = (uint32_t)crc32(0, job->output, (uInt)job->output_size);
        job->status = Z_OK;
        return;
    }
#ifdef SWQC_CODEC_RAPID
    if (job->operation == SWQC_CODEC_DISCOVER) {
        swqc_rapid_discover(job);
        return;
    }
#endif
    job->output_size = 0;
    if (job->operation != SWQC_CODEC_DEFLATE || !job->input || !job->output || job->input_size > UINT_MAX ||
        job->output_capacity > UINT_MAX || job->dictionary_size > 32768 ||
        (job->dictionary_size && !job->dictionary))
        return;
    stream.next_in = (Bytef *)job->input;
    stream.avail_in = (uInt)job->input_size;
    stream.next_out = job->output;
    stream.avail_out = (uInt)job->output_capacity;
    result = deflateInit2(&stream, job->level, Z_DEFLATED, -15, 8, Z_DEFAULT_STRATEGY);
    if (result != Z_OK) { job->status = result; return; }
    if (job->dictionary_size)
        result = deflateSetDictionary(&stream, job->dictionary, job->dictionary_size);
    if (result == Z_OK) result = deflate(&stream, Z_SYNC_FLUSH);
    job->output_size = job->output_capacity - stream.avail_out;
    job->status = result == Z_OK && !stream.avail_in && stream.avail_out ? Z_OK : Z_BUF_ERROR;
    if (job->status == Z_OK)
        job->crc = (uint32_t)crc32(0, job->input, (uInt)job->input_size);
    /* An intentionally unfinished raw stream ends with Z_SYNC_FLUSH.
     * The host appends one final block after concatenating all jobs. */
    deflateEnd(&stream);
}
