#ifndef SWQC_CODEC_JOB_H
#define SWQC_CODEC_JOB_H
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif
enum { SWQC_CODEC_DEFLATE = 1, SWQC_CODEC_DISCOVER = 2, SWQC_CODEC_RESOLVE = 3 };
typedef struct {
    const unsigned char *input;
    unsigned char *output;
    const unsigned char *dictionary;
    size_t input_size, output_capacity, output_size;
    uint32_t dictionary_size, crc;
    int operation, level, status;
    /* First-pass rapidgzip: bit offsets are relative to input. */
    uint16_t *symbols;
    size_t logical_size, start_bit, start_max_bit, stop_bit, end_bit;
    int known_start, final_block;
    unsigned char *tail;
    int tail_known;
    /* Distinguish output exhaustion from input bounds/invalid candidates. */
    int output_full;
    /* Optional newline counts for each 4096-byte decoded page. */
    uint32_t *line_counts;
    size_t state_bytes;
    int state_in_ldm;
} swqc_codec_job;
typedef struct {
    swqc_codec_job *jobs;
    unsigned count, workers;
    double run_s, wait_s;
} swqc_codec_batch;
void swqc_codec_execute(swqc_codec_job *job);
void swqc_codec_launch(swqc_codec_batch *batch);
void swqc_codec_init(void);
void swqc_codec_finish(void);
#ifdef __cplusplus
}
#endif
#endif
