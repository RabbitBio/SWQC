#include "codec_job.h"
#include <time.h>
void swqc_codec_init(void) {}
void swqc_codec_finish(void) {}
void swqc_codec_launch(swqc_codec_batch *batch)
{
    unsigned i;
    struct timespec begin, end;
    clock_gettime(CLOCK_MONOTONIC, &begin);
    for (i = 0; i < batch->count; ++i) swqc_codec_execute(&batch->jobs[i]);
    clock_gettime(CLOCK_MONOTONIC, &end);
    batch->run_s = end.tv_sec - begin.tv_sec + (end.tv_nsec - begin.tv_nsec) * 1e-9;
    batch->wait_s = 0;
}
