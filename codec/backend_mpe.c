#include <athread.h>
#include "codec_job.h"
#include <time.h>
extern void slave_swqc_codec_worker();
void swqc_codec_init(void) { athread_init(); }
void swqc_codec_finish(void) { athread_halt(); }
void swqc_codec_launch(swqc_codec_batch *batch)
{
    struct timespec begin, end;
    clock_gettime(CLOCK_MONOTONIC, &begin);
    athread_spawn(swqc_codec_worker, batch);
    athread_join();
    clock_gettime(CLOCK_MONOTONIC, &end);
    batch->run_s = end.tv_sec - begin.tv_sec + (end.tv_nsec - begin.tv_nsec) * 1e-9;
    batch->wait_s = 0;
}
