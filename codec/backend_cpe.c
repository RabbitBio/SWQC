#include <slave.h>
#include "codec_job.h"
#if SWQC_CODEC_LDM == 1
#include "swqc_runtime.h"
#endif
void swqc_codec_worker(swqc_codec_batch *batch)
{
    unsigned i;
#if SWQC_CODEC_LDM == 1
    swqc_ldm_begin();
    swqc_ldm_transient(1);
#endif
    for (i = (unsigned)_MYID; i < batch->count; i += batch->workers) {
        if ((unsigned)_MYID >= batch->workers) break;
        swqc_codec_execute(&batch->jobs[i]);
    }
#if SWQC_CODEC_LDM == 1
    swqc_ldm_end();
#endif
}
