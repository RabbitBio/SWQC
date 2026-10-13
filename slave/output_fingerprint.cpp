#include <slave.h>
#include "swqc_fingerprint.h"

extern "C" void swqc_output_fingerprint(SwqcFingerprintTask *task)
{
    const int id = (int)_MYID;
    int error = 0;
    for (int m = 0; m < 2; ++m) {
        const SwqcOutputSpan &span = task->mates[m][id];
        SwqcOutputFingerprint result;
        if (!result.append(span.data, span.bytes, span.offset)) error = 1;
        task->results[id][m] = result;
    }
    task->errors[id] = error;
}
