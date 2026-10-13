#ifndef SWQC_OUTPUT_FINGERPRINT_H
#define SWQC_OUTPUT_FINGERPRINT_H
#include "swqc_fingerprint.h"
#include "swqc_host_io.h"

/* Dispatch once per CPE wave, not once per MiB. Payloads remain owned by the
 * consumer until this synchronous call returns and publishes the writer queue. */
inline bool swqc_fingerprint_batch(const SwqcOutputSpan *spans, size_t count,
                                   SwqcOutputFingerprint &result) {
    struct Work {
        const SwqcOutputSpan *spans;
        SwqcOutputFingerprint parts[6];
        bool ok[6] = {true, true, true, true, true, true};
    } work;
    work.spans = spans;
    swqc_host_for(count, [](void *opaque, int worker, size_t begin, size_t end) {
        auto &w = *static_cast<Work *>(opaque);
        for (size_t i = begin; i < end; ++i) {
            const auto &span = w.spans[i];
            if (!w.parts[worker].append(span.data, span.bytes, span.offset)) w.ok[worker] = false;
        }
    }, &work);
    for (int i = 0; i < swqc_host_threads(); ++i)
        if (!work.ok[i] || !result.combine(work.parts[i])) return false;
    return true;
}
#endif
