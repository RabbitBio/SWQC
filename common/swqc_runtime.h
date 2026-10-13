#ifndef SWQC_RUNTIME_H
#define SWQC_RUNTIME_H
#include "swqc_config.h"
#include <stddef.h>

/* Layout is shared by the MPE and CPE compilers. Addresses are 64 bit. */
typedef struct {
    volatile unsigned long sequence;
    unsigned long entry;
    void *argument;
    unsigned long relocated_gp;
    unsigned long segment_offset;
    void *private_copies;
    unsigned long *tls_relocations;
    unsigned long tls_count;
    char *pool;
    size_t pool_bytes;
    volatile int completed[SWQC_CPE_COUNT];
    unsigned long pool_calls[SWQC_CPE_COUNT];
    unsigned long pool_fallbacks[SWQC_CPE_COUNT];
    /* Per-task RTC deltas: format, QC, serialize, complete PE entry. */
    volatile unsigned long kernel_ticks[SWQC_CPE_COUNT][4];
    volatile unsigned long duplicate_ticks[SWQC_CPE_COUNT];
    volatile unsigned long duplicate_pairs[SWQC_CPE_COUNT];
    unsigned long ldm_stats[SWQC_CPE_COUNT][7];
    volatile unsigned long error[SWQC_CPE_COUNT][3];
} swqc_command;

#ifdef __cplusplus
extern "C" {
#endif
void swqc_runtime_init(void);
void swqc_runtime_run(void *entry, void *argument);
void swqc_runtime_run_profile(void *entry, void *argument, double *run_s, double *wait_s);
void swqc_runtime_finish(void);
void swqc_record_kernel_ticks(unsigned long format, unsigned long qc,
                              unsigned long serialize, unsigned long total);
void swqc_record_duplicate_ticks(unsigned long ticks, unsigned long pairs);
void swqc_ldm_begin(void);
void swqc_ldm_end(void);
void swqc_ldm_transient(int enabled);
void *swqc_scratch_alloc(size_t bytes);
void swqc_scratch_free(void *ptr, size_t bytes);
/* Report from CPE memory, without calling relocated stdio on a failure path. */
void swqc_cpe_error(unsigned long code, unsigned long first, unsigned long second)
    __attribute__((noreturn));
#ifdef __cplusplus
}
#endif
#endif
