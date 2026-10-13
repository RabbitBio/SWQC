#include "swqc_runtime.h"
#include "swqc_log.h"
#include "swqc_layout.h"
#include <athread.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <sched.h>
#include <mpi.h>
#include <pthread.h>

extern void slave_swqc_pass(void *);
extern void slave_swqc_copy_private(void *);
extern void slave_swqc_relocate_private(void *);
extern void slave_swqc_dispatch(void);

static swqc_command *command;
__uncached static swqc_command shared_command;
static void *segments;
static int started;
static int launch_cgmask;
static unsigned long tasks;
static int rank_id;
static double launch_wall;
static unsigned long max_ticks_sum[4];
static unsigned long dup_ticks_sum, qc_ticks_sum, dup_pairs_sum;
static unsigned long qc_rounds;
static pthread_mutex_t launch_mutex = PTHREAD_MUTEX_INITIALIZER;

static double monotonic_time(void)
{
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    return now.tv_sec + now.tv_nsec * 1e-9;
}
/* Read from storage: constant-folding a placeholder layout changes pass-1 text. */
static const volatile unsigned long text_start = SWQC_TEXT_START;
static const volatile unsigned long text_size = SWQC_TEXT_SIZE;
static const volatile unsigned long data_start = SWQC_DATA_START;
static const volatile unsigned long data_size = SWQC_DATA_SIZE;

static void fail(const char *message)
{
    fprintf(stderr, "[SWQC runtime] %s\n", message);
    MPI_Abort(MPI_COMM_WORLD, 1);
    abort();
}

static void *aligned_buffer(size_t bytes)
{
    void *raw = malloc(bytes + 4095);
    if (!raw) fail("allocation failed");
    return (void *)(((uintptr_t)raw + 4095) & ~(uintptr_t)4095);
}

static void native_run(void *entry, void *argument)
{
    int spawn_status, join_status;
#if SWQC_EXEC_MODE == SWQC_SINGLE
    /* A pthread can run on another MPE; target the MPI rank's original CG. */
    spawn_status = __real_athread_spawn_cgmask(launch_cgmask, entry, argument, 1);
    join_status = athread_join_cgmask(launch_cgmask);
#else
    spawn_status = __real_athread_spawn_cgs(entry, argument, 1);
    join_status = athread_join_cgs();
#endif
    if (spawn_status || join_status) {
        fprintf(stderr, "[SWQC runtime] spawn=%d join=%d pid=%ld\n",
                spawn_status, join_status, (long)getpid());
        fail("native CPE launch failed");
    }
}

static void *read_relocations(const char *path, size_t width, unsigned long *count)
{
    FILE *fp = fopen(path, "rb");
    void *data;
    if (!fp || fread(count, sizeof(*count), 1, fp) != 1 || *count > SIZE_MAX / width)
        fail("invalid/missing relocation file; run the complete build.sh workflow");
    data = malloc(*count * width + 1);
    if (!data || fread(data, width, *count, fp) != *count) fail("truncated relocation file");
    fclose(fp);
    return data;
}

void swqc_runtime_init(void)
{
    if (command) return;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank_id);
    launch_cgmask = 1 << current_array_id();
#if SWQC_EXEC_MODE == SWQC_SINGLE
    athread_init();
#else
    athread_init_cgs();
#endif
    command = &shared_command;
    memset(command, 0, sizeof(*command));
#if SWQC_POOL
    command->pool_bytes = 8UL << 20;
    command->pool = aligned_buffer(command->pool_bytes * SWQC_CPE_COUNT);
#endif
    native_run((void *)slave_swqc_pass, command);
    fprintf(stderr, "[SWQC rank %d] pid=%ld cg=%d max_threads=%d\n",
            rank_id, (long)getpid(), current_array_id(), athread_get_max_threads());
    for (int i = 0; i < SWQC_CPE_COUNT; ++i)
        if (command->completed[i] != 1) fail("CPE initialization did not execute");
#if SWQC_EXEC_MODE == SWQC_CROSS
    unsigned long gp, count;
    short *got;
    if (text_start == 123 || data_start < text_start)
        fail("cross mode requires a two-pass build");
    for (long cg = 0; cg < 6; ++cg)
        for (long pe = 0; pe < 64; ++pe) {
            volatile unsigned long *permission = (void *)(0x800040004000UL + (cg << 40) +
                ((pe & 6) << 24) + ((pe & 0x30) << 7) + 0x680);
            *permission |= 7UL << 32;
        }
    segments = aligned_buffer(data_start + data_size - text_start);
    command->private_copies = aligned_buffer((2UL << 20) * SWQC_CPE_COUNT);
    asm volatile("mov $29, %0" : "=r"(gp));
    command->segment_offset = (unsigned long)segments - text_start;
    command->relocated_gp = gp + command->segment_offset;
    fprintf(stderr, "[SWQC cross layout] original_text=%#lx relocated_text=%p text_bytes=%lu\n",
            text_start, segments, text_size);
    command->tls_relocations = read_relocations("data.bin2", sizeof(unsigned long), &command->tls_count);
    got = read_relocations("data.bin", sizeof(short), &count);
    memcpy(segments, (void *)text_start, text_size);
    memcpy((char *)segments + data_start - text_start,
           (void *)data_start, data_size);
    for (unsigned long i = 0; i < count; ++i) {
        unsigned long *old = (void *)(gp + got[i]);
        unsigned long *copy = (void *)(gp + command->segment_offset + got[i]);
        if (*old != *copy) fail("GOT image mismatch");
        if (*old < 0x500000000000UL) *copy += command->segment_offset;
    }
    free(got);
    asm volatile("memb" ::: "memory");
    native_run((void *)slave_swqc_copy_private, command);
    native_run((void *)slave_swqc_relocate_private, command);
#endif
    fprintf(stderr, "[SWQC rank %d] mode=%d cpes=%d pool=%d pool_bytes_per_cpe=%zu\n",
            rank_id, SWQC_EXEC_MODE, SWQC_CPE_COUNT, SWQC_POOL, command->pool_bytes);
    fprintf(stderr, "[SWQC memory] manual_ldm=%d ldm_pool=%d ldm_pool_bytes=%d\n",
            SWQC_MANUAL_LDM, SWQC_LDM_POOL, SWQC_LDM_POOL_BYTES);
}

void swqc_runtime_run(void *entry, void *argument)
{
    swqc_runtime_run_profile(entry, argument, NULL, NULL);
}

void swqc_runtime_run_profile(void *entry, void *argument, double *run_s, double *wait_s)
{
    /* Reader, processor and writer share one CPE array/mailbox. */
    double wait_start = monotonic_time();
    pthread_mutex_lock(&launch_mutex);
    if (wait_s) *wait_s = monotonic_time() - wait_start;
    if (!command) swqc_runtime_init();
    ++tasks;
    for (int i = 0; i < SWQC_CPE_COUNT; ++i) {
        command->duplicate_ticks[i] = 0;
        command->duplicate_pairs[i] = 0;
    }
    for (int i = 0; i < SWQC_CPE_COUNT; ++i)
        for (int j = 0; j < 4; ++j) command->kernel_ticks[i][j] = 0;
    asm volatile("memb" ::: "memory");
    double t0 = monotonic_time();
#if SWQC_EXEC_MODE != SWQC_CROSS
    native_run(entry, argument);
#else
    for (int i = 0; i < SWQC_CPE_COUNT; ++i) command->completed[i] = 0;
    command->entry = (unsigned long)entry + command->segment_offset;
    command->argument = argument;
    asm volatile("memb" ::: "memory");
    ++command->sequence;
    asm volatile("memb" ::: "memory");
    if (!started) {
        for (long cg = 0; cg < 6; ++cg)
            for (long pe = 0; pe < 64; ++pe)
                *(volatile long *)(0x800000008100UL + (cg << 40) + (pe << 24)) = 3;
        for (long cg = 0; cg < 6; ++cg)
            for (long pe = 0; pe < 64; ++pe)
                *(volatile long *)(0x800000008000UL + (cg << 40) + (pe << 24)) =
                    (unsigned long)slave_swqc_dispatch + command->segment_offset;
        for (long cg = 0; cg < 6; ++cg)
            for (long pe = 0; pe < 64; ++pe)
                *(volatile long *)(0x800000008100UL + (cg << 40) + (pe << 24)) = 0;
        started = 1;
    }
    const double spin_until = monotonic_time() + 0.0005;
    double next_report = spin_until + 30;
    for (;;) {
        int done = 0;
        for (int i = 0; i < SWQC_CPE_COUNT; ++i) done += command->completed[i] != 0;
        if (done == SWQC_CPE_COUNT) break;
        double now = monotonic_time();
        if (now >= next_report) {
            fprintf(stderr, "[SWQC cross wait] task=%lu completed=%d/%d\n", tasks, done, SWQC_CPE_COUNT);
            next_report = now + 30;
        }
        for (int i = 0; i < SWQC_CPE_COUNT; ++i) {
            if (command->error[i][0]) {
                fprintf(stderr, "[SWQC CPE error] core=%d code=%lu first=%lu second=%lu\n",
                        i, command->error[i][0], command->error[i][1], command->error[i][2]);
                fail("CPE reported an invalid state");
            }
        }
        /* Tiny fingerprint tasks usually finish before a scheduler handoff.
         * Longer QC work still yields to the pipeline's other MPE threads. */
        if (now >= spin_until) sched_yield();
    }
    asm volatile("memb" ::: "memory");
#endif
    double wall = monotonic_time() - t0;
    if (run_s) *run_s = wall;
    unsigned long maxima[4] = {0};
    for (int i = 0; i < SWQC_CPE_COUNT; ++i)
        for (int j = 0; j < 4; ++j)
            if (command->kernel_ticks[i][j] > maxima[j])
                maxima[j] = command->kernel_ticks[i][j];
    launch_wall += wall;
    unsigned long dup_sum = 0, qc_sum = 0, pairs = 0, dup_max = 0;
    for (int i = 0; i < SWQC_CPE_COUNT; ++i) {
        unsigned long d = command->duplicate_ticks[i];
        dup_sum += d;
        if (d > dup_max) dup_max = d;
        qc_sum += command->kernel_ticks[i][1];
        pairs += command->duplicate_pairs[i];
    }
    dup_ticks_sum += dup_sum; qc_ticks_sum += qc_sum; dup_pairs_sum += pairs;
    const char *profile = getenv("SWQC_PROFILE_ROUNDS");
    if (maxima[1] && profile && strcmp(profile, "1") == 0) {
        unsigned long minimum = maxima[1], active = 0;
        for (int i = 0; i < SWQC_CPE_COUNT; ++i) {
            unsigned long ticks = command->kernel_ticks[i][1];
            if (ticks) {
                ++active;
                if (ticks < minimum) minimum = ticks;
            }
        }
        swqc_log_fprintf(stderr, "[SWQC QC round] rank=%d round=%lu active_cpes=%lu qc_min_ticks=%lu qc_max_ticks=%lu qc_sum_ticks=%lu dup_sum_ticks=%lu host_s=%.6f\n",
            rank_id, qc_rounds++, active, minimum, maxima[1], qc_sum, dup_sum, wall);
    }
    /* Preserve complete totals, but bound per-task diagnostics on long runs.
     * Log both calls of sampled QC/fingerprint pairs, not only one parity. */
    const int log_task = tasks <= 8 || ((tasks - 1) / 2) % 64 == 0;
    if (pairs && log_task)
        swqc_log_fprintf(stderr, "[SWQC duplicate task] rank=%d task=%lu pairs=%lu dup_sum_ticks=%lu qc_sum_ticks=%lu dup_max_ticks=%lu\n",
            rank_id, tasks, pairs, dup_sum, qc_sum, dup_max);
    for (int j = 0; j < 4; ++j) max_ticks_sum[j] += maxima[j];
    if (log_task) swqc_log_fprintf(stderr, "[SWQC kernel] rank=%d task=%lu host_s=%.6f format_ticks=%lu qc_ticks=%lu serialize_ticks=%lu total_ticks=%lu\n",
            rank_id, tasks, wall, maxima[0], maxima[1], maxima[2], maxima[3]);
    pthread_mutex_unlock(&launch_mutex);
}

void swqc_runtime_finish(void)
{
    unsigned long calls = 0, fallbacks = 0;
    if (!command) return;
    for (int i = 0; i < SWQC_CPE_COUNT; ++i) {
        calls += command->pool_calls[i];
        fallbacks += command->pool_fallbacks[i];
    }
    swqc_log_fprintf(stderr, "[SWQC rank %d] tasks=%lu pool_calls=%lu pool_fallbacks=%lu\n",
            rank_id, tasks, calls, fallbacks);
    swqc_log_fprintf(stderr, "[SWQC kernel total] rank=%d host_s=%.6f format_ticks=%lu qc_ticks=%lu serialize_ticks=%lu total_ticks=%lu\n",
            rank_id, launch_wall, max_ticks_sum[0], max_ticks_sum[1],
            max_ticks_sum[2], max_ticks_sum[3]);
    unsigned long ldm[7] = {0};
    swqc_log_fprintf(stderr, "[SWQC duplicate total] rank=%d pairs=%lu dup_sum_ticks=%lu qc_sum_ticks=%lu ticks_per_pair=%.3f qc_fraction=%.6f\n",
        rank_id, dup_pairs_sum, dup_ticks_sum, qc_ticks_sum,
        dup_pairs_sum ? (double)dup_ticks_sum / dup_pairs_sum : 0,
        qc_ticks_sum ? (double)dup_ticks_sum / qc_ticks_sum : 0);
    for (int i = 0; i < SWQC_CPE_COUNT; ++i) {
        for (int j = 0; j < 6; ++j) ldm[j] += command->ldm_stats[i][j];
        if (command->ldm_stats[i][6] > ldm[6]) ldm[6] = command->ldm_stats[i][6];
    }
    swqc_log_fprintf(stderr, "[SWQC LDM total] rank=%d enabled=%d bytes_per_cpe=%d scratch_requests=%lu scratch_hits=%lu new_requests=%lu new_hits=%lu arenas=%lu refusals=%lu peak_bytes=%lu\n",
            rank_id, SWQC_LDM_POOL, SWQC_LDM_POOL_BYTES, ldm[0], ldm[1], ldm[2],
            ldm[3], ldm[4], ldm[5], ldm[6]);
    /* Cross CPEs remain in their persistent dispatcher until process exit.
     * Do not free the mailbox/pool or call the ordinary 64-CPE halt here. */
#if SWQC_EXEC_MODE == SWQC_SINGLE
    athread_halt();
#endif
}
