#include <cstdlib>
#include <cstring>
#include <cstdint>
#include <cstdio>
#include <new>
extern "C" {
#include <slave.h>
#include <crts.h>
}
#include "swqc_runtime.h"
#include "swqc_layout.h"
#if SWQC_LDM_POOL
#include "swqc_ldm_arena.h"
extern "C" void *ldm_malloc(size_t);
extern "C" void ldm_free(void *, size_t);
struct SwqcLdmState {
    SwqcLdmArena arena;
    void *backing;
    bool transient;
    unsigned long stats[7];
};
static SwqcLdmState *ldm_states[SWQC_CPE_COUNT];
static SwqcLdmState ldm_fallback_states[SWQC_CPE_COUNT];
static const size_t ldm_backing_bytes = sizeof(SwqcLdmState) + SWQC_LDM_POOL_BYTES + 63;
#else
extern "C" void *ldm_malloc(size_t);
extern "C" void ldm_free(void *, size_t);
#endif

__thread swqc_command *command;

extern "C" void swqc_cpe_error(unsigned long code, unsigned long first, unsigned long second)
{
    swqc_command *mailbox = (swqc_command *)((unsigned long)command | (1UL << 45));
    mailbox->error[_MYID][1] = first;
    mailbox->error[_MYID][2] = second;
    asm volatile("memb" ::: "memory");
    mailbox->error[_MYID][0] = code;
#if SWQC_EXEC_MODE == SWQC_CROSS
    for (;;) asm volatile("" ::: "memory");
#else
    abort();
#endif
}

extern "C" void swqc_record_duplicate_ticks(unsigned long ticks, unsigned long pairs)
{
    swqc_command *mailbox = (swqc_command *)((unsigned long)command | (1UL << 45));
    mailbox->duplicate_ticks[_MYID] = ticks;
    mailbox->duplicate_pairs[_MYID] = pairs;
}

extern "C" void swqc_record_kernel_ticks(unsigned long format, unsigned long qc,
                                         unsigned long serialize, unsigned long total)
{
    swqc_command *mailbox = (swqc_command *)((unsigned long)command | (1UL << 45));
    mailbox->kernel_ticks[_MYID][0] = format;
    mailbox->kernel_ticks[_MYID][1] = qc;
    mailbox->kernel_ticks[_MYID][2] = serialize;
    mailbox->kernel_ticks[_MYID][3] = total;
}
static const volatile unsigned long text_start = SWQC_TEXT_START;
static const volatile unsigned long text_size = SWQC_TEXT_SIZE;

/* Segregated free lists live in the same cross-segment region as their data.
 * Ownership never migrates between CPEs; no inter-CPE allocator lock is needed. */
struct pool_block {
    pool_block *next;
    unsigned size_class;
    unsigned owner;
};
static pool_block *free_lists[SWQC_CPE_COUNT][20];
static size_t used_bytes[SWQC_CPE_COUNT];

extern "C" void swqc_ldm_begin(void)
{
#if SWQC_LDM_POOL
    if (ldm_states[_MYID]) abort();
    void *backing = ldm_malloc(ldm_backing_bytes);
    // Leave room for the stack and the legacy QC scratch fallback.
    volatile unsigned char stack_marker;
    uintptr_t stack = (uintptr_t)&stack_marker, begin = (uintptr_t)backing;
    if (backing && (begin >= stack || ldm_backing_bytes > stack - begin ||
        stack - begin - ldm_backing_bytes < (32UL << 10))) {
        ldm_free(backing, ldm_backing_bytes);
        backing = NULL;
    }
    SwqcLdmState *state = backing ? (SwqcLdmState *)backing : &ldm_fallback_states[_MYID];
    memset(state, 0, sizeof(*state));
    state->backing = backing;
    state->stats[backing ? 4 : 5] = 1;
    if (backing) {
        void *payload = (void *)(((uintptr_t)(state + 1) + 63) & ~(uintptr_t)63);
        state->arena.init(payload);
    }
    ldm_states[_MYID] = state;
#endif
}

extern "C" void swqc_ldm_transient(int enabled)
{
#if SWQC_LDM_POOL
    if (ldm_states[_MYID]) ldm_states[_MYID]->transient = enabled != 0;
#else
    (void)enabled;
#endif
}

extern "C" void *swqc_scratch_alloc(size_t bytes)
{
#if SWQC_LDM_POOL
    SwqcLdmState *state = ldm_states[_MYID];
    if (state) {
        ++state->stats[0];
        if (state->backing) {
            void *p = state->arena.allocate(bytes);
            if (p) { ++state->stats[1]; return p; }
        }
    }
#endif
#if SWQC_MANUAL_LDM
    void *result = ldm_malloc(bytes);
#else
    void *result = new unsigned char[bytes ? bytes : 1];
#endif
    if (!result) {
        swqc_cpe_error(1, bytes, 0);
    }
    return result;
}

extern "C" void swqc_scratch_free(void *ptr, size_t bytes)
{
#if SWQC_LDM_POOL
    SwqcLdmState *state = ldm_states[_MYID];
    if (state && state->backing && state->arena.contains(ptr)) {
        if (!state->arena.release(ptr)) abort();
        return;
    }
#endif
#if SWQC_MANUAL_LDM
    ldm_free(ptr, bytes);
#else
    delete[] static_cast<unsigned char *>(ptr);
#endif
}

extern "C" void swqc_ldm_end(void)
{
#if SWQC_LDM_POOL
    SwqcLdmState *state = ldm_states[_MYID];
    if (!state || state->arena.live) {
        swqc_cpe_error(2, state ? state->arena.live : 0, 0);
    }
    state->stats[6] = state->arena.peak;
    swqc_command *mailbox = (swqc_command *)((unsigned long)command | (1UL << 45));
    for (int i = 0; i < 6; ++i) mailbox->ldm_stats[_MYID][i] += state->stats[i];
    if (state->stats[6] > mailbox->ldm_stats[_MYID][6])
        mailbox->ldm_stats[_MYID][6] = state->stats[6];
    void *backing = state->backing;
    ldm_states[_MYID] = NULL;
    if (backing) ldm_free(backing, ldm_backing_bytes);
#endif
}

static void *allocate(size_t bytes)
{
#if SWQC_LDM_POOL
    SwqcLdmState *state = ldm_states[_MYID];
    if (state && state->transient) {
        ++state->stats[2];
        if (state->backing) {
            void *p = state->arena.allocate(bytes);
            if (p) { ++state->stats[3]; return p; }
        }
    }
#endif
#if SWQC_POOL
    if (command && command->pool) {
        const unsigned id = _MYID;
        size_t block_bytes = 32;
        unsigned cls = 0;
        ++command->pool_calls[id];
        if (bytes <= (size_t)-1 - sizeof(pool_block)) {
            while (block_bytes < bytes + sizeof(pool_block) && cls < 19) {
                block_bytes *= 2;
                ++cls;
            }
            if (block_bytes >= bytes + sizeof(pool_block)) {
                pool_block *p = free_lists[id][cls];
                if (p) free_lists[id][cls] = p->next;
                else if (block_bytes <= command->pool_bytes - used_bytes[id]) {
                    p = (pool_block *)(command->pool + id * command->pool_bytes + used_bytes[id]);
                    used_bytes[id] += block_bytes;
                }
                if (p) {
                    p->next = NULL;
                    p->size_class = cls;
                    p->owner = id;
                    return p + 1;
                }
            }
        }
        ++command->pool_fallbacks[id];
    }
#endif
    return malloc(bytes ? bytes : 1);
}

static void release(void *address)
{
    if (!address) return;
#if SWQC_LDM_POOL
    SwqcLdmState *state = ldm_states[_MYID];
    if (state && state->backing && state->arena.contains(address)) {
        if (!state->arena.release(address)) abort();
        return;
    }
#endif
#if SWQC_POOL
    if (command && command->pool) {
        uintptr_t base = (uintptr_t)command->pool;
        uintptr_t p = (uintptr_t)address;
        if (p >= base && p - base < command->pool_bytes * SWQC_CPE_COUNT) {
            pool_block *block = (pool_block *)address - 1;
            if (block->owner != (unsigned)_MYID || block->size_class >= 20) {
                swqc_cpe_error(3, block->owner, block->size_class);
            }
            block->next = free_lists[_MYID][block->size_class];
            free_lists[_MYID][block->size_class] = block;
            return;
        }
    }
#endif
    free(address);
}

#if SWQC_POOL
void *operator new(size_t bytes) { void *p = allocate(bytes); if (!p) abort(); return p; }
void *operator new[](size_t bytes) { return ::operator new(bytes); }
void operator delete(void *p) noexcept { release(p); }
void operator delete[](void *p) noexcept { release(p); }
void operator delete(void *p, size_t) noexcept { release(p); }
void operator delete[](void *p, size_t) noexcept { release(p); }
#endif

extern "C" void swqc_pass(swqc_command *p)
{
    command = p;
    if (_MYID < 0 || _MYID >= SWQC_CPE_COUNT) {
        fprintf(stderr, "[SWQC runtime] invalid CPE id=%d count=%d\n", _MYID, SWQC_CPE_COUNT);
        abort();
    }
    command->completed[_MYID] = 1;
    flush_slave_cache();
}

extern "C" void swqc_copy_private(swqc_command *p)
{
    memcpy((char *)p->private_copies + (2UL << 20) * _MYID,
           (void *)0x400000000000UL, 2UL << 20);
}

extern "C" void swqc_relocate_private(swqc_command *p)
{
    unsigned long *words = (unsigned long *)((char *)p->private_copies + (2UL << 20) * _MYID);
    for (size_t i = 0; i < (2UL << 20) / sizeof(*words); ++i) {
        if (words[i] < text_start || words[i] >= text_start + text_size) continue;
        for (size_t j = 0; j < p->tls_count; ++j) {
            if (words[i] == *(unsigned long *)p->tls_relocations[j]) {
                words[i] += p->segment_offset;
                break;
            }
        }
    }
}

/* Adapted from SWBWA's persistent dispatcher: redirect PC once, then let every
 * task return normally. Repeated PC redirection would leak stack frames. */
extern "C" __attribute__((noreturn, noinline)) void swqc_dispatch(void)
{
    unsigned long csr;
    asm volatile("mov %0, $29\n\t" :: "r"(command->relocated_gp));
    asm volatile("rcsr %0, 0xc4" : "=r"(csr));
    if (csr < 0x500000000000UL) {
        csr += (unsigned long)command->private_copies + (2UL << 20) * _MYID - 0x400000000000UL;
        asm volatile("wcsr %0, 0xc4" :: "r"(csr));
    }
    swqc_command *mailbox = (swqc_command *)((unsigned long)command | (1UL << 45));
    unsigned long previous = 0;
    for (;;) {
        unsigned long sequence = mailbox->sequence;
        if (sequence == previous) continue;
        asm volatile("memb" ::: "memory");
        ((void (*)(void *))mailbox->entry)(mailbox->argument);
        previous = sequence;
        flush_slave_cache();
        asm volatile("memb" ::: "memory");
        mailbox->completed[_MYID] = 1;
        asm volatile("memb" ::: "memory");
    }
}
