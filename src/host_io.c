#include "swqc_host_io.h"
#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#ifdef __sw_host__
#include <athread.h>
#endif

enum { MAX_READERS = 6 };
typedef struct {
    pthread_t thread;
    int id, target, actual, error;
    unsigned long calls;
} reader_t;
static struct {
    pthread_mutex_t dispatch, mutex;
    pthread_cond_t work, done;
    reader_t readers[MAX_READERS];
    int created, ready, remaining, stopping, initialized, fd, copy_only, copying;
    unsigned long generation;
    char *buffer;
    const char *source;
    size_t bytes;
    off_t offset;
    swqc_host_task task;
    void *task_argument;
} pool = {
    .dispatch = PTHREAD_MUTEX_INITIALIZER,
    .mutex = PTHREAD_MUTEX_INITIALIZER,
    .work = PTHREAD_COND_INITIALIZER,
    .done = PTHREAD_COND_INITIALIZER
};

static int exact_read(int fd, char *buffer, size_t bytes, off_t offset,
                      unsigned long *calls)
{
    *calls = 0;
    while (bytes) {
        ++*calls;
        ssize_t n = pread(fd, buffer, bytes, offset);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) return n == 0 ? EIO : errno;
        buffer += n;
        bytes -= n;
        offset += n;
    }
    return 0;
}

static void read_slice(reader_t *reader)
{
    size_t unit = (pool.bytes + (size_t)pool.created) / (pool.created + 1);
    size_t begin = unit * reader->id;
    size_t bytes = begin < pool.bytes ? pool.bytes - begin : 0;
    if (bytes > unit) bytes = unit;
    reader->calls = 0;
    reader->error = 0;
    if (bytes && pool.task)
        pool.task(pool.task_argument, reader->id, begin, begin + bytes);
    else if (bytes && pool.copying)
        memcpy(pool.buffer + begin, pool.source + begin, bytes);
    else if (bytes)
        reader->error = exact_read(pool.fd, pool.buffer + begin, bytes,
                                   pool.offset + begin, &reader->calls);
}

static void *reader_entry(void *arg)
{
    reader_t *reader = arg;
    unsigned long generation = 0;
#ifdef __sw_host__
    reader->error = SET_TO_SWCG(reader->target);
    unsigned long id;
    asm volatile("rcid %0" : "=r"(id));
    reader->actual = (int)(id & 7);
#else
    reader->actual = -1;
#endif
    pthread_mutex_lock(&pool.mutex);
    ++pool.ready;
    pthread_cond_broadcast(&pool.done);
    if (pool.copy_only) {
        pthread_mutex_unlock(&pool.mutex);
        /* These workers own otherwise idle MPEs. Acquire/release publishes
         * each copy request and completion without a futex wake per MiB. */
        while (!__atomic_load_n(&pool.stopping, __ATOMIC_ACQUIRE)) {
            unsigned long current = __atomic_load_n(&pool.generation, __ATOMIC_ACQUIRE);
            if (current == generation) continue;
            generation = current;
            read_slice(reader);
            __atomic_fetch_sub(&pool.remaining, 1, __ATOMIC_RELEASE);
        }
        return NULL;
    }
    while (!pool.stopping) {
        while (!pool.stopping && generation == pool.generation)
            pthread_cond_wait(&pool.work, &pool.mutex);
        if (pool.stopping) break;
        generation = pool.generation;
        pthread_mutex_unlock(&pool.mutex);
        read_slice(reader);
        pthread_mutex_lock(&pool.mutex);
        if (--pool.remaining == 0) pthread_cond_signal(&pool.done);
    }
    pthread_mutex_unlock(&pool.mutex);
    return NULL;
}

static int initialize_workers(int threads, int copy_only)
{
    int error = 0, home = 0;
    if (threads == 1) return 0;
    if (threads != MAX_READERS || pool.initialized) return EINVAL;
    pool.copy_only = copy_only;
#ifdef __sw_host__
    home = current_array_id();
#endif
    for (int i = 1; i < threads; ++i) {
        reader_t *reader = &pool.readers[i];
        reader->id = i;
        reader->error = 0;
        reader->target = (home + i) % MAX_READERS;
        error = pthread_create(&reader->thread, NULL, reader_entry, reader);
        if (error) break;
        ++pool.created;
    }
    pthread_mutex_lock(&pool.mutex);
    while (pool.ready != pool.created) pthread_cond_wait(&pool.done, &pool.mutex);
    pthread_mutex_unlock(&pool.mutex);
    for (int i = 1; i <= pool.created; ++i) {
        reader_t *reader = &pool.readers[i];
#ifdef __sw_host__
        if (reader->error || reader->actual != reader->target) error = EINVAL;
#endif
        fprintf(stderr, "[SWQC input helper] id=%d target=%d actual=%d bind=%d\n",
                reader->id, reader->target, reader->actual, reader->error);
    }
    if (error) { swqc_host_io_destroy(); return error; }
    pool.initialized = 1;
    return 0;
}

int swqc_host_io_init(int threads) { return initialize_workers(threads, 0); }
int swqc_host_memory_init(int threads) { return initialize_workers(threads, 1); }

int swqc_host_pread(int fd, void *buffer, size_t bytes, off_t offset,
                    unsigned long *calls)
{
    int error = 0;
    if (!pool.initialized || pool.copy_only || bytes < (1UL << 20))
        return exact_read(fd, buffer, bytes, offset, calls);
    pthread_mutex_lock(&pool.dispatch);
    pthread_mutex_lock(&pool.mutex);
    pool.fd = fd;
    pool.task = NULL;
    pool.copying = 0;
    pool.buffer = buffer;
    pool.bytes = bytes;
    pool.offset = offset;
    pool.remaining = pool.created;
    ++pool.generation;
    pthread_cond_broadcast(&pool.work);
    pthread_mutex_unlock(&pool.mutex);
    read_slice(&pool.readers[0]);
    pthread_mutex_lock(&pool.mutex);
    while (pool.remaining) pthread_cond_wait(&pool.done, &pool.mutex);
    pthread_mutex_unlock(&pool.mutex);
    *calls = 0;
    for (int i = 0; i <= pool.created; ++i) {
        *calls += pool.readers[i].calls;
        if (pool.readers[i].error) error = pool.readers[i].error;
    }
    pthread_mutex_unlock(&pool.dispatch);
    return error;
}

/* The caller owns dispatch and has populated the immutable task fields. */
static void dispatch_memory_work(void)
{
    if (pool.copy_only) {
        __atomic_store_n(&pool.remaining, pool.created, __ATOMIC_RELAXED);
        __atomic_add_fetch(&pool.generation, 1, __ATOMIC_RELEASE);
        read_slice(&pool.readers[0]);
        while (__atomic_load_n(&pool.remaining, __ATOMIC_ACQUIRE)) {}
        return;
    }
    pthread_mutex_lock(&pool.mutex);
    pool.remaining = pool.created;
    ++pool.generation;
    pthread_cond_broadcast(&pool.work);
    pthread_mutex_unlock(&pool.mutex);
    read_slice(&pool.readers[0]);
    pthread_mutex_lock(&pool.mutex);
    while (pool.remaining) pthread_cond_wait(&pool.done, &pool.mutex);
    pthread_mutex_unlock(&pool.mutex);
}

int swqc_host_threads(void) { return pool.initialized ? pool.created + 1 : 1; }

void swqc_host_for(size_t count, swqc_host_task task, void *argument)
{
    if (!count) return;
    if (!pool.initialized) { task(argument, 0, 0, count); return; }
    pthread_mutex_lock(&pool.dispatch);
    pool.bytes = count;
    pool.task = task;
    pool.task_argument = argument;
    dispatch_memory_work();
    pool.task = NULL;
    pthread_mutex_unlock(&pool.dispatch);
}

void swqc_host_memcpy(void *destination, const void *source, size_t bytes)
{
    if (!bytes) return;
    if (!pool.initialized || bytes < (512UL << 10)) {
        memcpy(destination, source, bytes);
        return;
    }
    pthread_mutex_lock(&pool.dispatch);
    pool.buffer = destination;
    pool.source = source;
    pool.bytes = bytes;
    pool.copying = 1;
    pool.task = NULL;
    dispatch_memory_work();
    pthread_mutex_unlock(&pool.dispatch);
}

void swqc_host_memmove(void *destination, const void *source, size_t bytes)
{
    uintptr_t d = (uintptr_t)destination, s = (uintptr_t)source;
    if (!bytes || d == s) return;
    /* Overlapping slices require the original memmove, not parallel memcpy. */
    if (!pool.initialized || (d < s ? s - d : d - s) < bytes)
        memmove(destination, source, bytes);
    else
        swqc_host_memcpy(destination, source, bytes);
}

void swqc_host_io_destroy(void)
{
    pthread_mutex_lock(&pool.dispatch);
    pthread_mutex_lock(&pool.mutex);
    if (pool.copy_only) __atomic_store_n(&pool.stopping, 1, __ATOMIC_RELEASE);
    else pool.stopping = 1;
    pthread_cond_broadcast(&pool.work);
    pthread_mutex_unlock(&pool.mutex);
    for (int i = 1; i <= pool.created; ++i) pthread_join(pool.readers[i].thread, NULL);
    pool.created = pool.ready = pool.remaining = pool.initialized = pool.stopping = 0;
    pool.generation = 0;
    pool.copy_only = pool.copying = 0;
    pthread_mutex_unlock(&pool.dispatch);
}
