#ifndef SWQC_HOST_IO_H
#define SWQC_HOST_IO_H
#include <stddef.h>
#include <sys/types.h>
#ifdef __cplusplus
extern "C" {
#endif
int swqc_host_io_init(int threads);
/* Same fixed workers, but positional reads remain serial. */
int swqc_host_memory_init(int threads);
void swqc_host_io_destroy(void);
/* Exact positional read; helpers do no allocation, parsing or MPI calls. */
int swqc_host_pread(int fd, void *buffer, size_t bytes, off_t offset,
                    unsigned long *calls);
void swqc_host_memcpy(void *destination, const void *source, size_t bytes);
void swqc_host_memmove(void *destination, const void *source, size_t bytes);
/* Synchronous disjoint slices; callbacks must not dispatch work recursively. */
typedef void (*swqc_host_task)(void *, int, size_t, size_t);
int swqc_host_threads(void);
void swqc_host_for(size_t count, swqc_host_task task, void *argument);
#ifdef __cplusplus
}
#endif
#endif
