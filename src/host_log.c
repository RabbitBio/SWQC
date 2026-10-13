#include "swqc_log.h"
#include <pthread.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>

/* Bounded, process-local diagnostics. Overflow is reported, never hidden. */
static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
static char buffer[SWQC_LOG_BUFFER_BYTES];
static size_t used;
static int active, overflow, registered;
static FILE *rank_log;

int swqc_log_open_rank(int rank)
{
    char path[80];
    snprintf(path, sizeof(path), "swqc.rank%06d.log", rank);
    rank_log = fopen(path, "w");
    return rank_log ? 0 : -1;
}

void swqc_log_begin(void)
{
    const char *enabled = getenv("SWQC_DEFER_LOGS");
    if (!enabled || strcmp(enabled, "1")) return;
    pthread_mutex_lock(&mutex);
    used = 0;
    overflow = 0;
    active = 1;
    if (!registered) { atexit(swqc_log_end); registered = 1; }
    pthread_mutex_unlock(&mutex);
}

void swqc_log_end(void)
{
    pthread_mutex_lock(&mutex);
    if (active) {
        active = 0;
        if (rank_log) {
            fwrite(buffer, 1, used, rank_log);
            fprintf(rank_log, "[SWQC deferred logs] bytes=%zu overflow=%d\n", used, overflow);
            fflush(rank_log);
        }
        for (size_t begin = 0; begin < used;) {
            const char *newline = memchr(buffer + begin, '\n', used - begin);
            size_t bytes = newline ? (size_t)(newline - buffer) + 1 - begin : used - begin;
            fwrite(buffer + begin, 1, bytes, stderr);
            begin += bytes;
        }
        fprintf(stderr, "[SWQC deferred logs] bytes=%zu overflow=%d\n", used, overflow);
        fflush(stderr);
        used = 0;
    }
    pthread_mutex_unlock(&mutex);
}

static int log_arguments(FILE *stream, const char *format, va_list arguments)
{
    int result;
    pthread_mutex_lock(&mutex);
    if (active && (stream == stdout || stream == stderr)) {
        va_list copy;
        va_copy(copy, arguments);
        result = vsnprintf(buffer + used, sizeof(buffer) - used, format, copy);
        va_end(copy);
        if (result >= 0 && (size_t)result < sizeof(buffer) - used) {
            used += (size_t)result;
        } else {
            overflow = 1;
            result = vfprintf(stream, format, arguments);
        }
    } else {
        if (rank_log && (stream == stdout || stream == stderr)) {
            va_list copy;
            va_copy(copy, arguments);
            vfprintf(rank_log, format, copy);
            va_end(copy);
            fflush(rank_log);
        }
        result = vfprintf(stream, format, arguments);
    }
    pthread_mutex_unlock(&mutex);
    return result;
}

int swqc_log_fprintf(FILE *stream, const char *format, ...)
{
    va_list arguments;
    va_start(arguments, format);
    int result = log_arguments(stream, format, arguments);
    va_end(arguments);
    return result;
}

int swqc_log_printf(const char *format, ...)
{
    va_list arguments;
    va_start(arguments, format);
    int result = log_arguments(stdout, format, arguments);
    va_end(arguments);
    return result;
}
