#ifndef SWQC_LOG_H
#define SWQC_LOG_H
#include <stdio.h>
enum { SWQC_LOG_BUFFER_BYTES = 1024 << 10 };
#ifdef __cplusplus
extern "C" {
#endif
void swqc_log_begin(void);
void swqc_log_end(void);
int swqc_log_open_rank(int rank);
int swqc_log_fprintf(FILE *stream, const char *format, ...) __attribute__((format(printf, 2, 3)));
int swqc_log_printf(const char *format, ...) __attribute__((format(printf, 1, 2)));
#ifdef __cplusplus
}
#endif
#endif
