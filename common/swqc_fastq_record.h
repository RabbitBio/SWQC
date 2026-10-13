#ifndef SWQC_FASTQ_RECORD_H
#define SWQC_FASTQ_RECORD_H
#include <stddef.h>

/* Parse and validate once, where the record is consumed. Offsets still point
 * into the original input; CRLF is excluded from lengths, not copied away. */
template<class Reference>
inline int swqc_fastq_record(unsigned char *data, size_t bytes, size_t &pos, Reference &ref) {
    if (pos == bytes) return 0;
    size_t begin[4], length[4];
    for (unsigned line = 0; line < 4; ++line) {
        begin[line] = pos;
        while (pos < bytes && data[pos] != '\n') ++pos;
        if (pos == bytes) return -1;
        length[line] = pos - begin[line];
        if (length[line] && data[pos - 1] == '\r') --length[line];
        if (length[line] > (1U << 16)) return -1;
        ++pos;
    }
    if (!length[0] || data[begin[0]] != '@' || !length[2] || data[begin[2]] != '+' ||
        !length[1] || length[1] != length[3]) return -1;
    ref.base = data;
    ref.pname = begin[0]; ref.lname = length[0];
    ref.pseq = begin[1]; ref.lseq = length[1];
    ref.pstrand = begin[2]; ref.lstrand = length[2];
    ref.pqual = begin[3]; ref.lqual = length[3];
    return 1;
}
#endif
