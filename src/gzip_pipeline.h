#ifndef SWQC_GZIP_PIPELINE_H
#define SWQC_GZIP_PIPELINE_H
#include "../codec/gzip_stream.h"
#include "FastxChunk.h"
#include <memory>
#include <initializer_list>
#include <stdexcept>
#include <string>

bool swqc_standard_gzip();
bool swqc_gzip_zero_copy();
inline bool swqc_gzip_path(const std::string &path) {
    return path.size() >= 3 && path.compare(path.size() - 3, 3, ".gz") == 0;
}
inline bool swqc_select_standard_gzip(bool force, bool legacy,
                                     std::initializer_list<std::string> paths) {
    if (force && legacy)
        throw std::invalid_argument("--standardGzip and --legacyGzip are mutually exclusive");
    if (legacy) return false;
    for (const auto &path : paths)
        if (swqc_gzip_path(path)) return true;
    return force;
}
inline void swqc_validate_gzip_paths(const std::string &read1, const std::string &read2,
                                     const std::string &write1, const std::string &write2) {
    if (read1.empty() || read2.empty())
        throw std::invalid_argument("the standard gzip pipeline requires two input files");
    if (write1.empty() != write2.empty())
        throw std::invalid_argument("paired output requires both output paths or neither");
    if (swqc_gzip_path(read1) != swqc_gzip_path(read2) ||
        swqc_gzip_path(write1) != swqc_gzip_path(write2))
        throw std::invalid_argument("paired files must have matching plain/gzip types");
    for (const auto &path : {read1, read2, write1, write2})
        if (path == "/dev/stdin" || path == "/dev/stdout" || path == "-")
            throw std::invalid_argument("the standard gzip pipeline requires file paths, not stdin/stdout");
}
unsigned swqc_gzip_workers();
size_t swqc_gzip_chunk_bytes();
void swqc_gzip_report(const char *phase, const char *path, const SwqcGzipMetrics &m);

// Bulk input and paired record boundaries; full parsing is done by QC CPEs.
class SwqcGzipPairReader {
public:
    SwqcGzipPairReader(const std::string &r1, const std::string &r2);
    ~SwqcGzipPairReader();
    rabbit::fq::FastqDataPairChunk *next(rabbit::fq::FastqDataPool *pool);
private:
    struct Input;
    std::unique_ptr<Input> input_[2];
    uint64_t chunks_ = 0, pairs_ = 0, bytes_ = 0;
    double acquire_s_ = 0;
};
#endif
