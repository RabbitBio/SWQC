#ifndef SWQC_RAPID_READER_H
#define SWQC_RAPID_READER_H
#include <cstddef>
#include <memory>
#include <vector>
#include "decoded_view.h"
struct SwqcGzipMetrics;
class SwqcLineIndex;

class SwqcRapidReader {
public:
    SwqcRapidReader(const char *path, size_t chunk, unsigned workers, SwqcGzipMetrics &metrics,
                    const std::vector<unsigned char> *memory = nullptr, bool views = false);
    ~SwqcRapidReader();
    size_t read(void *out, size_t bytes, SwqcLineIndex *lines = nullptr);
    bool eof() const;
    bool peek(size_t maximum, SwqcDecodedView &view);
    void consume(size_t bytes);
private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};
#endif
