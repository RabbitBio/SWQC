#ifndef SWQC_LINE_INDEX_H
#define SWQC_LINE_INDEX_H
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <vector>

// CPEs count newlines while packing decoded bytes. The host only scans the
// partial pages and the page containing a requested record boundary.
class SwqcLineIndex {
public:
    enum { PAGE = 4096 };
    void clear() { spans_.clear(); bytes_ = lines_ = 0; }
    size_t lines() const { return lines_; }
    void append_slice(const SwqcLineIndex &source, const unsigned char *data,
                      size_t offset, size_t size) {
        if (offset > source.bytes_ || size > source.bytes_ - offset)
            throw std::out_of_range("newline metadata slice");
        auto it = std::upper_bound(source.spans_.begin(), source.spans_.end(), offset,
            [](size_t pos, const Span &span) { return pos < span.end; });
        size_t copied = 0;
        while (copied < size) {
            const size_t start = it == source.spans_.begin() ? 0 : (it - 1)->end;
            const size_t previous = it == source.spans_.begin() ? 0 : (it - 1)->lines;
            const size_t n = std::min(size - copied, it->end - offset);
            size_t count = it->lines - previous;
            if (offset != start || n != it->end - start) {
                count = 0;
                const unsigned char *p = data + copied, *end = p + n;
                while (p < end) {
                    p = static_cast<const unsigned char *>(std::memchr(p, '\n', end - p));
                    if (!p) break;
                    ++count; ++p;
                }
            }
            bytes_ += n; lines_ += count;
            spans_.push_back({bytes_, lines_});
            copied += n; offset += n; ++it;
        }
    }
    void append(const unsigned char *data, size_t size,
                const uint32_t *counts = nullptr, size_t source_offset = 0) {
        size_t pos = 0;
        while (pos < size) {
            size_t within = (source_offset + pos) % PAGE;
            size_t n = std::min(size - pos, size_t(PAGE) - within);
            size_t count = 0;
            if (counts && !within && n == PAGE)
                count = counts[(source_offset + pos) / PAGE];
            else {
                const unsigned char *p = data + pos, *end = p + n;
                while (p < end) {
                    p = static_cast<const unsigned char *>(std::memchr(p, '\n', end - p));
                    if (!p) break;
                    ++count; ++p;
                }
            }
            bytes_ += n; lines_ += count; pos += n;
            spans_.push_back({bytes_, lines_});
        }
    }
    size_t end_of_line(const unsigned char *data, size_t line) const {
        if (!line || line > lines_) throw std::runtime_error("invalid FASTQ line boundary");
        auto it = std::lower_bound(spans_.begin(), spans_.end(), line,
            [](const Span &span, size_t n) { return span.lines < n; });
        size_t pos = 0, previous = 0;
        if (it != spans_.begin()) { pos = (it - 1)->end; previous = (it - 1)->lines; }
        while (previous++ < line) {
            const auto *p = static_cast<const unsigned char *>(std::memchr(data + pos, '\n', it->end - pos));
            if (!p) throw std::runtime_error("inconsistent FASTQ line metadata");
            pos = p - data + 1;
        }
        return pos;
    }
private:
    struct Span { size_t end, lines; };
    std::vector<Span> spans_;
    size_t bytes_ = 0, lines_ = 0;
};
#endif
