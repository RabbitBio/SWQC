#include "gzip_stream.h"
#include "rapid_reader.h"
#include <algorithm>
#include <chrono>
#include <climits>
#include <cstring>
#include <fcntl.h>
#include <stdexcept>
#include <sys/stat.h>
#include <unistd.h>
#include <zlib.h>

namespace {
double now() {
    return std::chrono::duration<double>(std::chrono::steady_clock::now().time_since_epoch()).count();
}
void validate(size_t chunk, unsigned workers) {
    if (chunk < (64U << 10) || chunk > (64U << 20) || !workers || workers > 384)
        throw std::runtime_error("invalid gzip chunk size or worker count");
}
void launch(std::vector<swqc_codec_job> &jobs, unsigned workers, SwqcGzipMetrics &m) {
    if (jobs.empty()) return;
    swqc_codec_batch batch{jobs.data(), (unsigned)jobs.size(), workers, 0, 0};
    double t = now();
    swqc_codec_launch(&batch);
    m.launch_s += now() - t;
    m.kernel_s += batch.run_s; m.queue_s += batch.wait_s;
    m.jobs += jobs.size();
    for (const auto &job : jobs)
        if (job.status != Z_OK)
            throw std::runtime_error("parallel gzip job failed, status=" + std::to_string(job.status));
}
}

struct SwqcGzipReader::Impl {
    std::unique_ptr<SwqcRapidReader> rapid;
    gzFile serial = nullptr;
    const std::vector<unsigned char> *memory = nullptr;
    z_stream stream{};
    bool initialized = false;
    size_t position = 0;
    bool ended = false;
    bool views = false;
    ~Impl() {
        if (serial) gzclose(serial);
        if (initialized) inflateEnd(&stream);
    }
    size_t read_memory(void *out, size_t bytes) {
        if (ended) return 0;
        const uInt capacity = (uInt)std::min<size_t>(bytes, INT_MAX);
        stream.next_out = static_cast<Bytef *>(out);
        stream.avail_out = capacity;
        while (stream.avail_out && !ended) {
            if (!stream.avail_in && position < memory->size()) {
                stream.avail_in = (uInt)std::min<size_t>(memory->size() - position, INT_MAX);
                stream.next_in = const_cast<Bytef *>(memory->data() + position);
                position += stream.avail_in;
            }
            uInt before_in = stream.avail_in, before_out = stream.avail_out;
            int result = inflate(&stream, Z_NO_FLUSH);
            if (result == Z_STREAM_END) {
                position -= stream.avail_in;
                // Match ordinary gzip member chaining, including zero padding.
                if (position == memory->size() || (*memory)[position] == 0) {
                    while (position < memory->size())
                        if ((*memory)[position++] != 0)
                            throw std::runtime_error("nonzero bytes after gzip padding");
                    ended = true;
                } else {
                    Bytef *next = stream.next_out;
                    uInt available = stream.avail_out;
                    if (inflateReset(&stream) != Z_OK) throw std::runtime_error("gzip reset failed");
                    stream.next_in = nullptr; stream.avail_in = 0;
                    stream.next_out = next; stream.avail_out = available;
                }
            } else if (result != Z_OK ||
                       (before_in == stream.avail_in && before_out == stream.avail_out)) {
                throw std::runtime_error("invalid or truncated memory gzip stream");
            }
        }
        return capacity - stream.avail_out;
    }
};

SwqcGzipReader::SwqcGzipReader(const char *path, size_t chunk, unsigned workers,
                             const std::string &backend,
                             const std::vector<unsigned char> *memory, bool views) : impl_(new Impl) {
    validate(chunk, workers);
    if (backend != "auto" && backend != "rapid" && backend != "zlib")
        throw std::runtime_error("gzip reader must be auto, rapid or zlib");
    bool rapid = backend == "rapid";
#ifdef SWQC_CODEC_RAPID
    rapid = rapid || backend == "auto";
#else
    if (rapid) throw std::runtime_error("build with GZIP_DECODER=rapid for parallel input");
#endif
    if (rapid) {
        impl_->views = views;
        impl_->rapid.reset(new SwqcRapidReader(path, chunk, workers, metrics_, memory, views));
    }
    else if (memory) {
        impl_->memory = memory;
        metrics_.input_bytes = memory->size();
        if (inflateInit2(&impl_->stream, 31) != Z_OK) throw std::runtime_error("gzip initialize failed");
        impl_->initialized = true;
    }
    else {
        struct stat st;
        if (stat(path, &st)) throw std::runtime_error("cannot stat gzip input");
        metrics_.input_bytes = st.st_size;
        impl_->serial = gzopen(path, "rb");
        if (!impl_->serial) throw std::runtime_error("cannot open gzip input");
        gzbuffer(impl_->serial, 1U << 20);
    }
}
SwqcGzipReader::~SwqcGzipReader() = default;
bool SwqcGzipReader::supports_views() const { return impl_->views; }
bool SwqcGzipReader::peek(size_t maximum, SwqcDecodedView &view) {
    if (!supports_views()) throw std::logic_error("gzip reader does not support views");
    const double start = now();
    bool present = impl_->rapid->peek(maximum, view);
    metrics_.read_s += now() - start;
    return present;
}
void SwqcGzipReader::consume(size_t bytes) {
    if (!supports_views()) throw std::logic_error("gzip reader does not support views");
    impl_->rapid->consume(bytes);
    metrics_.output_bytes += bytes;
}
bool SwqcGzipReader::eof() const {
    return impl_->rapid ? impl_->rapid->eof() : impl_->ended;
}
size_t SwqcGzipReader::read(void *destination, size_t bytes, SwqcLineIndex *lines) {
    if (!bytes) return 0;
    const double start = now();
    size_t copied;
    if (impl_->rapid) copied = impl_->rapid->read(destination, bytes, lines);
    else if (impl_->memory) {
        copied = impl_->read_memory(destination, bytes);
        metrics_.serial_s += now() - start;
    } else {
        int n = gzread(impl_->serial, destination, (unsigned)std::min<size_t>(bytes, INT_MAX));
        int error = Z_OK;
        const char *message = gzerror(impl_->serial, &error);
        if (n < 0 || (error != Z_OK && error != Z_STREAM_END))
            throw std::runtime_error(std::string("serial gzip read: ") + message);
        copied = (size_t)n;
        impl_->ended = n == 0;
        metrics_.serial_s += now() - start;
    }
    if (lines && !impl_->rapid) lines->append(static_cast<unsigned char *>(destination), copied);
    metrics_.read_s += now() - start;
    metrics_.output_bytes += copied;
    return copied;
}

SwqcGzipWriter::SwqcGzipWriter(const char *path, size_t chunk, unsigned workers, int level)
    : path_(path), chunk_bytes_(chunk), workers_(workers), level_(level) {
    validate(chunk, workers);
    if (level < 1 || level > 9) throw std::runtime_error("invalid gzip compression level");
    batch_chunks_ = std::max<size_t>(1, std::min<size_t>(workers, (64U << 20) / chunk));
    int fd = open(path, O_WRONLY | O_CREAT | O_EXCL, 0666);
    if (fd < 0) throw std::runtime_error("gzip output already exists or cannot be created: " + path_);
    file_ = fdopen(fd, "wb");
    if (!file_) { close(fd); unlink(path); throw std::runtime_error("gzip fdopen failed"); }
    const unsigned char header[10] = {31,139,8,0,0,0,0,0,0,3};
    try { write(header, sizeof(header)); }
    catch (...) { fclose(file_); file_ = nullptr; unlink(path); throw; }
}
SwqcGzipWriter::~SwqcGzipWriter() {
    if (file_) fclose(file_);
    if (!complete_) unlink(path_.c_str());
}
void SwqcGzipWriter::write(const void *data, size_t bytes) {
    double t = now();
    if (bytes && fwrite(data, 1, bytes, file_) != bytes) throw std::runtime_error("gzip output write failed");
    metrics_.io_s += now() - t;
    metrics_.output_bytes += bytes;
}
void SwqcGzipWriter::append(const void *data, size_t bytes) {
    if (!file_) throw std::runtime_error("gzip append after close");
    const auto *p = static_cast<const unsigned char *>(data);
    while (bytes) {
        if (input_.empty() || input_.back().size() == chunk_bytes_) input_.emplace_back();
        auto &b = input_.back();
        size_t n = std::min(bytes, chunk_bytes_ - b.size());
        b.insert(b.end(), p, p + n);
        p += n; bytes -= n; metrics_.input_bytes += n;
        if (input_.size() == batch_chunks_ && b.size() == chunk_bytes_) flush();
    }
}
void SwqcGzipWriter::flush() {
    std::vector<std::vector<unsigned char>> output(input_.size()), dictionaries(input_.size());
    std::vector<swqc_codec_job> jobs;
    for (size_t i = 0; i < input_.size(); ++i) {
        const auto &b = input_[i];
        dictionaries[i] = previous_;
        // Append (not replace) history when the final block is shorter than 32 KiB.
        if (b.size() >= 32768) previous_.assign(b.end() - 32768, b.end());
        else {
            previous_.insert(previous_.end(), b.begin(), b.end());
            if (previous_.size() > 32768) previous_.erase(previous_.begin(), previous_.end() - 32768);
        }
        output[i].resize(compressBound(b.size()) + 64);
        swqc_codec_job job{};
        job.input = b.data(); job.input_size = b.size();
        job.output = output[i].data(); job.output_capacity = output[i].size();
        job.dictionary = dictionaries[i].data(); job.dictionary_size = dictionaries[i].size();
        job.level = level_; job.operation = SWQC_CODEC_DEFLATE;
        jobs.push_back(job);
    }
    launch(jobs, workers_, metrics_);
    for (const auto &job : jobs) {
        write(job.output, job.output_size);
        crc_ = (uint32_t)crc32_combine(crc_, job.crc, (z_off_t)job.input_size);
    }
    input_.clear();
}
void SwqcGzipWriter::finish() {
    if (complete_) return;
    flush();
    unsigned char trailer[10] = {3,0};
    for (unsigned i = 0; i < 4; ++i) {
        trailer[2+i] = (unsigned char)(crc_ >> (8*i));
        trailer[6+i] = (unsigned char)((uint32_t)metrics_.input_bytes >> (8*i));
    }
    write(trailer, sizeof(trailer));
    FILE *p = file_; file_ = nullptr;
    if (fclose(p)) throw std::runtime_error("gzip close failed");
    complete_ = true;
}
