#include "io_window.h"
#include <cassert>
#include <string>
#include <vector>

int main(int argc, char **argv) {
    MPI_Init(&argc, &argv);
    std::string expected(700001, 'x');
    for (size_t i = 0; i < expected.size(); ++i) expected[i] = char(i * 17 % 251);
    SwqcOutputFingerprint reference;
    assert(reference.append(expected.data(), expected.size(), 0));
    for (int ranks : {1, 6, 32, 192}) {
        uint64_t bytes = 0, samples = 0, sum = 0, x = 0;
        for (int r = 0; r < ranks; ++r) {
            SwqcOutputFingerprint f;
            for (size_t p = r * 1003; p < expected.size(); p += ranks * 1003)
                assert(f.append(expected.data() + p, std::min<size_t>(1003, expected.size() - p), p));
            bytes += f.bytes; samples += f.samples; sum += f.sum64; x ^= f.xor64;
        }
        assert(bytes == reference.bytes && samples == reference.samples);
        assert(sum == reference.sum64 && x == reference.xor64);
    }
    SwqcOutputFingerprint high, split;
    int64_t offset = INT64_C(1) << 35;
    assert(high.append(expected.data(), expected.size(), offset + 3));
    assert(split.append(expected.data(), 1234, offset + 3));
    assert(split.append(expected.data() + 1234, expected.size() - 1234, offset + 1237));
    assert(high.sum64 == split.sum64 && high.xor64 == split.xor64);
    assert(split.append(NULL, 0, INT64_MAX));
    assert(high.end_offset == split.end_offset);
    assert(!split.append("x", 1, -1));
    assert(!split.append("x", 1, INT64_MAX));
    for (bool helpers : {false, true}) {
        if (helpers) assert(swqc_host_memory_init(6) == 0);
        SwqcWriteWindow direct(MPI_FILE_NULL, 0, true, true);
        direct.append(expected.data(), 13, 0);
        direct.append(expected.data() + 13, expected.size() - 13, 13);
        direct.close();
        assert(direct.fingerprint.bytes == reference.bytes);
        assert(direct.fingerprint.samples == reference.samples);
        assert(direct.fingerprint.sum64 == reference.sum64);
        assert(direct.fingerprint.xor64 == reference.xor64);
        assert(direct.fingerprint.end_offset == reference.end_offset);
        std::vector<SwqcOutputSpan> spans;
        for (size_t p = 0; p < expected.size();) {
            size_t n = std::min<size_t>(1379, expected.size() - p);
            spans.push_back({expected.data() + p, n, int64_t(p)});
            p += n;
        }
        SwqcOutputFingerprint batched;
        assert(swqc_fingerprint_batch(spans.data(), spans.size(), batched));
        assert(batched.bytes == reference.bytes && batched.samples == reference.samples);
        assert(batched.sum64 == reference.sum64 && batched.xor64 == reference.xor64);
        assert(batched.end_offset == reference.end_offset);
        assert(swqc_fingerprint_batch(NULL, 0, batched));
        SwqcOutputSpan invalid = {"x", 1, INT64_MAX};
        assert(!swqc_fingerprint_batch(&invalid, 1, batched));
        if (helpers) swqc_host_io_destroy();
    }
    for (bool discard : {false, true}) {
        for (size_t capacity : {size_t(0), size_t(7), size_t(8192)}) {
            char path[] = "/tmp/swqc-discard-XXXXXX";
            int fd = mkstemp(path);
            assert(fd >= 0);
            close(fd);
            MPI_File file;
            assert(MPI_File_open(MPI_COMM_SELF, path, MPI_MODE_RDWR, MPI_INFO_NULL, &file) == MPI_SUCCESS);
            SwqcWriteWindow writer(file, capacity, discard, true);
            for (size_t p = 0; p < expected.size();) {
                size_t n = std::min<size_t>(1379, expected.size() - p);
                writer.append(expected.data() + p, n, p);
                p += n;
            }
            writer.close();
            assert(writer.bytes == expected.size());
            assert(writer.fingerprint.sum64 == reference.sum64);
            assert(writer.fingerprint.xor64 == reference.xor64);
            MPI_Offset size;
            assert(MPI_File_get_size(file, &size) == MPI_SUCCESS);
            assert(size == (discard ? 0 : (MPI_Offset)expected.size()));
            if (discard) assert(writer.io_s == 0);
            else {
                std::vector<char> actual(expected.size());
                assert(MPI_File_read_at(file, 0, actual.data(), actual.size(), MPI_BYTE, MPI_STATUS_IGNORE) == MPI_SUCCESS);
                assert(std::string(actual.begin(), actual.end()) == expected);
            }
            MPI_File_close(&file);
            unlink(path);
        }
    }
    MPI_Finalize();
    puts("PASS: discard writes nothing; exact real writes; position sampling invariant across 1/6/32/192 ranks, pieces and 64-bit offsets");
}
