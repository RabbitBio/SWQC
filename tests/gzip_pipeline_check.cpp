// Run through the scheduler on Sunway; no CPE application is launched as a child.
#include <mpi.h>
#include <zlib.h>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

static void require(bool yes, const char *message) {
    if (!yes) throw std::runtime_error(message);
}
static void prepare(const char *source, const char *prefix, size_t reads) {
    FILE *in = fopen(source, "rb");
    std::string raw = std::string(prefix) + ".fastq", zip = raw + ".gz";
    FILE *out = fopen(raw.c_str(), "wbx");
    gzFile gz = gzopen(zip.c_str(), "wb6x");
    require(in && out && gz, "prepare open failed");
    unsigned char buffer[65536];
    size_t lines = 0, bytes = 0;
    while (lines < reads * 4) {
        size_t got = fread(buffer, 1, sizeof(buffer), in), used = 0;
        require(got > 0, "input shorter than requested sample");
        for (; used < got; ++used)
            if (buffer[used] == '\n' && ++lines == reads * 4) { ++used; break; }
        require(fwrite(buffer, 1, used, out) == used, "prepare plain write failed");
        require(gzwrite(gz, buffer, used) == (int)used, "prepare gzip write failed");
        bytes += used;
    }
    fclose(in);
    require(fclose(out) == 0 && gzclose(gz) == Z_OK, "prepare close failed");
    printf("PREPARE_PASS prefix=%s reads=%zu bytes=%zu\n", prefix, reads, bytes);
}
static void compare(const char *reference, const char *candidate) {
    // gzread transparently accepts plain FASTQ as well as standard gzip.
    gzFile a = gzopen(reference, "rb"), b = gzopen(candidate, "rb");
    require(a && b, "compare open failed");
    unsigned char left[65536], right[65536];
    uint64_t bytes = 0, lines = 0;
    uint32_t crc = 0;
    for (;;) {
        int n = gzread(a, left, sizeof(left)), m = gzread(b, right, sizeof(right));
        require(n >= 0 && m >= 0, "gzip CRC/decode/read error");
        require(n == m && memcmp(left, right, n) == 0, "FULL_BYTE_COMPARE_FAILED");
        if (!n) break;
        bytes += n;
        for (int i = 0; i < n; ++i) lines += left[i] == '\n';
        crc = crc32(crc, left, n);
    }
    require(gzclose(a) == Z_OK && gzclose(b) == Z_OK, "gzip close/CRC failure");
    require(lines % 4 == 0, "output line count is not FASTQ");
    printf("FULL_BYTE_COMPARE_PASS reference=%s candidate=%s bytes=%llu records=%llu crc32=%08x\n",
        reference, candidate, (unsigned long long)bytes, (unsigned long long)(lines / 4), crc);
}
int main(int argc, char **argv) {
    MPI_Init(&argc, &argv);
    int result = 0;
    try {
        if (argc == 5 && !strcmp(argv[1], "prepare"))
            prepare(argv[2], argv[3], strtoull(argv[4], nullptr, 10));
        else if (argc == 4 && !strcmp(argv[1], "compare")) compare(argv[2], argv[3]);
        else if (argc == 6 && !strcmp(argv[1], "compare-pair")) {
            compare(argv[2], argv[3]);
            compare(argv[4], argv[5]);
        }
        else throw std::runtime_error("usage: prepare INPUT PREFIX READS | compare REF CANDIDATE | compare-pair REF1 OUT1 REF2 OUT2");
    } catch (const std::exception &e) {
        fprintf(stderr, "GZIP_PIPELINE_CHECK_FAILED: %s\n", e.what()); result = 1;
    }
    MPI_Finalize();
    return result;
}
