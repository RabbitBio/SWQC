#include "gzip_pipeline.h"
#include "globalMutex.h"
#include "serial_stages.h"
#include "Reference.h"
#include "swqc_fastq_record.h"
#include <cassert>
#include <cstring>
#include <fstream>
#include <stdexcept>
#include <thread>
#include <unistd.h>
#include <zlib.h>

extern "C" void slave_swqc_codec_worker(void *) {}
extern "C" void swqc_runtime_run(void *, void *arg) {
    auto *batch = static_cast<swqc_codec_batch *>(arg);
    for (unsigned i = 0; i < batch->count; ++i) swqc_codec_execute(batch->jobs + i);
}
extern "C" void swqc_runtime_run_profile(void *entry, void *arg, double *run, double *wait) {
    swqc_runtime_run(entry, arg);
    if (run) *run = 0;
    if (wait) *wait = 0;
}
static size_t consume(rabbit::fq::FastqDataChunk *part, std::string &output) {
    size_t pos = 0, count = 0;
    neoReference ref;
    for (;;) {
        int status = swqc_fastq_record(part->data.Pointer(), part->size + 1, pos, ref);
        if (status < 0) throw std::runtime_error("invalid FASTQ in consumer");
        if (!status) break;
        const char *data = reinterpret_cast<const char *>(ref.base);
        output.append(data + ref.pname, ref.lname); output += '\n';
        output.append(data + ref.pseq, ref.lseq); output += '\n';
        output.append(data + ref.pstrand, ref.lstrand); output += '\n';
        output.append(data + ref.pqual, ref.lqual); output += '\n';
        ++count;
    }
    return count;
}
int main() {
    char dir[] = "/tmp/swqc-gzip-pair-XXXXXX";
    assert(mkdtemp(dir));
    std::string a = std::string(dir) + "/R1.fastq.gz", b = std::string(dir) + "/R2.fastq";
    std::string expect[2];
    for (int i = 0; i < 20000; ++i) {
        for (int m = 0; m < 2; ++m) {
            size_t length = 75 + ((i + m) % 80);
            expect[m] += "@pair" + std::to_string(i) + (m ? "/2\n" : "/1\n") +
                std::string(length, m ? 'C' : 'A') + "\n+\n" + std::string(length, 'J') + "\n";
        }
    }
    gzFile gz = gzopen(a.c_str(), "wb");
    assert(gz && gzwrite(gz, expect[0].data(), expect[0].size()) == (int)expect[0].size());
    assert(gzclose(gz) == Z_OK);
    { std::ofstream out(b); out << expect[1]; }
    rabbit::fq::FastqDataPool pool(4, BLOCK_SIZE);
    auto verify = [&]() {
        SwqcGzipPairReader input(a, b);
        std::string actual[2];
        size_t chunks = 0;
        while (auto *p = input.next(&pool)) {
            rabbit::fq::FastqDataChunk *parts[] = {p->left_part, p->right_part};
            size_t count[2] = {0, 0};
            for (int m = 0; m < 2; ++m) {
                count[m] = consume(parts[m], actual[m]);
                pool.Release(parts[m]);
            }
            assert(count[0] == count[1]);
            delete p; ++chunks;
        }
        assert(chunks > 1 && actual[0] == expect[0] && actual[1] == expect[1]);
    };
    auto verify_serial = [&]() {
        SwqcGzipPairReader input(a, b);
        SwqcSerialStages stages;
        stages.enable();
        std::vector<rabbit::fq::FastqDataPairChunk *> batch;
        std::string actual[2];
        std::thread reader([&] {
            bool eof = false;
            while (stages.reader()) {
                batch.clear();
                for (int i = 0; i < 2 && !eof; ++i) {
                    auto *p = input.next(&pool);
                    if (p) batch.push_back(p);
                    else eof = true;
                }
                stages.read_done();
            }
        });
        std::thread processor([&] {
            while (stages.processor()) {
                stages.processor_ready();
                for (auto *p : batch) {
                    rabbit::fq::FastqDataChunk *parts[] = {p->left_part, p->right_part};
                    for (int m = 0; m < 2; ++m)
                        consume(parts[m], actual[m]);
                }
                stages.processed(batch.empty());
            }
        });
        std::thread writer([&] {
            while (stages.writer()) {
                for (auto *p : batch) {
                    pool.Release(p->left_part); pool.Release(p->right_part); delete p;
                }
                stages.written();
            }
        });
        reader.join(); processor.join(); writer.join();
        assert(actual[0] == expect[0] && actual[1] == expect[1]);
        stages.report(0);
    };
    verify();
    verify_serial();
    setenv("SWQC_PRELOAD_INPUT", "1", 1);
    setenv("SWQC_PRELOAD_LIMIT_MIB", "32", 1);
    verify();
    verify_serial();
    setenv("SWQC_PRELOAD_LIMIT_MIB", "1", 1);
    bool limited = false;
    try { SwqcGzipPairReader input(a, b); }
    catch (const std::runtime_error &) { limited = true; }
    assert(limited);
    setenv("SWQC_PRELOAD_LIMIT_MIB", "32", 1);
    std::string crlf;
    for (char c : expect[0]) {
        if (c == '\n') crlf += '\r';
        crlf += c;
    }
    crlf.pop_back();
    gz = gzopen(a.c_str(), "wb");
    assert(gz && gzwrite(gz, crlf.data(), crlf.size()) == (int)crlf.size());
    assert(gzclose(gz) == Z_OK);
    { std::ofstream out(b); out << expect[1].substr(0, expect[1].size() - 1); }
    verify();
    { std::ofstream out(b); out << expect[1]; }
    { std::ofstream out(b, std::ios::app); out << "@extra/2\nAC\n+\nJJ\n"; }
    bool rejected = false;
    try {
        SwqcGzipPairReader input(a, b);
        while (auto *p = input.next(&pool)) {
            pool.Release(p->left_part); pool.Release(p->right_part); delete p;
        }
    } catch (const std::runtime_error &) { rejected = true; }
    assert(rejected);
    assert(pool.partNum == 0);
    // Content validation belongs to the exact parser used by the CPE, not the
    // newline-only MPE splitter. None of these may reach QC as a valid record.
    for (std::string bad : {"name\nAC\n+\nJJ\n", "@name\nAC\n-\nJJ\n",
                           "@name\nAC\n+\nJ\n", "@name\n\n+\n\n",
                           "@name\nAC\n+\nJJ", "@name\nAC\n"}) {
        size_t pos = 0;
        neoReference ref;
        assert(swqc_fastq_record(reinterpret_cast<unsigned char *>(&bad[0]), bad.size(), pos, ref) < 0);
    }
    // Highly unequal mate lengths force complete surplus records across chunks.
    expect[0].clear(); expect[1].clear();
    for (int i = 0; i < 25000; ++i) {
        for (int m = 0; m < 2; ++m) {
            size_t n = m ? 300 : 10;
            expect[m] += "@var" + std::to_string(i) + "\n" + std::string(n, 'A') +
                         "\n+\n" + std::string(n, '@') + "\n";
        }
    }
    gz = gzopen(a.c_str(), "wb");
    assert(gz && gzwrite(gz, expect[0].data(), expect[0].size()) == (int)expect[0].size());
    assert(gzclose(gz) == Z_OK);
    { std::ofstream out(b); out << expect[1]; }
    verify(); verify_serial();
    // Diverse data spans multiple decode batches with two workers. Retain all
    // chunks past reader destruction: QC must still see valid leased storage.
    std::string both = std::string(dir) + "/R2.fastq.gz";
    expect[0].clear(); expect[1].clear();
    uint32_t random = 17;
    for (int i = 0; i < 30000; ++i) {
        for (int m = 0; m < 2; ++m) {
            std::string seq, qual;
            for (int j = 0; j < 80 + (i * 7 + m * 17) % 170; ++j) {
                random = random * 1664525U + 1013904223U;
                seq += "ACGT"[random >> 30];
                qual += char('!' + (random >> 23) % 41);
            }
            expect[m] += "@lease" + std::to_string(i) + "\n" + seq + "\n+\n" + qual + "\n";
        }
    }
    for (int m = 0; m < 2; ++m) {
        gz = gzopen((m ? both : a).c_str(), "wb");
        assert(gz && gzwrite(gz, expect[m].data(), expect[m].size()) == (int)expect[m].size());
        assert(gzclose(gz) == Z_OK);
    }
    for (const char *mode : {"0", "1"}) {
        setenv("SWQC_GZIP_ZERO_COPY", mode, 1);
        rabbit::fq::FastqDataPool held_pool(2048, 0);
        std::vector<rabbit::fq::FastqDataPairChunk *> held;
        {
            SwqcGzipPairReader input(a, both);
            while (auto *p = input.next(&held_pool)) held.push_back(p);
        }
        std::string actual[2];
        for (auto *p : held) {
            assert(consume(p->left_part, actual[0]) == consume(p->right_part, actual[1]));
            held_pool.Release(p->left_part); held_pool.Release(p->right_part); delete p;
        }
        assert(actual[0] == expect[0] && actual[1] == expect[1] && held_pool.partNum == 0);
    }
    unlink(both.c_str());
    // Empty streams and a partial last record must release every acquired block.
    gz = gzopen(a.c_str(), "wb"); assert(gz && gzclose(gz) == Z_OK);
    { std::ofstream out(b); }
    { SwqcGzipPairReader input(a, b); assert(!input.next(&pool)); assert(!input.next(&pool)); }
    assert(pool.partNum == 0);
    gz = gzopen(a.c_str(), "wb");
    assert(gz && gzwrite(gz, "@truncated\nAC\n", 14) == 14 && gzclose(gz) == Z_OK);
    rejected = false;
    try { SwqcGzipPairReader input(a, b); input.next(&pool); }
    catch (const std::runtime_error &) { rejected = true; }
    assert(rejected && pool.partNum == 0);
    unlink(a.c_str()); unlink(b.c_str()); rmdir(dir);
    puts("GZIP_PAIR_TEST_PASS: bulk chunks, CPE parser, serial stages, unequal mates, CRLF, EOF, malformed input, pool release");
}
