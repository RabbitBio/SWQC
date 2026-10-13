#include "ordered_input.h"
#include <cassert>
#include <fstream>
#include <map>
#include <thread>
#include <queue>
#include <mutex>

int main()
{
    char folder[] = "/tmp/swqc-order-test-XXXXXX";
    assert(mkdtemp(folder));
    std::string r1 = std::string(folder) + "/r1.fastq";
    std::string r2 = std::string(folder) + "/r2.fastq";
    for (int crlf = 0; crlf < 2; ++crlf) {
        std::string expected[2];
        std::string nl = crlf ? "\r\n" : "\n";
        for (int i = 0; i < 9000; ++i) {
            for (int m = 0; m < 2; ++m) {
                int len = 30 + (i * 17) % 151;
                expected[m] += "@read" + std::to_string(i) + nl +
                    std::string(len, m ? 'T' : 'A') + nl + "+" + nl +
                    std::string(len, (i % 2) ? '@' : '+') + nl;
            }
        }
        { std::ofstream f(r1); f << expected[0]; }
        { std::ofstream f(r2); f << expected[1]; }
        for (size_t chunk : {size_t(1024), size_t(64 << 10), size_t(256 << 10), size_t(512 << 10), size_t(1 << 20)}) {
        for (size_t window : {size_t(0), size_t(8192), size_t(32768)}) {
        size_t actual_window = window ? std::max(window, chunk * 2 + 1) : 0;
        for (int mode : {0, 1, 2}) {
        bool preload = mode != 0;
        bool borrow = mode == 2;
        if (preload && !window) continue;
        for (int ranks : {1, 2, 6, 12, 48}) {
            std::map<int64_t, std::string> chunks[2];
            for (int rank = 0; rank < ranks; ++rank) {
                rabbit::fq::FastqDataPool pool(4, borrow ? 0 : 1024);
                OrderedPairReader reader(r1, r2, rank, ranks, chunk, actual_window);
                if (preload) reader.preload(2 * (expected[0].size() + expected[1].size()) + 8192, borrow);
                int64_t ordinal = 0;
                while (auto *p = reader.next(&pool)) {
                    int64_t id = (ordinal / SWQC_CPE_COUNT * ranks + rank) * SWQC_CPE_COUNT
                               + ordinal % SWQC_CPE_COUNT;
                    ++ordinal;
                    rabbit::fq::FastqDataChunk *parts[2] = {p->left_part, p->right_part};
                    for (int m = 0; m < 2; ++m) {
                        size_t bytes = parts[m]->size + (crlf ? 2 : 1);
                        assert(chunks[m].insert({id, std::string((char *)parts[m]->data.Pointer(), bytes)}).second);
                        pool.Release(parts[m]);
                    }
                    delete p;
                }
            }
            for (int m = 0; m < 2; ++m) {
                std::string result;
                for (auto &p : chunks[m]) result += p.second;
                assert(result == expected[m]);
            }
        }
        }
        }
        }
        assert(swqc_host_memory_init(6) == 0);
        {
            rabbit::fq::FastqDataPool pool(4, 1024);
            OrderedPairReader reader(r1, r2, 0, 1, 1024, 8192);
            reader.preload(2 * (expected[0].size() + expected[1].size()) + 8192, true);
            { std::ofstream f(r1); }
            { std::ofstream f(r2); }
            std::string actual[2];
            while (auto *p = reader.next(&pool)) {
                rabbit::fq::FastqDataChunk *parts[2] = {p->left_part, p->right_part};
                for (int m = 0; m < 2; ++m) {
                    actual[m].append((char *)parts[m]->data.Pointer(), parts[m]->size + (crlf ? 2 : 1));
                    pool.Release(parts[m]);
                }
                delete p;
            }
            assert(actual[0] == expected[0] && actual[1] == expected[1]);
        }
        swqc_host_io_destroy();
    }
    { std::ofstream f(r1); }
    { std::ofstream f(r2); }
    {
        rabbit::fq::FastqDataPool pool(4, 1024);
        OrderedPairReader reader(r1, r2, 0, 6, 1024);
        assert(!reader.next(&pool));
    }
    {
        rabbit::fq::FastqDataPool pool(4, 1024);
        OrderedPairReader reader(r1, r2, 0, 6, 1024, 8192);
        reader.preload(8192);
        assert(!reader.next(&pool));
    }
    unlink(r1.c_str());
    unlink(r2.c_str());
    rmdir(folder);
    {
        unsigned char external[] = "borrowed";
        rabbit::core::Buffer view(0), owned(16);
        view.SetView(external, sizeof(external));
        view.Swap(owned);
        assert(owned.Pointer() == external);
        owned.Extend(32, true);
        assert(owned.Pointer() != external);
        assert(memcmp(owned.Pointer(), external, sizeof(external)) == 0);
        view.SetView(external, sizeof(external));
        view.ResetView();
        assert(view.Size() == 0 && view.Pointer() == NULL);
        view.Extend(64);
        assert(view.Size() == 64 && view.Pointer() != external);
    }
    printf("PASS: %d CPE stripes, disk/preload/borrow, LF/CRLF, variable lengths, quality @/+, empty input; ranks 1/2/6/12/48; preload survives source truncation; borrowed ownership/extend/swap/reset\n", SWQC_CPE_COUNT);
}
