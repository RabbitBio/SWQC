#include "ordered_input.h"
#include <cassert>
#include <fstream>
#include <map>
#include <sstream>
#include <unistd.h>

int main() {
    char folder[] = "/tmp/swqc-replay-XXXXXX";
    assert(mkdtemp(folder));
    std::string paths[2] = {std::string(folder) + "/r1.fq", std::string(folder) + "/r2.fq"};
    for (int m = 0; m < 2; ++m) {
        std::ofstream f(paths[m]);
        for (int i = 0; i < 137; ++i) {
            int n = 32 + i % 80;
            f << "@r" << i << '/' << m+1 << " comment\r\n" << std::string(n, m ? 'T' : 'A')
              << (i % 2 ? "\r\n+source-name\r\n" : "\r\n+\r\n")
              << std::string(n, i % 2 ? '@' : '+') << "\r\n";
        }
    }
    for (size_t bytes : {size_t(512), size_t(4096)}) {
        SwqcReplayInput sample(paths[0], paths[1], 137, 7, bytes, 1 << 20);
        std::string expected[2];
        for (int id = 0; id < 137*7; ++id) {
            for (int m = 0; m < 2; ++m) {
                char h[80]; snprintf(h, sizeof h, "@SWQC_%016llx/%d\n", (unsigned long long)id, m+1);
                int n = 32 + id % 137 % 80;
                expected[m] += std::string(h) + std::string(n, m ? 'T' : 'A') + "\n+\n" +
                               std::string(n, id % 137 % 2 ? '@' : '+') + "\n";
            }
        }
        assert(sample.bytes() * 7 == expected[0].size() + expected[1].size());
        for (int workers : {1, 6}) {
            if (workers == 6) assert(swqc_host_memory_init(6) == 0);
            for (int ranks : {1, 2, 4, 6, 8, 12, 24, 48}) {
                std::map<uint64_t, std::string> output[2];
                for (int rank = 0; rank < ranks; ++rank) {
                    rabbit::fq::FastqDataPool pool(SWQC_CPE_COUNT * 2 + 4, 0);
                    OrderedPairReader r(paths[0], paths[1], rank, ranks, bytes, bytes*2+1);
                    r.replay(paths[0], paths[1], 137, 7, 1 << 20);
                    uint64_t ordinal = 0;
                    while (auto *p = r.next(&pool)) {
                        uint64_t id = (ordinal / SWQC_CPE_COUNT * ranks + rank) * SWQC_CPE_COUNT
                                    + ordinal % SWQC_CPE_COUNT;
                        ++ordinal;
                        rabbit::fq::FastqDataChunk *parts[2] = {p->left_part, p->right_part};
                        for (int m = 0; m < 2; ++m) {
                            assert(output[m].emplace(id, std::string((char *)parts[m]->data.Pointer(), parts[m]->size+1)).second);
                            pool.Release(parts[m]);
                        }
                        delete p;
                    }
                }
                for (int m = 0; m < 2; ++m) {
                    std::string actual;
                    for (auto &c : output[m]) actual += c.second;
                    assert(actual == expected[m]);
                }
            }
            if (workers == 6) swqc_host_io_destroy();
        }
    }
    bool rejected = false;
    try { SwqcReplayInput s(paths[0], paths[1], 137, 20, 512, 100); }
    catch (const std::exception &) { rejected = true; }
    assert(rejected);
    for (auto &p : paths) assert(unlink(p.c_str()) == 0);
    assert(rmdir(folder) == 0);
    puts("replay coverage/order/names/qualities/limits PASS");
}
