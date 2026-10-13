#include "ordered_input.h"
#include <cassert>
#include <fstream>
#include <map>
#include <sstream>
#include <set>
#include <unistd.h>

int main() {
    char folder[] = "/tmp/swqc-bias-XXXXXX";
    assert(mkdtemp(folder));
    std::string paths[2] = {std::string(folder) + "/r1.fq", std::string(folder) + "/r2.fq"};
    const int pairs = 513, copies = 5;
    std::multiset<std::string> original;
    std::ofstream f0(paths[0]), f1(paths[1]);
    for (int i = 0; i < pairs; ++i) {
        std::string seq(48, 'A');
        for (int j = 0; j < 12; ++j) seq[j] = "ACGT"[(i >> (j % 5)) & 3];
        std::string qual(48, char(33 + i % 41));
        std::string mate(48, i % 2 ? 'T' : 'G');
        f0 << "@r" << i << "/1\n" << seq << "\n+\n" << qual << '\n';
        f1 << "@r" << i << "/2\n" << mate << "\n+\n" << qual << '\n';
        original.insert(seq + qual + mate + qual);
    }
    f0.close(); f1.close();
    for (std::string layout : {"source", "qc_blocks"}) {
        SwqcReplayInput sample(paths[0], paths[1], pairs, copies, 512, 1 << 20, layout, 8, 7);
        std::string expected[2];
        for (uint64_t id = 0; id < sample.chunks(); ++id) {
            const auto &c = sample.chunk(id);
            std::vector<char> left(c.data[0].size()+1), right(c.data[1].size()+1);
            sample.materialize(id, left.data(), right.data());
            expected[0].append(left.data(), c.data[0].size());
            expected[1].append(right.data(), c.data[1].size());
            if (id % sample.seed_chunks() + 1 < sample.seed_chunks())
                assert(c.names.size() == 4 && c.data[0].size() == 500 && c.data[1].size() == 500);
        }
        std::istringstream r0(expected[0]), r1(expected[1]);
        for (int copy = 0; copy < copies; ++copy) {
            std::multiset<std::string> actual;
            for (int i = 0; i < pairs; ++i) {
                std::string r[2][4];
                for (int j = 0; j < 4; ++j) { assert(std::getline(r0,r[0][j])); assert(std::getline(r1,r[1][j])); }
                for (int m = 0; m < 2; ++m) {
                    char name[80]; snprintf(name,sizeof name,"@SWQC_%016llx/%d",(unsigned long long)(copy*pairs+i),m+1);
                    assert(r[m][0] == name && r[m][2] == "+");
                }
                actual.insert(r[0][1]+r[0][3]+r[1][1]+r[1][3]);
            }
            assert(actual == original);
        }
        for (int workers : {1,6}) {
            if (workers == 6) assert(swqc_host_memory_init(6) == 0);
            for (int ranks : {1,2,6,8,12,24,48}) {
                std::map<uint64_t,std::string> out[2];
                for (int rank = 0; rank < ranks; ++rank) {
                    rabbit::fq::FastqDataPool pool(SWQC_CPE_COUNT*2+4,0);
                    OrderedPairReader reader(paths[0],paths[1],rank,ranks,512,1025);
                    reader.replay(paths[0],paths[1],pairs,copies,1<<20,layout,8,7);
                    uint64_t ordinal=0;
                    while (auto *p = reader.next(&pool)) {
                        uint64_t id=(ordinal/SWQC_CPE_COUNT*ranks+rank)*SWQC_CPE_COUNT+ordinal%SWQC_CPE_COUNT;
                        ++ordinal;
                        rabbit::fq::FastqDataChunk *parts[2]={p->left_part,p->right_part};
                        for (int m=0;m<2;++m) {
                            assert(out[m].emplace(id,std::string((char*)parts[m]->data.Pointer(),parts[m]->size+1)).second);
                            pool.Release(parts[m]);
                        }
                        delete p;
                    }
                }
                for (int m=0;m<2;++m) {
                    std::string actual;
                    for (const auto &entry:out[m]) actual+=entry.second;
                    assert(actual==expected[m]);
                }
            }
            if (workers == 6) swqc_host_io_destroy();
        }
        if (layout == "qc_blocks") {
            SwqcReplayInput changed(paths[0],paths[1],pairs,copies,512,1<<20,layout,8,8);
            bool different=false;
            for (uint64_t id=0;id<sample.chunks();++id)
                different |= sample.chunk(id).data[0]!=changed.chunk(id).data[0];
            assert(different);
        }
    }
    { std::ofstream f(paths[0],std::ios::app); f << "@last/1\nAAA\n+\nIII\n"; }
    { std::ofstream f(paths[1],std::ios::app); f << "@last/2\nTTT\n+\nIII\n"; }
    bool rejected=false;
    try { SwqcReplayInput sample(paths[0],paths[1],pairs+1,1,512,1<<20,"qc_blocks"); }
    catch (const std::exception &) { rejected=true; }
    assert(rejected);
    for (const auto &p:paths) assert(unlink(p.c_str())==0);
    assert(rmdir(folder)==0);
    puts("PASS: biased replay preserves real paired payloads, equal full chunks, unique ordered names, all rank/CPE layouts");
}
