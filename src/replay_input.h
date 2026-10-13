#ifndef SWQC_REPLAY_INPUT_H
#define SWQC_REPLAY_INPUT_H
#include <array>
#include <algorithm>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

/* Diagnostic input: one immutable paired sample, replayed in a global order
 * independent of rank count. Only headers are replaced; bases/qualities stay
 * unchanged. Materialization is timed, sample loading is not. */
class SwqcReplayInput {
public:
    struct Chunk {
        std::string data[2];
        std::vector<std::array<size_t, 2> > names;
        uint64_t first_pair = 0;
        uint64_t bases = 0, low_quality_bases = 0, anchor_pairs = 0;
    };
private:
    std::vector<Chunk> chunks_;
    uint64_t pairs_ = 0, copies_ = 0, bytes_ = 0;
    std::string layout_;
    size_t block_chunks_ = 0, blocks_ = 0, records_per_chunk_ = 0;
    std::vector<uint32_t> block_order_;

    static uint64_t random64(uint64_t &state) {
        uint64_t x = (state += UINT64_C(0x9e3779b97f4a7c15));
        x = (x ^ (x >> 30)) * UINT64_C(0xbf58476d1ce4e5b9);
        x = (x ^ (x >> 27)) * UINT64_C(0x94d049bb133111eb);
        return x ^ (x >> 31);
    }
    static uint64_t feature_key(const std::string (&r)[2][4]) {
        // A cheap loading-time proxy, not a QC decision or a timing oracle.
        std::string anchor;
        for (size_t i = 0; i < 24 && i < r[1][1].size(); ++i) {
            char c = r[1][1][r[1][1].size() - 1 - i];
            anchor += c == 'A' ? 'T' : c == 'T' ? 'A' :
                      c == 'C' ? 'G' : c == 'G' ? 'C' : '?';
        }
        size_t offset = anchor.size() == 24 ? r[0][1].find(anchor) : std::string::npos;
        uint32_t low = 0;
        for (int m = 0; m < 2; ++m)
            for (unsigned char q : r[m][3]) low += q < 53; // Phred33 Q20
        uint64_t band = offset == std::string::npos ? UINT32_MAX : offset;
        return (band << 32) | low;
    }
    void arrange(const std::vector<uint64_t> &keys, size_t chunk_bytes,
                 size_t limit_bytes, size_t used, uint64_t seed) {
        std::vector<uint64_t> order(pairs_);
        for (uint64_t i = 0; i < pairs_; ++i) order[i] = i;
        std::sort(order.begin(), order.end(), [&](uint64_t a, uint64_t b) {
            return keys[a] != keys[b] ? keys[a] < keys[b] : a < b;
        });
        std::vector<Chunk> packed;
        for (uint64_t i = 0; i < pairs_; ++i) {
            uint64_t src_id = order[i];
            const Chunk &src = chunks_[src_id / records_per_chunk_];
            size_t j = src_id % records_per_chunk_;
            if (i % records_per_chunk_ == 0) {
                packed.push_back(Chunk());
                packed.back().first_pair = i;
                for (int m = 0; m < 2; ++m) packed.back().data[m].reserve(chunk_bytes);
            }
            Chunk &dst = packed.back();
            std::array<size_t, 2> names;
            for (int m = 0; m < 2; ++m) {
                size_t begin = src.names[j][m] - 6;
                size_t end = j + 1 < src.names.size() ? src.names[j + 1][m] - 6 : src.data[m].size();
                names[m] = dst.data[m].size() + 6;
                dst.data[m].append(src.data[m], begin, end - begin);
                dst.bases += (end - begin - 29) / 2;
            }
            dst.names.push_back(names);
            dst.low_quality_bases += uint32_t(keys[src_id]);
            dst.anchor_pairs += (keys[src_id] >> 32) != UINT32_MAX;
        }
        chunks_.swap(packed);
        blocks_ = (pairs_ / records_per_chunk_) / block_chunks_;
        if (blocks_ > UINT32_MAX || (blocks_ && copies_ > (limit_bytes - used) / sizeof(uint32_t) / blocks_))
            throw std::runtime_error("replay permutation exceeds memory limit");
        block_order_.resize(blocks_ * copies_);
        for (uint64_t copy = 0; copy < copies_; ++copy) {
            uint64_t state = seed ^ (copy * UINT64_C(0xd1b54a32d192ed03));
            size_t base = copy * blocks_;
            for (size_t i = 0; i < blocks_; ++i) block_order_[base + i] = i;
            for (size_t i = blocks_; i > 1; --i)
                std::swap(block_order_[base + i - 1], block_order_[base + random64(state) % i]);
        }
    }
    static std::string name_key(const std::string &s) {
        std::string k = s.substr(0, s.find_first_of(" \t"));
        if (k.size() > 2 && k[k.size()-2] == '/' &&
            (k.back() == '1' || k.back() == '2')) k.resize(k.size()-2);
        return k;
    }
    static void read_record(std::ifstream &in, std::string (&r)[4]) {
        for (int i = 0; i < 4; ++i) {
            if (!std::getline(in, r[i])) throw std::runtime_error("short replay sample");
            if (!r[i].empty() && r[i].back() == '\r') r[i].pop_back();
        }
        if (r[0].empty() || r[0][0] != '@' || r[1].empty() ||
            r[2].empty() || r[2][0] != '+' || r[1].size() != r[3].size())
            throw std::runtime_error("replay requires four-line FASTQ");
    }
public:
    SwqcReplayInput(const std::string &r1, const std::string &r2,
                    uint64_t pairs, uint64_t copies, size_t chunk_bytes,
                    size_t limit_bytes, const std::string &layout = "source",
                    size_t block_chunks = 32, uint64_t seed = 1)
        : copies_(copies), layout_(layout), block_chunks_(block_chunks) {
        if (!pairs || !copies || pairs > UINT64_MAX / copies)
            throw std::runtime_error("invalid replay pair/copy count");
        if ((layout != "source" && layout != "qc_blocks") || !block_chunks)
            throw std::runtime_error("invalid replay layout/block size");
        std::ifstream inputs[2];
        inputs[0].open(r1); inputs[1].open(r2);
        if (!inputs[0] || !inputs[1]) throw std::runtime_error("cannot open replay sample");
        size_t used = 0;
        size_t lengths[2] = {0, 0};
        std::vector<uint64_t> keys;
        for (; pairs_ < pairs; ++pairs_) {
            std::string r[2][4];
            read_record(inputs[0], r[0]); read_record(inputs[1], r[1]);
            if (name_key(r[0][0]) != name_key(r[1][0]))
                throw std::runtime_error("replay mate names do not match");
            const size_t n[2] = {29 + r[0][1].size() + r[0][3].size(),
                                 29 + r[1][1].size() + r[1][3].size()};
            if (n[0] > chunk_bytes || n[1] > chunk_bytes)
                throw std::runtime_error("replay record exceeds chunk size");
            if (pairs_ == 0) {
                lengths[0] = n[0]; lengths[1] = n[1];
                records_per_chunk_ = chunk_bytes / std::max(n[0], n[1]);
            }
            if (layout != "source" && (n[0] != lengths[0] || n[1] != lengths[1]))
                throw std::runtime_error("qc_blocks requires fixed read lengths for equal-size chunks");
            size_t cost = n[0] + n[1] + sizeof(std::array<size_t, 2>);
            if (cost > limit_bytes - used)
                throw std::runtime_error("replay sample exceeds payload/record-index limit");
            used += cost;
            if (chunks_.empty() || chunks_.back().data[0].size() + n[0] > chunk_bytes ||
                                  chunks_.back().data[1].size() + n[1] > chunk_bytes) {
                chunks_.push_back(Chunk());
                chunks_.back().first_pair = pairs_;
            }
            Chunk &c = chunks_.back();
            uint64_t key = feature_key(r);
            if (layout != "source") keys.push_back(key);
            c.bases += r[0][1].size() + r[1][1].size();
            c.low_quality_bases += uint32_t(key);
            c.anchor_pairs += (key >> 32) != UINT32_MAX;
            std::array<size_t, 2> positions;
            for (int m = 0; m < 2; ++m) {
                positions[m] = c.data[m].size() + 6;
                c.data[m] += m ? "@SWQC_0000000000000000/2\n" : "@SWQC_0000000000000000/1\n";
                c.data[m] += r[m][1] + "\n+\n" + r[m][3] + "\n";
                bytes_ += n[m];
            }
            c.names.push_back(positions);
        }
        if (chunks_.size() > uint64_t(INT64_MAX) / copies || bytes_ > uint64_t(INT64_MAX) / copies)
            throw std::runtime_error("replay logical size overflow");
        if (layout != "source") arrange(keys, chunk_bytes, limit_bytes, used, seed);
    }
    uint64_t pairs() const { return pairs_; }
    uint64_t copies() const { return copies_; }
    uint64_t chunks() const { return chunks_.size() * copies_; }
    uint64_t seed_chunks() const { return chunks_.size(); }
    uint64_t bytes() const { return bytes_; }
    const std::string &layout() const { return layout_; }
    size_t block_chunks() const { return block_chunks_; }
    const Chunk &chunk(uint64_t id) const {
        if (id >= chunks()) throw std::out_of_range("replay chunk ID");
        size_t slot = id % chunks_.size();
        if (slot < blocks_ * block_chunks_)
            slot = block_order_[(id / chunks_.size()) * blocks_ + slot / block_chunks_] * block_chunks_ + slot % block_chunks_;
        return chunks_.at(slot);
    }
    void materialize(uint64_t id, char *left, char *right) const {
        const Chunk &c = chunk(id);
        char *dest[2] = {left, right};
        uint64_t first = (id / chunks_.size()) * pairs_ +
            (layout_ == "source" ? c.first_pair : (id % chunks_.size()) * records_per_chunk_);
        static const char hex[] = "0123456789abcdef";
        for (int m = 0; m < 2; ++m) {
            memcpy(dest[m], c.data[m].data(), c.data[m].size());
            for (size_t j = 0; j < c.names.size(); ++j) {
                uint64_t value = first + j;
                char *name = dest[m] + c.names[j][m];
                for (int k = 15; k >= 0; --k) { name[k] = hex[value & 15]; value >>= 4; }
            }
            dest[m][c.data[m].size()] = 0;
        }
    }
};
#endif
