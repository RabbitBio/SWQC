#include "state.h"
#include <cassert>
#include <climits>
#include <memory>
#include <mutex>
#include <type_traits>

std::mutex globalMutex;

static_assert(std::is_same<decltype(State::pos_qul_), int64_t *>::value,
              "position quality sums must be 64-bit");
static_assert(std::is_same<decltype(State::q20bases_), int64_t>::value &&
              std::is_same<decltype(State::q30bases_), int64_t>::value &&
              std::is_same<decltype(State::tot_bases_), int64_t>::value &&
              std::is_same<decltype(State::gc_bases_), int64_t>::value,
              "cumulative base totals must remain 64-bit");

static void large_quality_roundtrip_and_merge(CmdInfo &options)
{
    State first(&options, 3, 42, false), second(&options, 3, 42, false);
    const int64_t large = int64_t(INT_MAX) + 123;
    first.real_seq_len_ = second.real_seq_len_ = 3;
    first.lines_ = second.lines_ = 100000000;
    first.tot_bases_ = first.gc_bases_ = large;
    first.q20bases_ = first.q30bases_ = large;
    second.tot_bases_ = second.gc_bases_ = large + 1;
    second.q20bases_ = second.q30bases_ = large + 1;
    for (int i = 0; i < 3; ++i) {
        first.pos_qul_[i] = large + i;
        second.pos_qul_[i] = large + 10 + i;
        first.pos_cnt_[i * 4] = second.pos_cnt_[i * 4] = 100000000;
    }

    std::string encoded = first.ParseString();
    State decoded(encoded.data(), encoded.size(), &options, 3, 42, false);
    assert(decoded.ParseString() == encoded);
    assert(decoded.GetQ20Bases() == large && decoded.GetQ30Bases() == large);
    assert(decoded.GetTotBases() == large && decoded.GetGcBases() == large);
    for (int i = 0; i < 3; ++i) assert(decoded.GetPosQul()[i] == large + i);

    std::vector<State *> inputs = {&decoded, &second};
    std::unique_ptr<State> merged(State::MergeStates(inputs));
    for (int i = 0; i < 3; ++i) {
        assert(merged->GetPosQul()[i] == 2 * large + 10 + 2 * i);
        assert(merged->GetPosCnt()[i * 4] == 200000000);
    }
    assert(merged->GetQ20Bases() == 2 * large + 1);
    assert(merged->GetQ30Bases() == 2 * large + 1);
    assert(merged->GetTotBases() == 2 * large + 1);
    assert(merged->GetGcBases() == 2 * large + 1);
    encoded = merged->ParseString();
    State merged_decoded(encoded.data(), encoded.size(), &options, 3, 42, false);
    assert(merged_decoded.ParseString() == encoded);
    for (int i = 0; i < 3; ++i)
        assert(merged_decoded.GetPosQul()[i] == merged->GetPosQul()[i]);
}

static void quality_accumulation_and_growth(CmdInfo &options)
{
    State state(&options, 2, 42, false);
    assert(state.GetPosQul()[0] == 0 && state.GetPosQul()[1] == 0);
    state.pos_qul_[0] = int64_t(INT_MAX) - 10;
    state.pos_qul_[1] = int64_t(INT_MAX) * 4;
    unsigned char data[] = "ACGIII";
    neoReference read = {};
    read.base = data;
    read.lseq = read.lqual = 3;
    read.pqual = 3;
    // A three-base read exercises growth without the unrelated k-mer path.
    state.StateInfo(read);
    assert(state.GetPosQul()[0] == int64_t(INT_MAX) + 30);
    assert(state.GetPosQul()[1] == int64_t(INT_MAX) * 4 + 40);
    assert(state.GetPosQul()[2] == 40);
    for (int i = 3; i < state.GetMallocSeqLen(); ++i)
        assert(state.GetPosQul()[i] == 0);
    assert(state.GetQ20Bases() == 3 && state.GetQ30Bases() == 3);
    assert(state.GetTotBases() == 3 && state.GetGcBases() == 2);
}

static void ambiguous_bases(CmdInfo &options)
{
    State state(&options, 6, 42, false);
    unsigned char data[] = "NACNTNIIIIII";
    neoReference read = {};
    read.base = data;
    read.lseq = read.lqual = 6;
    read.pqual = 6;
    state.StateInfo(read);
    for (int pos = 0; pos < 6; ++pos) {
        for (int base = 0; base < 4; ++base) {
            const int expected = (pos == 1 && base == 0) ||
                                 (pos == 2 && base == 2) ||
                                 (pos == 4 && base == 1);
            assert(state.GetPosCnt()[4 * pos + base] == expected);
        }
        assert(state.GetPosQul()[pos] == 40);
    }
    assert(state.GetTotBases() == 6 && state.GetGcBases() == 1);
}

int main()
{
    CmdInfo options;
    options.do_overrepresentation_ = false;
    State empty(&options, 75, 42, false);
    std::vector<State *> inputs(1, &empty);
    State *merged = State::MergeStates(inputs);
    std::string encoded = merged->ParseString();
    State decoded(encoded.data(), encoded.size(), &options, 75, 42, false);
    assert(decoded.ParseString() == encoded);
    delete merged;
    large_quality_roundtrip_and_merge(options);
    quality_accumulation_and_growth(options);
    ambiguous_bases(options);
    puts("PASS: empty and 64-bit quality roundtrip, merge, accumulation and growth");
}
