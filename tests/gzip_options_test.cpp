#include "gzip_pipeline.h"
#include <cassert>
#include <cstdio>

int main() {
    assert(!swqc_select_standard_gzip(false, false, {"r1.fastq", "r2.fastq", "", ""}));
    assert(!swqc_select_standard_gzip(false, false, {"dir.gz/r1.fastq", "r2.fastq"}));
    assert(swqc_select_standard_gzip(false, false, {"r1.fastq.gz", "r2.fastq.gz"}));
    assert(swqc_select_standard_gzip(false, false, {"r1.fastq", "r2.fastq.gz"}));
    assert(swqc_select_standard_gzip(false, false, {"r1.fastq", "r2.fastq", "o1.fastq.gz", "o2.fastq.gz"}));
    assert(swqc_select_standard_gzip(false, false, {"r1.fastq", "r2.fastq", "o1.fastq", "o2.fastq.gz"}));
    assert(swqc_select_standard_gzip(true, false, {"r1.fastq", "r2.fastq"}));
    assert(!swqc_select_standard_gzip(false, true, {"legacy1.fastq.gz", "legacy2.fastq.gz"}));
    bool rejected = false;
    try { swqc_select_standard_gzip(true, true, {"r1.fastq.gz", "r2.fastq.gz"}); }
    catch (const std::invalid_argument &) { rejected = true; }
    assert(rejected);
    swqc_validate_gzip_paths("r1.gz", "r2.gz", "o1.fastq", "o2.fastq");
    swqc_validate_gzip_paths("r1.fastq", "r2.fastq", "o1.gz", "o2.gz");
    swqc_validate_gzip_paths("r1.gz", "r2.gz", "", "");
    const char *invalid[][4] = {
        {"", "r2.gz", "", ""}, {"r1.gz", "", "", ""},
        {"r1.gz", "r2.fastq", "", ""},
        {"r1.gz", "r2.gz", "o1.gz", "o2.fastq"},
        {"r1.gz", "r2.gz", "o1.fastq", ""},
        {"r1.gz", "r2.gz", "", "o2.fastq"},
        {"/dev/stdin", "r2.fastq", "o1.gz", "o2.gz"},
        {"r1.gz", "r2.gz", "/dev/stdout", "o2.fastq"},
        {"-", "r2.fastq", "o1.gz", "o2.gz"}
    };
    for (const auto &paths : invalid) {
        rejected = false;
        try { swqc_validate_gzip_paths(paths[0], paths[1], paths[2], paths[3]); }
        catch (const std::invalid_argument &) { rejected = true; }
        assert(rejected);
    }
    puts("GZIP_OPTIONS_TEST_PASS: automatic selection, legacy opt-in, conflicting flags, paired paths, stdin/stdout rejection");
}
