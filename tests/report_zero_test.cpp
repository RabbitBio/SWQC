#include "../src/repoter.cpp"
#include <cassert>
#include <cfenv>
#include <unistd.h>

int main() {
    std::feclearexcept(FE_ALL_EXCEPT);
    assert(report_ratio(0, 0) == 0);
    assert(report_ratio(35, 0) == 0);
    assert(report_ratio(35, 2) == 17.5);
    assert(report_ratio(0, 2) == 0);
    assert(!(std::fetestexcept(FE_DIVBYZERO | FE_INVALID)));
    CmdInfo options;
    State pre1(&options, 200, 50, false), pre2(&options, 200, 50, true);
    State post1(&options, 200, 50, false), post2(&options, 200, 50, true);
    int insert_sizes[1024] = {};
    char path[] = "/tmp/swqc-empty-report-XXXXXX";
    int fd = mkstemp(path);
    assert(fd >= 0);
    close(fd);
    Repoter::ReportHtmlPe(path, &pre1, &pre2, &post1, &post2, "r1", "r2", 0, insert_sizes);
    Repoter::ReportHtmlSe(path, &pre1, &post1, "r1", 0);
    unlink(path);
    assert(!(std::fetestexcept(FE_DIVBYZERO | FE_INVALID)));
    puts("PASS: empty/all-N report positions produce zero without floating exceptions");
}
