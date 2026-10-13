#include "swqc_log.h"
#include <cassert>
#include <cstdlib>
#include <string>
#include <thread>
#include <vector>
#include <unistd.h>

static std::string contents(FILE *file)
{
    fflush(file);
    long size = ftell(file);
    rewind(file);
    std::string text(size, '\0');
    assert(fread(&text[0], 1, text.size(), file) == text.size());
    return text;
}

int main()
{
    FILE *captured = tmpfile(), *payload = tmpfile();
    assert(captured && payload);
    int saved = dup(STDERR_FILENO);
    assert(saved >= 0 && dup2(fileno(captured), STDERR_FILENO) >= 0);
    setenv("SWQC_DEFER_LOGS", "1", 1);
    char directory[] = "/tmp/swqc-log-XXXXXX";
    assert(mkdtemp(directory) && chdir(directory) == 0);
    assert(swqc_log_open_rank(3) == 0);
    swqc_log_fprintf(stdout, "preload before timing\n");
    swqc_log_begin();
    std::vector<std::thread> threads;
    for (int t = 0; t < 6; ++t)
        threads.emplace_back([t] {
            for (int i = 0; i < 100; ++i) swqc_log_fprintf(stderr, "worker=%d item=%d\n", t, i);
        });
    for (auto &thread : threads) thread.join();
    fflush(stderr);
    assert(lseek(fileno(captured), 0, SEEK_END) == 0);
    FILE *rank = fopen("swqc.rank000003.log", "r");
    assert(rank && fseek(rank, 0, SEEK_END) == 0);
    assert(contents(rank) == "preload before timing\n");
    assert(swqc_log_fprintf(payload, "%s", "payload") == 7);
    swqc_log_end();
    std::string result = contents(captured);
    for (int t = 0; t < 6; ++t)
        for (int i = 0; i < 100; ++i) {
            std::string line = "worker=" + std::to_string(t) + " item=" + std::to_string(i) + "\n";
            size_t pos = result.find(line);
            assert(pos != std::string::npos && result.find(line, pos + 1) == std::string::npos);
        }
    assert(result.find("overflow=0") != std::string::npos);
    assert(fseek(rank, 0, SEEK_END) == 0);
    assert(contents(rank) == "preload before timing\n" + result);
    fclose(rank);
    assert(contents(payload) == "payload");
    swqc_log_begin();
    swqc_log_printf("%s\n", std::string(SWQC_LOG_BUFFER_BYTES + 1, 'X').c_str());
    swqc_log_end();
    assert(contents(captured).find("overflow=1") != std::string::npos);
    assert(dup2(saved, STDERR_FILENO) >= 0);
    close(saved);
    fclose(captured);
    fclose(payload);
    unlink("swqc.rank000003.log");
    assert(chdir("/") == 0 && rmdir(directory) == 0);
    puts("PASS: deferred diagnostics, concurrent callers, payload isolation, explicit overflow");
}
