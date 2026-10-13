#include "serial_stages.h"
#include <atomic>
#include <cassert>
#include <thread>
#include <vector>

int main() {
    SwqcSerialStages gate;
    gate.enable();
    std::atomic<int> active(0), steps(0);
    std::vector<int> order;
    auto work = [&](int phase) {
        assert(active.fetch_add(1) == 0);
        order.push_back(phase);
        std::this_thread::yield();
        ++steps;
        assert(active.fetch_sub(1) == 1);
    };
    std::thread reader([&] {
        while (gate.reader()) { work(0); gate.read_done(); }
    });
    std::thread processor([&] {
        for (int round = 0; round < 1000; ++round) {
            assert(gate.processor()); gate.processor_ready(); work(1); gate.processed(round == 999);
        }
    });
    std::thread writer([&] {
        while (gate.writer()) { work(2); gate.written(); }
    });
    reader.join(); processor.join(); writer.join();
    assert(steps == 3000);
    for (int i = 0; i < 3000; ++i) assert(order[i] == i % 3);
    gate.report(0);
    puts("PASS: strict reader/processor/writer order, exclusive execution and final wakeup");
}
