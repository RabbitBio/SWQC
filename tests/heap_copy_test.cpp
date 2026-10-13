#include "../slave/dma_funcs.hpp"
#include <cassert>
#include <cstdint>
#include <vector>

int main() {
    std::vector<int64_t> main(129), scratch(129);
    for (size_t i = 0; i < main.size(); ++i) main[i] = (int64_t(1) << 40) + i;
    dma_getn(main.data(), scratch.data(), 129);
    assert(main == scratch);
    scratch[3] = -1;
    dma_putn(main.data(), scratch.data(), 129);
    assert(main[3] == -1);
    dma_get(main.data(), scratch.data(), 0);
    dma_put(main.data(), scratch.data(), 0);
}
