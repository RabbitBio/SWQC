#ifndef GLOBALMUTEX_H
#define GLOBALMUTEX_H

#define USE_LIBDEFLATE
//#define WRITER_USE_LIBDEFLATE
#define CONSUMER_USE_LIBDEFLATE

#include <mutex>
#define Q_lim_se 4
#define Q_lim_pe 2

#define BLOCK_SIZE (1 << 20)
#define SWAP1_SIZE (1 << 18)
#define SWAP2_SIZE (1 << 14)
#define MY_PAGE_SIZE (8192)
#define USE_CC_GZ

#define use_align_64k

extern std::mutex globalMutex;
struct Para {
    char* in_buffer;
    char* out_buffer;
    size_t *out_size;
    size_t in_size;
    int level;
};

//struct QChunkItem {
//    char* buffer[64];
//    int buffer_len[64];
//    long long file_offset[64];
//};

#include <set>
#include <map>
#include <thread>
#include <mutex>
#include <condition_variable>


std::set<int> myPool;
std::map<char*, int> myMap;
//std::mutex myMutex;
//std::condition_variable cv;



constexpr size_t my_num_buffers = 1024;
//constexpr size_t my_num_buffers2 = 64;
constexpr size_t my_buffer_size = 1024 * 1024; // 1MB
constexpr size_t my_alignment = 8192; // 8192B (8KB)

int tot_id = 0;
int tot_id2 = 0;

//alignas(my_alignment) char buffers2[my_num_buffers2][my_buffer_size];
alignas(my_alignment) char buffers[my_num_buffers][my_buffer_size];

int getChunkFromMyPool() {
	//std::unique_lock<std::mutex> lock(myMutex);
    //cv.wait(lock, []{ return !myPool.empty(); });
    while(myPool.empty()) {
        usleep(100);
    }
    int value = *myPool.begin();
    myPool.erase(value);
    //std::cout << "Thread A got value: " << value << ", set size " << myPool.size() << std::endl;
    return value;
}

void putChunkToMyPool(int value) {
    //std::lock_guard<std::mutex> lock(myMutex);
    myPool.insert(value);
    //std::cout << "Thread B inserted value: " << value <<", set size " << myPool.size() << std::endl;
    //cv.notify_one();	
}
#endif
