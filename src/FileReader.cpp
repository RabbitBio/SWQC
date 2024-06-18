#include "FileReader.h"
#include <cstdio>
#include <cstdlib>
#include <unistd.h>


extern "C" {
#include <athread.h>
#include <pthread.h>
    void slave_decompressfunc();
}
#define USE_LIBDEFLATE
namespace rabbit {

    FileReader::FileReader(const std::string &fileName_, bool isZipped, int64 startPos, int64 endPos, bool inMem) {
        read_in_mem = inMem;
        read_times = 0;
        start_pos = 0;
        end_pos = 0;
        t_memcpy = 0;
        t_read = 0;
        read_cnt = 0;
        read_cnt_gg = 0;
            

        //MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);

        if (ends_with(fileName_, ".gz") || isZipped) {
#ifdef USE_LIBDEFLATE
            start_line = startPos;
            end_line = endPos;
            std::ifstream iff_idx;
            iff_idx.open(fileName_, std::ios::binary | std::ios::ate);
            if (!iff_idx.is_open()) {
                std::cerr << "Failed to open file!" << std::endl;
                exit(0);
            }
            size_t file_size = iff_idx.tellg();
            size_t blocknum = 0;
            iff_idx.seekg(-static_cast<int>(sizeof(size_t)), std::ios::end);
            iff_idx.read(reinterpret_cast<char*>(&blocknum), sizeof(size_t));

            block_sizes.reserve(blocknum);
            iff_idx.seekg(-static_cast<int>((blocknum + 1) * sizeof(size_t)), std::ios::end);

            size_t block_size = 0;
            for (size_t i = 0; i < blocknum; ++i) {
                iff_idx.read(reinterpret_cast<char*>(&block_size), sizeof(size_t));
                block_sizes.push_back(block_size);
            }
            iff_idx.close();


            //std::string index_file_name = fileName_ + ".swidx";
            //int block_size;
            //iff_idx.open(index_file_name);
            //if (!iff_idx.is_open()) {
            //    fprintf(stderr, "%s do not exits\n", index_file_name.c_str());
            //    exit(0);
            //}
            //while (iff_idx >> block_size) {
            //    block_sizes.push_back(block_size);
            //}
            if(start_line == end_line) {
                end_line = block_sizes.size();
            }
            for(int i = 0; i < start_line; i++) start_pos += block_sizes[i];
            for(int i = 0; i < end_line; i++) end_pos += block_sizes[i];
            fprintf(stderr, "FileReader zip [%lld %lld] [%d %d]\n", start_pos, end_pos, start_line, end_line);
            now_block = start_line;
            block_sizes.resize(end_line);
            iff_idx_end = 0;
            for (int i = 0; i < 64; i++) {
                in_buffer[i] = new char[BLOCK_SIZE];
                out_buffer[i] = new char[BLOCK_SIZE];
            }
            to_read_buffer = new char[(64 + 8) * BLOCK_SIZE];
            buffer_tot_size = 0;
            buffer_now_pos = 0;
            buffer_now_offset = 0;
            align_end = false;
            input_file = fopen(fileName_.c_str(), "rb");
            if (input_file == NULL) {
                perror("Failed to open input file");
                exit(0);
            }
            int64 file_offset = 0;
            for (int i = 0; i < start_line; i++) {
                file_offset += block_sizes[i];
            }
            fseek(input_file, file_offset, SEEK_SET);

            buffer_test = (char*)aligned_alloc(MY_PAGE_SIZE, 8 * 1024 * 1024);
            if (!buffer_test) {
                perror("Failed to allocate aligned buffer");
                exit(1);
            }
            buffer_test_last = (char*)aligned_alloc(MY_PAGE_SIZE, 1024 * 1024);
            if (!buffer_test) {
                perror("Failed to allocate aligned buffer");
                exit(1);
            }
            buffer_last_size = 0;
            //TODO
            buffer_now_offset = file_offset;
            //fprintf(stderr, "rank%d buffer_now_offset %lld\n", my_rank, buffer_now_offset);

            fd = open(fileName_.c_str(), O_RDWR | O_DIRECT);
            //fd = open(fileName_.c_str(), O_RDWR);
            if (fd == -1) {
                perror("Failed to open file");
                exit(0);
            }

            if (read_in_mem) {
                MemDataTotSize = end_pos - start_pos;
                fprintf(stderr, "read_in_mem %s %lld\n", fileName_.c_str(), MemDataTotSize);
                MemData = new byte[MemDataTotSize];
                int64 n = fread(MemData, 1, MemDataTotSize, input_file);
                fprintf(stderr, "read %lld bytes\n", n);
                if (n != MemDataTotSize) {
                    fprintf(stderr, "n != MemDataTotSize\n");
                    exit(0);
                }
            }

#else
            mZipFile = gzopen(fileName_.c_str(), "r");
            gzrewind(mZipFile);
            if (read_in_mem) {
                fprintf(stderr, "zlib not support read in mem\n");
                exit(0);
            }
#endif
            this->isZipped = true;
        } else {
            fprintf(stderr, "start fqreader init\n");
            start_pos = startPos;
            end_pos = endPos;
            if(start_pos == end_pos) {
                std::ifstream gFile;
                gFile.open(fileName_.c_str());
                gFile.seekg(0, std::ios_base::end);
                end_pos = gFile.tellg();
                gFile.close();
            }
            fprintf(stderr, "FileReader [%lld %lld]\n", start_pos, end_pos);
            has_read = 0;
            total_read = end_pos - start_pos;
            align_end = false;

            buffer_test = (char*)aligned_alloc(MY_PAGE_SIZE, 8 * 1024 * 1024);
            if (!buffer_test) {
                perror("Failed to allocate aligned buffer");
                exit(1);
            }

            fd = open(fileName_.c_str(), O_RDWR | O_DIRECT);
            //fd = open(fileName_.c_str(), O_RDWR);
            if (fd == -1) {
                perror("Failed to open file");
                exit(0);
            }

            //TODO 
            off_t offset = start_pos;
            if (lseek(fd, offset, SEEK_SET) == (off_t)-1) {
                perror("lseek error");
                close(fd);
                exit(0);
            }


            if (read_in_mem) {
                fprintf(stderr, "read_in_mem %s\n", fileName_.c_str());
                MemDataTotSize = end_pos - start_pos;
                MemData = new byte[MemDataTotSize];
                int64 n = fread(MemData, 1, MemDataTotSize, mFile);
                fprintf(stderr, "read %lld bytes\n", n);
                if (n != MemDataTotSize) {
                    fprintf(stderr, "n != MemDataTotSize\n");
                    exit(0);
                }
            }

        }
        fprintf(stderr, "filereader init done\n");
    }

    FileReader::FileReader(int fd, bool isZipped) {
        fprintf(stderr, "FileReader fd is todo ...\n");
        exit(0);
    }

    FileReader::~FileReader() {
        fprintf(stderr, "time %lf %lf, cnt %d / %d\n", t_memcpy, t_read, read_cnt_gg, read_cnt);
        if (mIgInbuf != NULL) delete mIgInbuf;
        if (read_in_mem) delete[] MemData;
        if (mFile != NULL) {
            fclose(mFile);
            mFile = NULL;
        }
        if (fd != -1) {
            if (close(fd) == -1) {
                perror("Failed to close file descriptor");
            } else {
                fd = -1;
            }
        }
        if (mZipFile != NULL) {
            gzclose(mZipFile);
        }
#ifdef USE_LIBDEFLATE
        if (isZipped) {
            if (input_file != NULL) {
                fclose(input_file);
                input_file = NULL;
            }
            //if (iff_idx.is_open()) {
            //    iff_idx.close();
            //}
            for (int i = 0; i < 64; i++) {
                delete[] in_buffer[i];
                delete[] out_buffer[i];
            }
            delete[] to_read_buffer;
        }
#endif
    }

    void FileReader::DecompressMore() {
        size_t length_to_copy = buffer_tot_size - buffer_now_pos;
        if (length_to_copy > buffer_now_pos) {
            fprintf(stderr, "warning: memmove may GG\n");
        }
        std::memmove(to_read_buffer, to_read_buffer + buffer_now_pos, length_to_copy);
        buffer_tot_size = length_to_copy;
        buffer_now_pos = 0;

        int ok = 1;
        Para paras[64];
        size_t to_read = 0;
        size_t in_size = 0;
        size_t out_size[64] = {0};
        for (int i = 0; i < 64; i++) {
            if (now_block == end_line) {
                to_read = 0;
                iff_idx_end = 1;
            } else {
                to_read = block_sizes[now_block++];
            }
            //            fprintf(stderr, "to_read %zu, now_block %d\n", to_read, now_block);
            if(read_in_mem) {
                int64 lastDataSize = MemDataTotSize - MemDataNowPos;
                if (to_read > lastDataSize) {
                    memcpy(in_buffer[i], MemData + MemDataNowPos, lastDataSize);
                    MemDataNowPos += lastDataSize;
                    MemDataReadFinish = 1;
                    in_size = lastDataSize;
                } else {
                    memcpy(in_buffer[i], MemData + MemDataNowPos, to_read);
                    MemDataNowPos += to_read;
                    in_size = to_read;
                }
            } else {
                in_size = fread(in_buffer[i], 1, to_read, input_file);
            }
            if (in_size == 0) ok = 0;
            paras[i].in_buffer = in_buffer[i];
            paras[i].out_buffer = out_buffer[i];
            paras[i].in_size = in_size;
            paras[i].out_size = &(out_size[i]);
        }

        {
            std::lock_guard <std::mutex> guard(globalMutex);
            __real_athread_spawn((void *) slave_decompressfunc, paras, 1);
            athread_join();
        }

        for (int i = 0; i < 64; i++) {
            if (out_size[i] <= 0) continue;
            memcpy(to_read_buffer + buffer_tot_size, paras[i].out_buffer, out_size[i]);
            buffer_tot_size += out_size[i];
            if (buffer_tot_size > (64 + 8) * BLOCK_SIZE) {
                fprintf(stderr, "buffer_tot_size is out of size\n");
                exit(0);
            }
        }
    }

    int64 FileReader::ReadAlign(byte *memory_, int64_t offset_, uint64 size_) {

        static long long pre_offset = 0;
        if(offset_ - pre_offset > (1ll << 30)) {
            fprintf(stderr, "rank%d read %lld / %lld\n", my_rank, offset_, end_pos);
            pre_offset = offset_;
        }

        read_cnt++;
        offset_read = offset_;
        off_t offset = offset_;
        if (lseek(fd, offset, SEEK_SET) == (off_t)-1) {
            perror("lseek error");
            close(fd);
            exit(0);
        }

        if(offset_read + size_ > end_pos) size_ = end_pos - offset_;
        if(size_ % MY_PAGE_SIZE) {
            size_ = ((size_ / MY_PAGE_SIZE) + 1) * MY_PAGE_SIZE; 
            fprintf(stderr, "read gg byte, -- %d\n", size_);
        }

        //fprintf(stderr, "%d == read21 byte, -- %d -- %p\n", int(t_memcpy), size_, memory_);

        double t0 = GetTime();
        int n;
        bool is_aligned_addr = ((uintptr_t)memory_ % MY_PAGE_SIZE) == 0;
        if(!is_aligned_addr) n = read(fd, buffer_test, size_);
        else n = read(fd, memory_, size_);
        //int n = read(fd, buffer_test, size_);
        //int n = read(fd, memory_, size_);
        t_read += GetTime() - t0;

        //fprintf(stderr, "read22 %d byte, -- %d\n", n, size_);
        if (n != (ssize_t)size_) {
            if (n == 0) {
                fprintf(stderr, "Reached end of file unexpectedly. Only %zd bytes were read.\n", n);
            } else if (n == -1) {
                //fprintf(stderr, "GG1 %zu %d %p\n", size_, n, buffer_test);
                //fprintf(stderr, "GG2 %zu %d %p\n", size_, n, memory_);
                //fprintf(stderr, "Error reading file: %s\n", strerror(errno));
                //exit(0);
                n = read(fd, buffer_test, size_);
                is_aligned_addr = 0;

            }
        }
        if(offset_read + n >= end_pos) {
            align_end = true; 
            n = end_pos - offset_read;
        } 

        t0 = GetTime();
        if(!is_aligned_addr) {
            read_cnt_gg++;
            memcpy(memory_, buffer_test, n);
        }
        t_memcpy += GetTime() - t0;

        return n;
    }



    int64 FileReader::Read(byte *memory_, uint64 size_) {
        if (isZipped) {
#ifdef USE_LIBDEFLATE
#ifdef USE_CC_GZ
            size_t to_read = 0;
            if (now_block == end_line) {
                to_read = 0;
                iff_idx_end = 1;
                align_end = true;
            } else {
                to_read = block_sizes[now_block++];
            }
            if(read_in_mem) {
                int64 lastDataSize = MemDataTotSize - MemDataNowPos;
                int64 in_size;
                if (to_read > lastDataSize) {
                    memcpy(memory_, MemData + MemDataNowPos, lastDataSize);
                    MemDataNowPos += lastDataSize;
                    MemDataReadFinish = 1;
                    in_size = lastDataSize;
                } else {
                    memcpy(memory_, MemData + MemDataNowPos, to_read);
                    MemDataNowPos += to_read;
                    in_size = to_read;
                }
                assert(to_read == in_size);
                return to_read;
                //fprintf(stderr, "use consumer slave gz in in_mem module is TODO!\n");
                //exit(0);
            } else {

                static long long pre_offset = 0;
                if(buffer_now_offset - pre_offset > (1ll << 30)) {
                    fprintf(stderr, "rank%d read %lld / %lld\n", my_rank, buffer_now_offset, end_pos);
                    pre_offset = buffer_now_offset;
                }


                int nouse_chunk_size;
                if(read_cnt == 0) {
                    nouse_chunk_size = buffer_now_offset % MY_PAGE_SIZE;
                    buffer_now_offset -= buffer_now_offset % MY_PAGE_SIZE;
                    to_read += nouse_chunk_size;
                }
                
                off_t offset = buffer_now_offset;

                if (lseek(fd, offset, SEEK_SET) == (off_t)-1) {
                    perror("lseek error");
                    close(fd);
                    exit(0);
                }


                int align_to_read = to_read - buffer_last_size;
                if(align_to_read % MY_PAGE_SIZE) {
                    align_to_read = align_to_read - align_to_read % MY_PAGE_SIZE + MY_PAGE_SIZE;
                }
                double t0 = GetTime();
                int n = read(fd, buffer_test, align_to_read);
                t_read += GetTime() - t0;

                //int n = read(fd, memory_, size_);
                if (n != (ssize_t)align_to_read) {
                    if (n == 0) {
                        fprintf(stderr, "Reached end of file unexpectedly. Only %zd bytes were read.\n", n);
                    } else if (n == -1) {
                        fprintf(stderr, "GG1 %zu %d %p\n", align_to_read, n, buffer_test);
                        fprintf(stderr, "Error reading file: %s\n", strerror(errno));
                    }
                }
    
                //fprintf(stderr, "rank%d readcnt %d, n %d, align_toread %d, offset %d, to_read %d, buffer_last_size %d\n", my_rank, read_cnt, n, align_to_read, buffer_now_offset, to_read, buffer_last_size);
                if(read_cnt == 0 && buffer_now_offset != 0) {
                    t0 = GetTime();
                    to_read -= nouse_chunk_size; 
                    memcpy(memory_, buffer_test + nouse_chunk_size, to_read);
                    buffer_last_size = align_to_read - to_read - nouse_chunk_size;
                    memcpy(buffer_test_last, buffer_test + to_read + nouse_chunk_size, buffer_last_size);
                    t_memcpy += GetTime() - t0;
                    buffer_now_offset += align_to_read;
                } else {
                    t0 = GetTime();
                    memcpy(memory_, buffer_test_last, buffer_last_size);
                    memcpy(memory_ + buffer_last_size, buffer_test, to_read - buffer_last_size);
                    int buffer_last_size2 = buffer_last_size;
                    buffer_last_size = align_to_read - (to_read - buffer_last_size2);
                    memcpy(buffer_test_last, buffer_test + to_read - buffer_last_size2, buffer_last_size);
                    t_memcpy += GetTime() - t0;
                    buffer_now_offset += align_to_read;
                }

                read_cnt++;
                //int64 n = fread(memory_, 1, to_read, input_file);
                //assert(n == to_read);
                //fprintf(stderr, "nn %lld\n", n);
                return to_read;
            }

#else
            if (buffer_now_pos + size_ > buffer_tot_size) {
                DecompressMore();
                if (buffer_now_pos + size_ > buffer_tot_size) {
                    fprintf(stderr, "after decom still not big enough, read done, -- %d\n", iff_idx_end);
                    size_ = buffer_tot_size - buffer_now_pos;
                }
            }
            memcpy(memory_, to_read_buffer + buffer_now_pos, size_);
            buffer_now_pos += size_;
            return size_;
#endif
#else
            if(read_in_mem) {
                fprintf(stderr, "zlib not support read in mem\n");
                exit(0);
            }
            int64 n = gzread(mZipFile, memory_, size_);
            if (n == -1) {
                int errNum;
                const char* errorMsg = gzerror(mZipFile, &errNum);
                if (errNum) {
                    std::cerr << "Error to read gzip file: " << errorMsg << std::endl;
                }
            }
            return n;
#endif
        } else {
            if (read_in_mem) {
                int64 lastDataSize = MemDataTotSize - MemDataNowPos;
                if (size_ > lastDataSize) {
                    memcpy(memory_, MemData + MemDataNowPos, lastDataSize);
                    MemDataNowPos += lastDataSize;
                    MemDataReadFinish = 1;
                    return lastDataSize;
                } else {
                    memcpy(memory_, MemData + MemDataNowPos, size_);
                    MemDataNowPos += size_;
                    return size_;
                }
            } else {

                static long long pre_offset = 0;
                if(has_read - pre_offset > (1ll << 30)) {
                    fprintf(stderr, "rank%d read %lld / %lld %lld\n", my_rank, has_read, end_pos, total_read);
                    pre_offset = has_read;
                }


                if(has_read + size_ > total_read) size_ = total_read - has_read;
                if(size_ % MY_PAGE_SIZE) {
                    if(size_ < MY_PAGE_SIZE) size_ = ((size_ / MY_PAGE_SIZE) + 1) * MY_PAGE_SIZE; 
                    else size_ = size_ - size_ % MY_PAGE_SIZE;
                    //fprintf(stderr, "read gg byte, -- %d\n", size_);
                }

                //fprintf(stderr, "read1 byte, -- %d -- %p\n", size_, memory_);

                double t0 = GetTime();
                int n = read(fd, buffer_test, size_);
                t_read += GetTime() - t0;

                //fprintf(stderr, "read2 %d byte, -- %d\n", n, size_);
                if (n != (ssize_t)size_) {
                    if (n == 0) {
                        fprintf(stderr, "Reached end of file unexpectedly. Only %zd bytes were read.\n", n);
                    } else if (n == -1) {
                        fprintf(stderr, "GG1 %zu %d %p\n", size_, n, buffer_test);
                        //fprintf(stderr, "GG1 %zu %d %p\n", size_, n, memory_);
                        fprintf(stderr, "Error reading file: %s\n", strerror(errno));
                    }
                }

                t0 = GetTime();
                memcpy(memory_, buffer_test, n);
                t_memcpy += GetTime() - t0;

                has_read += n;
                return n;
            }
        }
    }

    bool FileReader::FinishRead() {
        if (isZipped) {
            //fprintf(stderr, " finish ? %d %d--%lld %lld\n", iff_idx_end, int(align_end), buffer_now_pos, buffer_tot_size);
#ifdef USE_LIBDEFLATE
            if(read_in_mem) return (iff_idx_end || MemDataReadFinish) && (buffer_now_pos == buffer_tot_size);
# ifdef use_align_64k
            else return (iff_idx_end || align_end) && (buffer_now_pos == buffer_tot_size);
# else
            else return (iff_idx_end || feof(input_file)) && (buffer_now_pos == buffer_tot_size);
# endif
#else
            return gzeof(mZipFile);
#endif
        } else {
            //fprintf(stderr, " finish ? %lld %lld\n", total_read, has_read);
            if (read_in_mem) return MemDataReadFinish;
#ifdef use_align_64k
            else return align_end;
#else
            else return total_read == has_read;
#endif
        }
    }

    bool FileReader::Eof() {
        return eof;
    }

    void FileReader::setEof() {
        eof = true;
    }

}
