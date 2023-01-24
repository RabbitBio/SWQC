//
// Created by ylf9811 on 2021/7/6.
//
#include "seqc.h"
using namespace std;

struct dupInfo{
    uint32_t key;
    uint64_t kmer32;
    uint8_t gc;
};


struct qc_data {
    ThreadInfo **thread_info_;
    CmdInfo *cmd_info_;
    vector <dupInfo> *dups;
    vector <neoReference> *data1_;
    vector <neoReference> *data2_;
    vector <neoReference> *pass_data1_;
    vector <neoReference> *pass_data2_;
    int *cnt;
    int bit_len;
};

extern "C" {
#include <athread.h>
#include <pthread.h>
    void slave_tgsfunc();
    void slave_ngsfunc();
}

//extern void slave_tgsfunc(qc_data *para);
//extern void slave_ngsfunc(qc_data *para);

/**
 * @brief Construct function
 * @param cmd_info1 : cmd information
 */
SeQc::SeQc(CmdInfo *cmd_info1, int my_rank_, int comm_size_) {
    my_rank = my_rank_;
    comm_size = comm_size_;
    now_pos_ = 0;
    cmd_info_ = cmd_info1;
    filter_ = new Filter(cmd_info1);
    done_thread_number_ = 0;
    int out_block_nums = int(1.0 * cmd_info1->in_file_size1_ / cmd_info1->out_block_size_);
    out_queue_ = NULL;

    in_is_zip_ = cmd_info1->in_file_name1_.find(".gz") != string::npos;
    out_is_zip_ = cmd_info1->out_file_name1_.find(".gz") != string::npos;
    //cmd_info1->out_file_name1_ = "p" + to_string(my_rank) + cmd_info1->out_file_name1_;
    if (cmd_info1->write_data_) {
        out_queue_ = new moodycamel::ConcurrentQueue<pair < char * , int>>;
        queueNumNow = 0;
        queueSizeLim = 128;
        if (out_is_zip_) {
            if (cmd_info1->use_pigz_) {
                pigzQueueNumNow = 0;
                pigzQueueSizeLim = 1 << 5;
                string out_name1 = cmd_info1->out_file_name1_;
                out_name1 = out_name1.substr(0, out_name1.find(".gz"));
                if((out_stream_ = fopen(out_name1.c_str(),"w")) == NULL) {
                    printf("File cannot be opened\n");
                }
                fclose(out_stream_);
                //out_stream_.open(out_name1);
                //out_stream_.close();
#ifdef Verbose
                printf("now use pigz to compress output data\n");
#endif
            } else {
#ifdef Verbose
                printf("open gzip stream %s\n", cmd_info1->out_file_name1_.c_str());
#endif
                zip_out_stream = gzopen(cmd_info1->out_file_name1_.c_str(), "w");
                gzsetparams(zip_out_stream, cmd_info1->compression_level_, Z_DEFAULT_STRATEGY);
                gzbuffer(zip_out_stream, 1024 * 1024);
            }
        } else {
#ifdef Verbose
            printf("open stream %s\n", cmd_info1->out_file_name1_.c_str());
#endif
            ifstream gFile;
            gFile.open(cmd_info1->in_file_name1_.c_str());
            gFile.seekg(0, ios_base::end);
            long long file_size = gFile.tellg();
            gFile.close();
            printf("pre file size %lld\n", file_size);
            if(my_rank == 0) {
                int fd = open(cmd_info1->out_file_name1_.c_str(), O_CREAT | O_TRUNC | O_RDWR | O_EXCL, 0644);
                ftruncate(fd, sizeof(char) * file_size);
                out_stream_ = fdopen(fd, "w");
            } else {
                int found = 0;
                do {

                    if(-1 == access(cmd_info1->out_file_name1_.c_str(), F_OK)) {
                        if(ENOENT == errno) {
                            cerr << "waiting file be created..." << endl;
                            usleep(100);
                        } else {
                            cerr << "file open GG" << endl;
                            usleep(100);
                            //exit(0);
                        }
                    } else {
                        out_stream_ = fopen(cmd_info1->out_file_name1_.c_str(), "r+b");   
                        found = 1;
                    }
                } while(found == 0);
            }
            //if((out_stream_ = fopen(cmd_info1->out_file_name1_.c_str(),"w")) == NULL) {
            //    printf("File cannot be opened\n");
            //}
            //out_stream_.open(cmd_info1->out_file_name1_);
        }
    }

    duplicate_ = NULL;
    if (cmd_info1->state_duplicate_) {
        duplicate_ = new Duplicate(cmd_info1);
    }
    umier_ = NULL;
    if (cmd_info1->add_umi_) {
        umier_ = new Umier(cmd_info1);
    }
    if (cmd_info1->use_pugz_) {
        pugzQueue = new moodycamel::ReaderWriterQueue<pair < char * , int>>
            (1 << 10);
    }
    if (cmd_info1->use_pigz_) {
        pigzQueue = new moodycamel::ReaderWriterQueue<pair < char * , int>>;
        pigzLast.first = new char[1 << 24];
        pigzLast.second = 0;
    }
    pugzDone = 0;
    producerDone = 0;
    writerDone = 0;
}

SeQc::~SeQc() {
    delete filter_;
    if (cmd_info_->write_data_) {
        delete out_queue_;
    }

    if (cmd_info_->state_duplicate_) {
        delete duplicate_;
    }
    if (cmd_info_->add_umi_) {
        delete umier_;
    }
}


/**
 * @brief get fastq data chunk from fastq_data_pool and put it into data queue
 * @param file : fastq file name, which is also the input file
 * @param fastq_data_pool : fastq data pool
 * @param dq : a data queue
 */

void SeQc::ProducerSeFastqTask(string file, rabbit::fq::FastqDataPool *fastq_data_pool,
        rabbit::core::TDataQueue<rabbit::fq::FastqDataChunk> &dq) {

    double t0 = GetTime();
    rabbit::fq::FastqFileReader *fqFileReader;
    rabbit::uint32 tmpSize = 1 << 20;
    if (cmd_info_->seq_len_ <= 200) tmpSize = 1 << 14;
    fqFileReader = new rabbit::fq::FastqFileReader(file, *fastq_data_pool, "", in_is_zip_, tmpSize);
    int64_t n_chunks = 0;

    if (cmd_info_->use_pugz_) {
        pair<char *, int> last_info;
        last_info.first = new char[1 << 20];
        last_info.second = 0;
        while (true) {
            rabbit::fq::FastqDataChunk *fqdatachunk;
            fqdatachunk = fqFileReader->readNextChunk(pugzQueue, &pugzDone, last_info);
            if (fqdatachunk == NULL) break;
            dq.Push(n_chunks, fqdatachunk);
            n_chunks++;
        }
        delete[] last_info.first;
    } else {
        double t_sum1 = 0;
        double t_sum2 = 0;
        double t_sum3 = 0;
        while (true) {
            double tt0 = GetTime();
            rabbit::fq::FastqDataChunk *fqdatachunk;
            fqdatachunk = fqFileReader->readNextChunk();
            t_sum1 += GetTime() - tt0;
            tt0 = GetTime();
            if (fqdatachunk == NULL) {
                int res_chunks = comm_size - n_chunks % comm_size;
                printf("bu %d chunks, (%d, %d)\n", res_chunks, n_chunks, comm_size);
                if(res_chunks) {
                    for(int i = 0; i < res_chunks; i++) {
                        dq.Push(n_chunks, fqdatachunk);
                        n_chunks++;
                    }
                    printf("producer still push %d chunks\n", res_chunks);
                }
                break;
            }
            dq.Push(n_chunks, fqdatachunk);
            n_chunks++;
            t_sum2 += GetTime() - tt0;
        }
        printf("producer sum1 cost %lf\n", t_sum1);
        printf("producer sum2 cost %lf\n", t_sum2);
    }

    printf("producer cost %lf\n", GetTime() - t0);
    dq.SetCompleted();
    delete fqFileReader;
    producerDone = 1;
}

string SeQc::Read2String(neoReference &ref) {
    return string((char *) ref.base + ref.pname, ref.lname) + "\n" +
        string((char *) ref.base + ref.pseq, ref.lseq) + "\n" +
        string((char *) ref.base + ref.pstrand, ref.lstrand) + "\n" +
        string((char *) ref.base + ref.pqual, ref.lqual) + "\n";
}

void SeQc::Read2Chars(neoReference &ref, char *out_data, int &pos) {
    memcpy(out_data + pos, ref.base + ref.pname, ref.lname);
    pos += ref.lname;
    out_data[pos++] = '\n';
    memcpy(out_data + pos, ref.base + ref.pseq, ref.lseq);
    pos += ref.lseq;
    out_data[pos++] = '\n';
    memcpy(out_data + pos, ref.base + ref.pstrand, ref.lstrand);
    pos += ref.lstrand;
    out_data[pos++] = '\n';
    memcpy(out_data + pos, ref.base + ref.pqual, ref.lqual);
    pos += ref.lqual;
    out_data[pos++] = '\n';
}

void SeQc::ConsumerSeFastqTask(ThreadInfo **thread_infos, rabbit::fq::FastqDataPool *fastq_data_pool,
        rabbit::core::TDataQueue<rabbit::fq::FastqDataChunk> &dq) {
    rabbit::int64 id = 0;
    rabbit::fq::FastqDataChunk *fqdatachunk;
    qc_data para;
    para.cmd_info_ = cmd_info_;
    para.thread_info_ = thread_infos;
    para.bit_len = 0;
    if(cmd_info_->state_duplicate_) {
        para.bit_len = duplicate_->key_len_base_;
    }
    athread_init();
    if (cmd_info_->is_TGS_) {
        athread_init();
        double t0 = GetTime();
        double tsum1 = 0;
        double tsum2 = 0;
        double tsum3 = 0;
        while (dq.Pop(id, fqdatachunk)) {
            double tt0 = GetTime();
            tsum1 += GetTime() - tt0;
            tt0 = GetTime();
            vector <neoReference> data;
            rabbit::fq::chunkFormat(fqdatachunk, data, true);
            tsum2 += GetTime() - tt0;
            tt0 = GetTime();
            para.data1_ = &data;
            __real_athread_spawn((void *)slave_tgsfunc, &para, 1);
            athread_join();
            tsum3 += GetTime() - tt0;
            fastq_data_pool->Release(fqdatachunk);
        }
        printf("TGSnew tot cost %lf\n", GetTime() - t0);
        printf("TGSnew producer cost %lf\n", tsum1);
        printf("TGSnew format cost %lf\n", tsum2);
        printf("TGSnew slave cost %lf\n", tsum3);
    } else {
        int sum_ = 0;
        double t0 = GetTime();
        double tsum1 = 0;
        double tsum2 = 0;
        double tsum3 = 0;
        double tsum4 = 0;
        double tsum5 = 0;
        while (dq.Pop(id, fqdatachunk)) {
            
            if(id % comm_size != my_rank) {
                if(fqdatachunk != NULL) fastq_data_pool->Release(fqdatachunk);
                continue;
            } 
            if(fqdatachunk == NULL) {
                if (cmd_info_->write_data_) {
                    while (queueNumNow >= queueSizeLim) {
#ifdef Verbose
                        //printf("waiting to push a chunk to out queue %d\n",out_len);
#endif
                        usleep(100);
                    }

                    out_queue_->enqueue(make_pair((char *)(NULL), 0));
                    queueNumNow++;
                    //mylock.unlock();

                }
                continue;
            }

            //printf("id %lld\n", id);

            ///*
            double tt0 = GetTime();
            vector <neoReference> data;
            rabbit::fq::chunkFormat(fqdatachunk, data, true);
            vector <neoReference> pass_data;
            vector <dupInfo> dups;
            if(cmd_info_->write_data_) {
                pass_data.resize(data.size());
            }
            if(cmd_info_->state_duplicate_) {
                dups.resize(data.size());
            }
            tsum1 += GetTime() - tt0;
            tt0 = GetTime();
            para.data1_ = &data;
            para.pass_data1_ = &pass_data;
            para.dups = &dups;
            __real_athread_spawn((void *)slave_ngsfunc, &para, 1);
            athread_join();
            tsum2 += GetTime() - tt0;
            tt0 = GetTime(); 
            //printf("pass size %d\n", pass_data.size());
            if(cmd_info_->state_duplicate_) {
                for(auto item : dups) {
                    auto key = item.key;
                    auto kmer32 = item.kmer32;
                    auto gc = item.gc;
                    if (duplicate_->counts_[key] == 0) {
                        duplicate_->counts_[key] = 1;
                        duplicate_->dups_[key] = kmer32;
                        duplicate_->gcs_[key] = gc;
                    } else {
                        if (duplicate_->dups_[key] == kmer32) {
                            duplicate_->counts_[key]++;
                            if (duplicate_->gcs_[key] > gc) duplicate_->gcs_[key] = gc;
                        } else if (duplicate_->dups_[key] > kmer32) {
                            duplicate_->dups_[key] = kmer32;
                            duplicate_->counts_[key] = 1;
                            duplicate_->gcs_[key] = gc;
                        }
                    }
                }
            }
            tsum3 += GetTime() - tt0;
            tt0 = GetTime();
            if (cmd_info_->write_data_) {
                if (pass_data.size() > 0) {
                    //mylock.lock();
                    int out_len = 0;
                    for(auto item : pass_data){
                        if(item.lname == 0) continue;
                        out_len += item.lname + item.lseq + item.lstrand + item.lqual + 4;
                    }
                    char *out_data = new char[out_len];
                    int pos = 0;
                    for (auto item: pass_data) {
                        if(item.lname == 0) continue;
                        Read2Chars(item, out_data, pos);
                    }
                    ASSERT(pos == out_len);
                    while (queueNumNow >= queueSizeLim) {
#ifdef Verbose
                        //printf("waiting to push a chunk to out queue %d\n",out_len);
#endif
                        usleep(100);
                    }

                    out_queue_->enqueue(make_pair(out_data, out_len));
                    queueNumNow++;
                    //mylock.unlock();

                }
            }
            tsum4 += GetTime() - tt0;
            tt0 = GetTime();
            //*/
            fastq_data_pool->Release(fqdatachunk);
            tsum5 += GetTime() - tt0;
        }
        printf("NGSnew tot cost %lf\n", GetTime() - t0);
        printf("NGSnew format cost %lf\n", tsum1);
        printf("NGSnew slave cost %lf\n", tsum2);
        printf("NGSnew dup cost %lf\n", tsum3);
        printf("NGSnew write cost %lf\n", tsum4);
        printf("NGSnew release cost %lf\n", tsum5);
    }
    done_thread_number_++;
    athread_halt();
    printf("consumer done\n");
}

/**
 * @brief a function to write data from out_data queue to file
 */
void SeQc::WriteSeFastqTask() {
#ifdef Verbose
    double t0 = GetTime();
#endif
    int cnt = 0;
    bool overWhile = 0;
    pair<char *, int> now;
    while (true) {
        while (out_queue_->try_dequeue(now) == 0) {
            if (done_thread_number_ == cmd_info_->thread_number_) {
                if (out_queue_->size_approx() == 0) {
                    overWhile = 1;
                    printf("write done\n");
                    break;
                }
            }
            usleep(100);
        }
        if (overWhile) break;
        queueNumNow--;
        if (out_is_zip_) {
            if (cmd_info_->use_pigz_) {
                while (pigzQueueNumNow > pigzQueueSizeLim) {
#ifdef Verbose
                    //printf("waiting to push a chunk to pigz queue\n");
#endif
                    usleep(100);
                }
                pigzQueue->enqueue(now);
                pigzQueueNumNow++;
            } else {
                int written = gzwrite(zip_out_stream, now.first, now.second);
                if (written != now.second) {
                    printf("gzwrite error\n");
                    exit(0);
                }
                delete[] now.first;
            }
        } else {
            //out_stream_.write(now.first, now.second);
            int now_size = now.second;
            char *now_pos = now.first;
            int now_sizes[comm_size];
            now_sizes[0] = now_size;
            MPI_Barrier(MPI_COMM_WORLD);
            if(my_rank) {
                MPI_Send(&now_size, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
            } else {
                for(int ii = 1; ii < comm_size; ii++) {
                    MPI_Recv(&(now_sizes[ii]), 1, MPI_INT, ii, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                } 
            }
            MPI_Barrier(MPI_COMM_WORLD);
            if(my_rank == 0) {
                for(int ii = 1; ii < comm_size; ii++) {
                    MPI_Send(now_sizes, comm_size, MPI_INT, ii, 0, MPI_COMM_WORLD);
                }
            } else {
                MPI_Recv(now_sizes, comm_size, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            }
            MPI_Barrier(MPI_COMM_WORLD);
            int pre_sizes[comm_size];
            pre_sizes[0] = 0;
            for(int ii = 1; ii < comm_size; ii++) {
                pre_sizes[ii] = pre_sizes[ii - 1] + now_sizes[ii - 1];
            }
            if(now.second) {
                fseek(out_stream_, now_pos_ + pre_sizes[my_rank], SEEK_SET);
                fwrite(now.first, sizeof(char), now.second, out_stream_);
            }
            MPI_Barrier(MPI_COMM_WORLD);
            for(int ii = 0; ii < comm_size; ii++) {
                now_pos_ += now_sizes[ii];
                //printf("this round size %d\n", now_sizes[ii]);
            }
            delete[] now.first;
        }
    }

    if (out_is_zip_) {
        if (cmd_info_->use_pigz_) {

        } else {
            if (zip_out_stream) {
                gzflush(zip_out_stream, Z_FINISH);
                gzclose(zip_out_stream);
                zip_out_stream = NULL;
            }
        }
    } else {
        //out_stream_.close();
        fclose(out_stream_);
        if(my_rank == 0) {
            truncate(cmd_info_->out_file_name1_.c_str(), sizeof(char) * now_pos_);
        }

    }
#ifdef Verbose
    printf("write cost %.5f\n", GetTime() - t0);
#endif
}


/**
 * @brief do pugz
 */

void SeQc::PugzTask() {
#ifdef Verbose
    printf("pugz start\n");
    auto t0 = GetTime();
#endif
    main_pugz(cmd_info_->in_file_name1_, cmd_info_->pugz_threads_, pugzQueue, &producerDone);
#ifdef Verbose
    printf("pugz cost %.5f\n", GetTime() - t0);
#endif
    pugzDone = 1;
}


void SeQc::PigzTask() {
    /*
       argc 9
       argv ./pigz
       argv -p
       argv 16
       argv -k
       argv -4
       argv -f
       argv -b
       argv -4096
       argv p.fq
       */
    int cnt = 9;

    char **infos = new char *[9];
    infos[0] = "./pigz";
    infos[1] = "-p";
    int th_num = cmd_info_->pigz_threads_;
    //    printf("th num is %d\n", th_num);
    string th_num_s = to_string(th_num);
    //    printf("th num s is %s\n", th_num_s.c_str());
    //    printf("th num s len is %d\n", th_num_s.length());

    infos[2] = new char[th_num_s.length() + 1];
    memcpy(infos[2], th_num_s.c_str(), th_num_s.length());
    infos[2][th_num_s.length()] = '\0';
    infos[3] = "-k";


    string tmp_level = to_string(cmd_info_->compression_level_);
    tmp_level = "-" + tmp_level;
    infos[4] = new char[tmp_level.length() + 1];
    memcpy(infos[4], tmp_level.c_str(), tmp_level.length());
    infos[4][tmp_level.length()] = '\0';


    infos[5] = "-f";
    infos[6] = "-b";
    infos[7] = "4096";
    string out_name1 = cmd_info_->out_file_name1_;
    string out_file = out_name1.substr(0, out_name1.find(".gz"));
    //    printf("th out_file is %s\n", out_file.c_str());
    //    printf("th out_file len is %d\n", out_file.length());
    infos[8] = new char[out_file.length() + 1];
    memcpy(infos[8], out_file.c_str(), out_file.length());
    infos[8][out_file.length()] = '\0';
    main_pigz(cnt, infos, pigzQueue, &writerDone, pigzLast, &pigzQueueNumNow);
#ifdef Verbose
    printf("pigz done\n");
#endif
}


void SeQc::NGSTask(std::string file, rabbit::fq::FastqDataPool *fastq_data_pool, ThreadInfo **thread_infos){
    printf("single thread\n");
    rabbit::fq::FastqFileReader *fqFileReader;
    rabbit::uint32 tmpSize = 1 << 20;
    if (cmd_info_->seq_len_ <= 200) tmpSize = 1 << 14;
    fqFileReader = new rabbit::fq::FastqFileReader(file, *fastq_data_pool, "", in_is_zip_, tmpSize);
    int64_t n_chunks = 0;
    qc_data para;
    para.cmd_info_ = cmd_info_;
    para.thread_info_ = thread_infos;
    para.bit_len = 0;
    if(cmd_info_->state_duplicate_) {
        para.bit_len = duplicate_->key_len_base_;
    }
    athread_init();
    double t0 = GetTime();
    double tsum1 = 0;
    double tsum2 = 0;
    double tsum3 = 0;
    double tsum4 = 0;
    double tsum4_1 = 0;
    double tsum4_2 = 0;
    char enter_char[1];
    enter_char[0] = '\n';
    while (true) {
        double tt0 = GetTime();
        rabbit::fq::FastqDataChunk *fqdatachunk;
        fqdatachunk = fqFileReader->readNextChunk();
        if (fqdatachunk == NULL) break;
        n_chunks++;
        tsum1 += GetTime() - tt0;
        tt0 = GetTime();
        vector <neoReference> data;
        rabbit::fq::chunkFormat(fqdatachunk, data, true);
        vector <neoReference> pass_data;
        vector <dupInfo> dups;
        if(cmd_info_->write_data_) {
            pass_data.resize(data.size());
        }
        if(cmd_info_->state_duplicate_) {
            dups.resize(data.size());
        }
        tsum2 += GetTime() - tt0;
        tt0 = GetTime();
        para.data1_ = &data;
        para.pass_data1_ = &pass_data;
        para.dups = &dups;
        //athread_spawn_tupled(slave_ngsfunc, &para);
        __real_athread_spawn((void *)slave_ngsfunc, &para, 1);
        athread_join();
        tsum3 += GetTime() - tt0;
        tt0 = GetTime(); 
        if(cmd_info_->state_duplicate_) {
            for(auto item : dups) {
                auto key = item.key;
                auto kmer32 = item.kmer32;
                auto gc = item.gc;
                if (duplicate_->counts_[key] == 0) {
                    duplicate_->counts_[key] = 1;
                    duplicate_->dups_[key] = kmer32;
                    duplicate_->gcs_[key] = gc;
                } else {
                    if (duplicate_->dups_[key] == kmer32) {
                        duplicate_->counts_[key]++;
                        if (duplicate_->gcs_[key] > gc) duplicate_->gcs_[key] = gc;
                    } else if (duplicate_->dups_[key] > kmer32) {
                        duplicate_->dups_[key] = kmer32;
                        duplicate_->counts_[key] = 1;
                        duplicate_->gcs_[key] = gc;
                    }
                }
            }
        }
        if(cmd_info_->write_data_) {
            int tot_len = 0;
            double tt00 = GetTime();
            for(auto item : pass_data){
                if(item.lname == 0) continue;
                tot_len += item.lname + item.lseq + item.lstrand + item.lqual + 4;
            }
            char *tmp_out = new char[tot_len];
            char *now_out = tmp_out;
            for(auto item : pass_data) {
                if(item.lname == 0) continue;
                memcpy(now_out, (char *)item.base + item.pname, item.lname);
                now_out += item.lname;
                memcpy(now_out, enter_char, 1);
                now_out++;
                memcpy(now_out, (char *)item.base + item.pseq, item.lseq);
                now_out += item.lseq;
                memcpy(now_out, enter_char, 1);
                now_out++;
                memcpy(now_out, (char *)item.base + item.pstrand, item.lstrand);
                now_out += item.lstrand;
                memcpy(now_out, enter_char, 1);
                now_out++;
                memcpy(now_out, (char *)item.base + item.pqual, item.lqual);
                now_out += item.lqual;
                memcpy(now_out, enter_char, 1);
                now_out++;
            }
            tsum4_1 += GetTime() - tt00;
            tt00 = GetTime();
            //out_stream_.write(tmp_out, tot_len);
            fwrite(tmp_out, sizeof(char), tot_len, out_stream_);
            delete[] tmp_out;
            tsum4_2 += GetTime() - tt00;
        }
        tsum4 += GetTime() - tt0;
        fastq_data_pool->Release(fqdatachunk);
    }
    athread_halt();
    printf("NGS tot cost %lf\n", GetTime() - t0);
    printf("NGS producer cost %lf\n", tsum1);
    printf("NGS format cost %lf\n", tsum2);
    printf("NGS slave cost %lf\n", tsum3);
    printf("NGS write cost %lf\n", tsum4);
    printf("NGS write1 cost %lf\n", tsum4_1);
    printf("NGS write2 cost %lf\n", tsum4_2);
    delete fqFileReader;
}

/**
 * @brief do QC for single-end data
 */

void SeQc::ProcessSeFastq() {
    auto *fastqPool = new rabbit::fq::FastqDataPool(64, 1 << 22);
    rabbit::core::TDataQueue<rabbit::fq::FastqDataChunk> queue1(64, 1);
    auto **p_thread_info = new ThreadInfo *[slave_num];
    for (int t = 0; t < slave_num; t++) {
        p_thread_info[t] = new ThreadInfo(cmd_info_, false);
    }
    thread *write_thread;
    if (cmd_info_->write_data_) {
        write_thread = new thread(bind(&SeQc::WriteSeFastqTask, this));
    }
    thread producer(bind(&SeQc::ProducerSeFastqTask, this, cmd_info_->in_file_name1_, fastqPool, ref(queue1)));
    thread consumer(bind(&SeQc::ConsumerSeFastqTask, this, p_thread_info, fastqPool, ref(queue1)));
    producer.join();
    printf("producer done\n");
    consumer.join();
    printf("consumer done\n");
    if (cmd_info_->write_data_) {
        write_thread->join();
        writerDone = 1;
    }
    
    MPI_Barrier(MPI_COMM_WORLD);
    printf("all pro write done\n");

    printf("all thrad done\n");
    printf("now merge thread info\n");
    vector < State * > pre_vec_state;
    vector < State * > aft_vec_state;

    for (int t = 0; t < slave_num; t++) {
        pre_vec_state.push_back(p_thread_info[t]->pre_state1_);
        aft_vec_state.push_back(p_thread_info[t]->aft_state1_);
    }
    auto pre_state = State::MergeStates(pre_vec_state);
    auto aft_state = State::MergeStates(aft_vec_state);
#ifdef Verbose
    if (cmd_info_->do_overrepresentation_) {
        printf("orp cost %lf\n", pre_state->GetOrpCost() + aft_state->GetOrpCost());
    }
    printf("merge done\n");
#endif
    printf("\nprint read (before filter) info :\n");
    State::PrintStates(pre_state);
    printf("\nprint read (after filter) info :\n");
    State::PrintStates(aft_state);
    printf("\n");
    if (cmd_info_->print_what_trimmed_)
        State::PrintAdapterToFile(aft_state);
    State::PrintFilterResults(aft_state);
    printf("\n");

    if (cmd_info_->do_overrepresentation_ && cmd_info_->print_ORP_seqs_) {
        auto hash_graph1 = pre_state->GetHashGraph();
        int hash_num1 = pre_state->GetHashNum();

        auto hash_graph2 = aft_state->GetHashGraph();
        int hash_num2 = aft_state->GetHashNum();

        int spg = cmd_info_->overrepresentation_sampling_;

        ofstream ofs;
        string srr_name = cmd_info_->in_file_name1_;
        srr_name = PaseFileName(srr_name);

        string out_name = "se_" + srr_name + "_before_ORP_sequences.txt";
        ofs.open(out_name, ifstream::out);
        ofs << "sequence count"
            << "\n";
        int cnt1 = 0;
        for (int i = 0; i < hash_num1; i++) {
            if (!overRepPassed(hash_graph1[i].seq, hash_graph1[i].cnt, spg)) continue;
            ofs << hash_graph1[i].seq << " " << hash_graph1[i].cnt << "\n";
            cnt1++;
        }
        ofs.close();
        printf("in %s (before filter) find %d possible overrepresented sequences (store in %s)\n", srr_name.c_str(),
                cnt1, out_name.c_str());


        out_name = "se_" + srr_name + "_after_ORP_sequences.txt";
        ofs.open(out_name, ifstream::out);
        ofs << "sequence count"
            << "\n";
        int cnt2 = 0;
        for (int i = 0; i < hash_num2; i++) {
            if (!overRepPassed(hash_graph2[i].seq, hash_graph2[i].cnt, spg)) continue;
            ofs << hash_graph2[i].seq << " " << hash_graph2[i].cnt << "\n";
            cnt2++;
        }
        ofs.close();
        printf("in %s (after filter) find %d possible overrepresented sequences (store in %s)\n", srr_name.c_str(),
                cnt2, out_name.c_str());
        printf("\n");
    }


    int *dupHist = NULL;
    double *dupMeanGC = NULL;
    double dupRate = 0.0;
    int histSize = 32;
    if (cmd_info_->state_duplicate_) {
        dupHist = new int[histSize];
        memset(dupHist, 0, sizeof(int) * histSize);
        dupMeanGC = new double[histSize];
        memset(dupMeanGC, 0, sizeof(double) * histSize);
        dupRate = duplicate_->statAll(dupHist, dupMeanGC, histSize);
        printf("Duplication rate (may be overestimated since this is SE data): %.5f %%\n", dupRate * 100.0);
        delete[] dupHist;
        delete[] dupMeanGC;
    }
    string srr_name = cmd_info_->in_file_name1_;
    srr_name = PaseFileName(srr_name);
    if(pre_state->GetLines() == 0) {
        printf("read number is 0, don't print html reporter, please check input file\n");
    } else {
        Repoter::ReportHtmlSe(srr_name + "_RabbitQCPlus.html", pre_state, aft_state, cmd_info_->in_file_name1_,
                dupRate * 100.0);
    }

    delete pre_state;
    delete aft_state;

    delete fastqPool;


    for (int t = 0; t < slave_num; t++) {
        delete p_thread_info[t];
    }

    delete[] p_thread_info;
}

void SeQc::ProcessSeFastqOneThread() {
    auto *fastqPool = new rabbit::fq::FastqDataPool(1, 1 << 26);
    auto **p_thread_info = new ThreadInfo *[slave_num];
    for (int t = 0; t < slave_num; t++) {
        p_thread_info[t] = new ThreadInfo(cmd_info_, false);
    }

    NGSTask(cmd_info_->in_file_name1_, fastqPool, p_thread_info);
#ifdef Verbose
    printf("all thrad done\n");
    printf("now merge thread info\n");
#endif
    vector < State * > pre_vec_state;
    vector < State * > aft_vec_state;

    for (int t = 0; t < slave_num; t++) {
        pre_vec_state.push_back(p_thread_info[t]->pre_state1_);
        aft_vec_state.push_back(p_thread_info[t]->aft_state1_);
    }
    auto pre_state = State::MergeStates(pre_vec_state);
    auto aft_state = State::MergeStates(aft_vec_state);
#ifdef Verbose
    if (cmd_info_->do_overrepresentation_) {
        printf("orp cost %lf\n", pre_state->GetOrpCost() + aft_state->GetOrpCost());
    }
    printf("merge done\n");
#endif
    printf("\nprint read (before filter) info :\n");
    State::PrintStates(pre_state);
    printf("\nprint read (after filter) info :\n");
    State::PrintStates(aft_state);
    printf("\n");
    if (cmd_info_->print_what_trimmed_)
        State::PrintAdapterToFile(aft_state);
    State::PrintFilterResults(aft_state);
    printf("\n");

    if (cmd_info_->do_overrepresentation_ && cmd_info_->print_ORP_seqs_) {
        auto hash_graph1 = pre_state->GetHashGraph();
        int hash_num1 = pre_state->GetHashNum();

        auto hash_graph2 = aft_state->GetHashGraph();
        int hash_num2 = aft_state->GetHashNum();

        int spg = cmd_info_->overrepresentation_sampling_;

        ofstream ofs;
        string srr_name = cmd_info_->in_file_name1_;
        srr_name = PaseFileName(srr_name);

        string out_name = "se_" + srr_name + "_before_ORP_sequences.txt";
        ofs.open(out_name, ifstream::out);
        ofs << "sequence count"
            << "\n";
        int cnt1 = 0;
        for (int i = 0; i < hash_num1; i++) {
            if (!overRepPassed(hash_graph1[i].seq, hash_graph1[i].cnt, spg)) continue;
            ofs << hash_graph1[i].seq << " " << hash_graph1[i].cnt << "\n";
            cnt1++;
        }
        ofs.close();
        printf("in %s (before filter) find %d possible overrepresented sequences (store in %s)\n", srr_name.c_str(),
                cnt1, out_name.c_str());


        out_name = "se_" + srr_name + "_after_ORP_sequences.txt";
        ofs.open(out_name, ifstream::out);
        ofs << "sequence count"
            << "\n";
        int cnt2 = 0;
        for (int i = 0; i < hash_num2; i++) {
            if (!overRepPassed(hash_graph2[i].seq, hash_graph2[i].cnt, spg)) continue;
            ofs << hash_graph2[i].seq << " " << hash_graph2[i].cnt << "\n";
            cnt2++;
        }
        ofs.close();
        printf("in %s (after filter) find %d possible overrepresented sequences (store in %s)\n", srr_name.c_str(),
                cnt2, out_name.c_str());
        printf("\n");
    }


    int *dupHist = NULL;
    double *dupMeanGC = NULL;
    double dupRate = 0.0;
    int histSize = 32;
    if (cmd_info_->state_duplicate_) {
        dupHist = new int[histSize];
        memset(dupHist, 0, sizeof(int) * histSize);
        dupMeanGC = new double[histSize];
        memset(dupMeanGC, 0, sizeof(double) * histSize);
        dupRate = duplicate_->statAll(dupHist, dupMeanGC, histSize);
        printf("Duplication rate (may be overestimated since this is SE data): %.5f %%\n", dupRate * 100.0);
        delete[] dupHist;
        delete[] dupMeanGC;
    }
    string srr_name = cmd_info_->in_file_name1_;
    srr_name = PaseFileName(srr_name);
    Repoter::ReportHtmlSe(srr_name + "_RabbitQCPlus.html", pre_state, aft_state, cmd_info_->in_file_name1_,
            dupRate * 100.0);


    delete pre_state;
    delete aft_state;

    delete fastqPool;


    for (int t = 0; t < slave_num; t++) {
        delete p_thread_info[t];
    }

    delete[] p_thread_info;
}


void SeQc::TGSTask(std::string file, rabbit::fq::FastqDataPool *fastq_data_pool, ThreadInfo **thread_infos){
    rabbit::fq::FastqFileReader *fqFileReader;
    rabbit::uint32 tmpSize = 1 << 20;
    if (cmd_info_->seq_len_ <= 200) tmpSize = 1 << 14;
    fqFileReader = new rabbit::fq::FastqFileReader(file, *fastq_data_pool, "", in_is_zip_, tmpSize);
    int64_t n_chunks = 0;
    qc_data para;
    para.cmd_info_ = cmd_info_;
    para.thread_info_ = thread_infos;
    athread_init();
    double t0 = GetTime();
    double tsum1 = 0;
    double tsum2 = 0;
    double tsum3 = 0;
    while (true) {
        double tt0 = GetTime();
        rabbit::fq::FastqDataChunk *fqdatachunk;
        fqdatachunk = fqFileReader->readNextChunk();
        if (fqdatachunk == NULL) break;
        n_chunks++;
        tsum1 += GetTime() - tt0;
        tt0 = GetTime();
        vector <neoReference> data;
        rabbit::fq::chunkFormat(fqdatachunk, data, true);
        tsum2 += GetTime() - tt0;
        printf("format cost %lf\n", GetTime() - tt0);
        tt0 = GetTime();
        para.data1_ = &data;
        //athread_spawn_tupled(slave_tgsfunc, &para);
        __real_athread_spawn((void *)slave_tgsfunc, &para, 1);
        athread_join();
        printf("slave cost %lf\n", GetTime() - tt0);
        tsum3 += GetTime() - tt0;
        fastq_data_pool->Release(fqdatachunk);
        printf("ppp\n");
    }
    athread_halt();
    printf("TGS tot cost %lf\n", GetTime() - t0);
    printf("TGS producer cost %lf\n", tsum1);
    printf("TGS format cost %lf\n", tsum2);
    printf("TGS slave cost %lf\n", tsum3);
    delete fqFileReader;
}


void SeQc::ProcessSeTGS() {

    auto *fastqPool = new rabbit::fq::FastqDataPool(8, 1 << 26);
    rabbit::core::TDataQueue<rabbit::fq::FastqDataChunk> queue1(16, 1);
    auto **p_thread_info = new ThreadInfo *[slave_num];
    for (int t = 0; t < slave_num; t++) {
        p_thread_info[t] = new ThreadInfo(cmd_info_, false);
    }
    thread producer(bind(&SeQc::ProducerSeFastqTask, this, cmd_info_->in_file_name1_, fastqPool, ref(queue1)));
    thread consumer(bind(&SeQc::ConsumerSeFastqTask, this, p_thread_info, fastqPool, ref(queue1)));
    producer.join();
    printf("producer done\n");
    consumer.join();
    printf("consumer done\n");

#ifdef Verbose
    printf("all thrad done\n");
    printf("now merge thread info\n");
#endif
    vector <TGSStats*> vec_state;

    for (int t = 0; t < slave_num; t++) {
        vec_state.push_back(p_thread_info[t]->TGS_state_);
    }
    auto mer_state = TGSStats::merge(vec_state);
#ifdef Verbose
    printf("merge done\n");
#endif
    printf("\nprint TGS state info :\n");

    //report3(mer_state);
    mer_state->CalReadsLens();

    mer_state->print();
    string srr_name = cmd_info_->in_file_name1_;
    srr_name = PaseFileName(srr_name);
    string command = cmd_info_->command_;
    Repoter::ReportHtmlTGS(srr_name + "_RabbitQCPlus.html", command, mer_state, cmd_info_->in_file_name1_);

    delete fastqPool;
    for (int t = 0; t < slave_num; t++) {
        delete p_thread_info[t];
    }
    delete[] p_thread_info;
}
void SeQc::ProcessSeTGSOneThread() {

    auto *fastqPool = new rabbit::fq::FastqDataPool(1, 1 << 26);
    auto **p_thread_info = new ThreadInfo *[slave_num];
    for (int t = 0; t < slave_num; t++) {
        p_thread_info[t] = new ThreadInfo(cmd_info_, false);
    }
    TGSTask(cmd_info_->in_file_name1_, fastqPool, p_thread_info);
#ifdef Verbose
    printf("all thrad done\n");
    printf("now merge thread info\n");
#endif
    vector <TGSStats*> vec_state;

    for (int t = 0; t < slave_num; t++) {
        vec_state.push_back(p_thread_info[t]->TGS_state_);
    }
    auto mer_state = TGSStats::merge(vec_state);
#ifdef Verbose
    printf("merge done\n");
#endif
    printf("\nprint TGS state info :\n");

    //report3(mer_state);
    mer_state->CalReadsLens();

    mer_state->print();
    string srr_name = cmd_info_->in_file_name1_;
    srr_name = PaseFileName(srr_name);
    string command = cmd_info_->command_;
    Repoter::ReportHtmlTGS(srr_name + "_RabbitQCPlus.html", command, mer_state, cmd_info_->in_file_name1_);

    delete fastqPool;
    for (int t = 0; t < slave_num; t++) {
        delete p_thread_info[t];
    }
    delete[] p_thread_info;
}
