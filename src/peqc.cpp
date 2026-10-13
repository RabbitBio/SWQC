// 
// Created by ylf9811 on 2021/7/6.
//

#include "peqc.h"
#include "swqc_runtime.h"
#include "ordered_input.h"
#include "swqc_log.h"
#include "ring_cursor.h"
#include "gzip_pipeline.h"

using namespace std;

static int open_output(const char *path, bool split, MPI_File *file)
{
    if (swqc_standard_gzip() && swqc_gzip_path(path)) {
        *file = MPI_FILE_NULL;
        return MPI_SUCCESS;
    }
    if (swqc_discard_output()) {
        *file = MPI_FILE_NULL;
        return MPI_SUCCESS;
    }
    MPI_Comm comm = split ? MPI_COMM_SELF : MPI_COMM_WORLD;
    if (swqc_posix_output()) {
        int rank, failed = 0;
        MPI_Comm_rank(comm, &rank);
        if (rank == 0) {
            int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
            if (fd < 0) failed = 1;
            else if (close(fd) != 0) failed = 1;
        }
        if (MPI_Bcast(&failed, 1, MPI_INT, 0, comm) != MPI_SUCCESS || failed)
            swqc_io_fail("cannot create POSIX output");
        *file = MPI_FILE_NULL;
        return MPI_SUCCESS;
    }
    int result = MPI_File_open(comm, path, MPI_MODE_CREATE | MPI_MODE_WRONLY,
                               MPI_INFO_NULL, file);
    if (result == MPI_SUCCESS) result = MPI_File_set_size(*file, 0);
    return result;
}



extern "C" {
#include <athread.h>
#include <pthread.h>
    void slave_ngspefunc();
    void slave_formatpefunc();
    void slave_peallfunc();
    void slave_swqc_output_fingerprint();
    void slave_decompressfunc();
    void slave_compressfunc();
}




void PrintRef(neoReference &ref) {
    printf("%.*s\n", ref.lname, (char *) ref.base + ref.pname);
    printf("%.*s\n", ref.lseq, (char *) ref.base + ref.pseq);
    printf("%.*s\n", ref.lstrand, (char *) ref.base + ref.pstrand);
    printf("%.*s\n", ref.lqual, (char *) ref.base + ref.pqual);
}

void SkipToLineEnd(char *data_, int64_t &pos_, const int64_t size_) {
    // cerr << "pos: " << pos_ << " size: " << size_ << endl;
    ASSERT(pos_ < size_);

    while (data_[pos_] != '\n' && data_[pos_] != '\r' && pos_ < size_) ++pos_;

    if (data_[pos_] == '\r' && pos_ < size_) {
        if (data_[pos_ + 1] == '\n') {
            //usesCrlf = true;
            ++pos_;
        }
    }
}


int64_t GetNextFastq(char *data_, int64_t pos_, const int64_t size_) {
    SkipToLineEnd(data_, pos_, size_);
    ++pos_;

    // find beginning of the next record
    while (data_[pos_] != '@') {
        SkipToLineEnd(data_, pos_, size_);
        ++pos_;
    }
    int64_t pos0 = pos_;

    SkipToLineEnd(data_, pos_, size_);
    ++pos_;

    if (data_[pos_] == '@')// previous one was a quality field
        return pos_;
    //-----[haoz:] is the following code necessary??-------------//
    SkipToLineEnd(data_, pos_, size_);
    ++pos_;
    if (data_[pos_] != '+') cout << "core dump is pos: " << pos_ << " char: " << data_[pos_] << endl;
    ASSERT(data_[pos_] == '+');// pos0 was the start of tag
    return pos0;
}

static bool use_in_mem = 0;
static bool use_out_mem = 0;

static bool use_swidx_file = 0;

#define use_mpi_file

char* OutMemData1;
char* OutMemData2;



/**
 * @brief Construct function
 * @param cmd_info1 : cmd information
 */
PeQc::PeQc(CmdInfo *cmd_info1, int my_rank_, int comm_size_) {
    my_rank = my_rank_;
    comm_size = comm_size_;
    part_sizes = new int64_t[comm_size];
    now_pos1_ = 0;
    now_pos2_ = 0;
    zip_now_pos1_ = 0;
    zip_now_pos2_ = 0;
    cmd_info_ = cmd_info1;
    filter_ = new Filter(cmd_info1);
    done_thread_number_ = 0;
    int out_block_nums = int(1.0 * cmd_info1->in_file_size1_ / cmd_info1->out_block_size_);
    out_queue_ = NULL;
    p_out_queue_ = NULL;
    //out_queue1_ = NULL;
    //out_queue2_ = NULL;
    in_is_zip_ = cmd_info1->in_file_name1_.find(".gz") != string::npos;
    out_is_zip_ = cmd_info1->out_file_name1_.find(".gz") != string::npos;
    // Standard gzip delivers/accepts raw FASTQ at the existing QC queues.
    if (swqc_standard_gzip()) in_is_zip_ = out_is_zip_ = false;

    if(in_is_zip_ && comm_size > 1) {
#ifdef USE_LIBDEFLATE
#else
        if(my_rank == 0) fprintf(stderr, "compress file input for multi-process is not support, please use -DUSE_LIBDEFLATE in makefile.\n");
        exit(0);
#endif
    }


    ifstream gFile;
    gFile.open(cmd_info1->in_file_name1_.c_str());
    gFile.seekg(0, ios_base::end);
    long long real_file_size = gFile.tellg();
    gFile.close();

    gFile.open(cmd_info1->in_file_name2_.c_str());
    gFile.seekg(0, ios_base::end);
    long long real_file_size2 = gFile.tellg();
    gFile.close();

    if(!in_is_zip_ && real_file_size != real_file_size2 && comm_size > 1) {
        fprintf(stderr, "PE in files size error %lld %lld\n", real_file_size, real_file_size2);
        exit(0);
    }


    int64_t start_pos, end_pos;
    if(in_is_zip_) {
#ifdef USE_CC_GZ
        for(int i = 0; i < SWQC_CPE_COUNT; i++) {
            cc_gz_in_buffer[i] = new char[BLOCK_SIZE];
        }
#endif
        vector<size_t> block_sizes;
        if(use_swidx_file) {
            ifstream iff_idx;
            iff_idx.open(cmd_info1->in_file_name1_ + ".swidx");
            size_t block_size = 0;
            while (iff_idx >> block_size) {
                block_sizes.push_back(block_size);
            }
            iff_idx.close();
        } else {
            ifstream iff_idx;
            iff_idx.open(cmd_info1->in_file_name1_, ios::binary | ios::ate);
            if (!iff_idx.is_open()) {
                cerr << "Failed to open file!" << endl;
                exit(0);
            }
            size_t file_size = iff_idx.tellg();
            size_t blocknum = 0;
            iff_idx.seekg(-static_cast<int>(sizeof(size_t)), ios::end);
            iff_idx.read(reinterpret_cast<char*>(&blocknum), sizeof(size_t));

            real_file_size -= (blocknum + 1) * sizeof(size_t);
            block_sizes.reserve(blocknum);
            iff_idx.seekg(-static_cast<int>((blocknum + 1) * sizeof(size_t)), ios::end);

            size_t block_size = 0;
            for (size_t i = 0; i < blocknum; ++i) {
                iff_idx.read(reinterpret_cast<char*>(&block_size), sizeof(size_t));
                block_sizes.push_back(block_size);
            }
            iff_idx.close();
        }

        if (block_sizes.empty()) {
            // Handle error or empty file scenario
            cerr << "Error: No data read from index file." << endl;
            exit(0);
        }
        int pre_size = ceil(1.0 * block_sizes.size() / comm_size);
        int start_line = pre_size * my_rank;
        int end_line = min(start_line + pre_size, (int) block_sizes.size());

        for (int i = 1; i < block_sizes.size(); i++) {
            block_sizes[i] += block_sizes[i - 1];
        }
        start_pos = (start_line == 0) ? 0 : block_sizes[start_line - 1];
        end_pos = (end_line == block_sizes.size()) ? real_file_size : block_sizes[end_line - 1];
        assert(block_sizes.back() == real_file_size);
        start_line_ = start_line;
        end_line_ = end_line;
    } else {
        int64_t pre_size = ceil(1.0 * real_file_size / comm_size);
        start_pos = pre_size * my_rank;
        end_pos = start_pos + pre_size;
        if(end_pos > real_file_size) end_pos = real_file_size;
    }


    if(in_is_zip_) {
        fprintf(stderr, "rank%d line: [%d %d]\n", my_rank, start_line_, end_line_);
    }


    int64_t right_pos;
    if(in_is_zip_ || swqc_standard_gzip()) {
        right_pos = 0;
    } else {
        FILE *pre_fp;
        pre_fp = fopen(cmd_info1->in_file_name1_.c_str(), "rb");
        fseek(pre_fp, start_pos, SEEK_SET);
        char *tmp_chunk = new char[1 << 20];
        int res_size = fread(tmp_chunk, sizeof(char), 1 << 20, pre_fp);
        MPI_Barrier(MPI_COMM_WORLD);
        if(my_rank == 0) right_pos = 0;
        else right_pos = GetNextFastq(tmp_chunk, 0, res_size);
        fclose(pre_fp);
    }

    MPI_Barrier(MPI_COMM_WORLD);
    long long now_pos = right_pos + start_pos;
    long long now_poss[comm_size];
    now_poss[0] = now_pos;
    if(my_rank) {
        MPI_Send(&now_pos, 1, MPI_LONG, 0, 0, MPI_COMM_WORLD);
    } else {
        for(int ii = 1; ii < comm_size; ii++) {
            MPI_Recv(&(now_poss[ii]), 1, MPI_LONG, ii, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        } 
    }
    MPI_Barrier(MPI_COMM_WORLD);
    if(my_rank == 0) {
        for(int ii = 1; ii < comm_size; ii++) {
            MPI_Send(now_poss, comm_size, MPI_LONG, ii, 0, MPI_COMM_WORLD);
        }
    } else {
        MPI_Recv(now_poss, comm_size, MPI_LONG, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    }
    MPI_Barrier(MPI_COMM_WORLD);
    for(int i = 0; i < comm_size; i++) {
        if(i == comm_size - 1) part_sizes[i] = real_file_size - now_poss[i];
        else part_sizes[i] = now_poss[i + 1] - now_poss[i];
    }

    start_pos_ = now_poss[my_rank];
    if(my_rank == 0 && start_pos_ != 0) fprintf(stderr, "GG size division\n");
    if(my_rank == comm_size - 1) end_pos_ = real_file_size;
    else end_pos_ = now_poss[my_rank + 1];

    p_out_queue_ = new vector<rabbit::fq::FastqDataPairChunk *>[Q_lim_pe];
    p_queueP1 = 0;
    p_queueP2 = 0;
    p_queueNumNow = 0;
    p_queueSizeLim = Q_lim_pe;

 
    if (cmd_info1->write_data_) {

        out_queue_ = new pair<CIPair, CIPair>[Q_lim_pe * SWQC_CPE_COUNT];
        queueP1 = 0;
        queueP2 = 0;
        queueNumNow = 0;
        queueSizeLim = Q_lim_pe * SWQC_CPE_COUNT;
        if(use_out_mem) {
            OutMemData1 = new char[128 << 20];
            OutMemData2 = new char[128 << 20];
        }
        if (out_is_zip_) {
            if (cmd_info1->use_pigz_) {
                fprintf(stderr, "use pigz TODO...\n");
                exit(0);
            } else {
#ifdef Verbose
                printf("open gzip stream1 %s\n", cmd_info1->out_file_name1_.c_str());
                printf("open gzip stream2 %s\n", cmd_info1->out_file_name2_.c_str());
#endif

#ifdef USE_LIBDEFLATE


                int err1 = open_output(cmd_info1->out_file_name1_.c_str(), cmd_info1->splitWrite_, &fh1);

                if (err1 != MPI_SUCCESS) {
                    char error_string[BUFSIZ];
                    int length_of_error_string, error_class;

                    MPI_Error_class(err1, &error_class);
                    MPI_Error_string(error_class, error_string, &length_of_error_string);

                    fprintf(stderr, "%3d: Couldn't open file1, MPI Error: %s\n", my_rank, error_string);

                    MPI_Abort(MPI_COMM_WORLD, err1);
                    exit(0);
                }


                int err2 = open_output(cmd_info1->out_file_name2_.c_str(), cmd_info1->splitWrite_, &fh2);

                if (err2 != MPI_SUCCESS) {
                    char error_string[BUFSIZ];
                    int length_of_error_string, error_class;

                    MPI_Error_class(err2, &error_class);
                    MPI_Error_string(error_class, error_string, &length_of_error_string);

                    fprintf(stderr, "%3d: Couldn't open file, MPI Error: %s\n", my_rank, error_string);

                    MPI_Abort(MPI_COMM_WORLD, err2);
                    exit(0);
                }
#else
                zip_out_stream1 = gzopen(cmd_info1->out_file_name1_.c_str(), "w");
                gzsetparams(zip_out_stream1, cmd_info1->compression_level_, Z_DEFAULT_STRATEGY);
                gzbuffer(zip_out_stream1, 1024 * 1024);
                zip_out_stream2 = gzopen(cmd_info1->out_file_name2_.c_str(), "w");
                gzsetparams(zip_out_stream2, cmd_info1->compression_level_, Z_DEFAULT_STRATEGY);
                gzbuffer(zip_out_stream2, 1024 * 1024);
#endif
            }
        } else {
#ifdef Verbose
            printf("open stream1 %s\n", cmd_info1->out_file_name1_.c_str());
            if (cmd_info_->interleaved_out_ == 0)
                printf("open stream2 %s\n", cmd_info1->out_file_name2_.c_str());
#endif



#ifdef use_mpi_file


            int err1 = open_output(cmd_info1->out_file_name1_.c_str(), cmd_info1->splitWrite_, &fh1);

            if (err1 != MPI_SUCCESS) {
                char error_string[BUFSIZ];
                int length_of_error_string, error_class;

                MPI_Error_class(err1, &error_class);
                MPI_Error_string(error_class, error_string, &length_of_error_string);

                fprintf(stderr, "%3d: Couldn't open file, MPI Error: %s\n", my_rank, error_string);

                MPI_Abort(MPI_COMM_WORLD, err1);
                exit(0);
            }

            if (cmd_info_->interleaved_out_ == 0) {


                int err2 = open_output(cmd_info1->out_file_name2_.c_str(), cmd_info1->splitWrite_, &fh2);

                if (err2 != MPI_SUCCESS) {
                    char error_string[BUFSIZ];
                    int length_of_error_string, error_class;

                    MPI_Error_class(err2, &error_class);
                    MPI_Error_string(error_class, error_string, &length_of_error_string);

                    fprintf(stderr, "%3d: Couldn't open file, MPI Error: %s\n", my_rank, error_string);

                    MPI_Abort(MPI_COMM_WORLD, err2);
                    exit(0);
                }               
            }


#else
            if(my_rank == 0) {
                printf("pre real file1 size %lld\n", real_file_size);
                int fd = open(cmd_info1->out_file_name1_.c_str(), O_CREAT | O_TRUNC | O_RDWR | O_EXCL, 0644);
                ftruncate(fd, sizeof(char) * real_file_size);
                out_stream1_ = fdopen(fd, "w");
            } else {
                int found = 0;
                do {
                    if(-1 == access(cmd_info1->out_file_name1_.c_str(), F_OK)) {
                        if(ENOENT == errno) {
                            //cerr << "waiting file be created..." << endl;
                            usleep(10000);
                        } else {
                            //cerr << "file open GG" << endl;
                            usleep(10000);
                            //exit(0);
                        }
                    } else {
                        out_stream1_ = fopen(cmd_info1->out_file_name1_.c_str(), "r+b");   
                        found = 1;
                    }
                } while(found == 0);
            }

            if (cmd_info_->interleaved_out_ == 0){

                if(my_rank == 0) {
                    printf("pre real file2 size %lld\n", real_file_size2);
                    int fd = open(cmd_info1->out_file_name2_.c_str(), O_CREAT | O_TRUNC | O_RDWR | O_EXCL, 0644);
                    ftruncate(fd, sizeof(char) * real_file_size2);
                    out_stream2_ = fdopen(fd, "w");
                } else {
                    int found = 0;
                    do {

                        if(-1 == access(cmd_info1->out_file_name2_.c_str(), F_OK)) {
                            if(ENOENT == errno) {
                                //cerr << "waiting file be created..." << endl;
                                usleep(10000);
                            } else {
                                //cerr << "file open GG" << endl;
                                usleep(10000);
                                //exit(0);
                            }
                        } else {
                            out_stream2_ = fopen(cmd_info1->out_file_name2_.c_str(), "r+b");   
                            found = 1;
                        }
                    } while(found == 0);
                }
                //MPI_Barrier(MPI_COMM_WORLD);
            }
#endif
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
        fprintf(stderr, "use pugz TODO...\n");
        exit(0);
 
    }
    if (cmd_info1->use_pigz_) {
        fprintf(stderr, "use pigz TODO...\n");
        exit(0);
    }

    pugzDone1 = 0;
    pugzDone2 = 0;
    writerDone1 = 0;
    writerDone2 = 0;
}


PeQc::~PeQc() {
    delete[] part_sizes;
    delete filter_;
    delete[] p_out_queue_;
    if (cmd_info_->write_data_) {
        delete []out_queue_;
    }
    if(in_is_zip_) {
#ifdef USE_CC_GZ
        for(int i = 0; i < SWQC_CPE_COUNT; i++) {
            delete []cc_gz_in_buffer[i];
        }
#endif
    }
    if (cmd_info_->state_duplicate_) {
        delete duplicate_;
    }
    if (cmd_info_->add_umi_) {
        delete umier_;
    }
}


string PeQc::Read2String(neoReference &ref) {
    return string((char *) ref.base + ref.pname, ref.lname) + "\n" +
        string((char *) ref.base + ref.pseq, ref.lseq) + "\n" +
        string((char *) ref.base + ref.pstrand, ref.lstrand) + "\n" +
        string((char *) ref.base + ref.pqual, ref.lqual) + "\n";
}

void PeQc::Read2Chars(neoReference &ref, char *out_data, int &pos) {
    //PrintRef(ref);
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


static rabbit::fq::FastqFileReader *fqFileReader;

void PeQc::ProducerPeFastqTask64(string file, string file2, rabbit::fq::FastqDataPool *fastqPool) {
    double t0 = GetTime();
    rabbit::uint32 tmpSize = SWAP1_SIZE;
    if (cmd_info_->seq_len_ <= 200) tmpSize = SWAP2_SIZE;
    int64_t n_chunks = 0;

    double t_sum1 = 0;
    double t_sum2 = 0;
    bool is_first = 1;
    int64_t tot_size = 0;
    vector<rabbit::fq::FastqDataPairChunk *> tmp_chunks;
    OrderedPairReader *ordered = ordered_reader_.get();
    if (serial_stages_.enabled()) {
        bool eof = false;
        while (serial_stages_.reader()) {
            double tt = GetTime();
            tmp_chunks.clear();
            for (int i = 0; i < SWQC_CPE_COUNT && !eof; ++i) {
                auto *pair = gzip_reader_ ? gzip_reader_->next(fastqPool) : ordered->next(fastqPool);
                if (!pair) { eof = true; break; }
                tmp_chunks.push_back(pair);
            }
            t_sum1 += GetTime() - tt;
            p_out_queue_[swqc_next_ring_slot(p_queueP2, p_queueSizeLim)] = tmp_chunks;
            ++p_queueNumNow;
            serial_stages_.read_done();
        }
        swqc_log_fprintf(stderr, "producer%d sum1 cost %lf\n", my_rank, t_sum1);
        return;
    }
    while (true) {
        double tt0 = GetTime();
        rabbit::fq::FastqDataPairChunk *fqdatachunk;
        int64_t offset;
        if(is_first) {
            offset = 0;
            for(int i = 0; i < my_rank; i++)
                offset += part_sizes[i];
        } else {
            offset = -1;
        }
        is_first = 0;
        fqdatachunk = gzip_reader_ ? gzip_reader_->next(fastqPool) :
            (ordered ? ordered->next(fastqPool) : fqFileReader->readNextPairChunk());
        t_sum1 += GetTime() - tt0;

        tt0 = GetTime();
        if ((fqdatachunk == NULL) || (fqdatachunk != NULL && fqdatachunk->left_part->size == 1ll << 32)) {
            if(tmp_chunks.size()) {
//                fprintf(stderr, "producer %d put last %d\n", my_rank, tmp_chunks.size());
                while (p_queueNumNow >= p_queueSizeLim) {
                    usleep(1000);
                    //fprintf(stderr, "producer %d waiting2\n", my_rank);
                }
                p_out_queue_[swqc_next_ring_slot(p_queueP2, p_queueSizeLim)] = tmp_chunks;
                p_queueNumNow++;
                tmp_chunks.clear();
                n_chunks++;
            }
//            fprintf(stderr, "producer %d stop, tot chunk size %d\n", my_rank, n_chunks);
            tmp_chunks.clear();
            while (p_queueNumNow >= p_queueSizeLim) {
                usleep(100);
            }
            p_out_queue_[swqc_next_ring_slot(p_queueP2, p_queueSizeLim)] = tmp_chunks;
            p_queueNumNow++;
            break;
        }
        tot_size += fqdatachunk->left_part->size;
        tmp_chunks.push_back(fqdatachunk); 
//        fprintf(stderr, "producer %d read a chunk\n", my_rank);
        if(tmp_chunks.size() == SWQC_CPE_COUNT) {
//            fprintf(stderr, "producer %d put SWQC_CPE_COUNT\n", my_rank);
            while (p_queueNumNow >= p_queueSizeLim) {
                usleep(1000);
            }
            p_out_queue_[swqc_next_ring_slot(p_queueP2, p_queueSizeLim)] = tmp_chunks;
            p_queueNumNow++;
            tmp_chunks.clear();
            n_chunks++;
        }
        t_sum2 += GetTime() - tt0;
    }
    //printf("totsize %lld\n", tot_size);

    swqc_log_fprintf(stderr, "producer%d sum1 cost %lf\n", my_rank, t_sum1);
    swqc_log_fprintf(stderr, "producer%d sum2 cost %lf\n", my_rank, t_sum2);
    swqc_log_fprintf(stderr, "producer%d cost %lf\n", my_rank, GetTime() - t0);
    swqc_log_fprintf(stderr, "producer %d in func done\n", my_rank);
    //dq.SetCompleted();
}



struct formatpe_data {
    vector <neoReference> *data1;
    vector <neoReference> *data2;
    rabbit::fq::FastqDataPairChunk *fqdatachunk;
    uint64_t *res;
    int fqSize;
    int my_rank;
};

struct PeMerge_data {
    char* out_data1[SWQC_CPE_COUNT];
    char* out_data2[SWQC_CPE_COUNT];
    int out_lens1[SWQC_CPE_COUNT] = {0};
    int out_lens2[SWQC_CPE_COUNT] = {0};
    std::vector <neoReference> *pass_data1[SWQC_CPE_COUNT];
    std::vector <neoReference> *pass_data2[SWQC_CPE_COUNT];
    int pass_data_size1[SWQC_CPE_COUNT];
    int pass_data_size2[SWQC_CPE_COUNT];
};

struct PeAll_data{
    formatpe_data para1[SWQC_CPE_COUNT];
    qc_data *para2;
    PeMerge_data *para3;
    int out_len_slave1[SWQC_CPE_COUNT];
    int out_len_slave2[SWQC_CPE_COUNT];
    bool write_data;
};


double tsum1 = 0;
double tsum2 = 0;
double tsum3 = 0;
double tsum4 = 0;
double tsum4_1 = 0;
double tsum4_1_1 = 0;
double tsum4_1_2 = 0;
double tsum4_1_3 = 0;
double tsum4_2 = 0;
double tsum4_3 = 0;
double tsum4_4 = 0;
double tsum5 = 0;
double tsum6 = 0;
double tsum7 = 0;
double t_wait_producer = 0;
double t_format = 0;
double t_resize = 0;
double t_decom = 0;
double t_ngsfunc = 0;
double t_slave_gz = 0;
double t_slave_gz2 = 0;
double t_push_q = 0;
double t_copy_data = 0;
double t_output_digest = 0;
double t_output_arrival = 0;


struct ConsumerWriteInfoPE{
    char* buffer;
    char* buffer2;
    int buffer_len;
    int buffer_len2;
    long long file_offset;
    long long file_offset2;
};

vector<ConsumerWriteInfoPE> writeInfosPE;

static int pe_pre_out_len_slave1[SWQC_CPE_COUNT];
static int pe_pre_out_len_slave2[SWQC_CPE_COUNT];

static int pe_pre_pass_data_size1[SWQC_CPE_COUNT];
static int pe_pre_pass_data_size2[SWQC_CPE_COUNT];

void PeQc::ProcessFormatQCWrite(bool &allIsNull, vector <neoReference> *data1, vector <neoReference> *data2,
                          vector <neoReference> *pass_data1, vector <neoReference> *pass_data2,
                          vector <neoReference> *pre_pass_data1, vector <neoReference> *pre_pass_data2,
                          vector <rabbit::fq::FastqDataPairChunk *> fqdatachunks, vector <rabbit::fq::FastqDataPairChunk *> pre_fqdatachunks,
                          qc_data *para, rabbit::fq::FastqDataPool *fastq_data_pool) {
    const char *profile_setting = getenv("SWQC_PROFILE_ROUNDS");
    const bool profile_round = profile_setting && strcmp(profile_setting, "1") == 0;
    const double round_started = GetTime(), cpe_before = tsum3;
    const double coord_before = tsum4, arrival_before = t_output_arrival;
    const double release_before = tsum7;
    const uint64_t pairs_before = processed_pairs_;
    double tt0 = GetTime();
    formatpe_data para2[SWQC_CPE_COUNT];
    uint64_t counts[SWQC_CPE_COUNT] = {0};
    int fq_size = fqdatachunks.size();
    for(int i = 0; i < SWQC_CPE_COUNT; i++) {
        para2[i].fqSize = fq_size;
        para2[i].my_rank = my_rank;
    }
    for(int i = 0; i < fq_size; i++) {
        para2[i].data1 = &(data1[i]);
        para2[i].data2 = &(data2[i]);
        para2[i].fqdatachunk = fqdatachunks[i];
        para2[i].res = counts;
        para2[i].fqSize = fq_size;
    }
    for(int i = 0; i < SWQC_CPE_COUNT; i++) {
        para->data1_[i] = &data1[i];
        para->data2_[i] = &data2[i];
        para->pass_data1_[i] = &pass_data1[i];
        para->pass_data2_[i] = &pass_data2[i];
    }
    tsum1 += GetTime() - tt0;

    tt0 = GetTime();
    PeMerge_data pe_merge_data;
    PeAll_data pe_all_data;
    pe_all_data.write_data = cmd_info_->write_data_;
    int out_lens1[SWQC_CPE_COUNT] = {0};
    int out_lens2[SWQC_CPE_COUNT] = {0};
    char *out_data1[SWQC_CPE_COUNT];
    char *out_data2[SWQC_CPE_COUNT];
    for(int i = 0; i < SWQC_CPE_COUNT; i++) {
        pe_all_data.para1[i] = para2[i];
        pe_all_data.out_len_slave1[i] = 0;
        pe_all_data.out_len_slave2[i] = 0;
        if(cmd_info_->write_data_) {
            out_lens1[i] = pe_pre_out_len_slave1[i];
            if(out_lens1[i]) out_data1[i] = new char[out_lens1[i]];
            else out_data1[i] = (char *)(NULL);
            out_lens2[i] = pe_pre_out_len_slave2[i];
            if(out_lens2[i]) out_data2[i] = new char[out_lens2[i]];
            else out_data2[i] = (char *)(NULL);
        }
    }
    for(int i = 0; i < SWQC_CPE_COUNT; i++) {
        pe_merge_data.out_data1[i] = out_data1[i];
        pe_merge_data.out_lens1[i] = out_lens1[i];
        pe_merge_data.pass_data1[i] = &(pre_pass_data1[i]);
        pe_merge_data.pass_data_size1[i] = pe_pre_pass_data_size1[i];

        pe_merge_data.out_data2[i] = out_data2[i];
        pe_merge_data.out_lens2[i] = out_lens2[i];
        pe_merge_data.pass_data2[i] = &(pre_pass_data2[i]);
        pe_merge_data.pass_data_size2[i] = pe_pre_pass_data_size2[i];
    }
    pe_all_data.para2 = para;
    pe_all_data.para3 = &pe_merge_data;
    tsum2 += GetTime() - tt0;

    tt0 = GetTime();
    swqc_runtime_run((void *)slave_peallfunc, &pe_all_data);
    tsum3 += GetTime() - tt0;
    for (int i = 0; i < SWQC_CPE_COUNT; ++i) processed_pairs_ += counts[i];


    tt0 = GetTime();
    if (cmd_info_->write_data_) {
        long long now_pos_base1 = 0;
        long long now_pos_base2 = 0;
        bool use_consumer_libdeflate = 0;

#ifdef CONSUMER_USE_LIBDEFLATE
        use_consumer_libdeflate = 1;
#endif
        if(!out_is_zip_ || use_consumer_libdeflate == 0) {
            int out_len1 = 0;
            for(int i = 0; i < SWQC_CPE_COUNT; i++) {
                out_len1 += out_lens1[i];
            }
            if(cmd_info_->splitWrite_ == 0) {
                int now_size1 = out_len1;
                int now_sizes1[comm_size];
                now_sizes1[0] = now_size1;
                const double arrival_started = GetTime();
                MPI_Barrier(MPI_COMM_WORLD);
                t_output_arrival += GetTime() - arrival_started;
                if(my_rank) {
                    MPI_Send(&now_size1, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
                } else {
                    for(int ii = 1; ii < comm_size; ii++) {
                        MPI_Recv(&(now_sizes1[ii]), 1, MPI_INT, ii, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                    }
                }
                MPI_Barrier(MPI_COMM_WORLD);
                if(my_rank == 0) {
                    for(int ii = 1; ii < comm_size; ii++) {
                        MPI_Send(now_sizes1, comm_size, MPI_INT, ii, 0, MPI_COMM_WORLD);
                    }
                } else {
                    MPI_Recv(now_sizes1, comm_size, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                }
                MPI_Barrier(MPI_COMM_WORLD);
                long long pre_sizes1[comm_size];
                pre_sizes1[0] = 0;
                for(int ii = 1; ii < comm_size; ii++) {
                    pre_sizes1[ii] = pre_sizes1[ii - 1] + now_sizes1[ii - 1];
                }

                now_pos_base1 = now_pos1_ + pre_sizes1[my_rank];

                for(int ii = 0; ii < comm_size; ii++) {
                    now_pos1_ += now_sizes1[ii];
                }


                int out_len2 = 0;
                for(int i = 0; i < SWQC_CPE_COUNT; i++) {
                    out_len2 += out_lens2[i];
                }

                int now_size2 = out_len2;
                int now_sizes2[comm_size];
                now_sizes2[0] = now_size2;
                MPI_Barrier(MPI_COMM_WORLD);
                if(my_rank) {
                    MPI_Send(&now_size2, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
                } else {
                    for(int ii = 1; ii < comm_size; ii++) {
                        MPI_Recv(&(now_sizes2[ii]), 1, MPI_INT, ii, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                    }
                }
                MPI_Barrier(MPI_COMM_WORLD);
                if(my_rank == 0) {
                    for(int ii = 1; ii < comm_size; ii++) {
                        MPI_Send(now_sizes2, comm_size, MPI_INT, ii, 0, MPI_COMM_WORLD);
                    }
                } else {
                    MPI_Recv(now_sizes2, comm_size, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                }
                MPI_Barrier(MPI_COMM_WORLD);
                long long pre_sizes2[comm_size];
                pre_sizes2[0] = 0;
                for(int ii = 1; ii < comm_size; ii++) {
                    pre_sizes2[ii] = pre_sizes2[ii - 1] + now_sizes2[ii - 1];
                }

                now_pos_base2 = now_pos2_ + pre_sizes2[my_rank];

                for(int ii = 0; ii < comm_size; ii++) {
                    now_pos2_ += now_sizes2[ii];
                }
            }


        }
        writeInfosPE.clear();
        for(int i = 0; i < SWQC_CPE_COUNT; i++) {
            writeInfosPE.push_back({out_data1[i], out_data2[i], out_lens1[i], out_lens2[i], now_pos_base1, now_pos_base2});
        }
    }
    for(int i = 0; i < SWQC_CPE_COUNT; i++) {
        pe_pre_out_len_slave1[i] = pe_all_data.out_len_slave1[i];
        pe_pre_out_len_slave2[i] = pe_all_data.out_len_slave2[i];
        pe_pre_pass_data_size1[i] = pe_merge_data.pass_data_size1[i]; 
        pe_pre_pass_data_size2[i] = pe_merge_data.pass_data_size2[i]; 
    }
    tsum4 += GetTime() - tt0;

    tt0 = GetTime();
    int isNull = 1;
    int all_stop = 1;
    for(int i = 0; i < SWQC_CPE_COUNT; i++) {
        if(counts[i]) isNull = 0;
    }
    if(cmd_info_->splitWrite_ == 0) {
        int isNulls[comm_size];
        isNulls[0] = isNull;
        MPI_Barrier(MPI_COMM_WORLD);
        if(my_rank) {
            MPI_Send(&isNull, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
        } else {
            for(int ii = 1; ii < comm_size; ii++) {
                MPI_Recv(&(isNulls[ii]), 1, MPI_INT, ii, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            }
        }
        MPI_Barrier(MPI_COMM_WORLD);
        if(my_rank == 0) {
            for(int ii = 1; ii < comm_size; ii++) {
                MPI_Send(isNulls, comm_size, MPI_INT, ii, 0, MPI_COMM_WORLD);
            }
        } else {
            MPI_Recv(isNulls, comm_size, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        }
        for(int ii = 0; ii < comm_size; ii++) {
            if(isNulls[ii] == 0) all_stop = 0;
        }
        if(all_stop) allIsNull = 1;
    } else {
        allIsNull = isNull;
    }
    tsum5 += GetTime() - tt0;


    tt0 = GetTime();
    if(cmd_info_->splitWrite_ == 0 && !all_stop && isNull && !serial_stages_.enabled()) {
        swqc_log_fprintf(stderr, "consumer%d push null\n", my_rank);
        vector<rabbit::fq::FastqDataPairChunk *> tmp_chunks;
        tmp_chunks.clear();
        while (p_queueNumNow >= p_queueSizeLim) {
            usleep(100);
        }
        p_out_queue_[swqc_next_ring_slot(p_queueP2, p_queueSizeLim)] = tmp_chunks;
        p_queueNumNow++;
    }
    tsum6 += GetTime() - tt0;

    tt0 = GetTime();
    const rabbit::fq::FastqDataChunk *released_parts[SWQC_CPE_COUNT * 2];
    size_t released_count = 0;
    for(int i = 0; i < SWQC_CPE_COUNT; i++) {
        if(pre_fqdatachunks[i] != NULL) {
            released_parts[released_count++] = pre_fqdatachunks[i]->left_part;
            released_parts[released_count++] = pre_fqdatachunks[i]->right_part;
        }
    }
    fastq_data_pool->ReleaseBatch(released_parts, released_count);
    for (auto *pair : pre_fqdatachunks) delete pair;
    tsum7 += GetTime() - tt0;
    if (profile_round) {
        swqc_log_fprintf(stderr, "[SWQC processor round] rank=%d round=%llu pairs=%llu total_s=%.6f cpe_s=%.6f arrival_s=%.6f coord_s=%.6f release_s=%.6f\n",
            my_rank, (unsigned long long)profile_round_,
            (unsigned long long)(processed_pairs_ - pairs_before), GetTime() - round_started,
            tsum3 - cpe_before, t_output_arrival - arrival_before,
            tsum4 - coord_before, tsum7 - release_before);
    }
    ++profile_round_;
}

inline void gather_and_sort_vectors_pe(int rank, int comm_size, int out_round, vector<pair<int, size_t>>& local_vec, int root) {
    int local_size = local_vec.size();
    vector<int> send_data(local_size * 2);
    for (int i = 0; i < local_size; ++i) {
        send_data[2 * i] = local_vec[i].first;
        send_data[2 * i + 1] = static_cast<int>(local_vec[i].second);  // Assuming size_t can be safely cast to int
    }

    // Root process initializes a vector to hold all received data
    vector<int> recv_data;
    if (rank == root) {
        recv_data.reserve(local_size * 2 * comm_size);  // Reserve enough space
    }

    // Root process receives data from all other processes
    if (rank == root) {
        vector<int> buffer(local_size * 2);
        for (int i = 0; i < comm_size; ++i) {
            if (i == root) {
                // Include the root's own data
                recv_data.insert(recv_data.end(), send_data.begin(), send_data.end());
            } else {
                MPI_Recv(buffer.data(), local_size * 2, MPI_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                recv_data.insert(recv_data.end(), buffer.begin(), buffer.end());
            }
        }
    } else {
        // Other processes send their data to the root
        MPI_Send(send_data.data(), local_size * 2, MPI_INT, root, 0, MPI_COMM_WORLD);
    }

    // Root process sorts all received data
    if (rank == root) {
        vector<pair<int, size_t>> all_data;
        for (int i = 0; i < recv_data.size() / 2; ++i) {
            all_data.emplace_back(recv_data[2 * i], static_cast<size_t>(recv_data[2 * i + 1]));
        }
        sort(all_data.begin(), all_data.end());

        //// Print sorted data for verification
        //for (const auto& p : all_data) {
        //    cout << "(" << p.first << ", " << p.second << ") ";
        //}
        //cout << endl;

        // Optionally clear and repopulate local_vec
        local_vec.clear();
        for (const auto& item : all_data) {
            local_vec.push_back(item);
        }
    }
}


void PeQc::ConsumerPeFastqTask64(ThreadInfo **thread_infos, rabbit::fq::FastqDataPool *fastqPool) {
    rabbit::int64 id = 0;
    rabbit::fq::FastqDataPairChunk *fqdatachunk;
    qc_data para;
    para.cmd_info_ = cmd_info_;
    para.thread_info_ = thread_infos;
    para.bit_len = 0;
    para.duplicate_ = duplicate_;

    bool allIsNull = 0;
    if(cmd_info_->state_duplicate_) {
        para.bit_len = duplicate_->key_len_base_;
    }
    swqc_runtime_init();

    double t0 = GetTime();

    vector<rabbit::fq::FastqDataPairChunk *> fqdatachunks;
    vector<rabbit::fq::FastqDataPairChunk *> pre_fqdatachunks;
    int out_round = 0;
    vector<neoReference> data1[SWQC_CPE_COUNT];
    vector<neoReference> data2[SWQC_CPE_COUNT];
    vector<neoReference> pass_data1[SWQC_CPE_COUNT];
    vector<neoReference> pass_data2[SWQC_CPE_COUNT];
    vector<neoReference> pre_pass_data1[SWQC_CPE_COUNT];
    vector<neoReference> pre_pass_data2[SWQC_CPE_COUNT];
    int max_data_size = BLOCK_SIZE / (cmd_info_->seq_len_ * 2);
    struct VectorInit {
        vector<neoReference> *data1, *data2, *pass1, *pass2, *previous1, *previous2;
        int capacity;
        bool write;
    } init = {data1, data2, pass_data1, pass_data2, pre_pass_data1, pre_pass_data2,
              max_data_size, cmd_info_->write_data_};
    pre_fqdatachunks.assign(SWQC_CPE_COUNT, NULL);
    double vector_init_started = GetTime();
    swqc_host_for(SWQC_CPE_COUNT, [](void *opaque, int, size_t begin, size_t end) {
        auto &v = *static_cast<VectorInit *>(opaque);
        for (size_t i = begin; i < end; ++i) {
            v.data1[i].resize(v.capacity);
            v.data2[i].resize(v.capacity);
            if (v.write) {
                v.pass1[i].resize(v.capacity);
                v.pass2[i].resize(v.capacity);
                v.previous1[i].resize(v.capacity);
                v.previous2[i].resize(v.capacity);
            }
        }
    }, &init);
    swqc_log_fprintf(stderr, "[SWQC host initialization] rank=%d workers=%d vector_s=%.6f\n",
            my_rank, swqc_host_threads(), GetTime() - vector_init_started);
    while(true) {
        if (serial_stages_.enabled()) {
            if (!serial_stages_.processor()) break;
            // Every rank must finish reading before any CPE enters the kernel.
            MPI_Barrier(MPI_COMM_WORLD);
            serial_stages_.processor_ready();
        }
        double tt0 = GetTime();
        while (p_queueNumNow == 0) {
            usleep(1000);
        }
        t_wait_producer += GetTime() - tt0;


        tt0 = GetTime();
        fqdatachunks = p_out_queue_[swqc_next_ring_slot(p_queueP1, p_queueSizeLim)];
        p_queueNumNow--;

        for(int i = fqdatachunks.size(); i < SWQC_CPE_COUNT; i++) {
            fqdatachunks.push_back(NULL);
        }

#ifdef USE_CC_GZ
#ifdef USE_LIBDEFLATE
        if(in_is_zip_) {

            //left chunk
            {
                Para degz_paras[SWQC_CPE_COUNT];
                size_t out_size[SWQC_CPE_COUNT] = {0};
                for (int i = 0; i < SWQC_CPE_COUNT; i++) {
                    if(fqdatachunks[i] == NULL) {
                        degz_paras[i].in_buffer = NULL;
                        degz_paras[i].in_size = 0;
                        continue;
                    }
                    memcpy(cc_gz_in_buffer[i], fqdatachunks[i]->left_part->data.Pointer(), fqdatachunks[i]->left_part->size);
                    degz_paras[i].in_buffer = cc_gz_in_buffer[i];
                    degz_paras[i].out_buffer = (char*)fqdatachunks[i]->left_part->data.Pointer();
                    degz_paras[i].in_size = fqdatachunks[i]->left_part->size;
                    degz_paras[i].out_size = &(out_size[i]);
                }


                {
                    lock_guard <mutex> guard(globalMutex);
                    __real_athread_spawn((void *) slave_decompressfunc, degz_paras, 1);
                    athread_join();
                }


                for (int i = 0; i < SWQC_CPE_COUNT; i++) {
                    if(fqdatachunks[i] == NULL) {
                    } else {
                        if(out_size[i]) fqdatachunks[i]->left_part->size = out_size[i] - 1;
                        else fqdatachunks[i]->left_part->size = out_size[i];
                    }
                }

            }

            //right chunk
            {
                Para degz_paras[SWQC_CPE_COUNT];
                size_t out_size[SWQC_CPE_COUNT] = {0};
                for (int i = 0; i < SWQC_CPE_COUNT; i++) {
                    if(fqdatachunks[i] == NULL) {
                        degz_paras[i].in_buffer = NULL;
                        degz_paras[i].in_size = 0;
                        continue;
                    }
                    memcpy(cc_gz_in_buffer[i], fqdatachunks[i]->right_part->data.Pointer(), fqdatachunks[i]->right_part->size);
                    degz_paras[i].in_buffer = cc_gz_in_buffer[i];
                    degz_paras[i].out_buffer = (char*)fqdatachunks[i]->right_part->data.Pointer();
                    degz_paras[i].in_size = fqdatachunks[i]->right_part->size;
                    degz_paras[i].out_size = &(out_size[i]);
                }


                {
                    lock_guard <mutex> guard(globalMutex);
                    __real_athread_spawn((void *) slave_decompressfunc, degz_paras, 1);
                    athread_join();
                }


                for (int i = 0; i < SWQC_CPE_COUNT; i++) {
                    if(fqdatachunks[i] == NULL) {
                    } else {
                        if(out_size[i]) fqdatachunks[i]->right_part->size = out_size[i] - 1;
                        else fqdatachunks[i]->right_part->size = out_size[i];
                    }
                }

            }

        }
#endif
#endif

        t_decom += GetTime() - tt0;

        tt0 = GetTime();
        ProcessFormatQCWrite(allIsNull, data1, data2, pass_data1, pass_data2, pre_pass_data1, pre_pass_data2, fqdatachunks, pre_fqdatachunks, &para, fastqPool);
        t_ngsfunc += GetTime() - tt0;

        //fprintf(stderr, "rank%d pending write size %d\n", my_rank, writeInfosPE.size());
#ifdef CONSUMER_USE_LIBDEFLATE
        tt0 = GetTime();
        if (cmd_info_->write_data_ && out_is_zip_) {
            Para paras[SWQC_CPE_COUNT];
            size_t out_size[SWQC_CPE_COUNT] = {0};
            for(int i = 0; i < writeInfosPE.size(); i++) {
                paras[i].in_buffer = writeInfosPE[i].buffer;
                paras[i].out_buffer = new char[BLOCK_SIZE];
                paras[i].in_size = writeInfosPE[i].buffer_len;
                paras[i].out_size = &(out_size[i]);
                paras[i].level = 1;
            }
            for(int i = writeInfosPE.size(); i < SWQC_CPE_COUNT; i++) {
                paras[i].in_size = 0;
            }
            double tt00 = GetTime();
            {
                lock_guard<mutex> guard(globalMutex);
                __real_athread_spawn((void *)slave_compressfunc, paras, 1);
                athread_join();
            }
            t_slave_gz2 += GetTime() - tt00;
            for(int i = 0; i < SWQC_CPE_COUNT; i++) {
                out_gz_block_sizes1.push_back(make_pair(out_round * comm_size * SWQC_CPE_COUNT + my_rank * SWQC_CPE_COUNT + i, out_size[i]));
            }

            int sum_buffer_len1 = 0;
            for(int i = 0; i < writeInfosPE.size(); i++) {
                delete[] writeInfosPE[i].buffer;
                writeInfosPE[i].buffer = paras[i].out_buffer;
                writeInfosPE[i].buffer_len = out_size[i];
                sum_buffer_len1 += out_size[i];
            }
            if(cmd_info_->splitWrite_ == 0) {
                int now_size = sum_buffer_len1;
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
                long long now_pos_base1 = zip_now_pos1_ + pre_sizes[my_rank];
                MPI_Barrier(MPI_COMM_WORLD);
                for(int ii = 0; ii < comm_size; ii++) {
                    zip_now_pos1_ += now_sizes[ii];
                }
                for(int i = 0; i < writeInfosPE.size(); i++) {
                    writeInfosPE[i].file_offset = now_pos_base1;
                }
            }


            Para paras2[SWQC_CPE_COUNT];
            size_t out_size2[SWQC_CPE_COUNT] = {0};
            for(int i = 0; i < writeInfosPE.size(); i++) {
                paras2[i].in_buffer = writeInfosPE[i].buffer2;
                paras2[i].out_buffer = new char[BLOCK_SIZE];
                paras2[i].in_size = writeInfosPE[i].buffer_len2;
                paras2[i].out_size = &(out_size2[i]);
                paras2[i].level = 1;
            }
            for(int i = writeInfosPE.size(); i < SWQC_CPE_COUNT; i++) {
                paras2[i].in_size = 0;
            }
            tt00 = GetTime();
            {
                lock_guard<mutex> guard(globalMutex);
                __real_athread_spawn((void *)slave_compressfunc, paras2, 1);
                athread_join();
            }
            t_slave_gz2 += GetTime() - tt00;
            for(int i = 0; i < SWQC_CPE_COUNT; i++) {
                out_gz_block_sizes2.push_back(make_pair(out_round * comm_size * SWQC_CPE_COUNT + my_rank * SWQC_CPE_COUNT + i, out_size2[i]));
            }
            out_round++;

            int sum_buffer_len2 = 0;
            for(int i = 0; i < writeInfosPE.size(); i++) {
                delete[] writeInfosPE[i].buffer2;
                writeInfosPE[i].buffer2 = paras2[i].out_buffer;
                writeInfosPE[i].buffer_len2 = out_size2[i];
                sum_buffer_len2 += out_size2[i];
            }
            if(cmd_info_->splitWrite_ == 0) {
                int now_size = sum_buffer_len2;
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
                long long now_pos_base2 = zip_now_pos2_ + pre_sizes[my_rank];
                MPI_Barrier(MPI_COMM_WORLD);
                for(int ii = 0; ii < comm_size; ii++) {
                    zip_now_pos2_ += now_sizes[ii];
                }
                for(int i = 0; i < writeInfosPE.size(); i++) {
                    writeInfosPE[i].file_offset2 = now_pos_base2;
                }
            }

        }
        
        t_slave_gz += GetTime() - tt0;
#endif

        if (cmd_info_->write_data_) {
            tt0 = GetTime();
            assert(writeInfosPE.size() == SWQC_CPE_COUNT);
            long long real_pre_chunk_pos1 = writeInfosPE[0].file_offset;
            long long real_pre_chunk_pos2 = writeInfosPE[0].file_offset2;
            if (prehashed_output_) {
                double started = GetTime();
                SwqcOutputSpan spans1[SWQC_CPE_COUNT], spans2[SWQC_CPE_COUNT];
                int64_t offset1 = real_pre_chunk_pos1, offset2 = real_pre_chunk_pos2;
                for (int i = 0; i < SWQC_CPE_COUNT; ++i) {
                    const auto &w = writeInfosPE[i];
                    spans1[i] = {w.buffer, size_t(w.buffer_len), offset1};
                    spans2[i] = {w.buffer2, size_t(w.buffer_len2), offset2};
                    offset1 += w.buffer_len;
                    offset2 += w.buffer_len2;
                }
                const char *cpe_digest = getenv("SWQC_CPE_FINGERPRINT");
                if (cpe_digest && strcmp(cpe_digest, "1") == 0) {
                    SwqcOutputFingerprint results[SWQC_CPE_COUNT][2];
                    int errors[SWQC_CPE_COUNT];
                    SwqcFingerprintTask task = {{spans1, spans2}, results, errors};
                    swqc_runtime_run((void *)slave_swqc_output_fingerprint, &task);
                    for (int i = 0; i < SWQC_CPE_COUNT; ++i) {
                        if (errors[i] || !output_fingerprints_[0].combine(results[i][0]) ||
                            !output_fingerprints_[1].combine(results[i][1]))
                            swqc_io_fail("invalid CPE output fingerprint batch");
                    }
                } else if (!swqc_fingerprint_batch(spans1, SWQC_CPE_COUNT, output_fingerprints_[0]) ||
                           !swqc_fingerprint_batch(spans2, SWQC_CPE_COUNT, output_fingerprints_[1])) {
                    swqc_io_fail("invalid output fingerprint batch");
                }
                t_output_digest += GetTime() - started;
            }
            for(int i = 0; i < SWQC_CPE_COUNT; i++) {
                while (queueNumNow >= queueSizeLim) {
#ifdef Verbose
                    //printf("waiting to push a chunk to out queue1\n");
#endif
                    usleep(1000);
                }
                out_queue_[swqc_next_ring_slot(queueP2, queueSizeLim)] = make_pair(make_pair(writeInfosPE[i].buffer, make_pair(writeInfosPE[i].buffer_len, real_pre_chunk_pos1)), make_pair(writeInfosPE[i].buffer2, make_pair(writeInfosPE[i].buffer_len2, real_pre_chunk_pos2)));
                queueNumNow++;
                real_pre_chunk_pos1 += writeInfosPE[i].buffer_len;
                real_pre_chunk_pos2 += writeInfosPE[i].buffer_len2;
            }

            t_push_q += GetTime() - tt0;
        }

        tt0 = GetTime();
        pre_fqdatachunks.clear();
        for(int i = 0; i < SWQC_CPE_COUNT; i++) {
            if(cmd_info_->write_data_) {
                pass_data1[i].swap(pre_pass_data1[i]);
                pass_data2[i].swap(pre_pass_data2[i]);
            }
            pre_fqdatachunks.push_back(fqdatachunks[i]);
        }
        t_copy_data += GetTime() - tt0;
        serial_stages_.processed(allIsNull);
        if(allIsNull) break;

    }


    double tt0 = GetTime();
    if(cmd_info_->splitWrite_ == 0 && out_is_zip_) {
        gather_and_sort_vectors_pe(my_rank, comm_size, out_round, out_gz_block_sizes1, 0);
        gather_and_sort_vectors_pe(my_rank, comm_size, out_round, out_gz_block_sizes2, 0);
    }

    swqc_log_printf("comm gz block size cost %lf\n", GetTime() - tt0);


    swqc_log_fprintf(stderr, "consumer%d NGSnew tot cost %lf\n", my_rank, GetTime() - t0);
    swqc_log_fprintf(stderr, "consumer%d NGSnew wait producer cost %lf\n", my_rank, t_wait_producer);
    swqc_log_fprintf(stderr, "consumer%d NGSnew format cost %lf\n", my_rank, t_format);
    swqc_log_fprintf(stderr, "consumer%d NGSnew resize cost %lf\n", my_rank, t_resize);
    swqc_log_fprintf(stderr, "consumer%d NGSnew ngsfunc cost %lf\n", my_rank, t_ngsfunc);
    swqc_log_fprintf(stderr, "consumer%d NGSnew      pre_qc1 cost %lf\n", my_rank, tsum1);
    swqc_log_fprintf(stderr, "consumer%d NGSnew      pre_qc1 cost %lf\n", my_rank, tsum2);
    swqc_log_fprintf(stderr, "consumer%d NGSnew      slave cost %lf\n", my_rank, tsum3);
    swqc_log_fprintf(stderr, "consumer%d NGSnew      write cost %lf (%lf [%lf %lf %lf], %lf, %lf, %lf)\n", my_rank, tsum4, tsum4_1, tsum4_1_1, tsum4_1_2, tsum4_1_3, tsum4_2, tsum4_3, tsum4_4);
    swqc_log_fprintf(stderr, "consumer%d NGSnew      isnull comm cost %lf\n", my_rank, tsum5);
    swqc_log_fprintf(stderr, "consumer%d NGSnew      push null comm cost %lf\n", my_rank, tsum6);
    swqc_log_fprintf(stderr, "consumer%d NGSnew      release cost %lf\n", my_rank, tsum7);
    swqc_log_fprintf(stderr, "[SWQC processor detail] rank=%d input_pairs=%llu prepare_s=%.6f cpe_s=%.6f output_coord_s=%.6f eof_coord_s=%.6f release_s=%.6f\n",
            my_rank, (unsigned long long)processed_pairs_, tsum1 + tsum2, tsum3, tsum4, tsum5, tsum7);
    swqc_log_fprintf(stderr, "[SWQC output digest] rank=%d consumer=%d seconds=%.6f\n",
            my_rank, int(prehashed_output_), t_output_digest);
    swqc_log_fprintf(stderr, "[SWQC output coordination] rank=%d arrival_s=%.6f after_ready_s=%.6f\n",
            my_rank, t_output_arrival, tsum4 - t_output_arrival);
    swqc_log_fprintf(stderr, "consumer%d NGSnew gz slave cost %lf\n", my_rank, t_slave_gz);
    swqc_log_fprintf(stderr, "consumer%d NGSnew gz slave2 cost %lf\n", my_rank, t_slave_gz2);
    swqc_log_fprintf(stderr, "consumer%d NGSnew push to queue cost %lf\n", my_rank, t_push_q);
    swqc_log_fprintf(stderr, "consumer%d NGSnew copy data cost %lf\n", my_rank, t_copy_data);
    double vector_release_started = GetTime();
    swqc_host_for(SWQC_CPE_COUNT, [](void *opaque, int, size_t begin, size_t end) {
        auto &v = *static_cast<VectorInit *>(opaque);
        for (size_t i = begin; i < end; ++i) {
            vector<neoReference>().swap(v.data1[i]);
            vector<neoReference>().swap(v.data2[i]);
            vector<neoReference>().swap(v.pass1[i]);
            vector<neoReference>().swap(v.pass2[i]);
            vector<neoReference>().swap(v.previous1[i]);
            vector<neoReference>().swap(v.previous2[i]);
        }
    }, &init);
    swqc_log_fprintf(stderr, "[SWQC host release] rank=%d vector_s=%.6f\n",
            my_rank, GetTime() - vector_release_started);
    done_thread_number_++;
    swqc_log_fprintf(stderr, "consumer %d in func done\n", my_rank);
}


/**
 * @brief a function to write pe data from out_data1 queue to file1
 */
void PeQc::WriteSeFastqTask12() {
#ifdef Verbose
    double t0 = GetTime();
#endif
    int cnt = 0;
    long long tot_size1 = 0;
    long long tot_size2 = 0;
    bool overWhile = 0;
    pair<CIPair, CIPair> now0;
    CIPair now1;
    CIPair now2;
    double t_wait = 0;
    double t_gz_slave = 0;
    double t_write = 0;
    double t_del = 0;
    double t_free = 0;
    unsigned long serial_items = 0;
    const char *crc_setting = getenv("SWQC_GZIP_FULL_CRC");
    const bool full_discard_crc = swqc_standard_gzip() && swqc_discard_output() &&
        crc_setting && strcmp(crc_setting, "1") == 0;
    uint32_t discarded_crc[2] = {0, 0};
    double discard_crc_seconds = 0;

    std::unique_ptr<SwqcWriteWindow> buffered1, buffered2;
    std::unique_ptr<SwqcGzipWriter> gzip1, gzip2;
    if (swqc_standard_gzip() && swqc_gzip_path(cmd_info_->out_file_name1_)) {
        gzip1.reset(new SwqcGzipWriter(cmd_info_->out_file_name1_.c_str(), swqc_gzip_chunk_bytes(),
                                      swqc_gzip_workers(), cmd_info_->compression_level_));
        gzip2.reset(new SwqcGzipWriter(cmd_info_->out_file_name2_.c_str(), swqc_gzip_chunk_bytes(),
                                      swqc_gzip_workers(), cmd_info_->compression_level_));
    }
    size_t output_window = swqc_window_bytes("SWQC_OUTPUT_WINDOW_MIB");
    if (swqc_posix_output()) {
        buffered1.reset(new SwqcWriteWindow(cmd_info_->out_file_name1_.c_str(), output_window));
        buffered2.reset(new SwqcWriteWindow(cmd_info_->out_file_name2_.c_str(), output_window));
    } else if (!out_is_zip_ && !use_out_mem &&
               (output_window || swqc_discard_output() || swqc_output_fingerprint())) {
        buffered1.reset(new SwqcWriteWindow(fh1, output_window, swqc_discard_output(), swqc_output_fingerprint() && !prehashed_output_));
        buffered2.reset(new SwqcWriteWindow(fh2, output_window, swqc_discard_output(), swqc_output_fingerprint() && !prehashed_output_));
    }

    while (true) {
        if (serial_stages_.enabled() && serial_items % SWQC_CPE_COUNT == 0 &&
            !serial_stages_.writer()) break;
        double tt0 = GetTime();
        while (queueNumNow == 0) {
            if (done_thread_number_ == cmd_info_->thread_number_) {
                // Producer may have published its final item after our empty check.
                if (queueNumNow != 0) break;
                overWhile = 1;
                swqc_log_printf("writer %d write done\n", my_rank);
                break;
            }

            //fprintf(stderr, "writer %d waiting\n", my_rank);
            usleep(1000);
        }
        if (overWhile) break;
        now0 = out_queue_[swqc_next_ring_slot(queueP1, queueSizeLim)];
        now1 = now0.first;
        now2 = now0.second;
        queueNumNow--;
        t_wait += GetTime() - tt0;



        if (full_discard_crc) {
            const double started = GetTime();
            if (now1.second.first) discarded_crc[0] = crc32(discarded_crc[0],
                reinterpret_cast<const Bytef *>(now1.first), now1.second.first);
            if (now2.second.first) discarded_crc[1] = crc32(discarded_crc[1],
                reinterpret_cast<const Bytef *>(now2.first), now2.second.first);
            discard_crc_seconds += GetTime() - started;
        }
        if (gzip1) {
            tt0 = GetTime();
            gzip1->append(now1.first, now1.second.first);
            gzip2->append(now2.first, now2.second.first);
            tot_size1 += now1.second.first; tot_size2 += now2.second.first;
            t_write += GetTime() - tt0;
            delete[] now1.first; delete[] now2.first;
        } else if (out_is_zip_) {
            if (cmd_info_->use_pigz_) {
                fprintf(stderr, "use pigz TODO...\n");
                exit(0);
            } else {

                tt0 = GetTime();
                if(use_out_mem) {
                    memcpy(OutMemData1, now1.first, now1.second.first);
                    memcpy(OutMemData2, now2.first, now2.second.first);

                } else {
#ifdef USE_LIBDEFLATE

#ifdef use_mpi_file
                    if(cmd_info_->splitWrite_ == 0) {
                        MPI_File_seek(fh1, now1.second.second, MPI_SEEK_SET);
                    }
                    MPI_File_write(fh1, now1.first, now1.second.first, MPI_CHAR, &status1);

                    if(cmd_info_->splitWrite_ == 0) {
                        MPI_File_seek(fh2, now2.second.second, MPI_SEEK_SET);
                    }
                    MPI_File_write(fh2, now2.first, now2.second.first, MPI_CHAR, &status2);
#else

                    fseek(out_stream1_, now1.second.second, SEEK_SET);
                    fwrite(now1.first, sizeof(char), now1.second.first, out_stream1_);

                    fseek(out_stream2_, now2.second.second, SEEK_SET);
                    fwrite(now2.first, sizeof(char), now2.second.first, out_stream2_);
#endif


#else
                    int written1 = gzwrite(zip_out_stream1, now1.first, now1.second.first);
                    if (written1 != now1.second.first) {
                        printf("gzwrite error\n");
                        exit(0);
                    }

                    int written2 = gzwrite(zip_out_stream2, now2.first, now2.second.first);
                    if (written2 != now2.second.first) {
                        printf("gzwrite error\n");
                        exit(0);
                    }
#endif
                }

                t_write += GetTime() - tt0;

                tt0 = GetTime();
                delete[] now1.first;
                delete[] now2.first;
                t_del += GetTime() - tt0;
            }
        } else {

            tot_size1 += now1.second.first;
            if(now1.second.first) {
                tt0 = GetTime();
                if(use_out_mem) {
                    memcpy(OutMemData1, now1.first, now1.second.first);
                } else {
#ifdef use_mpi_file
                    if (buffered1) {
                        buffered1->append(now1.first, now1.second.first, now1.second.second);
                    } else {
                        if(cmd_info_->splitWrite_ == 0) {
                            MPI_File_seek(fh1, now1.second.second, MPI_SEEK_SET);
                        }
                        MPI_File_write(fh1, now1.first, now1.second.first, MPI_CHAR, &status1);
                    }
#else
                    fseek(out_stream1_, now1.second.second, SEEK_SET);
                    //fprintf(stderr, "rank %d write ww %lld\n", my_rank, now.second.first);
                    fwrite(now1.first, sizeof(char), now1.second.first, out_stream1_);
                    //fprintf(stderr, "rank %d write ww done\n", my_rank);
#endif
                }
                t_write += GetTime() - tt0;

                tt0 = GetTime();
                delete[] now1.first;
                t_del += GetTime() - tt0;
                //fprintf(stderr, "writer %d write del done\n", my_rank);
            }


            tot_size2 += now2.second.first;
            if(now2.second.first) {
                tt0 = GetTime();
                if(use_out_mem) {
                    memcpy(OutMemData2, now2.first, now2.second.first);
                } else {

#ifdef use_mpi_file
                    if (buffered2) {
                        buffered2->append(now2.first, now2.second.first, now2.second.second);
                    } else {
                        if(cmd_info_->splitWrite_ == 0) {
                            MPI_File_seek(fh2, now2.second.second, MPI_SEEK_SET);
                        }
                        MPI_File_write(fh2, now2.first, now2.second.first, MPI_CHAR, &status2);
                    }
#else
                    fseek(out_stream2_, now2.second.second, SEEK_SET);
                    //fprintf(stderr, "rank %d write ww %lld\n", my_rank, now.second.first);
                    fwrite(now2.first, sizeof(char), now2.second.first, out_stream2_);
                    //fprintf(stderr, "rank %d write ww done\n", my_rank);
#endif
                }
                t_write += GetTime() - tt0;

                tt0 = GetTime();
                delete[] now2.first;
                t_del += GetTime() - tt0;
                //fprintf(stderr, "writer %d write del done\n", my_rank);
            }
            //fprintf(stderr, "writer %d %d done\n", my_rank, cnt++);
        }
        if (serial_stages_.enabled() && ++serial_items % SWQC_CPE_COUNT == 0) {
            double tt = GetTime();
            if (buffered1) { buffered1->flush(); buffered2->flush(); }
            t_write += GetTime() - tt;
            // No rank starts the next read/compute round while a writer runs.
            MPI_Barrier(MPI_COMM_WORLD);
            serial_stages_.written();
        }
    }
    if (gzip1) {
        double tt = GetTime();
        gzip1->finish(); gzip2->finish();
        t_write += GetTime() - tt;
        swqc_gzip_report("deflate", cmd_info_->out_file_name1_.c_str(), gzip1->metrics());
        swqc_gzip_report("deflate", cmd_info_->out_file_name2_.c_str(), gzip2->metrics());
    }
    if (full_discard_crc) {
        swqc_log_fprintf(stderr, "[SWQC discard CRC] mate=1 bytes=%lld crc32=%08x\n", tot_size1, discarded_crc[0]);
        swqc_log_fprintf(stderr, "[SWQC discard CRC] mate=2 bytes=%lld crc32=%08x\n", tot_size2, discarded_crc[1]);
        swqc_log_fprintf(stderr, "[SWQC discard CRC] full_scan_s=%.6f physical_output_bytes=0\n", discard_crc_seconds);
    }
    if (buffered1) {
        double tt = GetTime();
        buffered1->close();
        buffered2->close();
        t_write += GetTime() - tt;
        swqc_log_fprintf(stderr, "[SWQC output detail] rank=%d window_bytes=%zu calls=%llu bytes=%llu io_s=%.6f copy_s=%.6f discarded=%d physical_calls=%llu physical_bytes=%llu\n",
                my_rank, output_window,
                (unsigned long long)(buffered1->calls + buffered2->calls),
                (unsigned long long)(buffered1->bytes + buffered2->bytes),
                buffered1->io_s + buffered2->io_s, buffered1->copy_s + buffered2->copy_s,
                int(swqc_discard_output()),
                (unsigned long long)(swqc_discard_output() ? 0 : buffered1->calls + buffered2->calls),
                (unsigned long long)(swqc_discard_output() ? 0 : buffered1->bytes + buffered2->bytes));
        swqc_log_fprintf(stderr, "[SWQC output backend] rank=%d backend=%s\n",
                my_rank, swqc_discard_output() ? "discard_ordered" :
                (swqc_posix_output() ? "pwrite" : "MPI_File_write_at"));
        if (swqc_output_fingerprint()) {
            SwqcWriteWindow *windows[2] = {buffered1.get(), buffered2.get()};
            for (int m = 0; m < 2; ++m) {
                const auto &f = prehashed_output_ ? output_fingerprints_[m] : windows[m]->fingerprint;
                swqc_log_fprintf(stderr, "[SWQC output fingerprint] rank=%d mate=%d discarded=%d stride=%d bytes=%llu samples=%llu sum64=%016llx xor64=%016llx end_offset=%llu\n",
                        my_rank, m + 1, int(swqc_discard_output()), SwqcOutputFingerprint::STRIDE,
                        (unsigned long long)f.bytes, (unsigned long long)f.samples,
                        (unsigned long long)f.sum64, (unsigned long long)f.xor64,
                        (unsigned long long)f.end_offset);
            }
        }
    }
    if(cmd_info_->splitWrite_ == 0) {
        MPI_Barrier(MPI_COMM_WORLD);
    }
    //fprintf(stderr, "writer rank%d write donedone\n", my_rank);
    if (out_is_zip_) {
        if (cmd_info_->use_pigz_) {
            fprintf(stderr, "use pigz TODO...\n");
            exit(0);

        } else {
#ifdef USE_LIBDEFLATE
#ifdef use_mpi_file
            MPI_File_close(&fh1);
            MPI_File_close(&fh2);
#else
            fclose(out_stream1_);
            if(my_rank == 0) {
                truncate(cmd_info_->out_file_name1_.c_str(), sizeof(char) * zip_now_pos1_);
            }
            fclose(out_stream2_);
            if(my_rank == 0) {
                truncate(cmd_info_->out_file_name2_.c_str(), sizeof(char) * zip_now_pos2_);
            }
#endif
            //off_idx1.close();
            //off_idx2.close();

#else
            if (zip_out_stream1) {
                gzflush(zip_out_stream1, Z_FINISH);
                gzclose(zip_out_stream1);
                zip_out_stream1 = NULL;
            }
            if (zip_out_stream2) {
                gzflush(zip_out_stream2, Z_FINISH);
                gzclose(zip_out_stream2);
                zip_out_stream2 = NULL;
            }
#endif
        }

    } else {
#ifdef use_mpi_file
        if (!gzip1 && !swqc_posix_output() && !swqc_discard_output()) {
            MPI_File_close(&fh1);
            MPI_File_close(&fh2);
        }
#else
        fclose(out_stream1_);
        if(my_rank == 0) {
            truncate(cmd_info_->out_file_name1_.c_str(), sizeof(char) * now_pos1_);
        }
        fclose(out_stream2_);
        if(my_rank == 0) {
            truncate(cmd_info_->out_file_name2_.c_str(), sizeof(char) * now_pos2_);
        }
#endif
    }

    if(my_rank == 0 && out_is_zip_) {
        double tt0 = GetTime();
        ofstream ofs(cmd_info_->out_file_name1_, ios::binary | ios::app);
        for (const auto& pair : out_gz_block_sizes1) {
            ofs.write(reinterpret_cast<const char*>(&pair.second), sizeof(size_t));
        }
        size_t vector_size = out_gz_block_sizes1.size();
        ofs.write(reinterpret_cast<const char*>(&vector_size), sizeof(size_t));
        ofs.close();

        ofstream ofs2(cmd_info_->out_file_name2_, ios::binary | ios::app);
        for (const auto& pair : out_gz_block_sizes2) {
            ofs2.write(reinterpret_cast<const char*>(&pair.second), sizeof(size_t));
        }
        size_t vector_size2 = out_gz_block_sizes2.size();
        ofs2.write(reinterpret_cast<const char*>(&vector_size2), sizeof(size_t));
        ofs2.close();
        swqc_log_printf("writer final cost %lf\n", GetTime() - tt0);
    }

#ifdef Verbose
    swqc_log_printf("writer wait queue cost %.4f\n", t_wait);
    swqc_log_printf("writer gz slave cost %.4f\n", t_gz_slave);
    swqc_log_fprintf(stderr, "[SWQC rank %d] writer_io_seconds=%.6f\n", my_rank, t_write);
    swqc_log_printf("writer write cost %.4f\n", t_write);
    swqc_log_printf("writer del cost %.4f\n", t_del);
    swqc_log_printf("writer free cost %.4f\n", t_free);
    swqc_log_printf("writer %d cost %lf, tot size %lld %lld\n", my_rank, GetTime() - t0, tot_size1, tot_size2);
    swqc_log_fprintf(stderr, "writer %d in func done\n", my_rank);
    //printf("write %d cost %.5f --- %lld\n", my_rank, GetTime() - t0, tot_size);
    //
#endif
}


void PeQc::ProcessPeFastq() {
    /* Initialize the CPE resources on the MPI main thread before workers exist. */
    swqc_runtime_init();
    const char *serial = getenv("SWQC_SERIAL_STAGES");
    if (serial && strcmp(serial, "1") == 0) serial_stages_.enable();
    const char *readers_env = getenv("SWQC_INPUT_READERS");
    int readers = readers_env ? atoi(readers_env) : 1;
    const char *memory_env = getenv("SWQC_MEMORY_WORKERS");
    int memory_workers = memory_env ? atoi(memory_env) : 1;
    int worker_error = memory_workers > 1 ? 0 : swqc_host_io_init(readers);
    if (worker_error != 0) swqc_io_fail("host helper initialization failed");
    //if(my_rank == 0) block_size = 6 * (1 << 20);
    //else block_size = 4 * (1 << 20);
    const char *borrow_env = getenv("SWQC_BORROW_INPUT");
    const bool borrow_input = borrow_env && strcmp(borrow_env, "1") == 0;
    prehashed_output_ = swqc_output_fingerprint() && !out_is_zip_ && !use_out_mem && !swqc_standard_gzip();
    auto *fastqPool = new rabbit::fq::FastqDataPool(Q_lim_pe * SWQC_CPE_COUNT * 2,
        (borrow_input || (swqc_standard_gzip() && swqc_gzip_zero_copy())) ? 0 : BLOCK_SIZE);
    //rabbit::core::TDataQueue<rabbit::fq::FastqDataPairChunk> queue1(Q_lim_pe, 1);
    auto **p_thread_info = new ThreadInfo *[slave_num];
    for (int t = 0; t < slave_num; t++) {
        p_thread_info[t] = new ThreadInfo(cmd_info_, true);
    }
    rabbit::uint32 tmpSize = SWAP1_SIZE;
    if (cmd_info_->seq_len_ <= 200) tmpSize = SWAP2_SIZE;
    
    if (swqc_standard_gzip()) {
        gzip_reader_.reset(new SwqcGzipPairReader(cmd_info_->in_file_name1_, cmd_info_->in_file_name2_));
    } else if(in_is_zip_) {
        fqFileReader = new rabbit::fq::FastqFileReader(cmd_info_->in_file_name1_, *fastqPool, cmd_info_->in_file_name2_, in_is_zip_, tmpSize, start_line_, end_line_, use_in_mem);
    } else {
        fqFileReader = new rabbit::fq::FastqFileReader(cmd_info_->in_file_name1_, *fastqPool, cmd_info_->in_file_name2_, in_is_zip_, tmpSize, start_pos_, end_pos_, use_in_mem);
    }
    if(use_in_mem) {
        MPI_Barrier(MPI_COMM_WORLD);
    }
    if (getenv("SWQC_ORDERED_INPUT")) {
        const char *chunk_setting = getenv("SWQC_ORDERED_CHUNK_KIB");
        const int64_t chunk_bytes = chunk_setting ? int64_t(atoi(chunk_setting)) * 1024 : BLOCK_SIZE;
        if (chunk_bytes < (64 << 10) || chunk_bytes > BLOCK_SIZE)
            swqc_io_fail("invalid ordered input chunk size");
        swqc_log_fprintf(stderr, "[SWQC ordered chunks] rank=%d cpes=%d bytes_per_cpe=%lld\n",
                my_rank, SWQC_CPE_COUNT, (long long)chunk_bytes);
        ordered_reader_.reset(new OrderedPairReader(cmd_info_->in_file_name1_, cmd_info_->in_file_name2_,
                              my_rank, comm_size, chunk_bytes, swqc_window_bytes("SWQC_INPUT_WINDOW_MIB")));
        const char *preload = getenv("SWQC_PRELOAD_INPUT");
        uint64_t replay_pairs = strtoull(getenv("SWQC_REPLAY_PAIRS"), NULL, 10);
        if (replay_pairs) {
            try {
                ordered_reader_->replay(cmd_info_->in_file_name1_, cmd_info_->in_file_name2_,
                    replay_pairs, strtoull(getenv("SWQC_REPLAY_COPIES"), NULL, 10),
                    size_t(strtoul(getenv("SWQC_REPLAY_LIMIT_MIB"), NULL, 10)) << 20,
                    getenv("SWQC_REPLAY_LAYOUT"),
                    size_t(strtoul(getenv("SWQC_REPLAY_BLOCK_CHUNKS"), NULL, 10)),
                    strtoull(getenv("SWQC_REPLAY_SEED"), NULL, 10));
            } catch (const std::exception &e) {
                fprintf(stderr, "[SWQC replay] rank=%d error=%s\n", my_rank, e.what());
                MPI_Abort(MPI_COMM_WORLD, 1);
            }
            fflush(stderr);
            MPI_Barrier(MPI_COMM_WORLD);
        }
        if (preload && strcmp(preload, "1") == 0) {
            size_t limit = size_t(strtoul(getenv("SWQC_PRELOAD_LIMIT_MIB"), NULL, 10)) << 20;
            try {
                ordered_reader_->preload(limit, borrow_input);
            } catch (const std::bad_alloc &) {
                swqc_io_fail("cannot allocate real input preload payload");
            }
            MPI_Barrier(MPI_COMM_WORLD);
        }
    }
    /* Copy-only helpers are unnecessary during disk preload. */
    if (memory_workers > 1 && swqc_host_memory_init(memory_workers) != 0)
        swqc_io_fail("host memory helper initialization failed");
    swqc_log_fprintf(stderr, "[SWQC host memory] rank=%d workers=%d dispatch=%s\n",
            my_rank, memory_workers, memory_workers > 1 ? "poll" : "serial");
    swqc_log_begin();
    double ttt = GetTime();


    //tagg

    thread producer([&] {
        try { ProducerPeFastqTask64(cmd_info_->in_file_name1_, cmd_info_->in_file_name2_, fastqPool); }
        catch (const std::exception &e) { swqc_io_fail(e.what()); }
    });
    thread consumer(bind(&PeQc::ConsumerPeFastqTask64, this, p_thread_info, fastqPool));

    if (cmd_info_->write_data_) {
        try { WriteSeFastqTask12(); }
        catch (const std::exception &e) { swqc_io_fail(e.what()); }
        swqc_log_printf("write%d done\n", my_rank);
    }


    consumer.join();
    swqc_log_printf("consumer%d done\n", my_rank);

    producer.join();
    swqc_log_printf("producer%d done\n", my_rank);
    ordered_reader_.reset();
    gzip_reader_.reset();
    // The writer can still have a final compression batch after QC completes.
    swqc_runtime_finish();
    // Helpers outlive both producer teardown and the final CPE task.
    swqc_host_io_destroy();
    serial_stages_.report(my_rank);


    swqc_log_printf("rank%d all pro done1\n", my_rank);
    MPI_Barrier(MPI_COMM_WORLD);
    swqc_log_printf("rank%d all pro done2\n", my_rank);


#ifdef Verbose
    swqc_log_printf("all thread done\n");
    swqc_log_printf("now merge thread info\n");
#endif

    double pipeline_seconds = GetTime() - ttt;
    swqc_log_fprintf(stderr, "[SWQC rank %d] pipeline_seconds=%.6f\n", my_rank, pipeline_seconds);
    swqc_log_end();
    printf("TOT TIME1 %lf\n", pipeline_seconds);

    double tt00 = GetTime();
    vector<State *> pre_vec_state1;
    vector<State *> pre_vec_state2;
    vector<State *> aft_vec_state1;
    vector<State *> aft_vec_state2;

    for (int t = 0; t < slave_num; t++) {
        pre_vec_state1.push_back(p_thread_info[t]->pre_state1_);
        pre_vec_state2.push_back(p_thread_info[t]->pre_state2_);
        aft_vec_state1.push_back(p_thread_info[t]->aft_state1_);
        aft_vec_state2.push_back(p_thread_info[t]->aft_state2_);
    }
    State* pre_state_tmp1;
    State* pre_state_tmp2;
    State* aft_state_tmp1;
    State* aft_state_tmp2;
    
    if(cmd_info_->do_overrepresentation_) {
    //if(0) {
        pre_state_tmp1 = State::MergeStatesSlave(pre_vec_state1);
        pre_state_tmp2 = State::MergeStatesSlave(pre_vec_state2);
        aft_state_tmp1 = State::MergeStatesSlave(aft_vec_state1);
        aft_state_tmp2 = State::MergeStatesSlave(aft_vec_state2);
    } else {
        pre_state_tmp1 = State::MergeStates(pre_vec_state1);
        pre_state_tmp2 = State::MergeStates(pre_vec_state2);
        aft_state_tmp1 = State::MergeStates(aft_vec_state1);
        aft_state_tmp2 = State::MergeStates(aft_vec_state2);
    }
    printf("merge1 done\n");
    printf("merge cost %lf\n", GetTime() - tt00);

    tt00 = GetTime();
    vector<State *> pre_state_mpis1;
    pre_state_mpis1.push_back(pre_state_tmp1);
    vector<State *> aft_state_mpis1;
    aft_state_mpis1.push_back(aft_state_tmp1);
    vector<State *> pre_state_mpis2;
    pre_state_mpis2.push_back(pre_state_tmp2);
    vector<State *> aft_state_mpis2;
    aft_state_mpis2.push_back(aft_state_tmp2);
    printf("merge2 done\n");
   


    MPI_Barrier(MPI_COMM_WORLD);
    if(my_rank == 0) {
        for(int i = 1; i < comm_size; i++) {
            int now_size = 0;
            MPI_Recv(&now_size, 1, MPI_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            char* info = new char[now_size];
            MPI_Recv(info, now_size, MPI_CHAR, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            State* tmp_state = new State(info, now_size, cmd_info_, cmd_info_->seq_len_, cmd_info_->qul_range_, false);
            delete[] info;
            pre_state_mpis1.push_back(tmp_state);
        }
    } else {
        string pre_state_is = pre_state_tmp1->ParseString();
        State* tmp_state = new State(pre_state_is.c_str(), pre_state_is.length(), cmd_info_, cmd_info_->seq_len_, cmd_info_->qul_range_, false);
        //if(!res) cerr << "GGGGGGGG" << endl;
        int now_size = pre_state_is.length();
        MPI_Send(&now_size, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
        MPI_Send(pre_state_is.c_str(), now_size, MPI_CHAR, 0, 0, MPI_COMM_WORLD);
    }

    MPI_Barrier(MPI_COMM_WORLD);
    if(my_rank == 0) {
        for(int i = 1; i < comm_size; i++) {
            int now_size = 0;
            MPI_Recv(&now_size, 1, MPI_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            char* info = new char[now_size];
            MPI_Recv(info, now_size, MPI_CHAR, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            State* tmp_state = new State(info, now_size, cmd_info_, cmd_info_->seq_len_, cmd_info_->qul_range_, false);
            delete[] info;
            pre_state_mpis2.push_back(tmp_state);
        }
    } else {
        string pre_state_is = pre_state_tmp2->ParseString();
        State* tmp_state = new State(pre_state_is.c_str(), pre_state_is.length(), cmd_info_, cmd_info_->seq_len_, cmd_info_->qul_range_, false);
        //if(!res) cerr << "GGGGGGGG" << endl;
        int now_size = pre_state_is.length();
        MPI_Send(&now_size, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
        MPI_Send(pre_state_is.c_str(), now_size, MPI_CHAR, 0, 0, MPI_COMM_WORLD);
    }

    MPI_Barrier(MPI_COMM_WORLD);
    printf("pre merge done\n");

    if(my_rank == 0) {
        for(int i = 1; i < comm_size; i++) {
            int now_size = 0;
            MPI_Recv(&now_size, 1, MPI_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            char* info = new char[now_size];
            MPI_Recv(info, now_size, MPI_CHAR, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            State* tmp_state = new State(info, now_size, cmd_info_, cmd_info_->seq_len_, cmd_info_->qul_range_, false);
            delete[] info;
            aft_state_mpis1.push_back(tmp_state);
        }
    } else {
        string aft_state_is = aft_state_tmp1->ParseString();
        int now_size = aft_state_is.length();
        MPI_Send(&now_size, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
        MPI_Send(aft_state_is.c_str(), now_size, MPI_CHAR, 0, 0, MPI_COMM_WORLD);
    }
    MPI_Barrier(MPI_COMM_WORLD);

    if(my_rank == 0) {
        for(int i = 1; i < comm_size; i++) {
            int now_size = 0;
            MPI_Recv(&now_size, 1, MPI_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            char* info = new char[now_size];
            MPI_Recv(info, now_size, MPI_CHAR, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            State* tmp_state = new State(info, now_size, cmd_info_, cmd_info_->seq_len_, cmd_info_->qul_range_, false);
            delete[] info;
            aft_state_mpis2.push_back(tmp_state);
        }
    } else {
        string aft_state_is = aft_state_tmp2->ParseString();
        int now_size = aft_state_is.length();
        MPI_Send(&now_size, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
        MPI_Send(aft_state_is.c_str(), now_size, MPI_CHAR, 0, 0, MPI_COMM_WORLD);
    }
    MPI_Barrier(MPI_COMM_WORLD);

    printf("mpi comm cost %lf\n", GetTime() - tt00);
    printf("aft merge done\n");

    tt00 = GetTime();
    if(my_rank) {
        delete pre_state_tmp1;
        delete pre_state_tmp2;
        delete aft_state_tmp1;
        delete aft_state_tmp2;
        delete fastqPool;
        for (int t = 0; t < slave_num; t++) {
            delete p_thread_info[t];
        }
        delete[] p_thread_info;
        delete fqFileReader;
        if(use_out_mem) {
            delete[] OutMemData1;
            delete[] OutMemData2;
        }
        return;
    }



    auto pre_state1 = State::MergeStates(pre_state_mpis1);
    auto pre_state2 = State::MergeStates(pre_state_mpis2);
    auto aft_state1 = State::MergeStates(aft_state_mpis1);
    auto aft_state2 = State::MergeStates(aft_state_mpis2);

    printf("merge2 cost %lf\n", GetTime() - tt00);

    tt00 = GetTime();
#ifdef Verbose
    if (cmd_info_->do_overrepresentation_) {
        printf("orp cost %f\n", pre_state1->GetOrpCost() + pre_state2->GetOrpCost() + aft_state1->GetOrpCost() + aft_state2->GetOrpCost());
    }
    printf("merge done\n");
#endif
    printf("\nprint read1 (before filter) info :\n");
    State::PrintStates(pre_state1);
    printf("\nprint read1 (after filter) info :\n");
    State::PrintStates(aft_state1);
    printf("\n");

    printf("\nprint read2 (before filter) info :\n");
    State::PrintStates(pre_state2);
    printf("\nprint read2 (after filter) info :\n");
    State::PrintStates(aft_state2);
    printf("\n");
    if (cmd_info_->print_what_trimmed_) {
        State::PrintAdapterToFile(aft_state1);
        State::PrintAdapterToFile(aft_state2);
    }
    State::PrintFilterResults(aft_state1);
    printf("\n");

    if (cmd_info_->do_overrepresentation_ && cmd_info_->print_ORP_seqs_) {

        auto pre_hash_graph1 = pre_state1->GetHashGraph();
        int pre_hash_num1 = pre_state1->GetHashNum();
        auto pre_hash_graph2 = pre_state2->GetHashGraph();
        int pre_hash_num2 = pre_state2->GetHashNum();

        auto aft_hash_graph1 = aft_state1->GetHashGraph();
        int aft_hash_num1 = aft_state1->GetHashNum();
        auto aft_hash_graph2 = aft_state2->GetHashGraph();
        int aft_hash_num2 = aft_state2->GetHashNum();


        int spg = cmd_info_->overrepresentation_sampling_;
        ofstream ofs;

        string srr_name1 = cmd_info_->in_file_name1_;
        srr_name1 = PaseFileName(srr_name1);

        string srr_name2 = cmd_info_->in_file_name2_;
        srr_name2 = PaseFileName(srr_name2);

        string out_name1 = "pe_" + srr_name1 + "_before_ORP_sequences.txt";
        ofs.open(out_name1, ifstream::out);

        int cnt1 = 0;
        ofs << "sequence count"
            << "\n";
        for (int i = 0; i < pre_hash_num1; i++) {
            if (!overRepPassed(pre_hash_graph1[i].seq, pre_hash_graph1[i].cnt, spg)) continue;
            ofs << pre_hash_graph1[i].seq << " " << pre_hash_graph1[i].cnt << "\n";
            cnt1++;
        }
        ofs.close();
        printf("in %s (before filter) find %d possible overrepresented sequences (store in %s)\n",
                srr_name1.c_str(), cnt1, out_name1.c_str());

        string out_name2 = "pe_" + srr_name2 + "_before_ORP_sequences.txt";
        ofs.open(out_name2, ifstream::out);
        int cnt2 = 0;
        ofs << "sequence count"
            << "\n";
        for (int i = 0; i < pre_hash_num2; i++) {
            if (!overRepPassed(pre_hash_graph2[i].seq, pre_hash_graph2[i].cnt, spg)) continue;
            ofs << pre_hash_graph2[i].seq << " " << pre_hash_graph2[i].cnt << "\n";
            cnt2++;
        }
        ofs.close();
        printf("in %s (before filter) find %d possible overrepresented sequences (store in %s)\n",
                srr_name2.c_str(), cnt2, out_name2.c_str());


        out_name1 = "pe_" + srr_name1 + "_after_ORP_sequences.txt";
        ofs.open(out_name1, ifstream::out);
        cnt1 = 0;
        ofs << "sequence count"
            << "\n";
        for (int i = 0; i < aft_hash_num1; i++) {
            if (!overRepPassed(aft_hash_graph1[i].seq, aft_hash_graph1[i].cnt, spg)) continue;
            ofs << aft_hash_graph1[i].seq << " " << aft_hash_graph1[i].cnt << "\n";
            cnt1++;
        }
        ofs.close();
        printf("in %s (after filter) find %d possible overrepresented sequences (store in %s)\n", srr_name1.c_str(),
                cnt1, out_name1.c_str());

        out_name2 = "pe_" + srr_name2 + "_after_ORP_sequences.txt";
        ofs.open(out_name2, ifstream::out);
        cnt2 = 0;
        ofs << "sequence count"
            << "\n";
        for (int i = 0; i < aft_hash_num2; i++) {
            if (!overRepPassed(aft_hash_graph2[i].seq, aft_hash_graph2[i].cnt, spg)) continue;
            ofs << aft_hash_graph2[i].seq << " " << aft_hash_graph2[i].cnt << "\n";
            cnt2++;
        }
        ofs.close();
        printf("in %s (after filter) find %d possible overrepresented sequences (store in %s)\n", srr_name2.c_str(),
                cnt2, out_name2.c_str());

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
        printf("Duplication rate : %.5f %%\n", dupRate * 100.0);
        delete[] dupHist;
        delete[] dupMeanGC;
    }

    int *merge_insert_size;
    if (!cmd_info_->no_insert_size_) {
        merge_insert_size = new int[cmd_info_->max_insert_size_ + 1];
        memset(merge_insert_size, 0, sizeof(int) * (cmd_info_->max_insert_size_ + 1));

        for (int t = 0; t < cmd_info_->thread_number_; t++) {
            for (int i = 0; i <= cmd_info_->max_insert_size_; i++) {
                merge_insert_size[i] += p_thread_info[t]->insert_size_dist_[i];
            }
        }
        int mx_id = 0;
        for (int i = 0; i < cmd_info_->max_insert_size_; i++) {
            if (merge_insert_size[i] > merge_insert_size[mx_id]) mx_id = i;
        }
        //printf("Insert size peak (evaluated by paired-end reads): %d\n", mx_id);
        printf("Insert size peak (based on PE overlap analyze): %d\n", mx_id);
    }
    printf("print and other cost %lf\n", GetTime() - tt00);

    tt00 = GetTime();
    string srr_name1 = cmd_info_->in_file_name1_;
    srr_name1 = PaseFileName(srr_name1);
    string srr_name2 = cmd_info_->in_file_name2_;
    srr_name2 = PaseFileName(srr_name2);
    Repoter::ReportHtmlPe(srr_name1 + "_" + srr_name2 + "_SWQC.html", pre_state1, pre_state2, aft_state1,
            aft_state2, cmd_info_->in_file_name1_,
            cmd_info_->in_file_name2_, dupRate * 100.0, merge_insert_size);
#ifdef Verbose
    printf("report done\n");
#endif
    printf("report cost %lf\n", GetTime() - tt00);

    if(use_in_mem) {
        fprintf(stderr, "TOT TIME %lf\n", GetTime() - ttt);
    }
    tt00 = GetTime();
    delete pre_state1;
    delete pre_state2;
    delete aft_state1;
    delete aft_state2;
    if (!cmd_info_->no_insert_size_)
        delete[] merge_insert_size;

    delete fastqPool;
    for (int t = 0; t < slave_num; t++) {
        delete p_thread_info[t];
    }

    delete[] p_thread_info;
    
    delete fqFileReader;
    if(use_out_mem) {
        delete[] OutMemData1;
        delete[] OutMemData2;
    }
    printf("delete cost %lf\n", GetTime() - tt00);

}
