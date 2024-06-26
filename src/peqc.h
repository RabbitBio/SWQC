//
// Created by ylf9811 on 2021/7/6.
//

#ifndef RERABBITQC_PEQC_H
#define RERABBITQC_PEQC_H

#include <atomic>
#include <cstring>
#include <fstream>
#include <functional>

#include "Formater.h"
#include "Globals.h"
#include "adapter.h"
#include "cmdinfo.h"
#include "duplicate.h"
#include "filter.h"
#include "pigz.h"
#include "polyx.h"
#include "pugz.h"
#include "state.h"
#include "threadinfo.h"
#include "umier.h"
#include "qcdata.h"

#include "globalMutex.h"


#define CIPair std::pair<char *, std::pair<int, long long>>
#define CRPair std::pair<int64_t, std::vector<rabbit::fq::FastqDataPairChunk *>>


class PeQc {
public:
    PeQc(CmdInfo *cmd_info, int my_rank = 0, int comm_size = 1);

    PeQc();

    ~PeQc();

    void ProcessPeFastq();


private:

    std::string Read2String(neoReference &ref);

    void Read2Chars(neoReference &ref, char *out_data, int &pos);

    void ProducerPeFastqTask64(std::string file, std::string file2, rabbit::fq::FastqDataPool *fastqPool);


//    void ProducerPeFastqTask(std::string file, std::string file2, rabbit::fq::FastqDataPool *fastqPool,
//                             rabbit::core::TDataQueue<rabbit::fq::FastqDataPairChunk> &dq);
//
//    void ProducerPeInterFastqTask(std::string file, rabbit::fq::FastqDataPool *fastq_data_pool,
//                                  rabbit::core::TDataQueue<rabbit::fq::FastqDataChunk> &dq);

    void ConsumerPeFastqTask64(ThreadInfo **thread_info, rabbit::fq::FastqDataPool *fastqPool);


//    void ConsumerPeFastqTask(ThreadInfo **thread_info, rabbit::fq::FastqDataPool *fastqPool,
//                             rabbit::core::TDataQueue<rabbit::fq::FastqDataPairChunk> &dq);
//
//    void ConsumerPeInterFastqTask(ThreadInfo *thread_info, rabbit::fq::FastqDataPool *fastqPool,
//                                  rabbit::core::TDataQueue<rabbit::fq::FastqDataChunk> &dq);

    void WriteSeFastqTask12();
//
//    void WriteSeFastqTask1();
//
//    void WriteSeFastqTask2();

//    void ProcessNgsData(bool &allIsNull, std::vector <neoReference> data1[64], std::vector <neoReference> data2[64], std::vector <rabbit::fq::FastqDataPairChunk *> fqdatachunks, qc_data *para, rabbit::fq::FastqDataPool *fastqPool);

    void ProcessFormatQCWrite(bool &allIsNull, std::vector <neoReference> *data1, std::vector <neoReference> *data2,
                              std::vector <neoReference> *pass_data1, std::vector <neoReference> *pass_data2,
                              std::vector <neoReference> *pre_pass_data1, std::vector <neoReference> *pre_pass_data2,
                              std::vector <rabbit::fq::FastqDataPairChunk *> fqdatachunks, std::vector <rabbit::fq::FastqDataPairChunk *> pre_fqdatachunks,
                              qc_data *para, rabbit::fq::FastqDataPool *fastq_data_pool);




private:

    int my_rank, comm_size;
    CmdInfo *cmd_info_;
    Filter *filter_;

    CIPair *out_queue1_;
    CIPair *out_queue2_;
    //std::pair<QChunkItem, QChunkItem> *out_queue_;
    std::pair<CIPair, CIPair> *out_queue_;
    std::vector<rabbit::fq::FastqDataPairChunk *> *p_out_queue_;
    std::atomic_int done_thread_number_;
    FILE *out_stream1_;
    FILE *out_stream2_;
    MPI_File fh1;
    MPI_File fh2;
    MPI_Status status1;
    MPI_Status status2;
    Duplicate *duplicate_;
    Umier *umier_;
    long long now_pos1_;
    long long now_pos2_;
    long long zip_now_pos1_;
    long long zip_now_pos2_;


    gzFile zip_out_stream1;
    gzFile zip_out_stream2;
    std::ofstream off_idx1;
    std::ofstream off_idx2;
    bool in_is_zip_;
    bool out_is_zip_;
    int start_line_;
    int end_line_;
    int64_t start_pos_;
    int64_t end_pos_;

    moodycamel::ReaderWriterQueue<std::pair<char *, int>> *
            pugzQueue1;
    moodycamel::ReaderWriterQueue<std::pair<char *, int>> *
            pugzQueue2;
    std::atomic_int pugzDone1;
    std::atomic_int pugzDone2;
    moodycamel::ReaderWriterQueue<std::pair<char *, int>> *
            pigzQueue1;
    std::pair<char *, int> pigzLast1;
    moodycamel::ReaderWriterQueue<std::pair<char *, int>> *
            pigzQueue2;
    std::pair<char *, int> pigzLast2;

    std::atomic_int p_queueP1;
    std::atomic_int p_queueP2;
    std::atomic_int p_queueNumNow;
    std::atomic_int p_queueSizeLim;

    std::atomic_int queueP1;
    std::atomic_int queueP2;
    std::atomic_int queueNumNow;
    std::atomic_int queueSizeLim;




    std::atomic_int producerDone;
    std::atomic_int writerDone1;
    std::atomic_int queue1P1;
    std::atomic_int queue1P2;
    std::atomic_int queueNumNow1;
    std::atomic_int queueSizeLim1;
    std::atomic_int pigzQueueNumNow1;
    std::atomic_int pigzQueueSizeLim1;

    std::atomic_int writerDone2;
    std::atomic_int queue2P1;
    std::atomic_int queue2P2;
    std::atomic_int queueNumNow2;
    std::atomic_int queueSizeLim2;
    std::atomic_int pigzQueueNumNow2;
    std::atomic_int pigzQueueSizeLim2;
    std::atomic_int now_chunks;
    int64_t mx_chunks;
    std::atomic_int consumerCommDone;
    std::atomic_int producerStop;
    std::mutex mylock;
    std::mutex p_mylock;

    int64_t *part_sizes;
    std::vector<std::pair<int, size_t>> out_gz_block_sizes1;
    std::vector<std::pair<int, size_t>> out_gz_block_sizes2;

#ifdef USE_CC_GZ
    char* cc_gz_in_buffer[64];
#endif
};


#endif//RERABBITQC_PEQC_H
