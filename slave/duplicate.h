//
// Created by ylf9811 on 2021/7/13.
//

#ifndef SLAVE_RERABBITQC_DUPLICATE_H
#define SLAVE_RERABBITQC_DUPLICATE_H



//#include "Globals.h"
#include "Reference.h"
#include "cmdinfo.h"


class Duplicate {
public:
    Duplicate(CmdInfo *cmd_info);

    ~Duplicate();

    void statRead(neoReference &ref);

    void statPair(neoReference &r1, neoReference &r2);

    uint64_t seq2int(const char *data, int start, int key_len, bool &valid);

    void addRecord(uint32_t key, uint64_t kmer32, uint8_t gc);

    // make histogram and get duplication rate
    double statAll(int *hist, double *meanGC, int hist_size);

public:
    CmdInfo *cmd_info_;
    int key_len_base_;
    int key_len_bit;
    uint64_t *dups_;
    uint16_t *counts_;
    uint8_t *gcs_;
    //std::mutex lok_;
};


#endif//SLAVE_RERABBITQC_DUPLICATE_H
