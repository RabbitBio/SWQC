#include <sys/time.h>
#include <string>
#include <iostream>
#include "CLI11.hpp"
#include "seqc.h"
#include "peqc.h"
#include "cmdinfo.h"

double GetTime() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (double) tv.tv_sec + (double) tv.tv_usec / 1000000;
}


int main(int argc, char **argv) {

    CmdInfo cmd_info;
    CLI::App app("RabbitQCPlus");
    auto opt = app.add_option("-i,--inFile1", cmd_info.in_file_name1_, "input fastq name 1, can not be ''");
    opt->required();
    app.add_option("-I,--inFile2", cmd_info.in_file_name2_, "input fastq name 2, can be '' when single data");
    app.add_option("-o,--outFile1", cmd_info.out_file_name1_, "output fastq name 1");
    app.add_option("-O,--outFile2", cmd_info.out_file_name2_, "output fastq name 2");

    app.add_flag("-a,--noTrimAdapter", cmd_info.no_trim_adapter_, "no trim adapter");
    app.add_flag("--decAdaForSe", cmd_info.se_auto_detect_adapter_, "detect adapter for se data");
    app.add_flag("--decAdaForPe", cmd_info.pe_auto_detect_adapter_, "detect adapter for pe data");
    app.add_option("--adapter_seq1", cmd_info.adapter_seq1_, "input adapter sequence1");
    app.add_option("--adapter_seq2", cmd_info.adapter_seq2_, "input adapter sequence2");

    app.add_flag("-c,--correctData", cmd_info.correct_data_, "correct data");

    app.add_option("-w,--threadNum", cmd_info.thread_number_, "number thread used to solve fastq data");

    //filter
    app.add_flag("-5,--trim5End", cmd_info.trim_5end_, "do sliding window 5end trim");
    app.add_flag("-3,--trim3End", cmd_info.trim_3end_, "do sliding window 3end trim");
    app.add_option("--trimFront1", cmd_info.trim_front1_, "ref1 trim front size");
    app.add_option("--trimFront2", cmd_info.trim_front2_, "ref2 trim front size");
    app.add_option("--trimTail1", cmd_info.trim_tail1_, "ref1 trim tail size");
    app.add_option("--trimTail2", cmd_info.trim_tail2_, "ref2 trim tail size");


    app.add_flag("-g,--trimPolyg", cmd_info.trim_polyg_, "do polyg trim");
    app.add_flag("-x,--trimPolyx", cmd_info.trim_polyx_, "do polyx trim");


    CLI11_PARSE(app, argc, argv);
    printf("in1 is %s\n", cmd_info.in_file_name1_.c_str());
    if (cmd_info.in_file_name2_.length())printf("in2 is %s\n", cmd_info.in_file_name2_.c_str());
    if (cmd_info.out_file_name1_.length())printf("out1 is %s\n", cmd_info.out_file_name1_.c_str());
    if (cmd_info.out_file_name2_.length())printf("out2 is %s\n", cmd_info.out_file_name2_.c_str());

    if (cmd_info.no_trim_adapter_)cmd_info.trim_adapter_ = false;
    ASSERT(cmd_info.no_trim_adapter_ != cmd_info.trim_adapter_);
    if (!cmd_info.trim_adapter_) {
        cmd_info.se_auto_detect_adapter_ = false;
        cmd_info.pe_auto_detect_adapter_ = false;
        cmd_info.adapter_seq1_ = "";
        cmd_info.adapter_seq2_ = "";
        printf("no adapter trim!\n");
    }

    if (cmd_info.trim_5end_) {
        printf("now do 5end trim\n");
    }
    if (cmd_info.trim_3end_) {
        printf("now do 3end trim\n");
    }
    if (cmd_info.trim_polyg_) {
        printf("now do polyg trim\n");
    }
    if (cmd_info.trim_polyx_) {
        printf("now do polyx trim\n");
    }

    printf("now use %d thread\n", cmd_info.thread_number_);

    double t1 = GetTime();
    if (cmd_info.in_file_name2_.length()) {
        if (cmd_info.out_file_name1_.length() > 0 && cmd_info.out_file_name2_.length() > 0) {
            cmd_info.write_data_ = true;
            printf("auto set write_data_ 1\n");
        }
        //calculate file size and estimate reads number
        FILE *p_file;
        p_file = fopen(cmd_info.in_file_name1_.c_str(), "r");
        fseek(p_file, 0, SEEK_END);
        int64_t total_size = ftell(p_file);
        cmd_info.in_file_size1_ = total_size;
        printf("in file total size is %lld\n", total_size);
        printf("my evaluate readNum is %lld\n", int64_t(total_size / 200.0));

        //adapter
        if (cmd_info.adapter_seq1_.length()) {
            printf("input adapter1 is %s\n", cmd_info.adapter_seq1_.c_str());
            if (cmd_info.adapter_seq2_.length() == 0) {
                cmd_info.adapter_seq2_ = cmd_info.adapter_seq1_;
            }
            printf("input adapter2 is %s\n", cmd_info.adapter_seq2_.c_str());
            cmd_info.pe_auto_detect_adapter_ = false;
            cmd_info.detect_adapter1_ = true;
            cmd_info.detect_adapter2_ = true;
        }
        if (cmd_info.pe_auto_detect_adapter_) {
            printf("now auto detect adapter\n");
        }
        if (cmd_info.correct_data_) {
            printf("now correct data\n");
        }
        if (cmd_info.trim_adapter_ || cmd_info.correct_data_) {
            cmd_info.analyze_overlap_ = true;
            printf("now do overlap analyze\n");
        }

        if (cmd_info.trim_front1_) {
            printf("ref1 trim front %d bases\n", cmd_info.trim_front1_);
            cmd_info.trim_front2_ = cmd_info.trim_front1_;
            printf("ref2 trim front %d bases\n", cmd_info.trim_front2_);

        }
        if (cmd_info.trim_tail1_) {
            printf("ref1 trim tail %d bases\n", cmd_info.trim_tail1_);
            cmd_info.trim_tail2_ = cmd_info.trim_tail1_;
            printf("ref2 trim tail %d bases\n", cmd_info.trim_tail2_);
        }

        PeQc pe_qc(&cmd_info);
        pe_qc.ProcessPeFastq();
    } else {
        if (cmd_info.out_file_name1_.length() > 0) {
            cmd_info.write_data_ = true;
            printf("auto set write_data_ 1\n");
        }
        //calculate file size and estimate reads number
        FILE *p_file;
        p_file = fopen(cmd_info.in_file_name1_.c_str(), "r");
        fseek(p_file, 0, SEEK_END);
        int64_t total_size = ftell(p_file);
        cmd_info.in_file_size1_ = total_size;
        printf("in file total size is %lld\n", total_size);
        printf("my evaluate readNum is %lld\n", int64_t(total_size / 200.0));

        //adapter
        if (cmd_info.adapter_seq1_.length()) {
            printf("input adapter is %s\n", cmd_info.adapter_seq1_.c_str());
            cmd_info.se_auto_detect_adapter_ = false;
            cmd_info.detect_adapter1_ = true;
        }
        if (cmd_info.se_auto_detect_adapter_) {
            printf("now auto detect adapter\n");
        }
        if (cmd_info.trim_front1_) {
            printf("trim front %d bases\n", cmd_info.trim_front1_);
        }
        if (cmd_info.trim_tail1_) {
            printf("trim tail %d bases\n", cmd_info.trim_tail1_);
        }


        SeQc se_qc(&cmd_info);
        se_qc.ProcessSeFastq();
    }
    printf("cost %.5f\n", GetTime() - t1);

    return 0;


}
/*
@SRR2496709.17 17 length=100
CCTTCCCCTCAAGCTCAGGGCCAAGCTGTCCGCCAACCTCGGCTCCTCCGGGCAGCCCTCGCCCGGGGTGCGCCCCGGGGCAGGACCCCCAGCCCACGCC
+SRR2496709.17 17 length=100
18+8?=>?>?==>?>?:>?1>,<<?)>><@@8>@>=>@?@8/?>@=?>=;;@A?BA>+>?8>,=;=?=01>;>>=(8->-8=9-2=>==;19>>A?.7>=
@SRR2496709.17 17 length=100
GGAGGGCAGGGGCGGGCCCTGGGCGTGGGCTGGGGGTCCTGCCCCGGGGCGCACCCCGGGCGAGGGCAGCCCGGAGGAGCCGAGGCTTGCGGACAGCTGG
+SRR2496709.17 17 length=100
819190-<>/;/:<//:===219-*<09.>0?>....891:8=@@;?><-6:1-=>############################################
===================================================
ov 21 79 4 2
reference before correct :
@SRR2496709.17 17 length=100
CCTTCCCCTCAAGCTCAGGGCCAAGCTGTCCGCCAACCTCGGCTCCTCCGGGCAGCCCTCGCCCGGGGTGCGCCCCGGGGCAGGACCCCCAGCCCACGCC
+SRR2496709.17 17 length=100
18+8?=>?>?==>?>?:>?1>,<<?)>><@@8>@>=>@?@8/?>@=?>=;;@A?BA>+>?8>,=;=?=01>;>>=(8->-8=9-2=>==;19>>A?.7>=
@SRR2496709.17 17 length=100
GGAGGGCAGGGGCGGGCCCTGGGCGTGGGCTGGGGGTCCTGCCCCGGGGCGCACCCCGGGCGAGGGCTGCCCGGAGGAGCCGAGGCTGGCGGACAGCTGG
+SRR2496709.17 17 length=100
819190-<>/;/:<//:===219-*<09.>0?>....891:8=@@;?><-6:1-=>###########?###################@############
===================================================




@SRR2496709.17 17 length=100
CCTTCCCCTCAAGCTCAGGGCCAAGCTGTCCGCCAACCTCGGCTCCTCCGGGCAGCCCTCGCCCGGGGTGCGCCCCGGGGCAGGACCCCCAGCCCACGCC
+SRR2496709.17 17 length=100
18+8?=>?>?==>?>?:>?1>,<<?)>><@@8>@>=>@?@8/?>@=?>=;;@A?BA>+>?8>,=;=?=01>;>>=(8->-8=9-2=>==;19>>A?.7>=
@SRR2496709.17 17 length=100
GGAGGGCAGGGGCGGGCCCTGGGCGTGGGCTGGGGGTCCTGCCCCGGGGCGCACCCCGGGCGAGGGCAGCCCGGAGGAGCCGAGGCTTGCGGACAGCTGG
+SRR2496709.17 17 length=100
819190-<>/;/:<//:===219-*<09.>0?>....891:8=@@;?><-6:1-=>############################################
===================================================
ov 21 79 4 2
reference after correct :
@SRR2496709.17 17 length=100
CCTTCCCCTCAAGCTCAGGGCCAAGCTGTCCGCCAACCTCGGCTCCTCCGGGCAGCCCTCGCCCGGGGTGCGCCCCGGGGCAGGACCCCCAGCCCACGCC
+SRR2496709.17 17 length=100
18+8?=>?>?==>?>?:>?1>,<<?)>><@@8>@>=>@?@8/?>@=?>=;;@A?BA>+>?8>,=;=?=01>;>>=(8->-8=9-2=>==;19>>A?.7>=
@SRR2496709.17 17 length=100
GGAGGGCAGGGGCGGGCCCTGGGCGTGGGCTGGGGGTCCTGCCCCGGGGCGCACCCCGGGCGAGGGCTGCCCGGAGGAGCCGAGGCTGGCGGACAGCTGG
+SRR2496709.17 17 length=100
819190-<>/;/:<//:===219-*<09.>0?>@...891:8=@@;?><-6:1?=>############################################
===================================================

 */