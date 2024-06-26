//
// Created by ylf9811 on 2021/7/6.
//

#ifndef RERABBITQC_CMDINFO_H
#define RERABBITQC_CMDINFO_H

class CmdInfo {
public:
    CmdInfo();
    void CmdInfoCopy(CmdInfo *cmd_info);

public:
    std::string in_file_name1_;
    std::string in_file_name2_;
    std::string out_file_name1_;
    std::string out_file_name2_;
    int thread_number_;
    std::string command_;
    bool isPhred64_;
    bool isStdin_;
    bool isStdout_;

    bool overWrite_;
    bool write_data_;
    int64_t out_block_size_;
    int64_t in_file_size1_;
    int64_t in_file_size2_;
    int seq_len_;
    int qul_range_;

    //param for filter
    int length_required_;
    int length_limit_;
    int n_number_limit_;
    int low_qual_perc_limit_;
    bool trim_5end_;
    bool trim_3end_;
    int cut_window_size_;
    int cut_mean_quality_;
    int trim_front1_;
    int trim_tail1_;
    int trim_front2_;
    int trim_tail2_;


    //param for adapter
    bool trim_adapter_;
    bool no_trim_adapter_;
    bool se_auto_detect_adapter_;
    bool pe_auto_detect_adapter_;
    bool detect_adapter1_;
    bool detect_adapter2_;
    std::string adapter_seq1_;
    std::string adapter_seq2_;
    int overlap_diff_limit_;
    int overlap_require_;
    bool correct_data_;
    bool analyze_overlap_;
    int adapter_len_lim_;
    bool print_what_trimmed_;
    std::vector<std::string> adapter_from_fasta_;
    std::string adapter_fasta_file_;

    //duplicate
    bool state_duplicate_;

    //polyx
    bool trim_polyx_;
    bool trim_polyg_;
    int trim_poly_len_;

    //umi
    bool add_umi_;
    int umi_loc_;
    int umi_len_;
    std::string umi_prefix_;
    int umi_skip_;

    //TGS
    bool is_TGS_;
    int TGS_min_len_;

    //overrepresentation_analysis
    bool do_overrepresentation_;
    int overrepresentation_sampling_;
    std::vector<std::string> hot_seqs_;
    std::vector<std::string> hot_seqs2_;
    int eva_len_;
    int eva_len2_;
    bool print_ORP_seqs_;

    //insert size
    bool no_insert_size_;
    int max_insert_size_;

    //gz
    int compression_level_;

    //interleaved
    bool interleaved_in_;
    bool interleaved_out_;


    //parallel gz
    bool use_pugz_;
    bool use_pigz_;
    int pugz_threads_;
    int pigz_threads_;
};


#endif//RERABBITQC_CMDINFO_H
