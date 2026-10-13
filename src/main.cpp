#include "CLI11.hpp"
#include "Globals.h"
#include "cmdinfo.h"
#include "peqc.h"
#include "swqc_log.h"
#include "gzip_pipeline.h"
#include "seqc.h"
#include "th_ass.h"
#include <iostream>
#include <fstream>
#include <stdexcept>
#include <sys/stat.h>
#include "mpi.h"

#include "globalMutex.h"

std::mutex globalMutex;

using namespace std;

inline bool exists_file(const std::string &name) {
    ifstream f(name.c_str());
    return f.good();
}

int main(int argc, char **argv) try {

    int my_rank = 0;
    int comm_size = 1;
    int provided = MPI_THREAD_SINGLE;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
    if (provided < MPI_THREAD_MULTIPLE) {
        fprintf(stderr, "SWQC requires MPI_THREAD_MULTIPLE for its consumer/writer threads\n");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    MPI_Comm_size(MPI_COMM_WORLD, &comm_size);
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
#ifdef use_swlu
    //swlu_debug_init();
    //swlu_prof_init();
    //swlu_prof_start();
#endif	
    

    //printf(" %d / %d\n", my_rank, comm_size);
    if(my_rank != 0) freopen("/dev/null", "w", stdout);
    //if(my_rank == 0) freopen("dev/null", "w", stdout);
    srand(time(0));
    CmdInfo cmd_info;
    CLI::App app("SWQC");
    app.add_option("-i,--inFile1", cmd_info.in_file_name1_, "input fastq name 1");
    app.add_option("-I,--inFile2", cmd_info.in_file_name2_, "input fastq name 2");
    app.add_option("-o,--outFile1", cmd_info.out_file_name1_, "output fastq name 1");
    app.add_option("-O,--outFile2", cmd_info.out_file_name2_, "output fastq name 2");


    app.add_option("--compressLevel", cmd_info.compression_level_,
            "output file compression level (1 - 9), default is 4");

    app.add_flag("--overWrite", cmd_info.overWrite_, "overwrite out file if already exists.");
    app.add_flag("--splitWrite", cmd_info.splitWrite_, "write split file when using multi-nodes.");
    bool ordered_output = false;
    bool standard_gzip = false;
    bool legacy_gzip = false;
    bool gzip_full_crc = false;
    int gzip_workers = SWQC_CPE_COUNT, gzip_chunk_mib = 1;
    int gzip_read_chunk_kib = 256;
    std::string gzip_reader = "auto";
    app.add_flag("--standardGzip", standard_gzip, "force the streaming PE pipeline for plain FASTQ (automatic for .gz paths)");
    app.add_flag("--legacyGzip", legacy_gzip, "use the historical SWQC private-footer gzip path, not ordinary gzip");
    app.add_flag("--gzipFullCRC", gzip_full_crc, "scan complete discarded FASTQ for validation (streaming gzip pipeline + discardOutput)");
    app.add_option("--gzipWorkers", gzip_workers, "CPEs per standard gzip batch (1..configured CPE count)");
    app.add_option("--gzipChunkMiB", gzip_chunk_mib, "standard gzip raw block/interval target (1..64 MiB)");
    app.add_option("--gzipReader", gzip_reader, "gzip input: rapid (parallel first pass), zlib (serial), auto (build default)");
    app.add_option("--gzipReadChunkKiB", gzip_read_chunk_kib, "parallel reader compressed-byte target (64..4096 KiB)");
    bool posix_output = false;
    bool serial_stages = false;
    bool discard_output = false, output_fingerprint = false, preload_input = false;
    bool borrow_input = false, cpe_fingerprint = false, defer_logs = false;
    int preload_limit_mib = 16384;
    int input_readers = 1;
    int memory_workers = 1;
    int ordered_chunk_kib = 1024;
    uint64_t replay_pairs = 0, replay_copies = 20;
    int replay_limit_mib = 1024;
    std::string replay_layout = "source";
    int replay_block_chunks = 32;
    uint64_t replay_seed = 1;
    bool profile_rounds = false;
    app.add_option("--replayPairs", replay_pairs, "cache this many paired records from the input prefix for diagnostic replay (0 disables)");
    app.add_option("--replayCopies", replay_copies, "number of full sample replays, default 20");
    app.add_option("--replayLimitMiB", replay_limit_mib, "replay sample payload/index limit per rank, default 1024 MiB");
    app.add_option("--replayLayout", replay_layout, "source or qc_blocks: fixed-length real pairs grouped by overlap/quality proxies (synthetic diagnostic)");
    app.add_option("--replayBlockChunks", replay_block_chunks, "consecutive feature-correlated CPE chunks per shuffled block, default 32");
    app.add_option("--replaySeed", replay_seed, "rank-independent replay block shuffle seed, default 1");
    app.add_flag("--profileRounds", profile_rounds, "record input features and CPE/coordination time for each processor round");
    app.add_option("--orderedChunkKiB", ordered_chunk_kib, "nominal FASTQ bytes per CPE and mate (64..1024 KiB, ordered input only)");
    app.add_option("--inputReaders", input_readers, "input window readers per rank (1, or 6 for BigShare)");
    app.add_option("--memoryWorkers", memory_workers, "BigShare resident-buffer copy workers (1 or 6, default 1)");
    int input_window_mib = 0, output_window_mib = 0;
    app.add_option("--inputWindowMiB", input_window_mib, "ordered input read-ahead window (0 disables, otherwise 4..256 MiB)");
    app.add_option("--outputWindowMiB", output_window_mib, "ordered output contiguous write buffer (0..256 MiB)");
    app.add_flag("--orderedOutput", ordered_output, "preserve global input order in uncompressed paired output");
    app.add_flag("--posixOutput", posix_output, "use positional POSIX writes for ordered plain paired output");
    app.add_flag("--serialStages", serial_stages, "run reader, processor and writer in separate synchronized phases");
    app.add_flag("--discardOutput", discard_output, "retain ordered output generation and coordination, skip physical writes");
    app.add_flag("--outputFingerprint", output_fingerprint, "sample output bytes at global 4096-byte intervals (not MD5)");
    app.add_flag("--cpeFingerprint", cpe_fingerprint, "calculate the same ordered output fingerprint on CPEs");
    app.add_flag("--deferLogs", defer_logs, "print normal pipeline diagnostics after timing (errors remain live)");
    app.add_flag("--preloadInput", preload_input, "load this rank's complete real input before pipeline timing");
    app.add_flag("--borrowInput", borrow_input, "reference stable preloaded FASTQ bytes without input copies");
    app.add_option("--preloadLimitMiB", preload_limit_mib, "maximum paired preload payload per rank, default 16384 MiB");
    app.add_flag("--phred64", cmd_info.isPhred64_, "input is using phred64 scoring, default is phred33");
    app.add_flag("--stdin", cmd_info.isStdin_,
            "input from stdin, or -i /dev/stdin, only for se data or interleaved pe data(which means use --interleavedIn)");
    app.add_flag("--stdout", cmd_info.isStdout_,
            "output to stdout, or -o /dev/stdout, only for se data or interleaved pe data(which means use --interleavedOut)");


    int tmp_no_dup_ = 0;
    app.add_flag("--noDuplicate", tmp_no_dup_, "don't do duplicate state, default is off");
    app.add_flag("-a,--noTrimAdapter", cmd_info.no_trim_adapter_, "don't trim adapter, default is off");
    app.add_flag("--decAdaForSe", cmd_info.se_auto_detect_adapter_, "detect adapter for se data, default is on");
    app.add_flag("--decAdaForPe", cmd_info.pe_auto_detect_adapter_,
            "detect adapter for pe data, default is off, tool prefers to use overlap to find adapter");
    app.add_flag("--printWhatTrimmed", cmd_info.print_what_trimmed_,
            "if print what trimmed to *_trimmed_adapters.txt or not, default is not");
    app.add_option("--adapterSeq1", cmd_info.adapter_seq1_, "specify adapter sequence for read1");
    app.add_option("--adapterSeq2", cmd_info.adapter_seq2_, "specify adapter sequence for read2");
    app.add_option("--adapterFastaFile", cmd_info.adapter_fasta_file_, "specify adapter sequences use fasta file");
    //app.add_option("--adapterLengthLimit", cmd_info.adapter_len_lim_, "minimum adapter length when trimming, default is 0");

    app.add_flag("-c,--correctData", cmd_info.correct_data_,
            "correcting low quality bases using information from overlap, default is off");

    app.add_option("-w,--threadNum", cmd_info.thread_number_,
            "number of thread used to do QC, including (de)compression for compressed data, default is 1");

    //filter
    app.add_flag("-5,--trim5End", cmd_info.trim_5end_,
            "do sliding window from 5end to 3end to trim low quality bases, default is off");
    app.add_flag("-3,--trim3End", cmd_info.trim_3end_,
            "do sliding window from 5end to 3end to trim low quality bases, default is off");
    app.add_option("--trimFront1", cmd_info.trim_front1_, "number of bases to trim from front in read1, default is 0");
    app.add_option("--trimFront2", cmd_info.trim_front2_, "number of bases to trim from front in read2, default is 0");
    app.add_option("--trimTail1", cmd_info.trim_tail1_, "number of bases to trim from tail in read1, default is 0");
    app.add_option("--trimTail2", cmd_info.trim_tail2_, "number of bases to trim from tail in read2, default is 0");


    app.add_flag("-g,--trimPolyg", cmd_info.trim_polyg_, "do polyg tail trim, default is off");
    app.add_flag("-x,--trimPolyx", cmd_info.trim_polyx_, "do polyx tail trim, default is off");


    app.add_flag("-u,--addUmi", cmd_info.add_umi_, "do unique molecular identifier (umi) processing, default is off");
    app.add_option("--umiLen", cmd_info.umi_len_, "umi length if it is in read1/read2, default is 0");
    string umiLoc = "";
    app.add_option("--umiLoc", umiLoc,
            "specify the location of umi, can be (index1/index2/read1/read2/per_index/per_read), default is 0");
    app.add_option("--umiPrefix", cmd_info.umi_prefix_,
            "identification to be added in front of umi, default is no prefix");
    app.add_option("--umiSkip", cmd_info.umi_skip_, "the number bases to skip if umi exists, default is 0");

    //app.add_option("--seqLen", cmd_info.seq_len_, "max sequence length, default is 200");


    //TGS
    app.add_flag("--TGS", cmd_info.is_TGS_,
            "process third generation sequencing (TGS) data (only for se data, does not support trimming and will not produce output files), default is off");


    //overrepresentation
    app.add_flag("-p,--doOverrepresentation", cmd_info.do_overrepresentation_,
            "do over-representation sequence analysis, default is off");
    app.add_option("-P,--overrepresentationSampling", cmd_info.overrepresentation_sampling_,
            "do overrepresentation every [] reads, default is 20");

    app.add_flag("--printORPSeqs", cmd_info.print_ORP_seqs_,
            "if print overrepresentation sequences to *ORP_sequences.txt or not, default is not");
    //insert size analyze
    app.add_flag("--noInsertSize", cmd_info.no_insert_size_,
            "no insert size analysis (only for pe data), default is to do insert size analysis");


    //interleaved
    app.add_flag("--interleavedIn", cmd_info.interleaved_in_,
            "use interleaved input (only for pe data), default is off");
    app.add_flag("--interleavedOut", cmd_info.interleaved_out_,
            "use interleaved output (only for pe data), default is off");


    //parallel gz
    //app.add_flag("--usePugz", cmd_info.use_pugz_, "use pugz to decompress data, default is off");
    //app.add_flag("--usePigz", cmd_info.use_pigz_, "use pigz to compress data, default is off");
    //app.add_option("--pugzThread", cmd_info.pugz_threads_, "pugz thread number(<=8), default is 2");
    //app.add_option("--pigzThread", cmd_info.pigz_threads_, "pigz thread number, default is 2");
    stringstream ss;
    for (int i = 0; i < argc; i++) {
        ss << argv[i] << " ";
    }
    string command = ss.str();

    cmd_info.command_ = command;


    bool quVersion = false;
    app.add_flag("-V,--version", quVersion, "application version");

    bool preCache = false;
    app.add_flag("-C", preCache, "pre cache data");

    CLI11_PARSE(app, argc, argv);
    standard_gzip = swqc_select_standard_gzip(standard_gzip, legacy_gzip,
        {cmd_info.in_file_name1_, cmd_info.in_file_name2_,
         cmd_info.out_file_name1_, cmd_info.out_file_name2_});
    if (standard_gzip)
        swqc_validate_gzip_paths(cmd_info.in_file_name1_, cmd_info.in_file_name2_,
                                cmd_info.out_file_name1_, cmd_info.out_file_name2_);
    if (gzip_full_crc && (!standard_gzip || !discard_output)) {
        fprintf(stderr, "--gzipFullCRC requires the streaming gzip pipeline and --discardOutput\n");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    setenv("SWQC_GZIP_FULL_CRC", gzip_full_crc ? "1" : "0", 1);
    if (standard_gzip && (comm_size != 1 || cmd_info.in_file_name2_.empty() ||
        cmd_info.thread_number_ != 1 || cmd_info.interleaved_in_ || cmd_info.interleaved_out_ ||
        cmd_info.isStdin_ || cmd_info.isStdout_ ||
        cmd_info.is_TGS_ || cmd_info.do_overrepresentation_ || ordered_output ||
        output_fingerprint || cpe_fingerprint || borrow_input || replay_pairs || posix_output ||
        memory_workers != 1 || input_readers != 1 ||
        (discard_output && (swqc_gzip_path(cmd_info.out_file_name1_) || swqc_gzip_path(cmd_info.out_file_name2_))) ||
        cmd_info.splitWrite_ || gzip_workers < 1 || gzip_workers > SWQC_CPE_COUNT ||
        gzip_chunk_mib < 1 || gzip_chunk_mib > 64 || gzip_read_chunk_kib < 64 || gzip_read_chunk_kib > 4096 ||
        (gzip_reader != "auto" && gzip_reader != "rapid" && gzip_reader != "zlib"))) {
        fprintf(stderr, "The standard gzip pipeline (automatic for .gz paths) requires one MPI rank, paired files, -w 1, no replay/ordered MPI/ORP/TGS; gzip workers=1..%d, chunk=1..64 MiB. --legacyGzip is only for the historical private-footer format.\n", SWQC_CPE_COUNT);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    setenv("SWQC_STANDARD_GZIP", standard_gzip ? "1" : "0", 1);
    setenv("SWQC_GZIP_WORKERS", std::to_string(gzip_workers).c_str(), 1);
    setenv("SWQC_GZIP_CHUNK_MIB", std::to_string(gzip_chunk_mib).c_str(), 1);
    setenv("SWQC_GZIP_READER", gzip_reader.c_str(), 1);
    setenv("SWQC_GZIP_READ_CHUNK_KIB", std::to_string(gzip_read_chunk_kib).c_str(), 1);
    if (standard_gzip && !discard_output) {
        for (const auto &path : {cmd_info.out_file_name1_, cmd_info.out_file_name2_}) {
            struct stat info;
            if (!path.empty() && lstat(path.c_str(), &info) == 0)
                throw std::runtime_error("the standard gzip pipeline requires new output paths: " + path);
        }
        if (!cmd_info.out_file_name1_.empty() && cmd_info.out_file_name1_ == cmd_info.out_file_name2_)
            throw std::runtime_error("paired outputs must use different paths");
    }
    if (ordered_chunk_kib < 64 || ordered_chunk_kib > 1024 ||
        (ordered_chunk_kib != 1024 && !ordered_output)) {
        fprintf(stderr, "--orderedChunkKiB requires ordered input and a value in 64..1024\n");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    setenv("SWQC_ORDERED_CHUNK_KIB", std::to_string(ordered_chunk_kib).c_str(), 1);
    setenv("SWQC_DEFER_LOGS", defer_logs ? "1" : "0", 1);
    if (defer_logs && swqc_log_open_rank(my_rank) != 0) {
        perror("cannot open rank diagnostic log");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    if (cpe_fingerprint && !output_fingerprint && !discard_output) {
        fprintf(stderr, "--cpeFingerprint requires --outputFingerprint or --discardOutput\n");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    setenv("SWQC_CPE_FINGERPRINT", cpe_fingerprint ? "1" : "0", 1);
    if ((discard_output || output_fingerprint || preload_input || replay_pairs) &&
        ((!ordered_output && !standard_gzip) || cmd_info.thread_number_ != 1 ||
         cmd_info.out_file_name1_.empty() || cmd_info.out_file_name2_.empty() ||
         cmd_info.do_overrepresentation_ || cmd_info.is_TGS_ || posix_output)) {
        fprintf(stderr, "I/O isolation requires plain ordered paired MPI output, -w 1, without ORP/TGS\n");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    if (borrow_input && !preload_input) {
        fprintf(stderr, "--borrowInput requires --preloadInput\n");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    setenv("SWQC_BORROW_INPUT", borrow_input ? "1" : "0", 1);
    if (preload_input && ((!standard_gzip && input_window_mib < 4) || preload_limit_mib < 1 || preload_limit_mib > 65536)) {
        fprintf(stderr, "preload requires an input window and a 1..65536 MiB payload limit per rank\n");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    setenv("SWQC_DISCARD_OUTPUT", discard_output ? "1" : "0", 1);
    setenv("SWQC_OUTPUT_FINGERPRINT", (output_fingerprint || discard_output) ? "1" : "0", 1);
    setenv("SWQC_PRELOAD_INPUT", preload_input ? "1" : "0", 1);
    setenv("SWQC_PRELOAD_LIMIT_MIB", std::to_string(preload_limit_mib).c_str(), 1);
    if (replay_pairs && (preload_input || borrow_input || !replay_copies ||
                        replay_limit_mib < 1 || replay_limit_mib > 2048)) {
        fprintf(stderr, "replay requires positive copies, a 1..2048 MiB limit, and no preload/borrow\n");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    setenv("SWQC_REPLAY_PAIRS", std::to_string(replay_pairs).c_str(), 1);
    setenv("SWQC_REPLAY_COPIES", std::to_string(replay_copies).c_str(), 1);
    setenv("SWQC_REPLAY_LIMIT_MIB", std::to_string(replay_limit_mib).c_str(), 1);
    if ((replay_layout != "source" && replay_layout != "qc_blocks") ||
        replay_block_chunks < 1 || replay_block_chunks > 4096 ||
        (replay_layout != "source" && (!replay_pairs || cmd_info.isPhred64_))) {
        fprintf(stderr, "qc_blocks requires replay of fixed-length Phred33 pairs; block chunks must be 1..4096\n");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    setenv("SWQC_REPLAY_LAYOUT", replay_layout.c_str(), 1);
    setenv("SWQC_REPLAY_BLOCK_CHUNKS", std::to_string(replay_block_chunks).c_str(), 1);
    setenv("SWQC_REPLAY_SEED", std::to_string(replay_seed).c_str(), 1);
    setenv("SWQC_PROFILE_ROUNDS", profile_rounds ? "1" : "0", 1);
    if (serial_stages && ((!ordered_output && !standard_gzip) || cmd_info.thread_number_ != 1 ||
        cmd_info.out_file_name1_.empty() || cmd_info.out_file_name2_.empty() ||
        cmd_info.do_overrepresentation_ || cmd_info.is_TGS_ ||
        (standard_gzip && (swqc_gzip_path(cmd_info.out_file_name1_) || swqc_gzip_path(cmd_info.out_file_name2_))))) {
        fprintf(stderr, "--serialStages requires ordered or standard-gzip paired input, plain output, -w 1, without ORP/TGS\n");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    setenv("SWQC_SERIAL_STAGES", serial_stages ? "1" : "0", 1);
    if ((input_readers != 1 && input_readers != 6) ||
        (input_readers > 1 && (!ordered_output || input_window_mib == 0 || SWQC_EXEC_MODE == SWQC_SINGLE))) {
        fprintf(stderr, "six input readers require BigShare, ordered output and an input window\n");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    setenv("SWQC_INPUT_READERS", std::to_string(input_readers).c_str(), 1);
    if ((memory_workers != 1 && memory_workers != 6) ||
        (memory_workers > 1 && ((!preload_input && !replay_pairs) || input_readers != 1 ||
                               SWQC_EXEC_MODE == SWQC_SINGLE))) {
        fprintf(stderr, "six memory workers require BigShare, preload/replay and one input reader\n");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    setenv("SWQC_MEMORY_WORKERS", std::to_string(memory_workers).c_str(), 1);
    if (input_window_mib < 0 || input_window_mib > 256 ||
        (input_window_mib > 0 && input_window_mib < 4) ||
        output_window_mib < 0 || output_window_mib > 256 ||
        ((input_window_mib || output_window_mib || posix_output) && !ordered_output)) {
        fprintf(stderr, "I/O windows require --orderedOutput and valid MiB sizes\n");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    setenv("SWQC_INPUT_WINDOW_MIB", std::to_string(input_window_mib).c_str(), 1);
    setenv("SWQC_OUTPUT_WINDOW_MIB", std::to_string(output_window_mib).c_str(), 1);
    setenv("SWQC_POSIX_OUTPUT", posix_output ? "1" : "0", 1);
    if (ordered_output) {
        if (cmd_info.in_file_name2_.empty() || cmd_info.splitWrite_ ||
            cmd_info.in_file_name1_.find(".gz") != string::npos ||
            cmd_info.in_file_name2_.find(".gz") != string::npos ||
            cmd_info.out_file_name1_.find(".gz") != string::npos ||
            cmd_info.out_file_name2_.find(".gz") != string::npos) {
            fprintf(stderr, "--orderedOutput requires plain paired FASTQ and shared output\n");
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
        setenv("SWQC_ORDERED_INPUT", "1", 1);
    } else {
        unsetenv("SWQC_ORDERED_INPUT");
    }

#if SWQC_EXEC_MODE != SWQC_SINGLE
    if (cmd_info.in_file_name2_.empty() || cmd_info.thread_number_ != 1 ||
        (!standard_gzip && (cmd_info.in_file_name1_.find(".gz") != string::npos ||
        cmd_info.in_file_name2_.find(".gz") != string::npos ||
        cmd_info.out_file_name1_.find(".gz") != string::npos ||
        cmd_info.out_file_name2_.find(".gz") != string::npos)) ||
        cmd_info.do_overrepresentation_ || cmd_info.is_TGS_) {
        fprintf(stderr, "BigShare requires PE, -w 1, without ORP/TGS; legacy gzip is not supported\n");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
#endif

    //TODO remove it
    if(preCache) {
        printf("now pre read to cache data...\n");
        double t_t0 = GetTime();
        ifstream iff;
        printf("now pre read %s\n", cmd_info.in_file_name1_.c_str());
        iff.open(cmd_info.in_file_name1_);
        const size_t chunkSize = 4 * 1024 * 1024;
        char* tmp_chunk = new char[chunkSize];
        streamsize tot_size = 0;
        while (!iff.eof()) {
            iff.read(tmp_chunk, chunkSize);
            streamsize bytesRead = iff.gcount();
            tot_size += bytesRead;
            if (bytesRead < chunkSize) {
                break;
            }
        }
        cout << "Total bytes read: " << tot_size << endl;
        iff.close();
        if(cmd_info.in_file_name2_.length()) {
            printf("now pre read %s\n", cmd_info.in_file_name1_.c_str());
            iff.open(cmd_info.in_file_name1_);
            streamsize tot_size2 = 0;
            while (!iff.eof()) {
                iff.read(tmp_chunk, chunkSize);
                streamsize bytesRead = iff.gcount();
                tot_size2 += bytesRead;
                if (bytesRead < chunkSize) {
                    break;
                }
            }
            cout << "Total bytes read2: " << tot_size2 << endl;
        }
        printf("cost %lf\n", GetTime() - t_t0);
    }


    if (cmd_info.compression_level_ < 1 || cmd_info.compression_level_ > 9) {
        printf("error : compression level should in [1, 9]!\n");
        return 0;
    }

    if (quVersion) {
        printf("0.0.2\n");
        return 0;
    }

    //for read split version
    //cmd_info.in_file_name1_ = cmd_info.in_file_name1_ + "." + to_string(my_rank);
    //printf("now input1 file %s\n", cmd_info.in_file_name1_.c_str());

    //if(cmd_info.in_file_name2_.length()) {
    //    cmd_info.in_file_name2_ = cmd_info.in_file_name2_ + "." + to_string(my_rank);
    //    printf("now input2 file %s\n", cmd_info.in_file_name2_.c_str());
    //}

    if (tmp_no_dup_)cmd_info.state_duplicate_ = false;

    if(cmd_info.splitWrite_) {
        cmd_info.out_file_name1_ = "rank" + to_string(my_rank) + "_" + cmd_info.out_file_name1_;
        printf("now use split write module, %s\n", cmd_info.out_file_name1_.c_str());
        if(cmd_info.out_file_name2_.length()) {
            cmd_info.out_file_name2_ = "rank" + to_string(my_rank) + "_" + cmd_info.out_file_name2_;
            printf("now use split write module, PE, %s\n", cmd_info.out_file_name2_.c_str());
        }
    }

    if (cmd_info.is_TGS_) {
        if (cmd_info.in_file_name2_.length() > 0) {
            printf("WARNING : the TGS module does not support pe inputs, so ignore the -I parameter.\n");
            cmd_info.in_file_name2_ = "";
        }
        if (cmd_info.out_file_name1_.length() != 0 || cmd_info.out_file_name2_.length() != 0) {
            printf("WARNING : the TGS module does not support trimming and will not produce output files, so ignore the -o and -O parameter.\n");
            cmd_info.out_file_name1_ = "";
            cmd_info.out_file_name2_ = "";
        }
    }

    if (cmd_info.in_file_name2_.length() > 0 && cmd_info.out_file_name1_.length() > 0 && cmd_info.out_file_name2_.length() == 0 &&
            cmd_info.interleaved_out_ == 0) {
        error_exit("when interleaved_out module is off, two input files can not correspond to one output.\n");
    }


    if (cmd_info.out_file_name2_.length() > 0 && cmd_info.in_file_name2_.length() == 0 &&
            cmd_info.interleaved_in_ == 0) {
        error_exit("when interleaved_in module is off, one input file can not correspond to two outputs.\n");
    }


    int is_pe = cmd_info.in_file_name2_.length() != 0;

    //check if files is ok
    int files_ok = 1;
    if (is_pe) {
        if (!cmd_info.interleaved_in_) {
            if (ends_with(cmd_info.in_file_name1_, ".gz") != ends_with(cmd_info.in_file_name2_, ".gz"))
                files_ok = 0;
        }
        if (!cmd_info.interleaved_out_) {
            if (ends_with(cmd_info.out_file_name1_, ".gz") != ends_with(cmd_info.out_file_name2_, ".gz"))
                files_ok = 0;
        }
    }
    if (files_ok == 0) {
        printf("error : for PE data, both files must be of the same type, i.e. cannot be one compressed and one uncompressed!");
        return 0;
    }

    int in_gz = ends_with(cmd_info.in_file_name1_, ".gz");
    int out_gz = ends_with(cmd_info.out_file_name1_, ".gz");


    int in_module = 0;
    if (in_gz) in_module |= 1;
    if (out_gz) in_module |= 2;

    if (cmd_info.in_file_name2_.length() == 0)cmd_info.thread_number_ = min(32, cmd_info.thread_number_);
    else cmd_info.thread_number_ = min(cmd_info.thread_number_, 64);

    int t1, t2, t3;
    if (is_pe) {
        //PE
        int tot_threads = cmd_info.thread_number_;
        if (in_module == 3) {
            t1 = thread_assignment_pe3[tot_threads].pugz_t;
            t2 = thread_assignment_pe3[tot_threads].w_t;
            t3 = thread_assignment_pe3[tot_threads].pigz_t;
        } else if (in_module == 2) {
            t1 = thread_assignment_pe2[tot_threads].pugz_t;
            t2 = thread_assignment_pe2[tot_threads].w_t;
            t3 = thread_assignment_pe2[tot_threads].pigz_t;
        } else if (in_module == 1) {
            t1 = thread_assignment_pe1[tot_threads].pugz_t;
            t2 = thread_assignment_pe1[tot_threads].w_t;
            t3 = thread_assignment_pe1[tot_threads].pigz_t;
        } else {
            t1 = thread_assignment_pe0[tot_threads].pugz_t;
            t2 = thread_assignment_pe0[tot_threads].w_t;
            t3 = thread_assignment_pe0[tot_threads].pigz_t;
        }
    } else {
        //SE
        int tot_threads = cmd_info.thread_number_;
        if (in_module == 3) {
            t1 = thread_assignment_se3[tot_threads].pugz_t;
            t2 = thread_assignment_se3[tot_threads].w_t;
            t3 = thread_assignment_se3[tot_threads].pigz_t;
        } else if (in_module == 2) {
            t1 = thread_assignment_se2[tot_threads].pugz_t;
            t2 = thread_assignment_se2[tot_threads].w_t;
            t3 = thread_assignment_se2[tot_threads].pigz_t;
        } else if (in_module == 1) {
            t1 = thread_assignment_se1[tot_threads].pugz_t;
            t2 = thread_assignment_se1[tot_threads].w_t;
            t3 = thread_assignment_se1[tot_threads].pigz_t;
        } else {
            t1 = thread_assignment_se0[tot_threads].pugz_t;
            t2 = thread_assignment_se0[tot_threads].w_t;
            t3 = thread_assignment_se0[tot_threads].pigz_t;
        }
    }

    cmd_info.thread_number_ = t2;
    if (t1 == 0) {
        cmd_info.use_pugz_ = 0;
    } else {
        cmd_info.use_pugz_ = 1;
        cmd_info.pugz_threads_ = t1;
    }
    if (t3 <= 1) {
        cmd_info.use_pigz_ = 0;
    } else {
        cmd_info.use_pigz_ = 1;
        cmd_info.pigz_threads_ = t3;
    }

    if (cmd_info.interleaved_in_ || cmd_info.interleaved_out_) {
        cmd_info.use_pugz_ = 0;
        cmd_info.use_pigz_ = 0;
    }


    //printf("t1, t2, t3 is %d %d %d\n", t1, t2, t3);

    //printf(" pugz is %d\n", cmd_info.use_pugz_);
    //printf(" pigz is %d\n", cmd_info.use_pigz_);
    //printf(" pugz threads are %d\n", cmd_info.pugz_threads_);
    //printf(" pigz threads are %d\n", cmd_info.pigz_threads_);
    //printf(" qc threads are %d\n", cmd_info.thread_number_);
    if (cmd_info.isStdin_) {
        cmd_info.in_file_name1_ = "/dev/stdin";
    }
    if (cmd_info.in_file_name1_ == "/dev/stdin") {
        cmd_info.se_auto_detect_adapter_ = false;
        cmd_info.pe_auto_detect_adapter_ = false;
        cmd_info.do_overrepresentation_ = false;
    }
    if (!quVersion && cmd_info.in_file_name1_.length() == 0) {
        error_exit("-i/--inFile1 can't be null");
    }
    printf("inFile1 is %s\n", cmd_info.in_file_name1_.c_str());
    if (cmd_info.isStdout_) cmd_info.out_file_name1_ = "/dev/stdout";
    if (cmd_info.in_file_name2_.length()) printf("inFile2 is %s\n", cmd_info.in_file_name2_.c_str());
    if (cmd_info.out_file_name1_.length()) {
        bool res = !discard_output && exists_file(cmd_info.out_file_name1_);
        if (res) {
            string tmps;
            if (cmd_info.overWrite_) {
                tmps = "y";
            } else {
                char tmp[100];
                printf("\n");
                printf("%s already exists, overwrite it or not ? (y/n)\n", cmd_info.out_file_name1_.c_str());
                scanf("%s", tmp);
                tmps = string(tmp);
                while (tmps != "y" && tmps != "n") {
                    printf("please input y or n\n");
                    scanf("%s", tmp);
                    tmps = string(tmp);
                }
            }
            if (tmps == "y") {
                remove(cmd_info.out_file_name1_.c_str());
            } else if (tmps == "n") {
                return 0;
            } else {
                assert(0);
            }
        }
        printf("outFile1 is %s\n", cmd_info.out_file_name1_.c_str());
    }
    if (cmd_info.out_file_name2_.length()) {
        bool res = !discard_output && exists_file(cmd_info.out_file_name2_);
        if (res) {
            string tmps;
            if (cmd_info.overWrite_) {
                tmps = "y";
            } else {
                char tmp[100];
                printf("\n");
                printf("%s already exists, overwrite it or not ? (y/n)\n", cmd_info.out_file_name2_.c_str());
                scanf("%s", tmp);
                tmps = string(tmp);
                while (tmps != "y" && tmps != "n") {
                    printf("please input y or n\n");
                    scanf("%s", tmp);
                    tmps = string(tmp);
                }
            }
            if (tmps == "y") {
                remove(cmd_info.out_file_name2_.c_str());
            } else if (tmps == "n") {
                return 0;
            } else {
                assert(0);
            }
        }
        printf("outFile2 is %s\n", cmd_info.out_file_name2_.c_str());
    }

    string prefix_name1;
    string prefix_name2;
    string suffix_name1;
    string suffix_name2;

    if (out_gz) {
        //printf("===out1 is %s\n", cmd_info.out_file_name1_.c_str());
        if (cmd_info.out_file_name2_.length() > 0) {
            //printf("===out2 is %s\n", cmd_info.out_file_name2_.c_str());
        }
        //printf("now change out file name...\n");
        int real_begin1 = 0;
        for (int i = cmd_info.out_file_name1_.length() - 1; i >= 0; i--) {
            if (cmd_info.out_file_name1_[i] == '/') {
                real_begin1 = i + 1;
                break;
            }
        }
        prefix_name1 = cmd_info.out_file_name1_.substr(0, real_begin1);
        suffix_name1 = cmd_info.out_file_name1_.substr(real_begin1, cmd_info.out_file_name1_.length() - real_begin1);
        //printf("prefix_name1 %s\n", prefix_name1.c_str());
        //printf("suffix_name1 %s\n", suffix_name1.c_str());
        //cmd_info.out_file_name1_ = prefix_name1 + "tmp_" + to_string(rand()) + suffix_name1;
        if (cmd_info.out_file_name2_.length() > 0) {
            int real_begin2 = 0;
            for (int i = cmd_info.out_file_name2_.length() - 1; i >= 0; i--) {
                if (cmd_info.out_file_name2_[i] == '/') {
                    real_begin2 = i + 1;
                    break;
                }
            }
            prefix_name2 = cmd_info.out_file_name2_.substr(0, real_begin2);
            suffix_name2 = cmd_info.out_file_name2_.substr(real_begin2,
                    cmd_info.out_file_name2_.length() - real_begin2);
            //printf("prefix_name2 %s\n", prefix_name2.c_str());
            //printf("suffix_name2 %s\n", suffix_name2.c_str());
            //cmd_info.out_file_name2_ = prefix_name2 + "tmp_" + to_string(rand()) + suffix_name2;
        }
        //printf("===out1 is %s\n", cmd_info.out_file_name1_.c_str());
        if (cmd_info.out_file_name2_.length() > 0) {
            //printf("===out2 is %s\n", cmd_info.out_file_name2_.c_str());
        }
    }

    if (cmd_info.no_trim_adapter_) cmd_info.trim_adapter_ = false;
    ASSERT(cmd_info.no_trim_adapter_ != cmd_info.trim_adapter_);
    if (!cmd_info.trim_adapter_) {
        cmd_info.se_auto_detect_adapter_ = false;
        cmd_info.pe_auto_detect_adapter_ = false;
        cmd_info.adapter_seq1_ = "";
        cmd_info.adapter_seq2_ = "";
        cmd_info.adapter_fasta_file_ = "";
        printf("no adapter trim (ignore '--adapterSeq*' and '--adapterFastaFile' options) because using the '-a (--noTrimAdapter)' option!\n");
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


    if (cmd_info.add_umi_) {
        printf("now doing umi add\n");
        printf("umi location is %s\n", umiLoc.c_str());
        printf("umi len is %d\n", cmd_info.umi_len_);
        printf("umi skip is %d\n", cmd_info.umi_skip_);
        printf("umi prefix is %s\n", cmd_info.umi_prefix_.c_str());

        if (umiLoc.empty())
            error_exit("You've enabled UMI by (--addUmi), you should specify the UMI location by (--umiLoc)");
        if (umiLoc != "index1" && umiLoc != "index2" && umiLoc != "read1" && umiLoc != "read2" &&
                umiLoc != "per_index" && umiLoc != "per_read") {
            error_exit("UMI location can only be index1/index2/read1/read2/per_index/per_read");
        }
        if (cmd_info.in_file_name2_.length() == 0 && (umiLoc == "index2" || umiLoc == "read2"))
            error_exit("You specified the UMI location as " + umiLoc + ", but the input data is not paired end.");
        if (cmd_info.umi_len_ == 0 && (umiLoc == "read1" || umiLoc == "read2" || umiLoc == "per_read"))
            error_exit(
                    "You specified the UMI location as " + umiLoc + ", but the length is not specified (--umiLen).");
        if (umiLoc == "index1") {
            cmd_info.umi_loc_ = UMI_LOC_INDEX1;
        } else if (umiLoc == "index2") {
            cmd_info.umi_loc_ = UMI_LOC_INDEX2;
        } else if (umiLoc == "read1") {
            cmd_info.umi_loc_ = UMI_LOC_READ1;
        } else if (umiLoc == "read2") {
            cmd_info.umi_loc_ = UMI_LOC_READ2;
        } else if (umiLoc == "per_index") {
            cmd_info.umi_loc_ = UMI_LOC_PER_INDEX;
        } else if (umiLoc == "per_read") {
            cmd_info.umi_loc_ = UMI_LOC_PER_READ;
        }
    }

    if (cmd_info.do_overrepresentation_) {
        printf("now doing overrepresentation\n");
        printf("overrepresentation sampling is %d\n", cmd_info.overrepresentation_sampling_);
    }

    //if (cmd_info.isPhred64_) {
    //    printf("now use phred64 input\n");
    //}


    if (cmd_info.use_pugz_) {
        if (cmd_info.pugz_threads_ > 8) {
            //printf("pugz thread number must <= 8, now set pugz thread number == 8.\n");
            cmd_info.pugz_threads_ = 8;
        }
        //printf("now use pugz, pugz thread number is %d\n", cmd_info.pugz_threads_);
    }
    if (cmd_info.use_pigz_) {
        //printf("now use pigz, pigz thread number is %d\n", cmd_info.pigz_threads_);
    }
    //if (cmd_info.thread_number_ == 1)
    //    printf("now use %d thread to do QC operations\n", cmd_info.thread_number_);
    //else
    //    printf("now use %d threads to do QC operations\n", cmd_info.thread_number_);
    int mx_len = Adapter::EvalMaxLen(cmd_info.in_file_name1_);
    //int mx_len = 100;
    //if (cmd_info.in_file_name1_.find("SRR24") != std::string::npos) {
    //    mx_len = 100;
    //} else if (cmd_info.in_file_name1_.find("SRR16") != std::string::npos) {
    //    mx_len = 150;
    //}
    printf("mx_len %d\n", mx_len);
    cmd_info.seq_len_ = mx_len > 0 ? mx_len : 200;
    if (cmd_info.adapter_fasta_file_.length() > 0) {
        printf("loading adatper from %s\n", cmd_info.adapter_fasta_file_.c_str());
        cmd_info.adapter_from_fasta_ = Adapter::LoadAdaptersFromFasta(cmd_info.adapter_fasta_file_);
        sort(cmd_info.adapter_from_fasta_.begin(), cmd_info.adapter_from_fasta_.end());
        for (auto item: cmd_info.adapter_from_fasta_)printf(" --- %s ---\n", item.c_str());
    }
    double t_start = GetTime();

    if (cmd_info.in_file_name2_.length() || cmd_info.interleaved_in_) {
        //PE
        if ((cmd_info.out_file_name1_.length() > 0 && cmd_info.out_file_name2_.length() > 0) ||
                cmd_info.interleaved_out_) {
            cmd_info.write_data_ = true;
        }

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
            double t2 = GetTime();
            printf("now auto detect adapter\n");
            cmd_info.adapter_seq1_ = Adapter::AutoDetect(cmd_info.in_file_name1_, cmd_info.trim_tail1_);
            cmd_info.adapter_seq2_ = Adapter::AutoDetect(cmd_info.in_file_name2_, cmd_info.trim_tail1_);
            if (cmd_info.adapter_seq1_.length()) {
                printf("find adapter %s in read1\n", cmd_info.adapter_seq1_.c_str());
                cmd_info.detect_adapter1_ = true;
            } else {
                printf("not find adapter in read1\n");
            }
            if (cmd_info.adapter_seq2_.length()) {
                printf("find adapter %s in read2\n", cmd_info.adapter_seq2_.c_str());
                cmd_info.detect_adapter2_ = true;
            } else {
                printf("not find adapter in read2\n");
            }
#ifdef Verbose
            printf("detect adapter cost %.5f\n", GetTime() - t2);
#endif
        }
#ifdef Verbose
        if (cmd_info.correct_data_) {
            printf("now correct data\n");
        }
#endif
        if (cmd_info.trim_adapter_ || cmd_info.correct_data_ || !cmd_info.no_insert_size_) {
            cmd_info.analyze_overlap_ = true;
            printf("for PE data, overlap analysis is used to find adapter by default\n");
        }

        if (cmd_info.trim_front1_) {
            printf("read1 trim front %d bases\n", cmd_info.trim_front1_);
            cmd_info.trim_front2_ = cmd_info.trim_front1_;
            printf("read2 trim front %d bases\n", cmd_info.trim_front2_);
        }
        if (cmd_info.trim_tail1_) {
            printf("read1 trim tail %d bases\n", cmd_info.trim_tail1_);
            cmd_info.trim_tail2_ = cmd_info.trim_tail1_;
            printf("read2 trim tail %d bases\n", cmd_info.trim_tail2_);
        }

        if (cmd_info.do_overrepresentation_) {
            double t2 = GetTime();
            printf("now doing overrepresent preprocessing part\n");
            Adapter::PreOverAnalyze(cmd_info.in_file_name1_, cmd_info.hot_seqs_, cmd_info.eva_len_);
            Adapter::PreOverAnalyze(cmd_info.in_file_name2_, cmd_info.hot_seqs2_, cmd_info.eva_len2_);
            printf("overrepresent preprocessing part done\n");
            printf("read1 has %d hot sequence\n", cmd_info.hot_seqs_.size());
            printf("read2 has %d hot sequence\n", cmd_info.hot_seqs2_.size());
#ifdef Verbose
            printf("pre over representation cost %.5f\n", GetTime() - t2);
#endif
        }

        if (cmd_info.interleaved_in_) {
            printf("now input use interleaved pe data\n");
        }
        if (cmd_info.interleaved_out_) {
            printf("now output use interleaved pe data\n");
        }
        if (cmd_info.adapter_seq1_.length() > 0)
            cmd_info.adapter_len_lim_ = min(cmd_info.adapter_len_lim_, int(cmd_info.adapter_seq1_.length()));
        if (cmd_info.adapter_seq2_.length() > 0)
            cmd_info.adapter_len_lim_ = min(cmd_info.adapter_len_lim_, int(cmd_info.adapter_seq2_.length()));

        PeQc *pe_qc = new PeQc(&cmd_info, my_rank, comm_size);
        pe_qc->ProcessPeFastq();
        delete pe_qc;

    } else {
        //SE
        cmd_info.no_insert_size_ = 1;
        if (cmd_info.out_file_name1_.length() > 0) {
            cmd_info.write_data_ = true;
        }
        //adapter
        if (cmd_info.adapter_seq1_.length()) {
            printf("input adapter is %s\n", cmd_info.adapter_seq1_.c_str());
            cmd_info.se_auto_detect_adapter_ = false;
            cmd_info.detect_adapter1_ = true;
        }
        if (cmd_info.se_auto_detect_adapter_) {
            double t2 = GetTime();
            printf("now auto detect adapter\n");
            cmd_info.adapter_seq1_ = Adapter::AutoDetect(cmd_info.in_file_name1_, cmd_info.trim_tail1_);
            if (cmd_info.adapter_seq1_.length()) {
                printf("find adapter %s\n", cmd_info.adapter_seq1_.c_str());
                cmd_info.detect_adapter1_ = true;
            } else {
                printf("not find adapter\n");
            }
#ifdef Verbose
            printf("detect adapter cost %.5f\n", GetTime() - t2);
#endif
        }
        if (cmd_info.trim_front1_) {
            printf("trim front %d bases\n", cmd_info.trim_front1_);
        }
        if (cmd_info.trim_tail1_) {
            printf("trim tail %d bases\n", cmd_info.trim_tail1_);
        }

        if (cmd_info.do_overrepresentation_) {
            double t2 = GetTime();
            printf("now doing overrepresent preprocessing part\n");
            Adapter::PreOverAnalyze(cmd_info.in_file_name1_, cmd_info.hot_seqs_, cmd_info.eva_len_);
            printf("overrepresent preprocessing part done\n");
            printf("total %d hot sqes\n", cmd_info.hot_seqs_.size());
#ifdef Verbose
            printf("pre over representation cost %.5f\n", GetTime() - t2);
#endif
        }
        if (cmd_info.adapter_seq1_.length() > 0)
            cmd_info.adapter_len_lim_ = min(cmd_info.adapter_len_lim_, int(cmd_info.adapter_seq1_.length()));
        SeQc *se_qc = new SeQc(&cmd_info, my_rank, comm_size);
        if (cmd_info.is_TGS_) {
            se_qc->ProcessSeTGS();
        } else {
            se_qc->ProcessSeFastq();

            MPI_Barrier(MPI_COMM_WORLD);
            //se_qc->ProcessSeFastqOneThread();
        }
        delete se_qc;
    }
    //if (out_gz) {
    //    string out_name1 = cmd_info.out_file_name1_;
    //    out_name1 = out_name1.substr(0, out_name1.find(".gz"));
    //    remove(out_name1.c_str());
    //    if (cmd_info.out_file_name2_.length() > 0) {
    //        string out_name2 = cmd_info.out_file_name2_;
    //        out_name2 = out_name2.substr(0, out_name2.find(".gz"));
    //        remove(out_name2.c_str());
    //    }
    //    string init_name1 = prefix_name1 + suffix_name1;
    //    rename(cmd_info.out_file_name1_.c_str(), init_name1.c_str());
    //    if (cmd_info.out_file_name2_.length() > 0) {
    //        string init_name2 = prefix_name2 + suffix_name2;
    //        rename(cmd_info.out_file_name2_.c_str(), init_name2.c_str());
    //    }
    //}
    printf("cmd is %s\n", command.c_str());
    printf("total cost %.5fs\n", GetTime() - t_start);

#ifdef use_swlu
    //swlu_prof_stop();
    //swlu_prof_print();
#endif	

    MPI_Finalize();
    printf("MPI_Finalize done\n");
    //exit(0);
    return 0;
} catch (const std::exception &e) {
    fprintf(stderr, "[SWQC error] %s\n", e.what());
    int initialized = 0, finalized = 0;
    MPI_Initialized(&initialized);
    if (initialized) MPI_Finalized(&finalized);
    if (initialized && !finalized) MPI_Abort(MPI_COMM_WORLD, 1);
    return 1;
}
