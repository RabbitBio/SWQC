#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
bash tests/run_script_test.sh
build=${BUILD_DIR:-tests/.build}
mkdir -p "$build"
flags=(-O1 -g -fsanitize=address,undefined -fno-omit-frame-pointer
       -ffunction-sections -fdata-sections -Wl,--gc-sections
       -Icommon -Isrc -Itests/stubs -I. -include common/swqc_config.h -pthread)
export ASAN_OPTIONS=${ASAN_OPTIONS:-detect_leaks=0}
run_check() {
    local test=$1
    if ! "$build/$test" > "$build/$test.log" 2>&1; then
        tail -n 40 "$build/$test.log"
        return 1
    fi
    printf 'PASS: %s\n' "$test"
}
${CC:-gcc} -std=gnu11 "${flags[@]}" -c src/host_io.c -o "$build/host_io.o"
${CC:-gcc} -std=gnu11 "${flags[@]}" -c src/host_log.c -o "$build/host_log.o"
for test in host_io host_log host_memory heap_copy ldm_arena serial_stages ring_cursor pool_batch buffer_view io_window output_discard read_ahead gzip_options; do
    extra=()
    [[ $test != heap_copy ]] || extra=(-DSWQC_MANUAL_LDM=0)
    [[ $test != buffer_view ]] || extra=(tests/cpe_buffer_layout.cpp)
    ${MPICXX:-mpicxx} -std=c++11 "${flags[@]}" "${extra[@]}" tests/${test}_test.cpp \
        "$build/host_io.o" "$build/host_log.o" -o "$build/$test"
    run_check "$test"
done
${MPICXX:-mpicxx} -std=c++11 "${flags[@]}" tests/crc_combine_test.cpp -lz -o "$build/crc_combine"
run_check crc_combine
for mode in SWQC_SINGLE SWQC_CROSS; do
    for test in ordered_input replay_input replay_bias; do
        ${MPICXX:-mpicxx} -std=c++11 "${flags[@]}" -DSWQC_EXEC_MODE=$mode \
            tests/${test}_test.cpp "$build/host_io.o" "$build/host_log.o" -o "$build/${test}_$mode"
        run_check "${test}_$mode"
    done
done
for test in state_roundtrip report_zero; do
    ${MPICXX:-mpicxx} -std=c++11 "${flags[@]}" tests/${test}_test.cpp \
        src/state.cpp src/cmdinfo.cpp -o "$build/$test"
    run_check "$test"
done
${MPICXX:-mpicxx} -std=c++11 "${flags[@]}" tests/gzip_pair_test.cpp \
    src/gzip_pipeline.cpp codec/gzip_stream.cpp codec/rapid_reader.cpp codec/codec_job.c \
    "$build/host_log.o" -lz -o "$build/gzip_pair"
run_check gzip_pair
${MPICXX:-mpicxx} -std=c++17 "${flags[@]}" -Icodec/vendor/rapidgzip -DNDEBUG -DSWQC_CODEC_RAPID=1 \
    -c codec/rapid_firstpass.cpp -o "$build/rapid_firstpass.o"
${CC:-gcc} -std=gnu11 "${flags[@]}" -DSWQC_CODEC_RAPID=1 \
    -c codec/codec_job.c -o "$build/codec_job_rapid.o"
${MPICXX:-mpicxx} -std=c++17 "${flags[@]}" -Icodec/vendor/rapidgzip -DSWQC_CODEC_RAPID=1 tests/gzip_pair_test.cpp \
    src/gzip_pipeline.cpp codec/gzip_stream.cpp codec/rapid_reader.cpp "$build/codec_job_rapid.o" \
    "$build/rapid_firstpass.o" "$build/host_log.o" -lz -o "$build/gzip_pair_rapid"
SWQC_GZIP_READ_CHUNK_KIB=64 SWQC_GZIP_WORKERS=2 run_check gzip_pair_rapid
printf 'PASS: all portable SWQC checks (ASan + UBSan)\n'
