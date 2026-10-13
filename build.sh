#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
jobs=${JOBS:-8}
mkdir -p obj

write_layout() {
    printf '#ifndef SWQC_LAYOUT_H\n#define SWQC_LAYOUT_H\n#define SWQC_TEXT_START %sUL\n#define SWQC_TEXT_SIZE %sUL\n#define SWQC_DATA_START %sUL\n#define SWQC_DATA_SIZE %sUL\n#endif\n' \
        "$1" "$2" "$3" "$4" > common/swqc_layout.h
}

# Same two-pass ELF/layout/TLS relocation workflow used by SWBWA.
write_layout 123 123 123 123
make clean
make -j "$jobs" "$@"
swreadelf -l SWQC > SWQC.segments
text_start=$(awk 'NR==9 {print $3}' SWQC.segments)
text_size=$(awk 'NR==10 {print $1}' SWQC.segments)
data_start=$(awk 'NR==11 {print $3}' SWQC.segments)
data_size=$(awk 'NR==12 {print $2}' SWQC.segments)
for value in "$text_start" "$text_size" "$data_start" "$data_size"; do
    [[ "$value" =~ ^0x[[:xdigit:]]+$ ]] || { echo 'Unrecognized CPE ELF layout' >&2; exit 1; }
done
write_layout "$text_start" "$text_size" "$data_start" "$data_size"
make clean
make -j "$jobs" "$@"
swreadelf -l SWQC > SWQC.segments.final
# Immediates must not change the section layout on the second pass.
diff <(sed -n '9,12p' SWQC.segments) <(sed -n '9,12p' SWQC.segments.final)
python3 common/xlink.py SWQC
swreadelf -r SWQC > SWQC.relocations
python3 common/get_tls.py SWQC.relocations
if [[ -f obj/codec_rapid_firstpass.o ]]; then
    cp SWQC SWQC.debug
    python3 common/compact_elf.py SWQC.debug SWQC.compact
    mv SWQC.compact SWQC
fi
