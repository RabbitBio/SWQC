#!/usr/bin/env bash
# Compute-node verifier: only Bash and coreutils, no Python shared libraries.
set -euo pipefail
[[ $# == 4 || $# == 5 ]] || { echo 'usage: verify_pair.sh R1 R2 MD5_R1 MD5_R2 [--remove-on-pass]' >&2; exit 2; }
remove=${5:-}
[[ -z "$remove" || "$remove" == --remove-on-pass ]] || exit 2
r1=$1
r2=$2
expected1=$3
expected2=$4
[[ "$expected1" =~ ^[0-9a-f]{32}$ && "$expected2" =~ ^[0-9a-f]{32}$ ]] || exit 2
actual1=$(md5sum -- "$r1")
actual2=$(md5sum -- "$r2")
printf '%s\n%s\n' "$actual1" "$actual2"
[[ "${actual1%% *}" == "$expected1" && "${actual2%% *}" == "$expected2" ]] || {
    echo 'RESULT: FAIL; outputs retained'
    exit 1
}
echo 'RESULT: PASS; both complete ordered FASTQ MD5 values match'
if [[ "$remove" == --remove-on-pass ]]; then
    rm -- "$r1" "$r2"
    echo 'Removed only the two verified output files.'
fi
