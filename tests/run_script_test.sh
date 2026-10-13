#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
cross=$(bash run.sh cgs_cross --nodes 2 --dry-run -- -w 1 --orderedOutput -i r1.fq -I r2.fq)
[[ $cross == *'-q q_share -N 2 -np 1'* ]]
[[ $cross == *'-mpecg 6 -share_size 2000 -xmalloc -cross_size 42000'* ]]
[[ $cross == *'./SWQC -w 1 --orderedOutput'* ]]
single=$(bash run.sh single --ranks-per-node 6 --dry-run -- -i 'input with spaces.fq')
[[ $single == *'-np 6'* && $single != *'-mpecg'* ]]
[[ $single == *'input\ with\ spaces.fq'* ]]
for args in 'cgs --ranks-per-node 6' 'single --nodes 0' 'single --nodes -1' \
            'single --ranks-per-node 7' 'unknown' 'single --bad'; do
    # Intentionally split only these fixed test argument strings.
    if bash run.sh $args --dry-run -- -i r1.fq >/dev/null 2>&1; then
        printf 'unexpected success: %s\n' "$args" >&2
        exit 1
    fi
done
if bash run.sh single --log >/dev/null 2>&1; then exit 1; fi
if bash run.sh single --dry-run -- >/dev/null 2>&1; then exit 1; fi
printf 'PASS: run.sh resource selection, quoting and argument rejection (no job submitted)\n'
