#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

usage() {
    printf '%s\n' \
        'Usage: bash run.sh single|cgs|cgs_cross [scheduler options] -- SWQC_OPTIONS' \
        '  --nodes N              Nodes (default 1)' \
        '  --ranks-per-node N     MPI ranks/node (single: 1..6; BigShare: 1)' \
        '  --job-name NAME        Scheduler job name (default swqc)' \
        '  --log FILE             Scheduler log path' \
        '  --dry-run              Print the command without submitting'
}
fail() { printf 'run.sh: %s\n' "$*" >&2; exit 2; }
if [[ ${1:-} == -h || ${1:-} == --help ]]; then usage; exit 0; fi
(( $# )) || { usage >&2; exit 2; }
mode=$1
shift
case $mode in single|cgs|cgs_cross) ;; *) fail "unknown mode: $mode" ;; esac
nodes=1
ranks=1
job=swqc
log=
dry_run=0
while (( $# )); do
    case $1 in
        --nodes|--ranks-per-node|--job-name|--log)
            (( $# >= 2 )) || fail "$1 requires a value"
            [[ -n $2 ]] || fail "$1 requires a nonempty value"
            case $1 in
                --nodes) nodes=$2 ;;
                --ranks-per-node) ranks=$2 ;;
                --job-name) job=$2 ;;
                --log) log=$2 ;;
            esac
            shift 2 ;;
        --dry-run) dry_run=1; shift ;;
        --) shift; break ;;
        *) fail "unknown scheduler option: $1 (put SWQC options after --)" ;;
    esac
done
[[ $nodes =~ ^[1-9][0-9]*$ ]] || fail 'nodes must be a positive integer'
[[ $ranks =~ ^[1-6]$ ]] || fail 'ranks per node must be 1..6'
[[ $mode == single || $ranks == 1 ]] || fail 'BigShare needs one MPI rank per node'
(( $# )) || fail 'missing SWQC options after --'

command=(bsub -b -I -q q_share -N "$nodes" -np "$ranks" -cgsp 64
         -J "$job" -cache_size 128 -priv_size 16)
if [[ $mode == single ]]; then
    command+=(-share_size 12000)
else
    command+=(-mpecg 6 -share_size 2000 -xmalloc -cross_size 42000)
fi
[[ -z $log ]] || command+=(-o "$log")
command+=(./SWQC "$@")
printf '%q ' "${command[@]}"
printf '\n'
(( dry_run )) && exit 0
for artifact in SWQC data.bin data.bin2; do
    [[ -f $artifact ]] || fail "missing $artifact; run build.sh with matching options first"
done
[[ -x SWQC ]] || fail 'SWQC is not executable'
# SWQC must be bsub's direct command, never a child of a scheduled shell.
exec "${command[@]}"
