# SWQC

SWQC performs sequencing-data quality control on the next-generation Sunway
platform. It supports adapter trimming, quality filtering, paired-read
correction and QC reports, with CPE processing and distributed FASTQ I/O.

This branch adds:

- Single-CG (64 CPE) and BigShare (384 CPE) execution, including cross-segment
  code placement and CPE allocation pools.
- Globally ordered, paired FASTQ output across MPI ranks.
- Ordinary gzip input and output integrated into the single-rank PE pipeline:
  CPE parallel first-pass decoding, leased decoded buffers and pigz-style
  parallel compression. No prebuilt index or preliminary full-file decode.
- Optional sample replay, discarded output and timing diagnostics. Normal
  file input and output remain the defaults.

## Build

The application requires the Sunway host/CPE compiler toolchain, MPI, zlib,
pthreads, Python 3 and the `swreadelf` utility. The default parallel gzip decoder
also requires C++17 support for its CPE translation unit; the rest uses C++11/C11.
Portable helper tests can run on Linux with GCC, MPI and zlib.

Use `build.sh`, not a standalone `make`, to produce a runnable deployment:

```bash
git clone https://github.com/RabbitBio/SWQC.git
cd SWQC
git switch feature/bigshare-gzip

# One core group, system allocator.
bash build.sh EXEC_MODE=single CPE_ALLOCATOR=system

# Six core groups, default code placement.
bash build.sh EXEC_MODE=cgs CPE_ALLOCATOR=system
bash build.sh EXEC_MODE=cgs CPE_ALLOCATOR=pool

# Six core groups, cross-segment code and pooled allocation.
bash build.sh EXEC_MODE=cgs_cross CPE_ALLOCATOR=pool
```

Each command replaces the previous build. `JOBS=8` is the default build
parallelism. The script builds twice, extracts and validates CPE segment
addresses, and generates relocation/TLS maps. This two-pass workflow is
required for cross-segment execution. Deploy `SWQC`, `data.bin` and `data.bin2`
together, and run from that directory. Keep `SWQC.debug`, when generated, for
symbolization. Do not replace a build while a job is using it.

The linker command selects the MPI wrapper's `multi_static` library instead of
`single_static`, without editing system MPI configuration. At startup SWQC
requests and checks `MPI_THREAD_MULTIPLE`; a single-thread-only MPI library is
not sufficient for the application threads.

| Build option | Default | Meaning |
| --- | --- | --- |
| `EXEC_MODE` | `single` | `single`, `cgs` or `cgs_cross` |
| `CPE_ALLOCATOR` | `system` | System allocation or `pool` |
| `CPE_MANUAL_LDM` | `1` | Explicit QC working-data placement in LDM |
| `CPE_LDM_POOL` | `0` | Optional LDM-first allocator; requires `CPE_ALLOCATOR=pool` |
| `LDM_POOL_BYTES` | `32768` | Per-CPE LDM pool capacity; multiple of 64, 256 bytes through 56 KiB |
| `GZIP_DECODER` | `rapid` | Parallel first-pass decoder, or serial `zlib` build |
| `GZIP_CPE_LDM` | `2` | Decoder state: `0` heap, `1` allocator pool, `2` split hot-state LDM placement |

`GZIP_CPE_LDM=1` also requires `CPE_LDM_POOL=1`. QC manual LDM placement and
gzip decoder placement are separate settings. For an allocator-only QC
configuration, use `CPE_MANUAL_LDM=0 CPE_LDM_POOL=1`. Do not enlarge the pool
without accounting for the CPE stack, cache and other LDM users.

## Run on Sunway

`run.sh` constructs a `q_share` submission. Its first argument must match the
build's execution mode. Use `--dry-run` to inspect the command before submission:

```bash
bash run.sh cgs_cross --nodes 1 --job-name swqc_pe --dry-run -- \
    -w 1 --orderedOutput -i reads_1.fastq -I reads_2.fastq \
    -o clean_1.fastq -O clean_2.fastq
```

Remove `--dry-run` to submit. Scheduler options go before `--`; SWQC options go
after it. Optional `--log run.log` selects the scheduler log file.

The helper requests 64 CPEs per CG and 128 KiB cache. BigShare requests six
CGs for one MPI rank per node, with `-xmalloc -cross_size 42000`. Single-CG
execution permits `--ranks-per-node 1` through `6`. The helper uses the site's
`-np` ranks-per-node convention. Review these resource flags for your cluster.

Examples after building the corresponding mode:

```bash
# Plain SE input, single-CG build.
bash run.sh single -- -i reads.fastq -o clean.fastq

# Plain PE input, distributed ordered output, one rank per node.
bash run.sh cgs_cross --nodes 2 -- -w 1 --orderedOutput \
    -i reads_1.fastq -I reads_2.fastq -o clean_1.fastq -O clean_2.fastq

# Same ordered-output path, six single-CG ranks per node.
bash run.sh single --nodes 2 --ranks-per-node 6 -- -w 1 --orderedOutput \
    -i reads_1.fastq -I reads_2.fastq -o clean_1.fastq -O clean_2.fastq

# Ordinary gzip input and output, one MPI rank.
bash run.sh cgs_cross --nodes 1 -- -w 1 \
    -i reads_1.fastq.gz -I reads_2.fastq.gz \
    -o clean_1.fastq.gz -O clean_2.fastq.gz
```

The plain distributed PE reader uses matching byte boundaries for the two mate
files: inputs must have matching record layouts. The gzip streaming PE reader
instead assembles complete pairs from independent decoded streams. BigShare
currently requires PE, `-w 1`, and no TGS/overrepresentation analysis. The
plain SE path remains available in single-CG mode.

## Ordinary Gzip Pipeline

A `.gz` input or output path automatically selects the integrated standard
gzip pipeline. It supports paired files, one MPI rank and `-w 1`; it does not
support distributed gzip, SE, interleaved streams, stdin/stdout, TGS or
overrepresentation analysis. Unsupported combinations fail explicitly.
Both mates must use matching input formats and matching output formats.
Choose new output paths: this pipeline does not overwrite existing files.

```text
compressed-byte prefetch
  -> CPE speculative DEFLATE decode
  -> dictionary linking and CPE reference resolution
  -> leased FASTQ spans / record-boundary bridge
  -> CPE QC
  -> CPE raw-DEFLATE compression and ordered gzip assembly
```

The first-pass engine uses adapted rapidgzip components. It finds DEFLATE
boundaries and resolves dictionary references without an index-building pass.
Decoded buffers stay alive through QC using leases; records crossing buffers
use a small bridge instead of copying every decoded byte. Unsupported
speculative cases use a bounded serial fallback. Gzip members and checksums
are validated. This is an SWQC integration, not the upstream rapidgzip CLI.

Compression uses parallel zlib jobs with preceding 32 KiB dictionaries and
ordered fragment/checksum assembly. Output is an ordinary gzip stream readable
by standard tools, not the historical SWQC private-footer format.

| Runtime option | Default | Meaning |
| --- | --- | --- |
| `--gzipReader` | `auto` | Build default; `rapid` parallel decode or `zlib` serial decode |
| `--gzipWorkers` | Configured CPE count | Workers per codec batch, up to 64 or 384 |
| `--gzipReadChunkKiB` | `256` | Compressed-byte job target, 64 through 4096 KiB |
| `--gzipChunkMiB` | `1` | Compression block target, 1 through 64 MiB |
| `--compressLevel` | `4` | Compression level, 1 through 9 |
| `--standardGzip` | Off | Explicitly use this streaming pipeline with plain input/output |
| `--legacyGzip` | Off | Explicit compatibility path for the old private-footer format |

Do not use `--legacyGzip` for arbitrary gzip files. `--orderedOutput` selects
the separate plain MPI path and must not be combined with the standard gzip
pipeline, whose single-rank paired output is already ordered.

Advanced memory controls are environment variables:

- `SWQC_GZIP_PREFETCH_MIB`: size of each compressed-byte prefetch slot,
  default 64 MiB, four slots per reader. `0` disables prefetch; maximum 256.
- `SWQC_GZIP_SYMBOL_BUDGET_MIB`: speculative symbol-buffer budget per reader,
  default 2048 MiB, range 512 through 2048. High-expansion inputs reduce batch
  concurrency as buffers grow. This is not a bound on total process memory.
- `SWQC_GZIP_ZERO_COPY`: default `1`; `0` uses the copying bridge for comparison.

PE has two readers. Their buffers, QC state, codec state and stack/cache
reservations all contribute to memory use; select a smaller budget when needed.

## Replay and Discard Diagnostics

These options are opt-in and do not change normal production I/O defaults.
For plain paired input they require `--orderedOutput`, `-w 1`, output path
arguments, and no TGS/overrepresentation analysis or `--posixOutput`.

```bash
bash run.sh cgs_cross --nodes 1 -- -w 1 --orderedOutput \
    -i reads_1.fastq -I reads_2.fastq -o unused_1.fastq -O unused_2.fastq \
    --replayPairs 100000 --replayCopies 20 --discardOutput --cpeFingerprint
```

`--replayPairs N` caches the first N real pairs before pipeline timing and
replays them `--replayCopies C` times (default 20). Names are made unique;
sequence and quality bytes are retained. `--replayLimitMiB` defaults to
1024 MiB per rank for the cached payload and index. This is synthetic input:
repeated sequences affect duplicate statistics and must not be presented as
independent biological reads.

`--replayLayout qc_blocks` groups fixed-length Phred33 pairs by overlap and
quality proxies to expose workload variation; `source` is the default.
`--replayBlockChunks` (default 32) and `--replaySeed` (default 1) control the
rank-independent grouping and shuffle. Replay cannot be combined with preload
or the standard gzip pipeline.

`--discardOutput` retains QC, output construction and ordered coordination,
but skips physical output writes and enables a sampled fingerprint. It does
not create or delete the named output files. The plain MPI output protocol
still exchanges sizes and synchronizes ranks. This diagnostic is not a
measurement of storage throughput.

The fingerprint samples one byte at each global 4096-byte interval per mate,
mixes in the byte position, and reports sample count, total bytes, 64-bit sum
and XOR. It is not a full checksum or proof of equality. `--cpeFingerprint`
moves the same sampling to CPEs for the plain ordered path. Use real output
and full-file checksums for final correctness validation.

The standard gzip pipeline also supports `--discardOutput` with plain output
path arguments, without `--orderedOutput` or `--cpeFingerprint`. Its optional
`--gzipFullCRC` checks all discarded FASTQ bytes; this costs a full scan and
is CRC, not a cryptographic hash.

## I/O and Timing Controls

For the plain ordered path:

- `--orderedChunkKiB`: nominal bytes per CPE per mate, default 1024 KiB.
- `--inputWindowMiB` / `--outputWindowMiB`: contiguous read-ahead / write
  buffers; default 0, disabled. Input accepts 4 through 256 MiB; output 0
  through 256 MiB.
- `--inputReaders 6`: use the six MPEs for BigShare input with an input window;
  default 1. `--memoryWorkers 6` instead parallelizes resident preload/replay
  copies; it requires BigShare and `--inputReaders 1`.
- `--preloadInput`: load this rank's real input before timing. Plain input
  requires an input window. `--preloadLimitMiB` defaults to 16384 MiB per rank
  and limits payload only, not process RSS; reduce it on memory-limited jobs.
  `--borrowInput` references stable preloaded plain bytes without input copies.
- `--posixOutput`: positional writes instead of MPI file writes for plain
  ordered output. Do not combine with replay, preload or discard diagnostics.

`--serialStages` disables reader/processor/writer overlap for timing, using
plain paired output on either the ordered or streaming pipeline.
Without it, stage timers overlap: adding reader, QC and writer time is not
the end-to-end pipeline time. Codec API timers can include preparation, CPE
work and handoff, so nested timers must not be added again.
`--profileRounds` logs individual plain processor rounds; `--deferLogs`
defers normal diagnostics while keeping errors live.

## Source and Tests

| Path | Contents |
| --- | --- |
| `src/` | MPE application, QC pipeline, input and output coordination |
| `slave/` | CPE QC kernels, runtime and allocation support |
| `common/` | Shared configuration, ABI, LDM and relocation helpers |
| `codec/` | Integrated gzip engine and standalone codec checks |
| `codec/vendor/` | Required rapidgzip/zlib sources and license notices |
| `tests/` | Portable unit, sanitizer and FASTQ verification tests |
| `build.sh`, `run.sh` | Two-pass build and scheduler submission helpers |

On a Linux development machine:

```bash
bash tests/run_host_checks.sh
make -C codec check
make -C codec DECODER=zlib OUT=build-zlib check
```

Tests cover record boundaries, mate pairing, replay, discarded output,
buffer ownership, LDM allocation, gzip headers/CRC, concatenated members and
high-expansion input. Host tests cannot validate Sunway CPE relocation,
instruction behavior or actual LDM capacity; run a small end-to-end Sunway
job after changing the native runtime or kernels.

SWQC's license is in [LICENSE](LICENSE). Third-party provenance and local
codec adaptations are recorded in [codec/vendor/NOTICE](codec/vendor/NOTICE);
upstream license and copyright notices are retained.
