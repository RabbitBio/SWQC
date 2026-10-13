#!/usr/bin/env python3
"""Check streaming queue boundaries and standard gzip interoperability."""
import gzip
import os
import pathlib
import random
import re
import subprocess
import sys
import tempfile
import zlib

binary = pathlib.Path(sys.argv[1]).resolve()
rng = random.Random(17)
with tempfile.TemporaryDirectory(prefix="swqc-gzip-stream-") as directory:
    root = pathlib.Path(directory)
    samples = [b"", b"x", b"ATCGN" * 8193,
               bytes(rng.randrange(256) for _ in range((2 << 20) + 31)),
               b"ACGT" * (9 << 20)]
    for i, data in enumerate(samples):
        # Concatenated members must be indistinguishable from a single stream.
        cut = len(data) // 3
        source = root / f"{i}.gz"
        source.write_bytes(gzip.compress(data[:cut]) + gzip.compress(data[cut:]))
        for workers in (1, 64, 384):
            out = root / f"{i}.{workers}.gz"
            subprocess.run([binary, source, out, str(workers)], check=True)
            compressed = out.read_bytes()
            assert gzip.decompress(compressed) == data
            decoder = zlib.decompressobj(31)
            assert decoder.decompress(compressed) == data and decoder.eof
            assert not decoder.unused_data, "writer must emit ONE member with no private footer"
            before = compressed
            assert subprocess.run([binary, source, out, str(workers)], capture_output=True).returncode
            assert out.read_bytes() == before, "existing output must not be overwritten"
            out.unlink()
    for level in (1, 3, 4, 9):
        out = root / f"level{level}.gz"
        subprocess.run([binary, source, out, "64", str(level)], check=True)
        assert gzip.decompress(out.read_bytes()) == samples[-1]
    bad = root / "bad.gz"
    damaged = bytearray(gzip.compress(b"abc" * 100))
    damaged[-8] ^= 1
    bad.write_bytes(damaged)
    failed = root / "failed.gz"
    assert subprocess.run([binary, bad, failed, "64"], capture_output=True).returncode
    assert not failed.exists()
print("STREAM_TEST_PASS: irregular fragments, dictionaries, CRC, empty, members, no footer")

# Multiple dynamic blocks, genuine unknown-window references, and exceptional headers.
with tempfile.TemporaryDirectory(prefix="swqc-gzip-firstpass-") as directory:
    root = pathlib.Path(directory)
    bases = bytes(rng.choices(b"ACGTN", weights=[30, 30, 30, 30, 1], k=8 << 20))
    data = bases + bases[-32768:] + bases
    source = root / "dynamic.gz"
    source.write_bytes(gzip.compress(data))
    for backend, kib in (("rapid", 64), ("rapid", 256), ("rapid", 1024), ("zlib", 1024)):
        out = root / f"{backend}{kib}.gz"
        run = subprocess.run([binary, source, out, "64"], check=True, capture_output=True,
                             env=dict(os.environ, SWQC_GZIP_READER=backend,
                                      SWQC_GZIP_READ_CHUNK_KIB=str(kib)), text=True)
        assert gzip.decompress(out.read_bytes()) == data
        if backend == "rapid":
            assert "fallback_bytes=0" in run.stdout, run.stdout
        print(run.stdout.strip())
    # A stored block may start at bit zero of a speculative input interval.
    # Looking ten bits behind its byte-aligned LEN field would underflow there.
    data = bytes(rng.randrange(256) for _ in range(65531 * 8))
    blocks = b"".join(bytes([i == 7]) + b"\xfb\xff\x04\x00" + data[i*65531:(i+1)*65531]
                      for i in range(8))
    source.write_bytes(b"\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\x03" + blocks +
                       zlib.crc32(data).to_bytes(4, "little") + len(data).to_bytes(4, "little"))
    out = root / "aligned-stored.gz"
    run = subprocess.run([binary, source, out, "64"], check=True, capture_output=True,
                         env=dict(os.environ, SWQC_GZIP_READER="rapid", SWQC_GZIP_READ_CHUNK_KIB="64"), text=True)
    assert gzip.decompress(out.read_bytes()) == data
    assert "fallback_bytes=0" in run.stdout, run.stdout
    assert int(re.search(r"read_jobs=(\d+)", run.stdout)[1]) <= 20, run.stdout
    print("ALIGNED_STORED_PASS", run.stdout.strip())
    plain = gzip.compress(b"header with optional fields" * 1000)
    header = bytearray(plain[:10])
    header[3] = 2 | 4 | 8 | 16
    header += b"\x03\x00abcname\x00comment\x00"
    header += (zlib.crc32(header) & 65535).to_bytes(2, "little")
    optional = bytes(header) + plain[10:]
    source.write_bytes(optional + b"\x00" * 32)
    out = root / "header.gz"
    subprocess.run([binary, source, out, "64"], check=True)
    assert gzip.decompress(out.read_bytes()) == gzip.decompress(plain)
    invalid_distance = plain[:10] + b"\x03\x7e" + b"\x00" * 8
    for i, invalid in enumerate((plain[:-1], plain[:13], optional[:20], plain + b"garbage", invalid_distance)):
        source.write_bytes(invalid)
        out = root / f"bad{i}.gz"
        assert subprocess.run([binary, source, out, "64"], capture_output=True).returncode
        assert not out.exists()
print("FIRSTPASS_TEST_PASS: dynamic markers without fallback, serial baseline, headers, truncation")

# ERR-like 75 bp reads: diverse bases but long uniform-quality runs. The
# compression ratio changes after an ordinary member, and again within one.
with tempfile.TemporaryDirectory(prefix="swqc-gzip-capacity-") as directory:
    root = pathlib.Path(directory)
    dna = bytes(rng.choices(b"ACGT", k=75 * 30000))
    # Three adjacent copies raise the ratio without a single giant A-run.
    reads = b"".join(b"@ERR.%d\n" % i + dna[(i//3)*75:(i//3+1)*75] +
                     b"\n+\n" + b"?" * 75 + b"\n" for i in range(90000))
    ordinary = bytes(rng.randrange(256) for _ in range(1 << 20))
    assert len(reads) / len(gzip.compress(reads)) > 8
    for name, compressed, expected in (
        ("err", gzip.compress(reads), reads),
        ("members", gzip.compress(ordinary) + gzip.compress(reads), ordinary + reads),
        ("changing", gzip.compress(ordinary + reads), ordinary + reads),
        ("extreme", gzip.compress(b"A" * (40 << 20)), b"A" * (40 << 20)),
    ):
        source = root / f"{name}.gz"
        source.write_bytes(compressed)
        for memory in (False, True):
            env = dict(os.environ, SWQC_GZIP_READER="rapid",
                       SWQC_GZIP_READ_CHUNK_KIB="64", SWQC_GZIP_SYMBOL_BUDGET_MIB="512")
            env.pop("SWQC_TEST_MEMORY", None)
            if memory:
                env["SWQC_TEST_MEMORY"] = "1"
            out = root / f"{name}.{memory}.gz"
            run = subprocess.run([binary, source, out, "384"], check=True,
                                 capture_output=True, env=env, text=True)
            assert gzip.decompress(out.read_bytes()) == expected
            assert "fallback_bytes=0" in run.stdout, run.stdout
            assert int(re.search(r"capacity_retries=(\d+)", run.stdout)[1]) > 0, run.stdout
            assert int(re.search(r"peak_symbol_bytes=(\d+)", run.stdout)[1]) <= 512 << 20
            print(name, "memory" if memory else "file", run.stdout.strip())
        damaged = bytearray(compressed)
        damaged[-8] ^= 1
        source.write_bytes(damaged)
        out = root / f"{name}.corrupt.gz"
        assert subprocess.run([binary, source, out, "384"], env=env,
                              capture_output=True).returncode
        assert not out.exists()
print("CAPACITY_TEST_PASS: high ratios, changing ratios, members, memory/file, bounded buffers, CRC")
