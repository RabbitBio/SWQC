#!/usr/bin/env python3
"""Native full-byte compatibility and corruption tests (not a Sunway launcher)."""
import gzip
import pathlib
import random
import subprocess
import sys
import tempfile
import zlib

TOOL = str(pathlib.Path(sys.argv[1]).resolve())
SERIAL_GZREAD = '--serial-gzread' in sys.argv[2:]
def run(mode, src, dst, ok=True, *args):
    p = subprocess.run([TOOL, mode, str(src), str(dst), *args],
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    if (p.returncode == 0) != ok:
        raise AssertionError((mode, src.name, p.returncode, p.stdout, p.stderr))
    if not ok and dst.exists():
        raise AssertionError('failed codec left partial output')
    return p.stdout

def encoded(raw, level=6, strategy=zlib.Z_DEFAULT_STRATEGY):
    c = zlib.compressobj(level, zlib.DEFLATED, 31, 8, strategy)
    return c.compress(raw) + c.flush()

with tempfile.TemporaryDirectory(prefix='swqc-gzip-') as tmp:
    root = pathlib.Path(tmp)
    rng = random.Random(1632)
    # Exercise dictionaries across independently compressed CPE chunks.
    block = bytes(rng.randrange(256) for _ in range(32768))
    mixed = block * 80 + bytes(rng.randrange(256) for _ in range(200000))
    cases = {'empty': b'', 'text': b'ACGTN\n'*11000, 'binary': mixed,
             'exact_chunk': b'xyz_'*(1048576//4), 'repeat': b'A'*3500000}
    for name, raw in cases.items():
        src, comp, dest = root/(name+'.raw'), root/(name+'.gz'), root/(name+'.out')
        src.write_bytes(raw)
        run('c', src, comp)
        assert gzip.decompress(comp.read_bytes()) == raw, name
        run('d', comp, dest)
        assert dest.read_bytes() == raw, name
        print('ROUNDTRIP_PASS', name, len(raw))
    for name, blob in [('dynamic', encoded(mixed)), ('fixed', encoded(mixed, strategy=zlib.Z_FIXED)),
                       ('stored', encoded(mixed, 0)),
                       ('members', encoded(b'')+encoded(mixed[:90000])+encoded(b'')+encoded(mixed[90000:])),
                       ('padding', encoded(mixed)+b'\0'*100)]:
        src, dest = root/(name+'.gz'), root/(name+'.out')
        src.write_bytes(blob)
        run('d', src, dest)
        assert dest.read_bytes() == mixed, name
        print('GENERAL_GZIP_PASS', name)
    # FNAME, FCOMMENT, FEXTRA and FHCRC, followed by an ordinary raw stream.
    header = bytearray(b'\x1f\x8b\x08\x1e\0\0\0\0\0\x03')
    header += b'\x04\0abcdname.fastq\0comment\0'
    header += (zlib.crc32(header)&65535).to_bytes(2, 'little')
    normal = encoded(mixed)
    src, dest = root/'header.gz', root/'header.out'
    src.write_bytes(header + normal[10:])
    run('d', src, dest)
    assert dest.read_bytes() == mixed
    print('GENERAL_GZIP_PASS', 'optional_headers')
    for name, blob in [('truncated', normal[:-4]), ('badcrc', normal[:-8]+bytes([normal[-8]^1])+normal[-7:]),
                       ('junk', normal+b'bad'), ('empty_file', b''),
                       ('bad_header_crc', header[:-1]+bytes([header[-1]^1])+normal[10:])]:
        src, dest = root/(name+'.gz'), root/(name+'.out')
        src.write_bytes(blob)
        # Preserve the actual system gzread baseline semantics: it accepts
        # empty input and ignores non-gzip bytes following a valid member.
        tolerated = SERIAL_GZREAD and name in ('junk', 'empty_file')
        run('d', src, dest, tolerated)
        if tolerated:
            assert dest.read_bytes() == (mixed if name == 'junk' else b'')
            print('GZREAD_TOLERANCE_CONFIRMED', name)
        else:
            print('CORRUPTION_REJECTED', name)
    existing = root/'preserve.out'
    existing.write_bytes(b'keep')
    p = subprocess.run([TOOL,'d',str(root/'dynamic.gz'),str(existing)],
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    assert p.returncode and existing.read_bytes() == b'keep'
    print('EXISTING_OUTPUT_PRESERVED')
print('CODEC_TESTS_PASS')
