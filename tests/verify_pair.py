#!/usr/bin/env python3
"""Read-only streaming FASTQ checks; run large checks on compute nodes."""
import argparse
import hashlib
import json
import os
import sys


def inspect(path):
    md5 = hashlib.md5()
    size = lines = 0
    last = b""
    with open(path, "rb") as stream:
        while True:
            block = stream.read(8 << 20)
            if not block:
                break
            md5.update(block)
            size += len(block)
            lines += block.count(b"\n")
            last = block[-1:]
    if size and (last != b"\n" or lines % 4):
        raise ValueError("incomplete four-line FASTQ: " + path)
    return {"path": path, "bytes": size, "lines": lines,
            "records": lines // 4, "md5": md5.hexdigest()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("r1")
    parser.add_argument("r2")
    parser.add_argument("--expect", nargs=2)
    parser.add_argument("--remove-on-pass", action="store_true")
    args = parser.parse_args()
    if args.remove_on_pass and not args.expect:
        parser.error("deletion requires two expected MD5 values")
    values = [inspect(args.r1), inspect(args.r2)]
    valid = values[0]["records"] == values[1]["records"]
    if args.expect:
        valid = valid and [v["md5"] for v in values] == args.expect
    result = {"mates": values, "reference_checked": bool(args.expect),
              "result": "PASS" if valid and args.expect else "MEASURED" if valid else "FAIL"}
    print(json.dumps(result, sort_keys=True))
    sys.stdout.flush()
    if not valid:
        return 1
    if args.remove_on_pass:
        for path in (args.r1, args.r2):
            os.unlink(path)
        print("Removed only the two verified output files.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
