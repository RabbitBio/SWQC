#!/usr/bin/env python3
"""Compact a fully relocated executable without changing allocated sections.

Some Sunway launchers fail before main on the larger rapidgzip ELF image.
Retain the original image for symbolization; compact only after xlink/TLS maps
have been generated. No instructions or allocated data are rewritten.
"""
import os
import pathlib
import subprocess
import sys

from xlink import ELF64

source, destination = map(pathlib.Path, sys.argv[1:])
before = ELF64(source.read_bytes())
subprocess.check_call([os.environ.get('OBJCOPY', 'swobjcopy'), '--strip-all',
                       '--keep-symbol=main', str(source), str(destination)])
after = ELF64(destination.read_bytes())
for name, old in before.shdrs.items():
    if not old.sh_flags & 2:  # SHF_ALLOC
        continue
    new = after.shdrs[name]
    if old.sh_addr != new.sh_addr or old.sh_size != new.sh_size:
        raise RuntimeError('allocated section layout changed: ' + name)
    if old.sh_type != 8 and (before.get_section_bin(before.bin, old) !=
                             after.get_section_bin(after.bin, new)):
        raise RuntimeError('allocated section contents changed: ' + name)
print('ELF metadata compacted: {} -> {} sections; allocated bytes unchanged'.format(
    before.shnum, after.shnum))
