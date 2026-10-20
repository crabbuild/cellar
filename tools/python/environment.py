#!/usr/bin/env python3
"""Capture or verify an immutable, provisioned CPython filesystem inventory.

All installation happens before capture. Run with a trusted build interpreter;
no package manager, application module or site hook runs during verification.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

MAX_FILES = 200_000


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def files(roots):
    result = {}

    def walk(path):
        if len(result) >= MAX_FILES:
            raise ValueError("Python inventory file limit exceeded")
        if path.is_symlink():
            target = path.resolve(strict=True)
            if not any(target.is_relative_to(root) for root in roots):
                raise ValueError("Python symlink escapes inventory closure: " + str(path))
            result[str(path)] = {"kind": "symlink", "target": os.readlink(path)}
        elif path.is_dir():
            for child in sorted(path.iterdir()):
                walk(child)
        elif path.is_file():
            before = path.stat()
            value = digest(path)
            after = path.stat()
            if (before.st_size, before.st_mtime_ns, before.st_ino) != (after.st_size, after.st_mtime_ns, after.st_ino):
                raise ValueError("File changed while hashing: " + str(path))
            result[str(path)] = {"kind": "file", "bytes": after.st_size, "sha256": value}
        else:
            raise ValueError("Special files are not allowed: " + str(path))

    for root in roots:
        walk(root)
    return result


def capture(args):
    # -I/-S/-B exclude ambient imports/site hooks and prevent bytecode changes.
    probe = r'''
import json, os, sys
assert sys.implementation.name == "cpython" and sys.version_info[:2] == (3, 14)
assert "t" not in sys.abiflags
json.dump(dict(home=os.path.realpath(sys.base_prefix), executable=os.path.realpath(sys.executable),
 standard_library_paths=[os.path.realpath(p) for p in sys.path if os.path.exists(p)],
 soabi=f"cpython-{sys.version_info.major}{sys.version_info.minor}{sys.abiflags}-{sys.implementation._multiarch}",
 version=".".join(map(str, sys.version_info[:3]))), sys.stdout)
'''
    layout = json.loads(subprocess.check_output([str(args.python.resolve(strict=True)), "-I", "-S", "-B", "-c", probe]))
    modules = [str(path.resolve(strict=True)) for path in args.site_packages]
    roots = sorted(set([layout["home"], *modules]))
    roots = [Path(root) for root in roots if not any(Path(root).is_relative_to(Path(other)) for other in roots if root != other)]
    source = args.source.read_bytes()
    descriptor = json.loads(args.descriptor.read_text())
    inventory = dict(format=1, target=args.target, **layout, module_paths=modules,
                     roots=[str(root) for root in roots],
                     source=dict(sha256=hashlib.sha256(source).hexdigest(), filename=args.filename,
                                 descriptor=descriptor["handlers"], distributions=descriptor["distributions"]),
                     files=files(roots))
    if any(args.output.resolve().is_relative_to(root) for root in roots):
        raise ValueError("Inventory output must be outside the captured roots")
    args.output.write_text(json.dumps(inventory, sort_keys=True, indent=2) + "\n")
    print(f"Captured {len(inventory['files'])} files; inventory SHA-256 {digest(args.output)}")


def verify(args):
    inventory = json.loads(args.inventory.read_text())
    if inventory["format"] != 1:
        raise ValueError("Unsupported inventory format")
    actual = files([Path(root) for root in inventory["roots"]])
    if actual != inventory["files"]:
        raise ValueError("Provisioned files differ from inventory")
    print(f"Verified {len(actual)} provisioned files")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    command = commands.add_parser("capture")
    command.add_argument("--python", type=Path, required=True)
    command.add_argument("--site-packages", type=Path, action="append", default=[])
    command.add_argument("--source", type=Path, required=True)
    command.add_argument("--filename", required=True)
    command.add_argument("--descriptor", type=Path, required=True)
    command.add_argument("--target", required=True)
    command.add_argument("--output", type=Path, required=True)
    command = commands.add_parser("verify")
    command.add_argument("inventory", type=Path)
    args = parser.parse_args()
    {"capture": capture, "verify": verify}[args.command](args)


if __name__ == "__main__":
    main()
