#!/usr/bin/env python3
"""Capture or verify an immutable, provisioned CPython filesystem inventory.

All installation happens before capture. Run with a trusted build interpreter;
no package manager, application module or site hook runs during verification.
"""
import argparse
import hashlib
import gzip
import io
import tarfile
import json
import os
from pathlib import Path
import subprocess
import sys
import native_prerequisites

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
            result[str(path)] = {"kind": "file", "bytes": after.st_size, "sha256": value, "executable": bool(after.st_mode & 0o111)}
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
    inventory["prerequisites"] = native_prerequisites.capture(inventory, args.native_host, args.native_library)
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
    native_prerequisites.verify(inventory)
    print(f"Verified {len(actual)} provisioned files")


def bundle(args):
    raw = args.inventory.read_bytes()
    inventory = json.loads(raw)
    prefix = args.prefix.resolve(strict=True)
    roots = [Path(root) for root in inventory["roots"]]
    if prefix == Path(prefix.anchor) or not all(root.is_relative_to(prefix) for root in roots):
        raise ValueError("Bundle roots must be within one explicit non-root prefix")
    if any(args.output.resolve().is_relative_to(root) for root in roots):
        raise ValueError("Archive output must be outside captured import roots")
    if files(roots) != inventory["files"]:
        raise ValueError("Provisioned files differ from inventory")
    native_prerequisites.verify(inventory)
    metadata = json.dumps(dict(format=1, prefix=str(prefix), inventory_sha256=hashlib.sha256(raw).hexdigest()), sort_keys=True).encode()
    with args.output.open("xb") as destination:
        with gzip.GzipFile(filename="", mode="wb", fileobj=destination, mtime=0) as compressed:
            with tarfile.open(fileobj=compressed, mode="w|", format=tarfile.USTAR_FORMAT) as archive:
                def add(name, size, source):
                    info = tarfile.TarInfo(name)
                    info.size, info.mode, info.mtime = size, 0o444, 0
                    archive.addfile(info, source)
                add("environment.json", len(metadata), io.BytesIO(metadata))
                add("inventory.json", len(raw), io.BytesIO(raw))
                seen = set()
                for path, entry in sorted(inventory["files"].items()):
                    if entry["kind"] == "file" and entry["sha256"] not in seen:
                        seen.add(entry["sha256"])
                        with open(path, "rb") as stream:
                            add("blobs/" + entry["sha256"], entry["bytes"], stream)
    print("Environment archive SHA-256 " + digest(args.output))


def project(args):
    source = args.project.resolve(strict=True)
    config = json.loads(source.read_text())
    if config.get("cohosted") or config["primary"].get("assets") or config["primary"].get("containers"):
        raise ValueError("Prepare this example's asset/container/cohost paths explicitly before bundling")
    config["cargo_manifest"] = str((source.parent / config["cargo_manifest"]).resolve(strict=True))
    if args.version is not None:
        config["primary"]["application_version"] = args.version
    config["python_environment"] = dict(inventory=str(args.inventory.resolve(strict=True)), archive=str(args.archive.resolve(strict=True)))
    args.output.write_text(json.dumps(config, indent=2) + "\n")
    print("Prepared native project " + str(args.output))


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
    command.add_argument("--native-host", type=Path, required=True)
    command.add_argument("--native-library", type=Path, action="append", default=[])
    command = commands.add_parser("verify")
    command.add_argument("inventory", type=Path)
    command = commands.add_parser("bundle")
    command.add_argument("inventory", type=Path)
    command.add_argument("--prefix", type=Path, required=True)
    command.add_argument("--output", type=Path, required=True)
    command = commands.add_parser("project")
    command.add_argument("project", type=Path)
    command.add_argument("--inventory", type=Path, required=True)
    command.add_argument("--archive", type=Path, required=True)
    command.add_argument("--version")
    command.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    {"capture": capture, "verify": verify, "bundle": bundle, "project": project}[args.command](args)


if __name__ == "__main__":
    main()
