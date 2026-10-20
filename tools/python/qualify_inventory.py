#!/usr/bin/env python3
"""Exercise a verified host against mutations in a disposable provisioned tree.

Only use a private scratch environment. Each mutation is restored in finally;
no running host or production environment may share these roots.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", type=Path, required=True)
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--scratch-prefix", type=Path, required=True)
    args = parser.parse_args()
    inventory = json.loads(args.inventory.read_text())
    scratch = args.scratch_prefix.resolve(strict=True)
    assert scratch != Path(scratch.anchor), "scratch prefix cannot be a filesystem root"
    roots = [Path(root) for root in inventory["roots"]]
    assert roots and all(root.is_relative_to(scratch) for root in roots)
    environment = dict(os.environ, CELLAR_PYTHON_SITE_PACKAGES=os.pathsep.join(inventory["module_paths"]))

    def run(expected_success):
        result = subprocess.run([str(args.host.resolve(strict=True)), "--help"], env=environment,
                                capture_output=True, timeout=15)
        if expected_success:
            assert result.returncode == 0, result.stderr.decode(errors="replace")
        else:
            assert result.returncode != 0, "corrupted environment admitted host"
            assert b"Python provisioned files differ" in result.stderr or b"No such file" in result.stderr, result.stderr
            assert b"UNAPPROVED-IMPORT-EXECUTED" not in result.stderr

    run(True)
    source = next(Path(path) for path, entry in inventory["files"].items()
                  if entry["kind"] == "file" and path.endswith("/numpy/__init__.py"))
    original = source.read_bytes()
    try:
        source.write_bytes(b'raise RuntimeError("UNAPPROVED-IMPORT-EXECUTED")\n')
        run(False)
    finally:
        source.write_bytes(original)
    extension = next(Path(path) for path, entry in inventory["files"].items()
                     if entry["kind"] == "file" and "pydantic_core" in path and path.endswith((".so", ".pyd")))
    original = extension.read_bytes()
    try:
        extension.write_bytes(original[:-1] + bytes([original[-1] ^ 1]))
        run(False)
    finally:
        extension.write_bytes(original)
    hidden = source.with_name(".cellar-qualification-missing")
    assert not hidden.exists()
    try:
        source.rename(hidden)
        run(False)
    finally:
        if hidden.exists():
            hidden.rename(source)
    added = Path(inventory["module_paths"][0]) / "unlisted_cellar_code.py"
    assert not added.exists()
    try:
        added.write_text('raise RuntimeError("UNAPPROVED-IMPORT-EXECUTED")\n')
        run(False)
    finally:
        added.unlink(missing_ok=True)
    run(True)
    print("PASS verified inventory: valid runtime, changed source/extension, missing and unlisted files, restored runtime")


if __name__ == "__main__":
    main()
