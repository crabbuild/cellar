#!/usr/bin/env python3
# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Run the finite native delivery drill twice against the same retained disks."""

import argparse
import hashlib
import json
from pathlib import Path
import subprocess


def digest(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("directory", type=Path)
    parser.add_argument("ca", type=Path)
    parser.add_argument("certificate", type=Path)
    parser.add_argument("key", type=Path)
    args = parser.parse_args()
    binary = args.binary.resolve(strict=True)
    root = args.directory.resolve()
    root.mkdir(parents=True, exist_ok=True)
    expected = digest(binary)
    command = [str(binary), str(root)] + [
        str(path.resolve(strict=True))
        for path in [args.ca, args.certificate, args.key]
    ]
    for number in [1, 2]:
        if digest(binary) != expected:
            raise RuntimeError("drill executable changed before run")
        log = root / f"run-{number}.log"
        with log.open("wb") as output:
            result = subprocess.run(
                command, stdout=output, stderr=subprocess.STDOUT, timeout=120,
                check=False,
            )
        if result.returncode:
            raise RuntimeError(f"drill failed ({result.returncode}); inspect {log}")
        if digest(binary) != expected:
            raise RuntimeError("drill executable changed during run")
        report = json.loads((root / "result.json").read_text())
        expected_report = {
            "producer_value": 7, "emitted": 1, "settled": 1,
            "receiver_value": 7, "primary_replaced": True,
        }
        if any(report.get(key) != value for key, value in expected_report.items()):
            raise RuntimeError(f"incorrect durable producer/receiver report in {root}")
    print(f"Two producer/receiver runs passed; executable sha256={expected}; logs={root}")


if __name__ == "__main__":
    main()
