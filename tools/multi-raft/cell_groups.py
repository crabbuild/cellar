#!/usr/bin/env python3
# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Qualify shared-group archive setup and local command responses against local RustFS.

Uses only the recorded owned backend. Each invocation owns a new evidence
directory and remote prefix. Credentials are passed through the child environment,
never arguments or evidence. Positive command acknowledgements require the explicit controller binding.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import subprocess
import time

from local import command
from store_probe import probe

ROOT = Path(__file__).resolve().parents[2]
TARGET = "native_cell_groups_archive_controller"
CASES = (
    "rustfs_controller_archive_publishes_stable_cuts",
    "rustfs_controller_archive_reconciles_exact_intent_and_joins_close",
    "rustfs_unproved_s3_responses_refuse",
    "rustfs_s3_commands_require_exact_archived_results",
    "rustfs_s3_command_deadlines_do_not_quarantine_committed_sql",
    "rustfs_s3_command_requires_the_sibling_archive_dependencies",
)


def sha(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sources():
    result = subprocess.check_output(["git", "ls-files", "-z"], cwd=ROOT)
    return [
        {"path": name, "sha256": sha(ROOT / name)}
        for name in result.decode().split("\0")
        if name and not name.startswith(("docs/", "plans/", "packages/", "examples/"))
        and (ROOT / name).is_file()
    ]


def backend(root):
    record = json.loads((root / "environment.json").read_text())
    if (record["phase"] != "backend-ready"
            or not re.fullmatch(r"cellar-multi-raft-[0-9a-f]{16}", record["owner"])
            or not re.fullmatch(r"colima-cellar-multi-raft(?:-[a-z0-9-]+)?", record["context"])
            or not re.fullmatch(r"http://127\.0\.0\.1:[0-9]+", record["endpoint"])):
        raise ValueError("requires an owned, running loopback RustFS backend")
    docker = ["docker", "--context", record["context"]]
    actual = json.loads(command(docker + [
        "inspect", "--format",
        '{"owner":{{json (index .Config.Labels "cellar.multi-raft.run")}},'
        '"image":{{json .Image}},"running":{{json .State.Running}},'
        '"limits":{{json .HostConfig}},"ports":{{json .NetworkSettings.Ports}}}',
        record["container"],
    ]))
    limits = actual["limits"]
    if (actual["owner"] != record["owner"] or actual["image"] != record["image_id"]
            or not actual["running"]
            or (limits["NanoCpus"], limits["Memory"], limits["MemorySwap"])
            != (10**9, 2 * 1024**3, 2 * 1024**3)):
        raise ValueError("backend identity/running state/resource limits changed")
    ports = actual["ports"]["9000/tcp"]
    if ports != [{"HostIp": "127.0.0.1", "HostPort": record["endpoint"].rsplit(":", 1)[1]}]:
        raise ValueError("backend endpoint differs from the captured container")
    credentials = dict(line.split("=", 1) for line in (root / "rustfs.env").read_text().splitlines())
    record["observed_rustfs"] = {
        "owner": actual["owner"], "image": actual["image"],
        "nano_cpus": limits["NanoCpus"], "memory_bytes": limits["Memory"],
        "memory_swap_bytes": limits["MemorySwap"], "ports": ports,
    }
    return record, credentials


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--target-directory", type=Path, required=True,
                        help="absolute checkout-private Cargo target on the workspace volume")
    args = parser.parse_args()
    if not args.target_directory.is_absolute():
        parser.error("--target-directory must be absolute and unique to this checkout")
    subprocess.run(["git", "diff", "--exit-code", "HEAD", "--", ".",
                    ":(exclude)docs/**", ":(exclude)plans/**"], cwd=ROOT, check=True,
                   stdout=subprocess.DEVNULL)
    untracked = subprocess.check_output(["git", "ls-files", "--others", "--exclude-standard", "-z"], cwd=ROOT)
    if any(name.startswith(("crates/", "tools/")) for name in untracked.decode().split("\0")):
        parser.error("commit qualification sources before running")
    remote, credentials = backend(args.backend.resolve())
    out = args.output.resolve()
    out.mkdir(mode=0o700, parents=True, exist_ok=False)
    prefix = "cell-groups/" + secrets.token_hex(16)
    record = {
        "format": 1, "status": "RUNNING", "scope": "shared-group RustFS archive/setup and local command S3 settlement",
        "source_revision": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "source_inputs": sources(), "backend": remote, "prefix": prefix, "commands": [],
        "limitations": "Host-native three-voter TLS fixture, both image forms; RustFS runs in one local VM. "
                        "No S3 receipt-lookup/enrollment or remote command acknowledgements, byte/disk outage bound, "
                        "Raft log file purge, process-kill restart, fencing/DR, performance or independent hosts.",
    }

    def save():
        (out / "record.json").write_text(json.dumps(record, indent=2) + "\n")

    save()
    try:
        bucket = remote["owner"] + "-cg-" + secrets.token_hex(4)
        record["s3_probe"] = probe(remote["endpoint"], bucket,
                                   credentials["RUSTFS_ACCESS_KEY"], credentials["RUSTFS_SECRET_KEY"])
        record["bucket"] = bucket
        env = dict(os.environ)
        # Avoid inheriting session credentials or arbitrary AWS endpoint overrides.
        for key in list(env):
            if key.startswith(("AWS_", "CELLAR_CELL_GROUPS_RUSTFS_")):
                del env[key]
        env.update(
            AWS_ACCESS_KEY_ID=credentials["RUSTFS_ACCESS_KEY"],
            AWS_SECRET_ACCESS_KEY=credentials["RUSTFS_SECRET_KEY"],
            AWS_EC2_METADATA_DISABLED="true",
            CELLAR_CELL_GROUPS_RUSTFS_ENDPOINT=remote["endpoint"],
            CELLAR_CELL_GROUPS_RUSTFS_BUCKET=bucket,
            CELLAR_CELL_GROUPS_RUSTFS_PREFIX=prefix,
            CARGO_TARGET_DIR=str(args.target_directory),
            CARGO_PROFILE_DEV_DEBUG="0", CARGO_PROFILE_TEST_DEBUG="0",
        )
        cargo = ["cargo", "+1.98.1", "test", "-p", "cellar", "--locked", "--test", TARGET, "--"]
        for number, selection in enumerate(("--list", *CASES)):
            argv = cargo + ([selection] if number == 0 else [selection, "--exact", "--ignored", "--test-threads=1"])
            print(f"START {number + 1}/{len(CASES) + 1}: {selection}", flush=True)
            started = time.monotonic()
            result = subprocess.run(argv, cwd=ROOT, env=env, text=True,
                                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            output = result.stdout
            for secret in credentials.values():
                output = output.replace(secret, "[REDACTED]")
            log = out / f"{number:02}.log"
            log.write_text(output)
            record["commands"].append({"argv": argv, "exit_code": result.returncode,
                                       "elapsed_seconds": round(time.monotonic() - started, 3),
                                       "output": log.name, "sha256": sha(log)})
            save()
            if result.returncode:
                raise RuntimeError(f"{selection} failed; see {log}")
            if number == 0:
                for case in CASES:
                    if not re.search(rf"^{re.escape(case)}: test$", output, re.M):
                        raise RuntimeError("missing exact RustFS test in executable inventory")
            elif not (re.search(rf"^test {re.escape(selection)} \.\.\. ok$", output, re.M)
                      and "test result: ok. 1 passed; 0 failed; 0 ignored;" in output):
                raise RuntimeError("exact RustFS test was not executed successfully")
            match = re.search(r"Running [^\n]*\(([^\n)]+/native_cell_groups_archive_controller-[a-f0-9]+)\)", output)
            if not match:
                raise RuntimeError("missing executable provenance")
            executable = Path(match[1])
            selected_hash = sha(executable)
            if "executable" in record and record["executable"]["sha256"] != selected_hash:
                raise RuntimeError("executable changed during qualification")
            destination = out / executable.name
            shutil.copy2(executable, destination)
            record["executable"] = {"path": destination.name, "sha256": selected_hash}
            print(f"PASS {selection}", flush=True)
        if sources() != record["source_inputs"]:
            raise RuntimeError("source changed during qualification")
        backend(args.backend.resolve())
        record["status"] = "PASSED"
        record["passed_cases"] = list(CASES)
    except Exception:
        record["status"] = "FAILED"
        raise
    finally:
        save()
    print(json.dumps({"record": str(out / "record.json"), "status": record["status"],
                      "rustfs_cases": len(CASES), "image_modes_per_case": 2}), flush=True)


if __name__ == "__main__":
    main()
