#!/usr/bin/env python3
# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Run the managed native daemon fixture in separate constrained Linux voters."""

import argparse
import json
import os
from pathlib import Path
import re
import secrets
import shlex
import subprocess
import sys
import time

from backend_control import atomic_json
from cell_groups import backend
from store_probe import probe

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "tools/perf"))
from raft_run import input_digest, parse_manifest, require, source_manifest

PROFILE = "cellar-multi-raft"
CONTEXT = "colima-" + PROFILE
LABEL = "io.cellar.daemon.run"
BIN = "/usr/local/bin"
NAMES = ("cellar-raft-daemon", "cellar")


def docker(*args):
    return subprocess.check_output(["docker", "--context", CONTEXT, *args], text=True).strip()


def guest(script):
    return subprocess.run(["colima", "ssh", "--profile", PROFILE, "--", "sh", "-s"],
                          input=script, text=True, stdout=subprocess.PIPE,
                          stderr=subprocess.STDOUT, check=False)


def verify_image(image):
    require(re.fullmatch(r"sha256:[0-9a-f]{64}", image), "immutable Docker image ID required")
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    require(docker("image", "inspect", "--format",
                   '{{index .Config.Labels "org.opencontainers.image.revision"}}', image) == revision,
            "image source revision differs")
    isolated = ("run", "--rm", "--network", "none", "--entrypoint")
    manifest = parse_manifest(docker(*isolated, "cat", image, f"{BIN}/raft-source.sha256"))
    require(manifest == source_manifest(), "image source inputs differ")
    binaries = {name: docker(*isolated, "sha256sum", image, f"{BIN}/{name}").split()[0] for name in NAMES}
    require(all(re.fullmatch(r"[0-9a-f]{64}", digest) for digest in binaries.values()), "invalid executable digest")
    return {"image": image, "source_revision": revision, "source_manifest_sha256": input_digest(manifest),
            "binaries": binaries}


def inventory(run, image):
    identifiers = docker("ps", "-aq", "--no-trunc", "--filter", f"label={LABEL}={run}").splitlines()
    rows = []
    for identifier in identifiers:
        row = json.loads(docker("inspect", "--type", "container", "--format",
                                '{"id":{{json .Id}},"image":{{json .Image}},'
                                '"run":{{json (index .Config.Labels "io.cellar.daemon.run")}},'
                                '"running":{{json .State.Running}},"exit_code":{{json .State.ExitCode}}}',
                                identifier))
        require(row["id"] == identifier and row["image"] == image and row["run"] == run,
                "foreign voter in lab inventory")
        rows.append(row)
    return rows


def execute(mode, output, remote, credentials, build, engine):
    run = secrets.token_hex(8)
    directory = f"/tmp/cellar-daemon-{run}"
    result = {"mode": mode, "run": run, "guest_directory": directory, "status": "RUNNING"}
    report = output / f"{mode}.json"
    atomic_json(report, result)
    start = time.monotonic()
    bucket = f"{remote['owner']}-cli-{run[:8]}"
    access = credentials["RUSTFS_ACCESS_KEY"]
    secret = credentials["RUSTFS_SECRET_KEY"]
    image = build["image"]
    env = {
        "AWS_ACCESS_KEY_ID": access, "AWS_SECRET_ACCESS_KEY": secret, "AWS_EC2_METADATA_DISABLED": "true",
        "CELLAR_CELL_GROUPS_RUSTFS_ENDPOINT": remote["endpoint"], "CELLAR_CELL_GROUPS_RUSTFS_BUCKET": bucket,
        "CELLAR_CELL_GROUPS_RUSTFS_PREFIX": f"managed-linux-daemon/{run}",
        "CELLAR_RAFT_PROCESS_IMAGE_MODE": mode, "CELLAR_RAFT_PROCESS_EVIDENCE": f"{directory}/process.jsonl",
        "CELLAR_RAFT_DOCKER_RUN": run, "CELLAR_RAFT_DOCKER_IMAGE": image,
        "CELLAR_RAFT_OPERATOR_CLI": f"{directory}/cellar", "TMPDIR": directory,
    }
    quoted = shlex.quote(directory)
    try:
        result["signed_s3"] = probe(remote["endpoint"], bucket, access, secret)
        lines = ["set -eu", "umask 077", "unset DOCKER_CONTEXT DOCKER_HOST",
                 f"test \"$(docker info --format '{{{{.ID}}}}')\" = {shlex.quote(engine)}",
                 f"mkdir -m 700 {quoted}",
                 f'helper=$(docker create --network none --label io.cellar.daemon.artifact={run} '
                 f'--entrypoint /bin/true {shlex.quote(image)})']
        for name, digest in build["binaries"].items():
            path = f"{directory}/{name}"
            lines += [f'docker cp "$helper:{BIN}/{name}" {shlex.quote(path)}',
                      f"printf '%s  %s\\n' {digest} {shlex.quote(path)} | sha256sum --check -"]
        lines += [f"export {key}={shlex.quote(value)}" for key, value in env.items()]
        lines += [f"exec {quoted}/cellar-raft-daemon"]
        completed = guest("\n".join(lines) + "\n")
        log = completed.stdout.replace(access, "[REDACTED]").replace(secret, "[REDACTED]")
        (output / f"{mode}.log").write_text(log)
        rows = inventory(run, image)
        result["voters"] = rows
        require(completed.returncode == 0 and "production Raft CLI qualification passed" in log,
                f"Linux daemon fixture failed in {mode}")
        require(len(rows) == 4 and all(not row["running"] for row in rows)
                and sorted(row["exit_code"] for row in rows) == [0, 0, 0, 137], "voter shutdown inventory differs")
        ledger = guest(f"cat {quoted}/process.jsonl\n")
        require(ledger.returncode == 0, "process ledger missing")
        events = [json.loads(line) for line in ledger.stdout.splitlines()]
        require(events[0]["store"] == "rustfs_catalog_and_archives"
                and sum(event["phase"] == "post_resume_write_verified" for event in events) == 2,
                "missing RustFS retained restart proof")
        (output / f"{mode}.jsonl").write_text(ledger.stdout)
        observations = guest(f'''find {quoted} -maxdepth 2 -name '*-docker.json' -type f -exec sh -c '\nfor file do tr -d "\\n" < "$file"; printf "\\n"; done' sh {{}} +\n''')
        require(observations.returncode == 0, "could not capture cgroup observations")
        proof = [json.loads(line) for line in observations.stdout.splitlines()]
        require(len(proof) == 5 and {row["id"] for row in proof if row["running"]} == {row["id"] for row in rows}
                and sum(row["exit_code"] == 137 and not row["running"] for row in proof) == 1,
                "missing live cgroup or SIGKILL observation")
        result.update(status="PASSED", resources=proof)
    except Exception:
        result["status"] = "FAILED"
        for row in inventory(run, image):
            if row["running"]:
                docker("kill", "--signal", "KILL", row["id"])
        raise
    finally:
        result["elapsed_seconds"] = round(time.monotonic() - start, 3)
        atomic_json(report, result)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", required=True)
    parser.add_argument("--backend", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    remote, credentials = backend(args.backend.resolve())
    require(remote["context"] == CONTEXT, "requires the dedicated daemon lab context")
    engine, os_type, cpus, memory = docker("info", "--format", "{{.ID}} {{.OSType}} {{.NCPU}} {{.MemTotal}}").split()
    require(os_type == "linux" and int(cpus) >= 6 and int(memory) >= 12 * 1024**3, "insufficient Linux VM resources")
    require(not docker("ps", "-q", "--filter", f"label={LABEL}"), "another daemon campaign is active")
    build = verify_image(args.image)
    output = args.output.resolve()
    output.mkdir(mode=0o700, parents=True, exist_ok=False)
    report = {"status": "RUNNING", "build": build, "backend": remote["observed_rustfs"],
              "scope": "three Linux voters with separate cgroups and disks in one VM"}
    atomic_json(output / "record.json", report)
    try:
        for mode in ("materialized", "extents"):
            execute(mode, output, remote, credentials, build, engine)
        report["status"] = "PASSED"
    except Exception:
        report["status"] = "FAILED"
        raise
    finally:
        atomic_json(output / "record.json", report)


if __name__ == "__main__":
    os.umask(0o077)
    main()
