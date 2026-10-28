#!/usr/bin/env python3
# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Compare bounded nft cache admission without committing firewall changes."""

import argparse
import json
from pathlib import Path
import re
import subprocess
import uuid


def probe(context, image, rules):
    task = "cellar-fence-probe-" + uuid.uuid4().hex
    table = "cellar_" + uuid.uuid4().hex[:24]
    rules = rules.replace("__TABLE__", table)
    rules = rules.replace("__OPEN__", "cl9deaprobe0")
    rules = rules.replace("__INTERNAL__", "cl9deaprobe1")
    docker = ["docker", "--context", context]
    command = [
        *docker, "run", "--name", task,
        "--network", "host", "--cap-add", "NET_ADMIN",
        "--memory", "64m", "--memory-swap", "64m", "--cpus", "0.0625",
        "--label", "cellar.task=" + task,
        "--env", "CELLAR_NFT=" + rules, image,
        "sh", "-c",
        'printf "%s" "$CELLAR_NFT" | nft --check -f -; status=$?; '
        'printf "memory_peak="; cat /sys/fs/cgroup/memory.peak; exit "$status"',
    ]
    try:
        completed = subprocess.run(
            command, text=True, capture_output=True, timeout=60, check=False
        )
        inspected = subprocess.run(
            [*docker, "inspect", task], text=True, capture_output=True, check=True
        )
        state = json.loads(inspected.stdout)[0]["State"]
        peak = re.search(r"memory_peak=(\d+)", completed.stdout)
        return {
            "exit": completed.returncode,
            "oom_killed": state["OOMKilled"],
            "peak_bytes": int(peak[1]) if peak else None,
            "error": completed.stderr[:500],
        }
    finally:
        inspected = subprocess.run(
            [*docker, "inspect", task], text=True, capture_output=True, check=False
        )
        if inspected.returncode == 0:
            container = json.loads(inspected.stdout)[0]
            if container["Config"]["Labels"].get("cellar.task") != task:
                raise RuntimeError("probe cleanup ownership mismatch")
            subprocess.run(
                [*docker, "rm", "--force", container["Id"]],
                text=True, capture_output=True, check=True,
            )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--context", required=True)
    parser.add_argument("--image", required=True, help="immutable local fence image ID")
    parser.add_argument("--require-oom-control", action="store_true")
    args = parser.parse_args()
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", args.image):
        parser.error("image must be an immutable local SHA-256 image ID")
    root = Path(__file__).resolve().parents[2]
    source = (root / "crates/cellar/containers/mod.rs").read_text()
    match = re.search(r'const FENCE_RULES: &str = r#"(.*?)"#;', source, re.S)
    if match is None:
        raise RuntimeError("production fence rules not found")
    rules = match[1]
    terminal = "list table inet __TABLE__\n"
    if not rules.endswith(terminal) or rules.count(terminal) != 1:
        raise RuntimeError("production own-table observation changed")
    if not re.search(r"FENCE_MEMORY_BYTES: u64 = 64 \* 1024 \* 1024;", source):
        raise RuntimeError("production fence memory bound changed")
    results = {
        "global_cache_control": probe(args.context, args.image, rules[:-len(terminal)]),
        "production": probe(args.context, args.image, rules),
    }
    print(json.dumps(results, indent=2))
    current = results["production"]
    if current["exit"] != 0 or current["oom_killed"]:
        raise SystemExit("production fence check failed")
    if args.require_oom_control and not results["global_cache_control"]["oom_killed"]:
        raise SystemExit("global-cache OOM control did not reproduce")


if __name__ == "__main__":
    main()
