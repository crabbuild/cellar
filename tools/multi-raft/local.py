#!/usr/bin/env python3
# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Prepare an isolated Colima/RustFS backend for local multi-Raft qualification.

This is environment preparation, not a passing Cellar qualification run. Node
launch and correctness observations must be recorded separately. Credentials
remain in the private run directory and never enter the public environment file.
"""

import argparse
import json
import os
import pathlib
import re
import secrets
import subprocess
import time
import urllib.error
import urllib.request
from store_probe import probe

GIB = 1024 ** 3
RUSTFS_IMAGE = "ghcr.io/rustfs/rustfs@sha256:bffcab0c9d647aab0055d1c69d340b202d0909966b385932d4ead1aeb7602858"


def command(argv):
    result = subprocess.run(argv, text=True, capture_output=True)
    if result.returncode:
        # Never include credential-bearing argv or a full container inspection.
        raise RuntimeError(f"{argv[0]} {argv[1]} failed: {result.stderr.strip()}")
    return result.stdout.strip()


def required_resources(nodes):
    if type(nodes) is not int or nodes not in (1, 3, 5):
        raise ValueError("choose one, three or five local nodes")
    # Each Cellar node: one CPU / 3 GiB. RustFS: one CPU / 2 GiB.
    # Leave one CPU and at least 512 MiB for the guest and observation work.
    return nodes + 2, (nodes * 3 + 2) * GIB + GIB // 2


def preflight(profile, nodes):
    if not re.fullmatch(r"cellar-multi-raft(?:-[a-z0-9-]+)?", profile):
        raise ValueError("use a separate cellar-multi-raft[-NAME] Colima profile")
    context = "colima-" + profile
    docker = ["docker", "--context", context]
    info = json.loads(command(docker + ["info", "--format", "{{json .}} "]))
    cpus, memory = required_resources(nodes)
    if info["OSType"] != "linux" or not info.get("MemoryLimit") or not info.get("CpuCfsQuota"):
        raise RuntimeError("qualification requires Linux CPU and memory enforcement")
    if info["NCPU"] < cpus or info["MemTotal"] < memory:
        raise RuntimeError(
            f"VM is too small for {nodes} nodes: need {cpus} CPUs and "
            f"{memory / GIB:g} GiB available guest memory; observed "
            f"{info['NCPU']} CPUs and {info['MemTotal'] / GIB:.2f} GiB. "
            "Resize only this dedicated profile after stopping its own run."
        )
    return docker, {
        "profile": profile, "context": context, "vm_cpus": info["NCPU"],
        "vm_memory_bytes": info["MemTotal"], "architecture": info["Architecture"],
        "docker_version": info["ServerVersion"], "kernel": info["KernelVersion"],
        "planned_nodes": nodes, "node_limits": {"cpus": 1, "memory_bytes": 3 * GIB},
        "rustfs_limits": {"cpus": 1, "memory_bytes": 2 * GIB},
    }


def save(root, record):
    temporary = root / "environment.json.tmp"
    temporary.write_text(json.dumps(record, indent=2) + "\n")
    temporary.replace(root / "environment.json")


def prepare(args):
    docker, record = preflight(args.profile, args.nodes)
    if not re.fullmatch(r"[a-z0-9./-]+@sha256:[0-9a-f]{64}", args.rustfs_image):
        raise ValueError("pin the RustFS image by repository digest")
    root = args.output.resolve()
    root.mkdir(mode=0o700, parents=True, exist_ok=False)
    owner = "cellar-multi-raft-" + secrets.token_hex(8)
    label = "cellar.multi-raft.run=" + owner
    record.update(format=1, owner=owner, completed=False, phase="preparing-backend",
                  rustfs_image=args.rustfs_image, network=owner, volume=owner + "-objects",
                  container=owner + "-rustfs")
    save(root, record)
    credential_file = root / "rustfs.env"
    with open(credential_file, "x", opener=lambda path, flags: os.open(path, flags, 0o600)) as output:
        output.write("RUSTFS_ACCESS_KEY=" + secrets.token_hex(16) + "\n")
        output.write("RUSTFS_SECRET_KEY=" + secrets.token_hex(32) + "\n")
    command(docker + ["pull", args.rustfs_image])
    command(docker + ["network", "create", "--label", label, record["network"]])
    command(docker + ["volume", "create", "--label", label, record["volume"]])
    command(docker + [
        "run", "--detach", "--name", record["container"], "--label", label,
        "--network", record["network"], "--network-alias", "rustfs",
        "--cpus", "1", "--memory", "2g", "--memory-swap", "2g",
        "--ulimit", "nofile=65536:65536", "--restart", "no",
        "--publish", "127.0.0.1::9000", "--env-file", str(credential_file),
        "--env", "RUSTFS_CONSOLE_ENABLE=false", "--env", "RUSTFS_OBS_LOGGER_LEVEL=error",
        "--mount", f"type=volume,src={record['volume']},dst=/data",
        args.rustfs_image, "/data",
    ])
    ports = json.loads(command(docker + [
        "inspect", "--format", '{{json (index .NetworkSettings.Ports "9000/tcp")}}', record["container"],
    ]))
    if len(ports) != 1 or ports[0]["HostIp"] != "127.0.0.1":
        raise RuntimeError("RustFS endpoint must bind only the loopback interface")
    record["endpoint"] = "http://127.0.0.1:" + ports[0]["HostPort"]
    record["image_id"] = command(docker + ["image", "inspect", "--format", "{{.Id}}", args.rustfs_image])
    save(root, record)
    deadline = time.monotonic() + 60
    while True:
        try:
            with urllib.request.urlopen(record["endpoint"] + "/health", timeout=2) as response:
                if response.status == 200:
                    break
        except (OSError, urllib.error.HTTPError):
            if time.monotonic() >= deadline:
                raise RuntimeError("RustFS health deadline expired; retain this run for inspection") from None
        time.sleep(0.25)
    actual = json.loads(command(docker + ["inspect", "--format", "{{json .HostConfig}}", record["container"]]))
    if (actual["NanoCpus"], actual["Memory"], actual["MemorySwap"]) != (10**9, 2 * GIB, 2 * GIB):
        raise RuntimeError("RustFS resource limits were not captured")
    record["phase"] = "backend-ready"
    save(root, record)
    print(json.dumps({"phase": record["phase"], "environment": str(root / "environment.json"),
                      "completed": False}))


def stop(args):
    root = args.output.resolve()
    record = json.loads((root / "environment.json").read_text())
    if record["profile"] != args.profile:
        raise ValueError("profile does not match the recorded environment")
    if not re.fullmatch(r"cellar-multi-raft-[0-9a-f]{16}", record["owner"]):
        raise ValueError("invalid environment owner")
    docker = ["docker", "--context", "colima-" + args.profile]
    # Stop just this backend; retain the volume and credentials for recovery.
    owner = command(docker + ["inspect", "--format", '{{index .Config.Labels "cellar.multi-raft.run"}}', record["container"]])
    if owner != record["owner"]:
        raise ValueError("container ownership does not match")
    command(docker + ["stop", "--time", "30", record["container"]])
    record["phase"] = "backend-stopped"
    save(root, record)


def probe_store(args):
    root = args.output.resolve()
    record = json.loads((root / "environment.json").read_text())
    if record["profile"] != args.profile or record["phase"] != "backend-ready":
        raise ValueError("probe requires the recorded running backend/profile")
    credentials = dict(line.split("=", 1) for line in (root / "rustfs.env").read_text().splitlines())
    bucket = record["owner"] + "-probe-" + secrets.token_hex(4)
    observations = probe(record["endpoint"], bucket, credentials["RUSTFS_ACCESS_KEY"], credentials["RUSTFS_SECRET_KEY"])
    record["s3_probe"] = {"bucket": bucket, "observations": observations,
                          "scope": "object-store HTTP protocol only; no Cellar qualification"}
    save(root, record)
    print(json.dumps({"operations": len(observations), "passed": True, "cellar_qualified": False}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("preflight", "prepare", "probe", "stop"))
    parser.add_argument("--profile", default="cellar-multi-raft")
    parser.add_argument("--nodes", type=int, choices=(1, 3, 5), default=3)
    parser.add_argument("--rustfs-image", default=RUSTFS_IMAGE)
    parser.add_argument("--output", type=pathlib.Path)
    args = parser.parse_args()
    if args.action == "preflight":
        print(json.dumps(preflight(args.profile, args.nodes)[1], indent=2))
    elif args.output is None:
        parser.error("--output is required for prepare/stop")
    elif args.action == "prepare":
        prepare(args)
    elif args.action == "probe":
        probe_store(args)
    else:
        stop(args)


if __name__ == "__main__":
    main()
