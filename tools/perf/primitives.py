#!/usr/bin/env python3
# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Measure real public SQL/KV/Queue bindings using RF3 and an owned RustFS lab.

Three SDK voters share one optimized Linux test process in a 3-CPU/9-GiB
container. This is a local primitive benchmark, not per-node capacity or
independent-failure-domain qualification. Failed windows and disks are retained.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "tools/multi-raft"))
from backend_control import atomic_json
from cell_groups import backend
from store_probe import probe
from raft_run import source_manifest, input_digest, parse_manifest, require

LABEL = "io.cellar.primitives-perf.run"
PREFIX = "PRIMITIVES_PERF "


def observations(path):
    result = []
    for line in path.read_text().splitlines():
        if PREFIX in line:
            result.append(json.loads(line.split(PREFIX, 1)[1]))
    return result


def check_rows(rows, primitive, image, seconds, repetitions):
    expected = []
    for repeat in range(repetitions):
        for concurrency in ([1, 4] if repeat % 2 == 0 else [4, 1]):
            for phase in (["warmup", "enqueue"] if primitive == "queue" else ["warmup", "write", "read"]):
                expected.append((repeat, concurrency, phase))
    require([(r["repeat"], r["measurement"]["concurrency"], r["phase"]) for r in rows] == expected,
            "missing or reordered measured windows")
    for row in rows:
        value = row["measurement"]
        duration = 2 if row["phase"] == "warmup" else seconds
        require(row["primitive"] == primitive and row["imageMode"] == image
                and row["durability"] == "quorum" and row["voters"] == 3
                and row["voterProcesses"] == 1 and row["verified"], "unverified primitive result")
        require(value["requestedSeconds"] == duration and value["elapsedSeconds"] >= duration
                and value["errors"] == 0 and value["successfulRequests"] == value["attempts"] > 0,
                "failed, empty or short measurement")
        latencies = value["latencyMicros"]
        require(len(latencies) == value["attempts"] and latencies == sorted(latencies), "invalid latency samples")
        for percentile in (50, 95, 99):
            index = (len(latencies) * percentile + 99) // 100 - 1
            require(value[f"p{percentile}Ms"] == latencies[index] / 1000, "percentile differs from raw samples")
        require(abs(value["requestsPerSecond"] - value["successfulRequests"] / value["elapsedSeconds"]) < 1e-8,
                "throughput differs from accounting")
        if primitive == "queue":
            require(value["batchMessages"] == 8 and value["acknowledgedMessages"] == value["successfulRequests"] * 8
                    and value["enqueueAndDrainSeconds"] >= value["elapsedSeconds"]
                    and value["verifiedMessagesPerSecondIncludingDrain"] > 0, "unverified Queue drain")


def activity():
    lines = subprocess.check_output(["ps", "-axo", "pid=,state=,comm="], text=True).splitlines()
    return [line.strip() for line in lines if re.search(r"/(rustc|clippy-driver|cellar-xtask)$", line)]


def run(arguments):
    require(re.fullmatch(r"sha256:[0-9a-f]{64}", arguments.image), "immutable image ID required")
    require(1 <= arguments.seconds <= 120 and 1 <= arguments.repetitions <= 5, "invalid timing settings")
    require(not arguments.output.exists(), "output must be a new directory")
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    require(not subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True).strip(),
            "commit the measured sources first")
    inputs = source_manifest()
    record, credentials = backend(arguments.backend)
    context = record["context"]

    def docker(*args, **kwargs):
        return subprocess.check_output(["docker", "--context", context, *args], text=True, **kwargs).strip()

    engine = json.loads(docker("info", "--format", '{"id":{{json .ID}},"cpus":{{.NCPU}},"memory":{{.MemTotal}}}'))
    require(engine["cpus"] >= 4 and engine["memory"] >= 12 * 1024**3, "VM cannot cover the fixed 11-GiB memory envelope")
    actual_revision = docker("image", "inspect", "--format", '{{index .Config.Labels "org.opencontainers.image.revision"}}', arguments.image)
    require(actual_revision == revision, "image revision differs")
    isolated = ("run", "--rm", "--network", "none", "--entrypoint")
    require(parse_manifest(docker(*isolated, "cat", arguments.image, "/usr/local/bin/raft-source.sha256")) == inputs,
            "image compiler inputs differ")
    executable = docker(*isolated, "sha256sum", arguments.image, "/usr/local/bin/cellar-perf-primitives").split()[0]
    compiler = docker(*isolated, "cat", arguments.image, "/usr/local/bin/compiler.txt")
    require("release: 1.98.1" in compiler, "unexpected measuring compiler")
    inventory = docker("run", "--rm", "--network", "none", arguments.image, "--list")
    require(all(f"rustfs_perf_{name}: test" in inventory for name in ("sql", "kv", "queue")), "measuring cases absent")
    arguments.output.mkdir(parents=True, mode=0o700)
    run_id = uuid.uuid4().hex
    bucket = "cellar-perf-" + run_id
    result = {"format": 1, "run_id": run_id, "status": "RUNNING", "source_revision": revision,
              "source_manifest_sha256": input_digest(inputs), "image": arguments.image,
              "executable_sha256": executable, "compiler": compiler, "profile": "lab",
              "engine": engine, "context": context, "rustfs": record["observed_rustfs"],
              "seconds": arguments.seconds, "repetitions": arguments.repetitions,
              "container_limits": {"cpus": 3, "memory_bytes": 9 * 1024**3, "memory_swap_bytes": 9 * 1024**3},
              "scope": __doc__, "cases": [], "host_activity_start": activity()}
    report = arguments.output / "run.json"
    atomic_json(report, result)
    atomic_json(arguments.output / "source-inputs.json", inputs)
    result["s3_probe"] = probe(record["endpoint"], bucket, credentials["RUSTFS_ACCESS_KEY"], credentials["RUSTFS_SECRET_KEY"])
    runtime_env = dict(os.environ, AWS_ACCESS_KEY_ID=credentials["RUSTFS_ACCESS_KEY"], AWS_SECRET_ACCESS_KEY=credentials["RUSTFS_SECRET_KEY"])
    (arguments.output / "inventory.txt").write_text(inventory + "\n")
    try:
        for image in ("materialized", "extents"):
            for primitive in ("sql", "kv", "queue"):
                require(source_manifest() == inputs, "source changed during measurements")
                stem = f"{primitive}-{image}"
                volume = "cellar-primitives-" + run_id + "-" + stem
                docker("volume", "create", "--label", f"{LABEL}={run_id}", volume)
                settings = {
                    "CELLAR_CELL_GROUPS_RUSTFS_ENDPOINT": record["endpoint"],
                    "CELLAR_CELL_GROUPS_RUSTFS_BUCKET": bucket,
                    "CELLAR_CELL_GROUPS_RUSTFS_PREFIX": run_id + "/" + stem,
                    "CELLAR_PRIMITIVES_PERF_IMAGE": image,
                    "CELLAR_PRIMITIVES_PERF_SECONDS": str(arguments.seconds),
                    "CELLAR_PRIMITIVES_PERF_REPETITIONS": str(arguments.repetitions),
                    "TMPDIR": "/data",
                }
                options = ["create", "--name", "cellar-primitives-" + run_id + "-" + stem,
                           "--label", f"{LABEL}={run_id}", "--network", "host", "--cpus", "3",
                           "--memory", "9g", "--memory-swap", "9g", "--pids-limit", "512",
                           "--mount", "type=volume,source=" + volume + ",target=/data",
                           "--env", "AWS_ACCESS_KEY_ID", "--env", "AWS_SECRET_ACCESS_KEY"]
                for key, value in settings.items():
                    options.extend(["--env", key + "=" + value])
                container = docker(*options, arguments.image, f"rustfs_perf_{primitive}", "--exact", "--ignored", "--nocapture", "--test-threads=1", env=runtime_env)
                case = {"primitive": primitive, "image_mode": image, "container": container, "volume": volume,
                        "host_activity_start": activity(), "status": "RUNNING", "samples": []}
                result["cases"].append(case)
                atomic_json(report, result)
                started = time.monotonic()
                process_budget = 120 + 2 * arguments.repetitions * (2 + arguments.seconds * (1 if primitive == "queue" else 2) + 60)
                log_path = arguments.output / (stem + ".log")
                with log_path.open("w") as log:
                    child = subprocess.Popen(["docker", "--context", context, "start", "--attach", container], stdout=log, stderr=subprocess.STDOUT)
                    try:
                        while child.poll() is None:
                            if time.monotonic() - started > process_budget:
                                case["watchdog_expired"] = True
                                docker("stop", "--time", "30", container)
                                child.wait(timeout=40)
                                break
                            try:
                                sample = docker("exec", container, "sh", "-c", "for f in cpu.stat memory.current memory.peak memory.events io.stat; do echo $f; cat /sys/fs/cgroup/$f; done", stderr=subprocess.DEVNULL)
                                case["samples"].append({"elapsed_seconds": time.monotonic() - started, "cgroup": sample})
                                atomic_json(report, result)
                            except subprocess.CalledProcessError:
                                pass  # Startup/exit races are followed by authoritative state inspection.
                            try:
                                child.wait(timeout=2)
                            except subprocess.TimeoutExpired:
                                pass
                    except BaseException:
                        docker("stop", "--time", "30", container)
                        child.wait()
                        raise
                state = json.loads(docker("inspect", "--format", '{"running":{{json .State.Running}},"exit_code":{{.State.ExitCode}},"oom":{{json .State.OOMKilled}},"image":{{json .Image}},"owner":{{json (index .Config.Labels "io.cellar.primitives-perf.run")}},"cpus":{{.HostConfig.NanoCpus}},"memory":{{.HostConfig.Memory}},"swap":{{.HostConfig.MemorySwap}}}', container))
                require(not state["running"] and state["image"] == arguments.image and state["owner"] == run_id
                        and (state["cpus"], state["memory"], state["swap"]) == (3 * 10**9, 9 * 1024**3, 9 * 1024**3), "runtime identity or resource limit changed")
                case.update(state=state, attach_returncode=child.returncode, elapsed_seconds=time.monotonic() - started,
                            log_sha256=hashlib.sha256(log_path.read_bytes()).hexdigest(), observations=observations(log_path))
                try:
                    require(state["exit_code"] == child.returncode == 0 and not state["oom"], "primitive process failed")
                    check_rows(case["observations"], primitive, image, arguments.seconds, arguments.repetitions)
                    require(case["samples"], "no kernel resource samples")
                    case["status"] = "PASSED"
                except Exception as error:
                    case.update(status="FAILED", error=str(error))
                atomic_json(report, result)
                print(json.dumps({"case": stem, "status": case["status"], "windows": len(case["observations"])}), flush=True)
        result["status"] = "PASSED" if all(c["status"] == "PASSED" for c in result["cases"]) else "FAILED"
    except BaseException as error:
        result.update(status="FAILED", error=str(error))
        raise
    finally:
        result["host_activity_end"] = activity()
        result["source_inputs_unchanged"] = source_manifest() == inputs
        atomic_json(report, result)
    require(result["status"] == "PASSED" and result["source_inputs_unchanged"], "failed measurements retained")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", required=True)
    parser.add_argument("--backend", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--seconds", type=int, default=30)
    parser.add_argument("--repetitions", type=int, default=2)
    arguments = parser.parse_args()
    result = run(arguments)
    print(json.dumps({"status": result["status"], "report": str(arguments.output / "run.json")}))


if __name__ == "__main__":
    main()
