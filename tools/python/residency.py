#!/usr/bin/env python3
# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Measure real HTTP bridge cost with a verified population of idle cells.

Local-only nodes isolate interpreter scheduling from fleet/storage latency.
This complements, and cannot replace, durable load and recovery qualification.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import time

from baseline import Connection, Host, digest, phase, require, save


def resident(host, target):
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        observation = host.observations()
        require(observation["native_resident_cells"] == target,
                f"native population changed: {observation['native_resident_cells']} != {target}")
        runtime = observation["runtime"]
        if runtime["kind"] == "native":
            return observation
        snapshot = runtime["interpreter"]
        if snapshot is not None and snapshot["age_ns"] < 1_000_000_000:
            work = snapshot["work"]
            if work["resident_objects"] == target and work["active_invocations"] == work["pending_retirements"] == 0:
                return observation
        time.sleep(.025)
    raise RuntimeError("Python resident population did not settle")


def populate(host, start, end):
    def worker(index):
        client = Connection(host.origin)
        verified = 0
        try:
            for cell in range(start + index, end, 4):
                body = json.loads(client.request("GET", f"/cells/resident-{cell}"))
                require(body == {"value": 0}, "unexpected fresh cell state")
                verified += 1
            return verified
        finally:
            client.close()
    with ThreadPoolExecutor(max_workers=4) as executor:
        require(sum(executor.map(worker, range(4))) == end - start, "population was not fully observed")


def run_case(executable, mode, root, args, evidence):
    host = Host(executable, mode + "-resident", root, "primary")
    evidence.update(status="running", levels=[], load_average_before=list(os.getloadavg()))
    try:
        evidence["startup_ns"] = host.readiness()
        evidence["host"] = host.ready
        require(host.ready["durability"] == "local-only" and host.ready["max_resident"] == 4096,
                "wrong residency fixture policy")
        require(host.ready["embed_compiled"] == (mode == "python"), "native reference must exclude libpython")
        require(host.ready["debug_assertions"] == (args.build_profile == "dev"), "incorrect build profile")
        evidence["python_runtime"] = host.python_runtime() if mode == "python" else None
        previous = 0
        for population in args.populations:
            level = {"population": population, "phases": [], "attempts": {}}
            evidence["levels"].append(level)
            started = time.perf_counter_ns()
            populate(host, previous, population)
            level["population_ns"] = time.perf_counter_ns() - started
            level["before"] = resident(host, population)
            for concurrency in sorted({1, args.concurrency}):
                for size in (0, 4096):
                    attempts = level["attempts"].setdefault(f"echo-{size}-c{concurrency}", {})
                    measured = phase(host, "echo", size, concurrency, args.count, args.warmup, {}, attempts)
                    level["phases"].append(measured)
                    print(f"{evidence['runtime']} r{evidence['repetition']} cells={population} "
                          f"{measured['name']}: {measured['requests_per_second']:.1f} rps "
                          f"p50={measured['latency_ns']['p50']/1e6:.3f} ms "
                          f"p99={measured['latency_ns']['p99']/1e6:.3f} ms", flush=True)
            level["after"] = resident(host, population)
            previous = population
        if mode == "python":
            require(host.python_runtime() == evidence["python_runtime"], "ABI/GIL state changed")
        stopping = time.perf_counter_ns()
        try:
            evidence["shutdown"] = host.stop()
        finally:
            evidence["shutdown_ns"] = time.perf_counter_ns() - stopping
        if mode == "python":
            report = evidence["shutdown"]["python"]
            require(report["objects_created"] == report["objects_retired"] == previous,
                    "resident objects churned or escaped joined cleanup")
        evidence["status"] = "complete"
    except BaseException as error:
        evidence.update(status="failed", failure=f"{type(error).__name__}: {error}")
        raise
    finally:
        host.cleanup()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--native-host", type=Path, required=True)
    parser.add_argument("--python-host", action="append", required=True, metavar="LABEL=PATH")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--build-profile", choices=("dev", "lab", "release"), required=True)
    parser.add_argument("--populations", type=int, nargs="+", default=[0, 64, 512, 2048])
    parser.add_argument("--count", type=int, default=200)
    parser.add_argument("--warmup", type=int, default=10)
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--repetitions", type=int, default=3)
    args = parser.parse_args()
    require(args.populations == sorted(set(args.populations)) and
            all(0 <= value <= 4096 for value in args.populations), "populations must increase within 0..4096")
    require(1 <= args.count <= 100_000 and 1 <= args.warmup <= 1000 and
            1 <= args.concurrency <= 16 and 1 <= args.repetitions <= 20, "invalid finite workload limits")
    hosts = [("native", "native", args.native_host.resolve(strict=True))]
    for item in args.python_host:
        label, path = item.split("=", 1)
        require(label and all(c.isascii() and (c.isalnum() or c in "-_") for c in label), "invalid label")
        hosts.append((label, "python", Path(path).resolve(strict=True)))
    require(len({label for label, _, _ in hosts}) == len(hosts), "duplicate runtime labels")
    args.output.mkdir(parents=True, exist_ok=False)
    document = {"format": 1, "status": "running", "cases": [],
                "environment": {"platform": platform.platform(), "client_python": sys.version,
                    "source_git_sha": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
                    "source_diff_sha256": hashlib.sha256(subprocess.check_output(["git", "diff", "HEAD"])).hexdigest()},
                "workload": {key: getattr(args, key) for key in ("build_profile", "populations", "count", "warmup", "concurrency", "repetitions")},
                "artifacts": [{"label": label, "path": str(path), "sha256": digest(path)} for label, _, path in hosts],
                "scope": "Closed-loop loopback, local-only cells; no durability, fleet capacity or recovery claim."}
    path = args.output / "residency.json"
    try:
        for repetition in range(args.repetitions):
            for label, mode, executable in hosts[repetition % len(hosts):] + hosts[:repetition % len(hosts)]:
                root = args.output / f"{label}-{repetition}"
                root.mkdir()
                evidence = {"runtime": label, "repetition": repetition}
                document["cases"].append(evidence)
                save(path, document)
                try:
                    run_case(executable, mode, root, args, evidence)
                finally:
                    save(path, document)
        document["status"] = "complete"
    except BaseException as error:
        document.update(status="failed", failure=f"{type(error).__name__}: {error}")
        raise
    finally:
        save(path, document)


if __name__ == "__main__":
    main()
