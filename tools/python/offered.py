#!/usr/bin/env python3
"""Compare native/Python HTTP echo, cell echo or reads at independent offered rates.

Every scheduled arrival is retained, including client drops and HTTP failures.
A completed experiment is not a claim that every offered request succeeded.
"""
import argparse
from collections import Counter
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import subprocess
import time

from baseline import Connection, Host, digest, require, save
import affinity
from residency import populate, resident


def observed_idle(host, population=0):
    observation = resident(host, population)
    if observation["runtime"]["kind"] == "native":
        return observation
    generation = observation["runtime"]["interpreter"]["generation"]
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        observation = resident(host, population)
        if observation["runtime"]["interpreter"]["generation"] > generation:
            return observation
        time.sleep(.025)
    raise RuntimeError("post-workload interpreter observation did not arrive")


def distribution(values):
    if not values:
        return None
    values = sorted(values)
    return {name: values[max(0, math.ceil(len(values) * fraction) - 1)]
            for name, fraction in (("p50", .50), ("p95", .95), ("p99", .99), ("max", 1))}


def summarize(report):
    require(report["format"] == 1, "unknown client report format")
    work = report["workload"]
    count = work["rate"] * work["duration_ms"] // 1000
    window = work["duration_ms"] * 1_000_000
    require(count > 0 and report["planned"] == len(report["samples"]) == count,
            "missing scheduled arrivals")
    require(report["elapsed_ns"] >= window, "truncated measurement window")
    require(0 <= report["peak_inflight"] <= work["max_inflight"], "unbounded client admission")
    counts, latency, from_schedule, dispatch_delay = Counter(), [], [], []
    successful_in_window = 0
    failure_details = 0
    for index, sample in enumerate(report["samples"]):
        scheduled = index * 1_000_000_000 // work["rate"]
        require(sample["id"] == index and sample["scheduled_ns"] == scheduled,
                "arrival identity or schedule changed")
        observed = sample["observed_ns"]
        require(scheduled <= observed <= report["elapsed_ns"], "invalid observation time")
        outcome = sample["outcome"]
        require(outcome in {"success", "client_capacity", "client_late", "timeout",
                            "transport", "http_status", "body_mismatch"}, "unknown outcome")
        counts[outcome] += 1
        if "response_prefix" in sample:
            prefix = sample["response_prefix"]
            failure_details += 1
            require(outcome == "http_status" and isinstance(prefix, str)
                    and len(prefix) <= 1024 and failure_details <= 16,
                    "invalid or unbounded HTTP failure diagnostics")
        if outcome.startswith("client_"):
            require(sample["dispatched_ns"] is None and sample["completed_ns"] is None
                    and sample["status"] is None, "client drop claims a network result")
            if outcome == "client_late":
                require(observed >= window or observed - scheduled > work["max_lag_ms"] * 1_000_000,
                        "on-time arrival classified as late")
            else:
                require(observed < window and observed - scheduled <= work["max_lag_ms"] * 1_000_000,
                        "late arrival classified as capacity")
            continue
        dispatched, completed = sample["dispatched_ns"], sample["completed_ns"]
        require(dispatched == observed and dispatched < window
                and dispatched - scheduled <= work["max_lag_ms"] * 1_000_000,
                "client sent outside its admission window")
        require(dispatched <= completed <= report["elapsed_ns"], "invalid completion time")
        if outcome in {"success", "body_mismatch"}:
            require(sample["status"] == 200, "successful HTTP status missing")
        if outcome == "http_status":
            require(isinstance(sample["status"], int) and sample["status"] != 200,
                    "HTTP failure status missing")
        dispatch_delay.append(dispatched - scheduled)
        if outcome == "success":
            latency.append(completed - dispatched)
            from_schedule.append(completed - scheduled)
            successful_in_window += completed <= window
    return {"arrivals": count, "outcomes": dict(counts), "peak_inflight": report["peak_inflight"],
            "successful_in_window": successful_in_window,
            "completed_in_window_per_second": successful_in_window * 1e9 / window,
            "successes_per_second_including_drain": counts["success"] * 1e9 / report["elapsed_ns"],
            "success_fraction": counts["success"] / count,
            "dispatch_delay_ns": distribution(dispatch_delay),
            "successful_request_latency_ns": distribution(latency),
            "successful_scheduled_latency_ns": distribution(from_schedule)}


def read_targets(population):
    require(0 <= population <= 64, "read population must be from 0 to 64")
    return [dict(path=f"/cells/resident-{index}", expected_body=list(b'{"value":1}'))
            for index in range(population)]


def measure(client, host, args, rate, duration, path, reads=()):
    cpus = args.cpu_placement["client"] if args.cpu_placement else None
    evidence = path.with_suffix(".affinity.json")
    origin = host.origin.rstrip("/") + "/cell-echo" if args.cell_echo else host.origin
    command = [*affinity.prefix(cpus, evidence), str(client), origin, str(rate), str(duration), str(args.max_inflight),
               str(args.body_bytes), str(args.timeout_ms), str(args.max_lag_ms), str(path)]
    if reads:
        command.append(str(path.parent / "reads.json"))
    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            timeout=(duration + args.timeout_ms) / 1000 + 30)
    path.with_suffix(".log").write_bytes(result.stdout)
    require(result.returncode == 0, f"native client failed ({result.returncode}); see {path}")
    if cpus is not None:
        affinity.verify(evidence, cpus)
    report = json.loads(path.read_text())
    require(report["debug_assertions"] == (args.build_profile == "dev"), "client build profile mismatch")
    route = "/" if reads else ("/cell-echo" if args.cell_echo else "/echo")
    expected = {"url": host.origin.rstrip("/") + route, "rate": rate,
                "duration_ms": duration, "max_inflight": args.max_inflight, "body_bytes": args.body_bytes,
                "timeout_ms": args.timeout_ms, "max_lag_ms": args.max_lag_ms}
    if reads:
        expected["reads"] = reads
    require(report["workload"] == expected, "client workload changed")
    return summarize(report)


def run_case(client, executable, mode, args, root, case):
    cpus = args.cpu_placement["host"] if args.cpu_placement else None
    evidence = root / "host-affinity.json"
    host = Host(executable, mode + "-resident", root, "primary",
                command_prefix=affinity.prefix(cpus, evidence))
    case.update(status="running", phases=[], load_average_before=list(os.getloadavg()))
    try:
        case["startup_ns"] = host.readiness()
        if cpus is not None:
            case["cpu_affinity"] = affinity.verify(evidence, cpus, host.process.pid)
            require(sorted(os.sched_getaffinity(host.process.pid)) == cpus,
                    "host changed CPU affinity after startup")
        case["host"] = host.ready
        require(host.ready["embed_compiled"] == (mode.startswith("python"))
                and host.ready["debug_assertions"] == (args.build_profile == "dev")
                and host.ready["durability"] == "local-only", "incorrect measurement host")
        case["python_runtime"] = host.python_runtime() if mode.startswith("python") else None
        reads = read_targets(args.read_cells)
        population = 1 if args.cell_echo else args.read_cells
        if reads:
            save(root / "reads.json", reads)
            populate(host, 0, args.read_cells)
            connection = Connection(host.origin)
            try:
                for index in range(args.read_cells):
                    require(json.loads(connection.request("POST", f"/cells/resident-{index}")) == {"value": 1}, "seed did not commit")
            finally:
                connection.close()
            case["populated"] = observed_idle(host, args.read_cells)
        elif args.cell_echo:
            connection = Connection(host.origin)
            try:
                require(connection.request("POST", "/cell-echo", b"populate") == b"populate",
                        "cell echo changed its bootstrap body")
            finally:
                connection.close()
            case["populated"] = observed_idle(host, population)
        for rate in args.rates:
            phase = {"rate": rate}
            case["phases"].append(phase)
            phase["warmup"] = measure(client, host, args, rate, args.warmup_ms, root / f"warmup-{rate}.json", reads)
            phase["before"] = observed_idle(host, population)
            if cpus is not None:
                phase["affinity_before"] = affinity.verify_threads(host.process.pid, cpus)
            phase["measurement"] = measure(client, host, args, rate, args.duration_ms, root / f"measured-{rate}.json", reads)
            phase["after"] = observed_idle(host, population)
            if cpus is not None:
                phase["affinity_after"] = affinity.verify_threads(host.process.pid, cpus)
            if mode.startswith("python"):
                before = phase["before"]["runtime"]["interpreter"]["interpreter"]
                after = phase["after"]["runtime"]["interpreter"]["interpreter"]
                require(after["handlers_completed"] - before["handlers_completed"]
                        >= (2 if population else 1) * phase["measurement"]["outcomes"].get("success", 0),
                        "interpreter observation omitted completed requests")
            print(f"{case['runtime']} r{case['repetition']} offered={rate}: "
                  f"{phase['measurement']['outcomes']}", flush=True)
        if mode.startswith("python"):
            require(host.python_runtime() == case["python_runtime"], "ABI/GIL state changed")
        case["shutdown"] = host.stop()
        if mode.startswith("python"):
            report = case["shutdown"]["python"]
            require(report["objects_created"] == report["objects_retired"] == population,
                    "resident objects churned or escaped joined cleanup")
        case.update(status="complete", load_average_after=list(os.getloadavg()))
    finally:
        host.cleanup()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--client", type=Path, required=True)
    parser.add_argument("--native-host", type=Path, required=True)
    parser.add_argument("--python-host", action="append", required=True, metavar="LABEL=PATH")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--build-profile", choices=("dev", "lab", "release"), required=True)
    parser.add_argument("--rates", type=int, nargs="+", default=[1000, 5000, 10000])
    parser.add_argument("--duration-ms", type=int, default=5000)
    parser.add_argument("--warmup-ms", type=int, default=500)
    parser.add_argument("--max-inflight", type=int, default=64)
    parser.add_argument("--body-bytes", type=int, default=4096)
    parser.add_argument("--read-cells", type=int, default=0,
                        help="Read 1..64 resident cells instead of echo; requires --body-bytes 0")
    parser.add_argument("--cell-echo", action="store_true",
                        help="Echo through one resident cell instead of the root handler; excludes --read-cells")
    parser.add_argument("--timeout-ms", type=int, default=1000)
    parser.add_argument("--max-lag-ms", type=int, default=5)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--host-cpus", help="Linux CPU IDs, e.g. 0,1; requires disjoint --client-cpus")
    parser.add_argument("--client-cpus", help="Linux CPU IDs, e.g. 2,3; requires --host-cpus")
    args = parser.parse_args()
    args.cpu_placement = affinity.placement(args.host_cpus, args.client_cpus)
    require(1 <= args.repetitions <= 20 and 1 <= args.max_inflight <= 512
            and 0 <= args.body_bytes <= 65536 and 1 <= args.timeout_ms <= 10000
            and 1 <= args.max_lag_ms <= 1000, "invalid workload limits")
    require(len(args.rates) == len(set(args.rates)) and all(1 <= rate <= 100000 for rate in args.rates)
            and all(1 <= duration <= 120000 and all(1 <= rate * duration // 1000 <= 1000000
                    for rate in args.rates) for duration in (args.warmup_ms, args.duration_ms)),
            "invalid offered rates or durations")
    require(args.read_cells == 8, "snapshot diagnostic requires eight populated cells")
    read_targets(args.read_cells)
    require(not args.cell_echo or not args.read_cells, "cell echo and cell reads are distinct workloads")
    require(not args.read_cells or args.body_bytes == 0, "cell reads require --body-bytes 0")
    client = args.client.resolve(strict=True)
    hosts = [("native", "native", args.native_host.resolve(strict=True))]
    for argument in args.python_host:
        label, path = argument.split("=", 1)
        require(label and all(c.isascii() and (c.isalnum() or c in "-_") for c in label), "invalid label")
        hosts.append((label, "python-sequential", Path(path).resolve(strict=True)))
        hosts.append((label + "-initial", "python", Path(path).resolve(strict=True)))
    require(len({label for label, _, _ in hosts}) == len(hosts), "duplicate label")
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=False)
    report = {"format": 1, "status": "running", "environment": {"platform": platform.platform(),
        "cpu_count": os.cpu_count(), "cpu_placement": args.cpu_placement, "build_profile": args.build_profile,
        "source_git_sha": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "source_diff_sha256": hashlib.sha256(subprocess.check_output(["git", "diff", "HEAD"])).hexdigest()},
        "read_cells": args.read_cells,
        "cell_echo": args.cell_echo,
        "client": {"path": str(client), "sha256": digest(client)},
        "measurement_sources": {name: digest(Path(__file__).with_name(name))
                                for name in ("offered.py", "offered_load.rs", "baseline.py", "residency.py", "affinity.py")},
        "hosts": [{"label": label, "mode": mode, "path": str(path), "sha256": digest(path)}
                  for label, mode, path in hosts], "cases": [],
        "limits": ["Local-only loopback root/cell echo or resident-cell reads; no durable-write, network-store or multi-node capacity claim.",
                   "Two native client workers share the host machine; dispatch jitter and client drops remain visible.",
                   "Successful latency percentiles exclude failures and drops; always read them with arrival outcomes.",
                   "Both request latency and latency from scheduled arrival are reported; delays are never omitted.",
                   "Tokio timer granularity may group arrivals; dispatch delay must qualify the requested rate.",
                   "Warmup and measured requests use separate client processes and connection pools."]}
    if args.cpu_placement:
        report["limits"].append("CPU affinity limits eligible logical CPUs, not exclusive cores, CPU quota or memory; other work can still compete.")
    path = args.output / "offered.json"
    try:
        for repetition in range(args.repetitions):
            order = hosts[repetition % len(hosts):] + hosts[:repetition % len(hosts)]
            for label, mode, executable in order:
                root = args.output / f"{label}-{repetition}"
                root.mkdir()
                case = {"runtime": label, "repetition": repetition}
                report["cases"].append(case)
                save(path, report)
                run_case(client, executable, mode, args, root, case)
                save(path, report)
        report["status"] = "complete"
    except BaseException as error:
        report.update(status="failed", failure=f"{type(error).__name__}: {error}")
        raise
    finally:
        save(path, report)


if __name__ == "__main__":
    main()
