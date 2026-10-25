#!/usr/bin/env python3
# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Qualify durable activation churn, settled resources and bucket-only recovery.

Each mutation is sent once. Failed runs retain uncertain attempts and still
check acknowledged state on recovery; recovery never changes a failure to a pass.
"""
from concurrent.futures import ThreadPoolExecutor
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import shutil
import signal
import sys
import subprocess
import threading
import time

from baseline import Connection, Host, digest, require, save
from operating_defaults_report import bridge_settled


def counter(raw):
    value = json.loads(raw)
    require(type(value) is dict and set(value) == {"value"} and
            type(value["value"]) is int and value["value"] >= 0, "malformed counter response")
    return value["value"]


def settled(host, python):
    first = host.observations()["runtime"]["interpreter"] if python else None
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        observation = host.observations()
        if observation["memory_sample_age_ns"] < 1_000_000_000:
            if not python:
                return observation
            runtime = observation["runtime"]
            sample = runtime["interpreter"]
            if sample is not None and sample["age_ns"] < 1_000_000_000 and (
                    first is None or sample["generation"] > first["generation"]):
                work, bridge = sample["work"], runtime["bridge"]
                if work["active_tasks"] == work["active_invocations"] == work["pending_retirements"] == 0 and (
                        bridge.get("native_invocations") == bridge["pending_reservations"] == bridge["queued_bytes"] == 0 and
                        all(bridge["operations"][key] == 0 for key in ("pending", "queued", "running", "replies_pending"))):
                    bridge_settled(bridge)
                    require(work["resident_objects"] <= host.ready["max_resident"], "settled object population exceeds native residency")
                    return observation
        time.sleep(.025)
    raise RuntimeError("churn work/telemetry did not settle")


def round_trip(host, population, concurrency, expected, stage):
    stage.update(expected_value=expected, attempts=[[] for _ in range(concurrency)])
    began = [None]
    barrier = threading.Barrier(concurrency + 1, action=lambda: began.__setitem__(0, time.perf_counter_ns()))
    stopped, failure_lock, first_failure = threading.Event(), threading.Lock(), []

    def worker(index):
        client = Connection(host.origin)
        try:
            require(client.request("POST", "/echo", b"warm") == b"warm", "warmup changed bytes")
            barrier.wait(timeout=30)
            for cell in range(index, population, concurrency):
                require(not stopped.is_set(), "another churn worker failed")
                row = dict(cell=cell, outcome="unknown")
                stage["attempts"][index].append(row)
                started = time.perf_counter_ns()
                try:
                    body = {"value": counter(client.request("POST", f"/cells/churn-{cell}"))}
                    row.update(latency_ns=time.perf_counter_ns() - started, response=body)
                    require(body == {"value": expected}, "wrong counter acknowledgement")
                    row["outcome"] = "acknowledged"
                except BaseException as error:
                    row.update(latency_ns=time.perf_counter_ns() - started,
                               error=f"{type(error).__name__}: {error}")
                    raise
            return time.perf_counter_ns()
        except BaseException as error:
            with failure_lock:
                if not first_failure:
                    first_failure.append(error)
                stopped.set()
            barrier.abort()
            raise
        finally:
            client.close()

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = [pool.submit(worker, index) for index in range(concurrency)]
        try:
            barrier.wait(timeout=30)
            ended = [future.result() for future in futures]
        except BaseException:
            stopped.set()
            barrier.abort()
            with failure_lock:
                error = first_failure[0] if first_failure else None
            if error is not None:
                raise error
            raise
    samples = sorted(row["latency_ns"] for rows in stage["attempts"] for row in rows)
    require(len(samples) == population, "missing churn requests")
    stage.update(elapsed_ns=max(ended) - began[0],
                 latency_ns={name: samples[math.ceil(q * len(samples)) - 1]
                             for name, q in (("p50", .5), ("p99", .99), ("max", 1))})


def bounds(population, rounds):
    """A lost response may have committed; only a reconciled read resolves it."""
    ledger = {cell: dict(acknowledged=0, attempted=0) for cell in range(population)}
    for stage in rounds:
        for group in stage["attempts"]:
            for row in group:
                entry = ledger[row["cell"]]
                entry["attempted"] += 1
                if row["outcome"] == "acknowledged":
                    require(row["response"] == {"value": entry["attempted"]}, "inconsistent acknowledged ledger")
                    entry["acknowledged"] = entry["attempted"]
    return ledger


def recover(host, ledger, evidence):
    evidence.update(reads=[], new_writes=[])
    client = Connection(host.origin)
    try:
        for cell, entry in sorted(ledger.items()):
            row = dict(cell=cell, outcome="unknown", **entry)
            evidence["reads"].append(row)
            value = counter(client.request("GET", f"/cells/churn-{cell}"))
            require(entry["acknowledged"] <= value <= entry["attempted"], "recovery lost acknowledged state or fabricated a write")
            row.update(value=value, outcome="acknowledged")
        for row in evidence["reads"]:
            # A fresh mutation after reconciliation, never a replay of an
            # uncertain command. Record the attempt before sending it.
            fresh = dict(cell=row["cell"], expected_value=row["value"] + 1, outcome="unknown")
            evidence["new_writes"].append(fresh)
            value = counter(client.request("POST", f"/cells/churn-{row['cell']}"))
            require(value == fresh["expected_value"], "incorrect fresh write after recovery")
            fresh.update(value=value, outcome="acknowledged")
    finally:
        client.close()


def run_case(artifact, root, args, case, checkpoint):
    primary = recovered = None
    python = artifact["mode"] == "python"
    case.update(status="running", rounds=[], load_average_before=list(os.getloadavg()))
    try:
        primary = Host(Path(artifact["path"]), artifact["mode"], root, "primary")
        try:
            case["startup_ns"] = primary.readiness()
            case["host"] = primary.ready
            require(primary.ready["max_resident"] < args.population and primary.ready["durability"] == "bucket", "workload does not exceed durable residency")
            require(primary.ready["embed_compiled"] == python and
                    primary.ready["debug_assertions"] == (args.build_profile == "dev"), "incorrect host build mode/profile")
            case["python_runtime"] = primary.python_runtime() if python else None
            case["before"] = settled(primary, python)
            for iteration in range(args.rounds):
                stage = dict(iteration=iteration)
                case["rounds"].append(stage)
                round_trip(primary, args.population, args.concurrency, iteration + 1, stage)
                stage.update(after=settled(primary, python), load_average=list(os.getloadavg()))
                checkpoint()
        except Exception as error:
            case["load_failure"] = f"{type(error).__name__}: {error}"
            checkpoint()
        if primary.ready is not None:
            case["owner_exit_before_kill"] = primary.process.poll()
            primary.hard_kill()
            case["owner_exit_after_kill"] = primary.process.returncode
            if case["owner_exit_before_kill"] is not None or case["owner_exit_after_kill"] != -9:
                case.setdefault("load_failure", "owner exited before the planned hard kill")
            require((root / "disk").is_dir(), "owner disk missing before deletion")
            shutil.rmtree(root / "disk")
            require(not (root / "disk").exists(), "owner disk survived deletion")
            case["owner_disk_removed"] = True
            time.sleep(primary.ready["lease_ttl_ms"] / 1000 + .1)
            case["recovery"] = {}
            recovered = Host(Path(artifact["path"]), artifact["mode"], root, "recovered")
            case["recovery"]["startup_ns"] = recovered.readiness()
            recover(recovered, bounds(args.population, case["rounds"]), case["recovery"])
            case["recovery"]["after"] = settled(recovered, python)
            case["recovery"]["python_runtime"] = recovered.python_runtime() if python else None
            case["shutdown"] = recovered.stop()
        case["status"] = "failed" if "load_failure" in case else "complete"
    except Exception as error:
        case.update(status="failed", failure=f"{type(error).__name__}: {error}")
    except BaseException as error:
        case.update(status="failed", failure=f"{type(error).__name__}: {error}")
        raise
    finally:
        for host in (primary, recovered):
            if host is not None:
                try:
                    host.cleanup()
                except Exception as error:
                    case.setdefault("cleanup_failures", []).append(f"{type(error).__name__}: {error}")
                    case["status"] = "failed"
        case["load_average_after"] = list(os.getloadavg())
        checkpoint()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--native-host", type=Path, required=True)
    parser.add_argument("--python-host", action="append", required=True, metavar="LABEL=PATH")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--build-profile", choices=("dev", "lab", "release"), required=True)
    parser.add_argument("--population", type=int, default=128)
    parser.add_argument("--rounds", type=int, default=16)
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--repetitions", type=int, default=3)
    args = parser.parse_args()
    require(65 <= args.population <= 1024 and 2 <= args.rounds <= 128 and
            1 <= args.concurrency <= 16 and 1 <= args.repetitions <= 5, "invalid finite churn limits")
    hosts = [("native", "native", args.native_host.resolve(strict=True))]
    for value in args.python_host:
        label, path = value.split("=", 1)
        require(label and all(c.isascii() and (c.isalnum() or c in "-_") for c in label), "invalid runtime label")
        hosts.append((label, "python", Path(path).resolve(strict=True)))
    require(len({label for label, _, _ in hosts}) == len(hosts) and
            args.population * (args.rounds + 1) * args.repetitions * len(hosts) <= 1_000_000, "duplicate label or excessive workload")
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=False)
    report = dict(format=1, status="running", cases=[],
        source=subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        source_diff_sha256=hashlib.sha256(subprocess.check_output(["git", "diff", "HEAD"])).hexdigest(),
        harness_sources={name: digest(Path(__file__).with_name(name)) for name in
                         ("churn.py", "churn_report.py", "baseline.py", "baseline_report.py", "operating_defaults_report.py")},
        platform=platform.platform(), client_python=sys.version,
        client_gil_enabled=getattr(sys, "_is_gil_enabled", lambda: True)(),
        workload={key: getattr(args, key) for key in ("population", "rounds", "concurrency", "repetitions", "build_profile")},
        artifacts=[dict(label=label, mode=mode, path=str(path), sha256=digest(path)) for label, mode, path in hosts],
        scope="Closed-loop loopback; private SQLite bucket; cached memory samples; no remote-store, fleet capacity or production SLO claim.")
    def checkpoint():
        save(args.output / "churn.json", report)
    checkpoint()
    try:
        artifacts = report["artifacts"]
        for repetition in range(args.repetitions):
            for artifact in artifacts[repetition % len(artifacts):] + artifacts[:repetition % len(artifacts)]:
                root = args.output / f"{artifact['label']}-{repetition}"
                root.mkdir()
                case = dict(runtime=artifact["label"], repetition=repetition)
                report["cases"].append(case)
                run_case(artifact, root, args, case, checkpoint)
                print(f"{case['status'].upper()} {artifact['label']} {repetition}", flush=True)
        report["status"] = "complete" if all(case["status"] == "complete" for case in report["cases"]) else "failed"
        if report["status"] == "complete":
            from churn_report import verify
            verify(report)
    except BaseException as error:
        report.update(status="failed", failure=f"{type(error).__name__}: {error}")
        raise
    finally:
        checkpoint()
    require(report["status"] == "complete", f"churn qualification failed; see {args.output / 'churn.json'}")


if __name__ == "__main__":
    def terminate(signum, frame):
        raise KeyboardInterrupt("churn interrupted")
    signal.signal(signal.SIGTERM, terminate)
    main()
