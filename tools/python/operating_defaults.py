#!/usr/bin/env python3
# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Qualify actual Python defaults under mixed load, admission pressure and takeover.

Uses only private compiled loopback hosts. Every mutation is attempted once;
unknown responses fail the run and remain in the raw ledger. No installation.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys
import threading
import time

from baseline import Connection, Host, digest, require, save, tool_version


def request(client, method, route, body, rows, **identity):
    row = dict(identity, method=method, route=route, request_bytes=len(body), outcome="unknown")
    rows.append(row)
    started = time.perf_counter_ns()
    client.http.request(method, route, body, {"Content-Type": "application/octet-stream"})
    response = client.http.getresponse()
    payload = response.read()
    row.update(status=response.status, response_bytes=len(payload),
               latency_ns=time.perf_counter_ns() - started)
    return row, payload


def success(row, body, expected):
    require(row["status"] == 200 and body == expected, f"unexpected response: {row}: {body[:256]!r}")
    row["outcome"] = "acknowledged"


def settled(host):
    # Observation is a native bypass; no Python invocation is created by polling.
    until = time.monotonic() + 5
    while time.monotonic() < until:
        observation = host.observations()
        bridge = observation["runtime"]["bridge"]
        if bridge["pending_reservations"] == bridge["queued_bytes"] == 0 and all(
                bridge["operations"][key] == 0 for key in ("pending", "queued", "running", "replies_pending")):
            return observation
        time.sleep(.025)
    raise RuntimeError("managed bridge did not settle")


def mixed(host, concurrency, count, expected, evidence):
    evidence.update(concurrency=concurrency, before=settled(host), attempts={})
    epoch = [None]
    barrier = threading.Barrier(concurrency + 1, action=lambda: epoch.__setitem__(0, time.perf_counter_ns()))

    def worker(index):
        client = Connection(host.origin)
        rows = []
        evidence["attempts"][str(index)] = rows
        value = expected.get(index, 0)
        try:
            require(client.request("POST", "/echo", b"warm") == b"warm", "warmup failed")
            barrier.wait(timeout=30)
            for ordinal in range(count):
                kind = ("echo-0", "counter", "echo-4096", "cpu")[ordinal % 4]
                route = f"/cells/defaults-{index}" if kind == "counter" else "/defaults/cpu" if kind == "cpu" else "/echo"
                payload = b"x" * (4096 if kind == "echo-4096" else 0)
                row, body = request(client, "POST", route, payload, rows, worker=index, ordinal=ordinal, kind=kind)
                if kind == "counter":
                    value += 1
                    require(row["status"] == 200 and json.loads(body) == {"value": value},
                            f"wrong counter acknowledgement: expected={value}: {row}: {body[:256]!r}")
                    row.update(value=value, outcome="acknowledged")
                elif kind == "cpu":
                    require(row["status"] == 200 and json.loads(body) == {"value": sum((i ^ 7) % 97 for i in range(2000))},
                            f"wrong CPU result: {row}: {body[:256]!r}")
                    row["outcome"] = "acknowledged"
                else:
                    success(row, body, payload)
            return index, value, time.perf_counter_ns()
        except BaseException:
            barrier.abort()
            raise
        finally:
            client.close()

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [executor.submit(worker, index) for index in range(concurrency)]
        barrier.wait(timeout=30)
        results = [future.result() for future in futures]
    expected.update({index: value for index, value, _ in results})
    samples = sorted(row["latency_ns"] for rows in evidence["attempts"].values() for row in rows)
    evidence.update(elapsed_ns=max(end for _, _, end in results) - epoch[0], after=settled(host),
                    latency_ns={name: samples[math.ceil(q * len(samples)) - 1] for name, q in (("p50", .5), ("p95", .95), ("p99", .99), ("max", 1))})
    evidence["requests_per_second"] = len(samples) * 1e9 / evidence["elapsed_ns"]


def saturation(host, limits, evidence):
    target = limits["max_invocations"]
    evidence.update(target=target, held=[], rejected=[], observations=[], before=settled(host))
    barrier = threading.Barrier(target + 1)
    payload = b"h" * 65536

    def participant(index):
        client = Connection(host.origin)
        try:
            require(client.request("POST", "/echo", b"warm") == b"warm", "pressure warmup failed")
            barrier.wait(timeout=30)
            row, body = request(client, "POST", f"/defaults/held/{target}", payload, evidence["held"], worker=index)
            success(row, body, payload)
        except BaseException:
            barrier.abort()
            raise
        finally:
            client.close()

    with ThreadPoolExecutor(max_workers=target) as executor:
        futures = [executor.submit(participant, index) for index in range(target)]
        barrier.wait(timeout=30)
        deadline = time.monotonic() + 4
        while time.monotonic() < deadline:
            observation = host.observations()
            evidence["observations"].append(observation)
            snapshot = observation["runtime"]["interpreter"]
            if snapshot is not None and snapshot["age_ns"] < 1_000_000_000 and snapshot["work"]["active_invocations"] == target:
                break
            time.sleep(.025)
        else:
            raise RuntimeError("actual default invocation cap was never observed full")
        client = Connection(host.origin)
        try:
            for ordinal in range(16):
                row, body = request(client, "POST", "/echo", b"excess", evidence["rejected"], ordinal=ordinal)
                require(row["status"] == 503 and json.loads(body) ==
                        {"error": "application admission refused", "code": "Overloaded", "outcome": "unknown"},
                        f"wrong admission rejection: {row}: {body[:256]!r}")
                row.update(outcome="refused", reason="Python invocation capacity exhausted", error_body=body.decode())
        finally:
            client.close()
        for future in futures:
            future.result()
    evidence["after"] = settled(host)
    client = Connection(host.origin)
    try:
        row, body = request(client, "POST", "/echo", b"usable-after-overload", evidence.setdefault("subsequent", []))
        success(row, body, b"usable-after-overload")
    finally:
        client.close()


def body_boundary(host, limit, rows):
    client = Connection(host.origin)
    try:
        payload = b"b" * limit
        row, body = request(client, "POST", "/echo", payload, rows)
        success(row, body, payload)
        row, body = request(client, "POST", "/echo", payload + b"b", rows)
        require(row["status"] >= 500 and b"Python request body exceeds limit" in body, f"wrong body rejection: {row}: {body[:256]!r}")
        row.update(outcome="refused", reason="Python request body exceeds limit", error_body=body.decode())
    finally:
        client.close()


def run_case(executable, root, args, evidence):
    primary = recovered = None
    expected = {}
    evidence.update(status="running", phases=[], body_boundary=[], saturation={}, recovery={}, load_average_before=list(os.getloadavg()))
    try:
        primary = Host(executable, "python-defaults", root, "primary")
        evidence["startup_ns"] = primary.readiness()
        evidence["host"] = primary.ready
        require(primary.ready["debug_assertions"] == (args.build_profile == "dev"), "incorrect declared build profile")
        evidence["python_runtime"] = primary.python_runtime()
        limits = settled(primary)["runtime"]["limits"]
        evidence["limits"] = limits
        require(all(isinstance(value, int) and value > 0 for value in limits.values()), "nonfinite default limit")
        require(primary.ready["native_max_stateless_requests"] > limits["max_invocations"] and primary.ready["native_admission_wait_ms"] == 0,
                "native fixture must leave headroom for Python admission qualification")
        for concurrency in args.concurrency:
            phase = {}
            evidence["phases"].append(phase)
            mixed(primary, concurrency, args.count, expected, phase)
        body_boundary(primary, limits["max_body_bytes"], evidence["body_boundary"])
        saturation(primary, limits, evidence["saturation"])
        evidence["python_runtime_after"] = primary.python_runtime()
        require(evidence["python_runtime_after"] == evidence["python_runtime"], "ABI/GIL changed under load")
        evidence["before_kill"] = settled(primary)
        killed = time.perf_counter_ns()
        primary.hard_kill()
        require((root / "disk").is_dir(), "missing owner state")
        shutil.rmtree(root / "disk")
        require(not (root / "disk").exists(), "owner state retained")
        evidence["owner_disk_deleted"] = True
        time.sleep(primary.ready["lease_ttl_ms"] / 1000 + .1)
        recovered = Host(executable, "python-defaults", root, "recovered")
        recovery = evidence["recovery"]
        recovery.update(startup_ns=recovered.readiness(), reads=[], new_writes=[])
        client = Connection(recovered.origin)
        try:
            for operation, increment in (("reads", 0), ("new_writes", 1)):
                for worker, value in sorted(expected.items()):
                    row, body = request(client, "GET" if increment == 0 else "POST", f"/cells/defaults-{worker}", b"", recovery[operation], worker=worker)
                    require(row["status"] == 200 and json.loads(body) == {"value": value + increment}, "recovered counter disagrees with acknowledged ledger")
                    row.update(outcome="acknowledged", value=value + increment)
                if increment == 0:
                    recovery["kill_to_verified_reads_ns"] = time.perf_counter_ns() - killed
        finally:
            client.close()
        evidence["python_runtime_recovered"] = recovered.python_runtime()
        require(evidence["python_runtime_recovered"] == evidence["python_runtime"], "recovery changed ABI/GIL")
        recovery["observations"] = settled(recovered)
        evidence["shutdown"] = recovered.stop()
        evidence.update(status="complete", load_average_after=list(os.getloadavg()))
    finally:
        for host in (primary, recovered):
            if host is not None:
                host.cleanup()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--python-host", action="append", required=True, metavar="LABEL=PATH")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--build-profile", choices=("dev", "lab", "release"), required=True)
    parser.add_argument("--count", type=int, default=40)
    parser.add_argument("--concurrency", type=int, nargs="+", default=[1, 4, 16, 32])
    parser.add_argument("--repetitions", type=int, default=3)
    args = parser.parse_args()
    require(4 <= args.count <= 10000 and args.count % 4 == 0 and 1 <= args.repetitions <= 20 and
            args.concurrency == sorted(set(args.concurrency)) and args.concurrency[0] == 1 and args.concurrency[-1] <= 32,
            "invalid finite workload; counts must include complete four-operation cycles")
    hosts = []
    for value in args.python_host:
        label, path = value.split("=", 1)
        require(label and all(c.isascii() and (c.isalnum() or c in "-_") for c in label), "invalid runtime label")
        hosts.append((label, Path(path).resolve(strict=True)))
    require(len({label for label, _ in hosts}) == len(hosts), "duplicate runtime label")
    args.output.mkdir(parents=True, exist_ok=False)
    document = {"format": 1, "status": "running", "cases": [],
                "environment": {"platform": platform.platform(), "cpu_count": os.cpu_count(), "client_python": sys.version,
                    "build_profile": args.build_profile, "caller_rustc": tool_version(["rustc", "-vV"]),
                    "source_git_sha": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
                    "source_diff_sha256": hashlib.sha256(subprocess.check_output(["git", "diff", "HEAD"])).hexdigest()},
                "workload": {"count": args.count, "concurrency": args.concurrency, "repetitions": args.repetitions},
                "artifacts": [{"label": label, "path": str(path), "sha256": digest(path)} for label, path in hosts],
                "scope": ["Shipping Python Options::default; native stateless capacity raised and admission wait zero to isolate Python pressure.",
                    "Closed-loop client shares development machine; private SQLite bucket; not a production SLO or remote-store benchmark.",
                    "Memory observations are cached point samples; no arbitrary Python heap or private native pool bound.",
                    "Held Python requests suspend cooperatively for two seconds after all participants enter; no durable mutations during overload."]}
    output = args.output / "operating-defaults.json"
    try:
        for repetition in range(args.repetitions):
            order = hosts[repetition % len(hosts):] + hosts[:repetition % len(hosts)]
            for label, executable in order:
                root = args.output / f"{label}-{repetition}"
                root.mkdir()
                evidence = {"runtime": label, "repetition": repetition}
                document["cases"].append(evidence)
                save(output, document)
                run_case(executable, root, args, evidence)
                save(output, document)
                print(f"PASS {label} {repetition}: mixed load, default admission/body limits, killed-owner recovery and join", flush=True)
        document["status"] = "complete"
    except BaseException as error:
        document.update(status="failed", failure=f"{type(error).__name__}: {error}")
        raise
    finally:
        save(output, document)


if __name__ == "__main__":
    main()
