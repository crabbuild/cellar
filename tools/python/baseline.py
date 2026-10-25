#!/usr/bin/env python3
# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Measure equivalent real-node Rust/Python operations and bucket-only takeover.

All mutations are sent once. Errors fail the run, retain raw attempt evidence,
and never establish rollback. Only read-only readiness probes repeat. The
caller provides already-built executables; this script installs no dependencies.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import http.client
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
from urllib.parse import urlsplit


def save(path, value):
    pending = path.with_suffix(path.suffix + ".pending")
    pending.write_text(json.dumps(value, indent=2) + "\n")
    pending.replace(path)


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def tool_version(command):
    try:
        result = subprocess.run(command, check=True, capture_output=True, text=True, timeout=10)
        return result.stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return None


class Connection:
    def __init__(self, origin):
        parsed = urlsplit(origin)
        require(parsed.hostname == "127.0.0.1" and parsed.scheme == "http", "private loopback host required")
        self.http = http.client.HTTPConnection(parsed.hostname, parsed.port, timeout=10)

    def request(self, method, route, body=b""):
        self.http.request(method, route, body, {"Content-Type": "application/octet-stream"})
        response = self.http.getresponse()
        payload = response.read()
        require(response.status == 200, f"{method} {route}: HTTP {response.status}: {payload[:1024]!r}")
        return payload

    def close(self):
        self.http.close()


class Host:
    def __init__(self, executable, mode, root, role, environment=None, *, command_prefix=()):
        self.root = root
        self.role = role
        self.started_ns = time.perf_counter_ns()
        for name in ("ready.json", "complete.json", "native-complete.json"):
            (root / name).unlink(missing_ok=True)
        self.log = (root / f"{role}.log").open("wb")
        self.process = subprocess.Popen([*command_prefix, str(executable), mode, str(root), role],
                                        stdout=self.log, stderr=self.log, env=environment)
        self.ready = None
        self.origin = None

    def readiness(self):
        deadline = time.monotonic() + 30
        last = None
        while time.monotonic() < deadline:
            require(self.process.poll() is None, f"{self.role} exited before readiness; see {self.root}")
            try:
                ready = json.loads((self.root / "ready.json").read_text())
                client = Connection(ready["url"])
                try:
                    json.loads(client.request("GET", "/native-observations"))
                finally:
                    client.close()
                self.ready, self.origin = ready, ready["url"]
                return time.perf_counter_ns() - self.started_ns
            except (OSError, ValueError, http.client.HTTPException, RuntimeError) as error:
                last = str(error)
                time.sleep(0.025)
        raise RuntimeError(f"{self.role} readiness elapsed: {last}")

    def observations(self):
        client = Connection(self.origin)
        try:
            result = json.loads(client.request("GET", "/native-observations"))
            memory = result["memory"]
            result["memory_sample_age_ns"] = result["observed_elapsed_ns"] - memory["sampled_elapsed_ns"]
            require(result["memory_sample_age_ns"] >= 0, "invalid memory sample timestamp")
            return result
        finally:
            client.close()

    def python_runtime(self):
        client = Connection(self.origin)
        try:
            value = json.loads(client.request("GET", "/runtime"))
            require(value["gil_enabled"] != value["free_threaded"], "unexpected GIL state for selected ABI")
            return value
        finally:
            client.close()

    def hard_kill(self):
        self.process.kill()
        self.process.wait(timeout=10)
        self.log.close()
        require(not (self.root / "complete.json").exists(), "hard-killed owner fabricated drain")

    def stop(self):
        client = Connection(self.origin)
        try:
            # Send once: ingress may close before this response arrives.
            try:
                client.request("POST", "/native-stop")
            except (OSError, http.client.HTTPException):
                pass
        finally:
            client.close()
        status = self.process.wait(timeout=20)
        self.log.close()
        require(status == 0, f"{self.role} incomplete stop ({status}); see {self.root}")
        report = json.loads((self.root / "complete.json").read_text())
        require(report["complete"] and report["native"]["complete"], "missing complete native drain")
        if report["python"] is not None:
            python = report["python"]
            require(not python["host_failed"] and not python["runtime_failed"] and
                    python["pending_tasks"] == python["pending_objects"] == python["pending_threads"] == 0 and
                    python["objects_created"] == python["objects_retired"] and
                    type(python["bridge"].get("native_invocations")) is int and python["bridge"]["native_invocations"] == 0 and
                    python["bridge"]["pending_reservations"] == python["bridge"]["queued_bytes"] == 0,
                    "missing joined Python cleanup")
        return report

    def cleanup(self):
        # A closed connection can precede the host's fence/proof diagnostic and
        # terminal status. Preserve those before killing a still-running failed
        # fixture. This sends no request and never retries a business mutation.
        before = self.process.poll()
        killed = False
        try:
            if before is None:
                try:
                    self.process.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    killed = True
                    self.process.kill()
                    self.process.wait(timeout=10)
        finally:
            self.log.close()
            save(self.root / f"{self.role}-exit.json", {
                "returncode_before_cleanup": before,
                "killed_by_cleanup": killed,
                "returncode": self.process.poll(),
            })


def phase(host, kind, size, concurrency, count, warmup, expected, attempts):
    before = host.observations()
    measured_start = [None]
    def start_measurement():
        measured_start[0] = time.perf_counter_ns()
    barrier = threading.Barrier(concurrency + 1, action=start_measurement)
    failed = threading.Event()
    failure_lock = threading.Lock()
    first_failure = []
    phase_name = f"{kind}-{size}-c{concurrency}"
    payload = b"x" * size

    def worker(index):
        connection = Connection(host.origin)
        local = []
        try:
            value = expected.get(index, 0)
            for ordinal in range(warmup + count):
                require(not failed.is_set(), "another measurement worker failed")
                if ordinal == warmup:
                    barrier.wait(timeout=30)
                record = {"worker": index, "ordinal": ordinal, "warmup": ordinal < warmup,
                          "phase": phase_name, "outcome": "unknown"}
                local.append(record)
                start = time.perf_counter_ns()
                body = connection.request("POST", "/echo" if kind == "echo" else f"/cells/worker-{index}", payload)
                record["latency_ns"] = time.perf_counter_ns() - start
                if kind == "echo":
                    require(body == payload, "echo response bytes changed")
                else:
                    value += 1
                    require(json.loads(body) == {"value": value}, f"counter acknowledged wrong value: {body!r}")
                record["outcome"] = "acknowledged"
                record["response_bytes"] = len(body)
            return local, value, time.perf_counter_ns()
        except BaseException as error:
            # Publish the cause before other workers observe the abort. Future
            # submission order is unrelated to which request actually failed.
            with failure_lock:
                if not first_failure:
                    first_failure.append(error)
                failed.set()
            if local and local[-1]["outcome"] == "unknown":
                local[-1]["error"] = {"type": type(error).__name__, "message": str(error)}
            barrier.abort()
            raise
        finally:
            # Retain even the failed/unknown mutation attempt. Never replay it.
            attempts[index] = local
            connection.close()

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [executor.submit(worker, index) for index in range(concurrency)]
        try:
            barrier.wait(timeout=30)
            results = [future.result() for future in futures]
        except BaseException:
            failed.set()
            barrier.abort()
            with failure_lock:
                cause = first_failure[0] if first_failure else None
            if cause is not None:
                raise cause
            raise
    # The action stamps the epoch before releasing any measurement worker.
    samples = [record for rows, _, _ in results for record in rows if not record["warmup"]]
    elapsed = max(finished for _, _, finished in results) - measured_start[0]
    if kind == "counter":
        expected.update({index: value for index, (_, value, _) in enumerate(results)})
    after = host.observations()
    latencies = sorted(record["latency_ns"] for record in samples)
    quantile = lambda q: latencies[max(0, math.ceil(q * len(latencies)) - 1)]
    copy_delta = None
    if after["runtime"]["bridge"] is not None:
        a, b = after["runtime"]["bridge"], before["runtime"]["bridge"]
        copy_delta = {key: a[key] - b[key] for key in ("input_copied_bytes", "output_copied_bytes")}
    return {"name": phase_name, "kind": kind, "payload_bytes": size, "concurrency": concurrency,
            "requests": len(samples), "warmup_per_worker": warmup, "elapsed_ns": elapsed,
            "requests_per_second": len(samples) * 1e9 / elapsed,
            "latency_ns": {"p50": quantile(.5), "p95": quantile(.95), "p99": quantile(.99), "max": max(latencies)},
            "before": before, "after": after, "bridge_copy_delta_including_warmup": copy_delta,
            "samples": samples}


def run_case(executable, mode, root, count, warmup, concurrency, profile, evidence):
    primary = recovered = None
    expected = {}
    evidence.update({"status": "running", "phases": [], "raw_attempts": {},
                     "load_average_before": list(os.getloadavg())})
    try:
        primary = Host(executable, mode, root, "primary")
        evidence["startup_ns"] = primary.readiness()
        evidence["host"] = primary.ready
        require(primary.ready["debug_assertions"] == (profile == "dev"), "binary assertions disagree with declared build profile")
        evidence["python_runtime"] = primary.python_runtime() if mode == "python" else None
        require(primary.ready["embed_compiled"] == (mode == "python"), "native reference must exclude embedded Python")
        for workers in sorted({1, concurrency}):
            for kind, size in (("echo", 0), ("echo", 4096), ("counter", 0)):
                attempts = evidence["raw_attempts"].setdefault(f"{kind}-{size}-c{workers}", {})
                evidence["phases"].append(phase(primary, kind, size, workers, count, warmup, expected, attempts))
        evidence["before_kill"] = primary.observations()
        if mode == "python":
            require(primary.python_runtime() == evidence["python_runtime"], "workload changed interpreter ABI/GIL state")
        killed_ns = time.perf_counter_ns()
        primary.hard_kill()
        require((root / "disk").is_dir(), "missing owner-local state for deletion drill")
        shutil.rmtree(root / "disk")
        require(not (root / "disk").exists(), "owner-local files were not removed")
        time.sleep(primary.ready["lease_ttl_ms"] / 1000 + .1)
        recovered = Host(executable, mode, root, "recovered")
        recovery = {"startup_ns": recovered.readiness(), "reads": [], "new_writes": []}
        evidence["recovery"] = recovery
        client = Connection(recovered.origin)
        try:
            for worker, value in sorted(expected.items()):
                start = time.perf_counter_ns()
                body = json.loads(client.request("GET", f"/cells/worker-{worker}"))
                latency = time.perf_counter_ns() - start
                require(body == {"value": value}, f"takeover lost acknowledged value: {body!r} != {value}")
                recovery["reads"].append({"worker": worker, "value": value, "latency_ns": latency})
            recovery["kill_to_all_verified_reads_ns"] = time.perf_counter_ns() - killed_ns
            for worker, value in sorted(expected.items()):
                start = time.perf_counter_ns()
                body = json.loads(client.request("POST", f"/cells/worker-{worker}"))
                latency = time.perf_counter_ns() - start
                require(body == {"value": value + 1}, "takeover refused a new write")
                recovery["new_writes"].append({"worker": worker, "value": value + 1, "latency_ns": latency})
        finally:
            client.close()
        recovery["observations"] = recovered.observations()
        if mode == "python":
            require(recovered.python_runtime() == evidence["python_runtime"], "recovered interpreter changed ABI/GIL state")
        evidence["shutdown"] = recovered.stop()
        evidence["load_average_after"] = list(os.getloadavg())
        evidence["status"] = "complete"
    finally:
        for host in (primary, recovered):
            if host is not None:
                host.cleanup()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--native-host", type=Path, required=True)
    parser.add_argument("--python-host", action="append", required=True, metavar="LABEL=PATH")
    parser.add_argument("--output", type=Path, required=True, help="new evidence directory; must not already exist")
    parser.add_argument("--build-profile", choices=("dev", "lab", "release"), required=True, help="record the profile used for every supplied executable")
    parser.add_argument("--count", type=int, default=100, help="measured requests per worker per phase")
    parser.add_argument("--warmup", type=int, default=10)
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--repetitions", type=int, default=3)
    args = parser.parse_args()
    require(1 <= args.count <= 100_000 and 1 <= args.warmup <= 1000 and
            1 <= args.concurrency <= 16 and 1 <= args.repetitions <= 20, "invalid finite workload limits")
    hosts = [("native", "native", args.native_host.resolve(strict=True))]
    for value in args.python_host:
        label, path = value.split("=", 1)
        require(label and all(c.isascii() and (c.isalnum() or c in "-_") for c in label), "invalid runtime label")
        hosts.append((label, "python", Path(path).resolve(strict=True)))
    require(len({label for label, _, _ in hosts}) == len(hosts), "duplicate runtime label")
    args.output.mkdir(parents=True, exist_ok=False)
    document = {"format": 1, "status": "running", "environment": {"platform": platform.platform(),
                "machine": platform.machine(), "cpu_count": os.cpu_count(), "client_python": sys.version, "client_threads": args.concurrency,
                "build_profile": args.build_profile, "caller_rustc": tool_version(["rustc", "-vV"]),
                "caller_cargo": tool_version(["cargo", "--version"]),
                "source_git_sha": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
                "source_diff_sha256": hashlib.sha256(subprocess.check_output(["git", "diff", "HEAD"])).hexdigest()},
                "workload": {"count": args.count, "warmup": args.warmup, "concurrency": args.concurrency, "repetitions": args.repetitions},
                "artifacts": [{"label": label, "mode": mode, "path": str(path),
                               "sha256": digest(path)} for label, mode, path in hosts],
                "cases": [], "limits": ["Closed-loop loopback client shares the development machine with the host.",
                "SQLite development bucket; no network-store or multi-node follower throughput claim.",
                "Bridge copy counters cover explicit Rust/Python UTF-8 boundaries, not total transport/codec/SQLite copies.",
                "Copy deltas include warmup; raw request latency/throughput samples exclude warmup.",
                "Memory values are cached point samples, not exact peaks or per-cell heap attribution.",
                "Recovery includes explicit lease-expiry wait and every verified cell read; it is not an application SLO."]}
    path = args.output / "baseline.json"
    try:
        for repetition in range(args.repetitions):
            # Rotate order to expose cache/order variation without overlapping
            # independent lease-sensitive node fixtures.
            order = hosts[repetition % len(hosts):] + hosts[:repetition % len(hosts)]
            for label, mode, executable in order:
                root = args.output / f"{label}-{repetition}"
                root.mkdir()
                evidence = {"runtime": label, "repetition": repetition}
                document["cases"].append(evidence)
                save(path, document)
                run_case(executable, mode, root, args.count, args.warmup, args.concurrency, args.build_profile, evidence)
                save(path, document)
                print(f"PASS {label} repetition {repetition}: measured, killed, restored and joined", flush=True)
        document["status"] = "complete"
    except BaseException as error:
        document["status"] = "failed"
        document["failure"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        save(path, document)


if __name__ == "__main__":
    main()
