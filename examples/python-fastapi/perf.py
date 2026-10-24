#!/usr/bin/env python3
"""Benchmark the real FastAPI order service using private local durable state.

Writes are submitted once, to distinct orders. Every acknowledged mutation is
read back. Raw attempts and generated reports belong outside the source tree.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import http.client
import json
import math
import os
from pathlib import Path
import platform
import signal
import statistics
import subprocess
import sys
import threading
import time
from urllib.parse import urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from smoke import Host, private_project

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "tools/python"))
import affinity

PAYLOAD = {"customer_id": "customer-perf", "items": [
    {"sku": "coffee", "quantity": 2, "unit_price_cents": 1250}]}
CREATE = json.dumps(PAYLOAD, separators=(",", ":")).encode()
UPDATE = b'{"expected_version":1,"status":"confirmed"}'


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def save(path, value):
    pending = path.with_suffix(".pending")
    pending.write_text(json.dumps(value, indent=2) + "\n")
    pending.replace(path)


class Connection:
    def __init__(self, origin):
        parsed = urlsplit(origin)
        require(parsed.scheme == "http" and parsed.hostname == "127.0.0.1", "private loopback origin required")
        self.http = http.client.HTTPConnection(parsed.hostname, parsed.port, timeout=20)

    def request(self, method, path, body=None, status=200):
        start = time.perf_counter_ns()
        connected = self.http.sock is not None
        self.http.request(method, path, body, {"content-type": "application/json"})
        response = self.http.getresponse()
        raw = response.read()
        elapsed = time.perf_counter_ns() - start
        require(response.status == status, f"{method} {path}: HTTP {response.status}: {raw[:512]!r}")
        return json.loads(raw), elapsed, connected

    def close(self):
        self.http.close()


def batch(origin, operations, concurrency=8):
    """Untimed preparation/verification, without mutation retries."""
    rows = [[] for _ in range(concurrency)]
    for index, operation in enumerate(operations):
        rows[index % concurrency].append(operation)

    def worker(operations):
        connection, values = Connection(origin), {}
        try:
            for method, path, body, status in operations:
                values[path] = connection.request(method, path, body, status)[0]
            return values
        finally:
            connection.close()

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        return {key: value for values in pool.map(worker, rows) for key, value in values.items()}


def process_sample(pid):
    fields = subprocess.check_output(["ps", "-o", "time=,rss=", "-p", str(pid)], text=True).split()
    parts = fields[0].split(":")
    return {"cpu_seconds": sum(float(part) * 60 ** power for power, part in enumerate(reversed(parts))),
            "rss_bytes": int(fields[1]) * 1024}


def executable_path(pid):
    if sys.platform.startswith("linux"):
        return Path(f"/proc/{pid}/exe").resolve(strict=True)
    return Path(subprocess.check_output(["ps", "-o", "comm=", "-p", str(pid)], text=True).strip())


def phase(origin, kind, concurrency, paths, seconds, native_pid, output):
    attempts = [[] for _ in range(concurrency)]
    start, stop = [0], threading.Event()
    boundary = {}

    def begin():
        boundary["native"] = process_sample(native_pid)
        boundary["client_cpu"] = time.process_time()
        start[0] = time.perf_counter_ns()

    barrier = threading.Barrier(concurrency + 1, action=begin)
    count = len(paths)

    def worker(index):
        connection = Connection(origin)
        try:
            connection.request("GET", "/health")  # Connect/warm outside timing.
            barrier.wait(timeout=30)
            ordinal = index
            while not stop.is_set():
                if kind == "read":
                    if time.perf_counter_ns() - start[0] >= seconds * 1e9:
                        break
                    path, body, method, status = paths[ordinal % count], None, "GET", 200
                else:
                    if ordinal >= count:
                        break
                    path = paths[ordinal]
                    body, method, status = (CREATE, "POST", 201) if kind == "create" else (UPDATE, "PATCH", 200)
                row = {"path": path, "outcome": "unknown"}
                attempts[index].append(row)
                value, latency, reused = connection.request(method, path, body, status)
                require(value["order_id"] == path.rsplit("/", 1)[1] and value["total_cents"] == 2500,
                        "acknowledged incorrect order identity or total")
                require(value["version"] == (2 if kind == "update" else 1), "acknowledged incorrect version")
                require(value["status"] == ("confirmed" if kind == "update" else "pending"), "acknowledged incorrect status")
                row.update(outcome="acknowledged", latency_ns=latency, connection_reused=reused)
                if kind != "read":
                    row["response"] = value
                ordinal += concurrency
            return time.perf_counter_ns()
        except BaseException as error:
            stop.set()
            barrier.abort()
            if attempts[index]:
                attempts[index][-1]["error"] = str(error)[:1024]
            raise
        finally:
            connection.close()

    try:
        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            futures = [pool.submit(worker, index) for index in range(concurrency)]
            barrier.wait(timeout=30)
            ends = [future.result() for future in futures]
    finally:
        save(output, {"kind": kind, "concurrency": concurrency, "attempts": attempts})
    wall = (max(ends) - start[0]) / 1e9
    after = process_sample(native_pid)
    rows = [row for worker_rows in attempts for row in worker_rows]
    require(rows and all(row["outcome"] == "acknowledged" for row in rows), "unacknowledged attempt")
    latencies = sorted(row["latency_ns"] for row in rows)
    result = {"kind": kind, "concurrency": concurrency, "requests": len(rows), "errors": 0,
              "seconds": wall, "requests_per_second": len(rows) / wall,
              "latency_ms": {name: latencies[min(len(rows) - 1, math.ceil(p * len(rows)) - 1)] / 1e6
                             for name, p in [("p50", .5), ("p95", .95), ("p99", .99)]},
              "mean_ms": statistics.mean(latencies) / 1e6, "max_ms": latencies[-1] / 1e6,
              "client_cpu_cores": (time.process_time() - boundary["client_cpu"]) / wall,
              "native_cpu_cores": (after["cpu_seconds"] - boundary["native"]["cpu_seconds"]) / wall,
              "native_rss_after_bytes": after["rss_bytes"],
              "reused_connections": sum(row["connection_reused"] for row in rows),
              "raw_attempts": output.name}
    print(json.dumps(result), flush=True)
    return result, {row["path"]: row["response"] for row in rows if "response" in row}


def stop_host(host):
    host.stop()
    require(host.child.returncode == 0, "server failed shutdown")
    reports = [json.loads(line[line.index('{'):])["report"] for line in host.log.read_text().splitlines()
               if '"event":"embedded_python_shutdown"' in line]
    require(reports, "missing combined shutdown report")
    report = reports[-1]
    require(not report["runtime_failed"] and not report["host_failed"] and
            report["pending_tasks"] == report["pending_threads"] == report["pending_objects"] == 0 and
            report["objects_created"] == report["objects_retired"] and
            report["bridge"]["pending_reservations"] == report["bridge"]["queued_bytes"] == 0 and
            report["native_outcomes"] and all(outcome["complete"] for outcome in report["native_outcomes"]),
            "server did not completely drain")
    return report


def start_host(args, directory, *, host_cpus=None):
    directory.mkdir()
    project = private_project(args.project, directory)
    evidence = directory / "host-affinity.json"
    environment = {"CELLAR_TOKIO_THREADS": str(args.rust_threads),
                   "CELLAR_DURABILITY": "bucket", "CELLAR_SHUTDOWN_TOTAL_MS": "15000",
                   "CELLAR_SHUTDOWN_REPORT_JSON": "1", "PYTHONDONTWRITEBYTECODE": "1"}
    if getattr(args, "resident_cells", None) is not None:
        environment["CELLAR_MAX_RESIDENT_CELLS"] = str(args.resident_cells)
    host = Host(args.cellar, project, directory, command_prefix=affinity.prefix(host_cpus, evidence),
                extra_env=environment)
    try:
        origin = host.ready()
        children = subprocess.check_output(["pgrep", "-P", str(host.child.pid)], text=True).split()
        require(len(children) == 1, "expected exactly one compiled native application child")
        pid = int(children[0])
        if host_cpus is not None:
            affinity.verify(evidence, host_cpus, host.child.pid)
            affinity.verify_threads(pid, host_cpus)
        return host, origin, pid
    except BaseException:
        host.child.terminate()
        host.child.wait(timeout=40)
        host.output.close()
        raise


def render(document):
    configuration = document["configuration"]
    lines = ["# FastAPI read/write benchmark", "",
             "Status: " + ("complete" if document["completed"] else "INCOMPLETE: " + document.get("failure", "run in progress")), "",
             f"Machine: {document['machine'].get('cpu_model', document['machine']['platform'])}; "
             f"{document['machine']['cpu_count']} CPUs. Build: {configuration['profile']}; "
             f"{configuration['rust_threads']} Rust worker threads. Repetitions: {configuration['repetitions']}.", "",
             "Local loopback, fixed concurrency, HTTP/1.1 keep-alive. "
             "Throughput aggregates acknowledged requests / timed wall; percentiles pool raw samples across repetitions.", "",
             "| Operation | Clients | Requests/s | p50 ms | p95 ms | p99 ms | Requests | Errors |",
             "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"]
    for concurrency in document["configuration"]["concurrency"]:
        for kind in ("read", "create", "update"):
            phases = [p for case in document["cases"] if case["concurrency"] == concurrency for p in case["phases"] if p["kind"] == kind]
            if not phases:
                continue
            latencies = []
            for p in phases:
                raw = json.loads((Path(document["output"]) / p["raw_attempts"]).read_text())
                latencies.extend(row["latency_ns"] / 1e6 for worker in raw["attempts"] for row in worker if row["outcome"] == "acknowledged")
            latencies.sort()
            count = sum(p["requests"] for p in phases)
            ps = [latencies[math.ceil(q * len(latencies)) - 1] for q in (.5, .95, .99)]
            rate = count / sum(p["seconds"] for p in phases)
            lines.append(f"| {kind} | {concurrency} | {rate:.1f} | {ps[0]:.2f} | {ps[1]:.2f} | {ps[2]:.2f} | {count} | 0 |")
    lines += ["", f"Each fresh server starts with 32 read targets and {configuration['writes_per_phase']} pending update targets. "
              "Setup and correctness verification are excluded from timing. Reads access the 32 resident targets. "
              "Creation writes always use new IDs and include "
              "cell/SQLite creation and ownership acquisition. Updates confirm distinct pre-created pending orders. "
              "Every acknowledged creation/update is read back; no writes are automatically retried.", "",
              "One application node, bucket durability backed by a private local SQLite development store; "
              "no network object store, follower quorum, TLS or CPU isolation. Client and server share the machine. "
              "These are local application measurements, not production capacity/SLOs.", "",
              "Configuration and machine/runtime identities are in `results.json`; per-request samples are in the adjacent JSON files.", ""]
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cellar", type=Path, required=True)
    parser.add_argument("--project", type=Path, required=True, help="prepared inventory-bound project; prefer profile lab/release")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--concurrency", type=int, nargs="+", default=[1, 8, 32])
    parser.add_argument("--repetitions", type=int, default=2)
    parser.add_argument("--read-seconds", type=float, default=8)
    parser.add_argument("--writes", type=int, default=512, help="distinct creations and pre-created updates per phase")
    parser.add_argument("--rust-threads", type=int, default=4)
    parser.add_argument("--keep-serving", action="store_true")
    args = parser.parse_args()
    require(all(1 <= n <= 32 for n in args.concurrency) and 1 <= args.repetitions <= 5 and
            args.read_seconds > 0 and 32 <= args.writes <= 1024 and args.rust_threads > 0, "invalid workload bounds")
    args.cellar, args.project, args.output = (p.resolve() for p in (args.cellar, args.project, args.output))
    args.output.mkdir()
    config = json.loads(args.project.read_text())
    inventory = Path(config["python_environment"]["inventory"])
    runtime_inventory = json.loads(inventory.read_text())
    document = {"format": 1, "completed": False, "output": str(args.output),
                "machine": {"platform": platform.platform(), "cpu_count": os.cpu_count(), "load_before": os.getloadavg(),
                    "cpu_model": subprocess.check_output(["sysctl", "-n", "machdep.cpu.brand_string"], text=True).strip()
                                 if sys.platform == "darwin" else platform.processor(),
                    "memory_bytes": os.sysconf("SC_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE")},
                "source": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
                "python_environment": {"version": runtime_inventory["version"], "soabi": runtime_inventory["soabi"],
                    "inventory_sha256": hashlib.sha256(inventory.read_bytes()).hexdigest(),
                    "source_sha256": runtime_inventory["source"]["sha256"]},
                "rustc": subprocess.check_output(["rustc", "--version"], text=True).strip(),
                "client": {"python": sys.version, "gil_enabled": getattr(sys, "_is_gil_enabled", lambda: True)()},
                "configuration": {"profile": config["profile"], "rust_threads": args.rust_threads,
                    "concurrency": args.concurrency, "repetitions": args.repetitions,
                    "read_seconds": args.read_seconds, "writes_per_phase": args.writes,
                    "durability": "bucket", "storage": "private local SQLite development bucket"}, "cases": []}
    target = args.output / "results.json"
    save(target, document)
    try:
        for repetition in range(args.repetitions):
            for concurrency in args.concurrency:
                label = f"r{repetition + 1}-c{concurrency}"
                host, origin, pid = start_host(args, args.output / label)
                case = {"repetition": repetition + 1, "concurrency": concurrency, "origin": origin,
                        "native_pid": pid, "phases": []}
                document["cases"].append(case)
                try:
                    executable = executable_path(pid)
                    with executable.open("rb") as stream:
                        case["executable"] = {"sha256": hashlib.file_digest(stream, "sha256").hexdigest(),
                                              "bytes": executable.stat().st_size}
                    connection = Connection(origin)
                    case["runtime"] = connection.request("GET", "/health")[0]
                    connection.close()
                    reads = ["/orders/read-" + str(i) for i in range(32)]
                    creates = ["/orders/create-" + str(i) for i in range(args.writes)]
                    updates = ["/orders/update-" + str(i) for i in range(args.writes)]
                    batch(origin, [("POST", path, CREATE, 201) for path in reads + updates])
                    for kind, paths in [("read", reads), ("create", creates), ("update", updates)]:
                        result, acknowledged = phase(origin, kind, concurrency, paths, args.read_seconds,
                                                      pid, args.output / f"{label}-{kind}.json")
                        case["phases"].append(result)
                        if acknowledged:
                            observed = batch(origin, [("GET", path, None, 200) for path in acknowledged])
                            require(observed == acknowledged, "readback differs from acknowledged writes")
                            result["verified_writes"] = len(observed)
                        save(target, document)
                finally:
                    case["shutdown"] = stop_host(host)
                    save(target, document)
        document["completed"] = True
        document["machine"]["load_after"] = os.getloadavg()
    except BaseException as error:
        document["failure"] = str(error)
        raise
    finally:
        save(target, document)
        (args.output / "report.md").write_text(render(document))
    print("BENCHMARK_COMPLETE " + str(args.output / "report.md"), flush=True)
    if args.keep_serving:
        host, origin, pid = start_host(args, args.output / "live-server")
        stopping = threading.Event()
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, lambda *_: stopping.set())
        try:
            batch(origin, [("POST", "/orders/demo", CREATE, 201)], 1)
            document["live_server"] = {"origin": origin, "native_pid": pid, "benchmark_pid": os.getpid(),
                                        "stop_command": f"kill -TERM {os.getpid()}"}
            save(target, document)
            print("SERVER_READY " + json.dumps(document["live_server"]), flush=True)
            while not stopping.wait(1):
                require(host.child.poll() is None, "live server exited")
        finally:
            shutdown = stop_host(host)
            document.setdefault("live_server", {})["shutdown"] = shutdown
            save(target, document)


if __name__ == "__main__":
    main()
