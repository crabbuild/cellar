#!/usr/bin/env python3
# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Functional S3 recovery with local hosts; timings do not qualify fleet capacity.

Uses the existing fleet scenario and receipt checks, with a private, bounded
RustFS container and host processes instead of the eight-CPU perf environment.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import time

import affinity

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "perf"))
from run import (Harness, SCENARIOS, command, event_report, logs, request,
                 validate_node_limits, validate_python_outcomes, validate_python_shutdown,
                 validate_python_startup, validate_routing_cache, validate_shutdown)


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def validate_python_path(observed, requested):
    if observed.get("path_mode") != requested:
        raise RuntimeError("Python fleet path differs from the requested measurement")


class LocalFleet(Harness):
    def __init__(self, args):
        super().__init__(args)
        self.processes = {}
        self.ports = [(free_port(), free_port()) for _ in range(3)]
        self.generations = [0, 0, 0]

    def setup(self):
        # No shared volumes or credentials; the container owns disposable data.
        placement = self.args.cpu_placement
        cpus = ["--cpuset-cpus", ",".join(map(str, placement["store"]))] if placement else []
        quota = str(len(placement["store"])) if placement else "0.5"
        command("docker", "run", "-d", "--name", self.store,
                "--cpus", quota, *cpus, "--memory", "640m", "--memory-swap", "640m",
                "--publish", f"127.0.0.1:{self.args.port}:9000",
                "-e", f"RUSTFS_ACCESS_KEY={self.access}", "-e", f"RUSTFS_SECRET_KEY={self.secret}",
                "-e", "RUSTFS_CONSOLE_ENABLE=false", "-e", "RUSTFS_OBS_LOGGER_LEVEL=error",
                self.args.rustfs_image, "/data")
        self.wait_url(f"http://127.0.0.1:{self.args.port}/health")
        self.verify_placement("store-started")

    def verify_placement(self, label):
        placement = self.args.cpu_placement
        if not placement:
            return
        inspected = json.loads(command("docker", "inspect", self.store))[0]
        config, state = inspected["HostConfig"], inspected["State"]
        expected = {"CpusetCpus": ",".join(map(str, placement["store"])),
                    "NanoCpus": len(placement["store"]) * 1_000_000_000,
                    "Memory": 640 * 1024 * 1024, "MemorySwap": 640 * 1024 * 1024}
        if any(config.get(key) != value for key, value in expected.items()) or not state["Running"]:
            raise RuntimeError("object store resource limits changed")
        # Requires a local Linux Docker daemon: reject remote or VM-backed
        # daemon PIDs rather than assuming host and container CPU IDs coincide.
        container_id = inspected["Id"]
        cgroup = Path(f"/proc/{state['Pid']}/cgroup").read_text()
        if container_id not in cgroup:
            raise RuntimeError("CPU comparison requires the local Linux Docker daemon")
        proof = {"store_limits": expected, "store_id": container_id,
                 "store_threads": affinity.verify_threads(state["Pid"], placement["store"]),
                 "host_threads": {}}
        for i, (process, _) in self.processes.items():
            if process.poll() is not None:
                raise RuntimeError("measurement host exited before affinity observation")
            proof["host_threads"][str(i + 1)] = affinity.verify_threads(process.pid, placement["host"])
        (self.output / f"{label}-placement.json").write_text(json.dumps(proof, indent=2) + "\n")

    def start_nodes(self, scenario, count, durability):
        # Reuse bucket creation/retry policy without Docker process inspection.
        from run import create_bucket
        import urllib.error
        deadline = time.monotonic() + 60
        while True:
            try:
                create_bucket(f"http://127.0.0.1:{self.args.port}", scenario.replace("_", "-"),
                              self.access, self.secret)
                break
            except urllib.error.HTTPError as error:
                if error.code != 503 or time.monotonic() >= deadline:
                    raise
                time.sleep(1)
        for i in range(count):
            self.start_node(i, scenario.replace("_", "-"), durability)
        for i in range(count):
            self.wait_node(scenario, i)

    def start_node(self, i, bucket, durability):
        if i in self.processes:
            raise RuntimeError("previous node process was not joined")
        self.generations[i] += 1
        path = self.output / f"process-{i + 1}-{self.generations[i]}.log"
        env = dict(os.environ, AWS_ACCESS_KEY_ID=self.access, AWS_SECRET_ACCESS_KEY=self.secret,
                   CELLAR_PERF_NODE=f"node{i + 1}", CELLAR_PERF_DATA_DIR=str(self.output / f"state-{i + 1}"),
                   CELLAR_PERF_THREADS="1", CELLAR_PERF_PEER_KEY=self.peer_key,
                   CELLAR_PERF_MAX_RESIDENT=str(self.args.max_resident),
                   CELLAR_PERF_MAX_CACHED_ROUTES=str(self.args.max_cached_routes),
                   CELLAR_PERF_MAX_REQUESTS=str(self.args.max_requests),
                   CELLAR_PERF_SHUTDOWN_MS=str(self.args.shutdown_ms), CELLAR_PERF_DURABILITY=durability)
        env["CELLAR_PERF_PYTHON_PATH"] = self.args.python_path
        for key in ("CELLAR_PYTHON_INVENTORY", "CELLAR_PERF_LOG_LEVEL"):
            env.pop(key, None)
        with path.open("w") as stream:
            placement = self.args.cpu_placement
            prefix = affinity.prefix(placement["host"] if placement else None,
                                     path.with_suffix(".affinity.json"))
            process = subprocess.Popen([*prefix, str(self.args.host), "--bucket", bucket,
                "--endpoint", f"http://127.0.0.1:{self.args.port}", "--region", "us-east-1",
                "--listen", f"127.0.0.1:{self.ports[i][0]}",
                "--internal-listen", f"127.0.0.1:{self.ports[i][1]}",
                "--advertise", f"127.0.0.1:{self.ports[i][1]}"], env=env, stdout=stream, stderr=stream)
        self.processes[i] = (process, path)

    def wait_node(self, scenario, i):
        process, path = self.processes[i]
        deadline = time.monotonic() + 120
        while True:
            if process.poll() is not None:
                raise RuntimeError(f"node {i + 1} exited during startup; see {path.name}")
            try:
                request(f"http://127.0.0.1:{self.ports[i][0]}/.well-known/cellar/health")
                break
            except Exception:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.2)
        text = path.read_text()
        if self.args.cpu_placement:
            affinity.verify(path.with_suffix(".affinity.json"), self.args.cpu_placement["host"], process.pid)
            self.verify_placement(f"process-{i + 1}-{self.generations[i]}-started")
        policy = {"scenario": scenario, "node": i + 1, "generation": self.generations[i],
                  "proof": validate_node_limits(text, self.args.max_resident,
                                                self.args.max_cached_routes, self.args.max_requests)}
        if self.args.runtime == "python":
            policy["python"] = validate_python_startup(text, self.args.max_requests, self.args.shutdown_ms)
            validate_python_path(policy["python"], self.args.python_path)
            if any(previous.get("python") != policy["python"] for previous in self.node_policies):
                raise RuntimeError("Python ABI or admission limits changed across fleet processes")
        self.node_policies.append(policy)

    def remove_node(self, i, label, graceful=False, mode="handoff"):
        process, path = self.processes[i]
        if graceful:
            query = "?handoff=preserve" if mode == "preserve" else ""
            request(f"http://127.0.0.1:{self.ports[i][1]}/shutdown{query}", method="POST")
        else:
            if process.poll() is not None:
                raise RuntimeError("node exited before the planned kill")
            process.kill()
        code = process.wait(timeout=self.args.shutdown_ms / 1000 + 15)
        del self.processes[i]
        text = path.read_text()
        (self.output / f"{label}-node{i + 1}.log").write_text(text)
        if graceful:
            native = event_report(text, "embedded_node_stopped")
            outcome = {"label": label, "node": i + 1, "exit_code": code, "proof": native}
            if self.args.runtime == "python":
                outcome["python"] = event_report(text, "embedded_python_stopped")
            self.shutdown_outcomes.append(outcome)
            validate_shutdown(native, code, mode)
            if self.args.runtime == "python":
                validate_python_shutdown(outcome["python"], native)
        elif code != -9:
            raise RuntimeError(f"planned SIGKILL returned {code}")

    def load(self, scenario, count, operation, concurrency, seconds, label, exclude=None, only=None):
        urls = [f"http://127.0.0.1:{self.ports[i][0]}" for i in range(count)
                if i != exclude and (only is None or i == only)]
        self.verify_placement(f"{scenario}-{label}-before")
        placement = self.args.cpu_placement
        evidence = self.output / f"{scenario}-{label}.affinity.json"
        prefix = affinity.prefix(placement["client"] if placement else None, evidence)
        result = subprocess.run([*prefix, str(self.args.load), operation, ",".join(urls), str(self.args.cells),
            str(concurrency), str(seconds), str(self.args.bytes),
            str(self.output / f"{scenario}-accounting.json")], text=True, capture_output=True, timeout=240)
        (self.output / f"{scenario}-{label}.stderr").write_text(result.stderr)
        path = self.output / f"{scenario}-{label}.json"
        path.write_text(result.stdout)
        if result.returncode:
            raise RuntimeError(f"load failed; see {path.name} and stderr")
        if placement:
            affinity.verify(evidence, placement["client"])
        self.verify_placement(f"{scenario}-{label}-after")
        value = json.loads(result.stdout)
        if value.get("errors") or value.get("mismatches"):
            raise RuntimeError(f"load returned failures; see {path.name}")
        value.update(scenario=scenario, label=label, concurrency=concurrency)
        path.write_text(json.dumps(value, indent=2) + "\n")
        return value

    def snapshot(self, scenario, count, label, exclude=None, only=None):
        for i in range(count):
            if i == exclude or (only is not None and i != only):
                continue
            text = request(f"http://127.0.0.1:{self.ports[i][1]}/state", timeout=20)
            (self.output / f"{scenario}-{label}-node{i + 1}-state.json").write_text(text)
            validate_routing_cache(json.loads(text), self.args.max_cached_routes)

    def clear_disks(self):
        if self.processes:
            raise RuntimeError("cannot remove state while hosts are live")
        for i in range(3):
            path = self.output / f"state-{i + 1}"
            if path.exists():
                shutil.rmtree(path)

    def cleanup(self):
        for process, _ in self.processes.values():
            if process.poll() is None:
                process.kill()
            process.wait(timeout=15)
        self.processes.clear()
        (self.output / "rustfs.log").write_text(logs(self.store))
        (self.output / "rustfs-inspect.json").write_text(command("docker", "inspect", "--format",
            "{{json .State}}", self.store, check=False))
        command("docker", "rm", "-f", self.store, check=False)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", type=Path, required=True)
    parser.add_argument("--load", type=Path, required=True)
    parser.add_argument("--runtime", choices=["native", "python"], required=True)
    parser.add_argument("--python-path", choices=["bridge", "direct-initial"], default="bridge",
                        help="Python root/read bridge or native routing with initial storage snapshots")
    parser.add_argument("--rustfs-image", required=True, help="already installed disposable backend image")
    parser.add_argument("--output", type=Path, required=True, help="new evidence directory")
    parser.add_argument("--max-cached-routes", type=int, default=0)
    parser.add_argument("--host-cpus", help="Linux CPU pool shared by all host processes")
    parser.add_argument("--client-cpus", help="disjoint Linux CPU pool for the load generator")
    parser.add_argument("--store-cpus", help="disjoint Linux CPU pool for the local Docker object store")
    parser.add_argument("--cells", type=int, default=32)
    parser.add_argument("--max-resident", type=int, default=8)
    parser.add_argument("--max-requests", type=int, default=32)
    parser.add_argument("--seconds", type=int, default=3)
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--concurrency", type=int, nargs="+", default=[4])
    parser.add_argument("--read-measurements", action="store_true",
                        help="also measure GET requests against the acknowledged-write ledger")
    parser.add_argument("--scenario", nargs="+", choices=[s[0] for s in SCENARIOS], default=[s[0] for s in SCENARIOS])
    args = parser.parse_args()
    if args.runtime != "python" and args.python_path != "bridge":
        parser.error("--python-path requires --runtime python")
    args.cpu_placement = affinity.fleet_placement(args.host_cpus, args.client_cpus, args.store_cpus)
    args.host, args.load = args.host.resolve(strict=True), args.load.resolve(strict=True)
    args.port = free_port()
    if args.max_cached_routes < 0:
        parser.error("max-cached-routes must be nonnegative")
    if not (1 <= args.cells <= 4096 and 1 <= args.max_resident <= 4096
            and 1 <= args.max_requests <= 2048 and 1 <= args.seconds <= 120
            and 1 <= args.repeats <= 10 and len(set(args.concurrency)) == len(args.concurrency)
            and all(1 <= c <= min(args.cells, 512) for c in args.concurrency)):
        parser.error("invalid or unbounded workload")
    args.shutdown_ms, args.bytes, args.warmup_seconds = 30000, 256, 5
    harness = LocalFleet(args)
    source_root = Path(__file__).resolve().parents[2]
    sources = ("Cargo.toml", "Cargo.lock", "tools/perf/cells.rs", "tools/perf/load.rs", "tools/perf/deployment.rs",
               "tools/perf/embedded.rs", "tools/perf/run.py", "tools/python/fleet.rs",
               "tools/python/fleet_app.py", "tools/python/fleet.Dockerfile", "tools/python/fleet_smoke.py",
               "tools/python/affinity.py", "tools/python/fleet_gateway.rs")
    scope = ("CPU-separated closed-loop S3 measurement; shared host memory and disk; not production qualification"
             if args.cpu_placement else "functional S3 recovery; not a performance qualification")
    metadata = {"scope": scope,
                "revision": command("git", "rev-parse", "HEAD"),
                "dirty": bool(command("git", "status", "--porcelain")),
                "arguments": {k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()},
                "executables": {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in (args.host, args.load)},
                "source_sha256": {name: hashlib.sha256((source_root / name).read_bytes()).hexdigest()
                                  for name in sources},
                "rustfs_image": command("docker", "image", "inspect", "--format", "{{.Id}}", args.rustfs_image)}
    (harness.output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    completed = False
    try:
        harness.setup()
        for name, count, durability in SCENARIOS:
            if name in args.scenario:
                print(f"Starting {name}", flush=True)
                harness.scenario(name, count, durability)
        if args.runtime == "python":
            validate_python_outcomes(harness.output, harness.shutdown_outcomes, args.scenario,
                                     args.max_requests, args.shutdown_ms)
        completed = True
    finally:
        (harness.output / "results.json").write_text(json.dumps({"completed": completed,
            "measurements": harness.results, "recovery": harness.recovery,
            "shutdown_outcomes": harness.shutdown_outcomes,
            "node_policies": harness.node_policies}, indent=2) + "\n")
        harness.cleanup()
    print("PASS functional S3 recovery, rolling replacement and joined shutdown", flush=True)


if __name__ == "__main__":
    main()
