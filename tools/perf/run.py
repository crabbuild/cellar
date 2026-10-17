#!/usr/bin/env python3
"""Run the Rust cell/load binaries with RustFS on a local Colima Docker VM.

Requires Docker, Colima, and an already built embedded Cellar performance image.
Creates isolated ext4 loop files and Docker resources; cleans them up on exit.
Output includes raw load samples, cgroup statistics, state snapshots and logs.
"""

import argparse
import base64
import datetime
import hashlib
import hmac
import json
import pathlib
import secrets
import shlex
import subprocess
import threading
import time
import urllib.request
import urllib.error

SCENARIOS = (("one_node_bucket", 1, "bucket"), ("three_node_bucket", 3, "bucket"), ("three_node_fleet", 3, "fleet"))


def command(*args, check=True, **kwargs):
    result = subprocess.run(args, text=True, capture_output=True, **kwargs)
    if check and result.returncode:
        raise RuntimeError(f"{' '.join(args[:4])}: {result.stderr or result.stdout}")
    return result.stdout.strip()


def logs(name):
    result = subprocess.run(["docker", "logs", name], text=True, capture_output=True)
    return result.stdout + result.stderr


def event_report(text, event):
    reports = []
    for line in text.splitlines():
        if line.startswith("{"):
            value = json.loads(line)
            if value.get("event") == event:
                reports.append(value)
    return reports[0] if len(reports) == 1 else None


def shutdown_proof(text):
    return event_report(text, "embedded_node_stopped")


def validate_node_limits(text, resident, cached_routes, requests, log_level=None):
    proof = event_report(text, "embedded_node_started")
    expected = {"max_resident": resident, "max_cached_routes": cached_routes, "max_requests": requests}
    if not proof or any(type(proof.get(field)) is not int or proof[field] != value for field, value in expected.items()):
        raise RuntimeError("embedding application's captured limits do not match the requested workload")
    if log_level is not None and proof.get("log_level") != log_level:
        raise RuntimeError("embedding application's captured log level does not match the requested diagnostics")
    return proof


def validate_routing_cache(state, limit):
    count = state.get("cached_routes")
    if type(count) is not int or not 0 <= count <= limit:
        raise RuntimeError(f"missing, invalid or unbounded cached_routes: {count!r}, limit {limit}")


def routing_cache_checkpoints(scenarios):
    checkpoints = []
    for name, count, durability in SCENARIOS:
        if scenarios and name not in scenarios:
            continue
        phases = [("warm", range(count)), ("measured", range(count)),
                  ("recovered", range(count) if durability == "bucket" else range(1, count))]
        if count > 1:
            phases.extend((f"rolling-node{i + 1}-after-writes", range(count)) for i in range(count))
        phases.append(("preserve-reload", [count - 1]))
        for label, nodes in phases:
            for i in nodes:
                checkpoints.append({"scenario": name, "label": label, "node": i + 1,
                                    "file": f"{name}-{label}-node{i + 1}-state.json"})
    return checkpoints


def validate_shutdown(proof, exit_code, mode):
    if exit_code != 0 or not proof or proof.get("outcome") != "stopped" or proof.get("mode") != mode:
        raise RuntimeError("missing or failed embedded node shutdown outcome; see saved logs")
    if not all(proof.get(field) is True for field in (
            "ingress_drained", "application_work_drained", "cells_drained",
            "log_quiesced", "local_durability_closed", "caller_executors_alive")):
        raise RuntimeError("incomplete embedded node drain proof; see saved outcome")
    if any(proof.get(field) != 0 for field in ("pending_application_events", "pending_dispatches")) or any(
            proof.get(field) is not False for field in ("drain_timed_out", "deadline_elapsed")):
        raise RuntimeError("embedded node shutdown exhausted its budget or left work pending")
    if mode == "preserve" and proof.get("clean_reload_prepared") is not True:
        raise RuntimeError("preserving shutdown did not prepare an exact local reload")


def request(url, method="GET", timeout=5):
    with urllib.request.urlopen(urllib.request.Request(url, method=method), timeout=timeout) as response:
        return response.read().decode()


def create_bucket(endpoint, name, access, secret):
    # SigV4 CreateBucket using only the Python standard library.
    timestamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    date = timestamp[:8]
    host = endpoint.removeprefix("http://")
    payload_hash = hashlib.sha256(b"").hexdigest()
    signed = "host;x-amz-content-sha256;x-amz-date"
    canonical = f"PUT\n/{name}\n\nhost:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{timestamp}\n\n{signed}\n{payload_hash}"
    scope = f"{date}/us-east-1/s3/aws4_request"
    signing_input = f"AWS4-HMAC-SHA256\n{timestamp}\n{scope}\n{hashlib.sha256(canonical.encode()).hexdigest()}"
    key = ("AWS4" + secret).encode()
    for component in (date, "us-east-1", "s3", "aws4_request"):
        key = hmac.new(key, component.encode(), hashlib.sha256).digest()
    signature = hmac.new(key, signing_input.encode(), hashlib.sha256).hexdigest()
    headers = {
        "x-amz-content-sha256": payload_hash,
        "x-amz-date": timestamp,
        "Authorization": f"AWS4-HMAC-SHA256 Credential={access}/{scope}, SignedHeaders={signed}, Signature={signature}",
    }
    with urllib.request.urlopen(urllib.request.Request(endpoint + "/" + name, data=b"", headers=headers, method="PUT"), timeout=30) as response:
        return response.status


class Harness:
    def __init__(self, args, node_binary="cellar-perf-cells"):
        self.args = args
        self.node_binary = node_binary
        self.tag = "cellar-perf-" + datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d-%H%M%S") + "-" + secrets.token_hex(3)
        self.output = pathlib.Path(args.output or f".cellar/perf/{self.tag}").resolve()
        self.output.mkdir(parents=True, exist_ok=False)
        self.vm_root = "/mnt/lima-colima/" + self.tag
        self.access = "cellar-perf-" + secrets.token_hex(6)
        self.secret = secrets.token_hex(24)
        self.peer_key = base64.b64encode(secrets.token_bytes(32)).decode()
        self.network = self.tag
        self.store = self.tag + "-rustfs"
        self.nodes = [self.tag + f"-node{i}" for i in range(1, 4)]
        self.results = []
        self.recovery = []
        self.shutdown_outcomes = []
        self.node_policies = []
        self.routing_cache_snapshots = []
        self.failures = []
        self.stop_sampling = threading.Event()
        self.phase = "setup"
        self.sampler = None
        self.disks_created = False

    def disks(self):
        command("colima", "ssh", "--", "sudo", "mkdir", self.vm_root)
        self.disks_created = True
        script = f"""set -euo pipefail
for node in 1 2 3; do
  truncate -s {getattr(self.args, 'disk_gib', 5)}G {shlex.quote(self.vm_root)}/node${{node}}.img
  mkfs.ext4 -q -F -m 0 {shlex.quote(self.vm_root)}/node${{node}}.img
  mkdir {shlex.quote(self.vm_root)}/node${{node}}
  mount -o loop,noatime {shlex.quote(self.vm_root)}/node${{node}}.img {shlex.quote(self.vm_root)}/node${{node}}
done
mkdir {shlex.quote(self.vm_root)}/rustfs
chown 10001:10001 {shlex.quote(self.vm_root)}/rustfs
df -B1 {shlex.quote(self.vm_root)}/node{{1,2,3}}
"""
        output = command("colima", "ssh", "--", "sudo", "bash", "-s", input=script)
        (self.output / "disks.txt").write_text(output + "\n")

    def setup(self):
        self.disks()
        command("docker", "network", "create", self.network)
        command("docker", "run", "-d", "--name", self.store, "--network", self.network,
                "--network-alias", "rustfs", "--cpus", "2", "--cpuset-cpus", "4,5", "--memory", "4g", "--memory-swap", "4g",
                "--publish", f"127.0.0.1:{self.args.port}:9000", "--mount", f"type=bind,src={self.vm_root}/rustfs,dst=/data",
                "-e", f"RUSTFS_ACCESS_KEY={self.access}", "-e", f"RUSTFS_SECRET_KEY={self.secret}",
                "-e", "RUSTFS_CONSOLE_ENABLE=false", "-e", "RUSTFS_OBS_LOGGER_LEVEL=error", self.args.rustfs_image, "/data")
        self.wait_url(f"http://127.0.0.1:{self.args.port}/health")
        self.sampler = threading.Thread(target=self.sample, daemon=True)
        self.sampler.start()

    def wait_url(self, url, timeout=120):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                request(url)
                return
            except Exception:
                time.sleep(1)
        raise RuntimeError(f"readiness timeout: {url}")

    def sample(self):
        with (self.output / "resources.jsonl").open("w") as stream:
            while not self.stop_sampling.is_set():
                try:
                    running = set(command("docker", "ps", "--format", "{{.Names}}").splitlines())
                    targets = [name for name in (self.store, *self.nodes, self.tag + "-load") if name in running]
                    output = command("docker", "stats", "--no-stream", "--format", "{{json .}}", *targets, check=False) if targets else ""
                    for line in output.splitlines():
                        if line.startswith("{"):
                            stream.write(json.dumps({"timestamp": time.time(), "phase": self.phase, "stats": json.loads(line)}) + "\n")
                    stream.flush()
                except Exception as error:
                    stream.write(json.dumps({"error": str(error)}) + "\n")
                self.stop_sampling.wait(2)

    def start_nodes(self, scenario, count, durability):
        bucket = scenario.replace("_", "-")
        deadline = time.monotonic() + 60
        while True:
            try:
                create_bucket(f"http://127.0.0.1:{self.args.port}", bucket, self.access, self.secret)
                break
            except urllib.error.HTTPError as error:
                body = error.read().decode()
                if error.code != 503 or time.monotonic() >= deadline:
                    raise RuntimeError(f"CreateBucket HTTP {error.code}: {body}") from error
                time.sleep(1)
        for i in range(count):
            self.start_node(i, bucket, durability)
        for i in range(count):
            self.wait_node(scenario, i)
        limits = []
        for i in range(count):
            inspected = json.loads(command("docker", "inspect", self.nodes[i]))[0]
            limits.append({"node": i + 1, "host_config": {key: inspected["HostConfig"][key] for key in ("NanoCpus", "CpusetCpus", "Memory", "MemorySwap")}})
        (self.output / f"{scenario}-limits.json").write_text(json.dumps(limits, indent=2) + "\n")
        time.sleep(6)  # Membership publication before cell placement.

    def wait_node(self, scenario, i):
        self.wait_url(f"http://127.0.0.1:{self.args.port + 1 + i}/.well-known/cellar/health")
        proof = validate_node_limits(logs(self.nodes[i]), self.args.max_resident,
                                     self.args.max_cached_routes, self.args.max_requests,
                                     getattr(self.args, "log_level", None))
        self.node_policies.append({"scenario": scenario, "node": i + 1, "proof": proof})

    def start_node(self, i, bucket, durability):
        command("docker", "run", "-d", "--name", self.nodes[i], "--network", self.network,
                "--network-alias", f"node{i + 1}", "--cpus", "1", "--cpuset-cpus", str(i + 1),
                "--memory", "2g", "--memory-swap", "2g", "--ulimit", "nofile=65536:65536",
                "--publish", f"127.0.0.1:{self.args.port + 1 + i}:8080",
                "--publish", f"127.0.0.1:{self.args.port + 11 + i}:8081",
                "--mount", f"type=bind,src={self.vm_root}/node{i + 1},dst=/data",
                "-e", f"AWS_ACCESS_KEY_ID={self.access}", "-e", f"AWS_SECRET_ACCESS_KEY={self.secret}",
                "-e", f"CELLAR_PERF_NODE=node{i + 1}", "-e", "CELLAR_PERF_DATA_DIR=/data/state",
                "-e", "CELLAR_PERF_THREADS=1", "-e", f"CELLAR_PERF_PEER_KEY={self.peer_key}",
                "-e", f"CELLAR_PERF_MAX_RESIDENT={self.args.max_resident}",
                "-e", f"CELLAR_PERF_MAX_CACHED_ROUTES={self.args.max_cached_routes}",
                "-e", f"CELLAR_PERF_MAX_REQUESTS={self.args.max_requests}",
                "-e", f"CELLAR_PERF_SHUTDOWN_MS={self.args.shutdown_ms}", "-e", f"CELLAR_PERF_DURABILITY={durability}",
                "-e", f"CELLAR_PERF_LOG_LEVEL={getattr(self.args, 'log_level', 'warn')}",
                "--entrypoint", self.node_binary, self.args.image, "--bucket", bucket, "--endpoint", "http://rustfs:9000", "--region", "us-east-1",
                "--listen", "0.0.0.0:8080", "--internal-listen", "0.0.0.0:8081", "--advertise", f"node{i + 1}:8081")

    def snapshot(self, scenario, count, label, exclude=None, only=None):
        for i in range(count):
            if i == exclude or (only is not None and i != only):
                continue
            try:
                value = request(f"http://127.0.0.1:{self.args.port + 11 + i}/state", timeout=20)
                (self.output / f"{scenario}-{label}-node{i + 1}-state.json").write_text(value)
                state = json.loads(value)
                validate_routing_cache(state, self.args.max_cached_routes)
                self.routing_cache_snapshots.append({"scenario": scenario, "label": label, "node": i + 1,
                                                    "cached_routes": state["cached_routes"]})
                usage = command("docker", "exec", self.nodes[i], "sh", "-c", "df -B1 /data; du -sb /data/state; du -sb /data/state/*; cat /sys/fs/cgroup/cpu.stat; cat /sys/fs/cgroup/memory.peak; cat /sys/fs/cgroup/memory.events")
                (self.output / f"{scenario}-{label}-node{i + 1}-disk-cpu.txt").write_text(usage + "\n")
            except Exception as error:
                self.failures.append({"operation": "state_snapshot", "scenario": scenario,
                                      "label": f"{label}-node{i + 1}", "error": str(error)})
                raise RuntimeError(f"required state snapshot failed: {scenario}-{label}-node{i + 1}") from error

    def load(self, scenario, count, operation, concurrency, seconds, label, exclude=None, only=None):
        self.phase = f"{scenario}-{label}"
        urls = ",".join(f"http://node{i + 1}:8080" for i in range(count) if i != exclude and (only is None or i == only))
        arguments = ["docker", "run", "--rm", "--name", self.tag + "-load", "--network", self.network,
                         "--cpus", "2", "--cpuset-cpus", "0,6", "--memory", "2g", "--memory-swap", "2g",
                         "--mount", f"type=bind,src={self.output},dst=/results", "--entrypoint", "/usr/local/bin/cellar-perf-load",
                         self.args.image, operation, urls, str(self.args.cells), str(concurrency), str(seconds),
                         str(self.args.bytes), f"/results/{scenario}-accounting.json"]
        completed = subprocess.run(arguments, text=True, capture_output=True)
        stdout = completed.stdout.strip()
        (self.output / f"{scenario}-{label}-stderr.txt").write_text(completed.stderr)
        (self.output / f"{scenario}-{label}-stdout.txt").write_text(stdout + "\n")
        if not stdout:
            raise RuntimeError(f"load generator failed: {completed.stderr}")
        value = json.loads(stdout)
        value.update(scenario=scenario, label=label, nodes=count)
        (self.output / f"{scenario}-{label}.json").write_text(json.dumps(value, indent=2) + "\n")
        print(json.dumps(value), flush=True)
        if completed.returncode or value.get("errors") or value.get("mismatches"):
            self.failures.append(value)
            raise RuntimeError("load or recovery errors; see the saved measurement")
        return value

    def remove_node(self, i, label, graceful=False, mode="handoff"):
        exit_code = None
        if graceful:
            query = "?handoff=preserve" if mode == "preserve" else ""
            request(f"http://127.0.0.1:{self.args.port + 11 + i}/shutdown{query}", method="POST", timeout=5)
            exit_code = int(command("docker", "wait", self.nodes[i], timeout=self.args.shutdown_ms / 1000 + 15))
        else:
            command("docker", "kill", self.nodes[i], check=False)
        text = logs(self.nodes[i])
        (self.output / f"{label}-node{i + 1}.log").write_text(text)
        proof = None
        if graceful:
            proof = shutdown_proof(text)
            self.shutdown_outcomes.append({"label": label, "node": i + 1, "exit_code": exit_code, "proof": proof})
        command("docker", "rm", "-f", self.nodes[i], check=False)
        if graceful:
            validate_shutdown(proof, exit_code, mode)

    def clear_disks(self):
        # Only the fresh harness-owned state directories, never arbitrary paths.
        script = "set -e\n" + "\n".join(f"rm -rf -- {shlex.quote(self.vm_root + '/node' + str(i) + '/state')}" for i in range(1, 4))
        command("colima", "ssh", "--", "sudo", "bash", "-s", input=script)

    def scenario(self, name, count, durability):
        self.phase = name + "-boot"
        self.start_nodes(name, count, durability)
        self.load(name, count, "write", min(32, self.args.cells), self.args.warmup_seconds, "warmup")
        accounting = json.loads((self.output / f"{name}-accounting.json").read_text())
        if not all(writes > 0 for writes in accounting["writes"]):
            raise RuntimeError("warmup did not write every cell; increase warmup duration")
        self.load(name, count, "verify", 1, 1, "initial-verify")
        self.snapshot(name, count, "warm")
        for repeat in range(self.args.repeats):
            for concurrency in self.args.concurrency:
                self.results.append(self.load(name, count, "write", concurrency, self.args.seconds, f"c{concurrency}-r{repeat + 1}"))
        self.recovery.append(self.load(name, count, "verify", 1, 1, "before-recovery"))
        self.snapshot(name, count, "measured")
        if durability == "bucket":
            # SIGKILL all nodes and discard all local SQLite/log files. Every
            # acknowledged write must restore from the actual RustFS bucket.
            for i in range(count):
                self.remove_node(i, name + "-killed")
            self.clear_disks()
            time.sleep(2)
            for i in range(count):
                self.start_node(i, name.replace("_", "-"), durability)
            for i in range(count):
                self.wait_node(name, i)
            self.recovery.append(self.load(name, count, "verify", 1, 1, "bucket-restore"))
            self.snapshot(name, count, "recovered")
        else:
            self.remove_node(0, name + "-killed")
            self.recovery.append(self.load(name, count, "verify", 1, 1, "single-node-loss", exclude=0))
            self.snapshot(name, count, "recovered", exclude=0)
            self.start_node(0, name.replace("_", "-"), durability)
            self.wait_node(name, 0)
        # Restart each node while successors remain available. Verify the exact
        # acknowledged counters after each handoff and again after new writes.
        if count > 1:
            for i in range(count):
                self.remove_node(i, name + "-rolling", graceful=True)
                self.start_node(i, name.replace("_", "-"), durability)
                self.wait_node(name, i)
                time.sleep(6)
                self.recovery.append(self.load(name, count, "verify", 1, 1, f"rolling-node{i + 1}"))
                self.load(name, count, "write", min(32, self.args.cells), 5, f"rolling-node{i + 1}-writes")
                self.recovery.append(self.load(name, count, "verify", 1, 1, f"rolling-node{i + 1}-after-writes"))
                self.snapshot(name, count, f"rolling-node{i + 1}-after-writes")
        for i in range(count):
            # The last node has no successor. Exercise the SDK's retaining
            # stop instead of treating a timed-out handoff as successful.
            self.remove_node(i, name + "-drained", graceful=True, mode="preserve" if i == count - 1 else "handoff")
        # Consume the final node's retaining-stop certificate, rather than
        # merely checking that a marker was written. All acknowledgements must
        # still be present when that node is the fleet's only live process.
        last = count - 1
        self.start_node(last, name.replace("_", "-"), durability)
        self.wait_node(name, last)
        self.recovery.append(self.load(name, count, "verify", 1, 1, "preserve-reload", only=last))
        self.snapshot(name, count, "preserve-reload", only=last)
        self.remove_node(last, name + "-preserve-reloaded", graceful=True, mode="preserve")
        self.clear_disks()

    def cleanup(self):
        self.stop_sampling.set()
        if self.sampler:
            self.sampler.join(timeout=10)
        for node in [self.tag + "-load", *self.nodes]:
            (self.output / f"cleanup-{node}.log").write_text(logs(node))
            command("docker", "rm", "-f", node, check=False)
        (self.output / "rustfs.log").write_text(logs(self.store))
        command("docker", "rm", "-f", self.store, check=False)
        command("docker", "network", "rm", self.network, check=False)
        if self.disks_created:
            script = "set -e\n" + "\n".join(f"if mountpoint -q {shlex.quote(self.vm_root + '/node' + str(i))}; then umount {shlex.quote(self.vm_root + '/node' + str(i))}; fi" for i in range(1, 4))
            script += f"\nrm -rf -- {shlex.quote(self.vm_root)}\n"
            command("colima", "ssh", "--", "sudo", "bash", "-s", input=script, check=False)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", default="cellar-embedded-perf:local")
    parser.add_argument("--rustfs-image", default="rustfs/rustfs@sha256:1803faef57627e2d9c2e7d89d655d712ddded5389040054987163043fecb6a3c")
    parser.add_argument("--cells", type=int, default=200)
    parser.add_argument("--max-resident", type=int, default=256)
    parser.add_argument("--max-cached-routes", type=int, default=2048,
                        help="idle, unowned routing history per node; zero disables caching")
    parser.add_argument("--max-requests", type=int, default=512)
    parser.add_argument("--warmup-seconds", type=int, default=10)
    parser.add_argument("--shutdown-ms", type=int, default=30000)
    parser.add_argument("--log-level", choices=["warn", "info"], default="warn",
                        help="info retains capture/ship ledgers; use separate diagnostic runs")
    parser.add_argument("--bytes", type=int, default=1024)
    parser.add_argument("--seconds", type=int, default=20)
    parser.add_argument("--repeats", type=int, default=2)
    parser.add_argument("--concurrency", type=int, nargs="+", default=[1, 8, 32, 128, 200])
    parser.add_argument("--port", type=int, default=39000)
    parser.add_argument("--output")
    parser.add_argument("--scenario", choices=["one_node_bucket", "three_node_bucket", "three_node_fleet"], nargs="+")
    args = parser.parse_args()
    if any(value <= 0 for value in (args.cells, args.max_resident, args.max_requests, args.warmup_seconds, args.shutdown_ms, args.bytes, args.seconds, args.repeats)):
        parser.error("workload and node limits must be positive")
    if args.max_cached_routes < 0:
        parser.error("max-cached-routes must be nonnegative")
    if any(value <= 0 or value > args.cells for value in args.concurrency):
        parser.error("concurrency must be between 1 and the cell count")
    harness = Harness(args)
    image = json.loads(command("docker", "image", "inspect", args.image))[0]
    metadata = {
        "started_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "revision": command("git", "rev-parse", "HEAD"), "dirty": bool(command("git", "status", "--porcelain")),
        "arguments": vars(args), "node_limits": {"cpus": 1, "memory_bytes": 2147483648, "disk_image_bytes": 5368709120},
        "load_generator_limits": {"cpus": 2, "cpu_set": [0, 6], "memory_bytes": 2147483648},
        "rustfs_limits": {"cpus": 2, "cpu_set": [4, 5], "memory_bytes": 4294967296},
        "source_sha256": {name: hashlib.sha256(pathlib.Path(name).read_bytes()).hexdigest() for name in ("tools/perf/cells.rs", "tools/perf/deployment.rs", "tools/perf/embedded.rs", "tools/perf/load.rs", "tools/perf/run.py", "tools/perf/analyze.py", "tools/perf/Dockerfile", "Cargo.toml", "Cargo.lock")},
        "execution": "sdk::Node with caller-owned host/core runtimes, listeners, authenticated peer clients and object-store clients",
        "docker": json.loads(command("docker", "info", "--format", "{{json .}}")),
        "image": image["Id"],
        "image_labels": image["Config"].get("Labels"),
        "rustfs_image": json.loads(command("docker", "image", "inspect", args.rustfs_image))[0]["Id"],
    }
    (harness.output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    print(f"Artifacts: {harness.output}", flush=True)
    completed = False
    try:
        harness.setup()
        for name, count, durability in SCENARIOS:
            if not args.scenario or name in args.scenario:
                harness.scenario(name, count, durability)
        for checkpoint in routing_cache_checkpoints(args.scenario):
            validate_routing_cache(json.loads((harness.output / checkpoint["file"]).read_text()), args.max_cached_routes)
        completed = True
    finally:
        (harness.output / "results.json").write_text(json.dumps({"completed": completed, "measurements": harness.results, "recovery": harness.recovery, "shutdown_outcomes": harness.shutdown_outcomes, "node_policies": harness.node_policies, "routing_cache_snapshots": harness.routing_cache_snapshots, "failures": harness.failures}, indent=2) + "\n")
        harness.cleanup()


if __name__ == "__main__":
    main()
