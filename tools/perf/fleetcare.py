#!/usr/bin/env python3
"""Build and run the FleetCare application on an isolated Colima/RustFS fleet."""
import argparse
import concurrent.futures
import datetime
import hashlib
import json
import os
import pathlib
import shlex
import statistics
import subprocess
import tempfile
import time
import urllib.request

from fleetcare_report import markdown
from run import Harness, command, create_bucket, event_report, logs, request, validate_routing_cache

ROOT = pathlib.Path(__file__).resolve().parents[2]
RUSTFS = "rustfs/rustfs@sha256:1803faef57627e2d9c2e7d89d655d712ddded5389040054987163043fecb6a3c"
SCHEMA = 2


def source_digest():
    paths = [ROOT / "Cargo.toml", ROOT / "Cargo.lock"]
    for directory in ("crates", "examples/fleetcare"):
        paths += sorted((ROOT / directory).rglob("*.rs"))
        paths += sorted((ROOT / directory).rglob("Cargo.toml"))
    paths += sorted((ROOT / "tools/perf").glob("*.py"))
    paths += [ROOT / "tools/perf/fleetcare.Dockerfile"]
    digest = hashlib.sha256()
    for path in paths:
        digest.update(str(path.relative_to(ROOT)).encode() + b"\0" + path.read_bytes() + b"\0")
    return digest.hexdigest()


def store_filesystem(text):
    try:
        values = [int(value) for value in text.splitlines()[-1].split()]
        if len(values) != 6:
            raise ValueError('expected six filesystem counters')
        size, used, available, inodes, used_inodes, available_inodes = values
        if (size <= 0 or inodes <= 0 or min(values) < 0 or used + available > size
                or used_inodes + available_inodes > inodes):
            raise ValueError('inconsistent filesystem counters')
    except (IndexError, ValueError) as error:
        raise RuntimeError('unavailable or invalid RustFS filesystem observation') from error
    return dict(zip(('size_bytes', 'used_bytes', 'available_bytes', 'inodes', 'used_inodes', 'available_inodes'), values))


def read_store_filesystem(path):
    return store_filesystem(command('colima', 'ssh', '--', 'df', '-B1',
                                   '--output=size,used,avail,itotal,iused,iavail', path))


def require_store_headroom(value, minimum_gib):
    if value['available_bytes'] < minimum_gib * 1024**3 or value['available_inodes'] < 1000:
        raise RuntimeError(f"RustFS needs at least {minimum_gib} GiB free before a case; "
                           f"available {value['available_bytes']/1024**3:.2f} GiB and {value['available_inodes']} inodes")


def pressure_policy(mode):
    total = 1792*1024**2
    return {'high_bytes':total//5*4 if mode == 'auto' else None,
            'rss_hard_bytes':total//100*95 if mode == 'auto' else None}


def validate_pressure(state, mode):
    if state.get('pressure_config') != pressure_policy(mode):
        raise RuntimeError('node pressure watermarks differ from the captured benchmark policy')


def validate_stop(text, mode, host, exit_code):
    proof = event_report(text, f"{host}_node_stopped")
    if exit_code != 0 or not proof or proof.get("outcome") != "stopped" or proof.get("mode") != mode:
        raise RuntimeError(f"missing or failed {host} shutdown outcome")
    for field in ("ingress_drained", "application_work_drained", "cells_drained", "log_quiesced", "local_durability_closed"):
        if proof.get(field) is not True:
            raise RuntimeError(f"incomplete {host} shutdown phase: {field}")
    if host == "embedded" and proof.get("caller_executors_alive") is not True:
        raise RuntimeError("embedding application did not retain usable caller executors")
    for field in ("pending_application_events", "pending_dispatches"):
        if type(proof.get(field)) is not int or proof[field] != 0:
            raise RuntimeError(f"pending shutdown work: {field}")
    if any(proof.get(k) is not False for k in ("drain_timed_out", "deadline_elapsed")):
        raise RuntimeError("shutdown exhausted its budget")
    if mode == "preserve" and proof.get("clean_reload_prepared") is not True:
        raise RuntimeError("preserving stop did not certify local reload")
    return proof


def quantile(histogram, fraction):
    count = sum(histogram.values())
    if count == 0:
        return None
    rank = max(1, int(__import__("math").ceil(count * fraction)))
    total = 0
    for value, n in sorted((int(k), v) for k, v in histogram.items()):
        total += n
        if total >= rank:
            return value / 1000
    raise ValueError("invalid histogram")


def summarize(report):
    operations = {}
    for name, metrics in report["operations"].items():
        operations[name] = {
            "attempts": metrics["attempts"], "sent": metrics["sent"], "successful": metrics["successful"],
            "successful_rps": metrics["successful"] / report["arrival_seconds"],
            "acknowledged_commands_rps": metrics["acknowledged_commands"] / report["arrival_seconds"],
            "success_fraction": metrics["successful"] / max(1, metrics["attempts"]),
            "p50_ms": quantile(metrics["latency_us"], .5),
            "p95_ms": quantile(metrics["latency_us"], .95),
            "p99_ms": quantile(metrics["latency_us"], .99),
            "service_p99_ms": quantile(metrics["service_us"], .99),
            "scheduling_debt_p99_ms": quantile(metrics["scheduling_debt_us"], .99),
            "successful_p99_ms": quantile(metrics.get("success_latency_us", {}), .99),
            "outcome_p99_ms": {outcome: quantile(hist, .99) for outcome, hist in metrics.get("outcome_latency_us", {}).items()},
            "p99_confident": metrics["successful"] >= 10000,
            "outcomes": metrics["outcomes"],
        }
    return operations


def compatibility(metadata):
    return {key: metadata[key] for key in ("schema_version", "workload_contract", "profile", "suite", "matrix", "policy", "resources", "generator", "rustfs_image", "architecture")}


def compare(current, baseline, latency_fraction=.2, throughput_fraction=.1):
    if not baseline.get("completed") or not current.get("completed"):
        raise ValueError("only complete suites can be compared")
    if compatibility(current["metadata"]) != compatibility(baseline["metadata"]):
        raise ValueError("incompatible baseline: workload, resources, dependency image or policy changed")
    def group(report):
        groups = {}
        for measurement in report["measurements"]:
            key = (measurement["host"], measurement["durability"], measurement["mix"], measurement["rate"], measurement["skew"])
            for operation, values in measurement["summary"].items():
                groups.setdefault((*key, operation), []).append(values)
        return groups
    now, before = group(current), group(baseline)
    if not now or set(now) != set(before) or any(len(now[k]) != len(before[k]) for k in now):
        raise ValueError("baseline measurement matrix is incomplete or differs")
    regressions = []
    for key in sorted(now):
        for metric, fraction, higher in (("p99_ms", latency_fraction, True), ("successful_rps", throughput_fraction, False)):
            if any(v[metric] is None for v in now[key] + before[key]):
                regressions.append({"case": list(key), "metric": metric, "invalid_capacity_measurement": True})
                continue
            actual = statistics.median(v[metric] for v in now[key])
            prior = statistics.median(v[metric] for v in before[key])
            failed = actual > prior * (1 + fraction) if higher else actual < prior * (1 - fraction)
            if failed:
                regressions.append({"case": list(key), "metric": metric, "baseline": prior, "actual": actual})
        if min(v["success_fraction"] for v in now[key]) < min(v["success_fraction"] for v in before[key]) - .001:
            regressions.append({"case": list(key), "metric": "success_fraction"})
        if any(v["scheduling_debt_p99_ms"] is None or v["scheduling_debt_p99_ms"] > 10 for v in now[key]):
            regressions.append({"case": list(key), "metric": "generator_scheduling_debt", "invalid_capacity_measurement": True})
    return regressions


class FleetCareHarness(Harness):
    def __init__(self, args):
        super().__init__(args)
        self.token = __import__("secrets").token_hex(24)
        self.host = "embedded"
        self.bucket = ""
        self.durability = "bucket"
        self.measurements = []
        self.checks = []
        self.ledgers = []
        self.store_scenario = None

    def setup(self):
        super().setup()
        self.simulator = self.tag + "-technician"
        command("docker", "run", "-d", "--name", self.simulator, "--network", self.network,
                "--network-alias", "technician", "--cpus", "1", "--cpuset-cpus", "7",
                "--memory", "256m", "--memory-swap", "256m", "--publish", f"127.0.0.1:{self.args.port+21}:8080", "--entrypoint", "python3",
                self.args.image, "/usr/local/bin/fleetcare-simulator.py")
        self.wait_url(f"http://127.0.0.1:{self.args.port+21}/health")
        self.store_ip = json.loads(command("docker","inspect",self.store))[0]["NetworkSettings"]["Networks"][self.network]["IPAddress"]
        self.simulator_ip = json.loads(command("docker","inspect",self.simulator))[0]["NetworkSettings"]["Networks"][self.network]["IPAddress"]

    def store_snapshot(self, scenario, label, require=False):
        value = read_store_filesystem(self.vm_root + '/rustfs')
        (self.output / f'{scenario}-{label}-store-filesystem.json').write_text(json.dumps(value, indent=2) + '\n')
        if require:
            require_store_headroom(value, self.args.minimum_store_free_gib)
        self.checks.append({'phase':f'{scenario}-{label}', 'action':'store_filesystem_observation',
                            'filesystem':value, 'passed':True})
        return value

    def _require_stopped(self):
        if self.vm_root != '/mnt/lima-colima/' + self.tag:
            raise RuntimeError('state reset requires the private benchmark directory')
        running = set(command('docker', 'ps', '--format', '{{.Names}}').splitlines())
        if running.intersection([*self.nodes, self.tag + '-load']):
            raise RuntimeError('cannot reset state with a live benchmark node or client')

    def clear_disks(self):
        self._require_stopped()
        super().clear_disks()
        # Deleting files frees ext4 blocks but leaves their sparse backing-image
        # extents allocated. Trim only these stopped, private loop filesystems.
        script = 'set -euo pipefail\n' + '\n'.join(
            'fstrim -v -- ' + shlex.quote(self.vm_root + f'/node{i}') for i in range(1, 4))
        output = command('colima', 'ssh', '--', 'sudo', 'bash', '-s', input=script)
        with (self.output / 'disk-trims.jsonl').open('a') as stream:
            stream.write(json.dumps({'phase':self.phase, 'trim':output}) + '\n')

    def _reset_store(self, scenario):
        self._require_stopped()
        (self.output / f'{self.store_scenario or "setup"}-rustfs.log').write_text(logs(self.store))
        command('docker', 'rm', '-f', self.store)
        # Only after the preceding case's receipts, recovery and typed stops
        # passed. A failure inside a case never enters a recovery-by-reset path.
        path = shlex.quote(self.vm_root + '/rustfs')
        command('colima', 'ssh', '--', 'sudo', 'bash', '-s', input=
                f'set -euo pipefail\nrm -rf -- {path}\nmkdir {path}\nchown 10001:10001 {path}\n')
        self.store_snapshot(scenario, 'start', require=True)
        command('docker', 'run', '-d', '--name', self.store, '--network', self.network,
                '--network-alias', 'rustfs', '--cpus', '2', '--cpuset-cpus', '4,5',
                '--memory', '4g', '--memory-swap', '4g', '--publish', f'127.0.0.1:{self.args.port}:9000',
                '--mount', f'type=bind,src={self.vm_root}/rustfs,dst=/data',
                '-e', f'RUSTFS_ACCESS_KEY={self.access}', '-e', f'RUSTFS_SECRET_KEY={self.secret}',
                '-e', 'RUSTFS_CONSOLE_ENABLE=false', '-e', 'RUSTFS_OBS_LOGGER_LEVEL=warn',
                '-e', 'RUSTFS_OBS_LOG_STDOUT_ENABLED=true', self.args.rustfs_image, '/data')
        self.wait_url(f'http://127.0.0.1:{self.args.port}/health')
        self.store_ip = json.loads(command('docker', 'inspect', self.store))[0]['NetworkSettings']['Networks'][self.network]['IPAddress']
        self.store_scenario = scenario
        self.checks.append({'phase':scenario, 'action':'fresh_object_store', 'passed':True})

    def cleanup(self):
        # A transient host/Docker failure must not skip every later removal or
        # turn ignored command exits into a successful cleanup report.
        for attempt in range(3):
            try:
                self._cleanup_resources()
                self._verify_cleanup()
                return
            except (OSError, RuntimeError):
                if attempt == 2:
                    raise
                time.sleep(1)

    def _verify_cleanup(self):
        names = command("docker", "ps", "-a", "--format", "{{.Names}}").splitlines()
        if any(name.startswith(self.tag + "-") for name in names):
            raise RuntimeError("private benchmark containers remain after cleanup")
        if self.network in command("docker", "network", "ls", "--format", "{{.Name}}").splitlines():
            raise RuntimeError("private benchmark network remains after cleanup")
        tables = set()
        for i in range(3):
            key = hashlib.sha256(f"node{i+1}\0state:{self.vm_root}/node{i+1}/state".encode()).hexdigest()
            tables.add(f"cellar_{key[:24]}")
            if command("docker", "ps", "-aq", "--filter", f"label=cellar.host={key}"):
                raise RuntimeError("private SDK child containers remain after cleanup")
            if command("docker", "network", "ls", "-q", "--filter", f"label=cellar.host={key}"):
                raise RuntimeError("private SDK child networks remain after cleanup")
        if hasattr(self.args, "sandbox_image"):
            state = json.loads(command("docker", "run", "--rm", "--name", self.tag + "-cleanup-verify",
                "--network", "host", "--cap-add", "NET_ADMIN", "--cpus", ".0625", "--memory", "16m",
                "--entrypoint", "nft", self.args.sandbox_image, "-j", "list", "tables"))
            if any(entry.get("table", {}).get("name") in tables for entry in state["nftables"]):
                raise RuntimeError("private SDK fence tables remain after cleanup")
        if self.disks_created:
            command("colima", "ssh", "--", "sudo", "test", "!", "-e", self.vm_root)

    def _cleanup_resources(self):
        if hasattr(self, "simulator"):
            (self.output / "technician.log").write_text(logs(self.simulator))
            command("docker", "rm", "-f", self.simulator, check=False)
        # SDK child networks and nft tables have identities derived from these
        # unique VM directories. Remove only this harness's resources.
        for i in range(3):
            key = hashlib.sha256(f"node{i+1}\0state:{self.vm_root}/node{i+1}/state".encode()).hexdigest()
            child_ids = command("docker","ps","-aq","--filter",f"label=cellar.host={key}").splitlines()
            if child_ids:
                command("docker","rm","-f",*child_ids,check=False)
            network_ids = command("docker","network","ls","-q","--filter",f"label=cellar.host={key}").splitlines()
            for identity in network_ids:
                command("docker","network","rm",identity,check=False)
            if hasattr(self.args,"sandbox_image"):
                command("docker","run","--rm","--network","host","--cap-add","NET_ADMIN", "--cpus",".0625","--memory","16m",
                    "--entrypoint","nft",self.args.sandbox_image,"delete","table","inet",f"cellar_{key[:24]}",check=False)
        super().cleanup()

    def start_node(self, i, bucket, durability):
        env = {
            "AWS_ACCESS_KEY_ID": self.access, "AWS_SECRET_ACCESS_KEY": self.secret,
            "CELLAR_NODE": f"node{i+1}", "FLEETCARE_TOKEN": self.token,
            "FLEETCARE_PROFILE": self.args.profile, "FLEETCARE_ASYNC": "1",
            "FLEETCARE_SIMULATOR": "http://technician:8080",
            "FLEETCARE_CONTAINER_IMAGE": self.args.sandbox_image, "FLEETCARE_FENCE_IMAGE": self.args.sandbox_image,
            "CELLAR_ASSET_CACHE_DIR": f"{self.vm_root}/node{i+1}/asset-cache",
            "CELLAR_PERF_DATA_DIR": f"{self.vm_root}/node{i+1}/state", "CELLAR_PERF_THREADS": "1",
            "CELLAR_PERF_PEER_KEY": self.peer_key,
            "CELLAR_PERF_MAX_RESIDENT": str(self.args.max_resident),
            "CELLAR_PERF_MAX_CACHED_ROUTES": str(self.args.max_cached_routes),
            "CELLAR_PERF_MAX_REQUESTS": str(self.args.max_requests),
            "CELLAR_PERF_SHUTDOWN_MS": str(self.args.shutdown_ms), "CELLAR_PERF_DURABILITY": durability,
            "CELLAR_PERF_WAKE_MS": str(self.args.wake_ms),
            "CELLAR_PERF_MEMORY_PRESSURE": self.args.memory_pressure,
            "CELLAR_TEST_DATA_DIR": f"{self.vm_root}/node{i+1}/state", "CELLAR_TOKIO_THREADS": "1",
            "CELLAR_MAX_RESIDENT_CELLS": str(self.args.max_resident),
            "CELLAR_MAX_CACHED_ROUTES": str(self.args.max_cached_routes),
            "CELLAR_MAX_REQUESTS": str(self.args.max_requests), "CELLAR_ACTIVATIONS": "64",
            "CELLAR_MAX_STATELESS_ISOLATES": "2", "CELLAR_MAX_CELL_REQUESTS": "16",
            "CELLAR_HANDLER_BUDGET_S": "30", "CELLAR_ADMISSION_WAIT_MS": "100",
            "CELLAR_OPERATION_DEADLINE_MS": "30000",
            "CELLAR_RELEASES": "32", "CELLAR_PLACEMENT_WEIGHT": "1",
            "CELLAR_PRESSURE_OWNERSHIP": "sticky", "CELLAR_WAKER_TICK_MS": str(self.args.wake_ms),
            "CELLAR_DURABILITY": durability, "CELLAR_SHUTDOWN_TOTAL_MS": str(self.args.shutdown_ms),
            "CELLAR_SHUTDOWN_REPORT_JSON": "1", "RUST_LOG": "warn",
        }
        if self.args.memory_pressure == 'off':
            env['CELLAR_MAX_RSS_MB'] = '0'
        arguments = ["docker", "run", "-d", "--name", self.nodes[i], "--network", "host",
                     "--add-host", f"rustfs:{self.store_ip}", "--add-host", f"technician:{self.simulator_ip}", "--cpus", "0.9375", "--cpuset-cpus", str(i+1),
                     "--memory", "1792m", "--memory-swap", "1792m", "--ulimit", "nofile=65536:65536",
                     "--mount", f"type=bind,src={self.vm_root}/node{i+1},dst={self.vm_root}/node{i+1}",
                     "--mount", f"type=bind,src={self.vm_root}/node{i+1},dst=/data",
                     "--mount", "type=bind,src=/var/run/docker.sock,dst=/var/run/docker.sock"]
        for key, value in env.items():
            arguments += ["-e", f"{key}={value}"]
        arguments += ["--entrypoint", f"fleetcare-{self.host}", self.args.image]
        arguments += ["--bucket", bucket, "--endpoint", "http://rustfs:9000", "--region", "us-east-1",
                      "--listen", f"0.0.0.0:{self.args.port+1+i}", "--internal-listen", f"0.0.0.0:{self.args.port+11+i}", "--advertise", f"127.0.0.1:{self.args.port+11+i}"]
        command(*arguments)

    def wait_node(self, scenario, i):
        deadline = time.monotonic() + 120
        while True:
            if command("docker", "inspect", "--format", "{{.State.Running}}", self.nodes[i]) != "true":
                (self.output / f"{scenario}-failed-start-node{i+1}.log").write_text(logs(self.nodes[i]))
                raise RuntimeError(f"node{i+1} exited during startup; see saved logs")
            try:
                request(f"http://127.0.0.1:{self.args.port+1+i}/.well-known/cellar/health")
                break
            except Exception:
                if time.monotonic() >= deadline:
                    raise RuntimeError(f"node{i+1} readiness timed out")
                time.sleep(1)
        req = urllib.request.Request(f"http://127.0.0.1:{self.args.port+1+i}/metadata", headers={"Authorization": f"Bearer {self.token}"})
        with urllib.request.urlopen(req, timeout=10) as response:
            metadata = json.load(response)
        if metadata["node"] != f"node{i+1}" or metadata["profile"] != self.args.profile:
            raise RuntimeError("application identity/profile mismatch")
        state = json.loads(request(f'http://127.0.0.1:{self.args.port+11+i}/state', timeout=20))
        validate_pressure(state, self.args.memory_pressure)

    def stop_node(self, i, label, mode="handoff"):
        query = "?handoff=preserve" if mode == "preserve" else ""
        request(f"http://127.0.0.1:{self.args.port+11+i}/shutdown{query}", method="POST", timeout=10)
        code = int(command("docker", "wait", self.nodes[i], timeout=self.args.shutdown_ms/1000+30))
        text = logs(self.nodes[i])
        (self.output / f"{label}-node{i+1}.log").write_text(text)
        proof = validate_stop(text, mode, self.host, code)
        self.shutdown_outcomes.append({"label": label, "host": self.host, "node": i+1, "exit_code": code, "proof": proof})
        command("docker", "rm", self.nodes[i])

    def client(self, action, phase, **values):
        self.phase = phase
        name = self.tag + "-load"
        directory = self.vm_root + "/results"
        command("colima", "ssh", "--", "sudo", "mkdir", "-p", directory)
        ledger = values.pop("ledger", phase + ".jsonl")
        config = {"action": action, "phase": phase, "urls": [f"http://node{i+1}:{self.args.port+1+i}" for i in range(3)],
                  "token": self.token, "manifest": f"/results/{self.host}-{self.durability}-manifest.json",
                  "ledger": f"/results/{ledger}", "report": f"/results/{phase}.json",
                  "concurrency": self.args.concurrency if action == "load" else min(self.args.concurrency, self.args.verification_concurrency),
                  "bytes": self.args.bytes, "seed": 20261012, **values}
        command("docker", "create", "--name", name, "--network", self.network,
                "--add-host","node1:host-gateway","--add-host","node2:host-gateway","--add-host","node3:host-gateway",
                "--cpus", "2", "--cpuset-cpus", "0,6", "--memory", "2g", "--memory-swap", "2g",
                "--mount", f"type=bind,src={directory},dst=/results",
                "--entrypoint", "fleetcare-load", self.args.image, "/tmp/config.json")
        try:
            with tempfile.NamedTemporaryFile(mode="w", suffix=".json") as stream:
                json.dump(config, stream)
                stream.flush()
                command("docker", "cp", stream.name, f"{name}:/tmp/config.json")
            command("docker", "start", name)
            timeout = values.get("seconds", 0) + 3600
            code = int(command("docker", "wait", name, timeout=timeout))
            (self.output / f"{phase}.log").write_text(logs(name))
            # Retrieve only this invocation's outputs. Recopying every prior
            # ledger on every receipt audit makes long matrices quadratic in
            # retained evidence and adds unrelated VM disk traffic.
            outputs = [phase + ".json"]
            if action in ("seed", "load", "attachment_seed", "probes"):
                outputs.append(ledger)
            if action == "seed":
                outputs.append(f"{self.host}-{self.durability}-manifest.json")
            for filename in outputs:
                command("docker", "cp", f"{name}:/results/{filename}", str(self.output / filename), check=not code)
            if code:
                detail = (self.output / f"{phase}.log").read_text().strip()[-512:]
                raise RuntimeError(f"client {phase} exited {code}: {detail}; see {phase}.log")
            report = json.loads((self.output / f"{phase}.json").read_text())
            self.checks.append({"phase": phase, "action": action, "passed": True})
            return report
        finally:
            command("docker", "rm", "-f", name, check=False)

    def receipt_audit(self, prefix):
        for index, ledger in enumerate(self.ledgers):
            self.client("receipts", f"{prefix}-receipts-{index}", ledger=ledger)

    def policy_snapshot(self, scenario, label, expected=None):
        values = {}
        for tenant in range(2 if self.args.profile == "smoke" else 20):
            url = f"http://127.0.0.1:{self.args.port+1+tenant%3}/system/kv/{tenant}/0"
            req = urllib.request.Request(url, headers={"Authorization": f"Bearer {self.token}"})
            with urllib.request.urlopen(req, timeout=60) as response:
                value = json.load(response)
            if not isinstance(value.get("policy"), dict) or not isinstance(value.get("metadata"), dict):
                raise RuntimeError(f"tenant {tenant} policy or metadata is missing at {label}")
            values[str(tenant)] = {key:value[key] for key in ("policy", "metadata")}
        (self.output / f"{scenario}-{label}-policies.json").write_text(json.dumps(values, indent=2) + "\n")
        if expected is not None and json.dumps(values, sort_keys=True) != json.dumps(expected, sort_keys=True):
            raise RuntimeError(f"latest KV policies or metadata changed across {label}")
        self.checks.append({"phase":f"{scenario}-{label}", "action":"kv_latest_value_recovery",
                            "tenants":len(values), "passed":True})
        return values

    def ownership_check(self, scenario, label):
        self.snapshot(scenario, 3, label)
        counts = [json.loads((self.output / f"{scenario}-{label}-node{i+1}-state.json").read_text())["owned_cells"] for i in range(3)]
        expected = 100 if self.args.profile == "smoke" else 10000
        if any(type(n) is not int or n <= 0 for n in counts) or sum(counts) != expected:
            raise RuntimeError(f"fleet ownership inventory differs from {expected}: {counts}")
        tolerance = .3 if expected == 100 else .05
        balanced = all(abs(n-expected/3) <= expected/3*tolerance for n in counts)
        self.checks.append({"phase": f"{scenario}-{label}", "action": "ownership_inventory",
                            "expected": expected, "nodes": counts, "balance_tolerance": tolerance,
                            "balanced": balanced, "passed": True})

    def fault_traffic(self, scenario, fault):
        phase = scenario + "-fault-" + fault
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(self.client, "load", phase, mix="core", skew="hot", rate=25, seconds=10)
            deadline = time.monotonic() + 30
            while command("docker", "inspect", "--format", "{{.State.Running}}", self.tag+"-load", check=False) != "true":
                if future.done():
                    future.result()
                    raise RuntimeError("fault traffic ended before injection")
                if time.monotonic() >= deadline:
                    raise RuntimeError("fault client did not start")
                time.sleep(.1)
            time.sleep(2)
            if fault == "crash":
                command("docker", "kill", self.nodes[1])
                code = int(command("docker", "wait", self.nodes[1]))
                (self.output / (phase+"-killed-node2.log")).write_text(logs(self.nodes[1]))
                if code != 137:
                    raise RuntimeError("process crash did not produce SIGKILL exit")
                command("docker", "rm", self.nodes[1])
                self.start_node(1, scenario, self.durability)
                self.wait_node(scenario, 1)
                self.checks.append({"phase":phase,"action":"process_crash_restart","exit_code":code,"passed":True})
            else:
                command("docker", "pause", self.store)
                try:
                    if command("docker", "inspect", "--format", "{{.State.Paused}}", self.store) != "true":
                        raise RuntimeError("RustFS was not paused")
                    time.sleep(3)
                finally:
                    command("docker", "unpause", self.store)
                self.wait_url(f"http://127.0.0.1:{self.args.port}/health")
                self.checks.append({"phase":phase,"action":"object_store_outage","seconds":3,"passed":True})
            report = future.result()
        self.ledgers.append(phase + ".jsonl")
        self.checks.append({"phase":phase,"action":"fault_traffic_outcomes","operations":summarize(report),"passed":True})
        # Always inspect original acknowledgements BEFORE any ambiguity replay.
        self.receipt_audit(phase + "-pre-replay")
        self.client("replay",phase+"-replay",ledger=phase+".jsonl")

    def scenario(self, host, durability):
        self.host, self.durability = host, durability
        scenario = f"{host}-{durability}"
        self.bucket = scenario
        self.ledgers = []
        request(f"http://127.0.0.1:{self.args.port+21}/reset", method="POST")
        self.clear_disks()
        self._reset_store(scenario)
        self.start_nodes(scenario, 3, durability)
        self.client("seed", scenario + "-seed")
        self.ledgers.append(scenario + "-seed.jsonl")
        self.client("audit", scenario + "-seed-audit")
        self.ownership_check(scenario, "seed")
        self.client("attachment_seed", scenario + "-attachment-seed")
        self.ledgers.append(scenario + "-attachment-seed.jsonl")
        self.client("probes", scenario + "-probes")
        self.ledgers.append(scenario + "-probes.jsonl")
        for mix in self.args.mixes:
            for skew in self.args.skews:
                for rate in self.args.rates:
                    warm = f"{scenario}-{mix}-{skew}-{rate}-warm"
                    self.client("load", warm, mix=mix, skew=skew, rate=rate, seconds=self.args.warmup_seconds)
                    self.ledgers.append(warm + ".jsonl")
                    for repeat in range(self.args.repeats):
                        phase = f"{scenario}-{mix}-{skew}-{rate}-r{repeat}"
                        report = self.client("load", phase, mix=mix, skew=skew, rate=rate, seconds=self.args.seconds)
                        self.ledgers.append(phase + ".jsonl")
                        if any(outcome.startswith("error:protocol") or outcome == "business_rejected"
                               for metrics in report["operations"].values() for outcome in metrics["outcomes"]):
                            raise RuntimeError("invalid protocol acknowledgement or unexpected business rejection in measured traffic")
                        self.measurements.append({"host": host, "durability": durability, "mix": mix,
                                                  "skew": skew, "rate": rate, "repeat": repeat, "phase": phase,
                                                  "summary": summarize(report)})
        self.snapshot(scenario, 3, "measured")
        self.store_snapshot(scenario, 'measured')
        proofs = [json.loads((self.output / f"{scenario}-measured-node{i+1}-state.json").read_text())["output_proofs"] for i in range(3)]
        if sum(p[durability] for p in proofs) <= 0:
            raise RuntimeError(f"no observed {durability} output barriers; requested policy was not exercised")
        self.checks.append({"phase": scenario+"-measured", "action":"output_proof_sources", "nodes":proofs, "passed":True})
        self.client("projections", scenario + "-projection-audit")
        self.receipt_audit(scenario + "-pre-replay")
        for index, ledger in enumerate(self.ledgers):
            self.client("replay", f"{scenario}-replay-{index}", ledger=ledger)
        self.client("audit", scenario + "-replay-audit")
        if self.args.suite == "standard":
            self.fault_traffic(scenario,"crash")
            self.fault_traffic(scenario,"store")
        policies = self.policy_snapshot(scenario, "before-reload")
        # Rolling preserving reload verifies actual reports and the same local
        # directories, then audits original acknowledgements after every stop.
        for i in range(3):
            self.stop_node(i, scenario + f"-preserve-{i}", "preserve")
            self.start_node(i, scenario, durability)
            self.wait_node(scenario, i)
            self.receipt_audit(scenario + f"-reload-{i}")
            self.policy_snapshot(scenario, f"reload-{i}", policies)
        self.client("audit", scenario + "-final-audit")
        self.client("projections", scenario + "-final-projection-audit")
        self.ownership_check(scenario, "reload")
        self.store_snapshot(scenario, 'reload')
        for i in range(3):
            # Preserve the balanced assignment before the storage-only restore.
            # A handoff here deliberately concentrates all cells on node 3;
            # verify those handoffs after the restore instead.
            mode = "handoff" if self.args.suite != "standard" and i < 2 else "preserve"
            self.stop_node(i, scenario + "-final", mode)
        if self.args.suite == "standard":
            self.clear_disks()
            for i in range(3):
                self.start_node(i,scenario,durability)
            for i in range(3):
                self.wait_node(scenario,i)
            self.policy_snapshot(scenario,"bucket-restore",policies)
            self.receipt_audit(scenario+"-bucket-restore-pre-replay")
            self.client("audit",scenario+"-bucket-restore-audit")
            self.client("projections",scenario+"-bucket-restore-projections")
            self.ownership_check(scenario,"bucket-restore")
            self.store_snapshot(scenario, 'bucket-restore')
            for i in range(3):
                self.stop_node(i,scenario+"-bucket-restore-final","handoff" if i < 2 else "preserve")
        self.store_snapshot(scenario, 'complete')


def main():
    os.chdir(ROOT)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=["smoke", "full"], default="full")
    parser.add_argument("--suite", choices=["smoke", "standard"], default="standard")
    parser.add_argument("--hosts", choices=["embedded", "daemon"], nargs="+", default=["embedded", "daemon"])
    parser.add_argument("--durability", choices=["bucket", "fleet"], nargs="+", default=["bucket", "fleet"])
    parser.add_argument("--rates", type=int, nargs="+")
    parser.add_argument("--mixes", choices=["read", "write", "core", "attachments", "capabilities"], nargs="+")
    parser.add_argument("--skews", choices=["uniform", "80-20", "hot"], nargs="+", default=["uniform"])
    parser.add_argument("--seconds", type=int)
    parser.add_argument("--warmup-seconds", type=int)
    parser.add_argument("--repeats", type=int)
    parser.add_argument("--concurrency", type=int, default=128)
    parser.add_argument("--verification-concurrency", type=int, default=8)
    parser.add_argument("--max-resident", type=int, default=512)
    parser.add_argument("--max-cached-routes", type=int, default=2048)
    parser.add_argument("--max-requests", type=int, default=128)
    parser.add_argument("--shutdown-ms", type=int, default=120000)
    parser.add_argument("--wake-ms", type=int, default=1000)
    parser.add_argument("--memory-pressure", choices=['off','auto'], default='off')
    parser.add_argument("--bytes", type=int, default=1024)
    parser.add_argument("--disk-gib", type=int, default=10)
    parser.add_argument("--minimum-store-free-gib", type=int, default=25)
    parser.add_argument("--port", type=int, default=39100)
    parser.add_argument("--rustfs-image", default=RUSTFS)
    parser.add_argument("--output")
    parser.add_argument("--image")
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument("--slo-p99-ms",type=float,default=1000)
    parser.add_argument("--minimum-success",type=float,default=.999)
    parser.add_argument("--baseline", type=pathlib.Path)
    parser.add_argument("--latency-regression", type=float, default=.2)
    parser.add_argument("--throughput-regression", type=float, default=.1)
    args = parser.parse_args()
    smoke = args.suite == "smoke"
    args.rates = args.rates or ([10] if smoke else [25,100,400])
    args.mixes = args.mixes or ["read", "write", "core", "attachments", "capabilities"]
    args.seconds = args.seconds if args.seconds is not None else (10 if smoke else 60)
    args.warmup_seconds = args.warmup_seconds if args.warmup_seconds is not None else (3 if smoke else 30)
    args.repeats = args.repeats if args.repeats is not None else (1 if smoke else 3)
    if any(v <= 0 for v in [*args.rates, args.seconds, args.warmup_seconds, args.repeats, args.concurrency,
                           args.max_resident, args.max_requests, args.shutdown_ms, args.wake_ms, args.bytes, args.disk_gib, args.verification_concurrency, args.minimum_store_free_gib]):
        parser.error("workload and resource limits must be positive")
    if args.max_cached_routes < 0 or args.bytes > 60000 or not 1 <= args.disk_gib <= 100:
        parser.error("invalid cache, payload or disk limit")
    for field in ("hosts", "durability", "rates", "mixes", "skews"):
        if len(getattr(args, field)) != len(set(getattr(args, field))):
            parser.error(f"duplicate {field} cases would reuse command IDs")
    if args.slo_p99_ms <= 0 or not 0 < args.minimum_success <= 1:
        parser.error("invalid qualification SLO")
    if not 0 <= args.latency_regression <= 1 or not 0 <= args.throughput_regression < 1:
        parser.error("regression tolerances must be fractions in [0,1) (latency may be 1)")
    if args.baseline:
        path = args.baseline / "results.json" if args.baseline.is_dir() else args.baseline
        baseline = json.loads(path.read_text())
        if not baseline.get("completed"):
            parser.error("baseline suite is incomplete")
    info = json.loads(command("docker", "info", "--format", "{{json .}}"))
    if command("docker", "context", "show") != "colima" or info["NCPU"] < 8 or info["MemTotal"] < 15*1024**3:
        parser.error("use Colima with at least 8 vCPUs and 16 GiB RAM")
    digest = source_digest()
    checkout = hashlib.sha256(str(ROOT).encode()).hexdigest()[:12]
    args.image = args.image or f"cellar-fleetcare:{checkout}-{digest[:12]}"
    harness = FleetCareHarness(args)
    print(f"Artifacts: {harness.output}", flush=True)
    completed = False
    metadata = {"schema_version": SCHEMA, "workload_contract": "fleetcare-v2", "source_digest": digest, "revision": command("git", "rev-parse", "HEAD"),
                "dirty": bool(command("git", "status", "--porcelain")), "profile": args.profile, "suite": args.suite,
                "matrix": {"hosts": args.hosts, "durability": args.durability, "rates": args.rates, "mixes": args.mixes,
                           "skews": args.skews, "seconds": args.seconds, "warmup_seconds": args.warmup_seconds, "repeats": args.repeats},
                "policy": {"max_resident": args.max_resident, "max_cached_routes": args.max_cached_routes,
                           "max_requests": args.max_requests, "shutdown_ms": args.shutdown_ms, "ownership": "sticky",
                           "stateless_workers": 2, "cell_requests": 16, "cells_per_worker": 32,
                           "handler_budget_seconds": 30, "operation_deadline_ms":30000,
                           "admission_wait_ms": 100, "activations": 64,
                           "evictions": 4, "releases": 32, "lease_ttl_ms": 10000, "wake_ms": args.wake_ms,
                           "store_case_isolation":"fresh_per_host_policy", "store_logger_level":"warn",
                           "local_disk_reclamation":"trim_after_clear",
                           "memory_pressure":{"mode":args.memory_pressure, **pressure_policy(args.memory_pressure)},
                           "minimum_store_free_bytes":args.minimum_store_free_gib*1024**3,
                           "queue_dlq":{"projection_repair":True,"max_retries":10,"retry_delay_seconds":5},
                           "store_transport":{"request_seconds":15,"connect_seconds":3,"retry_seconds":30,"data_retries":2,"cas_retries":0,"isolated_lease_pool":True}},
                "resources": {"nodes": 3, "topology":"colima-host-network-separate-cgroups", "node_cpus": 1, "node_memory": 2*1024**3, "application_cpus": .9375,"application_memory":1792*1024**2,
                              "child_cpus": .0625,"child_memory":256*1024**2,"max_children_per_node":1, "disk_bytes": args.disk_gib*1024**3,
                              "store_cpus": 2, "store_memory": 4*1024**3, "generator_cpus": 2, "generator_memory": 2*1024**3,
                              "simulator_cpus": 1, "simulator_memory": 256*1024**2,
                              "vm_cpus": info["NCPU"], "vm_memory": info["MemTotal"]},
                "generator": {"algorithm": "splitmix64-v1", "arrival": "fixed_rate", "seed": 20261012,
                              "max_in_flight": args.concurrency, "verification_concurrency":args.verification_concurrency, "payload_bytes": args.bytes},
                "architecture": info["Architecture"]}
    try:
        filesystem = read_store_filesystem('/mnt/lima-colima')
        (harness.output / 'store-filesystem-preflight.json').write_text(json.dumps(filesystem, indent=2) + '\n')
        metadata['resources']['store_filesystem_bytes'] = filesystem['size_bytes']
        metadata['resources']['store_filesystem_inodes'] = filesystem['inodes']
        require_store_headroom(filesystem, args.minimum_store_free_gib)
        if not args.no_build:
            with (harness.output / "build.log").open("w") as output:
                subprocess.run(["docker", "build", "-f", "tools/perf/fleetcare.Dockerfile", "-t", args.image,
                                "--build-arg", f"TARGET_CACHE_ID=cellar-fleetcare-{checkout}",
                                "--build-arg", f"SOURCE_DIGEST={digest}", "."], stdout=output, stderr=subprocess.STDOUT, check=True)
        image = json.loads(command("docker", "image", "inspect", args.image))[0]
        if image["Config"]["Labels"].get("io.cellar.fleetcare.source-digest") != digest:
            raise RuntimeError("image does not match current application/framework/client source")
        sandbox_tag = args.image + "-sandbox"
        if not args.no_build:
            with (harness.output / "sandbox-build.log").open("w") as output:
                subprocess.run(["docker", "build", "-f", "tools/perf/fleetcare.Dockerfile", "--target", "sandbox", "-t", sandbox_tag,
                    "--build-arg", f"TARGET_CACHE_ID=cellar-fleetcare-{checkout}", "--build-arg", f"SOURCE_DIGEST={digest}", "."],
                    stdout=output,stderr=subprocess.STDOUT,check=True)
        sandbox = json.loads(command("docker", "image", "inspect", sandbox_tag))[0]
        if sandbox["Config"]["Labels"].get("io.cellar.fleetcare.source-digest") != digest:
            raise RuntimeError("sandbox image does not match current source")
        args.sandbox_image = "cellar-image:" + sandbox["Id"].removeprefix("sha256:")
        command("docker", "tag", sandbox_tag, args.sandbox_image)
        metadata["sandbox_image"] = sandbox["Id"]
        if source_digest() != digest:
            raise RuntimeError("source changed while building; rerun against a stable source snapshot")
        if not command("docker", "image", "inspect", args.rustfs_image,check=False):
            command("docker", "pull", args.rustfs_image,timeout=600)
        metadata["image"] = image["Id"]
        metadata["rustfs_image"] = json.loads(command("docker", "image", "inspect", args.rustfs_image))[0]["Id"]
        (harness.output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
        if args.baseline and compatibility(metadata) != compatibility(baseline["metadata"]):
            raise ValueError("incompatible baseline: workload, resources, dependency image or policy changed")
        harness.setup()
        for host in args.hosts:
            for durability in args.durability:
                print(f"Running {host}/{durability} ({args.profile})", flush=True)
                harness.scenario(host, durability)
        completed = True
    except Exception as error:
        harness.failures.append({"operation":"suite", "type":type(error).__name__, "error":str(error)})
        if harness.disks_created:
            try:
                harness.store_snapshot(harness.bucket or 'setup', 'failure')
            except Exception as observation_error:
                harness.failures.append({'operation':'store_filesystem_observation', 'error':str(observation_error)})
        running = set(command('docker', 'ps', '--format', '{{.Names}}', check=False).splitlines())
        if harness.bucket and running.intersection(harness.nodes):
            for i in range(3):
                try:
                    harness.snapshot(harness.bucket, 3, "failure", only=i)
                except Exception:
                    # Capture surviving peers even when one node has exited.
                    # snapshot() records its own error; keep the suite failure.
                    pass
        raise
    finally:
        report = {"schema_version": SCHEMA, "completed": completed, "metadata": metadata,
                  "measurements": harness.measurements, "checks": harness.checks,
                  "shutdown_outcomes": harness.shutdown_outcomes, "failures": harness.failures}
        try:
            harness.cleanup()
            report["cleanup_completed"] = True
        except Exception as error:
            report["completed"] = False
            report["cleanup_completed"] = False
            harness.failures.append({"operation":"cleanup", "type":type(error).__name__, "error":str(error)})
        (harness.output / "results.json").write_text(json.dumps(report, indent=2) + "\n")
        (harness.output / "report.md").write_text(markdown(report,args.slo_p99_ms,args.minimum_success))
    if not report["completed"]:
        raise SystemExit("suite or cleanup failed; see results.json")
    if args.baseline:
        baseline = args.baseline / "results.json" if args.baseline.is_dir() else args.baseline
        regressions = compare(report, json.loads(baseline.read_text()), args.latency_regression, args.throughput_regression)
        (harness.output / "comparison.json").write_text(json.dumps({"regressions": regressions}, indent=2) + "\n")
        if regressions:
            raise SystemExit("performance regression or invalid generator measurement; see comparison.json")
    print(f"Completed: {harness.output / 'results.json'}", flush=True)


if __name__ == "__main__":
    main()
