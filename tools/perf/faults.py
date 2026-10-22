#!/usr/bin/env python3
"""Check acknowledged and ambiguous operations across live process failures.

Use a private image built by tools/perf/Dockerfile. This is a correctness test,
not a throughput measurement. Resources belong to the reused private harness.
"""

import argparse
import concurrent.futures
import datetime
import hashlib
import http.client
import http.server
import json
import os
import pathlib
import re
import secrets
import socket
import subprocess
import threading
import time
import urllib.error
import urllib.request

from run import Harness, command, event_report, logs, shutdown_proof, validate_node_limits, validate_routing_cache, validate_shutdown


class JournalError(RuntimeError):
    pass


def validate_isolation(value):
    """A component failure is distinct from an observed authority loss."""
    proof = value.get("proof") or {}
    if (type(value.get("exit_code")) is not int or value["exit_code"] != 1
            or proof.get("caller_executors_alive") is not True
            or type(value.get("networks_before")) is not int or value["networks_before"] != 1
            or type(value.get("networks_after")) is not int or value["networks_after"] != 0):
        raise RuntimeError("missing network-isolation and owned-work join evidence")
    if proof.get("outcome") == "fenced":
        if type(proof.get("fence_code")) is not int or proof["fence_code"] != 3 or proof.get("reason") != "node_lease_expired":
            raise RuntimeError("isolated node did not report its actual expired-lease fence")
    elif proof.get("outcome") == "replication_failed":
        if proof.get("error_present") is not True or "reason" in proof or "fence_code" in proof:
            raise RuntimeError("replication failure fabricated an authority loss")
    else:
        raise RuntimeError("isolation caused an unexpected or unreported SDK outcome")


def validate_reply(operation, value):
    receipt = value.get("receipt")
    if (value.get("operation_id") != operation["operation_id"]
            or not isinstance(receipt, dict) or receipt.get("sha256") != operation["sha256"]
            or type(receipt.get("ordinal")) is not int or receipt["ordinal"] <= 0
            or value.get("node") not in ("node1", "node2", "node3")):
        raise RuntimeError("invalid operation acknowledgement")
    return receipt


def validate_snapshot(operations, state, final=False):
    """Run BEFORE replay: an acknowledged missing write must never be repaired."""
    issued = {op["operation_id"]: op for op in operations if op["attempts"]}
    receipts = state.get("receipts")
    writes = state.get("writes")
    if not isinstance(receipts, dict) or type(writes) is not int or writes != len(receipts):
        raise RuntimeError("counter does not equal distinct operation receipts")
    if not set(receipts) <= set(issued):
        raise RuntimeError("receipt for an operation the client never issued")
    required = {key for key, op in issued.items() if final or op.get("ack") or op.get("upstream_ack")}
    if not required <= set(receipts):
        raise RuntimeError("lost acknowledged operation before replay")
    ordinals = []
    for key, receipt in receipts.items():
        op = issued[key]
        if (not isinstance(receipt, dict) or receipt.get("sha256") != op["sha256"]
                or type(receipt.get("ordinal")) is not int or receipt["ordinal"] <= 0):
            raise RuntimeError("receipt payload or ordinal mismatch")
        for proof in (op.get("ack"), op.get("upstream_ack"), op.get("observed")):
            if proof is not None and proof != receipt:
                raise RuntimeError("original receipt changed during retry or recovery")
        ordinals.append(receipt["ordinal"])
    if sorted(ordinals) != list(range(1, writes + 1)):
        raise RuntimeError("duplicate or missing application counter increment")
    return {"issued": len(issued), "client_acknowledged": sum(bool(op.get("ack")) for op in issued.values()),
            "ambiguous_present": sum(not op.get("ack") for key, op in issued.items() if key in receipts),
            "ambiguous_absent": len(set(issued) - set(receipts)), "writes": writes}


class Ledger:
    """The measuring client owns this fsynced operation and outcome journal."""
    def __init__(self, path, cells, burst, burst_cell=None):
        self.path = path
        self.lock = threading.Lock()
        self.pending = set()
        self.fatal = []
        nonce = secrets.token_hex(8)
        self.operations = []
        for phase, count in (("warm", cells), ("lost-response", 1), ("burst", burst)):
            for i in range(count):
                operation_id = f"{nonce}-{phase}-{i:04}"
                payload = (hashlib.sha256(operation_id.encode()).hexdigest() * 16)
                cell = burst_cell if phase == "burst" and burst_cell is not None else i % cells
                self.operations.append({"operation_id": operation_id, "cell": cell,
                    "phase": phase, "payload": payload, "sha256": hashlib.sha256(payload.encode()).hexdigest(),
                    "attempts": 0, "ack": None, "upstream_ack": None, "observed": None})
        self.stream = path.with_suffix(".jsonl").open("x")
        self._append("plan", operations=self.operations)

    def _append(self, event, **fields):
        try:
            self.stream.write(json.dumps(dict(event=event, timestamp=time.time(), **fields)) + "\n")
            self.stream.flush()
            os.fsync(self.stream.fileno())
        except OSError as error:
            raise JournalError("measuring client journal could not be synchronized") from error

    def attempt(self, operation, url, hold_ms=0):
        key = operation["operation_id"]
        with self.lock:
            operation["attempts"] += 1
            self._append("attempt", operation_id=key, url=url, hold_ms=hold_ms, attempt=operation["attempts"])
            self.pending.add(key)
        try:
            body = json.dumps({"operation_id": key, "payload": operation["payload"], "hold_ms": hold_ms}).encode()
            req = urllib.request.Request(url, body, {"Content-Type": "application/json"}, method="POST")
            with urllib.request.urlopen(req, timeout=20) as response:
                value = json.loads(response.read())
            receipt = validate_reply(operation, value)
            with self.lock:
                for previous in (operation["ack"], operation["upstream_ack"], operation["observed"]):
                    if previous is not None and previous != receipt:
                        raise RuntimeError("retry changed original operation receipt")
                self._append("ack", operation_id=key, receipt=receipt, node=value["node"])
                operation["ack"] = receipt
                operation["ack_node"] = value["node"]
            return True
        except urllib.error.HTTPError as error:
            if error.code not in (408, 429, 500, 502, 503, 504):
                self.protocol_failure(key, error)
                raise RuntimeError("non-retryable operation failure") from error
            else:
                self.ambiguous(key, f"HTTP {error.code}")
            return False
        except (urllib.error.URLError, http.client.HTTPException, OSError) as error:
            self.ambiguous(key, str(error))
            return False
        except Exception as error:
            self.protocol_failure(key, error)
            raise RuntimeError("invalid operation acknowledgement") from error
        finally:
            with self.lock:
                self.pending.discard(key)

    def ambiguous(self, key, error):
        with self.lock:
            self._append("ambiguous", operation_id=key, error=error[:256])

    def protocol_failure(self, key, error):
        with self.lock:
            self.fatal.append(str(error))
            self._append("protocol_failure", operation_id=key, error=str(error)[:256])

    def save(self):
        with self.lock:
            self.path.write_text(json.dumps(self.operations, indent=2) + "\n")
            if self.fatal:
                raise RuntimeError(f"invalid workload response: {self.fatal[0]}")

    def close(self):
        self.stream.close()


class LostResponse:
    """Forward a write, consume its SDK response, then drop the client socket."""
    def __init__(self, ledger, operation, upstream):
        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                try:
                    body = self.rfile.read(int(self.headers["Content-Length"]))
                    request = urllib.request.Request(upstream, body, {"Content-Type": "application/json"}, method="POST")
                    with urllib.request.urlopen(request, timeout=20) as response:
                        value = json.loads(response.read())
                    receipt = validate_reply(operation, value)
                    with ledger.lock:
                        ledger._append("upstream_ack_response_dropped", operation_id=operation["operation_id"], receipt=receipt)
                        operation["upstream_ack"] = receipt
                except Exception as error:
                    ledger.protocol_failure(operation["operation_id"], error)
                finally:
                    self.close_connection = True
                    self.connection.shutdown(socket.SHUT_RDWR)

            def log_message(self, *_):
                pass

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def __enter__(self):
        self.thread.start()
        return f"http://127.0.0.1:{self.server.server_port}/drop-response"

    def __exit__(self, *_):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)


class FaultHarness(Harness):
    def __init__(self, args):
        super().__init__(args, node_binary="cellar-perf-fault-cells")
        self.verifications = []
        self.faults = []

    def wait_node(self, scenario, i):
        super().wait_node(scenario, i)
        if event_report(logs(self.nodes[i]), "fault_receipt_protocol") != {"event": "fault_receipt_protocol", "version": 1}:
            raise RuntimeError("image does not implement the operation receipt protocol")

    def url(self, node, cell):
        return f"http://127.0.0.1:{self.args.port + 1 + node}/cells/{cell:04}"

    def verify(self, scenario, label, ledger, nodes, final=False):
        self.phase = f"{scenario}-{label}"
        ledger.save()
        (self.output / f"{scenario}-{label}-operations.json").write_text(json.dumps(ledger.operations, indent=2) + "\n")
        totals = {k: 0 for k in ("issued", "client_acknowledged", "ambiguous_present", "ambiguous_absent", "writes")}
        states = []
        evidence = self.output / f"{scenario}-{label}-states.jsonl"
        for cell in range(self.args.cells):
            deadline = time.monotonic() + 120
            while True:
                try:
                    with urllib.request.urlopen(self.url(nodes[cell % len(nodes)], cell), timeout=10) as response:
                        state = json.loads(response.read())
                    break
                except urllib.error.HTTPError as error:
                    if error.code not in (408, 429, 500, 502, 503, 504) or time.monotonic() >= deadline:
                        raise
                    time.sleep(0.1)
                except (urllib.error.URLError, http.client.HTTPException, OSError):
                    if time.monotonic() >= deadline:
                        raise
                    time.sleep(0.1)
            operations = [op for op in ledger.operations if op["cell"] == cell]
            with evidence.open("a") as stream:
                stream.write(json.dumps(dict(cell=cell, **state)) + "\n")
            if state.get("node") not in ("node1", "node2", "node3"):
                raise RuntimeError("state response does not identify its owner")
            observed = validate_snapshot(operations, state, final)
            for op in operations:
                if receipt := state["receipts"].get(op["operation_id"]):
                    op["observed"] = receipt
            for key, value in observed.items():
                totals[key] += value
            states.append(dict(cell=cell, **state))
        value = dict(scenario=scenario, label=label, final=final, cells=self.args.cells, **totals)
        (self.output / f"{scenario}-{label}.json").write_text(json.dumps(dict(value, states=states), indent=2) + "\n")
        self.verifications.append(value)
        ledger.save()
        print(json.dumps(value), flush=True)

    def crash(self, i, scenario):
        started = time.monotonic()
        command("docker", "kill", self.nodes[i], timeout=10)
        state = json.loads(command("docker", "inspect", "--format", "{{json .State}}", self.nodes[i]))
        if state["Running"] or state["ExitCode"] != 137 or state["OOMKilled"]:
            raise RuntimeError("requested process kill was not observed")
        (self.output / f"{scenario}-killed-node{i + 1}.log").write_text(logs(self.nodes[i]))
        command("docker", "rm", self.nodes[i])
        return {"node": i + 1, "exit_code": state["ExitCode"], "finished_at": state["FinishedAt"],
                "elapsed_s": time.monotonic() - started}

    def isolate(self, i, scenario, burst_started):
        before = json.loads(command("docker", "inspect", "--format", "{{json .NetworkSettings.Networks}}", self.nodes[i]))
        if set(before) != {self.network}:
            raise RuntimeError("isolation requires the node's sole private network")
        command("docker", "network", "disconnect", "--force", self.network, self.nodes[i], timeout=10)
        after = json.loads(command("docker", "inspect", "--format", "{{json .NetworkSettings.Networks}}", self.nodes[i]))
        detached = time.monotonic()
        disconnect_window = detached - burst_started
        if disconnect_window >= 30:
            raise RuntimeError("node was not detached inside the accepted handler's hold window")
        value = {"node": i + 1, "networks_before": len(before), "networks_after": len(after),
                 "burst_to_disconnected_s": disconnect_window}
        evidence = self.output / f"{scenario}-isolation.json"
        evidence.write_text(json.dumps(value, indent=2) + "\n")
        self.phase = f"{scenario}-waiting-for-sdk-outcome"
        exit_code = int(command("docker", "wait", self.nodes[i], timeout=120))
        text = logs(self.nodes[i])
        (self.output / f"{scenario}-isolated-node{i + 1}.log").write_text(text)
        value.update(exit_code=exit_code, proof=shutdown_proof(text), outcome_after_disconnect_s=time.monotonic() - detached)
        evidence.write_text(json.dumps(value, indent=2) + "\n")
        validate_isolation(value)
        command("docker", "rm", self.nodes[i])
        return value

    def scenario(self, kind):
        if kind == "peer-partition":
            from peer_partition import scenario
            return scenario(self)
        durability = "bucket" if kind == "bucket" else "fleet"
        scenario = f"inflight-{kind}"
        self.start_nodes(scenario, 3, durability)
        ledger = Ledger(self.output / f"{scenario}-operations.json", self.args.cells, self.args.concurrency)
        try:
            warm = [op for op in ledger.operations if op["phase"] == "warm"]
            with concurrent.futures.ThreadPoolExecutor(self.args.concurrency) as pool:
                futures = [pool.submit(ledger.attempt, op, self.url(op["cell"] % 3, op["cell"])) for op in warm]
                if not all(f.result() for f in futures):
                    raise RuntimeError("warmup did not acknowledge every cell")
            self.verify(scenario, "before-fault", ledger, [0, 1, 2], final=True)
            self.snapshot(scenario, 3, "warm")
            dropped = next(op for op in ledger.operations if op["phase"] == "lost-response")
            with LostResponse(ledger, dropped, self.url(0, dropped["cell"])) as url:
                if ledger.attempt(dropped, url) or dropped["upstream_ack"] is None:
                    raise RuntimeError("response loss was not observed after an SDK acknowledgement")
            burst = [op for op in ledger.operations if op["phase"] == "burst"]
            self.phase = f"{scenario}-inflight"
            burst_started = time.monotonic()
            hold_ms = 30_000 if kind == "isolation" else 10_000
            with concurrent.futures.ThreadPoolExecutor(self.args.concurrency) as pool:
                futures = [pool.submit(ledger.attempt, op, self.url(op["cell"] % 3, op["cell"]), hold_ms) for op in burst]
                marker = None
                while time.monotonic() - burst_started < 5:
                    for i in range(3):
                        for line in logs(self.nodes[i]).splitlines():
                            if not line.startswith("{"):
                                continue
                            value = json.loads(line)
                            with ledger.lock:
                                pending = list(ledger.pending)
                            if (value.get("event") == "fault_write_applied" and value.get("inserted") is True
                                    and value.get("operation_id") in pending and value.get("hold_ms") == hold_ms
                                    and value.get("node") == f"node{i + 1}"):
                                marker = dict(value, client_pending=pending, owner_index=i)
                                break
                        if marker:
                            break
                    if marker:
                        break
                    time.sleep(0.05)
                if not marker:
                    raise RuntimeError("no accepted in-flight write observed; failure was not injected")
                owner = marker["owner_index"]
                if kind == "isolation":
                    # Detachment starts while the accepted handler is held.
                    # Its SDK outcome can return later after owned work joins.
                    fault = dict(scenario=scenario, marker=marker, burst_to_disconnect_started_s=time.monotonic() - burst_started)
                    (self.output / f"{scenario}-fault.json").write_text(json.dumps(fault, indent=2) + "\n")
                    fault["isolation"] = self.isolate(owner, scenario, burst_started)
                else:
                    killed = [owner] + ([i for i in range(3) if i != owner] if durability == "bucket" else [])
                    stops = [self.crash(i, scenario) for i in killed]
                    elapsed = time.monotonic() - burst_started
                    if elapsed >= 10:
                        raise RuntimeError("process fault was not completed inside the handler hold window")
                    fault = dict(scenario=scenario, marker=marker, kills=stops, burst_to_kill_s=elapsed)
                self.faults.append(fault)
                (self.output / f"{scenario}-fault.json").write_text(json.dumps(fault, indent=2) + "\n")
                for future in futures:
                    future.result()
            ledger.save()
            held = next(op for op in burst if op["operation_id"] == marker["operation_id"])
            if held["ack"] is not None:
                raise RuntimeError("controlled operation received an acknowledgement before owner failure")
            if durability == "bucket":
                self.clear_disks()
                for i in range(3):
                    self.start_node(i, scenario, durability)
                    self.wait_node(scenario, i)
                survivors = [0, 1, 2]
            else:
                survivors = [i for i in range(3) if i != owner]
            # Critical ordering: do not replay anything until acknowledged
            # operations have been checked on the recovered owners.
            self.verify(scenario, "before-replay", ledger, survivors)
            self.snapshot(scenario, 3, "recovered", exclude=owner if durability == "fleet" else None)
            issued = [op for op in ledger.operations if op["attempts"]]
            for op in issued:
                deadline = time.monotonic() + 120
                while not ledger.attempt(op, self.url(survivors[op["cell"] % len(survivors)], op["cell"])):
                    if time.monotonic() >= deadline:
                        raise RuntimeError("operation reconciliation timed out")
                    time.sleep(0.1)
            self.verify(scenario, "reconciled", ledger, survivors, final=True)
            self.snapshot(scenario, 3, "reconciled", exclude=owner if durability == "fleet" else None)
            if durability == "fleet":
                self.start_node(owner, scenario, durability)
                self.wait_node(scenario, owner)
            for i in range(3):
                self.remove_node(i, scenario + "-drained", graceful=True, mode="preserve" if i == 2 else "handoff")
            self.start_node(2, scenario, durability)
            self.wait_node(scenario, 2)
            self.verify(scenario, "preserve-reload", ledger, [2], final=True)
            self.snapshot(scenario, 3, "preserve-reload", only=2)
            self.remove_node(2, scenario + "-preserve-reloaded", graceful=True, mode="preserve")
            self.clear_disks()
        finally:
            try:
                ledger.save()
            finally:
                ledger.close()


def verify_image_sources(metadata):
    """Tie captured measuring files and runtime to the image's Git revision."""
    image_revision = (metadata.get("image_labels") or {}).get("org.opencontainers.image.revision", "")
    if metadata.get("dirty") is not False or not re.fullmatch(r"[0-9a-f]{40}", image_revision):
        raise RuntimeError("build the image with SOURCE_REVISION from a clean committed checkout")
    for name, digest in metadata["source_sha256"].items():
        result = subprocess.run(["git", "show", f"{image_revision}:{name}"], capture_output=True, check=True)
        if hashlib.sha256(result.stdout).hexdigest() != digest:
            raise RuntimeError(f"image measuring source does not match checkout: {name}")
    subprocess.run(["git", "diff", "--exit-code", image_revision, metadata["revision"], "--",
                    "crates", "vendor", "Cargo.toml", "Cargo.lock"], capture_output=True, check=True)


def audit_run(root):
    """Recheck saved stage ledgers before accepting a completed run."""
    metadata = json.loads((root / "metadata.json").read_text())
    results = json.loads((root / "results.json").read_text())
    if results.get("completed") is not True or results.get("failures"):
        raise RuntimeError("fault run did not complete")
    verify_image_sources(metadata)
    args = metadata["arguments"]
    for kind in args["scenario"]:
        if kind == "peer-partition":
            from peer_partition import audit
            audit(root, args, results)
            continue
        durability = "bucket" if kind == "bucket" else "fleet"
        scenario = f"inflight-{kind}"
        for label, final in (("before-fault", True), ("before-replay", False), ("reconciled", True), ("preserve-reload", True)):
            operations = json.loads((root / f"{scenario}-{label}-operations.json").read_text())
            report = json.loads((root / f"{scenario}-{label}.json").read_text())
            if report["label"] != label or report["final"] is not final or len(report["states"]) != args["cells"]:
                raise RuntimeError("missing or invalid verification stage")
            cells = set()
            totals = {k: 0 for k in ("issued", "client_acknowledged", "ambiguous_present", "ambiguous_absent", "writes")}
            for state in report["states"]:
                cell = state["cell"]
                if type(cell) is not int or cell in cells or not 0 <= cell < args["cells"]:
                    raise RuntimeError("missing or duplicate cell verification")
                cells.add(cell)
                counts = validate_snapshot([op for op in operations if op["cell"] == cell], state, final)
                for key, value in counts.items():
                    totals[key] += value
            if any(report[key] != value for key, value in totals.items()):
                raise RuntimeError("verification totals disagree with saved receipts")
            if label == "preserve-reload" and any(s["node"] != "node3" for s in report["states"]):
                raise RuntimeError("preserving reload was not served by the final node")
        fault = json.loads((root / f"{scenario}-fault.json").read_text())
        marker = fault["marker"]
        hold_ms = 30_000 if kind == "isolation" else 10_000
        if (marker["inserted"] is not True or marker["hold_ms"] != hold_ms
                or marker["operation_id"] not in marker["client_pending"]):
            raise RuntimeError("no proved process failure during an accepted write")
        if kind == "isolation":
            if not 0 <= fault["burst_to_disconnect_started_s"] < 5:
                raise RuntimeError("isolation did not start inside the accepted handler's hold window")
            if not 0 <= fault["isolation"]["burst_to_disconnected_s"] < 30:
                raise RuntimeError("network detachment was not observed inside the handler hold window")
            validate_isolation(fault["isolation"])
        else:
            if (not 0 <= fault["burst_to_kill_s"] < 10
                    or len(fault["kills"]) != (3 if durability == "bucket" else 1)
                    or any(k["exit_code"] != 137 for k in fault["kills"])):
                raise RuntimeError("missing controlled process kills")
        pre = json.loads((root / f"{scenario}-before-replay-operations.json").read_text())
        held = next(op for op in pre if op["operation_id"] == marker["operation_id"])
        dropped = next(op for op in pre if op["phase"] == "lost-response")
        if held["ack"] is not None or dropped["ack"] is not None or dropped["upstream_ack"] is None:
            raise RuntimeError("client ambiguity or dropped upstream response was not observed")
        node_sets = {"warm": range(3), "recovered": range(3), "reconciled": range(3), "preserve-reload": [2]}
        if durability == "fleet":
            survivors = [i for i in range(3) if i != marker["owner_index"]]
            node_sets.update(recovered=survivors, reconciled=survivors)
        for label, nodes in node_sets.items():
            for node in nodes:
                validate_routing_cache(json.loads((root / f"{scenario}-{label}-node{node + 1}-state.json").read_text()), args["max_cached_routes"])
        outcomes = [o for o in results["shutdown_outcomes"] if o["label"].startswith(scenario)]
        if len(outcomes) != 4:
            raise RuntimeError("missing returned shutdown proofs")
        for outcome in outcomes:
            mode = "preserve" if outcome["node"] == 3 else "handoff"
            validate_shutdown(outcome["proof"], outcome["exit_code"], mode)
        policies = [p for p in results["node_policies"] if p["scenario"] == scenario]
        if len(policies) != (7 if durability == "bucket" else 5):
            raise RuntimeError("missing captured startup policies")
        for policy in policies:
            validate_node_limits(json.dumps(policy["proof"]), args["max_resident"], args["max_cached_routes"], args["max_requests"])
    return {"completed": True, "scenarios": args["scenario"], "verified_stages": len(args["scenario"]) * 4}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", required=True)
    parser.add_argument("--rustfs-image", default="rustfs/rustfs@sha256:1803faef57627e2d9c2e7d89d655d712ddded5389040054987163043fecb6a3c")
    parser.add_argument("--cells", type=int, default=128)
    parser.add_argument("--concurrency", type=int, default=16)
    parser.add_argument("--max-resident", type=int, default=16)
    parser.add_argument("--max-cached-routes", type=int, default=8)
    parser.add_argument("--max-requests", type=int, default=128)
    parser.add_argument("--shutdown-ms", type=int, default=60_000)
    parser.add_argument("--port", type=int, default=39700)
    parser.add_argument("--output")
    parser.add_argument("--scenario", choices=("bucket", "fleet", "isolation", "peer-partition"), nargs="+", default=["bucket", "fleet"])
    args = parser.parse_args()
    if (min(args.cells, args.concurrency, args.max_resident, args.max_requests, args.shutdown_ms) <= 0
            or args.concurrency > args.cells or args.max_cached_routes < 0):
        parser.error("invalid workload or node limits")
    if "peer-partition" in args.scenario and args.concurrency < 2:
        parser.error("peer-partition requires an accepted operation and a new write after partition")
    harness = FaultHarness(args)
    image = json.loads(command("docker", "image", "inspect", args.image))[0]
    sources = ("tools/perf/fault_cells.rs", "tools/perf/faults.py", "tools/perf/deployment.rs",
               "tools/perf/embedded.rs", "tools/perf/run.py", "tools/perf/Dockerfile", "Cargo.toml", "Cargo.lock", "crates/cellar/Cargo.toml")
    if "peer-partition" in args.scenario:
        sources += ("tools/perf/peer_partition.py",)
    metadata = {"started_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "revision": command("git", "rev-parse", "HEAD"), "dirty": bool(command("git", "status", "--porcelain")),
                "arguments": vars(args), "image": image["Id"], "image_labels": image["Config"].get("Labels"),
                "source_sha256": {name: hashlib.sha256(pathlib.Path(name).read_bytes()).hexdigest() for name in sources},
                "execution": "sdk::Node on caller-owned executors; host-side fsynced operation journal and receipt verifier",
                "limitations": ["Local single-server RustFS and shared Colima infrastructure.",
                                "Application implements operation idempotency; Cellar does not promise exactly-once delivery.",
                                "Full isolation cuts peer, bucket and public connectivity; peer-partition cuts one owner's traffic to its two peers.",
                                "Correctness checks, not a throughput measurement or production fleet certification."]}
    (harness.output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    verify_image_sources(metadata)
    completed = False
    print("Artifacts:", harness.output, flush=True)
    try:
        harness.setup()
        for scenario in args.scenario:
            harness.scenario(scenario)
        completed = True
    except Exception as error:
        harness.failures.append({"operation": "fault_validation", "phase": harness.phase, "error": str(error)})
        raise
    finally:
        (harness.output / "results.json").write_text(json.dumps({"completed": completed,
            "verifications": harness.verifications, "faults": harness.faults,
            "shutdown_outcomes": harness.shutdown_outcomes, "node_policies": harness.node_policies,
            "routing_cache_snapshots": harness.routing_cache_snapshots, "failures": harness.failures}, indent=2) + "\n")
        harness.cleanup()
    try:
        print(json.dumps(audit_run(harness.output)), flush=True)
    except Exception as error:
        path = harness.output / "results.json"
        results = json.loads(path.read_text())
        results["completed"] = False
        results["failures"].append({"operation": "artifact_audit", "error": str(error)})
        path.write_text(json.dumps(results, indent=2) + "\n")
        raise


if __name__ == "__main__":
    main()
