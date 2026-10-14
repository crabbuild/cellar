#!/usr/bin/env python3
"""Audit stream obligations before retries across actual single-node process loss."""

import argparse
import concurrent.futures
import hashlib
import http.client
import http.server
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request


class AuditError(RuntimeError):
    pass


def require(condition, message):
    if not condition:
        raise AuditError(message)


def digest(payload):
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def integer(value):
    require(isinstance(value, str), "protocol integer is not a decimal string")
    try:
        result = int(value)
    except ValueError as error:
        raise AuditError("invalid protocol integer") from error
    require(str(result) == value and 0 <= result <= (1 << 63) - 1, "noncanonical protocol integer")
    return result


def read(origin, path, method="GET", timeout=10):
    try:
        response = urllib.request.urlopen(urllib.request.Request(origin + path, method=method), timeout=timeout)
    except urllib.error.HTTPError as error:
        raise AuditError(f"{method} {path}: HTTP {error.code}: {error.read(1024)!r}") from error
    with response:
        require(response.status in (200, 201), "unexpected successful status")
        return json.loads(response.read())


def eventually(read_value, accepted, timeout=60):
    deadline = time.monotonic() + timeout
    while True:
        value = read_value()
        if accepted(value):
            return value
        require(time.monotonic() < deadline, f"observation timed out: {value}")
        time.sleep(0.05)


def producer(receipt):
    return {key: receipt[key] for key in ("source", "incarnation", "lane", "sequence")}


def validate_source(operation, state):
    require(state.get("order") == operation["payload"], "acknowledged source state missing or changed before replay")
    receipt = state.get("receipt")
    require(isinstance(receipt, dict) and receipt.get("payload_sha256") == operation["sha256"], "source payload digest mismatch")
    for previous in (operation.get("ack"), operation.get("upstream_ack")):
        if previous is not None:
            require(receipt == previous, "original source receipt changed during recovery/retry")
    require(integer(receipt["sequence"]) == 1, "source emitted the command more than once")
    outbox = state["outbox"]
    lanes = outbox["lanes"]
    require(len(lanes) == 1, "command source has unexpected producer lanes")
    revision = lanes[0]["revision"]
    require(producer(receipt) == dict(source=revision["source"], incarnation=revision["incarnation"], lane=revision["lane"], sequence="1"), "source identity changed")
    require(outbox["incarnation"] == receipt["incarnation"] and integer(revision["next_sequence"]) == 2, "source incarnation/sequence changed")
    pending = integer(outbox["pending_rows"])
    settled = integer(revision["settled_through"])
    require(pending in (0, 1) and settled == 1 - pending and integer(lanes[0]["pending_rows"]) == pending, "source obligation accounting changed")
    return pending


def validate_event(operation, record):
    require(record.get("order_id") == operation["payload"]["order_id"] and record.get("payload_sha256") == operation["sha256"], "accepted event payload digest mismatch")
    receipt = operation.get("ack") or operation.get("upstream_ack")
    require(receipt is not None and producer(record) == producer(receipt), "event does not match acknowledged producer identity")
    require(record.get("event_id") == f"v1:{receipt['lane']}:{receipt['sequence']}", "logical event identity changed")
    require(integer(record["offset"]) == operation["offset"], "duplicate acceptance or reordered controlled command")
    envelope = record.get("envelope_sha256")
    require(isinstance(envelope, str) and len(envelope) == 64 and all(c in "0123456789abcdef" for c in envelope), "encoded event digest missing")
    if "event" in record:
        require(record["event"] == operation["payload"] and digest(record["event"]) == operation["sha256"], "event bytes contain changed business data")
    previous = operation.get("accepted")
    if previous is not None:
        require({key: record[key] for key in previous} == previous, "accepted event identity or immutable envelope changed")
    return {key: value for key, value in record.items() if key != "event"}


def validate_snapshot(operations, snapshot, expected_effects, expected_checkpoint, expected_pending):
    """No source POST or consumer replay is permitted until this audit succeeds."""
    require(set(snapshot["sources"]) == set(operations), "source inventory differs from issued commands")
    events = snapshot["events"]["records"]
    require(isinstance(events, list) and len(events) <= 1, "one-record stream capacity violated")
    event_ids = set()
    accepted = {}
    for record in events:
        key = record.get("order_id")
        require(key in operations and key not in accepted, "unknown or duplicate destination acceptance")
        accepted[key] = validate_event(operations[key], record)
        require(record["event_id"] not in event_ids, "duplicate logical event")
        event_ids.add(record["event_id"])
    effects = snapshot["effects"]["receipts"]
    require(isinstance(effects, dict) and set(effects) == set(expected_effects), "committed consumer receipt missing or extra effect")
    for key, record in effects.items():
        require(key in operations, "effect for an unissued command")
        evidence = validate_event(operations[key], record)
        if key in accepted:
            require(accepted[key] == evidence, "consumer/destination receipt mismatch")
        accepted[key] = evidence
    projection = snapshot["effects"]["projection"]
    require(type(projection.get("orders")) is int and type(projection.get("total_cents")) is int, "consumer totals have invalid numeric types")
    require(projection == {"orders": len(effects), "total_cents": sum(operations[key]["payload"]["amount_cents"] for key in effects)}, "consumer totals do not equal distinct receipt effects")
    require(integer(snapshot["subscription"]["checkpoint"]) == expected_checkpoint, "checkpoint skipped or lost acknowledged work")
    for key, operation in operations.items():
        require(type(operation.get("attempts")) is int and operation["attempts"]>0, "acknowledged command was never issued")
        require(operation.get("ack") or operation.get("upstream_ack"), "command was not acknowledged by source")
        pending = validate_source(operation, snapshot["sources"][key])
        require(pending == expected_pending[key], "expected pending/accepted source obligation changed")
        require(pending == 1 or key in accepted, "acknowledged event is neither pending nor independently accounted as accepted")
    return accepted


class Ledger:
    def __init__(self, root):
        self.path = root / "ledger.jsonl"
        self.stream = self.path.open("x")
        self.lock = threading.Lock()
        self.audited = False
        self.operations = {}
        for key, amount, offset in (("a", 1200, 1), ("b", 300, 2)):
            payload = {"order_id": key, "amount_cents": amount}
            self.operations[key] = dict(payload=payload, sha256=digest(payload), offset=offset, attempts=0)
        self.record("plan", operations=self.operations)

    def record(self, event, **value):
        with self.lock:
            self.stream.write(json.dumps(dict(event=event, **value)) + "\n")
            self.stream.flush()
            os.fsync(self.stream.fileno())

    def acknowledge(self, key, reply, upstream=False):
        operation = self.operations[key]
        require(reply.get("order") == operation["payload"] and reply.get("status") in ("created", "duplicate"), "invalid source acknowledgment")
        receipt = reply.get("receipt")
        require(isinstance(receipt, dict) and receipt.get("payload_sha256") == operation["sha256"], "acknowledgment payload changed")
        for previous in (operation.get("ack"), operation.get("upstream_ack")):
            require(previous is None or receipt == previous, "retry returned a changed original receipt")
        self.record("upstream_ack" if upstream else "ack", key=key, receipt=receipt)
        operation["upstream_ack" if upstream else "ack"] = receipt

    def issue(self, origin, key, replay=False):
        if replay:
            require(self.audited, "ambiguous replay attempted before successful recovery audit")
        operation = self.operations[key]
        operation["attempts"] += 1
        self.record("attempt", key=key, replay=replay, attempt=operation["attempts"])
        path = f"/orders/{key}?amount_cents={operation['payload']['amount_cents']}"
        reply = read(origin, path, "POST")
        self.acknowledge(key, reply)
        return reply

    def audit(self, origin, label, expected_effects, checkpoint, pending):
        self.audited = False
        snapshot = {"sources": {key: read(origin, f"/orders/{key}") for key in self.operations},
                    "events": read(origin, "/__test/events"), "effects": read(origin, "/__test/projection-receipts"),
                    "subscription": read(origin, "/subscription")}
        self.record("audit_snapshot", label=label, operations=self.operations, snapshot=snapshot)
        accepted = validate_snapshot(self.operations, snapshot, expected_effects, checkpoint, pending)
        for key, receipt in accepted.items():
            self.operations[key]["accepted"] = receipt
        self.record("audit_passed", label=label)
        self.audited = True
        return snapshot

    def close(self):
        self.stream.close()


class DroppedResponse:
    def __init__(self, ledger, origin):
        self.errors = []
        owner = self
        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                try:
                    reply = read(origin, self.path, "POST")
                    ledger.acknowledge("b", reply, upstream=True)
                except Exception as error:
                    owner.errors.append(error)
                finally:
                    self.close_connection = True
                    self.connection.shutdown(socket.SHUT_RDWR)
            def log_message(self, *_):
                pass
        self.server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever)
        self.thread.start()

    def run(self, ledger):
        operation = ledger.operations["b"]
        operation["attempts"] += 1
        ledger.record("attempt", key="b", replay=False, attempt=operation["attempts"])
        try:
            read(f"http://127.0.0.1:{self.server.server_port}", "/orders/b?amount_cents=300", "POST")
        except (urllib.error.URLError, http.client.HTTPException, OSError) as error:
            ledger.record("client_response_lost", key="b", error=type(error).__name__)
        else:
            raise AuditError("client received the intentionally discarded response")
        require(not self.errors and operation.get("upstream_ack"), "upstream did not acknowledge before response drop")

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        require(not self.thread.is_alive(), "response proxy did not join")


class Process:
    def __init__(self, binary, root, label):
        self.log = root / f"{label}.log"
        self.output = self.log.open("xb")
        self.child = subprocess.Popen([str(binary), str(root / "data"), "0", "--fault-harness"],
                                      stdout=self.output, stderr=subprocess.STDOUT,
                                      env=dict(os.environ, NO_COLOR="1"))

    def ready(self):
        def probe():
            require(self.child.poll() is None, self.log.read_text(errors="replace"))
            for line in self.log.read_text(errors="replace").splitlines():
                if line.startswith('{"ready":'):
                    return json.loads(line)["ready"]
            return None
        return eventually(probe, bool, timeout=90)

    def crash(self, ledger, label):
        require(self.child.poll() is None, "fault process had already exited")
        self.child.kill()
        code = self.child.wait(timeout=20)
        self.output.close()
        require(code == -signal.SIGKILL, "requested SIGKILL was not observed")
        require('"outcome":"stopped"' not in self.log.read_text(errors="replace"), "forced loss fabricated graceful shutdown")
        ledger.audited = False
        ledger.record("process_killed", label=label, pid=self.child.pid, exit_code=code)

    def stop(self):
        if self.child.poll() is None:
            self.child.send_signal(signal.SIGTERM)
            try:
                code = self.child.wait(timeout=80)
            except subprocess.TimeoutExpired:
                self.child.kill()
                self.child.wait()
                raise AuditError("fault fixture did not drain")
            finally:
                self.output.close()
            require(code == 0, self.log.read_text(errors="replace"))
            reports = [json.loads(line) for line in self.log.read_text().splitlines() if line.startswith('{"') and json.loads(line).get("outcome") == "stopped"]
            require(len(reports) == 1 and all(reports[0].get(key) is True for key in ("ingress_drained", "application_work_drained", "cells_drained")), "final graceful shutdown proof incomplete")


def audit_evidence(path):
    """Re-run every saved pre-retry audit with its original measuring ledger."""
    expected = {"before-source-crash": ([], 0, {"a": 0, "b": 1}),
                "source-recovered-before-replay": ([], 0, {"a": 0, "b": 1}),
                "consumer-recovered-before-replay": (["a"], 0, {"a": 0, "b": 1}),
                "final": (["a", "b"], 2, {"a": 0, "b": 0})}
    snapshots = set()
    passes = set()
    kills = set()
    phase_count = drops = upstream = replays = expiries = 0
    last_operations = None
    eligible = False
    last_snapshot = None
    records = [json.loads(line) for line in path.read_text().splitlines()]
    for record in records:
        event = record["event"]
        if event == "audit_snapshot":
            label = record["label"]
            require(label in expected and label not in snapshots, "unknown/repeated audit stage")
            validate_snapshot(record["operations"], record["snapshot"], *expected[label])
            snapshots.add(label)
            last_operations = record["operations"]
            last_snapshot = label
            eligible = False
        elif event == "audit_passed":
            require(record["label"] == last_snapshot and record["label"] not in passes, "audit pass has no matching verified snapshot")
            passes.add(record["label"])
            eligible = last_snapshot == "consumer-recovered-before-replay"
        elif event == "process_killed":
            require(type(record.get("exit_code")) is int and record["exit_code"] == -signal.SIGKILL, "kill proof is not SIGKILL")
            require(record["label"] not in kills, "repeated kill evidence")
            kills.add(record["label"])
            eligible = False
        elif event == "attempt" and record.get("replay"):
            require(eligible and record["key"] == "b", "journal replay preceded recovery audit")
            replays += 1
        elif event == "consumer_proven_before_ack":
            require(record["phase"]["boundary"] == "consumer_proven_before_ack" and record["phase"]["token"] == "consumer-boundary", "consumer boundary proof invalid")
            require(last_operations is not None and len(record["phase"]["records"]) == 1, "consumer boundary batch missing")
            validate_event(last_operations["a"], record["phase"]["records"][0])
            require(record["phase"]["projection"] == {"orders":1,"total_cents":last_operations["a"]["payload"]["amount_cents"]}, "consumer boundary effect mismatch")
            phase_count += 1
        elif event == "lease_expiry_observed":
            require(record["subscription"]["observation"]["lease_active"] is False and integer(record["subscription"]["checkpoint"]) == 0, "lease expiry proof skipped work")
            expiries += 1
        elif event == "client_response_lost":
            require(record["key"] == "b", "wrong discarded response")
            drops += 1
        elif event == "upstream_ack":
            require(record["key"] == "b", "wrong upstream acknowledgment")
            upstream += 1
    require(snapshots == set(expected) and passes == snapshots, "fault audit evidence incomplete")
    require(kills == {"acknowledged-source-with-pending-obligation", "consumer-proven-effects-before-ack"}, "fault kill stages incomplete")
    require(phase_count == drops == upstream == replays == expiries == 1, "response-loss/consumer/replay evidence incomplete")
    return {"audits": len(snapshots), "sigkills": len(kills), "response_drops": drops}


def exercise(binary, root):
    require(os.name == "posix", "this qualification requires SIGKILL")
    root.mkdir(parents=True, exist_ok=False)
    ledger = Ledger(root)
    node = None
    try:
        node = Process(binary, root, "initial")
        origin = node.ready()
        ledger.issue(origin, "a")
        eventually(lambda: read(origin, "/__test/events"), lambda page: len(page["records"]) == 1)
        eventually(lambda: read(origin, "/orders/a"), lambda state: state["outbox"]["pending_rows"] == "0")
        proxy = DroppedResponse(ledger, origin)
        try:
            proxy.run(ledger)
        finally:
            proxy.close()
        ledger.audit(origin, "before-source-crash", [], 0, {"a": 0, "b": 1})
        node.crash(ledger, "acknowledged-source-with-pending-obligation")
        node = Process(binary, root, "source-recovered")
        origin = node.ready()
        ledger.audit(origin, "source-recovered-before-replay", [], 0, {"a": 0, "b": 1})
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            hold = executor.submit(read, origin, "/projection/hold?token=consumer-boundary", "POST", 20)
            phase = eventually(lambda: read(origin, "/__test/phase"), lambda value: (value.get("phase") or {}).get("boundary") == "consumer_proven_before_ack", timeout=5)["phase"]
            require(phase["token"] == "consumer-boundary" and phase["projection"] == {"orders": 1, "total_cents": 1200}, "consumer proof boundary missing")
            require(len(phase["records"]) == 1, "consumer leased an unexpected batch")
            validate_event(ledger.operations["a"], phase["records"][0])
            ledger.record("consumer_proven_before_ack", phase=phase)
            require(not hold.done(), "consumer request completed before process loss")
            node.crash(ledger, "consumer-proven-effects-before-ack")
            try:
                hold.result(timeout=10)
            except (urllib.error.URLError, http.client.HTTPException, OSError) as error:
                ledger.record("consumer_response_interrupted", error=type(error).__name__)
            else:
                raise AuditError("held consumer request unexpectedly completed")
        node = Process(binary, root, "consumer-recovered")
        origin = node.ready()
        ledger.audit(origin, "consumer-recovered-before-replay", ["a"], 0, {"a": 0, "b": 1})
        require(ledger.issue(origin, "b", replay=True)["status"] == "duplicate", "ambiguous command replay emitted again")
        released = eventually(lambda: read(origin, "/subscription"), lambda status: not status["observation"]["lease_active"], timeout=45)
        require(integer(released["checkpoint"]) == 0, "lease expiry silently advanced checkpoint")
        ledger.record("lease_expiry_observed", subscription=released)
        resumed = read(origin, "/projection/process", "POST")
        require(resumed["status"] == "acknowledged" and resumed["fresh_effects"] == 0, "consumer redelivery repeated proven effects")
        eventually(lambda: read(origin, "/__test/events"), lambda page: len(page["records"]) == 1 and page["records"][0]["order_id"] == "b")
        eventually(lambda: read(origin, "/orders/b"), lambda state: state["outbox"]["pending_rows"] == "0")
        processed = read(origin, "/projection/process", "POST")
        require(processed["status"] == "acknowledged" and processed["fresh_effects"] == 1, "second command effect missing or repeated")
        require(read(origin, "/projection/process", "POST")["status"] == "empty", "checkpoint did not settle")
        ledger.audit(origin, "final", ["a", "b"], 2, {"a": 0, "b": 0})
        node.stop()
        evidence = audit_evidence(ledger.path)
        summary = dict(evidence, status="passed", binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
                       scope="single-node SQLite bucket; pending source and consumer-before-ack SIGKILL, source HTTP response loss", output=str(root))
        (root / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
        print(json.dumps(summary), flush=True)
    finally:
        if node is not None:
            node.stop()
        ledger.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    target=parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--binary", type=Path)
    target.add_argument("--audit", type=Path, help="recheck a saved measuring ledger without running or retrying the fixture")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.audit:
        require(args.output is None,"--output is only for process qualification")
        print(json.dumps(audit_evidence(args.audit.resolve())),flush=True)
    else:
        target = args.output or Path(tempfile.mkdtemp(prefix="cellar-stream-fault-parent-")) / "evidence"
        exercise(args.binary.resolve(), target.resolve())
