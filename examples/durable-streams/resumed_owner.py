#!/usr/bin/env python3
# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Resume a SIGSTOPed source after a successor claims and settles its obligation."""
import argparse
import concurrent.futures
import datetime
import hashlib
import hmac
import http.client
import json
import os
from pathlib import Path
import shutil
import signal
import sqlite3
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request

from before_send import PAYLOAD, TOKEN, inspect_crash_image as inspect_image, validate_boundary, validate_crash_image, validate_snapshot
from faults import AuditError, Ledger, digest, eventually, read, require
from fleet_faults import FleetProcess, NetworkStore, free_address


# The measuring client reads only its private test store, independently of nodes.
def store_json(root, store, key):
    return json.loads(store_read(root, store, key))


def store_read(root, store, key, query=""):
    if store is None:
        with sqlite3.connect((root / "fleet.sqlite").resolve().as_uri() + "?mode=ro", uri=True) as db:
            row = db.execute("SELECT body FROM __sys_local_objects WHERE key=?", (key,)).fetchone()
            require(row is not None, "measured authority object missing")
            return row[0]
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    day, host = stamp[:8], store.config["endpoint"].removeprefix("http://")
    path = "/" + store.config["name"] + "/" + urllib.parse.quote(key, safe="/")
    payload = hashlib.sha256(b"").hexdigest()
    signed = "host;x-amz-content-sha256;x-amz-date"
    canonical = f"GET\n{path}\n{query}\nhost:{host}\nx-amz-content-sha256:{payload}\nx-amz-date:{stamp}\n\n{signed}\n{payload}"
    scope = f"{day}/us-east-1/s3/aws4_request"
    signing = ("AWS4" + store.secret).encode()
    for component in (day, "us-east-1", "s3", "aws4_request"):
        signing = hmac.new(signing, component.encode(), hashlib.sha256).digest()
    text = f"AWS4-HMAC-SHA256\n{stamp}\n{scope}\n{hashlib.sha256(canonical.encode()).hexdigest()}"
    signature = hmac.new(signing, text.encode(), hashlib.sha256).hexdigest()
    headers = {"x-amz-content-sha256": payload, "x-amz-date": stamp,
               "Authorization": f"AWS4-HMAC-SHA256 Credential={store.access}/{scope}, SignedHeaders={signed}, Signature={signature}"}
    target = store.config["endpoint"] + path + ("?" + query if query else "")
    with urllib.request.urlopen(urllib.request.Request(target, headers=headers), timeout=5) as reply:
        body = reply.read((16 << 20) + 1)
        require(len(body) <= 16 << 20, "private store observation exceeded bounded body")
        return body


def authority(root, store, scope):
    value = store_json(root, store, f"cells/{scope}/own.json")
    return dict(node=value["node"], epoch=value["epoch"])


def copy_image(root, phase, label):
    scope = phase["command"]["receipt"]["source"]
    require(scope.startswith("Order:") and len(scope) == 70 and all(c in "0123456789abcdef" for c in scope[6:]), "unsafe source scope")
    candidates = list((root / "data/producer/node" / scope / "ltx").glob("e*/db.sqlite"))
    require(len(candidates) == 1, "old source epoch inventory changed")
    archive = root / label
    archive.mkdir()
    for suffix in ("", "-wal", "-shm"):
        source = Path(str(candidates[0]) + suffix)
        if source.exists():
            target = archive / ("db.sqlite" + suffix)
            shutil.copy2(source, target)
            with target.open("rb") as data:
                os.fsync(data.fileno())
    image = inspect_image(archive, scope)
    validate_crash_image(phase, image)
    return image


def snapshot(origins):
    return dict(source=read(origins["successor"], "/orders/first"),
                events=read(origins["destination"], "/__test/events"),
                effects=read(origins["consumer"], "/__test/projection-receipts"),
                subscription=read(origins["destination"], "/subscription"))


def audit_evidence(path, complete=True):
    plan = pids = phase = original_owner = original_lease = successor = new_owner = None
    stopped = expired = resumed = fenced = interrupted = replayed = False
    images, audits, joined = {}, {}, set()
    store = None
    startup = 0
    removed = False
    attempted = False
    for line in path.read_text().splitlines():
        row = json.loads(line)
        event = row["event"]
        if event == "plan":
            require(plan is None and set(row["operations"]) == {"first"}, "invalid resumed-owner plan")
            plan = row["operations"]["first"]
            require(plan["payload"] == PAYLOAD and plan["sha256"] == digest(PAYLOAD) and plan["attempts"] == 0, "changed original command")
        elif event == "network_store_startup_retry":
            require(pids is None and store is None and row["status"] == 503, "application error hidden as store startup")
            startup += 1
        elif event == "network_store_started":
            require(store is None and pids is None and row["backend"] == "s3" and row["image_id"].startswith("sha256:")
                    and row["container"].startswith("cellar-stream-store-"), "invalid private store")
            store = row
        elif event == "resumed_fleet_started":
            require(pids is None and set(row["pids"]) == {"destination", "producer", "consumer"}
                    and len(set(row["pids"].values())) == 3 and row["proof_mode"] == "bucket"
                    and row["node_lease_ms"] == 1000 and row["handler_budget_ms"] == 10000, "invalid role/policy evidence")
            pids = row["pids"]
        elif event == "stale_source_attempt":
            require(pids is not None and not attempted and row["payload"] == PAYLOAD and row["sha256"] == digest(PAYLOAD), "changed or repeated original attempt")
            attempted = True
        elif event == "source_proven_before_pause":
            require(attempted and phase is None and not stopped and row["events"]["records"] == [], "source boundary out of order")
            phase, original_owner, original_lease = row["phase"], row["owner"], row["lease"]
            validate_boundary(phase)
            require(original_owner["node"] == "producer" and type(original_owner["epoch"]) is int and original_owner["epoch"] > 0
                    and original_lease["generation"] and original_lease["expires_ms"] > 0, "original authority missing")
        elif event == "owner_stopped":
            require(phase is not None and not stopped and row["pid"] == pids["producer"]
                    and os.WIFSTOPPED(row["wait_status"]) and os.WSTOPSIG(row["wait_status"]) == signal.SIGSTOP,
                    "owner was not observed stopped")
            stopped = True
        elif event == "old_source_image":
            label = row["label"]
            require(label not in images and label in ("old-before", "old-after") and stopped, "repeated or premature old image")
            require((label == "old-before" and not resumed and successor is None) or (label == "old-after" and fenced), "old image has wrong boundary")
            validate_crash_image(phase, row["image"])
            if images:
                require({k: v for k, v in row["image"].items() if k != "files"}
                        == {k: v for k, v in images["old-before"].items() if k != "files"}, "resumed old source mutated business/outbox state")
            images[label] = row["image"]
        elif event == "authority_expired":
            require("old-before" in images and not expired and successor is None and row["lease"]["generation"] == original_lease["generation"]
                    and row["lease"]["expires_ms"] <= row["observed_ms"], "claim preceded real authority expiry")
            expired = True
        elif event == "successor_started":
            require(expired and successor is None and not resumed and row["pid"] not in pids.values(), "successor preceded expiry or reused a process")
            successor = row["pid"]
        elif event == "resumed_owner_audit":
            label = row["label"]
            require(successor is not None and label not in audits and label in ("before-resume", "after-resume", "final"), "invalid audit stage")
            require((label == "before-resume" and not resumed) or (label == "after-resume" and fenced and interrupted and "old-after" in images)
                    or (label == "final" and replayed and "after-resume" in audits), "audit preceded its boundary")
            current = row["owner"]
            require(current["node"] in {"destination", "consumer", "successor"} and current["epoch"] > original_owner["epoch"], "old authority survived takeover")
            require(new_owner in (None, current), "resumed old owner changed successor ownership")
            new_owner = current
            state = row["snapshot"]
            require(state["source"]["host"] == current["node"] and state["effects"]["host"] == "consumer", "snapshot did not reach the actual owners")
            validate_snapshot(phase, state, final=label == "final")
            require(state["source"]["outbox"]["pending_rows"] == "0" and len(state["events"]["records"]) == 1, "successor did not settle original obligation before resume")
            require(state["events"]["records"][0]["envelope_sha256"] == images["old-before"]["envelope_sha256"], "successor changed the original envelope")
            audits[label] = state
        elif event == "owner_resumed":
            require("before-resume" in audits and not resumed and row["pid"] == pids["producer"]
                    and row["signal"] == signal.SIGCONT and 0 < row["attempt_elapsed_ms"] < 10000, "old owner resumed before audit or after ordinary deadline")
            resumed = True
        elif event == "owner_fenced":
            require(resumed and not fenced and row["pid"] == pids["producer"] and row["exit_code"] == 1
                    and row["report"] == dict(outcome="fenced", fence_code=3, reason="node_lease_expired")
                    and row["graceful_reports"] == 0, "resumed owner did not fence for expired authority")
            fenced = True
        elif event == "stale_response_interrupted":
            require(fenced and not interrupted and row["kind"] in ("AuditError", "URLError", "RemoteDisconnected", "ConnectionResetError", "IncompleteRead"), "old response fabricated success or only timed out")
            interrupted = True
        elif event == "command_replayed":
            require("after-resume" in audits and not replayed and row["status"] == "duplicate"
                    and row["receipt"] == phase["command"]["receipt"], "command replay repaired state or changed identity")
            replayed = True
        elif event == "fleet_process_stopped":
            require("final" in audits and row["node"] in {"destination", "consumer", "successor"} - joined
                    and row["pid"] == (successor if row["node"] == "successor" else pids[row["node"]]) and row["exit_code"] == 0
                    and len(row["reports"]) == 1 and all(row["reports"][0].get(key) is True for key in ("ingress_drained", "application_work_drained", "cells_drained")), "incomplete survivor shutdown")
            joined.add(row["node"])
        elif event == "network_store_removed":
            require(store is not None and not removed and joined == {"destination", "consumer", "successor"}
                    and row["container"] == store["container"], "store removed before complete shutdown")
            removed = True
        else:
            require(False, f"unknown resumed-owner evidence: {event}")
    if complete:
        require(fenced and interrupted and replayed and set(images) == {"old-before", "old-after"}
                and set(audits) == {"before-resume", "after-resume", "final"} and joined == {"destination", "consumer", "successor"}
                and (store is None or removed) and (startup == 0 or store is not None), "incomplete resumed-owner qualification")
        for label, recorded in images.items():
            actual = inspect_image(path.parent / label, phase["command"]["receipt"]["source"])
            require(actual == recorded, "archived old database/WAL evidence changed")
    return dict(audits=len(audits), resumed_expired_owners=int(fenced), effects=1 if "final" in audits else 0, backend="s3" if store else "sqlite")


def exercise(binary, root, rustfs_image=None):
    require(os.name == "posix", "resumed-owner qualification requires POSIX signals")
    root.mkdir(parents=True, exist_ok=False)
    ledger = Ledger(root, commands=(("first", 725, 1),))
    processes, paused = [], False
    producer = None
    store = NetworkStore(root, ledger, rustfs_image) if rustfs_image else None
    try:
        if store:
            store.start()
        origins = {}
        for node in ("destination", "producer", "consumer"):
            peer = free_address()
            process = FleetProcess(binary, root, node, node, peer, peer, node == "destination", store,
                                   fleet_durability=False, node_lease_ms=1000)
            processes.append(process)
            origins[node] = process.ready()
        producer = processes[1]
        ledger.record("resumed_fleet_started", pids={process.node: process.child.pid for process in processes},
                      proof_mode="bucket", node_lease_ms=1000, handler_budget_ms=10000)
        ledger.record("stale_source_attempt", payload=PAYLOAD, sha256=digest(PAYLOAD))
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            began = time.monotonic()
            original = pool.submit(read, origins["producer"], f"/orders/first?amount_cents=725&hold_before_send={TOKEN}", "POST", 20)
            phase = eventually(lambda: read(origins["producer"], "/__test/phase"),
                               lambda value: (value.get("phase") or {}).get("boundary") == "source_proven_before_send", timeout=5)["phase"]
            validate_boundary(phase)
            scope = phase["command"]["receipt"]["source"]
            events = read(origins["destination"], "/__test/events")
            lease = store_json(root, store, "nodes/producer.json")
            ledger.record("source_proven_before_pause", phase=phase, events=events, owner=authority(root, store, scope),
                          lease=dict(generation=lease["generation"], expires_ms=lease["expires_ms"]))
            producer.child.send_signal(signal.SIGSTOP)
            paused = True
            deadline = time.monotonic() + 5
            while True:
                pid, status = os.waitpid(producer.child.pid, os.WUNTRACED | os.WNOHANG)
                if pid:
                    require(os.WIFSTOPPED(status), "producer exited before observed SIGSTOP")
                    break
                require(time.monotonic() < deadline, "SIGSTOP was not observed")
                time.sleep(.01)
            ledger.record("owner_stopped", pid=pid, wait_status=status)
            image = copy_image(root, phase, "old-before")
            ledger.record("old_source_image", label="old-before", image=image)
            def expired():
                value = store_json(root, store, "nodes/producer.json")
                return dict(lease=dict(generation=value["generation"], expires_ms=value["expires_ms"]), observed_ms=time.time_ns() // 1_000_000)
            observed = eventually(expired, lambda row: row["lease"]["expires_ms"] <= row["observed_ms"], timeout=5)
            ledger.record("authority_expired", **observed)
            peer = free_address()
            successor = FleetProcess(binary, root, "successor", "successor", peer, peer, False, store,
                                     fleet_durability=False, node_lease_ms=1000)
            processes.append(successor)
            origins["successor"] = successor.ready()
            ledger.record("successor_started", pid=successor.child.pid)
            eventually(lambda: read(origins["successor"], "/orders/first"), lambda state: state["outbox"]["pending_rows"] == "0", timeout=5)
            def audited(label):
                ledger.record("resumed_owner_audit", label=label, owner=authority(root, store, scope), snapshot=snapshot(origins))
                audit_evidence(ledger.path, complete=False)
            audited("before-resume")
            producer.child.send_signal(signal.SIGCONT)
            paused = False
            ledger.record("owner_resumed", pid=producer.child.pid, signal=int(signal.SIGCONT), attempt_elapsed_ms=int((time.monotonic() - began) * 1000))
            code = producer.child.wait(timeout=15)
            producer.output.close()
            reports = [json.loads(line) for line in producer.log.read_text().splitlines() if line.startswith('{"')]
            fencing = [row for row in reports if row.get("outcome") == "fenced"]
            require(len(fencing) == 1, "old owner lacks a specific fencing report")
            ledger.record("owner_fenced", pid=producer.child.pid, exit_code=code, report=fencing[0],
                          graceful_reports=sum(row.get("outcome") == "stopped" for row in reports))
            try:
                result = original.result(timeout=5)
            except concurrent.futures.TimeoutError as error:
                raise AuditError("old HTTP request did not terminate after fencing") from error
            except (AuditError, OSError, http.client.HTTPException) as error:
                ledger.record("stale_response_interrupted", kind=type(error).__name__)
            else:
                raise AuditError(f"stale source returned a successful result: {result}")
        ledger.record("old_source_image", label="old-after", image=copy_image(root, phase, "old-after"))
        audited("after-resume")
        result = read(origins["successor"], "/orders/first?amount_cents=725", "POST")
        ledger.record("command_replayed", status=result["status"], receipt=result["receipt"])
        result = read(origins["consumer"], "/projection/process", "POST")
        require(result["status"] == "acknowledged" and result["fresh_effects"] == 1, "consumer effect missing or duplicated")
        audited("final")
        for process in reversed(processes):
            process.stop_recorded(ledger)
        if store:
            store.close()
        summary = dict(audit_evidence(ledger.path), status="passed", binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
                       scope="independent loopback processes; bucket proof; SIGSTOP/expiry/takeover/SIGCONT; old source invocation fences before progress")
        (root / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
        print(json.dumps(summary), flush=True)
    finally:
        if paused and producer is not None and producer.child.poll() is None:
            producer.child.send_signal(signal.SIGCONT)
        for process in reversed(processes):
            process.stop()
        if store:
            store.close()
        ledger.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--binary", type=Path)
    target.add_argument("--audit", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--rustfs-image")
    args = parser.parse_args()
    if args.audit:
        print(json.dumps(audit_evidence(args.audit.resolve())), flush=True)
    else:
        exercise(args.binary.resolve(), (args.output or Path(tempfile.mkdtemp(prefix="cellar-resumed-owner-parent-")) / "evidence").resolve(), args.rustfs_image)
