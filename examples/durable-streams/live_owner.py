#!/usr/bin/env python3
"""Keep a source callback entered through independent-process fenced takeover."""
import argparse
import concurrent.futures
from contextlib import closing
import hashlib
import http.client
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import tempfile
import time

from faults import AuditError, Ledger, digest, eventually, read, require, validate_snapshot
from fleet_faults import FleetProcess, NetworkStore, free_address
from resumed_owner import authority, store_json

ROLES = {"destination", "producer", "consumer"}
STAGES = ("before-isolation", "before-release", "after-refusal", "final")


def marker(path, body=b""):
    with path.open("xb") as file:
        file.write(body)
        file.flush()
        os.fsync(file.fileno())


def live(process):
    require(process.child.poll() is None, "old process exited before callback release")
    state = subprocess.check_output(["ps", "-o", "stat=", "-p", str(process.child.pid)], text=True).strip()
    require(state and state[0].upper() not in "TZ", "old process was paused or dead")
    return state


def phase(data):
    path = data / "owner-phase.json"
    return json.loads(path.read_text()) if path.exists() else {}


def inspect_image(archive, scope):
    with tempfile.TemporaryDirectory(prefix="cellar-live-owner-read-") as directory:
        working = Path(directory)
        hashes = {}
        for suffix in ("", "-wal", "-shm"):
            source = archive / ("db.sqlite" + suffix)
            require(not source.is_symlink(), "unsafe forensic family")
            if source.exists():
                shutil.copy2(source, working / source.name)
                if suffix != "-shm":
                    hashes[source.name] = hashlib.sha256(source.read_bytes()).hexdigest()
        with closing(sqlite3.connect((working / "db.sqlite").resolve().as_uri() + "?mode=ro", uri=True)) as db:
            db.execute("PRAGMA query_only=ON")
            values = {key: json.loads(value) for key, value in db.execute("SELECT k,v FROM __sys_cell_kv WHERE scope=? AND k IN ('order','command_receipt')", (scope,))}
            lanes = db.execute("SELECT lane,next_sequence,settled_through FROM __sys_outbox_lanes").fetchall()
            require(len(lanes) == 1, "old producer inventory changed")
            pending, = db.execute("SELECT count(*) FROM __sys_outbox").fetchone()
            return dict(order=values["order"], receipt=values["command_receipt"], lane=lanes[0][0], next_sequence=str(lanes[0][1]),
                        settled_through=str(lanes[0][2]), pending_rows=str(pending), files=hashes)


def archive_source(root, scope):
    require(scope.startswith("Order:") and len(scope) == 70 and all(c in "0123456789abcdef" for c in scope[6:]), "unsafe source scope")
    candidates = list((root / "data/producer/node" / scope / "ltx").glob("e*/db.sqlite"))
    require(len(candidates) == 1, "old source epoch inventory changed")
    archive = root / "old-source-image"
    archive.mkdir()
    for suffix in ("", "-wal", "-shm"):
        source = Path(str(candidates[0]) + suffix)
        if source.exists():
            target = archive / ("db.sqlite" + suffix)
            shutil.copy2(source, target)
            with target.open("rb") as file:
                os.fsync(file.fileno())
    return inspect_image(archive, scope)


def audit_evidence(path, complete=True):
    operations = pids = cut = original_owner = entered = expired = successor = attempted = image = store = None
    isolated = released = fenced = interrupted = consumed = removed = stale_attempted = False
    stages, joined = [], set()
    for line in path.read_text().splitlines():
        row = json.loads(line)
        event = row["event"]
        if event == "plan":
            require(operations is None and set(row["operations"]) == {"cross"}, "invalid live-owner command plan")
            operations = row["operations"]
            require(operations["cross"]["payload"] == dict(order_id="cross", amount_cents=725)
                    and operations["cross"]["sha256"] == digest(operations["cross"]["payload"]), "changed command")
        elif event == "network_store_startup_retry":
            require(pids is None and store is None and row["status"] == 503, "application error hidden as store startup")
        elif event == "network_store_started":
            require(store is None and pids is None and row["backend"] == "s3" and row["image_id"].startswith("sha256:")
                    and row["container"].startswith("cellar-stream-store-"), "invalid private store")
            store = row
        elif event == "live_owner_started":
            require(pids is None and set(row["pids"]) == ROLES and len(set(row["pids"].values())) == 3
                    and row["proof_mode"] == "bucket" and row["node_lease_ms"] == 1000 and row["handler_budget_ms"] == 10000, "invalid process/policy evidence")
            pids, cut = row["pids"], row["cut"]
            require(cut in ("before_tx", "before_commit"), "invalid callback cut")
        elif event == "attempt":
            require(pids is not None and row["key"] == "cross" and row["attempt"] == 1 and not row["replay"]
                    and operations["cross"]["attempts"] == 0, "command replay concealed recovery")
            operations["cross"]["attempts"] = 1
        elif event == "ack":
            require(operations["cross"]["attempts"] == 1 and "ack" not in operations["cross"] and row["key"] == "cross", "invalid source acknowledgment")
            operations["cross"]["ack"] = row["receipt"]
        elif event == "live_owner_audit":
            label = row["label"]
            require(len(stages) < len(STAGES) and label == STAGES[len(stages)], "reordered live-owner audit")
            if label == "before-isolation":
                require(entered is None and not isolated and row["owner"]["node"] == "producer", "initial source audit followed isolation")
                original_owner = row["owner"]
            else:
                require(expired is not None and successor is not None and row["owner"]["epoch"] > original_owner["epoch"]
                        and row["owner"]["node"] == "successor", "successor did not replace old authority")
                require(label != "before-release" or not released and not attempted, "recovery audit followed old callback progress")
                require(label == "before-release" or fenced and attempted is not None and interrupted and image is not None, "post-refusal audit preceded fencing")
            require((label == "final") == consumed, "consumer effect preceded authorized processing")
            for field in ("payload", "sha256", "offset", "attempts", "ack"):
                require(row["operations"]["cross"].get(field) == operations["cross"].get(field), "audit changed original command/receipt")
            snapshot = row["snapshot"]
            require(snapshot["sources"]["cross"]["host"] == ("producer" if label == "before-isolation" else "successor")
                    and snapshot["effects"]["host"] == "consumer", "audit used another source/effect host")
            accepted = validate_snapshot(operations, snapshot, ["cross"] if consumed else [], int(consumed), {"cross": 0})
            for key, receipt in accepted.items():
                require(operations[key].get("accepted") in (None, receipt), "accepted envelope or effect changed")
                operations[key]["accepted"] = receipt
            require(consumed or len(snapshot["events"]["records"]) == 1, "original event disappeared before replay")
            stages.append(label)
        elif event == "live_owner_attempt":
            require(stages == ["before-isolation"] and not stale_attempted and row["pid"] == pids["producer"]
                    and row["cut"] == cut and row["payload"] == dict(order_id="cross", amount_cents=999)
                    and row["sha256"] == digest(row["payload"]), "changed or repeated stale probe")
            stale_attempted = True
        elif event == "live_owner_entered":
            entered = row["phase"]
            require(stages == ["before-isolation"] and stale_attempted and not isolated and entered["boundary"] == "source_poll_entered"
                    and entered["pid"] == pids["producer"] and entered["cut"] == cut
                    and entered["scope"] == operations["cross"]["ack"]["source"], "wrong entered callback")
        elif event == "live_owner_isolated":
            require(entered is not None and not isolated and row["pid"] == pids["producer"] and row["state"] and row["state"][0].upper() not in "TZ", "old process not live during isolation")
            isolated = True
        elif event == "live_owner_authority_expired":
            require(isolated and expired is None and row["lease"]["node"] == "producer"
                    and row["observed_ms"] > row["lease"]["expires_ms"]
                    and any(item["path"] == "nodes/producer.json" and item["operation"] in ("get", "put") for item in row["refused"]), "authority expiry/refusal was not observed")
            expired = row
        elif event == "live_owner_successor":
            require(expired is not None and successor is None and row["pid"] not in pids.values(), "successor reused an original process")
            successor = row["pid"]
        elif event == "live_owner_released":
            require(stages == ["before-isolation", "before-release"] and not released and row["phase"] == entered
                    and row["pid"] == pids["producer"] and row["state"] and row["state"][0].upper() not in "TZ"
                    and 0 <= row["elapsed_ms"] < 10000, "callback release preceded recovery or exceeded original budget")
            released = True
        elif event == "live_owner_attempted":
            require(released and attempted is None and row["phase"]["boundary"] == "source_poll_attempted"
                    and all(row["phase"][key] == entered[key] for key in ("pid", "scope", "cut"))
                    and row["phase"]["refused"] is True and row["phase"]["closed_domain"] is True, "stale callback committed or lacked closed-domain refusal")
            attempted = row["phase"]
        elif event == "live_owner_fenced":
            require(attempted is not None and not fenced and row["pid"] == pids["producer"] and row["exit_code"] == 1
                    and row["report"] == dict(outcome="fenced", fence_code=3, reason="node_lease_expired") and row["graceful_reports"] == 0, "old process did not join with lease expiry")
            fenced = True
        elif event == "live_owner_response_interrupted":
            require(fenced and not interrupted and row["kind"] in ("AuditError", "OSError", "HTTPException", "RemoteDisconnected", "ConnectionResetError"), "stale callback returned success or never terminated")
            interrupted = True
        elif event == "live_owner_old_image":
            require(fenced and interrupted and image is None, "forensic image preceded old process join")
            image = row["image"]
            require(image["order"] == operations["cross"]["payload"] and image["receipt"] == operations["cross"]["ack"]
                    and image["lane"] == image["receipt"]["lane"] and image["next_sequence"] == "2"
                    and image["settled_through"] == "1" and image["pending_rows"] == "0" and "db.sqlite" in image["files"], "old source committed stale state or event")
        elif event == "live_owner_consumed":
            require(stages == ["before-isolation", "before-release", "after-refusal"] and not consumed
                    and row["fresh_effects"] == 1 and row["status"] == "acknowledged", "effect missing, repeated or premature")
            consumed = True
        elif event == "fleet_process_stopped":
            require(stages == list(STAGES) and row["node"] in {"destination", "consumer", "successor"} - joined
                    and row["pid"] == (successor if row["node"] == "successor" else pids[row["node"]])
                    and row["exit_code"] == 0 and len(row["reports"]) == 1
                    and all(row["reports"][0].get(key) is True for key in ("ingress_drained", "application_work_drained", "cells_drained")), "incomplete survivor shutdown")
            joined.add(row["node"])
        elif event == "network_store_removed":
            require(store is not None and not removed and joined == {"destination", "consumer", "successor"} and row["container"] == store["container"], "store removed before survivor joins")
            removed = True
        else:
            require(False, f"unknown live-owner evidence: {event}")
    if complete:
        require(stages == list(STAGES) and joined == {"destination", "consumer", "successor"} and (store is None or removed), "incomplete live-owner qualification")
        require(inspect_image(path.parent / "old-source-image", operations["cross"]["ack"]["source"]) == image, "forensic archive changed")
    return dict(audits=len(stages), live_callback_takeovers=int(fenced), cut=cut, effects=int(consumed), backend="s3" if store else "sqlite")


def exercise(binary, root, rustfs_image=None, cut="before_tx"):
    require(os.name == "posix", "live-owner qualification requires POSIX process observation")
    root.mkdir(parents=True, exist_ok=False)
    ledger = Ledger(root, commands=(("cross", 725, 1),))
    store = NetworkStore(root, ledger, rustfs_image) if rustfs_image else None
    processes, origins, producer = [], {}, None
    data = root / "data/producer"
    try:
        if store: store.start()
        for node in ("destination", "producer", "consumer"):
            address = free_address()
            process = FleetProcess(binary, root, node, node, address, address, node == "destination", store,
                                   fleet_durability=False, node_lease_ms=1000, owner_fault=node == "producer")
            processes.append(process)
            origins[node] = process.ready()
            if node == "destination": require(read(origins[node], "/__test/events")["records"] == [], "nonempty fixture stream")
        producer = processes[1]
        ledger.record("live_owner_started", pids={process.node: process.child.pid for process in processes}, cut=cut,
                      proof_mode="bucket", node_lease_ms=1000, handler_budget_ms=10000)
        ledger.issue(origins["producer"], "cross")
        eventually(lambda: read(origins["producer"], "/orders/cross"), lambda state: state["outbox"]["pending_rows"] == "0")
        scope = ledger.operations["cross"]["ack"]["source"]
        def audit(label):
            snapshot = dict(sources={"cross": read(origins["producer"], "/orders/cross")}, events=read(origins["destination"], "/__test/events"),
                            effects=read(origins["consumer"], "/__test/projection-receipts"), subscription=read(origins["destination"], "/subscription"))
            ledger.record("live_owner_audit", label=label, owner=authority(root, store, scope), operations=ledger.operations, snapshot=snapshot)
            audit_evidence(ledger.path, complete=False)
            ledger.audited = True
        audit("before-isolation")
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            began = time.monotonic()
            probe = dict(order_id="cross", amount_cents=999)
            ledger.record("live_owner_attempt", pid=producer.child.pid, cut=cut, payload=probe, sha256=digest(probe))
            original = pool.submit(read, origins["producer"], f"/orders/cross?amount_cents=999&running_cut={cut}", "POST", 20)
            entered = eventually(lambda: phase(data), lambda value: value.get("boundary") == "source_poll_entered", timeout=5)
            ledger.record("live_owner_entered", phase=entered)
            marker(data / "owner-isolate")
            ledger.record("live_owner_isolated", pid=producer.child.pid, state=live(producer))
            def expired():
                path = data / "owner-refused.jsonl"
                raw = path.read_bytes() if path.exists() else b""
                complete = raw[:raw.rfind(b"\n")+1]
                refused = [json.loads(line) for line in complete.splitlines()]
                return dict(lease=store_json(root, store, "nodes/producer.json"), observed_ms=time.time_ns() // 1000000, refused=refused)
            observed = eventually(expired, lambda value: value["observed_ms"] > value["lease"]["expires_ms"] and value["refused"], timeout=5)
            ledger.record("live_owner_authority_expired", **observed)
            address = free_address()
            successor = FleetProcess(binary, root, "successor", "successor", address, address, False, store, fleet_durability=False, node_lease_ms=1000)
            processes.append(successor)
            ledger.record("live_owner_successor", pid=successor.child.pid)
            origins["producer"] = successor.ready()
            audit("before-release")
            require(phase(data) == entered, "old callback ended before successor audit")
            ledger.record("live_owner_released", pid=producer.child.pid, state=live(producer), phase=phase(data), elapsed_ms=int((time.monotonic()-began)*1000))
            audit_evidence(ledger.path, complete=False)
            marker(data / "owner-release", b"release")
            attempted = eventually(lambda: phase(data), lambda value: value.get("boundary") == "source_poll_attempted", timeout=5)
            ledger.record("live_owner_attempted", phase=attempted)
            code = producer.child.wait(timeout=10)
            producer.output.close()
            reports = [json.loads(line) for line in producer.log.read_text().splitlines() if line.startswith('{"')]
            fencing = [row for row in reports if row.get("outcome") == "fenced"]
            require(len(fencing) == 1, "old node lacks fencing report")
            ledger.record("live_owner_fenced", pid=producer.child.pid, exit_code=code, report=fencing[0], graceful_reports=sum(row.get("outcome") == "stopped" for row in reports))
            try: result = original.result(timeout=5)
            except concurrent.futures.TimeoutError as error: raise AuditError("old request did not terminate") from error
            except (AuditError, OSError, http.client.HTTPException) as error:
                kind = "AuditError" if isinstance(error, AuditError) else "OSError" if isinstance(error, OSError) else "HTTPException"
                ledger.record("live_owner_response_interrupted", kind=kind, error_type=type(error).__name__)
            else: raise AuditError(f"stale callback returned success: {result}")
        ledger.record("live_owner_old_image", image=archive_source(root, scope))
        audit("after-refusal")
        result = read(origins["consumer"], "/projection/process", "POST")
        ledger.record("live_owner_consumed", fresh_effects=result["fresh_effects"], status=result["status"])
        audit("final")
        for process in reversed(processes): process.stop_recorded(ledger)
        if store: store.close()
        summary = dict(audit_evidence(ledger.path), status="passed", binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(), output=str(root),
                       scope="four independent loopback processes; live source poll through store-client isolation, expiry and takeover; recovery audited before callback release")
        (root / "summary.json").write_text(json.dumps(summary, indent=2)+"\n")
        print(json.dumps(summary), flush=True)
    finally:
        if producer is not None and producer.child.poll() is None and not (data / "owner-release").exists(): marker(data / "owner-release", b"release")
        for process in reversed(processes): process.stop()
        if store: store.close()
        ledger.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--binary", type=Path); target.add_argument("--audit", type=Path)
    parser.add_argument("--output", type=Path); parser.add_argument("--rustfs-image")
    parser.add_argument("--cut", choices=("before_tx", "before_commit"), default="before_tx")
    args = parser.parse_args()
    if args.audit: print(json.dumps(audit_evidence(args.audit.resolve())), flush=True)
    else: exercise(args.binary.resolve(), (args.output or Path(tempfile.mkdtemp(prefix="cellar-live-owner-parent-"))/"evidence").resolve(), args.rustfs_image, args.cut)
