#!/usr/bin/env python3
"""Lose a proven fetch reply across destination SIGKILL; audit before exact replay."""
import argparse
import base64
import concurrent.futures
import hashlib
import json
import os
from pathlib import Path
import tempfile

from faults import AuditError, Ledger, integer, read, require, validate_event, validate_snapshot
from fleet_faults import FleetProcess, NetworkStore, PeerProxy, free_address

FETCH_ID = "000000000000000000000000000000c1"
ROLES = {"producer", "destination", "consumer"}
STAGES = ("accepted-before-fetch", "before-destination-loss", "recovered-before-replay",
          "effect-proven", "effect-replayed", "final")
TRANSIENT = {"Ambiguous", "Unavailable", "OwnershipLost", "transport_ambiguous"}


def validate_exchange(exchange):
    require(exchange["method"] == "__streamNext" and exchange["scope"].startswith("__EventStream:")
            and len(exchange["scope"].split(":", 1)[1]) == 64, "wrong fetch method/resource")
    for field in ("request", "response"):
        require(hashlib.sha256(base64.b64decode(exchange[field], validate=True)).hexdigest()
                == exchange[field + "_sha256"], "captured fetch bytes changed")


def validate_batch(result, snapshot, operations, original_lease, mode):
    batch = result["batch"]
    encoded = batch["receipt"]
    require(encoded.startswith("v1.") and len(encoded) <= 2048, "invalid recovered receipt")
    raw = encoded[3:]
    receipt = json.loads(base64.urlsafe_b64decode(raw + "=" * (-len(raw) % 4)))
    identity, lease = snapshot["subscription"]["identity"], snapshot["subscription"]["lease"]
    require(receipt["resource"] == identity["resource"] and receipt["subscription"] == identity["id"]
            and receipt["incarnation"] == identity["generation"] and receipt["name"] == identity["name"] == "projection"
            and receipt["partition"] == 0, "recovered receipt identity changed")
    require(integer(receipt["first"]) == integer(receipt["last"]) == 1
            and integer(batch["expires_ms"]) - integer(batch["issued_ms"]) == 30_000, "recovered batch range or lease interval changed")
    if mode == "live" or snapshot["subscription"]["observation"]["lease_active"]:
        require(lease is not None and all(receipt[key] == lease[key] for key in ("generation", "first", "last"))
                and batch["expires_ms"] == lease["expires_ms"], "recovered lease changed")
    else:
        require(integer(receipt["generation"]) > integer(original_lease["generation"])
                and integer(batch["issued_ms"]) >= integer(original_lease["expires_ms"]), "expired fetch did not obtain fresh authority")
    require(len(receipt["token"]) == 64 and all(c in "0123456789abcdef" for c in receipt["token"]), "invalid lease token")
    require(len(batch["records"]) == 1, "recovered batch count changed")
    validate_event(operations["cross"], batch["records"][0])
    return hashlib.sha256(encoded.encode()).hexdigest()


def audit(ledger, origins, label):
    ledger.audited = False
    snapshot = dict(sources={"cross": read(origins["producer"], "/orders/cross")},
                    events=read(origins["destination"], "/__test/events"),
                    effects=read(origins["consumer"], "/__test/projection-receipts"),
                    subscription=read(origins["destination"], "/subscription"))
    effects = list(snapshot["effects"]["receipts"])
    accepted = validate_snapshot(ledger.operations, snapshot, effects,
                                 1 if label == "final" else 0, {"cross": 0})
    ledger.record("fetch_audit", label=label, operations=ledger.operations, snapshot=snapshot)
    # Validate this actual prefix before marking it safe for another request.
    audit_evidence(ledger.path, complete=False)
    for key, receipt in accepted.items():
        ledger.operations[key]["accepted"] = receipt
    ledger.record("fetch_audit_passed", label=label)
    ledger.audited = True
    return snapshot


def audit_evidence(path, complete=True):
    operations = pids = held = replacement = original_lease = identity = batch = None
    mode = replay_wire = replay_lease = last_snapshot = None
    snapshots, passed, stopped = {}, set(), set()
    killed = dropped = resumed = False
    attempt = successes = forwarded = 0
    pending = None
    can_attempt = False
    last_effects = 0
    last_status = None
    receipt_digest = None
    final_ack = False
    store = None
    removed = False
    startup_retries = 0
    for line in path.read_text().splitlines():
        row = json.loads(line)
        event = row["event"]
        if event == "plan":
            require(operations is None and set(row["operations"]) == {"cross"}, "invalid fetch command plan")
            operations = row["operations"]
        elif event == "network_store_startup_retry":
            require(operations is not None and pids is None and store is None and row["status"] == 503,
                    "store startup retry occurred during application traffic")
            startup_retries += 1
        elif event == "network_store_started":
            require(store is None and pids is None and row["backend"] == "s3" and row["image_id"].startswith("sha256:")
                    and row["container"].startswith("cellar-stream-store-"), "invalid private fetch store")
            store = row
        elif event == "attempt":
            require(row["key"] == "cross" and row["attempt"] == 1 and not row["replay"]
                    and operations["cross"]["attempts"] == 0, "source replay could conceal missing acceptance")
            operations["cross"]["attempts"] = 1
        elif event == "ack":
            require(row["key"] == "cross" and operations["cross"]["attempts"] == 1 and "ack" not in operations["cross"], "invalid source ack")
            operations["cross"]["ack"] = row["receipt"]
        elif event == "fetch_fleet_started":
            require(pids is None and set(row["pids"]) == ROLES and len(set(row["pids"].values())) == 3,
                    "fetch roles were not separate processes")
            pids = row["pids"]
            mode = row["lease_state"]
            require(mode in ("live", "expired") and row["node_lease_ms"] == (1000 if mode == "live" else 30000),
                    "unrecorded or changed authority timing")
        elif event == "fetch_attempt":
            require(can_attempt and pending is None and row["attempt"] == attempt + 1 and row["fetch_id"] == FETCH_ID,
                    "fetch replay changed identity or preceded its independent audit")
            require((attempt == 0 and not killed) or (resumed and "recovered-before-replay" in passed), "premature fetch replay")
            attempt += 1
            pending = attempt
            can_attempt = False
        elif event == "fetch_reply_held":
            require(held is None and pending == 1 and "accepted-before-fetch" in passed, "fetch proof preceded initial audit")
            held = row["exchange"]
            validate_exchange(held)
        elif event == "fetch_audit":
            label, snapshot = row["label"], row["snapshot"]
            require(label not in snapshots, "repeated fetch audit")
            effects = list(snapshot["effects"]["receipts"])
            require(len(effects) >= last_effects, "proven consumer effect disappeared before replay")
            checkpoint = 1 if label == "final" else 0
            if label == "accepted-before-fetch":
                require(pids is not None and held is None and attempt == 0 and not effects, "initial fetch state changed")
                require(snapshot["subscription"]["lease"] is None, "initial fetch already leased")
            elif label == "before-destination-loss":
                require(held is not None and pending == 1 and not killed and not effects, "fetch loss audit lacks original boundary")
                original_lease = snapshot["subscription"]["lease"]
                identity = snapshot["subscription"]["identity"]
                require(original_lease is not None and original_lease["first"] == original_lease["last"] == "1", "original lease missing")
            elif label == "recovered-before-replay":
                require(killed and dropped and replacement is not None and pending is None and not resumed and not effects,
                        "recovery audit followed replay or repaired effects")
            elif label in ("effect-proven", "effect-replayed"):
                require(successes == (1 if label == "effect-proven" else 2) and pending is None and effects == ["cross"],
                        "effect audit lacks successful exact fetch replay")
            elif label.startswith("fetch-interrupted-"):
                require(resumed and pending is None and last_status in TRANSIENT and effects in ([], ["cross"]), "invalid interrupted fetch audit")
                require(label == f"fetch-interrupted-{attempt}" and successes < 2, "interrupted fetch audit has wrong attempt")
            elif label == "final":
                require(final_ack and successes == 2 and "effect-replayed" in passed and effects == ["cross"], "final fetch audit lacks acknowledgment")
                require(snapshot["subscription"]["lease"] is None, "final ack retained lease")
            else:
                require(False, "unknown fetch audit stage")
            require(snapshot["sources"]["cross"]["host"] == "producer" and snapshot["effects"]["host"] == "consumer", "fetch roles changed")
            require(set(row["operations"]) == {"cross"}, "fetch command inventory changed")
            for field in ("payload", "sha256", "offset", "attempts", "ack"):
                require(row["operations"]["cross"].get(field) == operations["cross"].get(field), "original command changed")
            accepted = validate_snapshot(row["operations"], snapshot, effects, checkpoint, {"cross": 0})
            for key, receipt in accepted.items():
                require(operations[key].get("accepted") in (None, receipt), "accepted event changed")
                operations[key]["accepted"] = receipt
            if original_lease is not None:
                require(snapshot["subscription"]["identity"] == identity, "subscription incarnation changed")
                if checkpoint == 0:
                    sub = snapshot["subscription"]
                    active = sub["observation"]["lease_active"]
                    observed = integer(sub["observation"]["observed_ms"])
                    if mode == "live" or label == "before-destination-loss":
                        require(sub["lease"] == original_lease and active is True
                                and observed < integer(original_lease["expires_ms"]), "original lease lost, expired or replaced")
                    elif label == "recovered-before-replay":
                        require(active is False and observed >= integer(original_lease["expires_ms"])
                                and sub["lease"] in (None, original_lease), "expiry recovery fabricated or renewed authority")
                    elif active:
                        lease = sub["lease"]
                        require(integer(lease["generation"]) > integer(original_lease["generation"])
                                and observed < integer(lease["expires_ms"]), "fresh lease is obsolete or expired")
                        require(replay_lease in (None, lease), "post-expiry lease changed during replay")
                        replay_lease = lease
                        if batch is not None:
                            raw = batch["receipt"][3:]
                            receipt = json.loads(base64.urlsafe_b64decode(raw + "=" * (-len(raw) % 4)))
                            require(all(receipt[key] == lease[key] for key in ("generation", "first", "last"))
                                    and batch["expires_ms"] == lease["expires_ms"], "fresh receipt differs from independently observed lease")
                    else:
                        require(label.startswith("fetch-interrupted-") and not effects and replay_lease is None
                                and sub["lease"] in (None, original_lease), "fresh fetch authority disappeared")
            if effects:
                observed = snapshot["effects"].get("ack_receipt_sha256")
                require(isinstance(observed, str) and len(observed) == 64, "proven saved receipt missing")
                if receipt_digest is not None:
                    require(observed == receipt_digest, "saved receipt changed")
                else:
                    receipt_digest = observed
            last_effects = len(effects)
            last_snapshot = snapshot
            snapshots[label] = snapshot
        elif event == "fetch_audit_passed":
            require(row["label"] in snapshots and row["label"] not in passed, "fetch audit passed without snapshot")
            passed.add(row["label"])
            can_attempt = True
        elif event == "process_killed":
            require(not killed and "before-destination-loss" in passed and row["label"] == "destination-after-proven-fetch"
                    and row["pid"] == pids["destination"] and row["exit_code"] == -9, "wrong or premature fetch destination kill")
            killed = True
        elif event == "fetch_reply_lost":
            require(killed and not dropped and row["request_sha256"] == held["request_sha256"], "fetch reply loss lacks original kill")
            dropped = True
        elif event == "fetch_outcome":
            require(pending == row["attempt"], "fetch outcome lacks original attempt")
            result = row["result"]
            last_status = result["status"]
            if pending == 1:
                require(killed and dropped and replacement is None and result["status"] == "Ambiguous"
                        and result["fresh_effects"] == 0 and result["projection"] == dict(orders=0, total_cents=0), "original fetch loss fabricated effects or success")
            elif result["status"] == "fetched":
                require(resumed and successes < 2 and forwarded >= 1 and result["fresh_effects"] == 1 - last_effects,
                        "fetch effects repeated or reply preceded recovery")
                digest = validate_batch(result, last_snapshot, operations, original_lease, mode)
                require(batch in (None, result["batch"]), "recovered batch changed on replay")
                require(receipt_digest in (None, digest), "fetch changed a previously proven saved receipt")
                batch, receipt_digest = result["batch"], digest
                successes += 1
            else:
                require(resumed and result["status"] in TRANSIENT, "unexpected fetch refusal")
            pending = None
        elif event == "fetch_destination_restarted":
            require(killed and dropped and pending is None and replacement is None and row["pid"] not in pids.values() and not resumed,
                    "fetch restart preceded loss or reused a process")
            replacement = row["pid"]
        elif event == "retry_blocked":
            require(held is not None and not resumed and row["request_sha256"] == held["request_sha256"], "blocked fetch retry changed bytes")
        elif event == "retry_resumed":
            require("recovered-before-replay" in passed and not resumed, "fetch replay concealed recovery")
            resumed = True
        elif event == "fetch_reply_forwarded":
            exchange = row["exchange"]
            validate_exchange(exchange)
            require(resumed and exchange["request"] == held["request"] and exchange["scope"] == held["scope"],
                    "fetch replay request bytes changed")
            if mode == "live":
                require(exchange == held, "live fetch receipt or returned bytes changed")
            else:
                require(exchange["response"] != held["response"] and replay_wire in (None, exchange["response"]),
                        "post-expiry reply reused old authority or changed on replay")
                replay_wire = exchange["response"]
            forwarded += 1
        elif event == "fetch_acknowledged":
            require(successes == 2 and "effect-replayed" in passed and row["status"] == "Acknowledged" and not final_ack,
                    "fresh ack missing or issued before replay audit")
            final_ack = True
        elif event == "peer_unavailable":
            require(row["expected"] is True, "unexpected fetch peer outage ignored")
        elif event == "fleet_process_stopped":
            require("final" in passed and row["node"] in ROLES - stopped and row["exit_code"] == 0
                    and len(row["reports"]) == 1 and all(row["reports"][0].get(key) is True
                        for key in ("ingress_drained", "application_work_drained", "cells_drained")), "incomplete fetch shutdown")
            require(row["pid"] == (replacement if row["node"] == "destination" else pids[row["node"]]), "fetch shutdown names another process")
            stopped.add(row["node"])
        elif event == "network_store_removed":
            require(store is not None and not removed and stopped == ROLES and row["container"] == store["container"], "fetch store removed before shutdown")
            removed = True
        else:
            require(False, f"unknown fetch evidence event: {event}")
    if complete:
        require(set(snapshots) == passed and set(STAGES).issubset(passed) and killed and dropped and resumed
                and successes == 2 and forwarded >= 2 and pending is None and stopped == ROLES
                and (store is None or removed) and (startup_retries == 0 or store is not None), "incomplete fetch fault evidence")
    return dict(audits=len(passed), destination_sigkills=1, response_drops=1, exact_fetch_replies=forwarded,
                backend="s3" if store else "sqlite", lease_state=mode)


def exercise(binary, root, rustfs_image=None, lease_state="live"):
    require(os.name == "posix", "fetch process qualification requires SIGKILL")
    root.mkdir(parents=True, exist_ok=False)
    node_lease_ms = 1000 if lease_state == "live" else 30000
    ledger = Ledger(root, commands=(("cross", 725, 1),))
    processes, proxy = [], None
    store = NetworkStore(root, ledger, rustfs_image) if rustfs_image else None
    try:
        if store:
            store.start()
        peer = free_address()
        proxy = PeerProxy(peer, ledger, "__streamNext")
        destination = FleetProcess(binary, root, "destination", "destination-initial", peer, proxy.address, True, store, node_lease_ms=node_lease_ms)
        processes.append(destination)
        origins = {"destination": destination.ready()}
        require(read(origins["destination"], "/__test/events")["records"] == [], "fetch destination not initially empty")
        for node in ("producer", "consumer"):
            address = free_address()
            process = FleetProcess(binary, root, node, node, address, address, False, store, node_lease_ms=node_lease_ms)
            processes.append(process)
            origins[node] = process.ready()
        ledger.record("fetch_fleet_started", lease_state=lease_state, node_lease_ms=node_lease_ms, pids={node: process.child.pid for node, process in zip(("destination", "producer", "consumer"), processes)})
        ledger.issue(origins["producer"], "cross")
        from faults import eventually
        eventually(lambda: read(origins["producer"], "/orders/cross"), lambda state: state["outbox"]["pending_rows"] == "0")
        audit(ledger, origins, "accepted-before-fetch")
        path = f"/projection/fetch?fetch_id={FETCH_ID}"
        proxy.arm()
        ledger.record("fetch_attempt", attempt=1, fetch_id=FETCH_ID)
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(read, origins["consumer"], path, "POST", 15)
            try:
                require(proxy.entered.wait(5), "proven fetch boundary not reached")
                audit(ledger, origins, "before-destination-loss")
                destination.crash(ledger, "destination-after-proven-fetch")
            finally:
                proxy.release.set()
            result = future.result(timeout=15)
            ledger.record("fetch_outcome", attempt=1, result=result)
            require(result["status"] == "Ambiguous" and result["fresh_effects"] == 0, "consumer did not observe fetch reply loss")
        destination = FleetProcess(binary, root, "destination", "destination-recovered", peer, proxy.address, True, store, node_lease_ms=node_lease_ms)
        processes.append(destination)
        origins["destination"] = destination.ready()
        ledger.record("fetch_destination_restarted", pid=destination.child.pid)
        audit(ledger, origins, "recovered-before-replay")
        proxy.resume("recovered-before-replay")
        attempt = 1
        for label in ("effect-proven", "effect-replayed"):
            for _ in range(10):
                require(ledger.audited, "fetch retry preceded audit")
                attempt += 1
                ledger.record("fetch_attempt", attempt=attempt, fetch_id=FETCH_ID)
                try:
                    result = read(origins["consumer"], path, "POST")
                except (AuditError, OSError) as error:
                    result = dict(status="transport_ambiguous", error=str(error))
                ledger.record("fetch_outcome", attempt=attempt, result=result)
                if result["status"] == "fetched":
                    break
                require(result["status"] in TRANSIENT, "permanent fetch refusal")
                audit(ledger, origins, f"fetch-interrupted-{attempt}")
            else:
                raise AuditError("fetch transport did not recover")
            audit(ledger, origins, label)
        result = read(origins["consumer"], "/projection/ack-stored", "POST")
        ledger.record("fetch_acknowledged", status=result["status"])
        audit(ledger, origins, "final")
        proxy.outage = True
        for process in reversed(processes):
            process.stop_recorded(ledger)
        proxy.close()
        proxy = None
        if store:
            store.close()
        summary = dict(audit_evidence(ledger.path), status="passed", binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
                       output=str(root), scope="three independent loopback processes; lost proven fetch plus destination SIGKILL; explicit live/expired lease replay")
        (root / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
        print(json.dumps(summary), flush=True)
    finally:
        if proxy:
            proxy.release.set()
            proxy.outage = True
        for process in reversed(processes):
            process.stop()
        if proxy:
            proxy.close()
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
    parser.add_argument("--lease-state", choices=("live", "expired"), default="live")
    args = parser.parse_args()
    if args.audit:
        print(json.dumps(audit_evidence(args.audit.resolve())), flush=True)
    else:
        exercise(args.binary.resolve(), (args.output or Path(tempfile.mkdtemp(prefix="cellar-fetch-fault-parent-")) / "evidence").resolve(), args.rustfs_image, args.lease_state)
