#!/usr/bin/env python3
"""Recover an acknowledged pending outbox from followers after leader data loss."""
import argparse
import copy
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import tempfile
import time
import urllib.parse
import xml.etree.ElementTree as ET

from faults import Ledger, digest, eventually, read, require, validate_snapshot
from fleet_faults import FleetProcess, NetworkStore, free_address
from resumed_owner import authority, store_json, store_read
from whole_fleet import inventory
from before_send import inspect_crash_image


ROLES = ("destination", "producer", "consumer")
STAGES = {
    "before-loss": ([], 0, {"cross": 0, "waiting": 1}),
    "recovered-before-replay": ([], 0, {"cross": 0, "waiting": 1}),
    "first-acknowledged": (["cross"], 1, {"cross": 0, "waiting": 0}),
    "final": (["cross", "waiting"], 2, {"cross": 0, "waiting": 0}),
}


def replica_inventory(root, store):
    if store is None:
        with sqlite3.connect((root / "fleet.sqlite").resolve().as_uri() + "?mode=ro", uri=True) as db:
            rows = db.execute("SELECT key,body FROM __sys_local_objects WHERE key LIKE 'cells/%/ltx/%' ORDER BY key LIMIT 1001").fetchall()
    else:
        query = urllib.parse.urlencode({"list-type": "2", "max-keys": "1000", "prefix": "cells/"}, quote_via=urllib.parse.quote)
        listing = ET.fromstring(store_read(root, store, "", query))
        require(listing.findtext("{*}IsTruncated") == "false", "fixture object inventory exceeded one bounded page")
        keys = [entry.findtext("{*}Key") for entry in listing.findall("{*}Contents")]
        rows = [(key, store_read(root, store, key)) for key in keys if "/ltx/" in key]
    require(len(rows) <= 1000, "fixture replica inventory exceeded limit")
    return {key: dict(bytes=len(body), sha256=hashlib.sha256(body).hexdigest()) for key, body in rows}


def source_inventory(objects, scope):
    return {key: value for key, value in objects.items() if key.startswith(f"cells/{scope}/ltx/")}


def archive_pending_source(root, scope):
    require(scope.startswith("Order:") and len(scope) == 70 and all(c in "0123456789abcdef" for c in scope[6:]), "unsafe source archive scope")
    databases = list((root / "data/producer/node" / scope / "ltx").glob("e*/db.sqlite"))
    require(len(databases) == 1, "unexpected source epoch inventory")
    archive = root / "source-image"
    archive.mkdir()
    for suffix in ("", "-wal", "-shm"):
        source = Path(str(databases[0]) + suffix)
        if source.exists():
            target = archive / ("db.sqlite" + suffix)
            shutil.copy2(source, target)
            with target.open("rb") as saved:
                os.fsync(saved.fileno())
    return inspect_crash_image(archive, scope)


def audit(ledger, origins, label, owner):
    ledger.audited = False
    snapshot = dict(sources={key: read(origins["producer"], "/orders/" + key) for key in ledger.operations},
                    events=read(origins["destination"], "/__test/events"),
                    effects=read(origins["consumer"], "/__test/projection-receipts"),
                    subscription=read(origins["destination"], "/subscription"))
    accepted = validate_snapshot(ledger.operations, snapshot, *STAGES[label])
    ledger.record("follower_audit", label=label, operations=ledger.operations, owner=owner, snapshot=snapshot)
    for key, receipt in accepted.items():
        ledger.operations[key]["accepted"] = receipt
    # Audit the saved prefix before permitting any effect, ack, or command replay.
    audit_evidence(ledger.path, complete=False)
    ledger.audited = True


def audit_evidence(path, complete=True):
    operations = pids = baseline = proof = lost = replacement = owner = store = image = None
    stages, joined = set(), set()
    removed = discarded = expired = False
    consumed = 0
    for line in path.read_text().splitlines():
        row = json.loads(line)
        event = row["event"]
        if event == "plan":
            require(operations is None and set(row["operations"]) == {"cross", "waiting"}, "invalid follower command plan")
            operations = copy.deepcopy(row["operations"])
            for key, amount, offset in (("cross", 725, 1), ("waiting", 375, 2)):
                operation = operations[key]
                require(operation["payload"] == dict(order_id=key, amount_cents=amount) and operation["offset"] == offset
                        and operation["sha256"] == digest(operation["payload"]) and operation["attempts"] == 0, "changed follower command plan")
        elif event == "network_store_startup_retry":
            require(pids is None and store is None and row["status"] == 503, "application failure hidden by startup retry")
        elif event == "network_store_started":
            require(store is None and pids is None and row["backend"] == "s3" and row["image_id"].startswith("sha256:")
                    and row["container"].startswith("cellar-stream-store-"), "invalid private store identity")
            store = row
        elif event == "follower_fleet_started":
            require(pids is None and row["proof_mode"] == "fleet" and row["replica_fault_node"] == "producer", "missing fleet-only proof control")
            pids = row["pids"]
            require(set(pids) == set(ROLES) and len(set(pids.values())) == 3, "fleet processes are not independent")
        elif event == "attempt":
            key = row["key"]
            require(pids is not None and key in operations and row["attempt"] == 1 and not row["replay"]
                    and operations[key]["attempts"] == 0 and not lost and (key != "waiting" or baseline is not None),
                    "replay or unordered command could repair follower recovery")
            operations[key]["attempts"] = 1
        elif event == "ack":
            operation = operations[row["key"]]
            require(operation["attempts"] == 1 and "ack" not in operation and not lost, "invalid original acknowledgment")
            operation["ack"] = row["receipt"]
        elif event == "replica_publication_blocked":
            require(baseline is None and operations["cross"].get("ack") and operations["waiting"]["attempts"] == 0,
                    "bucket block followed the transaction")
            baseline = row["objects"]
        elif event == "follower_only_acknowledged":
            require(proof is None and baseline is not None and operations["waiting"].get("ack") and not lost,
                    "follower proof lacks an original source acknowledgment")
            proof = row
            scope = operations["waiting"]["ack"]["source"]
            require(source_inventory(baseline, scope) == source_inventory(row["objects"], scope),
                    "bucket publication bypassed follower-only boundary")
            require(row["refused"] and any(entry["path"].startswith(f"cells/{scope}/ltx/") for entry in row["refused"]),
                    "source bucket upload was not actually refused")
            log = row["node_lease"]["log"]
            require(row["node_lease"]["node"] == "producer" and log["state"] == "open" and log["may_have_fleet_acks"] is True
                    and log["ensemble"] and set(log["ensemble"]) <= {"destination", "consumer"}, "no installed real follower ensemble")
            owner = row["owner"]
            require(owner["node"] == "producer" and type(owner["epoch"]) is int, "wrong source proof owner")
        elif event == "follower_audit":
            label = row["label"]
            require(label in STAGES and label not in stages and list(STAGES).index(label) == len(stages), "reordered follower audit")
            if label == "before-loss":
                require(proof is not None and not lost and row["owner"] == owner, "loss preceded original pending obligation audit")
            else:
                require(discarded and replacement is not None and row["owner"]["node"] == "successor"
                        and row["owner"]["epoch"] > owner["epoch"], "follower recovery did not cross independent ownership")
            if label in ("first-acknowledged", "final"):
                require(consumed == (1 if label == "first-acknowledged" else 2), "consumer effect preceded recovery audit")
            for key, operation in operations.items():
                for field in ("payload", "sha256", "offset", "attempts", "ack"):
                    require(row["operations"][key].get(field) == operation.get(field), "audit changed original command or acknowledgment")
            accepted = validate_snapshot(operations, row["snapshot"], *STAGES[label])
            expected_host = "producer" if label == "before-loss" else "successor"
            require(all(source["host"] == expected_host for source in row["snapshot"]["sources"].values())
                    and row["snapshot"]["effects"]["host"] == "consumer", "audit observed the wrong owner")
            require(label not in ("before-loss", "recovered-before-replay")
                    or [record["order_id"] for record in row["snapshot"]["events"]["records"]] == ["cross"],
                    "accepted event disappeared before consumer replay")
            for key, receipt in accepted.items():
                operations[key]["accepted"] = receipt
            if "waiting" in accepted:
                require(image is not None and accepted["waiting"]["envelope_sha256"] == image["envelope_sha256"],
                        "recovered encoded event differs from the killed leader's pending obligation")
            stages.add(label)
        elif event == "process_killed":
            require(lost is None and "before-loss" in stages and row["label"] == "follower-producer"
                    and row["pid"] == pids["producer"] and row["exit_code"] == -9, "wrong leader loss")
            lost = row
        elif event == "follower_source_image":
            require(lost and image is None and not discarded, "source image was not copied from the killed leader")
            image = row["image"]
            receipt = operations["waiting"]["ack"]
            require(image["order"] == operations["waiting"]["payload"] and image["receipt"] == receipt
                    and image["incarnation"] == receipt["incarnation"] and image["lane"] == receipt["lane"]
                    and image["sequence"] == image["rows"] == "1" and image["next_sequence"] == "2" and image["settled_through"] == "0"
                    and len(image["envelope_sha256"]) == 64 and "db.sqlite" in image["files"], "archived pending obligation changed")
        elif event == "follower_leader_data_discarded":
            require(lost and image is not None and not discarded and replacement is None and row["remaining_entries"] == 0 and row["files"], "leader local state survived loss")
            paths = set()
            for entry in row["files"]:
                name = Path(entry["path"])
                require(not name.is_absolute() and ".." not in name.parts and entry["path"] not in paths
                        and type(entry["bytes"]) is int and entry["bytes"] >= 0 and len(entry["sha256"]) == 64, "invalid discarded local inventory")
                paths.add(entry["path"])
            discarded = True
        elif event == "follower_successor_started":
            require(discarded and expired and replacement is None and row["node"] == "successor" and row["pid"] not in pids.values(), "replacement reused lost local state or live authority")
            replacement = row
        elif event == "follower_authority_expired":
            require(discarded and not expired and replacement is None and row["lease"]["node"] == "producer"
                    and row["lease"]["generation"] == proof["node_lease"]["generation"]
                    and row["lease"]["expires_ms"] >= proof["node_lease"]["expires_ms"]
                    and row["observed_ms"] > row["lease"]["expires_ms"], "takeover did not observe actual old authority expiry")
            expired = True
        elif event == "follower_consumer_acknowledged":
            require("recovered-before-replay" in stages and consumed < 2 and (consumed == 0 or "first-acknowledged" in stages)
                    and row["fresh_effects"] == 1 and row["status"] == "acknowledged", "repeated, skipped or premature inbox effect")
            consumed += 1
        elif event == "fleet_process_stopped":
            expected = replacement["pid"] if row["node"] == "successor" else pids[row["node"]]
            require("final" in stages and row["node"] in {"destination", "consumer", "successor"} - joined and row["pid"] == expected
                    and row["exit_code"] == 0 and len(row["reports"]) == 1 and all(row["reports"][0].get(key) is True
                        for key in ("ingress_drained", "application_work_drained", "cells_drained")), "incomplete survivor shutdown")
            joined.add(row["node"])
        elif event == "network_store_removed":
            require(store is not None and not removed and joined == {"destination", "consumer", "successor"}
                    and row["container"] == store["container"], "store removed before complete node shutdown")
            removed = True
        else:
            require(False, f"unknown follower evidence: {event}")
    if complete:
        require(stages == set(STAGES) and consumed == 2 and joined == {"destination", "consumer", "successor"}
                and (store is None or removed), "incomplete follower-only qualification")
        require(inspect_crash_image(path.parent / "source-image", operations["waiting"]["ack"]["source"]) == image,
                "archived source database/WAL changed after the kill")
    return dict(audits=len(stages), follower_only_recoveries=int("recovered-before-replay" in stages),
                effects=consumed, backend="s3" if store else "sqlite")


def exercise(binary, root, rustfs_image=None):
    require(os.name == "posix", "follower qualification requires SIGKILL")
    root.mkdir(parents=True, exist_ok=False)
    ledger = Ledger(root, commands=(("cross", 725, 1), ("waiting", 375, 2)))
    store = NetworkStore(root, ledger, rustfs_image) if rustfs_image else None
    processes, origins = [], {}
    try:
        if store:
            store.start()
        for node in ROLES:
            peer = free_address()
            process = FleetProcess(binary, root, node, node, peer, peer, node == "destination", store,
                                   fleet_durability=True, replica_fault=node == "producer", node_lease_ms=1000)
            processes.append(process)
            origins[node] = process.ready()
            if node == "destination":
                require(read(origins[node], "/__test/events")["records"] == [], "nonempty private fixture stream")
        ledger.record("follower_fleet_started", pids={node: process.child.pid for node, process in zip(ROLES, processes)},
                      proof_mode="fleet", replica_fault_node="producer")
        ledger.issue(origins["producer"], "cross")
        eventually(lambda: read(origins["producer"], "/orders/cross"), lambda state: state["outbox"]["pending_rows"] == "0")
        # Initialize this empty source before taking the object inventory.
        require(read(origins["producer"], "/orders/waiting")["order"] is None, "test command already exists")
        eventually(lambda: store_json(root, store, "nodes/producer.json"),
                   lambda lease: lease.get("log") is not None and lease["log"]["state"] == "open" and lease["log"]["ensemble"])
        data = root / "data/producer"
        with (data / "replica-block").open("xb") as marker:
            marker.flush()
            os.fsync(marker.fileno())
        ledger.record("replica_publication_blocked", objects=replica_inventory(root, store))
        command = ledger.issue(origins["producer"], "waiting")
        scope = command["receipt"]["source"]
        def refused():
            path = data / "replica-refused.jsonl"
            return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []
        denied = eventually(refused, lambda rows: any(row["path"].startswith(f"cells/{scope}/ltx/") for row in rows))
        original_owner = authority(root, store, scope)
        ledger.record("follower_only_acknowledged", owner=original_owner, node_lease=store_json(root, store, "nodes/producer.json"),
                      objects=replica_inventory(root, store), refused=denied)
        audit(ledger, origins, "before-loss", original_owner)
        processes[1].crash(ledger, "follower-producer")
        ledger.record("follower_source_image", image=archive_pending_source(root, scope))
        files = inventory(data)
        shutil.rmtree(data)
        require(not data.exists(), "old leader data survived removal")
        ledger.record("follower_leader_data_discarded", files=files, remaining_entries=0)
        old_lease = store_json(root, store, "nodes/producer.json")
        observed = eventually(lambda: int(time.time() * 1000), lambda now: now > old_lease["expires_ms"])
        ledger.record("follower_authority_expired", lease=old_lease, observed_ms=observed)
        peer = free_address()
        successor = FleetProcess(binary, root, "successor", "successor", peer, peer, False, store, node_lease_ms=1000)
        processes.append(successor)
        origins["producer"] = successor.ready()
        ledger.record("follower_successor_started", node="successor", pid=successor.child.pid)
        # These are reads only. No original source POST is ever replayed.
        read(origins["producer"], "/orders/waiting")
        new_owner = authority(root, store, scope)
        audit(ledger, origins, "recovered-before-replay", new_owner)
        for label in ("first-acknowledged", "final"):
            require(ledger.audited, "consumer entered before recovery audit")
            result = read(origins["consumer"], "/projection/process", "POST")
            ledger.record("follower_consumer_acknowledged", fresh_effects=result["fresh_effects"], status=result["status"])
            if label == "first-acknowledged":
                eventually(lambda: read(origins["producer"], "/orders/waiting"), lambda state: state["outbox"]["pending_rows"] == "0")
            audit(ledger, origins, label, new_owner)
        for process in reversed(processes):
            process.stop_recorded(ledger)
        if store:
            store.close()
        summary = dict(audit_evidence(ledger.path), status="passed", binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
                       output=str(root), scope="follower-only source proof; real replica upload refusal; leader SIGKILL and local data loss; recovery before replay")
        (root / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
        print(json.dumps(summary), flush=True)
    finally:
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
        exercise(args.binary.resolve(), (args.output or Path(tempfile.mkdtemp(prefix="cellar-follower-parent-")) / "evidence").resolve(), args.rustfs_image)
