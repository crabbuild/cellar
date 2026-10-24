# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Strict replay of live-consumer evidence; no mutation can repair a prefix."""
import base64
from contextlib import closing
import hashlib
import json
from pathlib import Path
import shutil
import sqlite3
import tempfile

from faults import digest, integer, require, validate_event, validate_snapshot

FETCH_ID = "000000000000000000000000000000e2"
STAGES = ("before-isolation", "before-release", "after-refusal", "final")
ROLES = {"destination", "producer", "consumer"}


def inspect_image(archive, scope):
    require(scope.startswith("Projection:") and len(scope) == 75 and all(c in "0123456789abcdef" for c in scope[11:]), "unsafe consumer scope")
    with tempfile.TemporaryDirectory(prefix="cellar-consumer-image-") as directory:
        working, hashes = Path(directory), {}
        for suffix in ("", "-wal", "-shm"):
            source = archive / ("db.sqlite" + suffix)
            require(not source.is_symlink(), "unsafe consumer archive")
            if source.exists():
                shutil.copy2(source, working / source.name)
                if suffix != "-shm": hashes[source.name] = hashlib.sha256(source.read_bytes()).hexdigest()
        require("db.sqlite" in hashes, "consumer database missing")
        with closing(sqlite3.connect((working / "db.sqlite").resolve().as_uri() + "?mode=ro", uri=True)) as db:
            db.execute("PRAGMA query_only=ON")
            values = {key: json.loads(value) for key, value in db.execute("SELECT k,v FROM __sys_cell_kv WHERE scope=? AND k IN ('projection','effect_receipts')", (scope,))}
            rows = [dict(scope=key, event_id=event, digest=bytes(digest).hex()) for key, event, digest in db.execute("SELECT processing_scope,event_id,digest FROM __sys_inbox ORDER BY processing_scope,event_id")]
            count, = db.execute("SELECT rows FROM __sys_inbox_stats WHERE singleton=1").fetchone()
            return dict(projection=values["projection"], receipts=values["effect_receipts"], inbox=rows, count=count, files=hashes)


def validate_delivery(delivery, subscription, operation):
    require(delivery["fetch_id"] == FETCH_ID, "fetch identity changed")
    encoded = delivery["receipt"]
    require(isinstance(encoded, str) and encoded.startswith("v1.") and len(encoded) <= 2048, "invalid delivery receipt")
    raw = encoded[3:]
    receipt = json.loads(base64.urlsafe_b64decode(raw + "=" * (-len(raw) % 4)))
    identity, lease = subscription["identity"], subscription["lease"]
    require(receipt["resource"] == identity["resource"] and receipt["subscription"] == identity["id"]
            and receipt["incarnation"] == identity["generation"] and receipt["name"] == identity["name"] == "projection"
            and receipt["partition"] == 0, "delivery identity changed")
    require(integer(receipt["first"]) == integer(receipt["last"]) == 2
            and integer(delivery["expires_ms"]) - integer(delivery["issued_ms"]) == 30000, "delivery range or policy changed")
    require(lease is not None and subscription["observation"]["lease_active"] is True
            and all(receipt[key] == lease[key] for key in ("generation", "first", "last"))
            and delivery["expires_ms"] == lease["expires_ms"], "current delivery lease changed")
    require(len(receipt["token"]) == 64 and all(c in "0123456789abcdef" for c in receipt["token"]), "invalid lease token")
    require(len(delivery["records"]) == 1, "delivery count changed")
    validate_event(operation, delivery["records"][0])


def stable_subscription(actual, expected):
    require(all(actual[field] == expected[field] for field in ("identity", "checkpoint", "lease", "earliest", "end")), "stale callback changed subscription")


def audit_evidence(path, complete=True):
    operations = pids = cut = owner = original = entered = leased = node_lease = successor = image = successor_image = final_effects = store = None
    seeded = isolated = expired = released = attempted = fenced = interrupted = acked = removed = False
    stages, fetched, joined = [], [], set()
    for line in path.read_text().splitlines():
        row = json.loads(line); event = row["event"]
        if event == "plan":
            require(operations is None and set(row["operations"]) == {"a", "b"}, "invalid consumer plan")
            operations = row["operations"]
            for key, amount, offset in (("a", 1200, 1), ("b", 300, 2)):
                op = operations[key]
                require(op["payload"] == dict(order_id=key, amount_cents=amount) and op["offset"] == offset and op["sha256"] == digest(op["payload"]), "changed consumer command")
        elif event == "network_store_startup_retry":
            require(pids is None and store is None and row["status"] == 503, "hidden application startup failure")
        elif event == "network_store_started":
            require(pids is None and store is None and row["backend"] == "s3" and row["image_id"].startswith("sha256:") and row["container"].startswith("cellar-stream-store-"), "invalid private store")
            store = row
        elif event == "consumer_owner_started":
            require(pids is None and set(row["pids"]) == ROLES and len(set(row["pids"].values())) == 3 and row["proof_mode"] == "bucket"
                    and row["node_lease_ms"] == 1000 and row["handler_budget_ms"] == 10000, "invalid processes or policies")
            pids, cut = row["pids"], row["cut"]
            require(cut in ("before_accept", "before_commit"), "invalid consumer cut")
        elif event == "attempt":
            key = row["key"]
            require(pids is not None and key == ("b" if seeded else "a") and row["attempt"] == 1 and row["replay"] is False and operations[key]["attempts"] == 0 and not stages, "command replay hid recovery")
            operations[key]["attempts"] = 1
        elif event == "ack":
            key = row["key"]
            require(key in operations and operations[key]["attempts"] == 1 and "ack" not in operations[key], "changed source acknowledgment")
            operations[key]["ack"] = row["receipt"]
        elif event == "consumer_owner_seeded":
            require(not seeded and not stages and "ack" in operations["a"] and operations["b"]["attempts"] == 0 and row["result"] == dict(status="acknowledged", fresh_effects=1, projection=dict(orders=1,total_cents=1200)), "invalid seed effect")
            seeded = True
        elif event == "consumer_owner_audit":
            label, snapshot = row["label"], row["snapshot"]
            require(seeded and len(stages) < len(STAGES) and label == STAGES[len(stages)], "reordered consumer audit")
            if label == "before-isolation":
                require(not isolated and entered is None and row["owner"]["node"] == "consumer", "initial audit followed fault")
                owner, original = row["owner"], snapshot["effects"]
                require(snapshot["subscription"]["lease"] is None, "initial batch already leased")
            else:
                require(expired and successor is not None and row["owner"]["node"] == "successor" and row["owner"]["epoch"] > owner["epoch"], "successor did not replace consumer authority")
                require(label != "before-release" or not released and not attempted, "prefix repaired before release audit")
                require(label == "before-release" or fenced and interrupted and image is not None, "audit preceded old process join/image")
                require(snapshot["effects"]["scope"] == original["scope"], "consumer database changed")
                if label != "final":
                    require(all(snapshot["effects"][field] == original[field] for field in ("projection","receipts")), "original effect prefix lost or changed")
                    stable_subscription(snapshot["subscription"], leased)
            final = label == "final"
            require(final == acked and (not final or fetched == ["applied","replayed"]), "final audit preceded settlement")
            require(all(state["host"] == "producer" for state in snapshot["sources"].values()) and snapshot["effects"]["host"] == ("consumer" if not stages else "successor"), "wrong source/consumer host")
            for key, op in operations.items():
                require(all(row["operations"][key].get(field) == op.get(field) for field in ("payload","sha256","offset","attempts","ack")), "operation changed during recovery")
            accepted = validate_snapshot(operations, snapshot, ["a","b"] if final else ["a"], 2 if final else 1, {"a":0,"b":0})
            for key, value in accepted.items(): operations[key]["accepted"] = value
            if final:
                require(snapshot["subscription"]["lease"] is None, "final ack retained a lease")
                final_effects = snapshot["effects"]
            else: require(len(snapshot["events"]["records"]) == 1 and snapshot["events"]["records"][0]["order_id"] == "b", "pending original event lost")
            stages.append(label)
        elif event == "consumer_owner_entered":
            require(entered is None and stages == ["before-isolation"], "repeated or premature consumer entry")
            entered, leased, node_lease = row["phase"], row["subscription"], row["node_lease"]
            require(stages == ["before-isolation"] and not isolated and entered["boundary"] == "consumer_poll_entered" and entered["scope"] == original["scope"]
                    and entered["pid"] == pids["consumer"] and entered["cut"] == cut and entered["accepted"] is (True if cut == "before_commit" else None), "wrong consumer entry cut")
            require(node_lease["node"] == "consumer" and node_lease["generation"], "wrong old node lease")
            validate_delivery(entered["delivery"], leased, operations["b"])
        elif event == "consumer_owner_isolated":
            require(entered is not None and not isolated and row["pid"] == pids["consumer"] and row["state"] and row["state"][0].upper() not in "TZ", "old consumer was stopped or dead")
            isolated = True
        elif event == "consumer_owner_expired":
            require(isolated and not expired and row["lease"]["generation"] == node_lease["generation"] and row["observed_ms"] > row["lease"]["expires_ms"]
                    and any(item["path"] == "nodes/consumer.json" and item["operation"] in ("get","put") for item in row["refused"]), "no actual consumer authority expiry/refusal")
            expired = True
        elif event == "consumer_owner_successor":
            require(expired and successor is None and row["pid"] not in pids.values(), "successor reused a process or preceded expiry")
            successor = row["pid"]
        elif event == "consumer_owner_released":
            require(stages == list(STAGES[:2]) and not released and row["phase"] == entered and row["pid"] == pids["consumer"] and row["state"] and row["state"][0].upper() not in "TZ"
                    and 0 <= row["elapsed_ms"] < 10000, "premature/late callback release or stopped old process")
            released = True
        elif event == "consumer_owner_attempted":
            phase = row["phase"]
            require(released and not attempted and phase["boundary"] == "consumer_poll_attempted" and all(phase[key] == entered[key] for key in ("scope","pid","cut"))
                    and phase["refused"] is True and phase["closed_domain"] is True, "stale consumer committed or lacked closed-domain refusal")
            require((phase["accepted"] is True and phase["acceptance_closed_domain"] is False) if cut == "before_commit" else (phase["accepted"] is None and phase["acceptance_closed_domain"] is True), "illegal acceptance hidden by later write refusal")
            attempted = True
        elif event == "consumer_owner_fenced":
            require(attempted and not fenced and row["pid"] == pids["consumer"] and row["exit_code"] == 1 and row["report"] == dict(outcome="fenced",fence_code=3,reason="node_lease_expired") and row["graceful_reports"] == 0, "old consumer did not join with expiry fencing")
            fenced = True
        elif event == "consumer_owner_interrupted":
            require(fenced and not interrupted and row["kind"] in ("AuditError","OSError","HTTPException"), "old request succeeded or failed to join")
            interrupted = True
        elif event == "consumer_owner_image":
            require(fenced and interrupted and image is None, "old image preceded process join")
            image = row["image"]; first = operations["a"]["accepted"]
            require(image["projection"] == original["projection"] and image["receipts"] == original["receipts"] and image["count"] == 1
                    and image["inbox"] == [dict(scope="order-projection-v1",event_id=first["event_id"],digest=first["envelope_sha256"])] and "db.sqlite" in image["files"], "old consumer retained stale receipt/effect or lost the original")
        elif event == "consumer_owner_fetch":
            label, result = row["label"], row["result"]
            require(stages == list(STAGES[:3]) and len(fetched) < 2 and label == ("applied" if not fetched else "replayed"), "fetch replay preceded recovery audits")
            require(result["status"] == "fetched" and result["fresh_effects"] == (1 if not fetched else 0) and result["projection"] == dict(orders=2,total_cents=1500)
                    and all(result["batch"][key] == entered["delivery"][key] for key in ("receipt","issued_ms","expires_ms","records")), "recovered fetch or effect identity changed")
            fetched.append(label)
        elif event == "consumer_owner_ack":
            require(fetched == ["applied","replayed"] and not acked and row["result"] == dict(status="Acknowledged",projection=dict(orders=2,total_cents=1500)), "invalid final acknowledgment")
            acked = True
        elif event == "fleet_process_stopped":
            require(stages == list(STAGES) and row["node"] in {"destination","producer","successor"} - joined and row["pid"] == (successor if row["node"] == "successor" else pids[row["node"]])
                    and row["exit_code"] == 0 and len(row["reports"]) == 1 and all(row["reports"][0].get(key) is True for key in ("ingress_drained","application_work_drained","cells_drained")), "incomplete survivor join")
            joined.add(row["node"])
        elif event == "consumer_owner_successor_image":
            require(joined == {"destination","producer","successor"} and successor_image is None and not removed, "successor image preceded joins")
            successor_image = row["image"]
            expected = [dict(scope="order-projection-v1", event_id=operations[key]["accepted"]["event_id"], digest=operations[key]["accepted"]["envelope_sha256"]) for key in ("a","b")]
            require(successor_image["projection"] == final_effects["projection"] and successor_image["receipts"] == final_effects["receipts"] and successor_image["count"] == 2
                    and successor_image["inbox"] == sorted(expected,key=lambda r:r["event_id"]) and "db.sqlite" in successor_image["files"], "successor lost or changed an original inbox receipt/effect")
        elif event == "network_store_removed":
            require(store is not None and successor_image is not None and not removed and joined == {"destination","producer","successor"} and row["container"] == store["container"], "store removed before joins")
            removed = True
        else: require(False, f"unknown consumer evidence: {event}")
    if complete:
        require(stages == list(STAGES) and joined == {"destination","producer","successor"} and successor_image is not None and (store is None or removed), "incomplete consumer takeover")
        require(inspect_image(path.parent / "old-consumer-image", original["scope"]) == image, "consumer archive changed")
        require(inspect_image(path.parent / "successor-consumer-image", original["scope"]) == successor_image, "successor archive changed")
    return dict(audits=len(stages), consumer_takeovers=int(fenced), cut=cut, effects=2 if acked else 1, backend="s3" if store else "sqlite")
