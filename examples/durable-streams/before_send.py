#!/usr/bin/env python3
"""Audit a proven, unacknowledged source commit after SIGKILL before first send."""

import argparse
import concurrent.futures
import hashlib
import http.client
import json
import os
import shutil
import sqlite3
from pathlib import Path
import tempfile
import urllib.error

from faults import AuditError, Ledger, Process, digest, eventually, integer, producer, read, require


PAYLOAD = {"order_id": "first", "amount_cents": 725}
TOKEN = "first-send-boundary"


def validate_boundary(phase):
    require(phase.get("boundary") == "source_proven_before_send" and phase.get("token") == TOKEN,
            "source proof boundary missing or wrong")
    command = phase["command"]
    require(command["status"] == "created" and command["order"] == PAYLOAD, "source proof has wrong command")
    receipt = command["receipt"]
    require(receipt["payload_sha256"] == digest(PAYLOAD) and integer(receipt["sequence"]) == 1,
            "proven command receipt changed")
    outbox = phase["outbox"]
    require(integer(outbox["pending_rows"]) == 1 and len(outbox["lanes"]) == 1 and outbox["next"] is None,
            "proven source obligation missing")
    lane = outbox["lanes"][0]
    revision = lane["revision"]
    require(outbox["incarnation"] == receipt["incarnation"] and integer(lane["pending_rows"]) == 1,
            "proven source identity/accounting changed")
    require(integer(lane["attempts"]) == integer(revision["dispatch_generation"]) == 0,
            "source already attempted dispatch before SIGKILL")
    require(integer(revision["settled_through"]) == 0 and integer(revision["next_sequence"]) == 2,
            "source sequence was skipped or repeated")
    require(all(receipt[key] == revision[key] for key in ("source", "incarnation", "lane")),
            "proven producer identity changed")
    return receipt


def validate_crash_image(phase, image):
    receipt = validate_boundary(phase)
    require(image["order"] == PAYLOAD and image["receipt"] == receipt and image["incarnation"] == receipt["incarnation"],
            "post-kill source image lost proven state")
    require(image["lane"] == receipt["lane"] and image["sequence"] == "1"
            and image["next_sequence"] == "2" and image["settled_through"] == "0"
            and image["attempts"] == image["dispatch_generation"] == "0"
            and image["rows"] == "1", "post-kill image does not prove zero dispatch attempts")
    require(len(image["envelope_sha256"]) == 64
            and all(c in "0123456789abcdef" for c in image["envelope_sha256"]), "post-kill envelope evidence missing")


def capture_crash_image(root, phase):
    # Copy the killed process's complete SQLite family before any framework
    # restart. SQLite reads only this forensic copy, never the live data tree.
    receipt = validate_boundary(phase)
    scope = receipt["source"]
    require(scope.startswith("Order:") and len(scope) == 70
            and all(c in "0123456789abcdef" for c in scope[6:]), "unsafe source image path")
    candidates = list((root/"data"/"node"/scope/"ltx").glob("e*/db.sqlite"))
    require(len(candidates) == 1, "fresh source did not have one captured epoch")
    original = candidates[0]
    archive = root/"killed-source-image"
    archive.mkdir()
    for suffix in ("", "-wal", "-shm"):
        path = Path(str(original)+suffix)
        if path.exists():
            copied = archive/("db.sqlite"+suffix)
            shutil.copy2(path, copied)
            with copied.open("rb") as stream:
                os.fsync(stream.fileno())
    image = inspect_crash_image(archive, scope)
    validate_crash_image(phase, image)
    return image


def inspect_crash_image(archive, scope):
    # SHM read marks are mutable even on a read-only connection; they are not
    # durable state. Authenticate the archived database and WAL, then inspect
    # only this copied family. Never open the node's current SQLite files.
    hashes = {}
    for name in ("db.sqlite", "db.sqlite-wal"):
        path = archive/name
        if path.exists():
            hashes[name] = hashlib.sha256(path.read_bytes()).hexdigest()
    require("db.sqlite" in hashes, "archived source database missing")
    with sqlite3.connect((archive/"db.sqlite").resolve().as_uri()+"?mode=ro", uri=True) as db:
        db.execute("PRAGMA query_only=ON")
        incarnation, = db.execute("SELECT incarnation FROM __sys_event_source WHERE singleton=1").fetchone()
        lanes = db.execute("SELECT lane,next_sequence,settled_through,dispatch_generation,attempts FROM __sys_outbox_lanes").fetchall()
        records = db.execute("SELECT sequence,envelope FROM __sys_outbox").fetchall()
        require(len(lanes) == len(records) == 1, "post-kill obligation inventory changed")
        lane, next_sequence, settled, generation, attempts = lanes[0]
        sequence, envelope = records[0]
        image = dict(incarnation=incarnation, lane=lane, next_sequence=str(next_sequence), settled_through=str(settled),
                     dispatch_generation=str(generation), attempts=str(attempts), sequence=str(sequence), rows=str(len(records)),
                     envelope_sha256=hashlib.sha256(envelope).hexdigest(), files=hashes)
        for key, field in (("order", "order"), ("command_receipt", "receipt")):
            value, = db.execute("SELECT v FROM __sys_cell_kv WHERE scope=? AND k=?", (scope, key)).fetchone()
            image[field] = json.loads(value)
    return image


def validate_snapshot(phase, snapshot, final=False):
    receipt = validate_boundary(phase)
    source = snapshot["source"]
    require(source["order"] == PAYLOAD and source["receipt"] == receipt,
            "proven source state lost or changed before replay")
    outbox = source["outbox"]
    require(outbox["incarnation"] == receipt["incarnation"] and len(outbox["lanes"]) == 1,
            "source incarnation lost or duplicated")
    lane = outbox["lanes"][0]
    revision = lane["revision"]
    require(all(revision[key] == receipt[key] for key in ("source", "incarnation", "lane")),
            "recovered producer identity changed")
    pending = integer(outbox["pending_rows"])
    require(pending in (0, 1) and integer(lane["pending_rows"]) == pending
            and integer(revision["next_sequence"]) == 2 and integer(revision["settled_through"]) == 1-pending,
            "source obligation was lost, skipped or duplicated")
    records = snapshot["events"]["records"]
    effects = snapshot["effects"]
    require(len(records) <= 1 and (pending == 1 or len(records) == 1),
            "proven obligation neither pending nor accepted exactly once")
    for record in records:
        require(record["event"] == PAYLOAD and record["payload_sha256"] == digest(PAYLOAD)
                and producer(record) == producer(receipt) and record["offset"] == "1"
                and record["event_id"] == f"v1:{receipt['lane']}:1", "accepted event identity/payload changed")
        require(len(record["envelope_sha256"]) == 64 and all(c in "0123456789abcdef" for c in record["envelope_sha256"]),
                "accepted envelope digest missing")
    expected = {"orders": 1 if final else 0, "total_cents": 725 if final else 0}
    require(effects["projection"] == expected and integer(snapshot["subscription"]["checkpoint"]) == int(final),
            "consumer effects/checkpoint changed before authorized processing")
    if final:
        require(pending == 0 and set(effects["receipts"]) == {"first"}, "final proven obligation missing")
        archived = dict(records[0])
        archived.pop("event")
        require(effects["receipts"]["first"] == archived, "consumer receipt/envelope mismatch")
    else:
        require(effects["receipts"] == {}, "effect before authorized consumer replay")


def snapshot(origin):
    return {"source": read(origin, "/orders/first"), "events": read(origin, "/__test/events"),
            "effects": read(origin, "/__test/projection-receipts"), "subscription": read(origin, "/subscription")}


def audit_records(records):
    phase = None
    attempted = killed = interrupted = replayed = False
    crash_image = None
    audits = set()
    last = None
    for record in records:
        event = record["event"]
        if event == "before_send_attempt":
            require(not attempted and record["payload"] == PAYLOAD and record["sha256"] == digest(PAYLOAD),
                    "independent command plan missing or changed")
            attempted = True
        elif event == "source_proven_before_send":
            require(attempted and phase is None and not killed, "source boundary preceded attempt or followed kill")
            phase = record["phase"]
            validate_boundary(phase)
            require(record["events"]["records"] == [], "destination accepted before source kill")
        elif event == "process_killed":
            require(phase is not None and not killed and record["label"] == "source-before-first-send"
                    and record["exit_code"] == -9, "missing real source SIGKILL")
            killed = True
        elif event == "before_send_crash_image":
            require(killed and phase is not None and crash_image is None and not audits,
                    "post-kill source audit missing or after restart")
            crash_image = record["image"]
            validate_crash_image(phase, crash_image)
        elif event == "before_send_response_interrupted":
            require(killed and not interrupted, "response interruption preceded kill")
            interrupted = True
        elif event == "before_send_audit":
            label = record["label"]
            require(killed and interrupted and phase is not None and crash_image is not None and label not in audits,
                    "recovery audit lacks proof or is repeated")
            require(label in ("recovered-before-replay", "final"), "unknown audit stage")
            if label == "final":
                require(replayed and "recovered-before-replay" in audits, "final audit preceded replay admission")
            else:
                require(not replayed, "recovery audit occurred after replay")
            validate_snapshot(phase, record["snapshot"], final=label == "final")
            for accepted in record["snapshot"]["events"]["records"]:
                require(accepted["envelope_sha256"] == crash_image["envelope_sha256"], "recovered envelope differs from killed source image")
            audits.add(label)
            last = record["snapshot"]
        elif event == "before_send_replay":
            require("recovered-before-replay" in audits and not replayed and record["receipt"] == phase["command"]["receipt"]
                    and record["status"] == "duplicate", "ambiguous command replay preceded audit or changed receipt")
            replayed = True
    require(phase is not None and attempted and killed and interrupted and replayed
            and audits == {"recovered-before-replay", "final"}, "before-send evidence incomplete")
    return {"audits": len(audits), "sigkills": 1, "effects": last["effects"]["projection"]["orders"]}


def audit_evidence(path):
    records = [json.loads(line) for line in path.read_text().splitlines()]
    result = audit_records(records)
    image, = [record["image"] for record in records if record["event"] == "before_send_crash_image"]
    phase, = [record["phase"] for record in records if record["event"] == "source_proven_before_send"]
    actual = inspect_crash_image(path.parent/"killed-source-image", phase["command"]["receipt"]["source"])
    require(actual == image, "saved SQLite image or WAL differs from original post-kill audit")
    validate_crash_image(phase, actual)
    return result


def exercise(binary, root):
    require(os.name == "posix", "this qualification requires SIGKILL")
    root.mkdir(parents=True, exist_ok=False)
    ledger = Ledger(root, commands=(("first", 725, 1),))
    node = None
    try:
        node = Process(binary, root, "before-first-send")
        origin = node.ready()
        ledger.record("before_send_attempt", payload=PAYLOAD, sha256=digest(PAYLOAD))
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            held = executor.submit(read, origin, f"/orders/first?amount_cents=725&hold_before_send={TOKEN}", "POST", 20)
            phase = eventually(lambda: read(origin, "/__test/phase"),
                               lambda value: (value.get("phase") or {}).get("boundary") == "source_proven_before_send",
                               timeout=5)["phase"]
            validate_boundary(phase)
            events = read(origin, "/__test/events")
            require(events["records"] == [] and not held.done(), "source escaped first-send gate")
            ledger.record("source_proven_before_send", phase=phase, events=events)
            node.crash(ledger, "source-before-first-send")
            try:
                held.result(timeout=10)
            except (urllib.error.URLError, http.client.HTTPException, OSError) as error:
                ledger.record("before_send_response_interrupted", error=type(error).__name__)
            else:
                raise AuditError("held source returned a response instead of being killed")
        image = capture_crash_image(root, phase)
        ledger.record("before_send_crash_image", image=image)
        node = Process(binary, root, "before-send-recovered")
        origin = node.ready()
        recovered = snapshot(origin)
        ledger.record("before_send_audit", label="recovered-before-replay", snapshot=recovered)
        validate_snapshot(phase, recovered)
        reply = read(origin, "/orders/first?amount_cents=725", "POST")
        require(reply["status"] == "duplicate" and reply["receipt"] == phase["command"]["receipt"],
                "ambiguous replay lost/repeated proven command")
        ledger.record("before_send_replay", status=reply["status"], receipt=reply["receipt"])
        eventually(lambda: read(origin, "/orders/first"), lambda state: state["outbox"]["pending_rows"] == "0")
        processed = read(origin, "/projection/process", "POST")
        require(processed["status"] == "acknowledged" and processed["fresh_effects"] == 1, "first effect missing/repeated")
        require(read(origin, "/projection/process", "POST")["status"] == "empty", "checkpoint failed to settle")
        final = snapshot(origin)
        validate_snapshot(phase, final, final=True)
        ledger.record("before_send_audit", label="final", snapshot=final)
        node.stop()
        evidence = audit_evidence(ledger.path)
        summary = dict(evidence, status="passed", binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
                       scope="single-node SQLite bucket; proven unacknowledged source before first dispatch SIGKILL", output=str(root))
        (root / "summary.json").write_text(json.dumps(summary, indent=2)+"\n")
        print(json.dumps(summary), flush=True)
    finally:
        if node is not None:
            node.stop()
        ledger.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--binary", type=Path)
    target.add_argument("--audit", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.audit:
        require(args.output is None, "--output requires --binary")
        print(json.dumps(audit_evidence(args.audit.resolve())), flush=True)
    else:
        root = args.output or Path(tempfile.mkdtemp(prefix="cellar-before-send-parent-"))/"evidence"
        exercise(args.binary.resolve(), root.resolve())
