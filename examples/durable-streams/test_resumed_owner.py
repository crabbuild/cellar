# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Resume evidence cannot replace real stop/expiry/fence or repair original work."""
import copy
import json
import os
import signal
import sqlite3
import hashlib
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from before_send import PAYLOAD
from faults import AuditError, digest
from resumed_owner import audit_evidence, inspect_image
from test_before_send import fixture as source_fixture


def fixture():
    phase, before = source_fixture()
    receipt = phase["command"]["receipt"]
    image = dict(order=PAYLOAD, receipt=receipt, incarnation=receipt["incarnation"], lane=receipt["lane"], sequence="1",
                 next_sequence="2", settled_through="0", dispatch_generation="0", attempts="0", rows="1",
                 envelope_sha256="c" * 64, files={"db.sqlite": "e" * 64})
    before["source"]["host"] = "successor"
    before["source"]["outbox"]["pending_rows"] = "0"
    before["source"]["outbox"]["lanes"][0]["pending_rows"] = "0"
    before["source"]["outbox"]["lanes"][0]["revision"]["settled_through"] = "1"
    event = dict(receipt, order_id="first", event_id=f"v1:{receipt['lane']}:1", offset="1", envelope_sha256="c" * 64, event=PAYLOAD)
    before["events"]["records"] = [event]
    before["effects"]["host"] = "consumer"
    final = copy.deepcopy(before)
    final["effects"].update(projection=dict(orders=1, total_cents=725), receipts={"first": {k: v for k, v in event.items() if k != "event"}})
    final["subscription"]["checkpoint"] = "1"
    lease = dict(generation="g", expires_ms=1000)
    new_owner = dict(node="successor", epoch=2)
    rows = [dict(event="plan", operations={"first": dict(payload=PAYLOAD, sha256=digest(PAYLOAD), attempts=0)}),
            dict(event="resumed_fleet_started", pids=dict(destination=1, producer=2, consumer=3), proof_mode="bucket", node_lease_ms=1000, handler_budget_ms=10000),
            dict(event="stale_source_attempt", payload=PAYLOAD, sha256=digest(PAYLOAD)),
            dict(event="source_proven_before_pause", phase=phase, owner=dict(node="producer", epoch=1), lease=lease, events=dict(records=[])),
            dict(event="owner_stopped", pid=2, wait_status=(int(signal.SIGSTOP) << 8) | 0x7f),
            dict(event="old_source_image", label="old-before", image=image),
            dict(event="authority_expired", lease=lease, observed_ms=1001),
            dict(event="successor_started", pid=4),
            dict(event="resumed_owner_audit", label="before-resume", owner=new_owner, snapshot=before),
            dict(event="owner_resumed", pid=2, signal=int(signal.SIGCONT), attempt_elapsed_ms=2000),
            dict(event="owner_fenced", pid=2, exit_code=1, report=dict(outcome="fenced", fence_code=3, reason="node_lease_expired"), graceful_reports=0),
            dict(event="stale_response_interrupted", kind="RemoteDisconnected"),
            dict(event="old_source_image", label="old-after", image=image),
            dict(event="resumed_owner_audit", label="after-resume", owner=new_owner, snapshot=before),
            dict(event="command_replayed", status="duplicate", receipt=phase["command"]["receipt"]),
            dict(event="resumed_owner_audit", label="final", owner=new_owner, snapshot=final)]
    for node, pid in (("destination", 1), ("consumer", 3), ("successor", 4)):
        rows.append(dict(event="fleet_process_stopped", node=node, pid=pid, exit_code=0,
                         reports=[dict(ingress_drained=True, application_work_drained=True, cells_drained=True)]))
    return json.loads(json.dumps(rows))


@unittest.skipUnless(os.name == "posix", "requires POSIX stop/continue signals")
class ResumedOwnerTests(unittest.TestCase):
    def audit(self, rows, changed_archive=False, complete=True):
        images = {row["label"]: row["image"] for row in rows if row["event"] == "old_source_image"}
        def archived(path, scope):
            value = copy.deepcopy(images[path.name])
            if changed_archive:
                value["files"]["db.sqlite"] = "f" * 64
            return value
        with tempfile.TemporaryDirectory() as directory, patch("resumed_owner.inspect_image", side_effect=archived):
            path = Path(directory) / "ledger.jsonl"
            path.write_text("".join(json.dumps(row) + "\n" for row in rows))
            return audit_evidence(path, complete=complete)

    def test_readonly_inspection_keeps_saved_database_family_immutable(self):
        phase, _ = source_fixture()
        receipt = phase["command"]["receipt"]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            db = sqlite3.connect(root / "db.sqlite")
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript("CREATE TABLE __sys_event_source(singleton INTEGER, incarnation TEXT);"
                "CREATE TABLE __sys_outbox_lanes(lane TEXT,next_sequence INTEGER,settled_through INTEGER,dispatch_generation INTEGER,attempts INTEGER);"
                "CREATE TABLE __sys_outbox(sequence INTEGER,envelope BLOB);CREATE TABLE __sys_cell_kv(scope TEXT,k TEXT,v TEXT);")
            db.execute("INSERT INTO __sys_event_source VALUES(1,?)", (receipt["incarnation"],))
            db.execute("INSERT INTO __sys_outbox_lanes VALUES(?,2,0,0,0)", (receipt["lane"],))
            db.execute("INSERT INTO __sys_outbox VALUES(1,?)", (b"encoded",))
            for key, value in (("order", PAYLOAD), ("command_receipt", receipt)):
                db.execute("INSERT INTO __sys_cell_kv VALUES(?,?,?)", (receipt["source"], key, json.dumps(value)))
            db.commit()
            db.close()
            before = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in root.iterdir()}
            first = inspect_image(root, receipt["source"])
            after = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in root.iterdir()}
            self.assertEqual(before, after)
            self.assertEqual(first, inspect_image(root, receipt["source"]))

    def test_complete_stopped_then_resumed_owner(self):
        self.assertEqual(self.audit(fixture())["resumed_expired_owners"], 1)

    def test_missing_stop_expiry_resume_or_specific_fence_cannot_qualify(self):
        for event in ("owner_stopped", "authority_expired", "owner_resumed", "owner_fenced", "stale_response_interrupted"):
            rows = fixture()
            rows.remove(next(row for row in rows if row["event"] == event))
            with self.assertRaises(AuditError):
                self.audit(rows)

    def test_generic_failure_or_original_deadline_expiry_is_not_fencing(self):
        rows = fixture()
        next(row for row in rows if row["event"] == "owner_fenced")["report"]["reason"] = "node_lease_missing"
        with self.assertRaises(AuditError):
            self.audit(rows)
        rows = fixture()
        next(row for row in rows if row["event"] == "owner_resumed")["attempt_elapsed_ms"] = 10000
        with self.assertRaises(AuditError):
            self.audit(rows)

    def test_expired_claim_and_higher_epoch_are_mandatory(self):
        rows = fixture()
        next(row for row in rows if row["event"] == "authority_expired")["observed_ms"] = 999
        with self.assertRaises(AuditError):
            self.audit(rows)
        rows = fixture()
        next(row for row in rows if row["event"] == "resumed_owner_audit")["owner"]["epoch"] = 1
        with self.assertRaises(AuditError):
            self.audit(rows)

    def test_old_dispatch_or_changed_successor_state_is_rejected(self):
        rows = fixture()
        next(row for row in rows if row["event"] == "old_source_image" and row["label"] == "old-after")["image"]["attempts"] = "1"
        with self.assertRaises(AuditError):
            self.audit(rows)
        rows = fixture()
        next(row for row in rows if row["event"] == "resumed_owner_audit" and row["label"] == "after-resume")["snapshot"]["source"]["receipt"]["sequence"] = "2"
        with self.assertRaises(AuditError):
            self.audit(rows)

    def test_command_replay_cannot_precede_resumed_owner_audit(self):
        rows = fixture()
        replay = next(row for row in rows if row["event"] == "command_replayed")
        rows.remove(replay)
        rows.insert(8, replay)
        with self.assertRaises(AuditError):
            self.audit(rows)

    def test_mutated_archive_missing_shutdown_and_foreign_process_are_rejected(self):
        with self.assertRaises(AuditError):
            self.audit(fixture(), changed_archive=True)
        rows = fixture()
        rows.pop()
        with self.assertRaises(AuditError):
            self.audit(rows)
        rows = fixture()
        next(row for row in rows if row["event"] == "owner_stopped")["pid"] = 99
        with self.assertRaises(AuditError):
            self.audit(rows)
