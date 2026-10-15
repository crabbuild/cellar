"""Follower proof cannot be replaced by bucket success, source replay or local state."""
import copy
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from faults import AuditError
from follower_faults import audit_evidence, replica_inventory, source_inventory
from test_whole_fleet import fixture as whole_fixture


def fixture():
    old = whole_fixture()
    plan = old[0]
    operations = next(row["operations"] for row in old if row["event"] == "whole_fleet_audit")
    source = operations["waiting"]["ack"]["source"]
    lease = dict(node="producer", generation="generation", expires_ms=1000,
                 log=dict(state="open", epoch=1, ensemble=["destination", "consumer"], may_have_fleet_acks=True))
    owner = dict(node="producer", epoch=1)
    proof = dict(event="follower_only_acknowledged", owner=owner, node_lease=lease, objects={},
                 refused=[dict(path=f"cells/{source}/ltx/e1/0000000000000001.ltx")])
    rows = [plan, dict(event="follower_fleet_started", pids=dict(destination=1, producer=2, consumer=3),
                       proof_mode="fleet", replica_fault_node="producer")]
    for key in ("cross", "waiting"):
        if key == "waiting":
            rows.append(dict(event="replica_publication_blocked", objects={}))
        rows.extend([dict(event="attempt", key=key, attempt=1, replay=False), dict(event="ack", key=key, receipt=operations[key]["ack"])])
    rows.append(proof)
    for label, old_label in (("before-loss", "before-whole-fleet-loss"), ("recovered-before-replay", "recovered-before-replay"),
                             ("first-acknowledged", "first-acknowledged"), ("final", "final")):
        snapshot = copy.deepcopy(next(row["snapshot"] for row in old if row["event"] == "whole_fleet_audit" and row["label"] == old_label))
        if label in ("before-loss", "recovered-before-replay"):
            snapshot["effects"].update(receipts={}, projection=dict(orders=0, total_cents=0))
        if label != "before-loss":
            for state in snapshot["sources"].values():
                state["host"] = "successor"
        new_owner = owner if label == "before-loss" else dict(node="successor", epoch=2)
        rows.append(dict(event="follower_audit", label=label, operations=operations, owner=new_owner, snapshot=snapshot))
        if label == "before-loss":
            rows.extend([dict(event="process_killed", label="follower-producer", pid=2, exit_code=-9),
                         dict(event="follower_source_image", image=dict(order=operations["waiting"]["payload"], receipt=operations["waiting"]["ack"],
                              incarnation=operations["waiting"]["ack"]["incarnation"], lane=operations["waiting"]["ack"]["lane"],
                              sequence="1", rows="1", next_sequence="2", settled_through="0", envelope_sha256="d" * 64, files={"db.sqlite": "e" * 64})),
                         dict(event="follower_leader_data_discarded", remaining_entries=0,
                              files=[dict(path="node/db.sqlite", bytes=4096, sha256="a" * 64)]),
                         dict(event="follower_authority_expired", lease=lease, observed_ms=1001),
                         dict(event="follower_successor_started", node="successor", pid=4)])
        if label in ("recovered-before-replay", "first-acknowledged"):
            rows.append(dict(event="follower_consumer_acknowledged", fresh_effects=1, status="acknowledged"))
    for node, pid in (("destination", 1), ("consumer", 3), ("successor", 4)):
        rows.append(dict(event="fleet_process_stopped", node=node, pid=pid, exit_code=0,
                         reports=[dict(ingress_drained=True, application_work_drained=True, cells_drained=True)]))
    return copy.deepcopy(rows)


class FollowerOracleTests(unittest.TestCase):
    def audit(self, rows):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "ledger.jsonl"
            path.write_text("".join(json.dumps(row) + "\n" for row in rows))
            image = next(row["image"] for row in rows if row["event"] == "follower_source_image")
            with patch("follower_faults.inspect_crash_image", return_value=image):
                return audit_evidence(path)

    def test_complete_follower_only_recovery(self):
        self.assertEqual(self.audit(fixture())["follower_only_recoveries"], 1)

    def test_bucket_bypass_missing_refusal_or_follower_marker_is_rejected(self):
        for kind in ("bucket", "refusal", "ensemble", "fleet_marker"):
            rows = fixture()
            proof = next(row for row in rows if row["event"] == "follower_only_acknowledged")
            if kind == "bucket":
                proof["objects"][proof["refused"][0]["path"]] = dict(bytes=100, sha256="c" * 64)
            elif kind == "refusal":
                proof["refused"] = [dict(path="cells/unrelated/ltx/e1/file.ltx")]
            elif kind == "ensemble":
                proof["node_lease"]["log"]["ensemble"] = []
            else:
                proof["node_lease"]["log"]["may_have_fleet_acks"] = False
            with self.subTest(kind=kind), self.assertRaises(AuditError):
                self.audit(rows)

    def test_arming_after_ack_or_replaying_source_cannot_repair_loss(self):
        rows = fixture()
        gate = next(row for row in rows if row["event"] == "replica_publication_blocked")
        rows.remove(gate)
        rows.insert(7, gate)
        with self.assertRaises(AuditError):
            self.audit(rows)
        rows = fixture()
        next(row for row in rows if row["event"] == "attempt")["replay"] = True
        with self.assertRaises(AuditError):
            self.audit(rows)

    def test_loss_without_local_discard_or_expiry_or_new_process_is_rejected(self):
        for kind in ("discard", "expiry", "process", "epoch"):
            rows = fixture()
            if kind == "discard":
                rows = [row for row in rows if row["event"] != "follower_leader_data_discarded"]
            elif kind == "expiry":
                next(row for row in rows if row["event"] == "follower_authority_expired")["observed_ms"] = 999
            elif kind == "process":
                next(row for row in rows if row["event"] == "follower_successor_started")["pid"] = 2
            else:
                next(row for row in rows if row["event"] == "follower_audit" and row["label"] == "recovered-before-replay")["owner"]["epoch"] = 1
            with self.subTest(kind=kind), self.assertRaises(AuditError):
                self.audit(rows)

    def test_recovery_must_retain_original_receipt_and_pending_obligation(self):
        for kind in ("receipt", "obligation", "accepted"):
            rows = fixture()
            state = next(row for row in rows if row["event"] == "follower_audit" and row["label"] == "recovered-before-replay")["snapshot"]
            if kind == "receipt":
                state["sources"]["waiting"]["receipt"]["incarnation"] = "b" * 32
            elif kind == "obligation":
                state["sources"]["waiting"]["outbox"]["pending_rows"] = "0"
            else:
                state["events"]["records"] = []
            with self.subTest(kind=kind), self.assertRaises(AuditError):
                self.audit(rows)

    def test_consumer_before_recovery_duplicate_effect_and_partial_shutdown_fail(self):
        rows = fixture()
        consume = next(row for row in rows if row["event"] == "follower_consumer_acknowledged")
        rows.remove(consume)
        rows.insert(8, consume)
        with self.assertRaises(AuditError):
            self.audit(rows)
        rows = fixture()
        next(row for row in rows if row["event"] == "follower_consumer_acknowledged")["fresh_effects"] = 0
        with self.assertRaises(AuditError):
            self.audit(rows)
        with self.assertRaises(AuditError):
            self.audit(fixture()[:-1])

    def test_changed_pending_envelope_and_archive_custody_are_rejected(self):
        rows = fixture()
        next(row for row in rows if row["event"] == "follower_source_image")["image"]["envelope_sha256"] = "a" * 64
        with self.assertRaises(AuditError):
            self.audit(rows)
        rows = fixture()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "ledger.jsonl"
            path.write_text("".join(json.dumps(row) + "\n" for row in rows))
            changed = copy.deepcopy(next(row["image"] for row in rows if row["event"] == "follower_source_image"))
            changed["files"]["db.sqlite"] = "a" * 64
            with patch("follower_faults.inspect_crash_image", return_value=changed), self.assertRaises(AuditError):
                audit_evidence(path)

    def test_private_network_store_is_removed_only_after_all_nodes_join(self):
        rows = fixture()
        rows.insert(1, dict(event="network_store_started", backend="s3", container="cellar-stream-store-test", image_id="sha256:" + "a" * 64))
        rows.append(dict(event="network_store_removed", container="cellar-stream-store-test"))
        self.assertEqual(self.audit(rows)["backend"], "s3")
        with self.assertRaises(AuditError):
            self.audit(rows[:-1])

    def test_sqlite_inventory_captures_exact_source_replica_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with sqlite3.connect(root / "fleet.sqlite") as db:
                db.execute("CREATE TABLE __sys_local_objects(key TEXT,body BLOB)")
                db.executemany("INSERT INTO __sys_local_objects VALUES(?,?)", [("nodes/producer.json", b"metadata"),
                    ("cells/Order:a/ltx/e1/file.ltx", b"actual-ltx"), ("cells/Order:ab/ltx/e1/file.ltx", b"other")])
            observed = source_inventory(replica_inventory(root, None), "Order:a")
            self.assertEqual(list(observed), ["cells/Order:a/ltx/e1/file.ltx"])
            self.assertEqual(next(iter(observed.values()))["bytes"], 10)

    def test_network_inventory_refuses_truncated_or_oversized_listing(self):
        xml = b'<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><IsTruncated>true</IsTruncated></ListBucketResult>'
        with patch("follower_faults.store_read", return_value=xml), self.assertRaises(AuditError):
            replica_inventory(Path("unused"), object())
