"""Follower proof cannot be replaced by bucket success, source replay or local state."""
import copy
import json
from pathlib import Path
import sqlite3
import struct
import hashlib
import tempfile
import unittest
from unittest.mock import patch

from faults import AuditError
from follower_faults import audit_evidence, inspect_follower_disk, replica_inventory, source_inventory
from test_whole_fleet import fixture as whole_fixture


def fixture():
    old = whole_fixture()
    plan = old[0]
    operations = next(row["operations"] for row in old if row["event"] == "whole_fleet_audit")
    source = operations["waiting"]["ack"]["source"]
    lease = dict(node="producer", lineage="1"*32, generation="generation", expires_ms=1000,
                 log=dict(state="open", epoch=1, ensemble=["destination", "consumer"], may_have_fleet_acks=True))
    owner = dict(node="producer", epoch=1)
    proof = dict(event="follower_only_acknowledged", owner=owner, node_lease=lease, objects={},
                 refused=[dict(path=f"cells/{source}/ltx/e1/0000000000000001.ltx")])
    rows = [plan, dict(event="follower_fleet_started", pids=dict(destination=1, producer=2, consumer=3),
                       proof_mode="fleet", replica_fault_node="producer", restart_followers=False, peer_proof="fleet")]
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


def restarted_fixture():
    rows = fixture()
    next(row for row in rows if row["event"] == "follower_fleet_started")["restart_followers"] = True
    proof = next(row for row in rows if row["event"] == "follower_only_acknowledged")
    proof["node_lease"]["log"]["epoch"] = 1
    source = next(row for row in rows if row["event"] == "ack" and row["key"] == "waiting")["receipt"]["source"]
    files = [dict(path="4-4.batch", bytes=1024, sha256="a" * 64), dict(path="state.json", bytes=51, sha256="b" * 64)]
    disk = dict(files=files, seed=dict(fragment_epoch=1, base=0, end=0, sealed_to=0),
                batches=[dict(path="4-4.batch", epoch=1, base=3, end=4, sealed_to=0,
                              entries=[dict(seq=4, cell=source, cell_epoch=1, txid=2, sha256="c" * 64)])])
    leases = {node: dict(node=node, lineage="1"*32, generation="old-" + node, expires_ms=1000) for node in ("destination", "consumer")}
    index = next(i for i, row in enumerate(rows) if row["event"] == "process_killed") + 1
    rows[index:index] = [dict(event="process_killed", label="follower-destination", pid=1, exit_code=-9),
                         dict(event="process_killed", label="follower-consumer", pid=3, exit_code=-9),
                         dict(event="follower_disks_retained", followers={node: copy.deepcopy(disk) for node in leases}, objects={}, leases=leases)]
    index = next(i for i, row in enumerate(rows) if row["event"] == "follower_successor_started")
    rows[index]["pid"] = 6
    rows[index:index] = [dict(event="follower_process_restarted", node=node, pid=pid, lease=leases[node], observed_ms=1001, files=files)
                         for node, pid in (("destination", 4), ("consumer", 5))]
    index = next(i for i, row in enumerate(rows) if row["event"] == "follower_audit" and row["label"] == "recovered-before-replay")
    rows[index:index] = [dict(event="follower_process_ready", node=node, pid=pid, lease=dict(node=node, lineage="1"*32, generation="new-" + node))
                         for node, pid in (("destination", 4), ("consumer", 5))]
    for row in rows:
        if row["event"] == "fleet_process_stopped":
            row["pid"] = dict(destination=4, consumer=5, successor=6)[row["node"]]
    return copy.deepcopy(rows)


class FollowerOracleTests(unittest.TestCase):
    def audit(self, rows):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "ledger.jsonl"
            path.write_text("".join(json.dumps(row) + "\n" for row in rows))
            image = next(row["image"] for row in rows if row["event"] == "follower_source_image")
            disks = next((row["followers"] for row in rows if row["event"] == "follower_disks_retained"), {})
            with patch("follower_faults.inspect_crash_image", return_value=image), \
                 patch("follower_faults.inspect_follower_disk", side_effect=lambda path: disks[path.name]):
                return audit_evidence(path)

    def test_selected_session_requires_lineage_and_excludes_other_lineages(self):
        objects = {f"log/producer/{lineage}/generation/bundle/e1-0.ltxb": lineage for lineage in ("1"*32, "2"*32)}
        self.assertEqual(list(source_inventory(objects, "Order:a", "generation", "1"*32).values()), ["1"*32])
        for lineage in (None, "", "../unsafe", "A"*32):
            with self.subTest(lineage=lineage), self.assertRaises(AuditError):
                source_inventory(objects, "Order:a", "generation", lineage)

    def test_complete_follower_only_recovery(self):
        self.assertEqual(self.audit(fixture())["follower_only_recoveries"], 1)

    def test_all_original_processes_lost_and_followers_restarted_from_disk(self):
        self.assertEqual(self.audit(restarted_fixture())["restarted_follower_processes"], 2)

    def test_source_only_fleet_proof_requires_bucket_backed_peer_evidence(self):
        rows = restarted_fixture()
        next(row for row in rows if row["event"] == "follower_fleet_started")["peer_proof"] = "bucket"
        self.assertEqual(self.audit(rows)["restarted_follower_processes"], 2)
        next(row for row in rows if row["event"] == "follower_disks_retained")["leases"]["destination"]["log"] = dict(may_have_fleet_acks=True)
        with self.assertRaises(AuditError):
            self.audit(rows)

    def test_node_log_bundle_upload_cannot_claim_follower_only_acceptance_or_restart(self):
        for event in ("follower_only_acknowledged", "follower_disks_retained"):
            rows = restarted_fixture()
            proof = next(row for row in rows if row["event"] == "follower_only_acknowledged")
            row = next(row for row in rows if row["event"] == event)
            row["objects"][f"log/producer/{proof['node_lease']['lineage']}/{proof['node_lease']['generation']}/bundle/e1-00000000.ltxb"] = dict(bytes=100, sha256="c" * 64)
            with self.subTest(event=event), self.assertRaises(AuditError):
                self.audit(rows)

    def test_live_follower_memory_missing_restart_or_stale_generation_is_rejected(self):
        for kind in ("kill", "restart", "ready", "generation", "lineage", "lease"):
            rows = restarted_fixture()
            if kind == "kill":
                rows = [row for row in rows if row.get("label") != "follower-consumer"]
            elif kind in ("restart", "ready"):
                event = "follower_process_restarted" if kind == "restart" else "follower_process_ready"
                rows = [row for row in rows if not (row["event"] == event and row["node"] == "destination")]
            elif kind == "lineage":
                next(row for row in rows if row["event"] == "follower_process_ready")["lease"]["lineage"] = "2"*32
            elif kind == "generation":
                next(row for row in rows if row["event"] == "follower_process_ready")["lease"]["generation"] = "old-destination"
            else:
                next(row for row in rows if row["event"] == "follower_process_restarted")["observed_ms"] = 999
            with self.subTest(kind=kind), self.assertRaises(AuditError):
                self.audit(rows)

    def test_cold_bucket_bypass_wrong_fragment_and_changed_retained_file_are_rejected(self):
        for kind in ("bucket", "fragment", "cell", "file", "pid"):
            rows = restarted_fixture()
            cold = next(row for row in rows if row["event"] == "follower_disks_retained")
            if kind == "bucket":
                proof = next(row for row in rows if row["event"] == "follower_only_acknowledged")
                cold["objects"][proof["refused"][0]["path"]] = dict(bytes=100, sha256="c" * 64)
            elif kind == "fragment":
                cold["followers"]["destination"]["batches"][0]["epoch"] = 2
            elif kind == "cell":
                cold["followers"]["destination"]["batches"][0]["entries"][0]["cell"] = "unrelated"
            elif kind == "file":
                next(row for row in rows if row["event"] == "follower_process_restarted")["files"][0]["sha256"] = "f" * 64
            else:
                next(row for row in rows if row["event"] == "follower_process_restarted")["pid"] = 1
            with self.subTest(kind=kind), self.assertRaises(AuditError):
                self.audit(rows)

    def test_restarted_follower_can_be_the_new_source_owner(self):
        rows = restarted_fixture()
        for row in rows:
            if row["event"] == "follower_audit" and row["label"] != "before-loss":
                row["owner"]["node"] = "destination"
                row["snapshot"]["sources"]["waiting"]["host"] = "destination"
        self.assertEqual(self.audit(rows)["follower_only_recoveries"], 1)

    def test_disk_archive_custody_is_rechecked_after_complete_recovery(self):
        rows = restarted_fixture()
        image = next(row["image"] for row in rows if row["event"] == "follower_source_image")
        changed = copy.deepcopy(next(row["followers"]["destination"] for row in rows if row["event"] == "follower_disks_retained"))
        changed["files"][0]["sha256"] = "f" * 64
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "ledger.jsonl"
            path.write_text("".join(json.dumps(row) + "\n" for row in rows))
            with patch("follower_faults.inspect_crash_image", return_value=image), \
                 patch("follower_faults.inspect_follower_disk", return_value=changed), self.assertRaises(AuditError):
                audit_evidence(path)

    def test_committed_batch_is_valid_despite_zero_seed_watermark(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            seed = dict(fragment_epoch=1, base=0, end=0, sealed_to=0)
            from test_follower_state import state_bytes
            (root / "state.json").write_bytes(state_bytes(seed))
            cell, body = b"Order:source", b"actual-ltx-frame"
            entry = b"CLE1" + struct.pack("<QH", 4, len(cell)) + cell + struct.pack("<QQI", 1, 2, len(body)) + body
            payload = b"CLB1" + struct.pack("<QQQQI", 1, 3, 4, 0, 1) + entry
            batch = root / "4-4.batch"
            batch.write_bytes(payload + hashlib.sha256(payload).digest())
            observed = inspect_follower_disk(root)
            self.assertEqual(observed["seed"], seed)
            self.assertEqual(observed["batches"][0]["entries"][0]["txid"], 2)
            damaged = bytearray(batch.read_bytes());damaged[-1] ^= 1
            batch.write_bytes(damaged)
            with self.assertRaises(AuditError):
                inspect_follower_disk(root)
            batch.write_bytes(payload[:45])
            with self.assertRaises(AuditError):
                inspect_follower_disk(root)
            batch.write_bytes(payload + hashlib.sha256(payload).digest())
            batch.rename(root / "3-4.batch")
            with self.assertRaises(AuditError):
                inspect_follower_disk(root)

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
                    ("cells/Order:a/ltx/e1/file.ltx", b"actual-ltx"), ("cells/Order:ab/ltx/e1/file.ltx", b"other"),
                    ("log/producer/"+"1"*32+"/generation/bundle/e1-00000000.ltxb", b"actual-bundle")])
            objects = replica_inventory(root, None)
            observed = source_inventory(objects, "Order:a")
            self.assertEqual(list(observed), ["cells/Order:a/ltx/e1/file.ltx"])
            self.assertEqual(next(iter(observed.values()))["bytes"], 10)
            self.assertEqual(len(source_inventory(objects, "Order:a", "generation", "1"*32)), 2)

    def test_network_inventory_refuses_truncated_or_oversized_listing(self):
        xml = b'<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><IsTruncated>true</IsTruncated></ListBucketResult>'
        with patch("follower_faults.store_read", return_value=xml), self.assertRaises(AuditError):
            replica_inventory(Path("unused"), object())
