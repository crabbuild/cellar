"""Saved fetch evidence must retain the live lease and audit before replay."""
import base64
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from faults import AuditError
from fetch_faults import FETCH_ID, audit_evidence
from test_fleet_faults import fixture as append_fixture


def fixture():
    baseline = append_fixture()
    initial = copy.deepcopy(next(row for row in baseline if row.get("label") == "accepted-after-retry" and row["event"] == "fleet_audit"))
    final = copy.deepcopy(next(row for row in baseline if row.get("label") == "final" and row["event"] == "fleet_audit"))
    identity = dict(resource="0" * 31 + "1", name="projection", id="1" * 32, generation="2" * 32)
    lease = dict(generation="1", first="1", last="1", expires_ms="30000")
    receipt = dict(resource=identity["resource"], partition=0, name="projection", subscription=identity["id"],
                   incarnation=identity["generation"], generation="1", token="a" * 64, first="1", last="1")
    encoded = "v1." + base64.urlsafe_b64encode(json.dumps(receipt).encode()).decode().rstrip("=")
    record = copy.deepcopy(final["snapshot"]["effects"]["receipts"]["cross"])
    batch = dict(receipt=encoded, issued_ms="0", expires_ms="30000", records=[record])
    exchange = dict(method="__streamNext", scope="__EventStream:" + "d" * 64)
    for field in ("request", "response"):
        data = field.encode()
        exchange[field] = base64.b64encode(data).decode()
        exchange[field + "_sha256"] = hashlib.sha256(data).hexdigest()
    rows = [copy.deepcopy(baseline[0]), dict(event="fetch_fleet_started", lease_state="live", node_lease_ms=1000, pids=dict(destination=1, producer=2, consumer=3)),
            copy.deepcopy(baseline[2]), copy.deepcopy(baseline[3])]

    def audit(label, effects=False, checkpoint=0, leased=True):
        current = copy.deepcopy(final if effects else initial)
        current["snapshot"]["subscription"] = dict(identity=identity, checkpoint=str(checkpoint), lease=lease if leased else None,
                    observation=dict(lease_active=leased, observed_ms="2"))
        if effects:
            current["snapshot"]["effects"]["ack_receipt_sha256"] = hashlib.sha256(encoded.encode()).hexdigest()
        rows.extend([dict(event="fetch_audit", label=label, operations=current["operations"], snapshot=current["snapshot"]),
                     dict(event="fetch_audit_passed", label=label)])

    audit("accepted-before-fetch", leased=False)
    rows.extend([dict(event="fetch_attempt", attempt=1, fetch_id=FETCH_ID), dict(event="fetch_reply_held", exchange=exchange)])
    audit("before-destination-loss")
    rows.extend([dict(event="process_killed", label="destination-after-proven-fetch", pid=1, exit_code=-9),
                 dict(event="fetch_reply_lost", request_sha256=exchange["request_sha256"]),
                 dict(event="fetch_outcome", attempt=1, result=dict(status="Ambiguous", fresh_effects=0, projection=dict(orders=0, total_cents=0))),
                 dict(event="fetch_destination_restarted", pid=4)])
    audit("recovered-before-replay")
    rows.append(dict(event="retry_resumed", label="recovered-before-replay"))
    for attempt, label in ((2, "effect-proven"), (3, "effect-replayed")):
        rows.extend([dict(event="fetch_attempt", attempt=attempt, fetch_id=FETCH_ID), dict(event="fetch_reply_forwarded", exchange=copy.deepcopy(exchange)),
                     dict(event="fetch_outcome", attempt=attempt, result=dict(status="fetched", fresh_effects=1 if attempt == 2 else 0, batch=batch))])
        audit(label, effects=True)
    rows.append(dict(event="fetch_acknowledged", status="Acknowledged"))
    audit("final", effects=True, checkpoint=1, leased=False)
    for node, pid in (("destination", 4), ("producer", 2), ("consumer", 3)):
        rows.append(dict(event="fleet_process_stopped", node=node, pid=pid, exit_code=0,
                         reports=[dict(ingress_drained=True, application_work_drained=True, cells_drained=True)]))
    return json.loads(json.dumps(rows))


def expired_fixture():
    rows = fixture()
    started = next(row for row in rows if row["event"] == "fetch_fleet_started")
    started.update(lease_state="expired", node_lease_ms=30000)
    result = next(row for row in rows if row["event"] == "fetch_outcome" and row["attempt"] == 2)
    batch = copy.deepcopy(result["result"]["batch"])
    raw = batch["receipt"][3:]
    receipt = json.loads(base64.urlsafe_b64decode(raw + "=" * (-len(raw) % 4)))
    receipt.update(generation="2", token="b" * 64)
    batch.update(receipt="v1." + base64.urlsafe_b64encode(json.dumps(receipt).encode()).decode().rstrip("="),
                 issued_ms="30001", expires_ms="60001")
    for row in rows:
        if row["event"] == "fetch_outcome" and row["attempt"] > 1:
            row["result"]["batch"] = copy.deepcopy(batch)
        if row["event"] == "fetch_reply_forwarded":
            data = b"fresh lease response"
            row["exchange"].update(response=base64.b64encode(data).decode(), response_sha256=hashlib.sha256(data).hexdigest())
        if row["event"] == "fetch_audit" and row["label"] not in ("accepted-before-fetch", "before-destination-loss"):
            sub = row["snapshot"]["subscription"]
            sub["observation"]["observed_ms"] = "30002"
            if row["label"] == "recovered-before-replay":
                sub.update(lease=None)
                sub["observation"]["lease_active"] = False
            elif row["label"] != "final":
                sub["lease"].update(generation="2", expires_ms="60001")
            if row["snapshot"]["effects"]["receipts"]:
                row["snapshot"]["effects"]["ack_receipt_sha256"] = hashlib.sha256(batch["receipt"].encode()).hexdigest()
    return rows


class FetchOracleTests(unittest.TestCase):
    def audit(self, rows):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "ledger.jsonl"
            path.write_text("".join(json.dumps(row) + "\n" for row in rows))
            return audit_evidence(path)

    def test_complete_fetch_process_loss(self):
        self.assertEqual(self.audit(fixture())["exact_fetch_replies"], 2)

    def test_expired_fetch_requires_new_authority_and_stable_later_replay(self):
        self.assertEqual(self.audit(expired_fixture())["lease_state"], "expired")
        rows = expired_fixture()
        old = next(row for row in rows if row["event"] == "fetch_reply_held")["exchange"]
        next(row for row in rows if row["event"] == "fetch_reply_forwarded")["exchange"] = old
        with self.assertRaises(AuditError):
            self.audit(rows)

    def test_failed_live_prefix_cannot_mark_recovery_safe_for_replay(self):
        rows = expired_fixture()
        next(row for row in rows if row["event"] == "fetch_fleet_started").update(lease_state="live", node_lease_ms=1000)
        end = next(i for i, row in enumerate(rows) if row["event"] == "fetch_audit" and row["label"] == "recovered-before-replay")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "ledger.jsonl"
            path.write_text("".join(json.dumps(row) + "\n" for row in rows[:end+1]))
            with self.assertRaises(AuditError):
                audit_evidence(path, complete=False)

    def test_private_store_cleanup_must_follow_complete_shutdown(self):
        rows = fixture()
        rows.insert(1, dict(event="network_store_started", backend="s3", image_id="sha256:" + "e" * 64,
                            container="cellar-stream-store-private"))
        rows.append(dict(event="network_store_removed", container="cellar-stream-store-private"))
        self.assertEqual(self.audit(rows)["backend"], "s3")
        with self.assertRaises(AuditError):
            self.audit(rows[:-1])
        rows[-1]["container"] = "another-store"
        with self.assertRaises(AuditError):
            self.audit(rows)

    def test_store_startup_retry_cannot_cover_application_failures(self):
        rows = fixture()
        rows.insert(1, dict(event="network_store_startup_retry", status=503))
        rows.insert(2, dict(event="network_store_started", backend="s3", image_id="sha256:" + "e" * 64,
                            container="cellar-stream-store-private"))
        rows.append(dict(event="network_store_removed", container="cellar-stream-store-private"))
        self.assertEqual(self.audit(rows)["backend"], "s3")
        late = rows.pop(1)
        rows.insert(5, late)
        with self.assertRaises(AuditError):
            self.audit(rows)
        rows = fixture()
        rows.insert(1, dict(event="network_store_startup_retry", status=503))
        with self.assertRaises(AuditError):
            self.audit(rows)

    def test_wrong_process_or_unchanged_expired_receipt_cannot_qualify(self):
        rows = fixture()
        next(row for row in rows if row["event"] == "process_killed")["pid"] = 99
        with self.assertRaises(AuditError):
            self.audit(rows)
        rows = expired_fixture()
        batch = next(row for row in rows if row["event"] == "fetch_outcome" and row["attempt"] == 2)["result"]["batch"]
        raw = batch["receipt"][3:]
        receipt = json.loads(base64.urlsafe_b64decode(raw + "=" * (-len(raw) % 4)))
        receipt["generation"] = "1"
        batch["receipt"] = "v1." + base64.urlsafe_b64encode(json.dumps(receipt).encode()).decode().rstrip("=")
        with self.assertRaises(AuditError):
            self.audit(rows)

    def test_original_response_must_be_ambiguous_without_effects(self):
        for field, value in (("status", "fetched"), ("fresh_effects", 1)):
            rows = fixture()
            next(row for row in rows if row["event"] == "fetch_outcome")["result"][field] = value
            with self.assertRaisesRegex(AuditError, "fabricated"):
                self.audit(rows)

    def test_lease_cannot_expire_change_or_disappear_before_replay(self):
        for change in (lambda s: s.update(lease=None), lambda s: s["lease"].update(generation="2"),
                       lambda s: s["observation"].update(observed_ms="30000"), lambda s: s["identity"].update(generation="3" * 32)):
            rows = fixture()
            row = next(row for row in rows if row["event"] == "fetch_audit" and row["label"] == "recovered-before-replay")
            change(row["snapshot"]["subscription"])
            with self.assertRaises(AuditError):
                self.audit(rows)

    def test_replay_before_recovery_audit_and_wrong_fetch_identity_are_rejected(self):
        rows = fixture()
        row = next(row for row in rows if row["event"] == "retry_resumed")
        rows.remove(row)
        rows.insert(6, row)
        with self.assertRaises(AuditError):
            self.audit(rows)
        rows = fixture()
        next(row for row in rows if row["event"] == "fetch_attempt" and row["attempt"] == 2)["fetch_id"] = "f" * 32
        with self.assertRaises(AuditError):
            self.audit(rows)

    def test_changed_wire_bytes_or_recovered_batch_are_rejected(self):
        for event in ("fetch_reply_held", "fetch_reply_forwarded"):
            rows = fixture()
            next(row for row in rows if row["event"] == event)["exchange"]["response"] = "YWx0ZXJlZA=="
            with self.assertRaises(AuditError):
                self.audit(rows)
        rows = fixture()
        next(row for row in rows if row["event"] == "fetch_outcome" and row["attempt"] == 3)["result"]["batch"]["expires_ms"] = "30001"
        with self.assertRaises(AuditError):
            self.audit(rows)

    def test_duplicate_effect_or_changed_saved_receipt_is_rejected(self):
        rows = fixture()
        next(row for row in rows if row["event"] == "fetch_outcome" and row["attempt"] == 3)["result"]["fresh_effects"] = 1
        with self.assertRaises(AuditError):
            self.audit(rows)
        rows = fixture()
        next(row for row in rows if row["event"] == "fetch_audit" and row["label"] == "effect-proven")["snapshot"]["effects"]["ack_receipt_sha256"] = "f" * 64
        with self.assertRaises(AuditError):
            self.audit(rows)

    def test_interrupted_replay_cannot_repair_a_lost_proven_effect(self):
        rows = fixture()
        index = next(i for i, row in enumerate(rows) if row["event"] == "fetch_attempt" and row["attempt"] == 3)
        prior = copy.deepcopy(next(row for row in rows if row["event"] == "fetch_audit" and row["label"] == "effect-proven"))
        prior["label"] = "fetch-interrupted-3"
        for row in rows[index:]:
            if row["event"] in ("fetch_attempt", "fetch_outcome"):
                row["attempt"] = 4
        rows[index:index] = [dict(event="fetch_attempt", attempt=3, fetch_id=FETCH_ID),
            dict(event="fetch_outcome", attempt=3, result=dict(status="transport_ambiguous")),
            prior, dict(event="fetch_audit_passed", label="fetch-interrupted-3")]
        self.assertEqual(self.audit(rows)["audits"], 7)
        prior["snapshot"]["effects"].update(receipts={}, projection=dict(orders=0, total_cents=0))
        next(row for row in rows if row["event"] == "fetch_outcome" and row["attempt"] == 4)["result"]["fresh_effects"] = 1
        with self.assertRaisesRegex(AuditError, "effect disappeared"):
            self.audit(rows)

    def test_missing_actual_kill_loss_shutdown_or_final_ack_is_rejected(self):
        for event in ("process_killed", "fetch_reply_lost", "fleet_process_stopped", "fetch_acknowledged"):
            rows = fixture()
            rows.remove(next(row for row in rows if row["event"] == event))
            with self.assertRaises(AuditError):
                self.audit(rows)
