# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""The process-fault oracle must reject lost acknowledged work before replay."""

import copy
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from faults import AuditError, Ledger, audit_evidence, digest, validate_snapshot


def fixture():
    operations = {}
    sources = {}
    events = []
    for key, amount, offset in (("a", 1200, 1), ("b", 300, 2)):
        payload = {"order_id": key, "amount_cents": amount}
        receipt = {"payload_sha256": digest(payload), "source": f"Order:{key}", "incarnation": "0" * 32,
                   "lane": key * 64, "sequence": "1"}
        operations[key] = {"payload": payload, "sha256": receipt["payload_sha256"], "offset": offset,
                           "attempts": 1, "upstream_ack" if key == "b" else "ack": receipt}
        revision = {key: value for key, value in receipt.items() if key != "payload_sha256" and key != "sequence"}
        revision.update(next_sequence="2", settled_through="1" if key == "a" else "0")
        sources[key] = {"order": payload, "receipt": receipt,
                        "outbox": {"incarnation": "0" * 32, "pending_rows": "0" if key == "a" else "1",
                                   "lanes": [{"revision": revision, "pending_rows": "0" if key == "a" else "1"}]}}
        if key == "a":
            events.append(dict(receipt, order_id="a", event_id=f"v1:{receipt['lane']}:1", offset="1",
                               envelope_sha256="1" * 64, event=payload))
    state = {"sources": sources, "events": {"records": events},
             "effects": {"projection": {"orders": 0, "total_cents": 0}, "receipts": {}},
             "subscription": {"checkpoint": "0"}}
    return operations, state


class FaultOracleTests(unittest.TestCase):
    def verify(self, operations, state):
        return validate_snapshot(operations, state, [], 0, {"a": 0, "b": 1})

    def test_missing_client_or_discarded_upstream_acknowledgment_cannot_be_repaired(self):
        for key in ("a", "b"):
            operations, state = fixture()
            state["sources"][key]["order"] = None
            with self.assertRaisesRegex(AuditError, "missing or changed before replay"):
                self.verify(operations, state)

    def test_unissued_acknowledgment_is_not_valid_evidence(self):
        operations, state = fixture()
        operations["b"]["attempts"] = 0
        with self.assertRaisesRegex(AuditError, "never issued"):
            self.verify(operations, state)

    def test_settled_source_without_accepted_event_is_loss(self):
        operations, state = fixture()
        state["events"]["records"] = []
        with self.assertRaisesRegex(AuditError, "neither pending"):
            self.verify(operations, state)

    def test_duplicate_destination_or_mutated_payload_receipt_is_rejected(self):
        operations, state = fixture()
        state["events"]["records"].append(copy.deepcopy(state["events"]["records"][0]))
        with self.assertRaises(AuditError):
            self.verify(operations, state)
        operations, state = fixture()
        state["events"]["records"][0]["payload_sha256"] = "2" * 64
        with self.assertRaisesRegex(AuditError, "digest mismatch"):
            self.verify(operations, state)

    def test_changed_envelope_or_producer_identity_is_rejected(self):
        operations, state = fixture()
        operations["a"]["accepted"] = self.verify(operations, state)["a"]
        state["events"]["records"][0]["envelope_sha256"] = "2" * 64
        with self.assertRaisesRegex(AuditError, "immutable envelope changed"):
            self.verify(operations, state)
        operations, state = fixture()
        state["events"]["records"][0]["incarnation"] = "1" * 32
        with self.assertRaisesRegex(AuditError, "producer identity"):
            self.verify(operations, state)

    def test_partial_consumer_effect_or_early_checkpoint_is_rejected(self):
        operations, state = fixture()
        state["effects"]["projection"]["orders"] = 1
        with self.assertRaisesRegex(AuditError, "distinct receipt effects"):
            self.verify(operations, state)
        operations, state = fixture()
        state["subscription"]["checkpoint"] = "1"
        with self.assertRaisesRegex(AuditError, "checkpoint"):
            self.verify(operations, state)

    def test_failed_recovery_audit_blocks_replay_before_network(self):
        with tempfile.TemporaryDirectory() as directory:
            ledger = Ledger(Path(directory))
            try:
                with mock.patch("faults.read") as network:
                    with self.assertRaisesRegex(AuditError, "before successful recovery audit"):
                        ledger.issue("http://example", "b", replay=True)
                    network.assert_not_called()
            finally:
                ledger.close()

    def test_archive_accounts_for_intentional_capacity_reclamation(self):
        operations, state = fixture()
        receipt = state["events"]["records"].pop()
        receipt.pop("event")
        operations["a"]["accepted"] = receipt
        state["effects"] = {"projection": {"orders": 1, "total_cents": 1200}, "receipts": {"a": receipt}}
        accepted = validate_snapshot(operations, state, ["a"], 0, {"a": 0, "b": 1})
        self.assertEqual(accepted["a"], receipt)
        state["effects"]["receipts"] = {}
        with self.assertRaisesRegex(AuditError, "consumer receipt missing"):
            validate_snapshot(operations, state, ["a"], 0, {"a": 0, "b": 1})

    def test_saved_evidence_requires_distinct_audits_and_replay_after_proof(self):
        import json
        operations, state = fixture()
        a = copy.deepcopy(state["events"]["records"][0])
        a.pop("event")
        consumer = copy.deepcopy(state)
        consumer["effects"] = {"projection":{"orders":1,"total_cents":1200},"receipts":{"a":a}}
        final = copy.deepcopy(consumer)
        b = dict(operations["b"]["upstream_ack"], order_id="b", event_id="v1:"+"b"*64+":1",offset="2",envelope_sha256="2"*64)
        final["sources"]["b"]["outbox"]["pending_rows"]="0"
        final["sources"]["b"]["outbox"]["lanes"][0]["pending_rows"]="0"
        final["sources"]["b"]["outbox"]["lanes"][0]["revision"]["settled_through"]="1"
        final["events"]["records"]=[b]
        final["effects"]={"projection":{"orders":2,"total_cents":1500},"receipts":{"a":a,"b":b}}
        final["subscription"]["checkpoint"]="2"
        records=[{"event":"upstream_ack","key":"b"},{"event":"client_response_lost","key":"b"}]
        for label, snapshot in [("before-source-crash",state),("source-recovered-before-replay",state),("consumer-recovered-before-replay",consumer),("final",final)]:
            records.extend([{"event":"audit_snapshot","label":label,"operations":operations,"snapshot":snapshot},{"event":"audit_passed","label":label}])
            if label=="before-source-crash":
                records.append({"event":"process_killed","label":"acknowledged-source-with-pending-obligation","exit_code":-9})
            if label=="source-recovered-before-replay":
                records.extend([{"event":"consumer_proven_before_ack","phase":{"boundary":"consumer_proven_before_ack","token":"consumer-boundary","records":[a],"projection":{"orders":1,"total_cents":1200}}},{"event":"process_killed","label":"consumer-proven-effects-before-ack","exit_code":-9}])
            if label=="consumer-recovered-before-replay":
                records.extend([{"event":"attempt","key":"b","replay":True},{"event":"lease_expiry_observed","subscription":{"checkpoint":"0","observation":{"lease_active":False}}}])
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/"ledger.jsonl"
            def save(values):
                path.write_text("".join(json.dumps(value)+"\n" for value in values))
            save(records)
            self.assertEqual(audit_evidence(path),{"audits":4,"sigkills":2,"response_drops":1})
            altered=copy.deepcopy(records)
            altered.insert(0,{"event":"attempt","key":"b","replay":True})
            save(altered)
            with self.assertRaisesRegex(AuditError,"preceded recovery audit"):
                audit_evidence(path)
            altered=copy.deepcopy(records)
            altered[2]["label"]="final"
            save(altered)
            with self.assertRaises(AuditError):
                audit_evidence(path)


if __name__ == "__main__":
    unittest.main()
