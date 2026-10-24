# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Follower-only subscription proof must precede loss and every replay."""
import copy
import unittest

from faults import AuditError
import test_ack_faults as ack_oracle
import test_fetch_faults as fetch_oracle


def fixture(operation, expired=False):
    if operation == "ack":
        rows = ack_oracle.fixture()
    else:
        rows = fetch_oracle.expired_fixture() if expired else fetch_oracle.fixture()
    scope = next(row for row in rows if row["event"] == operation + "_reply_held")["exchange"]["scope"]
    owner = dict(node="destination", epoch=1)
    lease = dict(node="destination", lineage="1"*32, generation="original", expires_ms=1000,
                 log=dict(state="open", ensemble=["producer"], may_have_fleet_acks=True))
    index = next(i for i, row in enumerate(rows) if row["event"] == operation + "_reply_held")
    rows.insert(index, dict(event="destination_proof_armed", scope=scope, owner=owner, lease=copy.deepcopy(lease), objects={},
                            proof_mode="fleet", peer_proof="bucket", replica_fault_node="destination", node_lease_ms=30000 if expired else 1000))
    index = next(i for i, row in enumerate(rows) if row["event"] == "process_killed")
    rows.insert(index, dict(event="destination_follower_accepted", scope=scope, owner=owner, lease=copy.deepcopy(lease), objects={},
                            refused=[dict(path=f"cells/{scope}/ltx/e1/2.ltx")]))
    index = next(i for i, row in enumerate(rows) if row["event"] == operation + "_destination_restarted")
    rows.insert(index, dict(event="destination_proof_discarded", files=[dict(path="node/data", bytes=100, sha256="e"*64)],
                            remaining_entries=0, lease=copy.deepcopy(lease), observed_ms=1001, objects={}))
    index = next(i for i, row in enumerate(rows) if row["event"] == "retry_resumed")
    rows.insert(index, dict(event="destination_follower_recovered", owner=dict(node="destination", epoch=2),
                           lease=dict(node="destination", lineage="1"*32, generation="replacement")))
    return copy.deepcopy(rows)


class SubscriptionProofTests(unittest.TestCase):
    def audit(self, rows, operation):
        oracle = ack_oracle.AckOracleTests if operation == "ack" else fetch_oracle.FetchOracleTests
        return oracle.audit(self, rows)

    def test_ack_live_fetch_and_expired_fetch_follower_recovery(self):
        for operation, expired in (("ack", False), ("fetch", False), ("fetch", True)):
            with self.subTest(operation=operation, expired=expired):
                self.assertEqual(self.audit(fixture(operation, expired), operation)["destination_follower_recoveries"], 1)

    def test_missing_proof_stages_cannot_claim_recovery(self):
        for operation in ("ack", "fetch"):
            for event in ("destination_proof_armed", "destination_follower_accepted", "destination_proof_discarded", "destination_follower_recovered"):
                rows = [row for row in fixture(operation) if row["event"] != event]
                with self.subTest(operation=operation, event=event), self.assertRaises(AuditError):
                    self.audit(rows, operation)

    def test_ltx_or_bundle_publication_invalidates_follower_only_claim(self):
        for operation in ("ack", "fetch"):
            for event in ("destination_follower_accepted", "destination_proof_discarded"):
                for kind in ("ltx", "bundle"):
                    rows = fixture(operation)
                    accepted = next(row for row in rows if row["event"] == "destination_follower_accepted")
                    key = accepted["refused"][0]["path"] if kind == "ltx" else "log/destination/"+"1"*32+"/original/bundle/e1-0.ltxb"
                    next(row for row in rows if row["event"] == event)["objects"][key] = dict(bytes=100, sha256="f"*64)
                    with self.subTest(operation=operation, event=event, kind=kind), self.assertRaisesRegex(AuditError, "bucket data"):
                        self.audit(rows, operation)

    def test_local_data_false_proof_or_stale_authority_is_rejected(self):
        for operation in ("ack", "fetch"):
            for kind in ("refused", "ensemble", "marker", "remaining", "expiry", "generation", "epoch", "policy"):
                rows = fixture(operation)
                accepted = next(row for row in rows if row["event"] == "destination_follower_accepted")
                discarded = next(row for row in rows if row["event"] == "destination_proof_discarded")
                recovered = next(row for row in rows if row["event"] == "destination_follower_recovered")
                if kind == "refused": accepted["refused"] = []
                elif kind == "ensemble": accepted["lease"]["log"]["ensemble"] = []
                elif kind == "marker": accepted["lease"]["log"]["may_have_fleet_acks"] = False
                elif kind == "remaining": discarded["remaining_entries"] = 1
                elif kind == "expiry": discarded["observed_ms"] = 1000
                elif kind == "generation": recovered["lease"]["generation"] = "original"
                elif kind == "epoch": recovered["owner"]["epoch"] = 1
                else: next(row for row in rows if row["event"] == "destination_proof_armed")["node_lease_ms"] = 30000
                with self.subTest(operation=operation, kind=kind), self.assertRaises(AuditError):
                    self.audit(rows, operation)

    def test_retry_cannot_precede_proof_recovery(self):
        for operation in ("ack", "fetch"):
            rows = fixture(operation)
            row = next(row for row in rows if row["event"] == "destination_follower_recovered")
            rows.remove(row)
            rows.append(row)
            with self.subTest(operation=operation), self.assertRaisesRegex(AuditError, "retry concealed follower"):
                self.audit(rows, operation)
