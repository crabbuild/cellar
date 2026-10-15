"""A destination proof cannot be repaired by retry or recovered from local data."""
import copy
import unittest

from faults import AuditError
import test_fleet_faults as fleet_oracle


def fixture():
    rows = fleet_oracle.fixture()
    scope = next(row for row in rows if row["event"] == "append_reply_held")["exchange"]["scope"]
    owner = dict(node="destination", epoch=1)
    lease = dict(node="destination", generation="original", expires_ms=1000,
                 log=dict(state="open", ensemble=["producer"], may_have_fleet_acks=True))
    index = next(i for i, row in enumerate(rows) if row["event"] == "attempt")
    rows.insert(index, dict(event="destination_proof_armed", scope=scope, owner=owner, lease=copy.deepcopy(lease), objects={},
                            proof_mode="fleet", peer_proof="bucket", replica_fault_node="destination", node_lease_ms=1000))
    index = next(i for i, row in enumerate(rows) if row["event"] == "process_killed")
    rows.insert(index, dict(event="destination_follower_accepted", scope=scope, owner=owner, lease=copy.deepcopy(lease), objects={},
                            refused=[dict(path=f"cells/{scope}/ltx/e1/2.ltx")]))
    index = next(i for i, row in enumerate(rows) if row["event"] == "destination_restarted")
    rows.insert(index, dict(event="destination_proof_discarded", files=[dict(path="node/data", bytes=100, sha256="e"*64)],
                            remaining_entries=0, lease=copy.deepcopy(lease), observed_ms=1001, objects={}))
    lost = next(row for row in rows if row["event"] == "append_reply_lost")
    rows.remove(lost)
    index = next(i for i, row in enumerate(rows) if row["event"] == "retry_resumed")
    rows[index:index] = [dict(event="destination_follower_recovered", owner=dict(node="destination", epoch=2),
                               lease=dict(node="destination", generation="replacement")), lost]
    return copy.deepcopy(rows)


class DestinationProofTests(unittest.TestCase):
    audit = fleet_oracle.FleetOracleTests.audit

    def test_follower_only_destination_recovery(self):
        self.assertEqual(self.audit(fixture())["destination_follower_recoveries"], 1)

    def test_missing_proof_stage_is_rejected(self):
        for event in ("destination_proof_armed", "destination_follower_accepted", "destination_proof_discarded", "destination_follower_recovered"):
            rows = [row for row in fixture() if row["event"] != event]
            with self.subTest(event=event), self.assertRaises(AuditError):
                self.audit(rows)

    def test_bucket_upload_at_acceptance_or_before_restart_is_rejected(self):
        for event in ("destination_follower_accepted", "destination_proof_discarded"):
            for kind in ("ltx", "bundle"):
                rows = fixture()
                accepted = next(row for row in rows if row["event"] == "destination_follower_accepted")
                key = accepted["refused"][0]["path"] if kind == "ltx" else "log/destination/original/bundle/e1-0.ltxb"
                next(row for row in rows if row["event"] == event)["objects"][key] = dict(bytes=100, sha256="f"*64)
                with self.subTest(event=event, kind=kind), self.assertRaisesRegex(AuditError, "bucket data"):
                    self.audit(rows)

    def test_false_proof_local_data_or_stale_authority_is_rejected(self):
        for kind in ("refused", "ensemble", "marker", "remaining", "expiry", "generation", "epoch", "policy"):
            rows = fixture()
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
            else: next(row for row in rows if row["event"] == "destination_proof_armed")["peer_proof"] = "fleet"
            with self.subTest(kind=kind), self.assertRaises(AuditError):
                self.audit(rows)

    def test_early_response_loss_cannot_enable_local_retry(self):
        rows = fixture()
        lost = next(row for row in rows if row["event"] == "append_reply_lost")
        rows.remove(lost)
        index = next(i for i, row in enumerate(rows) if row["event"] == "destination_restarted")
        rows.insert(index, lost)
        with self.assertRaisesRegex(AuditError, "reply loss enabled retry"):
            self.audit(rows)
