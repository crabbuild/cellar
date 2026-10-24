# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Reject peer-failure proofs that also lose bucket authority or miss healing."""

import copy
import json
import pathlib
import tempfile
import unittest

from faults import Ledger
from peer_partition import validate


def proof():
    peer_paths = [{"source": i, "target": j, "connected": True} for i, j in ((1, 2), (1, 3), (2, 1), (3, 1))]
    connectivity = {"peer_paths": peer_paths, "bucket_paths": [{"node": i, "connected": True} for i in range(1, 4)],
                    "unaffected_peer_paths": [{"source": i, "target": j, "connected": True} for i, j in ((2, 3), (3, 2))]}
    value = {"owner": 1, "partition_s": 25, "controlled_ack": True, "controlled_ack_node": "node1",
             "peer_reads": [{"node": i, "failed": True, "http_status": 503} for i in (2, 3)]}
    for label, now in (("before", 100_000), ("during", 125_000), ("after", 126_000)):
        value[label] = {"connectivity": copy.deepcopy(connectivity), "authority": {
            "node": "node1", "lease_node": "node1", "epoch": 2, "generation": "process-1",
            "observed_ms": now, "expires_ms": now + 10_000}}
        for path in value[label]["connectivity"]["peer_paths"]:
            path["connected"] = label != "during"
    return value


class PeerPartitionProofTests(unittest.TestCase):
    def test_valid_partition_preserves_lease_generation_and_cell_epoch(self):
        validate(proof())

    def test_unreachable_bucket_is_full_isolation_not_peer_only_failure(self):
        value = proof()
        value["during"]["connectivity"]["bucket_paths"][0]["connected"] = False
        with self.assertRaises(RuntimeError):
            validate(value)

    def test_every_peer_direction_must_fail_then_heal(self):
        for label in ("before", "during", "after"):
            value = proof()
            value[label]["connectivity"]["peer_paths"][0]["connected"] = label == "during"
            with self.subTest(label=label), self.assertRaises(RuntimeError):
                validate(value)
        value = proof()
        value["during"]["connectivity"]["peer_paths"].pop()
        with self.assertRaises(RuntimeError):
            validate(value)

    def test_unrelated_peer_connectivity_must_survive(self):
        value = proof()
        value["during"]["connectivity"]["unaffected_peer_paths"][0]["connected"] = False
        with self.assertRaises(RuntimeError):
            validate(value)

    def test_owner_epoch_and_process_generation_cannot_change(self):
        for field, invalid in (("node", "node2"), ("lease_node", "node2"), ("epoch", 3), ("generation", "replacement")):
            value = proof()
            value["during"]["authority"][field] = invalid
            with self.subTest(field=field), self.assertRaises(RuntimeError):
                validate(value)

    def test_lease_must_remain_valid_and_renew_beyond_two_lifetimes(self):
        value = proof()
        value["during"]["authority"]["expires_ms"] = value["before"]["authority"]["expires_ms"]
        with self.assertRaises(RuntimeError):
            validate(value)
        value = proof()
        value["partition_s"] = 5
        with self.assertRaises(RuntimeError):
            validate(value)

    def test_acknowledgement_must_come_from_retained_owner(self):
        for field, invalid in (("controlled_ack", False), ("controlled_ack_node", "node2")):
            value = proof()
            value[field] = invalid
            with self.subTest(field=field), self.assertRaises(RuntimeError):
                validate(value)

    def test_peers_cannot_serve_the_cell_without_its_owner(self):
        value = proof()
        value["peer_reads"][0]["failed"] = False
        with self.assertRaises(RuntimeError):
            validate(value)

    def test_hot_cell_workload_is_in_the_fsynced_plan_before_issuance(self):
        with tempfile.TemporaryDirectory() as directory:
            ledger = Ledger(pathlib.Path(directory) / "ops.json", cells=8, burst=4, burst_cell=0)
            try:
                plan = json.loads(ledger.path.with_suffix(".jsonl").read_text().splitlines()[0])
                self.assertEqual([o["cell"] for o in plan["operations"] if o["phase"] == "burst"], [0] * 4)
                self.assertEqual([o["cell"] for o in plan["operations"] if o["phase"] == "warm"], list(range(8)))
            finally:
                ledger.close()


if __name__ == "__main__":
    unittest.main()
