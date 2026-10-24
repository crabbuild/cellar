# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from destination_startup import audit,EVENT,MESSAGE
from faults import AuditError
from test_destination_disks import fixture
import test_destination_disks
import fetch_faults


def ordered():
    rows=fixture("fetch",expired=True)
    next(row for row in rows if row["event"]=="fetch_fleet_started")["recovery_first"]=True
    accepted=next(row for row in rows if row["event"]=="destination_follower_accepted")
    retained=next(row for row in rows if row["event"]=="destination_followers_retained")
    lease=copy.deepcopy(accepted["lease"]);lease["log"].update(state="recovering",claimant="destination")
    refusal=f"WARN {MESSAGE} attempt=1 error=node-log recovery for destination/{lease['lineage']}/{lease['generation']}: no complete true witness; refusing to seal while member state remains unverified"
    raw=("INFO recovering predecessor\n"+refusal+"\n").encode()
    row=dict(event=EVENT,owner=accepted["owner"],observed_ms=1001,leases=dict(destination=lease,**copy.deepcopy(retained["leases"])),
             prefix_bytes=len(raw),prefix_sha256=hashlib.sha256(raw).hexdigest(),refusal=refusal)
    index=next(i for i,row in enumerate(rows) if row["event"]=="destination_follower_process_started")
    rows.insert(index,row)
    return rows,raw


class StartupTests(unittest.TestCase):
    def check(self,rows,raw,complete=True):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);path=root/"ledger.jsonl"
            path.write_text("".join(json.dumps(row)+"\n" for row in rows))
            (root/"destination-recovered.log").write_bytes(raw+b"later successful recovery\n")
            return audit(path,complete)

    def test_actual_first_refusal_precedes_witness_return_and_survives_later_logs(self):
        self.assertEqual(self.check(*ordered()),1)
        rows,raw=ordered();index=next(i for i,row in enumerate(rows) if row["event"]=="destination_follower_process_started")
        self.assertEqual(self.check(rows[:index],raw,False),1)

    def test_fetch_auditor_delegates_the_registered_event_to_startup_validation(self):
        rows,raw=ordered()
        helper=test_destination_disks.DestinationDiskTests()
        self.assertEqual(helper.audit(rows,"fetch",startup_prefix=raw)["startup_recovery_cuts"],1)
        next(row for row in rows if row["event"]==EVENT)["owner"]=dict(node="producer",epoch=2)
        with self.assertRaises(AuditError):helper.audit(rows,"fetch",startup_prefix=raw)

    def test_missing_wrong_or_fabricated_stage_is_rejected(self):
        for kind in ("missing","late","sealed","claimant","peer","cause","owner","prefix","policy","duplicate"):
            rows,raw=ordered();row=next(row for row in rows if row["event"]==EVENT)
            if kind=="missing":rows.remove(row)
            elif kind=="late":rows.remove(row);rows.append(row)
            elif kind=="sealed":row["leases"]["destination"]["log"]["state"]="sealed"
            elif kind=="claimant":row["leases"]["destination"]["log"]["claimant"]="producer"
            elif kind=="peer":row["leases"]["producer"]["generation"]="new"
            elif kind=="cause":row["refusal"]="different failure"
            elif kind=="owner":row["owner"]=dict(node="producer",epoch=2)
            elif kind=="prefix":raw=b"changed"+raw
            elif kind=="policy":next(row for row in rows if row["event"]=="fetch_fleet_started")["node_lease_ms"]=1000
            else:rows.append(copy.deepcopy(row))
            with self.subTest(kind=kind),self.assertRaises(AuditError):self.check(rows,raw)

    def test_unsupported_policy_fails_before_creating_a_fixture(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)/"unused"
            with self.assertRaisesRegex(AuditError,"recovery-first requires"):
                fetch_faults.exercise(Path("unused-binary"),root,recovery_first=True)
            self.assertFalse(root.exists())
