"""Live-owner recovery cannot be repaired by replay or hidden stale commits."""
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from faults import AuditError, digest
from live_owner import audit_evidence
import test_fleet_faults as fleet_oracle


def fixture(cut="before_tx"):
    base = fleet_oracle.fixture()
    initial = copy.deepcopy(next(row for row in base if row["event"] == "fleet_audit" and row["label"] == "accepted-after-retry"))
    final = copy.deepcopy(next(row for row in base if row["event"] == "fleet_audit" and row["label"] == "final"))
    operations = initial["operations"]
    receipt = operations["cross"]["ack"]
    owner = dict(node="producer", epoch=1)
    entered = dict(boundary="source_poll_entered", pid=2, scope=receipt["source"], cut=cut)
    attempted = dict(entered, boundary="source_poll_attempted", refused=True, closed_domain=True)
    image = dict(order=operations["cross"]["payload"], receipt=receipt, lane=receipt["lane"], next_sequence="2", settled_through="1", pending_rows="0", files={"db.sqlite":"e"*64})
    rows = [copy.deepcopy(base[0]), dict(event="live_owner_started",pids=dict(destination=1,producer=2,consumer=3),cut=cut,
            proof_mode="bucket",node_lease_ms=1000,handler_budget_ms=10000),copy.deepcopy(base[2]),copy.deepcopy(base[3])]
    for label in ("before-isolation", "before-release", "after-refusal", "final"):
        snapshot = copy.deepcopy((final if label == "final" else initial)["snapshot"])
        if label != "before-isolation": snapshot["sources"]["cross"]["host"] = "successor"
        rows.append(dict(event="live_owner_audit",label=label,owner=owner if label=="before-isolation" else dict(node="successor",epoch=2),operations=operations,snapshot=snapshot))
        if label == "before-isolation":
            rows.extend([dict(event="live_owner_attempt",pid=2,cut=cut,payload=dict(order_id="cross",amount_cents=999),sha256=digest(dict(order_id="cross",amount_cents=999))),dict(event="live_owner_entered",phase=entered),dict(event="live_owner_isolated",pid=2,state="S"),
                dict(event="live_owner_authority_expired",lease=dict(node="producer",expires_ms=1000),observed_ms=1001,
                     refused=[dict(operation="get",path="nodes/producer.json")]),dict(event="live_owner_successor",pid=4)])
        elif label == "before-release":
            rows.extend([dict(event="live_owner_released",pid=2,state="S",phase=entered,elapsed_ms=2000),
                dict(event="live_owner_attempted",phase=attempted),dict(event="live_owner_fenced",pid=2,exit_code=1,
                     report=dict(outcome="fenced",fence_code=3,reason="node_lease_expired"),graceful_reports=0),
                dict(event="live_owner_response_interrupted",kind="OSError"),dict(event="live_owner_old_image",image=image)])
        elif label == "after-refusal": rows.append(dict(event="live_owner_consumed",fresh_effects=1,status="acknowledged"))
    for node,pid in (("destination",1),("consumer",3),("successor",4)):
        rows.append(dict(event="fleet_process_stopped",node=node,pid=pid,exit_code=0,
            reports=[dict(ingress_drained=True,application_work_drained=True,cells_drained=True)]))
    return json.loads(json.dumps(rows))


class LiveOwnerTests(unittest.TestCase):
    def audit(self, rows, changed_image=False):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/"ledger.jsonl"
            path.write_text("".join(json.dumps(row)+"\n" for row in rows))
            image=copy.deepcopy(next(row["image"] for row in fixture() if row["event"]=="live_owner_old_image"))
            if changed_image: image["files"]["db.sqlite"]="f"*64
            with patch("live_owner.inspect_image",return_value=image): return audit_evidence(path)

    def test_both_live_callback_cuts(self):
        for cut in ("before_tx","before_commit"):
            self.assertEqual(self.audit(fixture(cut))["live_callback_takeovers"],1)

    def test_missing_stages_or_premature_release_are_rejected(self):
        for event in ("live_owner_attempt","live_owner_entered","live_owner_isolated","live_owner_authority_expired","live_owner_successor","live_owner_released","live_owner_attempted","live_owner_fenced","live_owner_response_interrupted","live_owner_old_image","live_owner_consumed","fleet_process_stopped"):
            rows=[row for row in fixture() if row["event"]!=event]
            with self.subTest(event=event),self.assertRaises(AuditError): self.audit(rows)
        rows=fixture(); row=next(row for row in rows if row["event"]=="live_owner_released");rows.remove(row);rows.insert(5,row)
        with self.assertRaises(AuditError): self.audit(rows)

    def test_paused_dead_or_reused_process_and_stale_authority_are_rejected(self):
        for kind in ("paused","traced","dead","pid","scope","cut","epoch","expiry","refusal","budget"):
            rows=fixture()
            if kind in ("paused","traced","dead"): next(row for row in rows if row["event"]=="live_owner_released")["state"]={"paused":"T","traced":"t","dead":"Z"}[kind]
            elif kind=="pid": next(row for row in rows if row["event"]=="live_owner_successor")["pid"]=2
            elif kind in ("scope","cut"): next(row for row in rows if row["event"]=="live_owner_attempted")["phase"][kind]="other"
            elif kind=="epoch": next(row for row in rows if row["event"]=="live_owner_audit" and row["label"]=="before-release")["owner"]["epoch"]=1
            elif kind=="expiry": next(row for row in rows if row["event"]=="live_owner_authority_expired")["observed_ms"]=1000
            elif kind=="refusal": next(row for row in rows if row["event"]=="live_owner_authority_expired")["refused"]=[]
            else: next(row for row in rows if row["event"]=="live_owner_released")["elapsed_ms"]=10000
            with self.subTest(kind=kind),self.assertRaises(AuditError): self.audit(rows)

    def test_stale_commit_wrong_fence_or_forensic_changes_are_rejected(self):
        for kind in ("refused","closed_domain","order","sequence","pending","fence"):
            rows=fixture()
            if kind in ("refused","closed_domain"): next(row for row in rows if row["event"]=="live_owner_attempted")["phase"][kind]=False
            elif kind=="fence": next(row for row in rows if row["event"]=="live_owner_fenced")["report"]["reason"]="node_lease_missing"
            else:
                image=next(row for row in rows if row["event"]=="live_owner_old_image")["image"]
                if kind=="order": image["order"]["amount_cents"]=999
                elif kind=="sequence": image["next_sequence"]="3"
                else: image["pending_rows"]="1"
            with self.subTest(kind=kind),self.assertRaises(AuditError): self.audit(rows)
        with self.assertRaisesRegex(AuditError,"archive changed"): self.audit(fixture(),True)

    def test_replay_or_prefix_loss_cannot_repair_recovery(self):
        rows=fixture(); next(row for row in rows if row["event"]=="live_owner_attempt")["payload"]["amount_cents"]=1000
        with self.assertRaises(AuditError): self.audit(rows)
        rows=fixture(); rows.insert(8,dict(event="attempt",key="cross",attempt=2,replay=True))
        with self.assertRaises(AuditError): self.audit(rows)
        rows=fixture();next(row for row in rows if row["event"]=="live_owner_audit" and row["label"]=="before-release")["snapshot"]["events"]["records"]=[]
        with self.assertRaises(AuditError): self.audit(rows)

    def test_network_store_requires_complete_cleanup(self):
        rows=fixture(); rows.insert(1,dict(event="network_store_started",backend="s3",image_id="sha256:"+"a"*64,container="cellar-stream-store-private"))
        rows.append(dict(event="network_store_removed",container="cellar-stream-store-private"))
        self.assertEqual(self.audit(rows)["backend"],"s3")
        with self.assertRaises(AuditError): self.audit(rows[:-1])
