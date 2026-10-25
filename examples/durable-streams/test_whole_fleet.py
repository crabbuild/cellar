# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Whole-fleet evidence must prove loss and recovered state before replay."""
import copy
import json
from pathlib import Path
import tempfile
import unittest

from faults import AuditError, digest
from test_fleet_faults import fixture as append_fixture
from whole_fleet import audit_evidence, ROLES, STAGES


def fixture():
    baseline=append_fixture()
    cross=copy.deepcopy(next(row for row in baseline if row["event"]=="fleet_audit" and row["label"]=="final"))
    snapshot=cross["snapshot"];snapshot["subscription"]["checkpoint"]="0"
    event=copy.deepcopy(snapshot["events"]["records"][0])
    payload=dict(order_id="waiting",amount_cents=375)
    receipt=dict(payload_sha256=digest(payload),source="Order:"+"e"*64,incarnation="1"*32,lane="f"*64,sequence="1")
    waiting=dict(payload=payload,sha256=digest(payload),offset=2,attempts=1,ack=receipt)
    operations=dict(cross=cross["operations"]["cross"],waiting=waiting)
    source=copy.deepcopy(snapshot["sources"]["cross"])
    source.update(order=payload,receipt=receipt)
    source["outbox"].update(incarnation="1"*32,pending_rows="1")
    lane=source["outbox"]["lanes"][0];lane["pending_rows"]="1"
    lane["revision"].update(source=receipt["source"],incarnation=receipt["incarnation"],lane=receipt["lane"],settled_through="0")
    snapshot["sources"]["waiting"]=source
    event.update(receipt,order_id="waiting",event_id="v1:"+"f"*64+":1",offset="2",event=payload,envelope_sha256="d"*64)
    plan=copy.deepcopy(operations)
    for operation in plan.values():operation.pop("ack");operation["attempts"]=0
    rows=[dict(event="plan",operations=plan),dict(event="whole_fleet_started",pids=dict(destination=1,producer=2,consumer=3),proof_mode="bucket")]
    for key,operation in operations.items():
        rows.extend([dict(event="attempt",key=key,attempt=1,replay=False),dict(event="ack",key=key,receipt=operation["ack"])])
    for label in STAGES:
        current=copy.deepcopy(snapshot)
        if label in ("first-acknowledged","final"):
            current["subscription"]["checkpoint"]="1" if label=="first-acknowledged" else "2"
            outbox=current["sources"]["waiting"]["outbox"]
            outbox["pending_rows"]="0";outbox["lanes"][0]["pending_rows"]="0"
            outbox["lanes"][0]["revision"]["settled_through"]="1"
            current["events"]["records"]=[event]
        if label=="final":
            current["effects"]["receipts"]["waiting"]={key:value for key,value in event.items() if key!="event"}
            current["effects"]["projection"]=dict(orders=2,total_cents=1100)
        rows.extend([dict(event="whole_fleet_audit",label=label,operations=operations,snapshot=current),dict(event="whole_fleet_audit_passed",label=label)])
        if label=="before-whole-fleet-loss":
            for node,pid in zip(ROLES,(1,2,3)):
                rows.append(dict(event="process_killed",label="whole-fleet-"+node,pid=pid,exit_code=-9))
            for node in ROLES:
                rows.append(dict(event="local_node_data_discarded",node=node,files=[dict(path="node/state.sqlite",bytes=4096,sha256="a"*64)],remaining_entries=0))
            for node,pid in zip(ROLES,(4,5,6)):
                rows.append(dict(event="whole_fleet_restarted",node=node,pid=pid,proof_mode="bucket"))
        if label=="recovered-before-replay":rows.append(dict(event="consumer_replay",fresh_effects=0,status="acknowledged"))
    for node,pid in zip(ROLES,(4,5,6)):
        rows.append(dict(event="fleet_process_stopped",node=node,pid=pid,exit_code=0,reports=[dict(ingress_drained=True,application_work_drained=True,cells_drained=True)]))
    return copy.deepcopy(rows)


class WholeFleetOracleTests(unittest.TestCase):
    def audit(self,rows):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/"ledger.jsonl";path.write_text("".join(json.dumps(row)+"\n" for row in rows))
            return audit_evidence(path)

    def test_complete_bucket_recovery_evidence(self):
        self.assertEqual(self.audit(fixture())["discarded_node_directories"],3)

    def test_network_store_must_survive_until_complete_replacement_shutdown(self):
        rows=fixture()
        rows.insert(1,dict(event="network_store_started",backend="s3",container="cellar-stream-store-private",image_id="sha256:"+"a"*64))
        rows.append(dict(event="network_store_removed",container="cellar-stream-store-private"))
        self.assertEqual(self.audit(rows)["backend"],"s3")
        with self.assertRaises(AuditError):self.audit(rows[:-1])
        row=rows.pop();rows.insert(3,row)
        with self.assertRaises(AuditError):self.audit(rows)

    def test_missing_local_loss_or_wrong_process_is_rejected(self):
        rows=fixture();rows=[row for row in rows if row["event"]!="local_node_data_discarded" or row["node"]!="producer"]
        with self.assertRaises(AuditError):self.audit(rows)
        rows=fixture();next(row for row in rows if row["event"]=="process_killed")["pid"]=99
        with self.assertRaises(AuditError):self.audit(rows)

    def test_discard_before_all_kills_and_restart_before_discard_is_rejected(self):
        for event in ("local_node_data_discarded","whole_fleet_restarted"):
            rows=fixture();row=next(row for row in rows if row["event"]==event);rows.remove(row);rows.insert(6,row)
            with self.assertRaises(AuditError):self.audit(rows)

    def test_replay_before_recovery_or_duplicate_effect_is_rejected(self):
        rows=fixture();row=next(row for row in rows if row["event"]=="consumer_replay");rows.remove(row);rows.insert(6,row)
        with self.assertRaises(AuditError):self.audit(rows)
        rows=fixture();next(row for row in rows if row["event"]=="consumer_replay")["fresh_effects"]=1
        with self.assertRaises(AuditError):self.audit(rows)

    def test_lost_acceptance_effect_or_pending_obligation_is_rejected(self):
        for field in ("events","effects","source"):
            rows=fixture();snapshot=next(row for row in rows if row["event"]=="whole_fleet_audit" and row["label"]=="recovered-before-replay")["snapshot"]
            if field=="events":snapshot["events"]["records"]=[]
            elif field=="effects":snapshot["effects"]["receipts"]={}
            else:snapshot["sources"]["waiting"]["order"]=None
            with self.assertRaises(AuditError):self.audit(rows)

    def test_changed_command_and_missing_shutdown_are_rejected(self):
        rows=fixture();rows[0]["operations"]["waiting"]["payload"]["amount_cents"]=376
        with self.assertRaises(AuditError):self.audit(rows)
        with self.assertRaises(AuditError):self.audit(fixture()[:-1])

    def test_fleet_proof_or_empty_discard_inventory_cannot_claim_bucket_recovery(self):
        for event,field,value in (("whole_fleet_started","proof_mode","fleet"),("local_node_data_discarded","files",[])):
            rows=fixture();next(row for row in rows if row["event"]==event)[field]=value
            with self.assertRaises(AuditError):self.audit(rows)
