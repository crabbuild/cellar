# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Saved independent-process evidence cannot conceal loss by replay."""
import base64
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from fleet_faults import audit_evidence
from faults import AuditError, digest


def fixture():
    payload={"order_id":"cross","amount_cents":725}
    receipt=dict(payload_sha256=digest(payload),source="Order:"+"a"*64,incarnation="0"*32,lane="b"*64,sequence="1")
    operation=dict(payload=payload,sha256=digest(payload),offset=1,attempts=1,ack=receipt)
    event=dict(receipt,order_id="cross",event_id="v1:"+"b"*64+":1",offset="1",envelope_sha256="c"*64,event=payload)
    revision={key:receipt[key] for key in ("source","incarnation","lane")}
    revision.update(next_sequence="2",settled_through="0")
    snapshot=dict(sources={"cross":dict(order=payload,receipt=receipt,host="producer",outbox=dict(incarnation="0"*32,pending_rows="1",lanes=[dict(revision=revision,pending_rows="1")]))},
        events={"records":[event]},effects=dict(host="consumer",projection=dict(orders=0,total_cents=0),receipts={}),subscription={"checkpoint":"0"})
    exchange=dict(method="__streamAppend",scope="__EventStream:"+"d"*64)
    for key in ("request","response"):
        data=key.encode();exchange[key]=base64.b64encode(data).decode();exchange[key+"_sha256"]=hashlib.sha256(data).hexdigest()
    plan=copy.deepcopy(operation);plan.pop("ack");plan["attempts"]=0
    rows=[dict(event="plan",operations={"cross":plan}),dict(event="fleet_started",pids=dict(destination=1,producer=2,consumer=3)),
        dict(event="attempt",key="cross",attempt=1,replay=False),dict(event="ack",key="cross",receipt=receipt),dict(event="append_reply_held",exchange=exchange)]
    for label in ("before-destination-loss","recovered-before-retry","accepted-after-retry","final"):
        current=copy.deepcopy(snapshot)
        if label in ("accepted-after-retry","final"):
            current["sources"]["cross"]["outbox"]["pending_rows"]="0"
            current["sources"]["cross"]["outbox"]["lanes"][0]["pending_rows"]="0"
            current["sources"]["cross"]["outbox"]["lanes"][0]["revision"]["settled_through"]="1"
        if label=="final":
            effect={key:value for key,value in event.items() if key!="event"}
            current["effects"].update(projection=dict(orders=1,total_cents=725),receipts={"cross":effect})
            current["subscription"]["checkpoint"]="1"
        rows.extend([dict(event="fleet_audit",label=label,operations={"cross":operation},snapshot=current),dict(event="fleet_audit_passed",label=label)])
        if label=="before-destination-loss":
            rows.extend([dict(event="process_killed",label="destination-after-proven-append",pid=1,exit_code=-9),dict(event="append_reply_lost",request_sha256=exchange["request_sha256"]),dict(event="destination_restarted",pid=4)])
        if label=="recovered-before-retry":
            rows.extend([dict(event="retry_resumed",label=label),dict(event="append_reply_forwarded",exchange=exchange)])
    for node,pid in (("destination",4),("producer",2),("consumer",3)):
        rows.append(dict(event="fleet_process_stopped",node=node,pid=pid,exit_code=0,reports=[dict(outcome="stopped",ingress_drained=True,application_work_drained=True,cells_drained=True)]))
    return copy.deepcopy(rows)


class FleetOracleTests(unittest.TestCase):
    def audit(self,rows):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/"ledger.jsonl"
            path.write_text("".join(json.dumps(row)+"\n" for row in rows))
            return audit_evidence(path)

    def test_complete_independent_process_evidence(self):
        self.assertEqual(self.audit(fixture())["destination_sigkills"],1)

    def test_network_store_evidence_requires_matching_cleanup_after_shutdown(self):
        rows=fixture()
        rows.insert(1,dict(event="network_store_started",backend="s3",container="cellar-stream-store-private",
            image_id="sha256:"+"e"*64,bucket="stream-private",endpoint="http://127.0.0.1:9000"))
        rows.append(dict(event="network_store_removed",container="cellar-stream-store-private"))
        self.assertEqual(self.audit(rows)["backend"],"s3")
        with self.assertRaisesRegex(AuditError,"cleanup evidence"):self.audit(rows[:-1])
        altered=copy.deepcopy(rows);altered[-1]["container"]="another-store"
        with self.assertRaisesRegex(AuditError,"removed before"):self.audit(altered)
        altered=copy.deepcopy(rows);altered.insert(3,altered.pop())
        with self.assertRaisesRegex(AuditError,"removed before"):self.audit(altered)

    def test_changed_original_command_is_rejected(self):
        rows=fixture();rows[0]["operations"]["cross"]["payload"]["amount_cents"]=726
        with self.assertRaisesRegex(AuditError,"original measuring"):self.audit(rows)

    def test_missing_proven_acceptance_cannot_be_repaired(self):
        rows=fixture()
        next(row for row in rows if row.get("label")=="recovered-before-retry" and row["event"]=="fleet_audit")["snapshot"]["events"]["records"]=[]
        with self.assertRaisesRegex(AuditError,"lost proven"):self.audit(rows)

    def test_retry_before_recovery_audit_is_rejected(self):
        rows=fixture();row=next(row for row in rows if row["event"]=="retry_resumed");rows.remove(row);rows.insert(5,row)
        with self.assertRaisesRegex(AuditError,"concealed"):self.audit(rows)

    def test_changed_peer_reply_bytes_are_rejected(self):
        rows=fixture();next(row for row in rows if row["event"]=="append_reply_held")["exchange"]["response"]="YWx0ZXJlZA=="
        with self.assertRaisesRegex(AuditError,"peer bytes"):self.audit(rows)

    def test_foreign_process_kill_or_shutdown_is_rejected(self):
        for event in ("process_killed","fleet_process_stopped"):
            rows=fixture();next(row for row in rows if row["event"]==event)["pid"]=9
            with self.assertRaises(AuditError):self.audit(rows)

    def test_missing_final_shutdown_or_duplicate_effect_is_rejected(self):
        rows=fixture();rows.pop()
        with self.assertRaisesRegex(AuditError,"incomplete"):self.audit(rows)
        rows=fixture();next(row for row in rows if row["event"]=="fleet_audit" and row["label"]=="final")["snapshot"]["effects"]["projection"]["orders"]=2
        with self.assertRaisesRegex(AuditError,"totals"):self.audit(rows)

    def test_no_actual_reply_loss_is_not_qualified(self):
        with self.assertRaisesRegex(AuditError,"recovery audit"):self.audit([row for row in fixture() if row["event"]!="append_reply_lost"])

    def test_recovery_snapshot_or_shutdown_reordered_before_its_boundary_is_rejected(self):
        for event in ("fleet_audit", "fleet_process_stopped"):
            rows=fixture()
            row=next(row for row in rows if row["event"]==event and (event!="fleet_audit" or row["label"]=="recovered-before-retry"))
            rows.remove(row);rows.insert(5,row)
            with self.assertRaises(AuditError):self.audit(rows)

    def test_other_resource_class_and_mutated_blocked_retry_are_rejected(self):
        rows=fixture();next(row for row in rows if row["event"]=="append_reply_held")["exchange"]["scope"]="Order:"+"d"*64
        with self.assertRaisesRegex(AuditError,"resource class"):self.audit(rows)
        rows=fixture();rows.insert(6,dict(event="retry_blocked",request_sha256="e"*64))
        with self.assertRaisesRegex(AuditError,"original bytes"):self.audit(rows)
