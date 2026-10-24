# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""A saved ack-loss qualification cannot substitute another batch or repair state."""
import base64
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from ack_faults import audit_evidence
from faults import AuditError
from test_fleet_faults import fixture as append_fixture


def fixture():
    baseline=append_fixture()
    initial=copy.deepcopy(next(row for row in baseline if row["event"]=="fleet_audit" and row["label"]=="accepted-after-retry"))
    final=copy.deepcopy(next(row for row in baseline if row["event"]=="fleet_audit" and row["label"]=="final"))
    final["snapshot"]["effects"]["ack_receipt_sha256"]="e"*64
    final["snapshot"]["subscription"]["observation"]=dict(lease_active=False)
    exchange=dict(method="__streamAck",scope="__EventStream:"+"d"*64)
    for field in ("request","response"):
        data=field.encode();exchange[field]=base64.b64encode(data).decode();exchange[field+"_sha256"]=hashlib.sha256(data).hexdigest()
    rows=[copy.deepcopy(baseline[0]),dict(event="ack_fleet_started",pids=dict(destination=1,producer=2,consumer=3)),copy.deepcopy(baseline[2]),copy.deepcopy(baseline[3])]
    for label in ("accepted-before-consumption","before-destination-loss","recovered-before-replay","final"):
        source=initial if label=="accepted-before-consumption" else final
        rows.extend([dict(event="ack_audit",label=label,operations=copy.deepcopy(source["operations"]),snapshot=copy.deepcopy(source["snapshot"])),dict(event="ack_audit_passed",label=label)])
        if label=="accepted-before-consumption":rows.append(dict(event="ack_reply_held",exchange=exchange))
        if label=="before-destination-loss":
            rows.extend([dict(event="process_killed",label="destination-after-proven-ack",pid=1,exit_code=-9),
                dict(event="ack_reply_lost",request_sha256=exchange["request_sha256"]),
                dict(event="original_ack_response",status="reply_lost",fresh_effects=1),dict(event="ack_destination_restarted",pid=4)])
        if label=="recovered-before-replay":
            reply=copy.deepcopy(exchange);data=b'already_acknowledged'
            reply["response"]=base64.b64encode(data).decode();reply["response_sha256"]=hashlib.sha256(data).hexdigest()
            rows.extend([dict(event="retry_resumed",label=label),dict(event="ack_reply_forwarded",exchange=reply),dict(event="stored_ack_replayed",status="AlreadyAcknowledged")])
    for node,pid in (("destination",4),("producer",2),("consumer",3)):
        rows.append(dict(event="fleet_process_stopped",node=node,pid=pid,exit_code=0,reports=[dict(ingress_drained=True,application_work_drained=True,cells_drained=True)]))
    return copy.deepcopy(rows)


class AckOracleTests(unittest.TestCase):
    def audit(self,rows):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/"ledger.jsonl";path.write_text("".join(json.dumps(row)+"\n" for row in rows))
            return audit_evidence(path)

    def test_complete_independent_ack_loss_evidence(self):
        self.assertEqual(self.audit(fixture())["identical_receipt_replays"],1)

    def test_transport_ambiguity_requires_another_independent_audit(self):
        rows=fixture()
        index=next(i for i,row in enumerate(rows) if row["event"]=="ack_reply_forwarded")
        snapshot=copy.deepcopy(next(row for row in rows if row["event"]=="ack_audit" and row["label"]=="recovered-before-replay"))
        snapshot["label"]="replay-interrupted-0"
        rows[index:index]=[dict(event="stored_ack_ambiguous",error="stale pooled connection"),snapshot,dict(event="ack_audit_passed",label=snapshot["label"])]
        self.assertEqual(self.audit(rows)["audits"],5)
        with self.assertRaises(AuditError):self.audit([row for row in rows if row["event"]!="stored_ack_ambiguous"])

    def test_new_receipt_or_another_rpc_cannot_substitute_exact_ack(self):
        for field,value in (("request",base64.b64encode(b'new receipt').decode()),("method","__streamFetch")):
            rows=fixture();next(row for row in rows if row["event"]=="ack_reply_forwarded")["exchange"][field]=value
            with self.assertRaises(AuditError):self.audit(rows)

    def test_checkpoint_loss_live_lease_or_missing_effect_is_rejected(self):
        for field in ("checkpoint","lease","effect"):
            rows=fixture();snapshot=next(row for row in rows if row["event"]=="ack_audit" and row["label"]=="recovered-before-replay")["snapshot"]
            if field=="checkpoint":snapshot["subscription"]["checkpoint"]="0"
            elif field=="lease":snapshot["subscription"]["observation"]["lease_active"]=True
            else:snapshot["effects"]["receipts"]={}
            with self.assertRaises(AuditError):self.audit(rows)

    def test_original_reply_loss_and_process_identity_are_required(self):
        for event in ("original_ack_response","ack_reply_lost","process_killed"):
            rows=[row for row in fixture() if row["event"]!=event]
            with self.assertRaises(AuditError):self.audit(rows)
        rows=fixture();next(row for row in rows if row["event"]=="process_killed")["pid"]=3
        with self.assertRaises(AuditError):self.audit(rows)

    def test_replay_or_restart_before_its_audit_boundary_is_rejected(self):
        for event in ("retry_resumed","ack_destination_restarted","stored_ack_replayed"):
            rows=fixture();row=next(row for row in rows if row["event"]==event);rows.remove(row);rows.insert(5,row)
            with self.assertRaises(AuditError):self.audit(rows)

    def test_stored_receipt_or_proven_peer_bytes_cannot_change(self):
        rows=fixture();next(row for row in rows if row["event"]=="ack_audit" and row["label"]=="recovered-before-replay")["snapshot"]["effects"]["ack_receipt_sha256"]="f"*64
        with self.assertRaises(AuditError):self.audit(rows)
        rows=fixture();next(row for row in rows if row["event"]=="ack_reply_held")["exchange"]["response"]="YWx0ZXJlZA=="
        with self.assertRaises(AuditError):self.audit(rows)

    def test_wrong_replay_status_duplicate_effect_or_missing_shutdown_is_rejected(self):
        rows=fixture();next(row for row in rows if row["event"]=="stored_ack_replayed")["status"]="Acknowledged"
        with self.assertRaises(AuditError):self.audit(rows)
        rows=fixture();next(row for row in rows if row["event"]=="ack_audit" and row["label"]=="final")["snapshot"]["effects"]["projection"]["orders"]=2
        with self.assertRaises(AuditError):self.audit(rows)
        with self.assertRaises(AuditError):self.audit(fixture()[:-1])

    def test_private_network_store_cleanup_follows_complete_shutdown(self):
        rows=fixture();rows.insert(1,dict(event="network_store_started",backend="s3",container="cellar-stream-store-private",image_id="sha256:"+"a"*64))
        rows.append(dict(event="network_store_removed",container="cellar-stream-store-private"))
        self.assertEqual(self.audit(rows)["backend"],"s3")
        with self.assertRaises(AuditError):self.audit(rows[:-1])
        row=rows.pop();rows.insert(6,row)
        with self.assertRaises(AuditError):self.audit(rows)
