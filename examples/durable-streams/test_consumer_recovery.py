# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Follower proof cannot be replaced by bucket repair, replay or an empty inbox."""
import base64
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from consumer_recovery_audit import audit_evidence
from faults import AuditError
from test_consumer_owner import fixture as owner_fixture, row


def fixture(cold=False):
    base=owner_fixture();operations=copy.deepcopy(row(base,"consumer_owner_audit",label="final")["operations"])
    plan=copy.deepcopy(row(base,"plan"));batch=copy.deepcopy(row(base,"consumer_owner_fetch")["result"]["batch"])
    scope=row(base,"consumer_owner_entered")["phase"]["scope"]
    sha=hashlib.sha256(batch["receipt"].encode()).hexdigest()
    lease=dict(node="consumer",generation="c"*64,lineage="c"*32,expires_ms=2000,log=dict(state="open",epoch=1,ensemble=["destination","producer"],may_have_fleet_acks=True))
    old_image=copy.deepcopy(row(base,"consumer_owner_successor_image")["image"]);old_image["ack_receipt_sha256"]=sha
    peer_leases={name:dict(node=name,generation=char*64,lineage=char*32,expires_ms=2000,log=dict(state="open",may_have_fleet_acks=False)) for name,char in (("destination","d"),("producer","e"))}
    follower=dict(files=[dict(path="1-2.batch",bytes=100,sha256="1"*64)],seed={},batches=[dict(epoch=1,entries=[dict(cell=scope,cell_epoch=1,txid=2)])])
    retained={name:copy.deepcopy(follower) for name in peer_leases}
    successor=6 if cold else 4
    rows=[plan,dict(event="consumer_recovery_started",pids=dict(destination=1,producer=2,consumer=3),proof_mode="fleet",peer_proof="bucket",replica_fault_node="consumer",node_lease_ms=1000,restart_followers=cold),
        dict(event="attempt",key="a",attempt=1,replay=False),dict(event="ack",key="a",receipt=operations["a"]["ack"]),dict(event="consumer_recovery_seeded",result=dict(status="acknowledged",fresh_effects=1,projection=dict(orders=1,total_cents=1200))),
        dict(event="consumer_proof_armed",scope=scope,lease=lease,owner=dict(node="consumer",epoch=1),objects={}),dict(event="attempt",key="b",attempt=1,replay=False),dict(event="ack",key="b",receipt=operations["b"]["ack"]),
        dict(event="consumer_effect_proven",result=dict(status="fetched",fresh_effects=1,projection=dict(orders=2,total_cents=1500),batch=batch),owner=dict(node="consumer",epoch=1),lease=lease,objects={},refused=[dict(path=f"cells/{scope}/ltx/e1/data")])]
    for label in ("before-loss","recovered-before-replay","after-replay","final"):
        snapshot=copy.deepcopy(row(base,"consumer_owner_audit",label="final")["snapshot"])
        snapshot["effects"].update(host="consumer" if label=="before-loss" else "successor",ack_receipt_sha256=sha)
        snapshot["events"]["records"].insert(0,dict(snapshot["effects"]["receipts"]["a"],event=operations["a"]["payload"]))
        snapshot["subscription"]=copy.deepcopy(row(base,"consumer_owner_entered")["subscription"])
        snapshot["subscription"]["earliest"]="1"
        if label=="final":snapshot["subscription"].update(checkpoint="2",lease=None,observation=dict(lease_active=False,observed_ms="4000"))
        rows.append(dict(event="consumer_recovery_audit",label=label,operations=operations,owner=dict(node="consumer" if label=="before-loss" else "successor",epoch=1 if label=="before-loss" else 2),snapshot=snapshot,recoverer={} if label=="before-loss" else dict(pid=successor,lease=dict(generation="f"*64))))
        if label=="before-loss":
            rows.append(dict(event="process_killed",label="consumer-proof-consumer",pid=3,exit_code=-9))
            if cold:
                for name,pid in (("destination",1),("producer",2)):rows.append(dict(event="process_killed",label="consumer-proof-"+name,pid=pid,exit_code=-9))
                rows.append(dict(event="consumer_followers_retained",followers=retained,leases=peer_leases,objects={}))
            rows.extend([dict(event="consumer_proof_image",image=old_image),dict(event="consumer_data_discarded",files=[dict(path="node/db.sqlite",bytes=100,sha256="e"*64)],remaining_entries=0,objects={}),dict(event="consumer_authority_expired",lease=lease,observed_ms=2001)])
            if cold:
                for name,pid in (("destination",4),("producer",5)):rows.append(dict(event="consumer_witness_started",node=name,pid=pid,lease=peer_leases[name],observed_ms=2001,files=follower["files"]))
            rows.append(dict(event="consumer_successor_started",pid=successor))
            if cold:
                for name,pid in (("destination",4),("producer",5)):
                    fresh=copy.deepcopy(peer_leases[name]);fresh["generation"]="1"*64
                    rows.append(dict(event="consumer_witness_ready",node=name,pid=pid,lease=fresh))
                recovering=copy.deepcopy(lease);recovering["log"]["state"]="recovering"
                rows.extend([dict(event="consumer_witness_refused",status=503,body='{"error":"Unavailable: event RPC did not return a proven result"}',elapsed_ms=20,owner=dict(node="consumer",epoch=1),lease=recovering,objects={},gates={name:[dict(leader="consumer/"+"c"*32+"/"+"c"*64,epoch=1,request_sha256="1"*64)] for name in peer_leases}),dict(event="consumer_witness_returned",pid=successor,generation="f"*64)])
        elif label=="recovered-before-replay":
            rows.append(dict(event="consumer_inbox_verified",result=dict(accepted=[False,False],projection=dict(orders=2,total_cents=1500),records=[snapshot["effects"]["receipts"][key] for key in ("a","b")]),subscription=snapshot["subscription"]))
            rows.append(dict(event="consumer_fetch_replayed",result=dict(status="fetched",fresh_effects=0,projection=dict(orders=2,total_cents=1500),batch=batch),subscription=snapshot["subscription"]))
        elif label=="after-replay":rows.append(dict(event="consumer_receipt_acknowledged",result=dict(status="Acknowledged",projection=dict(orders=2,total_cents=1500))))
    for name,pid in (("destination",4 if cold else 1),("producer",5 if cold else 2),("successor",successor)):
        rows.append(dict(event="fleet_process_stopped",node=name,pid=pid,exit_code=0,reports=[dict(ingress_drained=True,application_work_drained=True,cells_drained=True)]))
    if cold:
        for name in peer_leases:rows.append(dict(event="consumer_witness_gate_joined",node=name))
    rows.append(dict(event="consumer_recovered_image",node="successor",image=old_image))
    return json.loads(json.dumps(rows))


class ConsumerRecoveryTests(unittest.TestCase):
    def audit(self,rows,changed_image=False,changed_witness=False):
        expected=fixture(any(v["event"]=="consumer_followers_retained" for v in rows))
        images={"lost-consumer-image":row(expected,"consumer_proof_image")["image"],"recovered-consumer-image":row(expected,"consumer_recovered_image")["image"]}
        if changed_image:images["lost-consumer-image"]["files"]["db.sqlite"]="0"*64
        witness=copy.deepcopy(row(fixture(True),"consumer_followers_retained")["followers"]["destination"])
        if changed_witness:witness["files"][0]["sha256"]="0"*64
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/"ledger.jsonl";path.write_text("".join(json.dumps(v)+"\n" for v in rows))
            with patch("consumer_recovery_audit.inspect_consumer",side_effect=lambda archive,scope:images[archive.name]),patch("consumer_recovery_audit.inspect_follower_disk",return_value=witness):return audit_evidence(path)

    def test_warm_and_cold_retained_proof(self):
        for cold in (False,True):self.assertEqual(self.audit(fixture(cold))["consumer_fleet_recoveries"],1)

    def test_missing_boundaries_and_early_replay_are_rejected(self):
        for event in ("consumer_effect_proven","consumer_proof_image","consumer_data_discarded","consumer_authority_expired","consumer_successor_started","consumer_inbox_verified","consumer_fetch_replayed","consumer_receipt_acknowledged","consumer_recovered_image","fleet_process_stopped"):
            with self.subTest(event=event),self.assertRaises(AuditError):self.audit([v for v in fixture() if v["event"]!=event])
        rows=fixture();value=row(rows,"consumer_fetch_replayed");rows.remove(value);rows.insert(10,value)
        with self.assertRaises(AuditError):self.audit(rows)

    def test_bucket_repair_missing_upload_refusal_or_wrong_proof_fails(self):
        for kind in ("bucket","refused","fleet","ensemble","generation","lineage"):
            rows=fixture();proof=row(rows,"consumer_effect_proven")
            if kind=="bucket":proof["objects"]={row(rows,"consumer_proof_armed")["scope"].join(["cells/","/ltx/data"]):dict(bytes=1,sha256="a"*64)}
            elif kind=="refused":proof["refused"]=[]
            elif kind=="fleet":proof["lease"]["log"]["may_have_fleet_acks"]=False
            elif kind=="ensemble":proof["lease"]["log"]["ensemble"]=[]
            elif kind=="generation":proof["lease"]["generation"]="1"*64
            else:proof["lease"]["lineage"]="1"*32
            with self.subTest(kind=kind),self.assertRaises(AuditError):self.audit(rows)

    def test_missing_inbox_effect_or_saved_receipt_cannot_be_replayed_away(self):
        for kind in ("inbox","digest","effect","saved","fresh","source"):
            rows=fixture()
            if kind in ("inbox","digest"):
                image=row(rows,"consumer_recovered_image")["image"]
                if kind=="inbox":image["inbox"].pop(0)
                else:image["inbox"][0]["digest"]="0"*64
            elif kind in ("effect","saved"):
                state=row(rows,"consumer_recovery_audit",label="recovered-before-replay")["snapshot"]["effects"]
                if kind=="effect":state["projection"]["orders"]=1
                else:state["ack_receipt_sha256"]="0"*64
            elif kind=="fresh":row(rows,"consumer_fetch_replayed")["result"]["fresh_effects"]=1
            else:rows.insert(18,dict(event="attempt",key="b",attempt=2,replay=True))
            with self.subTest(kind=kind),self.assertRaises(AuditError):self.audit(rows)
        with self.assertRaises(AuditError):self.audit(fixture(),changed_image=True)

    def test_correct_effect_totals_cannot_hide_a_missing_inbox_before_replay(self):
        for index in (0,1):
            for value in (True,0):
                rows=fixture();row(rows,"consumer_inbox_verified")["result"]["accepted"][index]=value
                with self.assertRaises(AuditError):self.audit(rows)

    def test_cold_witness_custody_fresh_generation_and_archive_are_required(self):
        for kind in ("member","epoch","files","pid","generation","lineage","peer_proof","ready","gate_join"):
            rows=fixture(True)
            if kind in ("member","epoch","peer_proof"):
                retained=row(rows,"consumer_followers_retained")
                if kind=="member":retained["followers"].pop("producer")
                elif kind=="epoch":retained["followers"]["destination"]["batches"][0]["entries"][0]["cell_epoch"]=2
                else:retained["leases"]["destination"]["log"]["may_have_fleet_acks"]=True
            elif kind=="files":row(rows,"consumer_witness_started",node="destination")["files"]=[]
            elif kind=="pid":row(rows,"consumer_witness_started",node="destination")["pid"]=1
            elif kind in ("generation","lineage"):row(rows,"consumer_witness_ready",node="destination")["lease"][kind]=("d"*64 if kind=="generation" else "0"*32)
            elif kind=="ready":rows=[v for v in rows if v["event"]!="consumer_witness_ready"]
            else:rows=[v for v in rows if v["event"]!="consumer_witness_gate_joined"]
            with self.subTest(kind=kind),self.assertRaises(AuditError):self.audit(rows)
        with self.assertRaises(AuditError):self.audit(fixture(True),changed_witness=True)

    def test_unavailable_witness_cannot_seal_reset_or_time_out(self):
        for kind in ("status","timeout","sealed","owner","fleet","gates","session","pid","generation"):
            rows=fixture(True);refusal=row(rows,"consumer_witness_refused")
            if kind=="status":refusal["status"]=500
            elif kind=="timeout":refusal["elapsed_ms"]=10000
            elif kind=="sealed":refusal["lease"]["log"]["state"]="sealed"
            elif kind=="owner":refusal["owner"]["epoch"]=2
            elif kind=="fleet":refusal["lease"]["log"]["may_have_fleet_acks"]=False
            elif kind=="gates":refusal["gates"]["producer"]=[]
            elif kind=="session":refusal["gates"]["producer"][0]["leader"]="other"
            elif kind=="pid":row(rows,"consumer_witness_returned")["pid"]=3
            else:row(rows,"consumer_recovery_audit",label="recovered-before-replay")["recoverer"]["lease"]["generation"]="a"*64
            with self.subTest(kind=kind),self.assertRaises(AuditError):self.audit(rows)

    def test_expired_fetch_requires_fresh_authority_and_stable_effects(self):
        rows=fixture();replayed=row(rows,"consumer_fetch_replayed");batch=replayed["result"]["batch"]
        encoded=batch["receipt"][3:];receipt=json.loads(base64.urlsafe_b64decode(encoded+"="*(-len(encoded)%4)))
        receipt["generation"]="2";receipt["token"]="b"*64
        batch.update(issued_ms="31000",expires_ms="61000",receipt="v1."+base64.urlsafe_b64encode(json.dumps(receipt).encode()).decode().rstrip("="))
        replayed["subscription"]["lease"].update(generation="2",expires_ms="61000")
        sha=hashlib.sha256(batch["receipt"].encode()).hexdigest()
        for label in ("after-replay","final"):row(rows,"consumer_recovery_audit",label=label)["snapshot"]["effects"]["ack_receipt_sha256"]=sha
        row(rows,"consumer_recovered_image")["image"]["ack_receipt_sha256"]=sha
        # The archive oracle must return the final new receipt too.
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/"ledger.jsonl";path.write_text("".join(json.dumps(v)+"\n" for v in rows))
            with patch("consumer_recovery_audit.inspect_consumer",side_effect=lambda archive,scope:row(rows,"consumer_proof_image" if archive.name=="lost-consumer-image" else "consumer_recovered_image")["image"]):self.assertEqual(audit_evidence(path)["consumer_fleet_recoveries"],1)
        bad=copy.deepcopy(rows);row(bad,"consumer_fetch_replayed")["result"]["batch"]["issued_ms"]="2000"
        with self.assertRaises(AuditError):self.audit(bad)


if __name__=="__main__":unittest.main()
