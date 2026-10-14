"""Independent source recovery cannot be repaired by replay or lose one lane."""
import copy
import json
from pathlib import Path
import tempfile
import unittest

from faults import AuditError, digest
from many_sources_audit import audit
from many_sources import cleanup


def fixture():
    operations={};records={}
    for index,key in enumerate(("a","b"),1):
        payload=dict(order_id=key,amount_cents=index)
        receipt=dict(source="Order:"+str(index)*64,incarnation=str(index)*32,lane=str(index+2)*64,sequence="1",payload_sha256=digest(payload))
        operations[key]=dict(payload=payload,sha256=digest(payload),offset=0,attempts=0)
        records[key]=dict(receipt,order_id=key,offset=str(index),event_id=f"v1:{receipt['lane']}:1",envelope_sha256="d"*64,event=payload)
    rows=[dict(event="plan",operations=copy.deepcopy(operations)),dict(event="many_plan",count=2,stream_rows=1,soak_seconds=0,consumer_delay=.1,resident_limit=128,cache_limit=256),
        dict(event="many_started",pids=dict(producer=1,consumer=2,destination=3))]
    def observation(label):
        rows.append(dict(event="capacity_observation",label=label,nodes={node:dict(pid=pid+3 if label in ("cold-before-retry","drained") else pid,
            rss_bytes=4096,state=dict(occupied=1,cached_routes=1),files=1,logical_file_bytes=1024,family_bytes=dict(sqlite=1024,wal=0,ltx=0,other=0))
            for node,pid in (("producer",1),("consumer",2),("destination",3))}))
    observation("empty")
    for key,operation in operations.items():
        operation["attempts"]=1;operation["ack"]={field:records[key][field] for field in ("source","incarnation","lane","sequence","payload_sha256")}
        rows.extend([dict(event="attempt",key=key,replay=False,attempt=1),dict(event="ack",key=key,receipt=operation["ack"])])
    def sources(label,pending):
        states={}
        for key,operation in operations.items():
            current_pending=0 if key=="a" else pending
            receipt=operation["ack"];revision={field:receipt[field] for field in ("source","incarnation","lane")}
            revision.update(next_sequence="2",settled_through=str(1-current_pending))
            states[key]=dict(host="producer",order=operation["payload"],receipt=receipt,outbox=dict(incarnation=receipt["incarnation"],pending_rows=str(current_pending),lanes=[dict(revision=revision,pending_rows=str(current_pending))]))
        rows.extend([dict(event="many_source_audit",label=label,sources=states),dict(event="many_source_audit_passed",label=label)])
    rows.append(dict(event="many_acceptance",key="a",record=records["a"]))
    sources("before-source-loss",1)
    for node,pid in (("producer",1),("consumer",2),("destination",3)):
        rows.append(dict(event="process_killed",label="many-cold-"+node,pid=pid,exit_code=-9))
    for node,pid in (("producer",4),("consumer",5),("destination",6)):
        rows.append(dict(event="many_restarted",node=node,pid=pid))
    observation("cold-before-retry")
    rows.extend([dict(event="many_cold_boundary",source_requests=0,events=[records["a"]]),dict(event="retry_resumed",label="cold-before-retry")])
    effects=dict(host="consumer",receipts={},projection=dict(orders=0,total_cents=0))
    for index,(key,record) in enumerate(records.items(),1):
        if key!="a":rows.append(dict(event="many_acceptance",key=key,record=record))
        rows.append(dict(event="many_effects_before",checkpoint=index-1,effects=copy.deepcopy(effects)))
        effects["receipts"][key]={field:value for field,value in record.items() if field!="event"}
        effects["projection"]=dict(orders=index,total_cents=sum(operations[k]["payload"]["amount_cents"] for k in effects["receipts"]))
        rows.extend([dict(event="many_effect_proven",checkpoint=index,effects=copy.deepcopy(effects)),
            dict(event="many_checkpoint",checkpoint=index,subscription=dict(checkpoint=str(index),observation=dict(lease_active=False)))])
    rows.append(dict(event="many_drain_finished",seconds=1))
    sources("final",0);rows.append(dict(event="many_final_effects",effects=effects));observation("drained")
    for node,pid in (("producer",4),("consumer",5),("destination",6)):
        rows.append(dict(event="fleet_process_stopped",node=node,pid=pid,exit_code=0,reports=[dict(ingress_drained=True,application_work_drained=True,cells_drained=True)]))
    return copy.deepcopy(rows)


class ManySourceOracleTests(unittest.TestCase):
    def audit(self,rows):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/"ledger.jsonl";path.write_text("".join(json.dumps(row)+"\n" for row in rows));return audit(path)

    def test_complete_independent_sources(self):self.assertEqual(self.audit(fixture())["sources"],2)

    def test_replay_or_source_request_cannot_repair_cold_recovery(self):
        rows=fixture();next(row for row in rows if row["event"]=="attempt")["replay"]=True
        with self.assertRaisesRegex(AuditError,"source replay"):self.audit(rows)
        rows=fixture();next(row for row in rows if row["event"]=="many_cold_boundary")["source_requests"]=1
        with self.assertRaisesRegex(AuditError,"repaired"):self.audit(rows)

    def test_missing_source_obligation_or_duplicate_acceptance_is_rejected(self):
        rows=fixture();next(row for row in rows if row["event"]=="many_source_audit")["sources"].pop("b")
        with self.assertRaisesRegex(AuditError,"inventory"):self.audit(rows)
        rows=fixture();record=next(row for row in rows if row["event"]=="many_acceptance");rows.insert(rows.index(record)+1,copy.deepcopy(record))
        with self.assertRaisesRegex(AuditError,"duplicate"):self.audit(rows)

    def test_lost_effect_or_checkpoint_skip_is_rejected(self):
        rows=fixture();next(row for row in rows if row["event"]=="many_effect_proven")["effects"]["receipts"].pop("a")
        with self.assertRaises(AuditError):self.audit(rows)
        rows=fixture();next(row for row in rows if row["event"]=="many_checkpoint")["subscription"]["checkpoint"]="3"
        with self.assertRaisesRegex(AuditError,"skipped"):self.audit(rows)

    def test_foreign_process_and_missing_shutdown_are_rejected(self):
        rows=fixture();next(row for row in rows if row["event"]=="process_killed")["pid"]=9
        with self.assertRaisesRegex(AuditError,"foreign"):self.audit(rows)
        with self.assertRaisesRegex(AuditError,"incomplete"):self.audit(fixture()[:-1])

    def test_resource_limit_is_enforced(self):
        rows=fixture();next(row for row in rows if row["event"]=="capacity_observation")["nodes"]["producer"]["state"]["occupied"]=129
        with self.assertRaisesRegex(AuditError,"resource"):self.audit(rows)

    def test_owner_movement_with_original_identity_is_valid(self):
        rows=fixture()
        for row in rows:
            if row["event"]=="many_source_audit":row["sources"]["b"]["host"]="destination"
        self.assertEqual(self.audit(rows)["sigkills"],3)

    def test_recovered_acceptance_cannot_change_or_disappear(self):
        rows=fixture();next(row for row in rows if row["event"]=="many_cold_boundary")["events"]=[]
        with self.assertRaises(AuditError):self.audit(rows)
        rows=fixture();next(row for row in rows if row["event"]=="many_cold_boundary")["events"][0]["envelope_sha256"]="e"*64
        with self.assertRaisesRegex(AuditError,"immutable envelope"):self.audit(rows)

    def test_all_replacements_must_start_after_all_observed_kills(self):
        rows=fixture();row=next(row for row in rows if row["event"]=="many_restarted");rows.remove(row)
        rows.insert(next(index for index,row in enumerate(rows) if row["event"]=="process_killed"),row)
        with self.assertRaisesRegex(AuditError,"replacement"):self.audit(rows)

    def test_failed_drain_still_closes_all_owned_resources(self):
        calls=[]
        class Resource:
            def __init__(self,name,failed=False):self.name=name;self.failed=failed
            def stop(self):
                calls.append(self.name)
                if self.failed:raise AuditError("failed drain")
            def close(self):calls.append(self.name)
        class Release:
            def set(self):calls.append("release")
        proxy=Resource("proxy");proxy.release=Release()
        with self.assertRaisesRegex(AuditError,"failed drain"):
            cleanup([Resource("first"),Resource("last",True)],proxy,Resource("store"),Resource("ledger"))
        self.assertEqual(calls,["release","last","first","proxy","store","ledger"])
