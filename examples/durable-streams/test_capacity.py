# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Capacity recovery evidence must retain the original commands and effects."""
import copy
import json
import socket
import threading
from pathlib import Path
import tempfile
import unittest

from capacity_audit import audit
from faults import AuditError, digest
from fleet_faults import PeerProxy, message


def fixture():
    operations={};records={}
    for index,key in enumerate(("a","b"),1):
        payload=dict(order_id=key,amount_cents=index)
        receipt=dict(source="Backlog:"+"a"*64,incarnation="b"*32,lane="c"*64,sequence=str(index),payload_sha256=digest(payload))
        operations[key]=dict(payload=payload,sha256=digest(payload),offset=index,attempts=0)
        records[key]=dict(receipt,order_id=key,offset=str(index),event_id=f"v1:{'c'*64}:{index}",envelope_sha256="d"*64,event=payload)
    rows=[dict(event="plan",operations=copy.deepcopy(operations)),dict(event="capacity_plan",count=2,outbox_rows=2,stream_rows=1,soak_seconds=0,consumer_delay=.1),
        dict(event="capacity_started",pids=dict(producer=1,consumer=2,destination=3))]
    def observation(label):
        rows.append(dict(event="capacity_observation",label=label,nodes={node:dict(pid=4 if node=="producer" and label in ("recovered-full","drained") else pid,
            rss_bytes=4096,state=dict(occupied=1,cached_routes=1),files=1,logical_file_bytes=1024,allocated_file_bytes=4096,family_bytes=dict(sqlite=1024,wal=0,ltx=0,other=0))
            for node,pid in (("producer",1),("consumer",2),("destination",3))}))
    observation("empty")
    for key,operation in operations.items():
        operation["attempts"]=1;operation["ack"]={field:records[key][field] for field in ("source","incarnation","lane","sequence","payload_sha256")}
        rows.extend([dict(event="attempt",key=key,replay=False,attempt=1),dict(event="ack",key=key,receipt=operation["ack"])])
    rows.append(dict(event="capacity_rejected",command="overflow",error="OutboxFull"))
    def source(label,settled=0):
        receipt=operations["a"]["ack"]
        revision={field:receipt[field] for field in ("source","incarnation","lane")};revision.update(next_sequence="3",settled_through=str(settled))
        rows.extend([dict(event="capacity_source_audit",label=label,operations=copy.deepcopy(operations),
            state=dict(host="producer",commands=2,outbox=dict(incarnation=receipt["incarnation"],pending_rows=str(2-settled),lanes=[dict(revision=revision,pending_rows=str(2-settled))])),
            commands={key:dict(host="producer",order=operation["payload"],receipt=operation["ack"]) for key,operation in operations.items()},
            rejected=dict(order=None,receipt=None,commands=2),events=[records["b"]] if settled else []),dict(event="capacity_source_audit_passed",label=label)])
    source("full-before-soak");source("before-source-loss")
    rows.extend([dict(event="process_killed",label="capacity-producer-pending",pid=1,exit_code=-9),dict(event="capacity_producer_restarted",pid=4)])
    source("recovered-before-retry");observation("recovered-full")
    rows.append(dict(event="retry_resumed"))
    effects={}
    def snapshot():return dict(host="consumer",receipts=copy.deepcopy(effects),projection=dict(orders=len(effects),total_cents=sum(operations[key]["payload"]["amount_cents"] for key in effects)))
    for index,key in enumerate(operations,1):
        operations[key]["accepted"]={field:value for field,value in records[key].items() if field!="event"}
        rows.extend([dict(event="capacity_acceptance",key=key,record=records[key]),dict(event="capacity_effects_before",key=key,effects=snapshot())])
        effects[key]=operations[key]["accepted"]
        rows.extend([dict(event="capacity_effect_proven",key=key,effects=snapshot()),dict(event="capacity_checkpoint",key=key,checkpoint=index,
            subscription=dict(checkpoint=str(index),observation=dict(lease_active=False)))])
    source("final",2);rows.append(dict(event="capacity_final_effects",effects=snapshot()));observation("drained")
    for node,pid in (("producer",4),("consumer",2),("destination",3)):
        rows.append(dict(event="fleet_process_stopped",node=node,pid=pid,exit_code=0,reports=[dict(ingress_drained=True,application_work_drained=True,cells_drained=True)]))
    return copy.deepcopy(rows)


class CapacityOracleTests(unittest.TestCase):
    def audit(self,rows):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/"ledger.jsonl";path.write_text("".join(json.dumps(row)+"\n" for row in rows))
            return audit(path)

    def test_complete_capacity_evidence(self):
        self.assertEqual(self.audit(fixture())["checkpoint"],2)

    def test_source_replay_before_recovery_is_rejected(self):
        rows=fixture();next(row for row in rows if row["event"]=="attempt")["replay"]=True
        with self.assertRaisesRegex(AuditError,"source replay"):self.audit(rows)
        rows=fixture();row=next(row for row in rows if row["event"]=="retry_resumed");rows.remove(row);rows.insert(3,row)
        with self.assertRaisesRegex(AuditError,"concealed"):self.audit(rows)

    def test_lost_source_state_or_overflow_write_is_rejected(self):
        for field in ("commands","rejected"):
            rows=fixture();row=next(row for row in rows if row["event"]=="capacity_source_audit" and row["label"]=="recovered-before-retry")
            if field=="commands":row[field]["a"]["order"]["amount_cents"]=999
            else:row[field]["order"]={"amount_cents":999}
            with self.assertRaises(AuditError):self.audit(rows)

    def test_duplicate_effect_or_skipped_checkpoint_is_rejected(self):
        for event in ("capacity_effect_proven","capacity_checkpoint"):
            rows=fixture();row=next(row for row in rows if row["event"]==event)
            if event=="capacity_effect_proven":row["effects"]["projection"]["orders"]=2
            else:row["checkpoint"]=2
            with self.assertRaises(AuditError):self.audit(rows)

    def test_foreign_loss_and_incomplete_shutdown_are_rejected(self):
        rows=fixture();next(row for row in rows if row["event"]=="process_killed")["pid"]=99
        with self.assertRaises(AuditError):self.audit(rows)
        with self.assertRaisesRegex(AuditError,"incomplete"):self.audit(fixture()[:-1])

    def test_residency_limit_and_memory_observation_are_required(self):
        for field,value in (("rss_bytes",0),("state",dict(occupied=129,cached_routes=1))):
            rows=fixture();next(row for row in rows if row["event"]=="capacity_observation")["nodes"]["producer"][field]=value
            with self.assertRaisesRegex(AuditError,"invalid memory"):self.audit(rows)

    def test_store_removal_must_follow_complete_shutdown(self):
        rows=fixture();rows.insert(1,dict(event="network_store_started",backend="s3",container="cellar-stream-store-private",image_id="sha256:"+"e"*64))
        rows.append(dict(event="network_store_removed",container="cellar-stream-store-private"))
        self.assertEqual(self.audit(rows)["backend"],"s3")
        with self.assertRaisesRegex(AuditError,"incomplete"):self.audit(rows[:-1])
        row=rows.pop();rows.insert(3,row)
        with self.assertRaisesRegex(AuditError,"before complete"):self.audit(rows)


class CapacityProxyTests(unittest.TestCase):
    def test_unarmed_capacity_error_is_forwarded_unchanged(self):
        class RecordingLedger:
            def __init__(self):self.rows=[]
            def record(self,event,**value):self.rows.append(dict(event=event,**value))
        ledger=RecordingLedger()
        listener=socket.socket();listener.bind(("127.0.0.1",0));listener.listen(1);listener.settimeout(5)
        body=b'{"error":"StreamFull","batch_records":"1"}'
        wire=b"HTTP/1.1 409 Conflict\r\nContent-Length: "+str(len(body)).encode()+b"\r\n\r\n"+body
        failures=[]
        def serve():
            try:
                with listener.accept()[0] as connection:
                    message(connection.makefile("rb"));connection.sendall(wire)
            except Exception as error:failures.append(error)
        worker=threading.Thread(target=serve);worker.start()
        proxy=PeerProxy(listener.getsockname(),ledger)
        try:
            with socket.create_connection(proxy.address,timeout=5) as client:
                client.sendall(b"POST /rpc HTTP/1.1\r\nx-cells-rpc-method: __streamAppend\r\nContent-Length: 0\r\n\r\n")
                reply=message(client.makefile("rb"),response=True)
                self.assertEqual(reply["wire"],wire)
            worker.join(timeout=5)
            self.assertFalse(worker.is_alive());self.assertEqual(failures,[])
        finally:
            listener.close();proxy.close()
        self.assertEqual(ledger.rows,[])
