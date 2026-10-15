"""Cold destination witnesses must survive memory loss without replay repair."""
import copy
import json
import hashlib
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from faults import AuditError
import ack_faults
import fetch_faults
import fleet_faults
from test_destination_proof import fixture as append_fixture
from test_ack_faults import fixture as ack_fixture
from test_fetch_faults import fixture as fetch_fixture, expired_fixture


def fixture(operation="ack", expired=False):
    if operation == "append":
        rows = [row for row in append_fixture() if row["event"] not in
                ("destination_proof_armed","destination_follower_accepted","destination_proof_discarded","destination_follower_recovered")]
    else:
        rows = ack_fixture() if operation == "ack" else expired_fixture() if expired else fetch_fixture()
    scope = next(row for row in rows if row["event"] == operation + "_reply_held")["exchange"]["scope"]
    owner = dict(node="destination", epoch=1)
    lease = dict(node="destination", generation="a"*64, expires_ms=1000,
                 log=dict(epoch=1, state="open", ensemble=["producer"], may_have_fleet_acks=True))
    peers = {node: dict(node=node, generation=node+"-old", expires_ms=1000,
                       log=dict(may_have_fleet_acks=False)) for node in ("producer", "consumer")}
    files = [dict(path="4-4.batch", bytes=100, sha256="b"*64), dict(path="state.json", bytes=50, sha256="c"*64)]
    disk = dict(files=files, seed=dict(epoch=0), batches=[dict(epoch=1, entries=[dict(cell=scope, cell_epoch=1)])])
    arm = dict(event="destination_proof_armed", scope=scope, owner=owner, lease=lease, objects={},
               proof_mode="fleet", peer_proof="bucket", replica_fault_node="destination",
               node_lease_ms=30000 if expired else 1000, restart_followers=True)
    index = next(i for i, row in enumerate(rows) if row["event"] == ("attempt" if operation=="append" else operation + "_reply_held"))
    rows.insert(index, arm)
    index = next(i for i, row in enumerate(rows) if row["event"] == "process_killed")
    rows.insert(index, dict(event="destination_follower_accepted", scope=scope, owner=owner, lease=lease, objects={},
                            refused=[dict(path=f"cells/{scope}/ltx/e1/2.ltx")]))
    index = next(i for i, row in enumerate(rows) if row["event"] == ("destination_restarted" if operation=="append" else operation + "_destination_restarted"))
    rows[index:index] = [dict(event="destination_follower_process_killed",node=node,pid=pid,exit_code=-9,graceful_reports=0)
                        for node,pid in (("producer",2),("consumer",3))] + [
        dict(event="destination_followers_retained",followers=dict(producer=disk),leases=peers,objects={}),
        dict(event="destination_proof_discarded",files=[dict(path="node/data",bytes=100,sha256="d"*64)],
             remaining_entries=0,lease=lease,observed_ms=1001,objects={})] + [
        dict(event="destination_follower_process_started",node=node,pid=pid,lease=peers[node],observed_ms=1001,
             files=files if node=="producer" else []) for node,pid in (("producer",5),("consumer",6))]
    for row in rows:
        if operation=="append" and row["event"]=="destination_follower_process_started":
            config=json.dumps(dict(node=row["node"],blocked_claim=scope)).encode()
            row["claim_gate"]=dict(scope=scope,marker_sha256=hashlib.sha256(b"").hexdigest(),configuration_sha256=hashlib.sha256(config).hexdigest())
    recovered_label="recovered-before-retry" if operation=="append" else "recovered-before-replay"
    index = next(i for i,row in enumerate(rows) if row["event"]==("fleet" if operation=="append" else operation)+"_audit" and row["label"]==recovered_label)
    rows[index:index] = [dict(event="destination_follower_process_ready",node=node,pid=pid,
                             lease=dict(node=node,generation=node+"-new")) for node,pid in (("producer",5),("consumer",6))]
    if operation=="append":
        index=next(i for i,row in enumerate(rows) if row["event"]=="fleet_audit" and row["label"]==recovered_label)
        held=next(row["exchange"] for row in rows if row["event"]=="append_reply_held")
        rows[index:index]=[dict(event="retry_blocked",request_sha256=held["request_sha256"]),
                           dict(event="destination_append_gate",scope=scope,owner=dict(node="destination",epoch=2),
                                refused=dict(producer=[dict(operation="put",path=f"cells/{scope}/own.json")],consumer=[]))]
        lost=next(row for row in rows if row["event"]=="append_reply_lost");rows.remove(lost)
    index = next(i for i,row in enumerate(rows) if row["event"]=="retry_resumed")
    rows.insert(index,dict(event="destination_follower_recovered",owner=dict(node="destination",epoch=2),
                           lease=dict(node="destination",generation="b"*64)))
    if operation=="append":rows.insert(index+1,lost)
    for row in rows:
        if row["event"]=="fleet_process_stopped" and row["node"]!="destination":row["pid"]={"producer":5,"consumer":6}[row["node"]]
    return json.loads(json.dumps(rows))


class DestinationDiskTests(unittest.TestCase):
    def audit(self, rows, operation="ack", changed=False, changed_gate=False):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/"ledger.jsonl"
            path.write_text("".join(json.dumps(row)+"\n" for row in rows))
            disk=copy.deepcopy(next(row["followers"]["producer"] for row in fixture(operation) if row["event"]=="destination_followers_retained"))
            if changed:disk["files"][0]["sha256"]="f"*64
            if operation=="append":
                scope=next(row["scope"] for row in fixture("append") if row["event"]=="destination_proof_armed")
                for node in ("producer","consumer"):
                    (Path(directory)/(node+"-cold-config.json")).write_text(json.dumps(dict(node=node,blocked_claim=scope)))
                    data=Path(directory)/"data"/node;data.mkdir(parents=True)
                    (data/"claim-block").write_bytes(b"changed" if changed_gate else b"")
            with patch("destination_disks.inspect_follower_disk",return_value=disk):
                return {"ack":ack_faults,"fetch":fetch_faults,"append":fleet_faults}[operation].audit_evidence(path)

    def test_ack_and_live_or_expired_fetch_require_cold_witnesses(self):
        for operation,expired in (("ack",False),("fetch",False),("fetch",True),("append",False)):
            with self.subTest(operation=operation,expired=expired):
                self.assertEqual(self.audit(fixture(operation,expired),operation)["restarted_destination_followers"],2)

    def test_missing_or_reordered_custody_is_rejected(self):
        for event in ("destination_follower_process_killed","destination_followers_retained","destination_follower_process_started","destination_follower_process_ready"):
            rows=[row for row in fixture() if row["event"]!=event]
            with self.subTest(event=event),self.assertRaises(AuditError):self.audit(rows)
        rows=fixture();row=next(row for row in rows if row["event"]=="destination_followers_retained");rows.remove(row);rows.insert(1,row)
        with self.assertRaises(AuditError):self.audit(rows)
        rows=fixture();row=next(row for row in rows if row["event"]=="destination_follower_process_ready");rows.remove(row)
        index=next(i for i,row in enumerate(rows) if row["event"]=="destination_follower_recovered");rows.insert(index,row)
        with self.assertRaisesRegex(AuditError,"snapshot preceded"):self.audit(rows)

    def test_fresh_process_expired_authority_and_exact_disk_are_required(self):
        for kind in ("pid","lease","files","generation","graceful","exit","destination_pid"):
            rows=fixture()
            started=next(row for row in rows if row["event"]=="destination_follower_process_started")
            ready=next(row for row in rows if row["event"]=="destination_follower_process_ready")
            killed=next(row for row in rows if row["event"]=="destination_follower_process_killed")
            if kind=="pid":started["pid"]=2
            elif kind=="lease":started["observed_ms"]=1000
            elif kind=="files":started["files"]=[]
            elif kind=="generation":ready["lease"]["generation"]=started["lease"]["generation"]
            elif kind=="graceful":killed["graceful_reports"]=1
            elif kind=="exit":killed["exit_code"]=0
            else:next(row for row in rows if row["event"]=="ack_destination_restarted")["pid"]=5
            with self.subTest(kind=kind),self.assertRaises(AuditError):self.audit(rows)

    def test_missing_witness_wrong_epoch_or_bucket_repair_is_rejected(self):
        for kind in ("ensemble","scope","epoch","bucket","peer_proof"):
            rows=fixture();retained=next(row for row in rows if row["event"]=="destination_followers_retained")
            if kind=="ensemble":retained["followers"]={}
            elif kind in ("scope","epoch"):
                entry=retained["followers"]["producer"]["batches"][0]["entries"][0]
                entry["cell" if kind=="scope" else "cell_epoch"]="wrong" if kind=="scope" else 2
            elif kind=="peer_proof":retained["leases"]["producer"]["log"]["may_have_fleet_acks"]=True
            else:retained["objects"]["log/destination/"+"a"*64+"/bundle/e1-0.ltxb"]=dict(bytes=100,sha256="e"*64)
            with self.subTest(kind=kind),self.assertRaises(AuditError):self.audit(rows)

    def test_archive_mutation_and_replay_before_recovery_are_rejected(self):
        with self.assertRaisesRegex(AuditError,"archived destination follower"):self.audit(fixture(),changed=True)
        rows=fixture();row=next(row for row in rows if row["event"]=="retry_resumed");rows.remove(row);rows.insert(8,row)
        with self.assertRaises(AuditError):self.audit(rows)

    def test_existing_live_follower_cases_stay_separate(self):
        self.assertEqual(ack_faults.audit_evidence(self.saved(ack_fixture()))["restarted_destination_followers"],0)

    def test_cold_mode_requires_follower_only_before_startup(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)/"unused"
            for module in (ack_faults,fetch_faults,fleet_faults):
                with self.subTest(module=module.__name__),self.assertRaisesRegex(AuditError,"require follower-only"):
                    module.exercise(Path("unused-binary"),root,restart_followers=True)
                self.assertFalse(root.exists())

    def test_cold_append_requires_a_real_blocked_retry_and_separate_owner(self):
        for kind in ("gate","blocked","scope","owner","claim_scope","marker","refused"):
            rows=fixture("append")
            gate=next(row for row in rows if row["event"]=="destination_append_gate")
            if kind=="gate":rows.remove(gate)
            elif kind=="blocked":rows=[row for row in rows if row["event"]!="retry_blocked"]
            elif kind=="scope":gate["scope"]="wrong"
            elif kind=="owner":gate["owner"]["node"]="producer"
            elif kind=="refused":gate["refused"]=dict(producer=[],consumer=[])
            else:
                started=next(row for row in rows if row["event"]=="destination_follower_process_started")
                started["claim_gate"]["scope" if kind=="claim_scope" else "marker_sha256"]="wrong"
            with self.subTest(kind=kind),self.assertRaises(AuditError):self.audit(rows,"append")
        with self.assertRaisesRegex(AuditError,"claim gate custody changed"):
            self.audit(fixture("append"),"append",changed_gate=True)

    def test_cold_source_can_move_between_returning_peers(self):
        rows=fixture("append")
        for row in rows:
            if row["event"]=="fleet_audit" and row["label"]!="before-destination-loss":
                row["snapshot"]["sources"]["cross"]["host"]="consumer"
        self.assertEqual(self.audit(rows,"append")["cold_append_gates"],1)
        next(row for row in rows if row["event"]=="fleet_audit" and row["label"]=="recovered-before-retry")["snapshot"]["sources"]["cross"]["host"]="destination"
        with self.assertRaises(AuditError):self.audit(rows,"append")

    def saved(self,rows):
        directory=tempfile.TemporaryDirectory();self.addCleanup(directory.cleanup)
        path=Path(directory.name)/"ledger.jsonl";path.write_text("".join(json.dumps(row)+"\n" for row in rows));return path
