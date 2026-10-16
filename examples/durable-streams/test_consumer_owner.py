"""Lost receipts, stale commits and replay cannot satisfy live-consumer evidence."""
import base64
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from consumer_owner_audit import FETCH_ID, audit_evidence
from faults import AuditError
import test_faults


def fixture(cut="before_accept"):
    operations, snapshot = test_faults.fixture()
    operations["b"]["ack"] = operations["b"].pop("upstream_ack")
    for state in snapshot["sources"].values():
        state["host"] = "producer"
        state["outbox"]["pending_rows"] = "0"
        state["outbox"]["lanes"][0].update(pending_rows="0")
        state["outbox"]["lanes"][0]["revision"]["settled_through"] = "1"
    first = snapshot["events"]["records"][0]; first.pop("event")
    second = dict(operations["b"]["ack"], order_id="b",event_id="v1:"+"b"*64+":1",offset="2",envelope_sha256="2"*64)
    snapshot["events"]["records"] = [dict(second,event=operations["b"]["payload"])]
    scope = "Projection:" + "c"*64
    projection = dict(orders=1,total_cents=1200)
    snapshot["effects"] = dict(projection=projection,receipts={"a":first},host="consumer",scope=scope)
    identity = dict(resource="0"*31+"1",name="projection",id="0"*31+"2",generation="0"*31+"3")
    lease = dict(generation="1",first="2",last="2",expires_ms="31000")
    receipt = dict(resource=identity["resource"],partition=0,name="projection",subscription=identity["id"],incarnation=identity["generation"],
                   generation="1",token="a"*64,first="2",last="2")
    batch = dict(receipt="v1."+base64.urlsafe_b64encode(json.dumps(receipt).encode()).decode().rstrip("="),issued_ms="1000",expires_ms="31000",records=[second])
    snapshot["subscription"] = dict(identity=identity,checkpoint="1",lease=None,earliest="2",end="3",observation=dict(lease_active=False,observed_ms="999"))
    entered = dict(boundary="consumer_poll_entered",scope=scope,cut=cut,pid=3,delivery=dict(batch,fetch_id=FETCH_ID),accepted=True if cut=="before_commit" else None)
    attempted = dict(boundary="consumer_poll_attempted",scope=scope,cut=cut,pid=3,refused=True,closed_domain=True,
                     accepted=True if cut=="before_commit" else None,acceptance_closed_domain=cut=="before_accept")
    plans = copy.deepcopy(operations)
    for op in plans.values(): op.pop("ack");op["attempts"]=0
    image = dict(projection=projection,receipts={"a":first},count=1,inbox=[dict(scope="order-projection-v1",event_id=first["event_id"],digest=first["envelope_sha256"])],files={"db.sqlite":"e"*64})
    rows = [dict(event="plan",operations=plans),dict(event="consumer_owner_started",pids=dict(destination=1,producer=2,consumer=3),cut=cut,
             proof_mode="bucket",node_lease_ms=1000,handler_budget_ms=10000),dict(event="attempt",key="a",attempt=1,replay=False),dict(event="ack",key="a",receipt=operations["a"]["ack"]),
             dict(event="consumer_owner_seeded",result=dict(status="acknowledged",fresh_effects=1,projection=projection)),dict(event="attempt",key="b",attempt=1,replay=False),dict(event="ack",key="b",receipt=operations["b"]["ack"])]
    for label in ("before-isolation","before-release","after-refusal","final"):
        state = copy.deepcopy(snapshot)
        if label != "before-isolation":
            state["effects"]["host"] = "successor"
            state["subscription"].update(lease=lease,observation=dict(lease_active=True,observed_ms="3000"))
        if label == "final":
            state["effects"].update(projection=dict(orders=2,total_cents=1500),receipts={"a":first,"b":second})
            state["subscription"].update(checkpoint="2",lease=None,observation=dict(lease_active=False,observed_ms="4000"))
        rows.append(dict(event="consumer_owner_audit",label=label,owner=dict(node="consumer" if label=="before-isolation" else "successor",epoch=1 if label=="before-isolation" else 2),operations=operations,snapshot=state))
        if label == "before-isolation":
            sub = copy.deepcopy(snapshot["subscription"]);sub.update(lease=lease,observation=dict(lease_active=True,observed_ms="1000"))
            rows.extend([dict(event="consumer_owner_entered",phase=entered,subscription=sub,node_lease=dict(node="consumer",generation="old-node",expires_ms=2000)),
                dict(event="consumer_owner_isolated",pid=3,state="S"),dict(event="consumer_owner_expired",lease=dict(node="consumer",generation="old-node",expires_ms=2000),observed_ms=2001,refused=[dict(path="nodes/consumer.json",operation="get")]),
                dict(event="consumer_owner_successor",pid=4)])
        elif label == "before-release":
            rows.extend([dict(event="consumer_owner_released",pid=3,state="S",phase=entered,elapsed_ms=2000),dict(event="consumer_owner_attempted",phase=attempted),
                dict(event="consumer_owner_fenced",pid=3,exit_code=1,report=dict(outcome="fenced",fence_code=3,reason="node_lease_expired"),graceful_reports=0),
                dict(event="consumer_owner_interrupted",kind="OSError"),dict(event="consumer_owner_image",image=image)])
        elif label == "after-refusal":
            for name, count in (("applied",1),("replayed",0)):
                rows.append(dict(event="consumer_owner_fetch",label=name,result=dict(status="fetched",fresh_effects=count,projection=dict(orders=2,total_cents=1500),batch=batch)))
            rows.append(dict(event="consumer_owner_ack",result=dict(status="Acknowledged",projection=dict(orders=2,total_cents=1500))))
    for name,pid in (("destination",1),("producer",2),("successor",4)):
        rows.append(dict(event="fleet_process_stopped",node=name,pid=pid,exit_code=0,reports=[dict(ingress_drained=True,application_work_drained=True,cells_drained=True)]))
    successor_image = dict(projection=dict(orders=2,total_cents=1500),receipts={"a":first,"b":second},count=2,
        inbox=sorted([dict(scope="order-projection-v1",event_id=value["event_id"],digest=value["envelope_sha256"]) for value in (first,second)],key=lambda r:r["event_id"]), files={"db.sqlite":"f"*64})
    rows.append(dict(event="consumer_owner_successor_image",image=successor_image))
    return json.loads(json.dumps(rows))


def row(rows, event, **fields):
    return next(value for value in rows if value["event"]==event and all(value.get(k)==v for k,v in fields.items()))


class ConsumerOwnerTests(unittest.TestCase):
    def audit(self, rows, changed_image=False):
        image = copy.deepcopy(row(fixture(),"consumer_owner_image")["image"])
        if changed_image: image["files"]["db.sqlite"]="f"*64
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/"ledger.jsonl";path.write_text("".join(json.dumps(value)+"\n" for value in rows))
            with patch("consumer_owner_audit.inspect_image",side_effect=lambda archive,scope: image if archive.name == "old-consumer-image" else copy.deepcopy(row(fixture(),"consumer_owner_successor_image")["image"])): return audit_evidence(path)

    def test_both_consumer_cuts(self):
        for cut in ("before_accept","before_commit"):
            self.assertEqual(self.audit(fixture(cut))["consumer_takeovers"],1)

    def test_missing_or_reordered_boundaries_are_rejected(self):
        for event in ("consumer_owner_seeded","consumer_owner_entered","consumer_owner_isolated","consumer_owner_expired","consumer_owner_successor","consumer_owner_released","consumer_owner_attempted","consumer_owner_fenced","consumer_owner_interrupted","consumer_owner_image","consumer_owner_fetch","consumer_owner_ack","fleet_process_stopped","consumer_owner_successor_image"):
            with self.subTest(event=event), self.assertRaises(AuditError): self.audit([r for r in fixture() if r["event"]!=event])
        rows=fixture();value=row(rows,"consumer_owner_fetch");rows.remove(value);rows.insert(10,value)
        with self.assertRaises(AuditError): self.audit(rows)
        rows=fixture();value=copy.deepcopy(row(rows,"consumer_owner_entered"));rows.insert(rows.index(row(rows,"consumer_owner_entered"))+1,value)
        with self.assertRaises(AuditError): self.audit(rows)

    def test_stopped_old_process_pid_reuse_and_changed_authority_fail(self):
        for kind in ("paused","traced","dead","pid","epoch","generation","expiry","refusal","budget"):
            rows=fixture()
            if kind in ("paused","traced","dead"): row(rows,"consumer_owner_released")["state"]={"paused":"T","traced":"t","dead":"Z"}[kind]
            elif kind=="pid": row(rows,"consumer_owner_successor")["pid"]=3
            elif kind=="epoch": row(rows,"consumer_owner_audit",label="before-release")["owner"]["epoch"]=1
            elif kind=="generation": row(rows,"consumer_owner_expired")["lease"]["generation"]="other"
            elif kind=="expiry": row(rows,"consumer_owner_expired")["observed_ms"]=2000
            elif kind=="refusal": row(rows,"consumer_owner_expired")["refused"]=[]
            else: row(rows,"consumer_owner_released")["elapsed_ms"]=10000
            with self.subTest(kind=kind),self.assertRaises(AuditError):self.audit(rows)

    def test_later_write_failure_cannot_hide_acceptance_or_commit(self):
        for cut in ("before_accept","before_commit"):
            for field in ("accepted","acceptance_closed_domain","refused","closed_domain"):
                rows=fixture(cut);phase=row(rows,"consumer_owner_attempted")["phase"]
                phase[field]=not phase[field] if field!="accepted" else (None if phase[field] is True else True)
                with self.subTest(cut=cut,field=field),self.assertRaises(AuditError):self.audit(rows)

    def test_original_receipt_effect_and_old_disk_cannot_be_repaired(self):
        for kind in ("receipt","effect","checkpoint","lease","scope","old_receipt","old_effect","old_count","old_digest","archive"):
            rows=fixture()
            if kind.startswith("old_"):
                image=row(rows,"consumer_owner_image")["image"]
                if kind=="old_receipt":image["inbox"]=[]
                elif kind=="old_effect":image["projection"]["orders"]=2
                elif kind=="old_count":image["count"]=2
                else:image["inbox"][0]["digest"]="f"*64
            elif kind!="archive":
                snapshot=row(rows,"consumer_owner_audit",label="before-release")["snapshot"]
                if kind=="receipt":snapshot["effects"]["receipts"]={}
                elif kind=="effect":snapshot["effects"]["projection"]["orders"]=0
                elif kind=="checkpoint":snapshot["subscription"]["checkpoint"]="2"
                elif kind=="lease":snapshot["subscription"]["lease"]["generation"]="2"
                else:snapshot["effects"]["scope"]="Projection:"+"d"*64
            with self.subTest(kind=kind),self.assertRaises(AuditError):self.audit(rows,changed_image=kind=="archive")

    def test_changed_fetch_envelope_receipt_or_duplicate_effect_is_rejected(self):
        for kind in ("fetch_id","receipt","envelope","fresh","projection","replay"):
            rows=fixture()
            if kind=="fetch_id":row(rows,"consumer_owner_entered")["phase"]["delivery"]["fetch_id"]="0"*32
            else:
                result=row(rows,"consumer_owner_fetch",label="replayed" if kind=="replay" else "applied")["result"]
                if kind=="receipt":result["batch"]["receipt"]+="x"
                elif kind=="envelope":result["batch"]["records"][0]["envelope_sha256"]="f"*64
                elif kind=="projection":result["projection"]["total_cents"]=1800
                else:result["fresh_effects"]=1 if kind=="replay" else 0
            with self.subTest(kind=kind),self.assertRaises(AuditError):self.audit(rows)
        rows=fixture();rows.insert(17,dict(event="attempt",key="b",attempt=2,replay=True))
        with self.assertRaises(AuditError):self.audit(rows)

    def test_successor_cannot_lose_first_inbox_receipt_behind_correct_totals(self):
        for kind in ("receipt","digest","count","effect"):
            rows=fixture();image=row(rows,"consumer_owner_successor_image")["image"]
            if kind=="receipt":image["inbox"].pop(0)
            elif kind=="digest":image["inbox"][0]["digest"]="0"*64
            elif kind=="count":image["count"]=1
            else:image["projection"]["orders"]=3
            with self.subTest(kind=kind),self.assertRaises(AuditError):self.audit(rows)

    def test_wrong_fence_and_incomplete_store_cleanup_are_rejected(self):
        rows=fixture();row(rows,"consumer_owner_fenced")["report"]["reason"]="node_lease_missing"
        with self.assertRaises(AuditError):self.audit(rows)
        rows=fixture();rows.insert(1,dict(event="network_store_started",backend="s3",image_id="sha256:"+"a"*64,container="cellar-stream-store-private"))
        with self.assertRaises(AuditError):self.audit(rows)
        rows.append(dict(event="network_store_removed",container="cellar-stream-store-private"))
        self.assertEqual(self.audit(rows)["backend"],"s3")


if __name__ == "__main__":unittest.main()
