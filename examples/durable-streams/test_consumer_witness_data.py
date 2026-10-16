"""Actual batch damage, custody, refusal and seal-preserving return boundaries."""
import copy
import hashlib
import json
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch

from consumer_recovery_audit import audit_evidence
from consumer_witness_data import damage, restore, validate_damage, validate_return
from faults import AuditError
from test_consumer_recovery import fixture
from test_consumer_owner import row
from whole_fleet import inventory


def data_fixture(mode):
    rows=fixture(True);row(rows,"consumer_recovery_started")["witness_data"]=mode
    retained=row(rows,"consumer_followers_retained")["followers"]
    damaged={};restored={}
    for node,data in retained.items():
        state=dict(path="state.json",bytes=50,sha256="2"*64)
        data["files"].append(state)
        changed=[copy.deepcopy(state)]
        if mode=="corrupt":changed.insert(0,dict(data["files"][0],sha256="3"*64))
        damaged[node]=changed
        row(rows,"consumer_witness_started",node=node)["files"]=copy.deepcopy(changed)
        sealed=dict(state,sha256="4"*64)
        before=[sealed] if mode=="missing" else [changed[0],sealed]
        restored[node]=dict(before=before,after=[copy.deepcopy(data["files"][0]),sealed],sha256={data["files"][0]["path"]:data["files"][0]["sha256"]})
    at=rows.index(row(rows,"consumer_followers_retained"))+1
    rows.insert(at,dict(event="consumer_witness_data_damaged",mode=mode,files=damaged))
    refused=row(rows,"consumer_witness_refused");refused["attempt"]=1
    leader="consumer/"+"c"*32+"/"+"c"*64
    refused["diagnostic"]=dict(leader=leader,epoch=1,recovery_blocked=True,ensemble=sorted(retained))
    for observations in refused["gates"].values():
        for observation in observations:observation.update(blocked=False,response=dict(fragment_epoch=0,held_fragment_epoch=1,base=0,end=2))
    second=copy.deepcopy(refused);second["attempt"]=2
    for node in second["gates"]:second["gates"][node]*=2
    steps=[second]
    for node in sorted(retained):
        ready=row(rows,"consumer_witness_ready",node=node)
        steps.append(dict(event="consumer_damaged_witness_joined",node=node,pid=ready["pid"],exit_code=0,reports=[dict(ingress_drained=True,application_work_drained=True,cells_drained=True)],lease=ready["lease"]))
    steps.append(dict(event="consumer_witness_connections_retired",connections={node:1 for node in retained}))
    steps.append(dict(event="consumer_witness_data_restored",files=restored))
    for node,pid in (("destination",7),("producer",8)):
        lease=row(rows,"consumer_witness_ready",node=node)["lease"]
        steps.append(dict(event="consumer_returned_witness_started",node=node,pid=pid,observed_ms=2001,lease=lease,files=restored[node]["after"]))
        row(rows,"fleet_process_stopped",node=node)["pid"]=pid
    for node,pid in (("destination",7),("producer",8)):
        lease=dict(row(rows,"consumer_witness_ready",node=node)["lease"],generation="2"*64)
        steps.append(dict(event="consumer_returned_witness_ready",node=node,pid=pid,lease=lease))
    at=rows.index(refused)+1;rows[at:at]=steps
    return json.loads(json.dumps(rows))


class WitnessDataTests(unittest.TestCase):
    def audit(self,rows,changed_archive=False,rolled_back=False):
        expected=data_fixture(row(rows,"consumer_recovery_started")["witness_data"])
        images={"lost-consumer-image":row(expected,"consumer_proof_image")["image"],"recovered-consumer-image":row(expected,"consumer_recovered_image")["image"]}
        retained=row(expected,"consumer_followers_retained")["followers"]
        damaged=row(expected,"consumer_witness_data_damaged")["files"]
        restored=row(expected,"consumer_witness_data_restored")["files"]
        def files(path):
            result=copy.deepcopy(damaged[path.name] if path.parent.name=="damaged-consumer-followers" else restored[path.name]["after"])
            if changed_archive:result[0]["sha256"]="0"*64
            return result
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);path=root/"ledger.jsonl";path.write_text("".join(json.dumps(value)+"\n" for value in rows))
            for node in retained:
                target=root/"returned-consumer-followers"/node;target.mkdir(parents=True)
                (target/"state.json").write_text(json.dumps(dict(sealed_to=0 if rolled_back else 1)))
            with patch("consumer_recovery_audit.inspect_consumer",side_effect=lambda archive,scope:images[archive.name]),patch("consumer_recovery_audit.inspect_follower_disk",side_effect=lambda archive:retained[archive.name]),patch("consumer_recovery_audit.inventory",side_effect=files):
                return audit_evidence(path)

    def test_missing_and_corrupt_data_require_two_real_refusals_then_return(self):
        for mode in ("missing","corrupt"):self.assertEqual(self.audit(data_fixture(mode))["consumer_fleet_recoveries"],1)

    def test_missing_custody_refusals_joins_and_reload_are_rejected(self):
        events=("consumer_witness_data_damaged","consumer_damaged_witness_joined","consumer_witness_connections_retired","consumer_witness_data_restored","consumer_returned_witness_started","consumer_returned_witness_ready")
        for event in events:
            with self.subTest(event=event),self.assertRaises((AuditError,KeyError)):self.audit([value for value in data_fixture("missing") if value["event"]!=event])
        rows=data_fixture("missing");rows.remove(row(rows,"consumer_witness_refused",attempt=2))
        with self.assertRaises(AuditError):self.audit(rows)

    def test_service_fault_complete_certificate_and_cached_diagnostic_cannot_replace_data_fault(self):
        for kind in ("blocked","complete","cached","diagnostic","epoch"):
            rows=data_fixture("corrupt");refused=row(rows,"consumer_witness_refused",attempt=2)
            if kind=="blocked":refused["gates"]["destination"][0]["blocked"]=True
            elif kind=="complete":refused["gates"]["destination"][0]["response"]["fragment_epoch"]=1
            elif kind=="cached":refused["gates"]=row(rows,"consumer_witness_refused",attempt=1)["gates"]
            elif kind=="diagnostic":refused["diagnostic"]["recovery_blocked"]=False
            else:refused["diagnostic"]["epoch"]=2
            with self.subTest(kind=kind),self.assertRaises(AuditError):self.audit(rows)

    def test_return_cannot_rewrite_seal_state_invent_batches_or_restart_recoverer(self):
        for kind in ("state","batch","pid","generation"):
            rows=data_fixture("missing")
            if kind in ("state","batch"):
                restored=row(rows,"consumer_witness_data_restored")["files"]["destination"]
                restored["after"][1 if kind=="state" else 0]["sha256"]="0"*64
            elif kind=="pid":row(rows,"consumer_returned_witness_started",node="destination")["pid"]=4
            else:row(rows,"consumer_returned_witness_ready",node="destination")["lease"]["generation"]="1"*64
            with self.subTest(kind=kind),self.assertRaises(AuditError):self.audit(rows)
        with self.assertRaises(AuditError):self.audit(data_fixture("missing"),changed_archive=True)
        with self.assertRaises(AuditError):self.audit(data_fixture("missing"),rolled_back=True)

    def test_actual_fault_and_return_modify_only_batch_bytes(self):
        for mode in ("missing","corrupt"):
            with self.subTest(mode=mode),tempfile.TemporaryDirectory() as directory:
                root=Path(directory);live=root/"live";live.mkdir()
                (live/"state.json").write_text('{"fragment_epoch":1,"end":2,"sealed_to":0}')
                for name in ("1-1.batch","2-2.batch"):(live/name).write_bytes(b"CLB1"+b"x"*100)
                original=inventory(live);archive=root/"consumer-follower-images"/"peer";shutil.copytree(live,archive)
                changed=damage({"peer":live},root,mode)["peer"];validate_damage(mode,original,changed)
                if mode=="corrupt":
                    for name in ("1-1.batch","2-2.batch"):
                        before=(archive/name).read_bytes();after=(live/name).read_bytes()
                        self.assertEqual(sum(a!=b for a,b in zip(before,after)),1)
                # Runtime seal advancement is preserved by return.
                (live/"state.json").write_text('{"fragment_epoch":1,"end":2,"sealed_to":1}')
                retained={"peer":dict(files=original,batches=[dict(path=name) for name in ("1-1.batch","2-2.batch")])}
                restored=restore({"peer":live},root,retained)["peer"]
                validate_return(original,restored["before"],restored["after"])
                self.assertEqual(json.loads((live/"state.json").read_text())["sealed_to"],1)
                for name in ("1-1.batch","2-2.batch"):self.assertEqual((live/name).read_bytes(),(archive/name).read_bytes())


if __name__=="__main__":unittest.main()
