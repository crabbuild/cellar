# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

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
from consumer_witness_data import damage, restore, validate_damage, validate_return, validate_oversized_batch, OVERSIZED_BATCH_BYTES
from faults import AuditError
from test_consumer_recovery import fixture
from test_consumer_owner import row
from whole_fleet import inventory
from test_follower_state import state_bytes, LEADER


def data_fixture(mode):
    rows=fixture(True);row(rows,"consumer_recovery_started")["witness_data"]=mode
    retained=row(rows,"consumer_followers_retained")["followers"]
    damaged={};restored={}
    for node,data in retained.items():
        data["seed"]=dict(fragment_epoch=1,base=0,end=2,sealed_to=0)
        for batch,file in zip(data["batches"],(entry for entry in data["files"] if entry["path"].endswith(".batch"))):batch["path"]=file["path"]
        state=dict(path="state.json",bytes=50,sha256="2"*64)
        data["files"].append(state)
        changed=[copy.deepcopy(state)]
        if mode=="corrupt":changed.insert(0,dict(data["files"][0],sha256="3"*64))
        if mode=="oversize":changed.insert(0,dict(data["files"][0],sha256="3"*64,bytes=OVERSIZED_BATCH_BYTES))
        if mode=="state-missing":changed=[copy.deepcopy(data["files"][0])]
        if mode=="state-corrupt":changed=[copy.deepcopy(data["files"][0]),dict(state,sha256="3"*64)]
        if mode=="state-oversize":changed=[copy.deepcopy(data["files"][0]),dict(state,sha256="3"*64,bytes=64<<10)]
        damaged[node]=changed
        row(rows,"consumer_witness_started",node=node)["files"]=copy.deepcopy(changed)
        sealed=dict(state,sha256="4"*64)
        before=[sealed] if mode=="missing" else [changed[0],sealed]
        restored[node]=dict(before=before,after=[copy.deepcopy(data["files"][0]),sealed],sha256={data["files"][0]["path"]:data["files"][0]["sha256"]})
        if mode.startswith("state-"):restored[node].update(before=copy.deepcopy(changed),after=copy.deepcopy(data["files"]))
    at=rows.index(row(rows,"consumer_followers_retained"))+1
    rows.insert(at,dict(event="consumer_witness_data_damaged",mode=mode,files=damaged))
    refused=row(rows,"consumer_witness_refused");refused["attempt"]=1
    leader="consumer/"+"c"*32+"/"+"c"*64
    refused["diagnostic"]=dict(leader=leader,epoch=1,recovery_blocked=True,ensemble=sorted(retained))
    for observations in refused["gates"].values():
        for observation in observations:
            observation.update(blocked=False,response=dict(fragment_epoch=0,held_fragment_epoch=1,base=0,end=2))
            if mode.startswith("state-"):
                reason="follower state exceeds format bound" if mode=="state-oversize" else "follower state checksum mismatch"
                body=(f"missing follower state for retained fragment {leader}" if mode=="state-missing" else f"invalid follower state for {leader}: {reason}")
                observation.update(status=400,response=None,response_body=body,response_sha256=hashlib.sha256(body.encode()).hexdigest())
    if mode.startswith("state-"):refused["diagnostic"]=None
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
    def audit(self,rows,changed_archive=False,rolled_back=False,altered_oversize=False,altered_batch=None):
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
                state=dict(retained[node]["seed"])
                if not row(rows,"consumer_recovery_started")["witness_data"].startswith("state-"):state["sealed_to"]=0 if rolled_back else 1
                (target/"state.json").write_bytes(state_bytes(state,leader=LEADER,node=node))
                mode=row(rows,"consumer_recovery_started")["witness_data"]
                if mode=="oversize":
                    original=root/"consumer-follower-images"/node;original.mkdir(parents=True)
                    bad=root/"damaged-consumer-followers"/node;bad.mkdir(parents=True)
                    for batch in retained[node]["batches"]:
                        size=next(entry["bytes"] for entry in retained[node]["files"] if entry["path"]==batch["path"])
                        raw=b"x"*size;(original/batch["path"]).write_bytes(raw)
                        with (bad/batch["path"]).open("wb") as file:
                            file.write(b"y"+raw[1:] if altered_batch=="prefix" else raw)
                            file.truncate(OVERSIZED_BATCH_BYTES)
                            if altered_batch=="padding":file.seek(len(raw)+5);file.write(b"x")
                if mode in ("state-corrupt","state-oversize"):
                    original=root/"consumer-follower-images"/node;original.mkdir(parents=True)
                    raw=state_bytes(state,leader=LEADER,node=node);(original/"state.json").write_bytes(raw)
                    bad=root/"damaged-consumer-followers"/node;bad.mkdir(parents=True)
                    if mode=="state-corrupt":
                        altered=json.loads(raw);altered["state"]["base"]=altered["state"]["end"]
                        (bad/"state.json").write_text(json.dumps(altered))
                    else:
                        padded=raw
                        if altered_oversize:
                            value=json.loads(raw);value["state"]["base"]=value["state"]["end"];padded=json.dumps(value).encode()
                        (bad/"state.json").write_bytes(padded+b" "*((64<<10)-len(padded)))
            with patch("consumer_recovery_audit.inspect_consumer",side_effect=lambda archive,scope:images[archive.name]),patch("consumer_recovery_audit.inspect_follower_disk",side_effect=lambda archive:retained[archive.name]),patch("consumer_recovery_audit.inventory",side_effect=files):
                return audit_evidence(path)

    def test_missing_and_corrupt_data_require_two_real_refusals_then_return(self):
        for mode in ("missing","corrupt","oversize","state-missing","state-corrupt","state-oversize"):self.assertEqual(self.audit(data_fixture(mode))["consumer_fleet_recoveries"],1)

    def test_metadata_errors_cannot_supply_a_seal_or_modify_state_before_return(self):
        for kind in ("success","blocked","hash","state","batch"):
            rows=data_fixture("state-corrupt");refusal=row(rows,"consumer_witness_refused",attempt=2)
            if kind=="success":refusal["gates"]["destination"][0]["status"]=200
            elif kind=="blocked":refusal["gates"]["destination"][0]["blocked"]=True
            elif kind=="hash":refusal["gates"]["destination"][0]["response_sha256"]=""
            else:
                value=row(rows,"consumer_witness_data_restored")["files"]["destination"]
                value["before" if kind=="state" else "after"][1 if kind=="state" else 0]["sha256"]="0"*64
            with self.subTest(kind=kind),self.assertRaises(AuditError):self.audit(rows)

    def test_oversized_metadata_needs_a_real_size_refusal_and_the_measured_length(self):
        rows=data_fixture("state-oversize");refused=row(rows,"consumer_witness_refused",attempt=2)
        value=refused["gates"]["destination"][0];value["response_body"]=value["response_body"].replace("exceeds format bound","checksum mismatch")
        value["response_sha256"]=hashlib.sha256(value["response_body"].encode()).hexdigest()
        with self.assertRaisesRegex(AuditError,"metadata error replaced"):self.audit(rows)
        rows=data_fixture("state-oversize")
        original=row(rows,"consumer_followers_retained")["followers"]["destination"]["files"]
        damaged=row(rows,"consumer_witness_data_damaged")["files"]["destination"]
        for size in (4096,(64<<10)-1,(64<<10)+1):
            wrong=copy.deepcopy(damaged);next(item for item in wrong if item["path"]=="state.json")["bytes"]=size
            with self.subTest(size=size),self.assertRaisesRegex(AuditError,"fault length"):validate_damage("state-oversize",original,wrong)

    def test_oversized_archive_cannot_hide_a_changed_state_or_checksum(self):
        with self.assertRaisesRegex(AuditError,"changed fields or checksum"):
            self.audit(data_fixture("state-oversize"),altered_oversize=True)

    def test_oversized_batch_requires_the_exact_fault_length(self):
        rows=data_fixture("oversize")
        original=row(rows,"consumer_followers_retained")["followers"]["destination"]["files"]
        damaged=row(rows,"consumer_witness_data_damaged")["files"]["destination"]
        for size in (OVERSIZED_BATCH_BYTES-1,OVERSIZED_BATCH_BYTES+1):
            wrong=copy.deepcopy(damaged);next(entry for entry in wrong if entry["path"].endswith(".batch"))["bytes"]=size
            with self.subTest(size=size),self.assertRaisesRegex(AuditError,"fault length"):validate_damage("oversize",original,wrong)

    def test_oversized_batch_cannot_change_prefix_or_add_nonzero_padding(self):
        for kind,message in (("prefix","original prefix"),("padding","nonzero padding")):
            with self.subTest(kind=kind),self.assertRaisesRegex(AuditError,message):
                self.audit(data_fixture("oversize"),altered_batch=kind)

    def test_actual_metadata_return_preserves_all_batches_and_refuses_newer_state(self):
        for mode in ("state-missing","state-corrupt","state-oversize"):
            with self.subTest(mode=mode),tempfile.TemporaryDirectory() as directory:
                root=Path(directory);live=root/"live";live.mkdir()
                raw=state_bytes(dict(fragment_epoch=1,base=1,end=2,sealed_to=0))
                (live/"state.json").write_bytes(raw);(live/"2-2.batch").write_bytes(b"retained batch")
                original=inventory(live);archive=root/"consumer-follower-images/peer";shutil.copytree(live,archive)
                changed=damage({"peer":live},root,mode)
                retained={"peer":dict(files=original,batches=[dict(path="2-2.batch")])}
                restored=restore({"peer":live},root,retained,mode,changed)["peer"]
                self.assertEqual(restored["after"],original);self.assertEqual((live/"state.json").read_bytes(),raw)
                # Returning bytes must never erase a state written since refusal.
                with self.assertRaises(AuditError):restore({"peer":live},root,retained,mode,changed)

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
        for mode in ("missing","corrupt","oversize"):
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
                if mode=="oversize":
                    for name in ("1-1.batch","2-2.batch"):validate_oversized_batch(archive/name,live/name)
                # Runtime seal advancement is preserved by return.
                (live/"state.json").write_text('{"fragment_epoch":1,"end":2,"sealed_to":1}')
                retained={"peer":dict(files=original,batches=[dict(path=name) for name in ("1-1.batch","2-2.batch")])}
                restored=restore({"peer":live},root,retained)["peer"]
                validate_return(original,restored["before"],restored["after"])
                self.assertEqual(json.loads((live/"state.json").read_text())["sealed_to"],1)
                for name in ("1-1.batch","2-2.batch"):self.assertEqual((live/name).read_bytes(),(archive/name).read_bytes())


if __name__=="__main__":unittest.main()
