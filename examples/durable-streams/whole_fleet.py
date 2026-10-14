#!/usr/bin/env python3
"""Audit bucket recovery after losing every fleet process and local node directory."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import tempfile

from faults import Ledger, eventually, read, require, validate_snapshot
from fleet_faults import FleetProcess, NetworkStore, free_address


ROLES=("destination","producer","consumer")
STAGES={
    "before-whole-fleet-loss":(["cross"],0,{"cross":0,"waiting":1}),
    "recovered-before-replay":(["cross"],0,{"cross":0,"waiting":1}),
    "first-acknowledged":(["cross"],1,{"cross":0,"waiting":0}),
    "final":(["cross","waiting"],2,{"cross":0,"waiting":0}),
}


def audit(ledger,origins,label):
    ledger.audited=False
    snapshot=dict(sources={key:read(origins["producer"],"/orders/"+key) for key in ledger.operations},
        events=read(origins["destination"],"/__test/events"),
        effects=read(origins["consumer"],"/__test/projection-receipts"),
        subscription=read(origins["destination"],"/subscription"))
    require(all(source["host"]=="producer" for source in snapshot["sources"].values())
        and snapshot["effects"]["host"]=="consumer","recovery changed the independently hosted roles")
    effects,checkpoint,pending=STAGES[label]
    accepted=validate_snapshot(ledger.operations,snapshot,effects,checkpoint,pending)
    if checkpoint==0:
        require([record["order_id"] for record in snapshot["events"]["records"]]==["cross"],
            "proven destination acceptance disappeared before replay")
    ledger.record("whole_fleet_audit",label=label,operations=ledger.operations,snapshot=snapshot)
    for key,receipt in accepted.items():ledger.operations[key]["accepted"]=receipt
    ledger.record("whole_fleet_audit_passed",label=label)
    ledger.audited=True


def inventory(path):
    records=[]
    for file in sorted(path.rglob("*")):
        require(not file.is_symlink(),"unexpected symlink in private node data")
        if file.is_file():
            records.append(dict(path=str(file.relative_to(path)),bytes=file.stat().st_size,
                sha256=hashlib.sha256(file.read_bytes()).hexdigest()))
    return records


def audit_evidence(path):
    operations=None; initial=None; replacements={};killed=set();discarded=set();stopped=set()
    snapshots={};passed=set();store=None;removed=False;replays=0
    for line in path.read_text().splitlines():
        row=json.loads(line);event=row["event"]
        if event=="plan":
            require(operations is None,"multiple operation plans")
            operations=row["operations"]
            require(set(operations)=={"cross","waiting"},"whole-fleet command inventory changed")
        elif event=="network_store_started":
            require(store is None and initial is None and row["backend"]=="s3"
                and row["container"].startswith("cellar-stream-store-") and row["image_id"].startswith("sha256:"),
                "invalid private store identity")
            store=row
        elif event=="whole_fleet_started":
            require(initial is None and row["proof_mode"]=="bucket","whole-fleet proof mode missing")
            initial=row["pids"]
            require(set(initial)==set(ROLES) and len(set(initial.values()))==3,"initial processes were not independent")
        elif event=="attempt":
            key=row["key"]
            require(key in operations and row["attempt"]==operations[key]["attempts"]+1,"unissued or reordered command")
            require(not row["replay"],"source command replay could conceal bucket loss")
            operations[key]["attempts"]=row["attempt"]
        elif event=="ack":
            key=row["key"]
            require(key in operations and operations[key]["attempts"]==1 and "ack" not in operations[key],"invalid original command acknowledgment")
            operations[key]["ack"]=row["receipt"]
        elif event=="whole_fleet_audit":
            label=row["label"]
            require(label in STAGES and label not in snapshots,"unknown or repeated whole-fleet stage")
            if label=="before-whole-fleet-loss":
                require(initial is not None and not killed,"initial audit followed process loss")
            elif label=="recovered-before-replay":
                require(killed==set(ROLES) and discarded==set(ROLES) and set(replacements)==set(ROLES),
                    "recovery audit preceded complete fleet/local-data loss")
            elif label=="first-acknowledged":
                require("recovered-before-replay" in passed and replays==1,"consumer replay concealed recovery")
            elif label=="final":require("first-acknowledged" in passed,"final audit preceded first checkpoint")
            require(set(row["operations"])==set(operations),"audit command inventory changed")
            for key,operation in operations.items():
                for field in ("payload","sha256","offset","attempts","ack"):
                    require(row["operations"][key].get(field)==operation.get(field),"audit changed original command or receipt")
            snapshot=row["snapshot"]
            require(all(source["host"]=="producer" for source in snapshot["sources"].values())
                and snapshot["effects"]["host"]=="consumer","whole-fleet role changed")
            effects,checkpoint,pending=STAGES[label]
            accepted=validate_snapshot(row["operations"],snapshot,effects,checkpoint,pending)
            if checkpoint==0:
                require([record["order_id"] for record in snapshot["events"]["records"]]==["cross"],"lost proven acceptance before replay")
            for key,receipt in accepted.items():
                require(operations[key].get("accepted") in (None,receipt),"accepted identity/effect changed after proof")
                operations[key]["accepted"]=receipt
            snapshots[label]=snapshot
        elif event=="whole_fleet_audit_passed":
            require(row["label"] in snapshots and row["label"] not in passed,"audit pass lacks original snapshot")
            passed.add(row["label"])
        elif event=="process_killed":
            node=row["label"].removeprefix("whole-fleet-")
            require(node in ROLES and node not in killed and "before-whole-fleet-loss" in passed
                and row["pid"]==initial[node] and row["exit_code"]==-9 and not replacements,
                "wrong, repeated or premature whole-fleet process loss")
            killed.add(node)
        elif event=="local_node_data_discarded":
            node=row["node"]
            require(killed==set(ROLES) and node in ROLES and node not in discarded and not replacements
                and row["remaining_entries"]==0 and row["files"],"local data was not discarded after all process losses")
            paths=set()
            for entry in row["files"]:
                file=Path(entry["path"])
                require(not file.is_absolute() and ".." not in file.parts and entry["path"] not in paths
                    and type(entry["bytes"]) is int and entry["bytes"]>=0
                    and len(entry["sha256"])==64,"invalid discarded-file inventory")
                paths.add(entry["path"])
            discarded.add(node)
        elif event=="whole_fleet_restarted":
            node=row["node"]
            require(discarded==set(ROLES) and node in ROLES and node not in replacements
                and row["pid"] not in (*initial.values(),*replacements.values())
                and row["proof_mode"]=="bucket","replacement did not start from bucket-only state")
            replacements[node]=row["pid"]
        elif event=="consumer_replay":
            require("recovered-before-replay" in passed and row["fresh_effects"]==0
                and row["status"]=="acknowledged" and replays==0 and "first-acknowledged" not in passed,
                "consumer replay repeated the proven effect or preceded audit")
            replays+=1
        elif event=="fleet_process_stopped":
            require("final" in passed and row["node"] not in stopped and row["pid"]==replacements.get(row["node"])
                and row["exit_code"]==0 and len(row["reports"])==1 and all(row["reports"][0].get(key) is True
                    for key in ("ingress_drained","application_work_drained","cells_drained")),"incomplete replacement shutdown")
            stopped.add(row["node"])
        elif event=="network_store_removed":
            require(store is not None and stopped==set(ROLES) and not removed and row["container"]==store["container"],
                "private store removal preceded complete recovery/shutdown")
            removed=True
    require(set(snapshots)==passed==set(STAGES) and killed==discarded==stopped==set(ROLES)
        and set(replacements)==set(ROLES) and replays==1 and (store is None or removed),"incomplete whole-fleet evidence")
    return dict(audits=len(passed),sigkills=3,discarded_node_directories=3,backend="s3" if store else "sqlite",proof_mode="bucket")


def exercise(binary,root,rustfs_image=None):
    require(os.name=="posix","whole-fleet qualification requires SIGKILL")
    root.mkdir(parents=True,exist_ok=False)
    ledger=Ledger(root,commands=(("cross",725,1),("waiting",375,2)))
    store=NetworkStore(root,ledger,rustfs_image) if rustfs_image else None
    processes=[];peers={};origins={}
    try:
        if store:store.start()
        for node in ROLES:
            peers[node]=free_address()
            process=FleetProcess(binary,root,node,node+"-initial",peers[node],peers[node],node=="destination",store,False)
            processes.append(process);origins[node]=process.ready()
            if node=="destination":require(read(origins[node],"/__test/events")["records"]==[],"bucket not initially empty")
        ledger.record("whole_fleet_started",pids={node:process.child.pid for node,process in zip(ROLES,processes)},proof_mode="bucket")
        ledger.issue(origins["producer"],"cross")
        eventually(lambda:read(origins["producer"],"/orders/cross"),lambda state:state["outbox"]["pending_rows"]=="0")
        first=read(origins["consumer"],"/projection/retry","POST")
        require(first["fresh_effects"]==1 and first["status"]=="retried","initial proven consumer effect missing")
        ledger.issue(origins["producer"],"waiting")
        audit(ledger,origins,"before-whole-fleet-loss")
        for node,process in zip(ROLES,processes):process.crash(ledger,"whole-fleet-"+node)
        # After observing every SIGKILL, inventory and remove only this driver's
        # private local data. Keep the store, command ledger and process logs.
        for node in ROLES:
            path=root/"data"/node
            files=inventory(path);require(files,"lost node had no local state to discard")
            shutil.rmtree(path)
            require(not path.exists(),"local node directory survived discard")
            ledger.record("local_node_data_discarded",node=node,files=files,remaining_entries=0)
        for node in ROLES:
            process=FleetProcess(binary,root,node,node+"-recovered",peers[node],peers[node],node=="destination",store,False)
            processes.append(process);origins[node]=process.ready()
            ledger.record("whole_fleet_restarted",node=node,pid=process.child.pid,proof_mode="bucket")
            if node=="destination":read(origins[node],"/__test/events")
        audit(ledger,origins,"recovered-before-replay")
        replay=read(origins["consumer"],"/projection/process","POST")
        require(replay["fresh_effects"]==0 and replay["status"]=="acknowledged","bucket-restored inbox repeated effect")
        ledger.record("consumer_replay",fresh_effects=replay["fresh_effects"],status=replay["status"])
        eventually(lambda:read(origins["producer"],"/orders/waiting"),lambda state:state["outbox"]["pending_rows"]=="0")
        audit(ledger,origins,"first-acknowledged")
        second=read(origins["consumer"],"/projection/process","POST")
        require(second["fresh_effects"]==1 and second["status"]=="acknowledged","recovered pending source did not produce one new effect")
        audit(ledger,origins,"final")
        for process in reversed(processes):process.stop_recorded(ledger)
        if store:store.close()
        summary=dict(audit_evidence(ledger.path),status="passed",binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),output=str(root),
            scope="three independent processes; every process SIGKILLed and local node directory discarded; bucket-proof recovery with pending source and proven unacked inbox effect")
        (root/"summary.json").write_text(json.dumps(summary,indent=2)+"\n")
        print(json.dumps(summary),flush=True)
    finally:
        for process in reversed(processes):process.stop()
        if store:store.close()
        ledger.close()


if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    target=parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--binary",type=Path);target.add_argument("--audit",type=Path)
    parser.add_argument("--output",type=Path);parser.add_argument("--rustfs-image")
    args=parser.parse_args()
    if args.audit:print(json.dumps(audit_evidence(args.audit.resolve())),flush=True)
    else:exercise(args.binary.resolve(),(args.output or Path(tempfile.mkdtemp(prefix="cellar-whole-fleet-parent-"))/"evidence").resolve(),args.rustfs_image)
