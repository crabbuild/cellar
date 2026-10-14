#!/usr/bin/env python3
"""Lose a proven ack and destination process; replay only the stored exact receipt."""
import argparse
import base64
import concurrent.futures
import hashlib
import json
import os
from pathlib import Path
import tempfile
import time

from faults import AuditError, Ledger, eventually, read, require, validate_snapshot
from fleet_faults import FleetProcess, NetworkStore, PeerProxy, free_address


def audit(ledger,origins,label,effects,checkpoint):
    ledger.audited=False
    snapshot=dict(sources={"cross":read(origins["producer"],"/orders/cross")},
        events=read(origins["destination"],"/__test/events"),
        effects=read(origins["consumer"],"/__test/projection-receipts"),
        subscription=read(origins["destination"],"/subscription"))
    require(snapshot["sources"]["cross"]["host"]=="producer" and snapshot["effects"]["host"]=="consumer",
        "source/effect roles were not independently hosted")
    accepted=validate_snapshot(ledger.operations,snapshot,effects,checkpoint,{"cross":0})
    if checkpoint==1:
        require(snapshot["subscription"]["observation"]["lease_active"] is False,"proven ack retained a live lease")
        require(len(snapshot["effects"].get("ack_receipt_sha256") or "")==64,"consumer did not retain its proven batch receipt")
    ledger.record("ack_audit",label=label,operations=ledger.operations,snapshot=snapshot)
    for key,receipt in accepted.items():ledger.operations[key]["accepted"]=receipt
    ledger.record("ack_audit_passed",label=label)
    ledger.audited=True


def audit_evidence(path):
    operations=None;pids=None;replacement=None;held=None;killed=False;dropped=False;resumed=False
    snapshots={};passed=set();forwarded=0;stopped=set();store=None;removed=False;replay=None;original_seen=False
    ambiguities=0;interrupted_audits=0
    for line in path.read_text().splitlines():
        row=json.loads(line);event=row["event"]
        if event=="plan":
            require(operations is None and set(row["operations"])=={"cross"},"invalid ack command plan")
            operations=row["operations"]
        elif event=="network_store_started":
            require(store is None and pids is None and row["backend"]=="s3" and row["image_id"].startswith("sha256:")
                and row["container"].startswith("cellar-stream-store-"),"invalid private ack store")
            store=row
        elif event=="attempt":
            require(row["key"]=="cross" and row["attempt"]==1 and not row["replay"] and operations["cross"]["attempts"]==0,
                "source replay could conceal lost acceptance")
            operations["cross"]["attempts"]=1
        elif event=="ack":
            require(row["key"]=="cross" and operations["cross"]["attempts"]==1 and "ack" not in operations["cross"],"invalid source acknowledgment")
            operations["cross"]["ack"]=row["receipt"]
        elif event=="ack_fleet_started":
            require(pids is None and set(row["pids"])=={"producer","destination","consumer"}
                and len(set(row["pids"].values()))==3,"ack roles were not separate processes")
            pids=row["pids"]
        elif event=="ack_reply_held":
            require(held is None and "accepted-before-consumption" in passed,"ack proof boundary preceded source audit")
            held=row["exchange"]
            require(held["method"]=="__streamAck" and held["scope"].startswith("__EventStream:")
                and len(held["scope"].split(":",1)[1])==64,"held reply names another method/resource")
            for field in ("request","response"):
                require(hashlib.sha256(base64.b64decode(held[field],validate=True)).hexdigest()==held[field+"_sha256"],"captured ack bytes changed")
        elif event=="ack_audit":
            label=row["label"]
            require(label not in snapshots,"repeated ack audit stage")
            if label=="accepted-before-consumption":
                require(pids is not None and held is None and not killed,"source audit followed ack loss")
                effects,checkpoint=[],0
            elif label=="before-destination-loss":
                require(held is not None and not killed and "accepted-before-consumption" in passed,"ack audit did not precede process loss")
                effects,checkpoint=["cross"],1
            elif label=="recovered-before-replay":
                require(killed and dropped and replacement is not None and not resumed and replay is None,
                    "ack recovery audit did not precede replay")
                effects,checkpoint=["cross"],1
            elif label=="final":
                require(resumed and forwarded>=1 and replay=="AlreadyAcknowledged" and "recovered-before-replay" in passed,
                    "final ack audit lacks exact stored receipt replay")
                effects,checkpoint=["cross"],1
            elif label.startswith("replay-interrupted-"):
                require(resumed and "recovered-before-replay" in passed and replay is None,
                    "interrupted ack replay preceded recovery or followed final receipt")
                require(ambiguities==interrupted_audits+1 and label==f"replay-interrupted-{interrupted_audits}",
                    "interrupted replay audit lacks its original ambiguous attempt")
                interrupted_audits+=1
                effects,checkpoint=["cross"],1
            else:require(False,"unknown ack audit stage")
            require(set(row["operations"])==set(operations),"ack command inventory changed")
            for field in ("payload","sha256","offset","attempts","ack"):
                require(row["operations"]["cross"].get(field)==operations["cross"].get(field),"ack audit changed original command")
            snapshot=row["snapshot"]
            require(snapshot["sources"]["cross"]["host"]=="producer" and snapshot["effects"]["host"]=="consumer","ack source/effect host changed")
            accepted=validate_snapshot(row["operations"],snapshot,effects,checkpoint,{"cross":0})
            if checkpoint==1:
                require(snapshot["subscription"]["observation"]["lease_active"] is False,"ack checkpoint recovered with live lease")
                digest=snapshot["effects"].get("ack_receipt_sha256")
                require(isinstance(digest,str) and len(digest)==64,"stored proven receipt missing")
                if "before-destination-loss" in snapshots:
                    require(digest==snapshots["before-destination-loss"]["effects"]["ack_receipt_sha256"],"stored receipt changed before exact replay")
            for key,receipt in accepted.items():
                require(operations[key].get("accepted") in (None,receipt),"ack event/effect changed after proof")
                operations[key]["accepted"]=receipt
            snapshots[label]=snapshot
        elif event=="ack_audit_passed":
            require(row["label"] in snapshots and row["label"] not in passed,"ack audit passed without snapshot")
            passed.add(row["label"])
        elif event=="process_killed":
            require(not killed and "before-destination-loss" in passed and row["label"]=="destination-after-proven-ack"
                and row["pid"]==pids["destination"] and row["exit_code"]==-9,"wrong or premature ack destination kill")
            killed=True
        elif event=="ack_reply_lost":
            require(killed and not dropped and not resumed and row["request_sha256"]==held["request_sha256"],"ack reply loss did not follow proof/kill")
            dropped=True
        elif event=="ack_destination_restarted":
            require(killed and dropped and original_seen and replacement is None and row["pid"] not in pids.values() and not resumed,"ack restart did not replace killed process")
            replacement=row["pid"]
        elif event=="retry_blocked":
            require(held is not None and not resumed and row["request_sha256"]==held["request_sha256"],"blocked ack replay changed bytes")
        elif event=="retry_resumed":
            require("recovered-before-replay" in passed and not resumed,"ack retry concealed recovery")
            resumed=True
        elif event=="ack_reply_forwarded":
            exchange=row["exchange"]
            require(resumed and exchange["method"]=="__streamAck" and exchange["scope"]==held["scope"]
                and exchange["request"]==held["request"] and exchange["request_sha256"]==held["request_sha256"],"ack replay changed receipt bytes or preceded audit")
            require(hashlib.sha256(base64.b64decode(exchange["response"],validate=True)).hexdigest()==exchange["response_sha256"],"ack replay response capture changed")
            forwarded+=1
        elif event=="stored_ack_replayed":
            require(resumed and forwarded>=1 and replay is None and row["status"]=="AlreadyAcknowledged","stored ack did not replay idempotently")
            replay=row["status"]
        elif event=="stored_ack_ambiguous":
            require(resumed and "recovered-before-replay" in passed and replay is None and ambiguities==interrupted_audits,
                "ambiguous stored replay preceded recovery or its prior audit")
            ambiguities+=1
        elif event=="original_ack_response":
            require(killed and dropped and not original_seen and replacement is None and row["status"]=="reply_lost" and row["fresh_effects"]==1,
                "original consumer outcome did not observe actual ack response loss")
            original_seen=True
        elif event=="peer_unavailable":require(row["expected"] is True,"unexpected ack peer outage ignored")
        elif event=="fleet_process_stopped":
            require("final" in passed and row["node"] not in stopped and row["exit_code"]==0 and len(row["reports"])==1
                and all(row["reports"][0].get(key) is True for key in ("ingress_drained","application_work_drained","cells_drained")),"incomplete ack fleet shutdown")
            require(row["pid"]==(replacement if row["node"]=="destination" else pids.get(row["node"])),"ack shutdown names another process")
            stopped.add(row["node"])
        elif event=="network_store_removed":
            require(store is not None and not removed and stopped==set(pids) and row["container"]==store["container"],"ack store removed before complete shutdown")
            removed=True
    required={"accepted-before-consumption","before-destination-loss","recovered-before-replay","final"}
    require(set(snapshots)==passed and required.issubset(passed)
        and held is not None and killed and dropped and replacement is not None and resumed and forwarded>=1 and replay=="AlreadyAcknowledged"
        and original_seen and ambiguities==interrupted_audits and stopped==set(pids) and (store is None or removed),"incomplete independent ack fault evidence")
    return dict(audits=len(passed),destination_sigkills=1,response_drops=1,identical_receipt_replays=forwarded,backend="s3" if store else "sqlite")


def exercise(binary,root,rustfs_image=None):
    require(os.name=="posix","ack process qualification requires SIGKILL")
    root.mkdir(parents=True,exist_ok=False)
    ledger=Ledger(root,commands=(("cross",725,1),));processes=[];proxy=None
    store=NetworkStore(root,ledger,rustfs_image) if rustfs_image else None
    try:
        if store:store.start()
        peer=free_address();proxy=PeerProxy(peer,ledger,"__streamAck")
        destination=FleetProcess(binary,root,"destination","destination-initial",peer,proxy.address,True,store)
        processes.append(destination);origins={"destination":destination.ready()}
        require(read(origins["destination"],"/__test/events")["records"]==[],"ack destination not initially empty")
        for node in ("producer","consumer"):
            address=free_address();process=FleetProcess(binary,root,node,node,address,address,False,store)
            processes.append(process);origins[node]=process.ready()
        ledger.record("ack_fleet_started",pids={node:process.child.pid for node,process in zip(("destination","producer","consumer"),processes)})
        ledger.issue(origins["producer"],"cross")
        eventually(lambda:read(origins["producer"],"/orders/cross"),lambda state:state["outbox"]["pending_rows"]=="0")
        audit(ledger,origins,"accepted-before-consumption",[],0)
        proxy.arm()
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future=pool.submit(read,origins["consumer"],"/projection/lost-ack","POST",15)
            try:
                require(proxy.entered.wait(5),"proven ack boundary not reached")
                audit(ledger,origins,"before-destination-loss",["cross"],1)
                destination.crash(ledger,"destination-after-proven-ack")
            finally:proxy.release.set()
            result=future.result(timeout=15)
            require(result["status"]=="reply_lost" and result["fresh_effects"]==1,"consumer did not observe ack reply loss")
            ledger.record("original_ack_response",status=result["status"],fresh_effects=result["fresh_effects"])
        destination=FleetProcess(binary,root,"destination","destination-recovered",peer,proxy.address,True,store)
        processes.append(destination);origins["destination"]=destination.ready()
        ledger.record("ack_destination_restarted",pid=destination.child.pid)
        audit(ledger,origins,"recovered-before-replay",["cross"],1)
        proxy.resume("recovered-before-replay")
        deadline=time.monotonic()+60;interruptions=0
        while True:
            require(time.monotonic()<deadline,"stored ack transport did not recover")
            try:
                result=read(origins["consumer"],"/projection/ack-stored","POST")
                break
            except AuditError as error:
                ledger.record("stored_ack_ambiguous",error=str(error))
                audit(ledger,origins,f"replay-interrupted-{interruptions}",["cross"],1)
                interruptions+=1
                time.sleep(.05)
        require(result["status"]=="AlreadyAcknowledged","stored receipt did not return AlreadyAcknowledged")
        ledger.record("stored_ack_replayed",status=result["status"])
        audit(ledger,origins,"final",["cross"],1)
        proxy.outage=True
        for process in reversed(processes):process.stop_recorded(ledger)
        proxy.close();proxy=None
        if store:store.close()
        summary=dict(audit_evidence(ledger.path),status="passed",binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),output=str(root),
            scope="three independent loopback processes; proven ack reply loss and destination SIGKILL; persisted receipt replay without new fetch")
        (root/"summary.json").write_text(json.dumps(summary,indent=2)+"\n")
        print(json.dumps(summary),flush=True)
    finally:
        if proxy:proxy.release.set();proxy.outage=True
        for process in reversed(processes):process.stop()
        if proxy:proxy.close()
        if store:store.close()
        ledger.close()


if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    target=parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--binary",type=Path);target.add_argument("--audit",type=Path)
    parser.add_argument("--output",type=Path);parser.add_argument("--rustfs-image")
    args=parser.parse_args()
    if args.audit:print(json.dumps(audit_evidence(args.audit.resolve())),flush=True)
    else:exercise(args.binary.resolve(),(args.output or Path(tempfile.mkdtemp(prefix="cellar-ack-fault-parent-"))/"evidence").resolve(),args.rustfs_image)
