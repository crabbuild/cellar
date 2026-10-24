#!/usr/bin/env python3
# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Recover a proven unacknowledged consumer effect after losing its local disk."""
import argparse
import hashlib
import hmac
import secrets
import json
import os
from pathlib import Path
import shutil
import tempfile
import time
import urllib.error
import urllib.request

from consumer_owner_audit import FETCH_ID
from consumer_recovery_audit import PEERS, audit_evidence, inspect_consumer
from consumer_witness_gate import WitnessGate
from consumer_witness_data import damage, restore
from faults import Ledger, cleanup_all, eventually, read, require
from fleet_faults import FleetProcess, NetworkStore, free_address
from follower_faults import inspect_follower_disk, log_lineage, replica_inventory
from live_owner import marker
from resumed_owner import authority, store_json
from whole_fleet import inventory


def archive_consumer(root,node,scope,label):
    require(node in PEERS|{"consumer","successor"} and scope.startswith("Projection:") and len(scope)==75 and all(c in "0123456789abcdef" for c in scope[11:]),"unsafe consumer image scope/node")
    files=list((root/"data"/node/"node"/scope/"ltx").glob("e*/db.sqlite"))
    require(len(files)==1,"consumer epoch inventory changed")
    archive=root/label;archive.mkdir()
    for suffix in ("","-wal","-shm"):
        source=Path(str(files[0])+suffix)
        if source.exists():
            target=archive/("db.sqlite"+suffix);shutil.copy2(source,target)
            with target.open("rb") as file:os.fsync(file.fileno())
    return inspect_consumer(archive,scope)


def follower_directory(root,node,lease):
    generation=lease["generation"]
    require(len(generation)==64 and all(c in "0123456789abcdef" for c in generation),"unsafe consumer session")
    return root/"data"/node/"node/peerlog/consumer"/log_lineage(lease)/generation


def archive_followers(root,lease):
    retained={}
    for node in lease["log"]["ensemble"]:
        require(node in PEERS,"unknown consumer witness")
        source=follower_directory(root,node,lease);files=inventory(source)
        require(files,"consumer witness has no retained data")
        archive=root/"consumer-follower-images"/node;shutil.copytree(source,archive)
        for file in archive.rglob("*"):
            if file.is_file():
                with file.open("rb") as saved:os.fsync(saved.fileno())
        require(inventory(archive)==files,"consumer witness changed during copy")
        retained[node]=inspect_follower_disk(archive)
    return retained


def framework_read(root,store,scope):
    # Read-only framework RPC uses its typed event outcome. A generic cell
    # fetch can exhaust routing retries and wrap the failure as application 500.
    secret=store_json(root,store,"fleet/peer-auth.json")
    require(secret["version"]==1,"unsupported private fleet authentication")
    key=bytes.fromhex(secret["key"]);require(len(key)==32,"invalid private fleet key")
    path="/outbox/"+scope;body=b'{"op":"inspect","limit":1}'
    source,target="consumer-recovery-probe","successor"
    stamp=str(time.time_ns()//1000000);nonce=secrets.token_hex(16);body_hash=hashlib.sha256(body).hexdigest()
    canonical="\n".join(["cells-peer-request-v1","1","POST",path,body_hash,source,target,stamp,nonce])
    headers={"x-cells-peer-version":"1","x-cells-peer-source":source,"x-cells-peer-target":target,"x-cells-peer-timestamp":stamp,
        "x-cells-peer-nonce":nonce,"x-cells-peer-body-sha256":body_hash,"x-cells-peer-signature":hmac.new(key,canonical.encode(),hashlib.sha256).hexdigest()}
    address=store_json(root,store,"nodes/successor.json")["addr"]
    return urllib.request.urlopen(urllib.request.Request("http://"+address+path,data=body,headers=headers,method="POST"),timeout=15)


def exercise(binary,root,rustfs_image=None,restart_followers=False,witness_data=None,trace=False):
    require(os.name=="posix","consumer proof qualification requires SIGKILL")
    require(witness_data is None or restart_followers,"witness data loss requires cold peers")
    root.mkdir(parents=True,exist_ok=False);ledger=Ledger(root)
    store=NetworkStore(root,ledger,rustfs_image) if rustfs_image else None
    processes=[];origins={};gates={};data=root/"data/consumer"
    try:
        if store:store.start()
        for name in ("destination","producer","consumer"):
            address=free_address();process=FleetProcess(binary,root,name,name,address,address,name=="destination",store,
                fleet_durability=name=="consumer",replica_fault=name=="consumer",node_lease_ms=1000,stream_rows=2,fixture_trace=trace)
            processes.append(process);origins[name]=process.ready()
            if name=="destination":require(read(origins[name],"/__test/events")["records"]==[],"nonempty fixture stream")
        ledger.record("consumer_recovery_started",pids={p.node:p.child.pid for p in processes},proof_mode="fleet",peer_proof="bucket",replica_fault_node="consumer",node_lease_ms=1000,restart_followers=restart_followers,witness_data=witness_data)
        ledger.issue(origins["producer"],"a")
        eventually(lambda:read(origins["producer"],"/orders/a"),lambda v:v["outbox"]["pending_rows"]=="0")
        ledger.record("consumer_recovery_seeded",result=read(origins["consumer"],"/projection/process","POST"))
        scope=read(origins["consumer"],"/projection/owner-audit")["scope"]
        lease=eventually(lambda:store_json(root,store,"nodes/consumer.json"),lambda v:v.get("log") and v["log"]["state"]=="open" and v["log"]["ensemble"])
        marker(data/"replica-block")
        ledger.record("consumer_proof_armed",scope=scope,owner=authority(root,store,scope),lease=lease,objects=replica_inventory(root,store))
        ledger.issue(origins["producer"],"b")
        eventually(lambda:read(origins["producer"],"/orders/b"),lambda v:v["outbox"]["pending_rows"]=="0")
        result=read(origins["consumer"],f"/projection/fetch?fetch_id={FETCH_ID}","POST")
        def refused():
            path=data/"replica-refused.jsonl";raw=path.read_bytes() if path.exists() else b""
            return [json.loads(line) for line in raw[:raw.rfind(b"\n")+1].splitlines()]
        denied=eventually(refused,lambda rows:any(v["path"].startswith(f"cells/{scope}/ltx/") for v in rows))
        proof=store_json(root,store,"nodes/consumer.json")
        ledger.record("consumer_effect_proven",result=result,owner=authority(root,store,scope),lease=proof,objects=replica_inventory(root,store),refused=denied)
        def audit(label):
            effects=read(origins["consumer"],"/projection/owner-audit")
            saved=read(origins["consumer"],"/__test/projection-receipts")
            require(all(effects[k]==saved[k] for k in ("projection","receipts","host")),"consumer changed during read-only audit")
            effects["ack_receipt_sha256"]=saved["ack_receipt_sha256"]
            snapshot=dict(sources={key:read(origins["producer"],"/orders/"+key) for key in ledger.operations},events=read(origins["destination"],"/__test/events"),effects=effects,subscription=read(origins["destination"],"/subscription"))
            recovery={} if label=="before-loss" else dict(pid=successor.child.pid,lease=store_json(root,store,"nodes/successor.json"))
            ledger.record("consumer_recovery_audit",label=label,operations=ledger.operations,owner=authority(root,store,scope),snapshot=snapshot,recoverer=recovery)
            audit_evidence(ledger.path,complete=False)
        audit("before-loss")
        processes[2].crash(ledger,"consumer-proof-consumer")
        retained={};cold_leases={}
        if restart_followers:
            for process in processes[:2]:process.crash(ledger,"consumer-proof-"+process.node)
            retained=archive_followers(root,proof)
            cold_leases={node:store_json(root,store,"nodes/"+node+".json") for node in PEERS}
            ledger.record("consumer_followers_retained",followers=retained,leases=cold_leases,objects=replica_inventory(root,store))
            if witness_data:
                changed=damage({node:follower_directory(root,node,proof) for node in retained},root,witness_data)
                ledger.record("consumer_witness_data_damaged",mode=witness_data,files=changed)
        ledger.record("consumer_proof_image",image=archive_consumer(root,"consumer",scope,"lost-consumer-image"))
        files=inventory(data);shutil.rmtree(data);require(not data.exists(),"consumer data survived removal")
        ledger.record("consumer_data_discarded",files=files,remaining_entries=0,objects=replica_inventory(root,store))
        old_lease=store_json(root,store,"nodes/consumer.json")
        observed=eventually(lambda:time.time_ns()//1000000,lambda now:now>old_lease["expires_ms"])
        ledger.record("consumer_authority_expired",lease=old_lease,observed_ms=observed)
        audit_evidence(ledger.path,complete=False)
        replacements={}
        if restart_followers:
            for name in sorted(PEERS):
                lease=cold_leases[name];observed=eventually(lambda:time.time_ns()//1000000,lambda now:now>lease["expires_ms"])
                files=inventory(follower_directory(root,name,proof)) if name in retained else []
                require(files==((changed if witness_data else {node:data["files"] for node,data in retained.items()}).get(name,[])),"consumer witness files changed before restart")
                address=free_address();gate=WitnessGate(address,f"consumer/{log_lineage(proof)}/{proof['generation']}",proof["log"]["epoch"]);gates[name]=gate
                if witness_data:gate.release()
                process=FleetProcess(binary,root,name,name+"-cold",address,gate.address,name=="destination",store,fleet_durability=False,node_lease_ms=1000,stream_rows=2,fixture_trace=trace)
                processes.append(process);replacements[name]=process
                ledger.record("consumer_witness_started",node=name,pid=process.child.pid,lease=lease,observed_ms=observed,files=files)
        address=free_address();successor=FleetProcess(binary,root,"successor","successor",address,address,False,store,fleet_durability=False,node_lease_ms=1000,stream_rows=2,fixture_trace=trace)
        processes.append(successor);ledger.record("consumer_successor_started",pid=successor.child.pid)
        # All replacements run before any readiness wait. Recovery can require
        # peer log service while those peers are resolving their own bindings.
        for name,process in replacements.items():
            origins[name]=process.ready();ledger.record("consumer_witness_ready",node=name,pid=process.child.pid,lease=store_json(root,store,"nodes/"+name+".json"))
        origins["consumer"]=successor.ready()
        if restart_followers:
            for attempt in range(2 if witness_data else 1):
                began=time.monotonic()
                try:
                    with framework_read(root,store,scope) as response:
                        (root/"unexpected-witness-success.json").write_text(json.dumps(dict(status=response.status,body=response.read(4097).decode(errors="replace"),owner=authority(root,store,scope),lease=store_json(root,store,"nodes/consumer.json"),objects=replica_inventory(root,store),gates={node:gate.observations() for node,gate in gates.items()}),indent=2))
                        raise AssertionError(f"consumer recovered without a complete witness: {response.status}")
                except urllib.error.HTTPError as error:
                    body=error.read(4097)
                    elapsed=int((time.monotonic()-began)*1000)
                    explicit=len(body)<=4096 and error.code==503 and b"Unavailable: event RPC did not return a proven result" in body
                    observation=dict(status=error.code,body=body.decode(errors="replace"),elapsed_ms=elapsed,observed_ms=time.time_ns()//1000000)
                    saved=root/f"witness-refusal-{attempt+1}.json"
                    saved.write_text(json.dumps(observation,indent=2))
                    if not explicit:
                        # Diagnostic only: retain the failed verdict before
                        # cleanup changes leases or closes the peer sockets.
                        try:observation.update(pid=successor.child.pid,exit_code=successor.child.poll(),successor_lease=store_json(root,store,"nodes/successor.json"),owner=authority(root,store,scope),gates={node:gate.observations() for node,gate in gates.items()})
                        except Exception as diagnostic:observation["diagnostic_error"]=type(diagnostic).__name__
                        saved.write_text(json.dumps(observation,indent=2))
                    require(explicit,"consumer refusal was not explicit Unavailable")
                    require(elapsed<10000,"request timeout cannot qualify witness refusal")
                    diagnostic=store_json(root,store,f"log/consumer/{log_lineage(proof)}/{proof['generation']}.e{proof['log']['epoch']}.loss.json") if witness_data and not witness_data.startswith("state-") else None
                    ledger.record("consumer_witness_refused",attempt=attempt+1,status=error.code,body=body.decode(),elapsed_ms=elapsed,owner=authority(root,store,scope),lease=store_json(root,store,"nodes/consumer.json"),objects=replica_inventory(root,store),gates={node:gate.observations() for node,gate in gates.items()},diagnostic=diagnostic)
                audit_evidence(ledger.path,complete=False)
            require(successor.child.poll() is None,"recoverer exited before real witness service returned")
            generation=store_json(root,store,"nodes/successor.json")["generation"]
            if witness_data:
                stopped_leases={}
                for name,process in replacements.items():
                    stopped_leases[name]=store_json(root,store,"nodes/"+name+".json")
                    process.stop()
                    reports=[json.loads(line) for line in process.log.read_text().splitlines() if line.startswith('{"') and json.loads(line).get("outcome")=="stopped"]
                    ledger.record("consumer_damaged_witness_joined",node=name,pid=process.child.pid,exit_code=process.child.returncode,reports=reports,lease=stopped_leases[name])
                ledger.record("consumer_witness_connections_retired",connections={node:gate.disconnect() for node,gate in gates.items()})
                restored=restore({node:follower_directory(root,node,proof) for node in retained},root,retained,witness_data,changed)
                ledger.record("consumer_witness_data_restored",files=restored)
                replacements={}
                for name in sorted(PEERS):
                    old=stopped_leases[name];observed=eventually(lambda:time.time_ns()//1000000,lambda now:now>old["expires_ms"])
                    address=gates[name].target
                    process=FleetProcess(binary,root,name,name+"-returned",address,gates[name].address,name=="destination",store,fleet_durability=False,node_lease_ms=1000,stream_rows=2,fixture_trace=trace)
                    processes.append(process);replacements[name]=process
                    ledger.record("consumer_returned_witness_started",node=name,pid=process.child.pid,observed_ms=observed,lease=old,files=inventory(follower_directory(root,name,proof)) if name in retained else [])
                for name,process in replacements.items():
                    origins[name]=process.ready();ledger.record("consumer_returned_witness_ready",node=name,pid=process.child.pid,lease=store_json(root,store,"nodes/"+name+".json"))
            for gate in gates.values():gate.release()
            ledger.record("consumer_witness_returned",pid=successor.child.pid,generation=generation)
        audit("recovered-before-replay")
        ledger.record("consumer_inbox_verified",result=read(origins["consumer"],"/projection/inbox-audit"),subscription=read(origins["destination"],"/subscription"))
        audit_evidence(ledger.path,complete=False)
        replay=read(origins["consumer"],f"/projection/fetch?fetch_id={FETCH_ID}","POST")
        ledger.record("consumer_fetch_replayed",result=replay,subscription=read(origins["destination"],"/subscription"))
        audit("after-replay")
        ledger.record("consumer_receipt_acknowledged",result=read(origins["consumer"],"/projection/ack-stored","POST"))
        audit("final")
        recovered_node=authority(root,store,scope)["node"]
        for process in reversed(processes):process.stop_recorded(ledger)
        for node,gate in gates.items():gate.close();ledger.record("consumer_witness_gate_joined",node=node)
        ledger.record("consumer_recovered_image",node=recovered_node,image=archive_consumer(root,recovered_node,scope,"recovered-consumer-image"))
        if store:store.close()
        summary=dict(audit_evidence(ledger.path),status="passed",binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),output=str(root),scope="consumer fleet-only effect/inbox proof; SIGKILL and local disk removal; original prefix audit before fetch/ack replay")
        (root/"summary.json").write_text(json.dumps(summary,indent=2)+"\n");print(json.dumps(summary),flush=True)
    finally:
        actions=[("release witness gate "+node,gate.release) for node,gate in gates.items()]
        actions.extend(("node "+process.node,process.stop) for process in reversed(processes))
        # Successful gates are already closed; close only live servers on error.
        actions.extend(("witness gate "+node,gate.close) for node,gate in gates.items() if gate.thread.is_alive())
        if store:actions.append(("private store",store.close))
        actions.append(("ledger",ledger.close))
        cleanup_all(actions)


if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__);target=parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--binary",type=Path);target.add_argument("--audit",type=Path)
    parser.add_argument("--output",type=Path);parser.add_argument("--rustfs-image");parser.add_argument("--restart-followers",action="store_true")
    parser.add_argument("--witness-data",choices=("missing","corrupt","oversize","state-missing","state-corrupt","state-oversize"))
    parser.add_argument("--trace",action="store_true",help="Capture selected lease/recovery diagnostics in the isolated fixture")
    args=parser.parse_args()
    if args.audit:print(json.dumps(audit_evidence(args.audit.resolve())),flush=True)
    else:exercise(args.binary.resolve(),(args.output or Path(tempfile.mkdtemp(prefix="cellar-consumer-recovery-parent-"))/"evidence").resolve(),args.rustfs_image,args.restart_followers,args.witness_data,args.trace)
