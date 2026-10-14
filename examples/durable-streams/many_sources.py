#!/usr/bin/env python3
"""Independent cold-source recovery under bounded residency and required retention."""
import argparse
import hashlib
import json
from pathlib import Path
import tempfile
import time
import urllib.parse

from capacity import observe
from faults import Ledger, eventually, integer, read, require, validate_event, validate_source
from fleet_faults import FleetProcess, NetworkStore, PeerProxy, free_address
from many_sources_audit import audit


def cleanup(processes,proxy,store,ledger):
    failures=[]
    if proxy:proxy.outage=True;proxy.release.set()
    for process in reversed(processes):
        try:process.stop()
        except Exception as error:failures.append(error)
    for resource in (proxy,store,ledger):
        if resource:
            try:resource.close()
            except Exception as error:failures.append(error)
    if failures:raise failures[0]


def inspect_sources(ledger,origins,label):
    ledger.audited=False
    states={key:read(origins["producer"],"/orders/"+key) for key in ledger.operations}
    ledger.record("many_source_audit",label=label,sources=states)
    for key,state in states.items():
        pending=0 if label=="final" or "accepted" in ledger.operations[key] else 1
        require(state["host"] in origins and validate_source(ledger.operations[key],state)==pending,"original source obligation lost")
    require(len({state["receipt"]["source"] for state in states.values()})==len(states),"commands did not use independent sources")
    ledger.record("many_source_audit_passed",label=label)
    ledger.audited=True


def drain(ledger,origins,count,stream_rows,delay):
    cursor=None;accepted={key:op["accepted"] for key,op in ledger.operations.items() if "accepted" in op};checkpoint=0;deadline=time.monotonic()+1800
    def archive():
        nonlocal cursor
        page=read(origins["destination"],"/__test/events"+("?cursor="+urllib.parse.quote(cursor,safe="") if cursor else ""))
        records=page["records"];cursor=page["cursor"]
        require(len(records)<=stream_rows,"destination exceeded configured retention capacity")
        for record in records:
            key=record["order_id"]
            if key in accepted:
                require(checkpoint==0,"resumable read repeated acknowledged history")
                require(validate_event(ledger.operations[key],record)==accepted[key],"recovered acceptance changed")
                continue
            require(key in ledger.operations and key not in accepted and integer(record["offset"])==len(accepted)+1,"duplicate/foreign/noncontiguous acceptance")
            # Cross-source ordering is assigned by the destination, not the caller.
            operation=dict(ledger.operations[key],offset=len(accepted)+1)
            receipt=validate_event(operation,record)
            ledger.record("many_acceptance",key=key,record=record)
            ledger.operations[key]["offset"]=operation["offset"];ledger.operations[key]["accepted"]=receipt
            accepted[key]=receipt
        return records
    while checkpoint<count:
        require(time.monotonic()<deadline,"cold-source recovery did not drain under captured deadlines")
        archive()
        # Fill the protected window before retry/redelivery. Otherwise a retry
        # legitimately fetches newly appended records as well as the old batch.
        if len(accepted)-checkpoint<min(stream_rows,count-checkpoint):time.sleep(.1);continue
        before=read(origins["consumer"],"/__test/projection-receipts")
        ledger.record("many_effects_before",checkpoint=checkpoint,effects=before)
        require(len(before["receipts"])==checkpoint,"consumer changed effects before processing")
        time.sleep(delay)
        processed=read(origins["consumer"],"/projection/retry","POST")
        # Retry leaves the batch unacked, so its protected records remain available
        # for independent destination observation even if appends raced the fetch.
        archive()
        effects=read(origins["consumer"],"/__test/projection-receipts")
        fresh=len(effects["receipts"])-checkpoint
        require(0<fresh<=20 and processed["fresh_effects"]==fresh and processed["status"]=="retried","consumer failed to prove a fresh bounded batch")
        require(set(effects["receipts"]).issubset(accepted),"effect appeared before its acceptance was archived")
        ledger.record("many_effect_proven",checkpoint=checkpoint+fresh,effects=effects)
        replay=read(origins["consumer"],"/projection/process","POST")
        require(replay["fresh_effects"]==0 and replay["status"]=="acknowledged","redelivery repeated inbox effects")
        checkpoint+=fresh
        status=read(origins["destination"],"/subscription")
        require(integer(status["checkpoint"])==checkpoint and not status["observation"]["lease_active"],"consumer checkpoint skipped proof")
        ledger.record("many_checkpoint",checkpoint=checkpoint,subscription=status)
    require(len(accepted)==count,"consumer skipped independent sources")


def exercise(binary,root,count,stream_rows,soak_seconds,delay,image):
    require(1<=stream_rows<=20 and stream_rows<count<=192 and 0<=soak_seconds<=3600 and 0<=delay<=5,"invalid bounded many-source policy")
    root.mkdir(parents=True,exist_ok=False)
    ledger=Ledger(root,commands=tuple((f"cold-{index:04}",index+1,0) for index in range(count)))
    store=NetworkStore(root,ledger,image) if image else None
    processes=[];live={};origins={};peers={};proxy=None
    try:
        if store:store.start()
        destination=free_address();proxy=PeerProxy(destination,ledger)
        ledger.record("many_plan",count=count,stream_rows=stream_rows,soak_seconds=soak_seconds,consumer_delay=delay,resident_limit=128,cache_limit=256)
        for node in ("destination","producer","consumer"):
            peer=destination if node=="destination" else free_address()
            process=FleetProcess(binary,root,node,node+"-initial",peer,proxy.address if node=="destination" else peer,node=="destination",store,stream_rows=stream_rows)
            processes.append(process);live[node]=process;origins[node]=process.ready();peers[node]=f"{peer[0]}:{peer[1]}"
            if node=="destination":require(read(origins[node],"/__test/events")["records"]==[],"many-source destination not fresh")
        ledger.record("many_started",pids={node:process.child.pid for node,process in live.items()})
        observe(ledger,live,peers,"empty")
        for key in ledger.operations:ledger.issue(origins["producer"],key)
        full=eventually(lambda:read(origins["destination"],"/__test/events"),lambda page:len(page["records"])==stream_rows)["records"]
        for index,record in enumerate(full,1):
            key=record["order_id"];operation=ledger.operations[key];operation["offset"]=index
            receipt=validate_event(operation,record);operation["accepted"]=receipt
            ledger.record("many_acceptance",key=key,record=record)
            eventually(lambda key=key:read(origins["producer"],"/orders/"+key),lambda state:integer(state["outbox"]["pending_rows"])==0)
        inspect_sources(ledger,origins,"before-source-loss")
        end=time.monotonic()+soak_seconds
        while time.monotonic()<end:
            observe(ledger,live,peers,"offline-soak");time.sleep(min(1,max(0,end-time.monotonic())))
        for node in ("producer","consumer","destination"):
            live[node].crash(ledger,"many-cold-"+node)
        for node in ("destination","producer","consumer"):
            address=peers[node].rsplit(':',1);address=(address[0],int(address[1]))
            replacement=FleetProcess(binary,root,node,node+"-recovered",address,proxy.address if node=="destination" else address,node=="destination",store,stream_rows=stream_rows)
            processes.append(replacement);live[node]=replacement
        # Fleet-mode recovery may need a peer's retained log. Start every
        # replacement before waiting for any one node's readiness.
        for node in ("destination","producer","consumer"):
            origins[node]=live[node].ready()
            ledger.record("many_restarted",node=node,pid=live[node].child.pid)
        observe(ledger,live,peers,"cold-before-retry")
        # No /orders request occurs between restart and complete delivery.
        recovered=read(origins["destination"],"/__test/events")["records"]
        ledger.record("many_cold_boundary",source_requests=0,events=recovered)
        require(recovered==full,"cold recovery lost or changed protected accepted history")
        ledger.audited=True;proxy.resume("cold-before-retry")
        started=time.monotonic();drain(ledger,origins,count,stream_rows,delay)
        ledger.record("many_drain_finished",seconds=time.monotonic()-started)
        for key in ledger.operations:
            eventually(lambda key=key:read(origins["producer"],"/orders/"+key),lambda state:integer(state["outbox"]["pending_rows"])==0)
        inspect_sources(ledger,origins,"final")
        effects=read(origins["consumer"],"/__test/projection-receipts")
        ledger.record("many_final_effects",effects=effects)
        observe(ledger,live,peers,"drained")
        proxy.outage=True
        for process in reversed(processes):process.stop_recorded(ledger)
        proxy.close();proxy=None
        if store:store.close()
        summary=dict(audit(ledger.path),status="passed",binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),output=str(root),
            scope="independent source cells, all-process loss with surviving local state/store, autonomous wake recovery without source requests, required retention and inbox effects on one machine")
        (root/"summary.json").write_text(json.dumps(summary,indent=2)+"\n");print(json.dumps(summary),flush=True)
    finally:
        cleanup(processes,proxy,store,ledger)


if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    target=parser.add_mutually_exclusive_group(required=True);target.add_argument("--binary",type=Path);target.add_argument("--audit",type=Path)
    parser.add_argument("--output",type=Path);parser.add_argument("--count",type=int,default=144);parser.add_argument("--stream-rows",type=int,default=8)
    parser.add_argument("--soak-seconds",type=float,default=30);parser.add_argument("--consumer-delay",type=float,default=.1);parser.add_argument("--rustfs-image")
    args=parser.parse_args()
    if args.audit:print(json.dumps(audit(args.audit.resolve())))
    else:exercise(args.binary.resolve(),(args.output or Path(tempfile.mkdtemp(prefix="cellar-many-parent-"))/"evidence").resolve(),args.count,args.stream_rows,args.soak_seconds,args.consumer_delay,args.rustfs_image)
