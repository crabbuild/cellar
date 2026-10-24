#!/usr/bin/env python3
# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Bounded offline-source capacity and slow-consumer recovery qualification."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
import urllib.error
import urllib.parse

from faults import Ledger, digest, eventually, integer, read, require, validate_event
from fleet_faults import FleetProcess, NetworkStore, PeerProxy, free_address
from capacity_audit import audit as audit_evidence


def source_audit(ledger,origins,label,accepted_count):
    ledger.audited=False
    commands={key:read(origins["producer"],"/__test/backlog?command="+key) for key in ledger.operations}
    state=read(origins["producer"],"/__test/backlog")
    records=read(origins["destination"],"/__test/events")["records"]
    count=len(ledger.operations)
    require(state["host"]=="producer" and state["commands"]==count,"backlog source moved or lost business commands")
    require(len(records)<=1,"one-record stream capacity exceeded")
    if accepted_count==0:require(records==[],"destination accepted while its path was paused")
    page=state["outbox"];require(len(page["lanes"])==1,"backlog split its producer lane")
    revision=page["lanes"][0]["revision"]
    require(integer(revision["next_sequence"])==count+1 and integer(revision["settled_through"])==accepted_count,
        "producer sequences were lost/skipped by overflow or recovery")
    require(integer(page["pending_rows"])==count-accepted_count and integer(page["lanes"][0]["pending_rows"])==count-accepted_count,
        "backlog obligation accounting changed")
    for index,(key,operation) in enumerate(ledger.operations.items(),1):
        command=commands[key]
        require(command["order"]==operation["payload"] and command["receipt"]==operation["ack"],"acknowledged source command changed before replay")
        receipt=operation["ack"]
        require(integer(receipt["sequence"])==index and receipt["source"]==revision["source"]
            and receipt["incarnation"]==page["incarnation"]==revision["incarnation"] and receipt["lane"]==revision["lane"]
            and receipt["payload_sha256"]==operation["sha256"],"source identity or FIFO sequence changed")
    rejected=read(origins["producer"],"/__test/backlog?command=overflow")
    require(rejected["order"] is None and rejected["receipt"] is None and rejected["commands"]==count,"rejected emission committed partial business state")
    ledger.record("capacity_source_audit",label=label,operations=ledger.operations,commands=commands,state=state,events=records,rejected=rejected)
    ledger.record("capacity_source_audit_passed",label=label)
    ledger.audited=True


def observe(ledger,processes,peers,label):
    nodes={}
    for node,process in processes.items():
        require(process.child.poll() is None,"capacity node exited before observation")
        state=read("http://"+peers[node],"/state")
        require(type(state["occupied"]) is int and state["occupied"]<=128 and state["cached_routes"]<=256,"captured residency/cache limits exceeded")
        rss=int(subprocess.check_output(["ps","-p",str(process.child.pid),"-o","rss="],text=True).strip())*1024
        require(rss>0,"live process RSS observation unavailable")
        files=logical=allocated=0;families={"sqlite":0,"wal":0,"ltx":0,"other":0}
        root=process.log.parent/"data"/node
        for file in root.rglob("*"):
            try:
                if not file.is_file():continue
                size=file.stat();files+=1;require(files<=10000,"fixture file census exceeded its measurement bound")
                logical+=size.st_size;allocated+=size.st_blocks*512
                family="wal" if file.name.endswith("-wal") else "sqlite" if file.suffix in (".sqlite",".db") else "ltx" if file.suffix==".ltx" else "other"
                families[family]+=size.st_size
            except FileNotFoundError:continue # live filesystem observation, not a snapshot
        nodes[node]=dict(pid=process.child.pid,rss_bytes=rss,state=state,files=files,logical_file_bytes=logical,
            allocated_file_bytes=allocated,family_bytes=families)
    ledger.record("capacity_observation",label=label,nodes=nodes)
    return nodes


def drain(ledger,origins,consumer_delay):
    known={};latencies=[];deadline=time.monotonic()+600;cursor=None
    while len(known)<len(ledger.operations):
        require(time.monotonic()<deadline,"backlog failed to drain under original retry/lease policy")
        page=read(origins["destination"],"/__test/events"+("?cursor="+urllib.parse.quote(cursor,safe="") if cursor else ""))
        events=page["records"]
        cursor=page["cursor"]
        if not events:time.sleep(.1);continue
        require(len(events)==1,"required stream exceeded its configured row capacity")
        record=events[0];key=record["order_id"]
        require(key in ledger.operations and key not in known and integer(record["offset"])==len(known)+1,"duplicate, foreign or skipped acceptance")
        receipt=validate_event(ledger.operations[key],record)
        ledger.record("capacity_acceptance",key=key,record=record)
        ledger.operations[key]["accepted"]=receipt
        eventually(lambda:read(origins["producer"],"/__test/backlog"),lambda state:integer(state["outbox"]["lanes"][0]["revision"]["settled_through"])>=len(known)+1)
        before=read(origins["consumer"],"/__test/projection-receipts")
        require(set(before["receipts"])==set(known),"consumer changed effects before replay")
        ledger.record("capacity_effects_before",key=key,effects=before)
        time.sleep(consumer_delay)
        started=time.monotonic();first=read(origins["consumer"],"/projection/retry","POST")
        require(first["fresh_effects"]==1 and first["status"]=="retried","slow consumer missed or repeated first effect")
        proven=read(origins["consumer"],"/__test/projection-receipts")
        require(set(proven["receipts"])==set(known)|{key} and proven["receipts"][key]==receipt,"consumer proof changed original acceptance")
        ledger.record("capacity_effect_proven",key=key,effects=proven)
        repeat=read(origins["consumer"],"/projection/process","POST")
        require(repeat["fresh_effects"]==0 and repeat["status"]=="acknowledged","inbox redelivery repeated an effect")
        known[key]=receipt
        status=read(origins["destination"],"/subscription")
        require(integer(status["checkpoint"])==len(known) and not status["observation"]["lease_active"],"checkpoint skipped work or retained an acknowledged lease")
        ledger.record("capacity_checkpoint",key=key,checkpoint=len(known),subscription=status)
        latencies.append(time.monotonic()-started)
    return latencies


def exercise(binary,root,count,soak_seconds,consumer_delay,image=None):
    require(os.name=="posix" and 1<=count<=256 and 0<=soak_seconds<=3600 and 0<=consumer_delay<=5,"invalid bounded capacity qualification settings")
    root.mkdir(parents=True,exist_ok=False)
    ledger=Ledger(root,commands=tuple((f"item-{i:04}",i+1,i+1) for i in range(count)))
    store=NetworkStore(root,ledger,image) if image else None
    processes=[];live={};origins={};peers={};proxy=None
    try:
        if store:store.start()
        destination_peer=free_address();proxy=PeerProxy(destination_peer,ledger)
        proxy.paused=True
        ledger.record("capacity_plan",count=count,soak_seconds=soak_seconds,consumer_delay=consumer_delay,outbox_rows=count,stream_rows=1)
        for node in ("destination","producer","consumer"):
            peer=destination_peer if node=="destination" else free_address()
            process=FleetProcess(binary,root,node,node+"-initial",peer,proxy.address if node=="destination" else peer,node=="destination",store,outbox_rows=count)
            processes.append(process);live[node]=process;origins[node]=process.ready();peers[node]=f"{peer[0]}:{peer[1]}"
            if node=="destination":require(read(origins[node],"/__test/events")["records"]==[],"capacity store not fresh")
        ledger.record("capacity_started",pids={node:process.child.pid for node,process in live.items()})
        observe(ledger,live,peers,"empty")
        timings=[]
        for key,operation in ledger.operations.items():
            started=time.monotonic();operation["attempts"]+=1
            ledger.record("attempt",key=key,replay=False,attempt=operation["attempts"])
            reply=read(origins["producer"],f"/__test/backlog?command={key}&amount_cents={operation['payload']['amount_cents']}","POST")
            ledger.acknowledge(key,reply);timings.append(time.monotonic()-started)
        try:
            read(origins["producer"],"/__test/backlog?command=overflow&amount_cents=999","POST")
            require(False,"source exceeded captured outbox row quota")
        except Exception as error:
            require("HTTP 429" in str(error) and "OutboxFull" in str(error),"overflow did not return typed capacity rejection")
            ledger.record("capacity_rejected",command="overflow",error="OutboxFull")
        source_audit(ledger,origins,"full-before-soak",0)
        end=time.monotonic()+soak_seconds
        while time.monotonic()<end:
            observe(ledger,live,peers,"offline-soak")
            time.sleep(min(1,max(0,end-time.monotonic())))
        source_audit(ledger,origins,"before-source-loss",0)
        live["producer"].crash(ledger,"capacity-producer-pending")
        address=tuple(peers["producer"].rsplit(':',1));address=(address[0],int(address[1]))
        replacement=FleetProcess(binary,root,"producer","producer-recovered",address,address,False,store,outbox_rows=count)
        processes.append(replacement);live["producer"]=replacement;origins["producer"]=replacement.ready()
        ledger.record("capacity_producer_restarted",pid=replacement.child.pid)
        source_audit(ledger,origins,"recovered-before-retry",0)
        observe(ledger,live,peers,"recovered-full")
        proxy.resume("recovered-before-retry")
        consumer_times=drain(ledger,origins,consumer_delay)
        source_audit(ledger,origins,"final",count)
        final=read(origins["consumer"],"/__test/projection-receipts")
        require(len(final["receipts"])==count and final["projection"]==dict(orders=count,total_cents=count*(count+1)//2),"final projection differs from acknowledged commands")
        ledger.record("capacity_final_effects",effects=final)
        observe(ledger,live,peers,"drained")
        proxy.outage=True
        for process in reversed(processes):process.stop_recorded(ledger)
        proxy.close();proxy=None
        if store:store.close()
        def percentiles(values):
            ordered=sorted(values)
            return {str(p):ordered[min(len(ordered)-1,int((len(ordered)-1)*p/100))] for p in (50,95,99)}
        summary=dict(audit_evidence(ledger.path),status="passed",count=count,soak_seconds=soak_seconds,consumer_delay=consumer_delay,
            producer_response_seconds=percentiles(timings),consumer_redelivery_ack_seconds=percentiles(consumer_times),
            binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),backend="s3" if store else "sqlite",output=str(root),
            scope="one source/lane at configured row capacity; offline append path, source SIGKILL, pre-retry audit and slow required consumer; local file/RSS observations")
        (root/"summary.json").write_text(json.dumps(summary,indent=2)+"\n");print(json.dumps(summary),flush=True)
    finally:
        if proxy:proxy.outage=True;proxy.release.set()
        for process in reversed(processes):process.stop()
        if proxy:proxy.close()
        if store:store.close()
        ledger.close()


if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    target=parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--binary",type=Path);target.add_argument("--audit",type=Path)
    parser.add_argument("--output",type=Path)
    parser.add_argument("--count",type=int,default=64);parser.add_argument("--soak-seconds",type=float,default=30)
    parser.add_argument("--consumer-delay",type=float,default=.1);parser.add_argument("--rustfs-image")
    args=parser.parse_args()
    if args.audit:print(json.dumps(audit_evidence(args.audit.resolve())),flush=True)
    else:exercise(args.binary.resolve(),(args.output or Path(tempfile.mkdtemp(prefix="cellar-capacity-parent-"))/"evidence").resolve(),args.count,args.soak_seconds,args.consumer_delay,args.rustfs_image)
