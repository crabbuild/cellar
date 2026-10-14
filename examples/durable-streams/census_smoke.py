#!/usr/bin/env python3
"""Real CLI census discovers accepted, undispatched and empty source cells."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile

from faults import Ledger, eventually, read, require
from fleet_faults import FleetProcess, NetworkStore, free_address


def exercise(cellar,binary,root,image):
    root.mkdir(parents=True,exist_ok=False)
    ledger=Ledger(root,commands=(("cross",725,1),("waiting",375,2)))
    store=NetworkStore(root,ledger,image);processes=[]
    try:
        store.start();origins={}
        for node in ("destination","producer"):
            address=free_address()
            process=FleetProcess(binary,root,node,node,address,address,node=="destination",store)
            processes.append(process);origins[node]=process.ready()
            if node=="destination":require(read(origins[node],"/__test/events")["records"]==[],"fresh stream not empty")
        ledger.issue(origins["producer"],"cross")
        eventually(lambda:read(origins["producer"],"/orders/cross"),lambda state:state["outbox"]["pending_rows"]=="0")
        ledger.issue(origins["producer"],"waiting")
        require(read(origins["producer"],"/orders/waiting")["outbox"]["pending_rows"]=="1","required retention did not block pending source")
        require(read(origins["producer"],"/orders/empty")["order"] is None,"empty source changed")
        env=dict(os.environ,AWS_ACCESS_KEY_ID=store.access,AWS_SECRET_ACCESS_KEY=store.secret,NO_COLOR="1")
        binding=root/"management-binding.json"
        binding.write_text(json.dumps(dict(environment="ADMIN_EVENTS",resource=dict(tenant="example",application="stream-example",
            id="00000000000000000000000000000100",name="orders",family="order-confirmed"),principal="stream-example",grant="management",
            publish=False,read=False,subscriptions=[],administer=True)))
        source_list=subprocess.run([str(cellar),"stream","sources","--binding",str(binding),"--bucket","s3://"+store.config["name"],
            "--endpoint",store.config["endpoint"],"--json"],env=env,capture_output=True,text=True,timeout=60)
        require(source_list.returncode==0,"destination source listing failed: "+source_list.stderr)
        destination_sources=json.loads(source_list.stdout)
        require(len(destination_sources["records"])==1 and destination_sources["records"][0]["source"]==ledger.operations["cross"]["ack"]["source"],
            "test did not contain a producer still unseen by destination inventory")
        def command(class_name="Order",cursor=None,limit=1,success=True):
            args=[str(cellar),"outbox","census",class_name,"--bucket","s3://"+store.config["name"],
                "--endpoint",store.config["endpoint"],"--limit",str(limit),"--json"]
            if cursor:args.extend(["--cursor",cursor])
            reply=subprocess.run(args,env=env,capture_output=True,text=True,timeout=60)
            require((reply.returncode==0)==success,"CLI census outcome differed: "+reply.stderr)
            return reply
        records={};cursor=None;pages=[]
        for _ in range(10):
            reply=command(cursor=cursor);page=json.loads(reply.stdout)
            require(page["format"]==1 and page["complete_probes"] and page["scanned_prefixes"]<=1 and len(page["records"])<=1,"census page exceeded bound or lost probes")
            pages.append(page)
            for row in page["records"]:
                require(row["source"] not in records and "lanes" not in row,"census duplicated source or leaked lane metadata")
                records[row["source"]]=row
            cursor=page["next"]
            if cursor is None:break
        require(cursor is None and len(records)==3,"bounded census missed a source")
        for key,pending in (("cross","0"),("waiting","1")):
            receipt=ledger.operations[key]["ack"];row=records[receipt["source"]]
            require(row["status"]=="source" and row["incarnation"]==receipt["incarnation"] and row["pending_rows"]==pending,"census lost accepted/undispatched source identity")
        require(sum(row["status"]=="empty" for row in records.values())==1,"empty source was not distinguished")
        require(len(pages)>1 and pages[0]["next"],"test did not exercise provider continuation")
        denied=command(class_name="Projection",cursor=pages[0]["next"],success=False)
        require("another fleet/class" in denied.stderr,"foreign-class cursor was accepted")
        def metrics(max_roots=10000,success=True,json_output=True):
            args=[str(cellar),"event","metrics","--bucket","s3://"+store.config["name"],
                "--endpoint",store.config["endpoint"],"--max-roots",str(max_roots)]
            if json_output:args.append("--json")
            reply=subprocess.run(args,env=env,capture_output=True,text=True,timeout=60)
            require((reply.returncode==0)==success,"CLI state metric outcome differed: "+reply.stderr)
            return reply
        state=json.loads(metrics().stdout)
        require(state["complete"] and state["directory_exhausted"] and state["unavailable"]==0,"fleet state collection incomplete")
        expected=dict(sources="2",outbox_rows="1",streams="1",stream_rows="1",active_leases="0",subscriptions="1",
            subscription_lag_records="1",subscription_retained_lag_records="1",protected_records="1")
        for key,value in expected.items():require(state["gauges"][key]==value,"fleet state gauge disagreed with original commands: "+key)
        require(int(state["gauges"]["outbox_bytes"])>0 and state["gauges"]["protected_bytes"]==state["gauges"]["stream_bytes"],"logical pending/protected accounting was lost")
        capped=json.loads(metrics(max_roots=1,success=False).stdout)
        require(not capped["complete"] and not capped["directory_exhausted"] and capped["observed"]==1 and capped["gauges"] is None,"bounded collection claimed complete fleet gauges")
        text=metrics(json_output=False).stdout
        require("cellar_event_fleet_outbox_rows 1" in text and "cellar_event_fleet_collection_complete 1" in text and "Order:" not in text and "resource=" not in text,"Prometheus gauges leaked identities or lost backlog")
        ledger.record("fleet_state_metrics_qualified",state=state,capped=capped,prometheus=text)
        def inventory(scope=None,page_kind=None,cursor=None,limit=1,success=True,bucket_suffix=""):
            args=[str(cellar),"event","inventory"]
            if scope:args.append(scope)
            args.extend(["--bucket","s3://"+store.config["name"]+bucket_suffix,"--endpoint",store.config["endpoint"],"--limit",str(limit),"--json"])
            if page_kind:args.extend(["--page",page_kind])
            if cursor:args.extend(["--cursor",cursor])
            reply=subprocess.run(args,env=env,capture_output=True,text=True,timeout=60)
            require((reply.returncode==0)==success,"CLI inventory outcome differed: "+reply.stderr)
            return reply
        discovered={};cursor=None;inventory_pages=[]
        for _ in range(12):
            page=json.loads(inventory(cursor=cursor).stdout)
            require(page["complete_probes"] and page["scanned_prefixes"]<=1 and len(page["records"])<=1,"inventory lost its bounded probes")
            inventory_pages.append(page)
            for row in page["records"]:
                require(row["status"]=="observed" and row["scope"] not in discovered,"inventory duplicated or lost a root")
                discovered[row["scope"]]=row["inventory"]["data"]
            cursor=page["next"]
            if cursor is None:break
        require(cursor is None and len(discovered)==5,"whole-fleet descriptor discovery missed a root")
        catalogs=[r for r in discovered.values() if r["kind"]=="catalog"]
        streams=[r for r in discovered.values() if r["kind"]=="stream"]
        require(len(catalogs)==1 and catalogs[0]["namespace"]==dict(tenant="example",application="stream-example"),"namespace discovery required an out-of-band namespace or changed identity")
        require(len(streams)==1 and streams[0]["resource"]["id"]=="00000000000000000000000000000100","stream descriptor identity changed")
        listed=json.loads(inventory(scope=catalogs[0]["scope"],page_kind="resources").stdout)
        require(listed["kind"]=="resources" and len(listed["data"]["records"])==1 and listed["data"]["records"][0]["resource"]==streams[0]["resource"],"resource inventory failed after namespace discovery")
        producers=json.loads(inventory(scope=streams[0]["scope"],page_kind="producers").stdout)
        require(not producers["data"]["retired"] and not producers["data"]["cleanup_complete"] and producers["data"]["page"]["records"]==destination_sources["records"],"platform producer inventory differed from accepted source audit")
        history=json.loads(inventory(scope=ledger.operations["waiting"]["ack"]["source"],page_kind="history").stdout)
        require(history["kind"]=="history" and history["data"]["records"]==[],"fresh source invented retained operation history")
        foreign=inventory(cursor=inventory_pages[0]["next"],success=False,bucket_suffix="/other")
        require("another fleet" in foreign.stderr,"fleet inventory cursor crossed bucket prefixes")
        ledger.record("fleet_inventory_qualified",pages=inventory_pages,resources=listed,producers=producers,history=history)
        # Stop every node but retain the bucket: failure must still print all
        # candidates, never claim zero pending rows or a complete census.
        for process in reversed(processes):process.stop()
        unavailable=command(limit=100,success=False)
        unavailable_page=json.loads(unavailable.stdout)
        require(not unavailable_page["complete_probes"] and len(unavailable_page["records"])==3
            and all(row["status"]=="unavailable" and "pending_rows" not in row for row in unavailable_page["records"]),
            "unavailable owners were silently reported empty")
        missing=json.loads(metrics(success=False).stdout)
        require(not missing["complete"] and missing["observed"]==0 and missing["unavailable"]>0 and missing["gauges"] is None,"unavailable owners became zero fleet gauges")
        missing_text=metrics(success=False,json_output=False).stdout
        require("cellar_event_fleet_collection_complete 0" in missing_text and "cellar_event_fleet_outbox_rows" not in missing_text,"incomplete Prometheus scrape published fake zero backlog")
        ledger.record("fleet_state_metrics_unavailable",state=missing,prometheus=missing_text)
        unavailable_inventory=json.loads(inventory(limit=100,success=False).stdout)
        require(not unavailable_inventory["complete_probes"] and len(unavailable_inventory["records"])==5
            and all(r["status"]=="unavailable" and "inventory" not in r for r in unavailable_inventory["records"]),"unavailable inventory roots became empty descriptors")
        ledger.record("fleet_inventory_unavailable",page=unavailable_inventory)
        ledger.record("census_qualified",pages=pages,unavailable=unavailable_page)
        summary=dict(status="passed",pages=len(pages),sources=2,undispatched_sources=1,empty_cells=1,unavailable_probes=3,
            binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),cellar_sha256=hashlib.sha256(cellar.read_bytes()).hexdigest(),output=str(root))
        (root/"summary.json").write_text(json.dumps(summary,indent=2)+"\n")
        print(json.dumps(summary),flush=True)
    finally:
        for process in reversed(processes):process.stop()
        store.close();ledger.close()


if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cellar",type=Path,required=True);parser.add_argument("--binary",type=Path,required=True)
    parser.add_argument("--rustfs-image",default="rustfs/rustfs:1.0.0-beta.8-glibc");parser.add_argument("--output",type=Path)
    args=parser.parse_args()
    exercise(args.cellar.resolve(),args.binary.resolve(),(args.output or Path(tempfile.mkdtemp(prefix="cellar-census-parent-"))/"evidence").resolve(),args.rustfs_image)
