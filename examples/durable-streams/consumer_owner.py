#!/usr/bin/env python3
# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Keep a consumer callback entered through independent-process fenced takeover."""
import argparse
import concurrent.futures
import hashlib
import http.client
import json
import os
from pathlib import Path
import shutil
import tempfile
import time

from consumer_owner_audit import FETCH_ID, audit_evidence, inspect_image
from faults import AuditError, Ledger, eventually, read, require
from fleet_faults import FleetProcess, NetworkStore, free_address
from live_owner import live, marker, phase
from resumed_owner import authority, store_json


def archive_consumer(root, scope, node="consumer"):
    require(node in ("consumer", "successor"), "invalid consumer image node")
    require(scope.startswith("Projection:") and len(scope) == 75 and all(c in "0123456789abcdef" for c in scope[11:]), "unsafe consumer scope")
    candidates = list((root / f"data/{node}/node" / scope / "ltx").glob("e*/db.sqlite"))
    require(len(candidates) == 1, "old consumer epoch inventory changed")
    archive = root / ("old-consumer-image" if node == "consumer" else "successor-consumer-image"); archive.mkdir()
    for suffix in ("", "-wal", "-shm"):
        source = Path(str(candidates[0]) + suffix)
        if source.exists():
            target = archive / ("db.sqlite" + suffix)
            shutil.copy2(source, target)
            with target.open("rb") as file: os.fsync(file.fileno())
    return inspect_image(archive, scope)


def exercise(binary, root, rustfs_image=None, cut="before_accept"):
    require(os.name == "posix", "consumer takeover requires POSIX process observation")
    root.mkdir(parents=True, exist_ok=False)
    ledger = Ledger(root)
    store = NetworkStore(root, ledger, rustfs_image) if rustfs_image else None
    processes, origins, consumer = [], {}, None
    data = root / "data/consumer"
    try:
        if store: store.start()
        for name in ("destination", "producer", "consumer"):
            address = free_address()
            process = FleetProcess(binary, root, name, name, address, address, name == "destination", store,
                                   fleet_durability=False, node_lease_ms=1000, owner_fault=name == "consumer")
            processes.append(process); origins[name] = process.ready()
            if name == "destination": require(read(origins[name], "/__test/events")["records"] == [], "nonempty fixture stream")
        consumer = processes[2]
        ledger.record("consumer_owner_started", pids={p.node:p.child.pid for p in processes},cut=cut,proof_mode="bucket",node_lease_ms=1000,handler_budget_ms=10000)
        ledger.issue(origins["producer"], "a")
        eventually(lambda: read(origins["producer"], "/orders/a"), lambda v:v["outbox"]["pending_rows"] == "0")
        ledger.record("consumer_owner_seeded", result=read(origins["consumer"], "/projection/process", "POST"))
        ledger.issue(origins["producer"], "b")
        eventually(lambda: read(origins["producer"], "/orders/b"), lambda v:v["outbox"]["pending_rows"] == "0")
        def audit(label):
            snapshot = dict(sources={key:read(origins["producer"],f"/orders/{key}") for key in ledger.operations},events=read(origins["destination"],"/__test/events"),
                            effects=read(origins["consumer"],"/projection/owner-audit"),subscription=read(origins["destination"],"/subscription"))
            scope = snapshot["effects"]["scope"]
            ledger.record("consumer_owner_audit",label=label,owner=authority(root,store,scope),operations=ledger.operations,snapshot=snapshot)
            audit_evidence(ledger.path,complete=False)
        audit("before-isolation")
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            began = time.monotonic()
            original = pool.submit(read,origins["consumer"],f"/projection/running?cut={cut}&fetch_id={FETCH_ID}","POST",20)
            entered = eventually(lambda:phase(data),lambda v:v.get("boundary") == "consumer_poll_entered",timeout=5)
            scope = entered["scope"]
            ledger.record("consumer_owner_entered",phase=entered,subscription=read(origins["destination"],"/subscription"),node_lease=store_json(root,store,"nodes/consumer.json"))
            marker(data / "owner-isolate")
            ledger.record("consumer_owner_isolated",pid=consumer.child.pid,state=live(consumer))
            def expired():
                path = data / "owner-refused.jsonl"; raw = path.read_bytes() if path.exists() else b""
                refused = [json.loads(line) for line in raw[:raw.rfind(b"\n")+1].splitlines()]
                return dict(lease=store_json(root,store,"nodes/consumer.json"),observed_ms=time.time_ns()//1000000,refused=refused)
            observed = eventually(expired,lambda v:v["observed_ms"]>v["lease"]["expires_ms"] and v["refused"],timeout=5)
            ledger.record("consumer_owner_expired",**observed)
            address = free_address()
            successor = FleetProcess(binary,root,"successor","successor",address,address,False,store,fleet_durability=False,node_lease_ms=1000)
            processes.append(successor); ledger.record("consumer_owner_successor",pid=successor.child.pid)
            origins["consumer"] = successor.ready()
            audit("before-release")
            require(phase(data) == entered, "old callback ended before original prefix audit")
            ledger.record("consumer_owner_released",pid=consumer.child.pid,state=live(consumer),phase=phase(data),elapsed_ms=int((time.monotonic()-began)*1000))
            audit_evidence(ledger.path,complete=False)
            marker(data / "owner-release",b"release")
            attempted = eventually(lambda:phase(data),lambda v:v.get("boundary") == "consumer_poll_attempted",timeout=5)
            ledger.record("consumer_owner_attempted",phase=attempted)
            code = consumer.child.wait(timeout=10); consumer.output.close()
            reports = [json.loads(line) for line in consumer.log.read_text().splitlines() if line.startswith('{"')]
            fencing = [r for r in reports if r.get("outcome") == "fenced"]
            require(len(fencing) == 1, "consumer lacks a fencing report")
            ledger.record("consumer_owner_fenced",pid=consumer.child.pid,exit_code=code,report=fencing[0],graceful_reports=sum(r.get("outcome") == "stopped" for r in reports))
            try: result = original.result(timeout=5)
            except concurrent.futures.TimeoutError as error: raise AuditError("old consumer request did not terminate") from error
            except (AuditError,OSError,http.client.HTTPException) as error:
                kind = "AuditError" if isinstance(error,AuditError) else "OSError" if isinstance(error,OSError) else "HTTPException"
                ledger.record("consumer_owner_interrupted",kind=kind,error_type=type(error).__name__)
            else: raise AuditError(f"fenced consumer returned success: {result}")
        ledger.record("consumer_owner_image",image=archive_consumer(root,scope))
        audit("after-refusal")
        for label in ("applied","replayed"):
            ledger.record("consumer_owner_fetch",label=label,result=read(origins["consumer"],f"/projection/fetch?fetch_id={FETCH_ID}","POST"))
            audit_evidence(ledger.path,complete=False)
        ledger.record("consumer_owner_ack",result=read(origins["consumer"],"/projection/ack-stored","POST"))
        audit("final")
        for process in reversed(processes): process.stop_recorded(ledger)
        ledger.record("consumer_owner_successor_image", image=archive_consumer(root,scope,"successor"))
        if store: store.close()
        summary = dict(audit_evidence(ledger.path),status="passed",binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),output=str(root),
                       scope="four independent loopback processes; live consumer poll across client isolation, expiry and takeover; original prefix audited before release")
        (root/"summary.json").write_text(json.dumps(summary,indent=2)+"\n");print(json.dumps(summary),flush=True)
    finally:
        if consumer is not None and consumer.child.poll() is None and not (data/"owner-release").exists(): marker(data/"owner-release",b"release")
        for process in reversed(processes): process.stop()
        if store: store.close()
        ledger.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--binary",type=Path);target.add_argument("--audit",type=Path)
    parser.add_argument("--output",type=Path);parser.add_argument("--rustfs-image")
    parser.add_argument("--cut",choices=("before_accept","before_commit"),default="before_accept")
    args = parser.parse_args()
    if args.audit: print(json.dumps(audit_evidence(args.audit.resolve())),flush=True)
    else: exercise(args.binary.resolve(),(args.output or Path(tempfile.mkdtemp(prefix="cellar-consumer-owner-parent-"))/"evidence").resolve(),args.rustfs_image,args.cut)
