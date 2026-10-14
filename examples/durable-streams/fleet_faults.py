#!/usr/bin/env python3
"""Independent-process destination loss after proven append, audited before retry."""
import argparse
import base64
import hashlib
import json
import os
import secrets
from pathlib import Path
import socket
import socketserver
import subprocess
import tempfile
import threading
import time
import urllib.request
import urllib.error

from faults import AuditError, Ledger, Process, eventually, read, require, validate_snapshot


def free_address():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()


def exact(stream, size):
    data = stream.read(size)
    require(len(data) == size, "incomplete peer body")
    return data


def message(stream, response=False):
    head = bytearray()
    while not head.endswith(b"\r\n\r\n"):
        line = stream.readline(65537)
        if not line and not head:
            return None
        require(line and len(head)+len(line) <= 65536, "incomplete/oversized peer head")
        head.extend(line)
    lines = bytes(head).split(b"\r\n")
    headers = {}
    for line in lines[1:]:
        if line:
            key, value = line.split(b":", 1)
            headers[key.lower()] = value.strip()
    wire, body = bytearray(), bytearray()
    if headers.get(b"transfer-encoding") == b"chunked":
        while True:
            line = stream.readline(129)
            require(line.endswith(b"\r\n") and len(line)<=128, "invalid chunk head")
            size = int(line.strip().split(b";", 1)[0], 16)
            require(size>=0 and len(body)+size <= 16<<20, "oversized peer body")
            wire.extend(line)
            if size == 0:
                while True:
                    trailer = stream.readline(65537)
                    require(trailer.endswith(b"\r\n") and len(wire)+len(trailer) <= (16<<20)+65536, "invalid chunk trailer")
                    wire.extend(trailer)
                    if trailer == b"\r\n":
                        break
                break
            chunk = exact(stream, size+2)
            require(chunk.endswith(b"\r\n"), "invalid chunk body")
            body.extend(chunk[:-2]); wire.extend(chunk)
    elif b"content-length" in headers:
        size = int(headers[b"content-length"])
        require(0<=size<=16<<20, "oversized peer body")
        body.extend(exact(stream, size)); wire.extend(body)
    else:
        require(not response or lines[0].split()[1] in (b"101", b"204", b"304"), "unframed peer response")
    return dict(wire=bytes(head)+bytes(wire), body=bytes(body), headers=headers, first=lines[0])


class PeerProxy:
    def __init__(self, target, ledger, method="__streamAppend"):
        require(method in ("__streamAppend","__streamAck"),"unsupported peer fault method")
        self.method=method
        self.label="append" if method=="__streamAppend" else "ack"
        self.target, self.ledger = target, ledger
        self.lock = threading.Lock()
        self.armed = False
        self.paused = False
        self.capture = None
        self.entered, self.release = threading.Event(), threading.Event()
        self.errors = []
        self.outage = False
        proxy = self
        class Handler(socketserver.BaseRequestHandler):
            def handle(self):
                upstream = None
                try:
                    client = self.request.makefile("rb")
                    while (request := message(client)) is not None:
                        method = request["headers"].get(b"x-cells-rpc-method", b"").decode()
                        targeted = method == proxy.method
                        with proxy.lock:
                            blocked = targeted and proxy.paused
                        if blocked:
                            proxy.ledger.record("retry_blocked", request_sha256=hashlib.sha256(request["body"]).hexdigest())
                            return
                        if upstream is None:
                            upstream = socket.create_connection(proxy.target, timeout=10)
                            upstream.settimeout(None)
                            owner = upstream.makefile("rb")
                        upstream.sendall(request["wire"])
                        reply = message(owner, response=True)
                        if reply is None:
                            return
                        if targeted:
                            require(reply["first"].split()[1] == b"200", "targeted RPC did not return a successful complete HTTP reply")
                            exchange = dict(method=method, scope=request["headers"][b"x-cells-scope"].decode(),
                                request_sha256=hashlib.sha256(request["body"]).hexdigest(), response_sha256=hashlib.sha256(reply["body"]).hexdigest(),
                                request=base64.b64encode(request["body"]).decode(), response=base64.b64encode(reply["body"]).decode())
                            with proxy.lock:
                                dropped = proxy.armed
                                if dropped:
                                    proxy.armed, proxy.paused = False, True
                                    proxy.capture = exchange
                            if dropped:
                                proxy.ledger.record(proxy.label+"_reply_held", exchange=exchange)
                                proxy.entered.set()
                                require(proxy.release.wait(20), "measuring client did not release the original proof boundary")
                                proxy.ledger.record(proxy.label+"_reply_lost", request_sha256=exchange["request_sha256"])
                                self.request.shutdown(socket.SHUT_RDWR)
                                return
                            proxy.ledger.record(proxy.label+"_reply_forwarded", exchange=exchange)
                        self.request.sendall(reply["wire"])
                except (ConnectionResetError, BrokenPipeError):
                    # A real node can close its pooled tunnel during retirement.
                    pass
                except ConnectionRefusedError as error:
                    with proxy.lock:
                        expected = proxy.paused or proxy.outage
                        if not expected:proxy.errors.append(repr(error))
                    proxy.ledger.record("peer_unavailable", expected=expected, error=type(error).__name__)
                except Exception as error:
                    with proxy.lock:
                        proxy.errors.append(repr(error))
                finally:
                    if upstream is not None:
                        upstream.close()
        class Server(socketserver.ThreadingTCPServer):
            allow_reuse_address = True
        self.server = Server(("127.0.0.1", 0), Handler)
        self.address = self.server.server_address
        self.thread = threading.Thread(target=self.server.serve_forever)
        self.thread.start()

    def arm(self):
        with self.lock:
            self.armed = True

    def resume(self, label):
        require(self.ledger.audited, "automatic retry released before successful recovery audit")
        self.ledger.record("retry_resumed", label=label)
        with self.lock:
            self.paused = False

    def close(self):
        self.release.set()
        self.server.shutdown(); self.server.server_close()
        self.thread.join(timeout=5)
        require(not self.thread.is_alive() and not self.errors, f"peer proxy did not finish cleanly: {self.errors}")


class FleetProcess(Process):
    def __init__(self, binary, root, node, label, peer, advertise, declare, store=None, fleet_durability=True):
        self.node = node
        data = root / "data" / node
        data.mkdir(parents=True, exist_ok=True)
        config = root / f"{label}-config.json"
        config.write_text(json.dumps(dict(node=node, bucket=store.config if store else str(root/"fleet.sqlite"), peer=f"{peer[0]}:{peer[1]}",
            advertise=f"{advertise[0]}:{advertise[1]}", declare=declare, fleet_durability=fleet_durability)))
        self.log = root/f"{label}.log"
        self.output = self.log.open("xb")
        self.child = subprocess.Popen([str(binary),str(data),"0","--fleet-fixture",str(config)],
            stdout=self.output, stderr=subprocess.STDOUT, env=dict(os.environ,NO_COLOR="1", **(store.credentials if store else {})))


    def stop_recorded(self, ledger):
        live = self.child.poll() is None
        self.stop()
        if live:
            ledger.record("fleet_process_stopped", node=self.node, pid=self.child.pid, exit_code=self.child.returncode,
                reports=[json.loads(line) for line in self.log.read_text().splitlines() if line.startswith('{"') and json.loads(line).get("outcome")=="stopped"])


def audit(ledger, origins, label, pending, effects, checkpoint):
    ledger.audited = False
    snapshot = dict(sources={"cross":read(origins["producer"],"/orders/cross")},
        events=read(origins["destination"],"/__test/events"), effects=read(origins["consumer"],"/__test/projection-receipts"),
        subscription=read(origins["destination"],"/subscription"))
    require(snapshot["sources"]["cross"]["host"]=="producer" and snapshot["effects"]["host"]=="consumer", "source/effects were not independently hosted")
    accepted = validate_snapshot(ledger.operations, snapshot, effects, checkpoint, {"cross":pending})
    # Before consumption, an already proven acceptance may not be repaired by retry.
    if checkpoint == 0:
        require(len(snapshot["events"]["records"])==1, "proven destination acceptance disappeared before retry")
    ledger.record("fleet_audit", label=label, operations=ledger.operations, snapshot=snapshot)
    for key, receipt in accepted.items():
        ledger.operations[key]["accepted"] = receipt
    ledger.record("fleet_audit_passed", label=label)
    ledger.audited = True
    return snapshot


class NetworkStore:
    """One owned ephemeral Docker server; never uses an existing bucket or store."""
    def __init__(self, root, ledger, image):
        self.name="cellar-stream-store-"+secrets.token_hex(8)
        self.started=False
        self.root, self.ledger, self.image=root, ledger, image
        self.access, self.secret="stream-"+secrets.token_hex(8), secrets.token_hex(24)
        self.credentials=dict(CELLAR_STREAM_FIXTURE_ACCESS=self.access,CELLAR_STREAM_FIXTURE_SECRET=self.secret)

    def start(self):
        # Pass credentials via the environment, never shell interpolation or
        # command diagnostics. The container's own writable layer is discarded.
        result=subprocess.run(["docker","run","-d","--name",self.name,
            "--publish","127.0.0.1::9000","--cpus","1","--memory","2g",
            "-e","RUSTFS_ACCESS_KEY","-e","RUSTFS_SECRET_KEY",
            "-e","RUSTFS_CONSOLE_ENABLE=false","-e","RUSTFS_OBS_LOGGER_LEVEL=error",self.image,"/data"],
            env=dict(os.environ,RUSTFS_ACCESS_KEY=self.access,RUSTFS_SECRET_KEY=self.secret),capture_output=True,text=True)
        require(result.returncode==0,"private RustFS server could not start")
        self.started=True
        info=json.loads(subprocess.check_output(["docker","inspect",self.name]))[0]
        port=info["NetworkSettings"]["Ports"]["9000/tcp"][0]["HostPort"]
        endpoint="http://127.0.0.1:"+port
        deadline=time.monotonic()+90
        while True:
            try:
                with urllib.request.urlopen(endpoint+"/health",timeout=2) as response:
                    require(response.status==200,"RustFS health failed")
                break
            except (OSError, AuditError):
                require(time.monotonic()<deadline,"private RustFS readiness timed out")
                time.sleep(.2)
        # Reuse the established stdlib SigV4 creation helper; no SDK dependency.
        import importlib.util
        spec=importlib.util.spec_from_file_location("cellar_perf_store",Path(__file__).resolve().parents[2]/"tools/perf/run.py")
        helper=importlib.util.module_from_spec(spec);spec.loader.exec_module(helper)
        bucket="stream-"+secrets.token_hex(8)
        deadline=time.monotonic()+60
        while True:
            try:
                require(helper.create_bucket(endpoint,bucket,self.access,self.secret)==200,"private bucket creation failed")
                break
            except urllib.error.HTTPError as error:
                require(error.code==503 and time.monotonic()<deadline,"private bucket creation failed after readiness")
                self.ledger.record("network_store_startup_retry",status=error.code)
                time.sleep(.2)
        self.config=dict(endpoint=endpoint,name=bucket)
        self.ledger.record("network_store_started",backend="s3",container=self.name,image_id=info["Image"],bucket=bucket,endpoint=endpoint)

    def close(self):
        if self.started:
            logs=subprocess.run(["docker","logs",self.name],capture_output=True,text=True)
            safe=(logs.stdout+logs.stderr).replace(self.access,"[redacted]").replace(self.secret,"[redacted]")
            (self.root/"rustfs.log").write_text(safe)
            result=subprocess.run(["docker","rm","-f",self.name],capture_output=True)
            require(result.returncode==0,"private RustFS cleanup failed")
            self.started=False
            self.ledger.record("network_store_removed",container=self.name)


def audit_evidence(path):
    operations, labels, passed = None, {}, set()
    held, kills, resumed, forwarded, drops = None, 0, False, 0, 0
    known_effects = set()
    pids, restarted, stopped = None, None, set()
    store, removed = None, False
    for line in path.read_text().splitlines():
        row = json.loads(line); event=row["event"]
        if event=="network_store_started":
            require(store is None and pids is None and row["backend"]=="s3" and row["image_id"].startswith("sha256:")
                and row["container"].startswith("cellar-stream-store-") and row["endpoint"].startswith("http://127.0.0.1:"),"invalid private network store evidence")
            store=row
        elif event=="network_store_removed":
            require(store is not None and not removed and stopped=={"producer","destination","consumer"}
                and row["container"]==store["container"],"network store removed before complete fleet shutdown")
            removed=True
        elif event=="plan":
            require(operations is None, "multiple fleet plans")
            operations=row["operations"]
        elif event=="attempt":
            key=row["key"];require(key in operations and row["attempt"]==operations[key]["attempts"]+1, "unissued or reordered source command")
            require(not row["replay"] or "recovered-before-retry" in passed, "source replay concealed recovery")
            operations[key]["attempts"]=row["attempt"]
        elif event in ("ack","upstream_ack"):
            key=row["key"];require(key in operations and operations[key]["attempts"]>0, "unissued source acknowledgment")
            prior=operations[key].get("ack") or operations[key].get("upstream_ack")
            require(prior is None or prior==row["receipt"], "source acknowledgment changed")
            operations[key]["upstream_ack" if event=="upstream_ack" else "ack"]=row["receipt"]
        elif event=="fleet_started":
            require(pids is None, "multiple fleet initializations")
            pids=row["pids"]
            require(set(pids)=={"producer","destination","consumer"} and len(set(pids.values()))==3, "fixture processes were not independent")
        elif event=="destination_restarted":
            require(restarted is None and kills==1 and not resumed and row["pid"] not in pids.values(), "restart did not replace the killed destination")
            restarted=row["pid"]
        elif event=="fleet_process_stopped":
            require("final" in passed and row["node"] not in stopped and row["exit_code"]==0 and len(row["reports"])==1
                and all(row["reports"][0].get(key) is True for key in ("ingress_drained","application_work_drained","cells_drained")), "incomplete graceful fleet shutdown")
            expected_pid=restarted if row["node"]=="destination" else pids.get(row["node"])
            require(row["pid"]==expected_pid, "shutdown report names another process")
            stopped.add(row["node"])
        elif event=="append_reply_held":
            require(held is None, "multiple held append boundaries")
            held=row["exchange"]
            require(held["method"]=="__streamAppend" and held["scope"].startswith("__EventStream:")
                and len(held["scope"].split(":",1)[1])==64, "held reply names another method or resource class")
            for field in ("request","response"):
                require(hashlib.sha256(base64.b64decode(held[field],validate=True)).hexdigest()==held[field+"_sha256"], "captured peer bytes changed")
        elif event=="fleet_audit":
            label=row["label"]
            require(label not in labels, "repeated audit stage")
            expected={"before-destination-loss":(1,[],0),"recovered-before-retry":(1,[],0),"accepted-after-retry":(0,[],0),"final":(0,["cross"],1)}
            snapshot=row["snapshot"]
            if label=="before-destination-loss":require(held is not None and kills==0 and not resumed, "pre-kill audit did not precede process loss")
            elif label=="recovered-before-retry":require(kills==1 and drops==1 and restarted is not None and not resumed, "recovery audit did not precede retry")
            elif label in ("accepted-after-retry","final"):
                require(resumed and forwarded>=1 and "recovered-before-retry" in passed, "post-retry audit preceded proven recovery")
                if label=="final":require("accepted-after-retry" in passed, "final audit preceded append settlement")
            elif label.startswith("consumer-interrupted-"):require("accepted-after-retry" in passed, "consumer replay preceded settlement audit")
            require(set(row["operations"])==set(operations), "measuring command inventory changed")
            for key, expected_operation in operations.items():
                for field in ("payload","sha256","offset","attempts","ack","upstream_ack"):
                    require(row["operations"][key].get(field)==expected_operation.get(field), "audit changed its original measuring command or acknowledgment")
            if label.startswith("consumer-interrupted-"):
                effects=list(snapshot["effects"]["receipts"])
                require(set(effects).issubset({"cross"}) and known_effects.issubset(effects), "consumer retry lost a previously proven effect")
                pending,checkpoint=0,0
            else:
                require(label in expected, "unknown fleet stage")
                pending,effects,checkpoint=expected[label]
            known_effects.update(effects)
            require(snapshot["sources"]["cross"]["host"]=="producer" and snapshot["effects"]["host"]=="consumer", "fleet role changed")
            accepted=validate_snapshot(row["operations"],snapshot,effects,checkpoint,{"cross":pending})
            for key,evidence in accepted.items():
                prior=operations[key].get("accepted")
                require(prior is None or prior==evidence, "accepted envelope or effect changed after proof")
                operations[key]["accepted"]=evidence
            require(checkpoint!=0 or len(snapshot["events"]["records"])==1, "lost proven destination acceptance")
            labels[label]=row
        elif event=="fleet_audit_passed":
            require(row["label"] in labels, "audit passed without snapshot")
            passed.add(row["label"])
        elif event=="process_killed":
            require(kills==0 and "before-destination-loss" in passed and row["label"]=="destination-after-proven-append" and row["exit_code"]==-9 and row["pid"]==pids["destination"], "destination kill did not follow proof audit")
            kills+=1
        elif event=="append_reply_lost":
            require(held is not None and kills==1 and not resumed and row["request_sha256"]==held["request_sha256"], "reply loss did not follow audited destination kill")
            drops+=1
        elif event=="peer_unavailable":
            require(row["expected"] is True, "unexpected peer outage was ignored")
        elif event=="retry_resumed":
            require("recovered-before-retry" in passed and kills==1, "retry concealed destination recovery")
            resumed=True
        elif event=="retry_blocked":
            require(held is not None and not resumed and row["request_sha256"]==held["request_sha256"], "blocked retry changed its original bytes")
        elif event=="append_reply_forwarded":
            require(held is not None and resumed and row["exchange"]["request_sha256"]==held["request_sha256"]
                and row["exchange"]["response_sha256"]==held["response_sha256"], "append retry changed bytes or preceded audit")
            forwarded+=1
    required={"before-destination-loss","recovered-before-retry","accepted-after-retry","final"}
    require(set(labels)==passed and required.issubset(passed)
        and held is not None and kills==1 and drops==1 and restarted is not None and resumed and forwarded>=1
        and stopped=={"producer","destination","consumer"}, "incomplete fleet fault evidence")
    require(store is None or removed,"network store cleanup evidence missing")
    return dict(audits=len(passed),independent_processes=3,destination_sigkills=kills,response_drops=drops,identical_retries=forwarded,backend="s3" if store else "sqlite")


def exercise(binary, root, rustfs_image=None):
    require(os.name=="posix", "fleet qualification requires SIGKILL")
    root.mkdir(parents=True,exist_ok=False)
    ledger=Ledger(root,commands=(("cross",725,1),)); processes=[]; proxy=None
    store=NetworkStore(root,ledger,rustfs_image) if rustfs_image else None
    try:
        if store:store.start()
        destination_peer=free_address(); proxy=PeerProxy(destination_peer,ledger)
        destination=FleetProcess(binary,root,"destination","destination-initial",destination_peer,proxy.address,True,store)
        processes.append(destination); origins={"destination":destination.ready()}
        require(read(origins["destination"],"/__test/events")["records"]==[], "destination not initially empty")
        for node in ("producer","consumer"):
            peer=free_address()
            process=FleetProcess(binary,root,node,node,peer,peer,False,store)
            processes.append(process);origins[node]=process.ready()
        ledger.record("fleet_started",pids={node:process.child.pid for node,process in zip(("destination","producer","consumer"),processes)})
        require(read(origins["producer"],"/orders/cross")["host"]=="producer", "source was placed on another process")
        require(read(origins["consumer"],"/__test/projection-receipts")["host"]=="consumer", "consumer was placed on another process")
        proxy.arm(); ledger.issue(origins["producer"],"cross")
        require(proxy.entered.wait(5), "append proof boundary was not reached")
        audit(ledger,origins,"before-destination-loss",1,[],0)
        destination.crash(ledger,"destination-after-proven-append")
        proxy.release.set()
        destination=FleetProcess(binary,root,"destination","destination-recovered",destination_peer,proxy.address,True,store)
        processes.append(destination);origins["destination"]=destination.ready()
        ledger.record("destination_restarted", pid=destination.child.pid)
        audit(ledger,origins,"recovered-before-retry",1,[],0)
        proxy.resume("recovered-before-retry")
        eventually(lambda:read(origins["producer"],"/orders/cross"),lambda state:state["outbox"]["pending_rows"]=="0")
        audit(ledger,origins,"accepted-after-retry",0,[],0)
        known_effects=[]
        deadline=time.monotonic()+60
        interruptions=0
        while True:
            require(time.monotonic()<deadline, "consumer did not recover its original transport/lease")
            try:
                first=read(origins["consumer"],"/projection/retry","POST")
                break
            except AuditError as error:
                ledger.record("consumer_response_ambiguous", error=str(error))
                observed=read(origins["consumer"],"/__test/projection-receipts")
                effects=list(observed["receipts"])
                require(set(known_effects).issubset(effects) and set(effects).issubset({"cross"}), "consumer interruption lost a proven effect")
                snapshot=audit(ledger,origins,f"consumer-interrupted-{interruptions}",0,effects,0)
                known_effects=effects;interruptions+=1
                if snapshot["subscription"]["observation"]["lease_active"]:
                    eventually(lambda:read(origins["destination"],"/subscription"),lambda state:not state["observation"]["lease_active"],timeout=45)
                time.sleep(.05)
        require(first["fresh_effects"]==1-len(known_effects) and first["status"]=="retried", "first effect missing or repeated")
        repeated=read(origins["consumer"],"/projection/process","POST")
        require(repeated["fresh_effects"]==0 and repeated["status"]=="acknowledged", "inbox redelivery repeated effects")
        audit(ledger,origins,"final",0,["cross"],1)
        proxy.outage=True
        for process in reversed(processes):process.stop_recorded(ledger)
        proxy.close();proxy=None
        if store:store.close()
        summary=dict(audit_evidence(ledger.path),status="passed",binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
            scope="three independent loopback processes sharing a private "+("RustFS S3" if store else "SQLite")+" bucket; proven destination append reply loss and destination SIGKILL",output=str(root))
        (root/"summary.json").write_text(json.dumps(summary,indent=2)+"\n")
        print(json.dumps(summary),flush=True)
    finally:
        if proxy is not None:
            proxy.release.set();proxy.outage=True
        for process in reversed(processes):process.stop()
        if proxy is not None:proxy.close()
        if store:store.close()
        ledger.close()


if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    target=parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--binary",type=Path);target.add_argument("--audit",type=Path)
    parser.add_argument("--output",type=Path)
    parser.add_argument("--rustfs-image",help="start a fresh private Docker RustFS server instead of the SQLite bucket")
    args=parser.parse_args()
    if args.audit:print(json.dumps(audit_evidence(args.audit.resolve())),flush=True)
    else:exercise(args.binary.resolve(),(args.output or Path(tempfile.mkdtemp(prefix="cellar-stream-fleet-parent-"))/"evidence").resolve(),args.rustfs_image)
