"""Observe a real failed recovery pass before returning its retained witnesses."""
import hashlib
import json
import time

from faults import eventually, require
from resumed_owner import authority, store_json

EVENT = "destination_startup_recovery_refused"
MESSAGE = "predecessor recovery failed; retrying before lease installation"


def observe(process, proof):
    def prefix():
        with process.log.open("rb") as file:
            raw=file.read((128<<10)+1)
        require(len(raw)<=128<<10,"startup diagnostic exceeded observation budget")
        length=0
        for line in raw.splitlines(keepends=True):
            length+=len(line)
            if line.endswith(b"\n") and MESSAGE.encode() in line:
                return raw[:length]
        return None
    raw=eventually(prefix,bool,timeout=5)
    leases={node:store_json(proof.root,proof.store,"nodes/"+node+".json") for node in ("destination","producer","consumer")}
    proof.ledger.record(EVENT,leases=leases,owner=authority(proof.root,proof.store,proof.scope),observed_ms=time.time_ns()//1000000,
                        prefix_bytes=len(raw),prefix_sha256=hashlib.sha256(raw).hexdigest(),refusal=raw.splitlines()[-1].decode())
    audit(proof.ledger.path,complete=False)


def audit(path,complete=True):
    rows=[json.loads(line) for line in path.read_text().splitlines()]
    plan=next((row for row in rows if row["event"]=="fetch_fleet_started"),{})
    enabled=plan.get("recovery_first",False)
    require(type(enabled) is bool,"invalid recovery-first policy")
    observed=[(i,row) for i,row in enumerate(rows) if row["event"]==EVENT]
    if not enabled:
        require(not observed,"unrequested startup ordering evidence")
        return 0
    require(plan["lease_state"]=="expired" and plan["node_lease_ms"]==30000,"startup cut changed the original lease budget")
    started=[i for i,row in enumerate(rows) if row["event"]=="destination_follower_process_started"]
    if not observed:
        require(not complete and not started,"missing initial recovery refusal before witness return")
        return 0
    require(len(observed)==1,"repeated startup ordering evidence")
    index,row=observed[0]
    def prior(event):
        values=[entry for i,entry in enumerate(rows) if i<index and entry["event"]==event]
        require(len(values)==1,"startup refusal preceded "+event)
        return values[0]
    armed=prior("destination_proof_armed");accepted=prior("destination_follower_accepted");retained=prior("destination_followers_retained")
    prior("destination_proof_discarded")
    require(armed["restart_followers"] is True and all(i>index for i in started),"startup refusal followed witness return")
    old=accepted["lease"];leader=f"destination/{old['lineage']}/{old['generation']}"
    lease=row["leases"]["destination"];log=lease["log"]
    require(row["owner"]==armed["owner"] and row["observed_ms"]>old["expires_ms"] and
            all(lease[key]==old[key] for key in ("node","lineage","generation")) and
            log["state"]=="recovering" and log["claimant"]=="destination" and log["epoch"]==old["log"]["epoch"] and
            log["ensemble"]==old["log"]["ensemble"] and log["may_have_fleet_acks"] is True,"startup refusal lost predecessor authority or proof obligation")
    require({node:value for node,value in row["leases"].items() if node!="destination"}==retained["leases"],"witness lease returned before measured refusal")
    refusal=row["refusal"]
    require(MESSAGE in refusal and "attempt=1" in refusal and leader in refusal and "no complete true witness" in refusal and
            "refusing to seal while member state remains unverified" in refusal,"startup cut observed another failure")
    require(type(row["prefix_bytes"]) is int and 0<row["prefix_bytes"]<=128<<10,"invalid startup prefix bound")
    with (path.parent/"destination-recovered.log").open("rb") as file:raw=file.read(row["prefix_bytes"])
    require(len(raw)==row["prefix_bytes"] and hashlib.sha256(raw).hexdigest()==row["prefix_sha256"] and
            raw.splitlines()[-1].decode()==refusal,"startup diagnostic prefix changed")
    return 1
