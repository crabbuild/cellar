"""Evidence for consumer inbox/effect recovery from retained follower proof."""
import base64
from contextlib import closing
import hashlib
import json
from pathlib import Path
import shutil
import sqlite3
import tempfile

from consumer_owner_audit import FETCH_ID, inspect_image, validate_delivery
from faults import digest, integer, require, validate_event, validate_source
from follower_faults import inspect_follower_disk, log_lineage
from consumer_witness_data import validate_damage, validate_return
from whole_fleet import inventory

PEERS = {"destination", "producer"}
STAGES = ("before-loss", "recovered-before-replay", "after-replay", "final")


def selected(objects, scope, lease):
    return {key:value for key,value in objects.items() if key.startswith(f"cells/{scope}/ltx/") or key.startswith(f"log/consumer/{log_lineage(lease)}/{lease['generation']}/")}


def inspect_consumer(archive, scope):
    image = inspect_image(archive,scope)
    with tempfile.TemporaryDirectory(prefix="cellar-consumer-receipt-") as directory:
        working=Path(directory)
        for suffix in ("","-wal","-shm"):
            source=archive/("db.sqlite"+suffix)
            if source.exists():shutil.copy2(source,working/source.name)
        with closing(sqlite3.connect((working/"db.sqlite").resolve().as_uri()+"?mode=ro",uri=True)) as db:
            db.execute("PRAGMA query_only=ON")
            value,=db.execute("SELECT v FROM __sys_cell_kv WHERE scope=? AND k='ack_receipt'",(scope,)).fetchone()
            encoded=json.loads(value)
            image["ack_receipt_sha256"]=hashlib.sha256(encoded.encode()).hexdigest()
    return image


def validate_image(image, operations, effects, receipt_sha):
    expected=[dict(scope="order-projection-v1",event_id=operations[key]["accepted"]["event_id"],digest=operations[key]["accepted"]["envelope_sha256"]) for key in ("a","b")]
    require(image["projection"]==effects["projection"] and image["receipts"]==effects["receipts"] and image["count"]==2
            and image["inbox"]==sorted(expected,key=lambda row:row["event_id"]) and image["ack_receipt_sha256"]==receipt_sha and "db.sqlite" in image["files"],"consumer image lost or changed an inbox/effect/receipt")


def validate_prefix(operations,snapshot,checkpoint):
    require(set(snapshot["sources"])==set(operations),"source inventory changed")
    for key,operation in operations.items():require(validate_source(operation,snapshot["sources"][key])==0,"original source obligation changed")
    records=snapshot["events"]["records"]
    require([v["order_id"] for v in records]==["a","b"],"original retained history changed")
    effects=snapshot["effects"]["receipts"]
    require(set(effects)==set(operations) and snapshot["effects"]["projection"]==dict(orders=2,total_cents=1500),"consumer effect prefix changed")
    accepted={}
    for record in records:
        key=record["order_id"];accepted[key]=validate_event(operations[key],record)
        require(validate_event(operations[key],effects[key])==accepted[key],"consumer/destination receipt differs")
    require(integer(snapshot["subscription"]["checkpoint"])==checkpoint and snapshot["subscription"]["earliest"]=="1" and snapshot["subscription"]["end"]=="3","checkpoint/history range lost/skipped original work")
    return accepted


def audit_evidence(path, complete=True):
    operations=pids=cold=seed=armed=proven=original=old_image=retained=cold_leases=successor=replay=new_image=store=None
    discarded=expired=acked=removed=False
    inbox_verified=False
    refused=returned=None
    fault=damaged=restored=None
    refusals=0;repair_joined={};repair_started=set();repair_ready=set()
    connections_retired=False
    gate_joined=set()
    killed,started,ready,joined={}, {}, set(),set()
    stages=[]
    for line in path.read_text().splitlines():
        row=json.loads(line);event=row["event"]
        if event=="plan":
            require(operations is None and set(row["operations"])=={"a","b"},"invalid consumer recovery plan")
            operations=row["operations"]
            for key,amount,offset in (("a",1200,1),("b",300,2)):
                op=operations[key];require(op["payload"]==dict(order_id=key,amount_cents=amount) and op["sha256"]==digest(op["payload"]) and op["offset"]==offset and op["attempts"]==0,"changed recovery command")
        elif event=="network_store_startup_retry":require(pids is None and store is None and row["status"]==503,"application failure hidden as startup retry")
        elif event=="network_store_started":
            require(pids is None and store is None and row["backend"]=="s3" and row["image_id"].startswith("sha256:") and row["container"].startswith("cellar-stream-store-"),"invalid private store")
            store=row
        elif event=="consumer_recovery_started":
            require(pids is None and set(row["pids"])==PEERS|{"consumer"} and len(set(row["pids"].values()))==3 and row["proof_mode"]=="fleet" and row["peer_proof"]=="bucket" and row["replica_fault_node"]=="consumer" and row["node_lease_ms"]==1000 and type(row["restart_followers"]) is bool,"wrong consumer proof policy/processes")
            pids,cold=row["pids"],row["restart_followers"]
            fault=row.get("witness_data")
            require(fault is None or cold and fault in ("missing","corrupt"),"witness damage requires cold recovery")
        elif event=="attempt":
            key=row["key"]
            require(pids is not None and key==("a" if seed is None else "b") and row["attempt"]==1 and row["replay"] is False and operations[key]["attempts"]==0 and proven is None,"source replay concealed recovery")
            operations[key]["attempts"]=1
        elif event=="ack":
            key=row["key"];require(key in operations and operations[key]["attempts"]==1 and "ack" not in operations[key] and proven is None,"changed source acknowledgment")
            operations[key]["ack"]=row["receipt"]
        elif event=="consumer_recovery_seeded":
            require(seed is None and "ack" in operations["a"] and operations["b"]["attempts"]==0 and row["result"]==dict(status="acknowledged",fresh_effects=1,projection=dict(orders=1,total_cents=1200)),"invalid original consumer prefix")
            seed=row
        elif event=="consumer_proof_armed":
            require(seed is not None and armed is None and proven is None and row["owner"]["node"]==row["lease"]["node"]=="consumer" and row["scope"].startswith("Projection:") and len(row["scope"])==75,"wrong consumer arm")
            log=row["lease"]["log"];require(log["state"]=="open" and log["ensemble"] and set(log["ensemble"])<=PEERS,"consumer lacks a real follower ensemble")
            log_lineage(row["lease"]);armed=row
        elif event=="consumer_effect_proven":
            require(armed is not None and proven is None and not killed and row["owner"]==armed["owner"],"consumer proof followed loss or changed owner")
            lease=row["lease"];log=lease["log"]
            require(lease["node"]=="consumer" and lease["generation"]==armed["lease"]["generation"] and log_lineage(lease)==log_lineage(armed["lease"]) and log["state"]=="open" and log["may_have_fleet_acks"] is True and log["ensemble"] and set(log["ensemble"])<=PEERS,"effect lacks installed fleet proof")
            require(selected(row["objects"],armed["scope"],lease)==selected(armed["objects"],armed["scope"],lease) and any(v["path"].startswith(f"cells/{armed['scope']}/ltx/") for v in row["refused"]),"bucket publication bypassed consumer fleet proof")
            require(row["result"]["status"]=="fetched" and row["result"]["fresh_effects"]==1 and row["result"]["projection"]==dict(orders=2,total_cents=1500),"consumer effect not proven before ack")
            proven=row
        elif event=="consumer_recovery_audit":
            label,snapshot=row["label"],row["snapshot"]
            require(proven is not None and len(stages)<len(STAGES) and label==STAGES[len(stages)],"reordered consumer recovery audit")
            if label=="before-loss":require(not killed and row["owner"]==proven["owner"],"loss preceded original consumer audit")
            else:
                require(discarded and expired and successor is not None and (not cold or ready==PEERS and refused is not None and returned is not None) and row["owner"]["epoch"]>proven["owner"]["epoch"] and row["owner"]["node"] in (PEERS|{"successor"} if cold else {"successor"}),"recovery retained old authority or preceded witnesses")
            if cold and label!="before-loss":require(row["recoverer"]["pid"]==successor and row["recoverer"]["lease"]["generation"]==returned["generation"],"witness return restarted/replaced the recoverer")
            for key,op in operations.items():require(all(row["operations"][key].get(k)==op.get(k) for k in ("payload","sha256","offset","attempts","ack")),"original command/ack changed")
            final=label=="final";require(final==acked and (label not in ("after-replay","final") or replay is not None),"audit preceded explicit replay/ack")
            require(snapshot["effects"]["host"]==row["owner"]["node"] and snapshot["effects"]["scope"]==armed["scope"],"consumer database/host changed")
            allowed=PEERS|{"successor"} if cold and label!="before-loss" else {"producer"}
            require(all(source["host"] in allowed for source in snapshot["sources"].values()),"wrong source host")
            accepted=validate_prefix(operations,snapshot,2 if final else 1)
            for key,value in accepted.items():operations[key]["accepted"]=value
            require([v["order_id"] for v in snapshot["events"]["records"]]==["a","b"],"original retained history disappeared")
            if original is None:
                original=snapshot
                validate_delivery(dict(proven["result"]["batch"],fetch_id=FETCH_ID),snapshot["subscription"],operations["b"])
            require(all(snapshot["effects"][key]==original["effects"][key] for key in ("projection","receipts")),"consumer receipt/effect prefix changed before/after replay")
            expected=proven["result"]["batch"] if replay is None else replay["batch"]
            require(snapshot["effects"]["ack_receipt_sha256"]==hashlib.sha256(expected["receipt"].encode()).hexdigest(),"saved exact ack receipt was lost or changed")
            if label=="recovered-before-replay":require(snapshot["subscription"]["identity"]==original["subscription"]["identity"],"subscription identity changed during recovery")
            if final:require(snapshot["subscription"]["lease"] is None,"ack retained a live lease")
            stages.append(label)
        elif event=="process_killed":
            node=row["label"].removeprefix("consumer-proof-")
            require(stages==["before-loss"] and node in PEERS|{"consumer"} and node not in killed and row["pid"]==pids[node] and row["exit_code"]==-9 and (node=="consumer" or cold and "consumer" in killed),"wrong/premature consumer or follower SIGKILL")
            killed[node]=row["pid"]
        elif event=="consumer_followers_retained":
            require(cold and set(killed)==PEERS|{"consumer"} and retained is None and not discarded,"custody preceded loss of all original memory")
            require(selected(row["objects"],armed["scope"],proven["lease"])==selected(armed["objects"],armed["scope"],proven["lease"]),"bucket data repaired cold consumer proof")
            retained,cold_leases=row["followers"],row["leases"]
            require(set(retained)==set(proven["lease"]["log"]["ensemble"]) and set(cold_leases)==PEERS and all(v["node"]==node and (not v.get("log") or not v["log"]["may_have_fleet_acks"]) for node,v in cold_leases.items()),"missing witness or non-bucket peer proof")
            for data in retained.values():require(any(batch["epoch"]==proven["lease"]["log"]["epoch"] and any(entry["cell"]==armed["scope"] and entry["cell_epoch"]==proven["owner"]["epoch"] for entry in batch["entries"]) for batch in data["batches"]),"witness lacks original consumer epoch")
        elif event=="consumer_proof_image":
            require("consumer" in killed and old_image is None and not discarded,"image preceded leader loss")
            old_image=row["image"];validate_image(old_image,operations,original["effects"],original["effects"]["ack_receipt_sha256"])
        elif event=="consumer_witness_data_damaged":
            require(fault is not None and row["mode"]==fault and retained is not None and damaged is None and not started and not discarded and set(row["files"])==set(retained),"witness damage preceded archive/join or followed startup")
            for node,files in row["files"].items():validate_damage(fault,retained[node]["files"],files)
            damaged=row["files"]
        elif event=="consumer_data_discarded":
            require(old_image is not None and not discarded and successor is None and row["files"] and row["remaining_entries"]==0 and (not cold or retained is not None),"consumer retained local state or lost it before witness custody")
            require(selected(row["objects"],armed["scope"],proven["lease"])==selected(armed["objects"],armed["scope"],proven["lease"]),"bucket data repaired consumer before restart")
            discarded=True
        elif event=="consumer_authority_expired":
            require(discarded and not expired and successor is None and row["lease"]["node"]=="consumer" and row["lease"]["generation"]==proven["lease"]["generation"] and log_lineage(row["lease"])==log_lineage(proven["lease"]) and row["observed_ms"]>row["lease"]["expires_ms"],"old consumer authority still live or changed")
            expired=True
        elif event=="consumer_witness_started":
            node=row["node"];require(cold and discarded and expired and retained is not None and node in PEERS-started.keys() and row["pid"] not in (*pids.values(),*started.values()) and row["lease"]==cold_leases[node] and row["observed_ms"]>row["lease"]["expires_ms"],"witness reused memory/live authority")
            require((fault is None or damaged is not None) and row["files"]==((damaged[node] if fault else retained[node]["files"]) if node in retained else []),"retained/damaged witness bytes changed before restart")
            started[node]=row["pid"]
        elif event=="consumer_witness_ready":
            node=row["node"];require(cold and successor is not None and set(started)==PEERS and node in PEERS-ready and row["pid"]==started[node] and row["lease"]["node"]==node and row["lease"]["generation"]!=cold_leases[node]["generation"] and log_lineage(row["lease"])==log_lineage(cold_leases[node]),"witness readiness lacks fresh generation/retained lineage")
            ready.add(node)
        elif event=="consumer_successor_started":
            require(discarded and expired and successor is None and row["pid"] not in (*pids.values(),*started.values()) and (not cold or set(started)==PEERS),"successor reused old state/process")
            successor=row["pid"]
        elif event=="consumer_witness_refused":
            require(cold and ready==PEERS and successor is not None and returned is None and restored is None and refusals<(2 if fault else 1) and row.get("attempt",1)==refusals+1 and stages==["before-loss"] and row["status"]==503 and "Unavailable: event RPC did not return a proven result" in row["body"] and 0<=row["elapsed_ms"]<10000,"consumer witness denial timed out or followed repair")
            require(row["owner"]==proven["owner"] and row["lease"]["generation"]==proven["lease"]["generation"] and log_lineage(row["lease"])==log_lineage(proven["lease"]) and row["lease"]["log"]["state"]=="recovering" and row["lease"]["log"]["may_have_fleet_acks"] is True,"missing witness reset owner or sealed away consumer proof")
            require(selected(row["objects"],armed["scope"],proven["lease"])==selected(armed["objects"],armed["scope"],proven["lease"]),"unavailable witnesses fabricated bucket coverage")
            leader=f"consumer/{log_lineage(proven['lease'])}/{proven['lease']['generation']}"
            require(set(row["gates"])==PEERS and all(row["gates"][node] for node in proven["lease"]["log"]["ensemble"]) and all(v["leader"]==leader and v["epoch"]==proven["lease"]["log"]["epoch"] and len(v["request_sha256"])==64 for rows in row["gates"].values() for v in rows),"witness service fault was not exercised on the retained session")
            if fault:
                diagnostic=row["diagnostic"]
                require(damaged is not None and diagnostic["leader"]==leader and diagnostic["epoch"]==proven["lease"]["log"]["epoch"] and diagnostic["recovery_blocked"] is True and set(diagnostic["ensemble"])==set(retained),"missing retained-data loss diagnostic")
                require(all(v.get("blocked") is False and v.get("response",{}).get("fragment_epoch")==0 for rows in row["gates"].values() for v in rows),"data loss replaced by service refusal or a complete seal certificate")
                if refused:require(all(len(row["gates"][node])>len(refused["gates"][node]) for node in retained),"cached diagnostic replaced actual repeated recovery")
            else:require(all(v.get("blocked",True) is True for rows in row["gates"].values() for v in rows),"service refusal was not exercised")
            refused=row;refusals+=1
        elif event=="consumer_damaged_witness_joined":
            node=row["node"]
            require(fault is not None and refusals==2 and restored is None and node in PEERS-repair_joined.keys() and row["pid"]==started[node] and row["exit_code"]==0 and len(row["reports"])==1 and all(row["reports"][0].get(key) is True for key in ("ingress_drained","application_work_drained","cells_drained")),"witness data returned before complete peer joins")
            require(row["lease"]["node"]==node and row["lease"]["generation"]!=cold_leases[node]["generation"] and log_lineage(row["lease"])==log_lineage(cold_leases[node]),"joined damaged witness changed lineage")
            repair_joined[node]=row
        elif event=="consumer_witness_connections_retired":
            require(set(repair_joined)==PEERS and not connections_retired and restored is None and set(row["connections"])==PEERS and all(type(value) is int and 0<=value<=1000 for value in row["connections"].values()),"proxy EOF propagation preceded actual peer joins")
            connections_retired=True
        elif event=="consumer_witness_data_restored":
            require(fault is not None and set(repair_joined)==PEERS and connections_retired and restored is None and returned is None and set(row["files"])==set(retained),"witness return preceded repeated refusal/joins")
            for node,data in row["files"].items():
                validate_return(retained[node]["files"],data["before"],data["after"])
                require(data["sha256"]=={entry["path"]:entry["sha256"] for entry in retained[node]["files"] if entry["path"].endswith(".batch")},"returned witness hash changed")
            restored=row["files"]
        elif event=="consumer_returned_witness_started":
            node=row["node"]
            require(restored is not None and node in PEERS-repair_started and row["lease"]==repair_joined[node]["lease"] and row["observed_ms"]>row["lease"]["expires_ms"] and row["pid"] not in (*pids.values(),*started.values(),successor,*[v["pid"] for v in repair_joined.values()]),"returned witness reused process/live authority")
            require(row["files"]==(restored[node]["after"] if node in retained else []),"returned bytes changed before cold reload")
            started[node]=row["pid"];repair_started.add(node)
        elif event=="consumer_returned_witness_ready":
            node=row["node"];lease=row["lease"]
            require(repair_started==PEERS and node in PEERS-repair_ready and row["pid"]==started[node] and lease["node"]==node and lease["generation"]!=repair_joined[node]["lease"]["generation"] and log_lineage(lease)==log_lineage(cold_leases[node]),"returned witness lacks fresh process generation/retained lineage")
            repair_ready.add(node)
        elif event=="consumer_witness_returned":
            require(refused is not None and returned is None and row["pid"]==successor and len(row["generation"])==64 and stages==["before-loss"],"witness returned after replay or recoverer restart")
            require(fault is None or refusals==2 and repair_ready==PEERS,"witness returned without complete data recovery")
            returned=row
        elif event=="consumer_witness_gate_joined":
            require(cold and stages==list(STAGES) and joined==PEERS|{"successor"} and row["node"] in PEERS-gate_joined,"witness gate outlived cleanup")
            gate_joined.add(row["node"])
        elif event=="consumer_inbox_verified":
            require(stages==list(STAGES[:2]) and not inbox_verified and replay is None,"inbox probe followed processing replay")
            result=row["result"]
            require(len(result["accepted"])==2 and all(value is False for value in result["accepted"]) and result["projection"]==original["effects"]["projection"] and result["records"]==[operations[key]["accepted"] for key in ("a","b")],"original inbox receipts were missing before fetch replay")
            require(row["subscription"]["identity"]==original["subscription"]["identity"] and row["subscription"]["checkpoint"]=="1","inbox probe settled or replaced subscription")
            inbox_verified=True
        elif event=="consumer_fetch_replayed":
            require(stages==list(STAGES[:2]) and inbox_verified and replay is None,"replay preceded original prefix audit")
            replay=row["result"];require(replay["status"]=="fetched" and replay["fresh_effects"]==0 and replay["projection"]==original["effects"]["projection"],"consumer repeated or lost a local effect")
            validate_delivery(dict(replay["batch"],fetch_id=FETCH_ID),row["subscription"],operations["b"])
            old,new=proven["result"]["batch"],replay["batch"]
            require(old["records"]==new["records"],"replay changed original envelope")
            if integer(new["issued_ms"])<integer(old["expires_ms"]):require(new==old,"live receipt replay changed")
            else:
                def receipt(value):
                    raw=value["receipt"][3:];return json.loads(base64.urlsafe_b64decode(raw+"="*(-len(raw)%4)))
                require(integer(receipt(new)["generation"])>integer(receipt(old)["generation"]),"expired replay lacked fresh authority")
        elif event=="consumer_receipt_acknowledged":
            require(stages==list(STAGES[:3]) and not acked and row["result"]==dict(status="Acknowledged",projection=dict(orders=2,total_cents=1500)),"ack preceded recovery/replay")
            acked=True
        elif event=="fleet_process_stopped":
            node=row["node"];require(stages==list(STAGES) and node in PEERS|{"successor"} and node not in joined and row["pid"]==(successor if node=="successor" else started[node] if cold else pids[node]) and row["exit_code"]==0 and len(row["reports"])==1 and all(row["reports"][0].get(k) is True for k in ("ingress_drained","application_work_drained","cells_drained")),"incomplete survivor join")
            joined.add(node)
        elif event=="consumer_recovered_image":
            require(joined==PEERS|{"successor"} and new_image is None,"recovered image preceded survivor joins")
            new_image=row;validate_image(row["image"],operations,original["effects"],hashlib.sha256(replay["batch"]["receipt"].encode()).hexdigest())
        elif event=="network_store_removed":
            require(store is not None and new_image is not None and not removed and row["container"]==store["container"],"store removed before survivor image/joins")
            removed=True
        else:require(False,f"unknown consumer recovery evidence: {event}")
    if complete:
        require(stages==list(STAGES) and joined==PEERS|{"successor"} and new_image is not None and (not cold or gate_joined==PEERS) and (store is None or removed),"incomplete consumer follower recovery")
        require(inspect_consumer(path.parent/"lost-consumer-image",armed["scope"])==old_image,"old consumer archive changed")
        require(inspect_consumer(path.parent/"recovered-consumer-image",armed["scope"])==new_image["image"],"recovered consumer archive changed")
        if cold:
            for node,data in retained.items():require(inspect_follower_disk(path.parent/"consumer-follower-images"/node)==data,"consumer witness archive changed")
        if fault:
            require(damaged is not None and restored is not None,"missing witness-data custody")
            for node in retained:
                require(inventory(path.parent/"damaged-consumer-followers"/node)==damaged[node],"damaged consumer witness archive changed")
                require(inventory(path.parent/"returned-consumer-followers"/node)==restored[node]["after"],"returned consumer witness archive changed")
                state=json.loads((path.parent/"returned-consumer-followers"/node/"state.json").read_text())
                require(state["sealed_to"]>=proven["lease"]["log"]["epoch"],"return rolled back witness seal floor")
    return dict(audits=len(stages),consumer_fleet_recoveries=int(len(stages)>1),cold_followers=len(ready),backend="s3" if store else "sqlite")
