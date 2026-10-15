"""Saved evidence oracle for autonomous recovery of independent source cells."""
import json
from pathlib import Path

from capacity_audit import validate_effects
from faults import digest, integer, require, validate_event, validate_source


def audit(path:Path):
    operations=None;plan=None;pids=None;replacement={};audits=set();passed=set();accepted=[];checkpoint=0;proven=0;before=False
    killed=set();cold=False;resumed=False;final=False;stopped=set();observations=set();store=None;removed=False;drain_seconds=None
    for line in path.read_text().splitlines():
        row=json.loads(line);event=row["event"]
        if event=="plan":
            require(operations is None,"duplicate measuring plan");operations=row["operations"]
            require(all(op["attempts"]==0 and op["offset"]==0 and digest(op["payload"])==op["sha256"] for op in operations.values()),"invalid original command plan")
        elif event=="many_plan":
            require(plan is None and row["stream_rows"]<row["count"]<=192 and row["count"]==len(operations) and 1<=row["stream_rows"]<=20
                and row["resident_limit"]==128 and row["cache_limit"]==256 and 0<=row["soak_seconds"]<=3600 and 0<=row["consumer_delay"]<=5,"invalid captured many-source policy")
            plan=row
        elif event=="network_store_started":
            require(store is None and pids is None and row["backend"]=="s3" and row["container"].startswith("cellar-stream-store-")
                and row["image_id"].startswith("sha256:"),"foreign network store")
            store=row
        elif event=="many_started":
            require(plan is not None and pids is None and set(row["pids"])=={"producer","consumer","destination"} and len(set(row["pids"].values()))==3,"processes not independent")
            pids=row["pids"]
        elif event=="attempt":
            key=row["key"];require(key in operations and pids is not None and not killed and not row["replay"] and row["attempt"]==1 and operations[key]["attempts"]==0,"source replay could repair recovery")
            operations[key]["attempts"]=1
        elif event=="ack":
            key=row["key"];require(key in operations and operations[key]["attempts"]==1 and "ack" not in operations[key] and not killed,"unissued/repeated acknowledgment")
            require(row["receipt"]["payload_sha256"]==operations[key]["sha256"] and integer(row["receipt"]["sequence"])==1,"source command identity changed")
            operations[key]["ack"]=row["receipt"]
        elif event=="many_source_audit":
            label=row["label"];require(label not in audits and set(row["sources"])==set(operations),"changed source inventory")
            if label=="before-source-loss":require(not killed and not resumed and len(accepted)==plan["stream_rows"] and all("ack" in op for op in operations.values()),"source loss preceded audit")
            elif label=="final":require(resumed and checkpoint==plan["count"],"source reads concealed cold recovery")
            else:require(False,"unknown source audit")
            for key,state in row["sources"].items():
                require(state["host"] in pids and validate_source(operations[key],state)==(0 if label=="final" or key in accepted else 1),"source obligation lost")
            require(len({op["ack"]["source"] for op in operations.values()})==plan["count"] and len({op["ack"]["lane"] for op in operations.values()})==plan["count"],"commands share sources/lanes")
            audits.add(label)
        elif event=="many_source_audit_passed":
            require(row["label"] in audits and row["label"] not in passed,"pass without source audit");passed.add(row["label"])
        elif event=="process_killed":
            node=row["label"].removeprefix("many-cold-")
            require(node in pids and node not in killed and "before-source-loss" in passed and not replacement and row["pid"]==pids[node] and row["exit_code"]==-9,"foreign process loss or no prior audit")
            killed.add(node)
        elif event=="many_restarted":
            node=row["node"]
            require(killed==set(pids) and node in pids and node not in replacement and row["pid"] not in (*pids.values(),*replacement.values()),"invalid replacement process");replacement[node]=row["pid"]
        elif event=="many_cold_boundary":
            require(set(replacement)==set(pids) and not cold and not resumed and "cold-before-retry" in observations and row["source_requests"]==0
                and len(row["events"])==plan["stream_rows"],"cold recovery repaired by requests")
            for index,record in enumerate(row["events"]):
                require(record["order_id"]==accepted[index] and validate_event(operations[accepted[index]],record)==operations[accepted[index]]["accepted"],"protected accepted history changed during recovery")
            cold=True
        elif event=="retry_resumed":
            require(cold and not resumed and row["label"]=="cold-before-retry","unaudited wake recovery");resumed=True
        elif event=="retry_blocked":require(not resumed and len(row["request_sha256"])==64,"append escaped pause")
        elif event=="many_acceptance":
            key=row["key"];require(key in operations and key not in accepted and (resumed or not killed and not audits and len(accepted)<plan["stream_rows"]),"duplicate/foreign acceptance")
            operations[key]["offset"]=len(accepted)+1
            operations[key]["accepted"]=validate_event(operations[key],row["record"]);accepted.append(key)
        elif event=="many_effects_before":
            require(row["checkpoint"]==checkpoint and proven==checkpoint and not before,"processing overlaps unacknowledged proof")
            validate_effects(operations,row["effects"],accepted[:checkpoint])
            before=True
        elif event=="many_effect_proven":
            require(before and checkpoint<row["checkpoint"]<=min(checkpoint+20,len(accepted)) and proven==checkpoint,"invalid effect batch")
            proven=row["checkpoint"];validate_effects(operations,row["effects"],accepted[:proven])
        elif event=="many_checkpoint":
            require(row["checkpoint"]==proven>checkpoint and integer(row["subscription"]["checkpoint"])==proven
                and not row["subscription"]["observation"]["lease_active"],"checkpoint skipped proof or retains lease")
            checkpoint=proven
            before=False
        elif event=="many_drain_finished":
            require(checkpoint==plan["count"] and drain_seconds is None and row["seconds"]>=0,"incomplete cold recovery");drain_seconds=row["seconds"]
        elif event=="many_final_effects":
            require("final" in passed and not final,"final effect without source audit");validate_effects(operations,row["effects"],list(operations));final=True
        elif event=="capacity_observation":
            label=row["label"];require(pids is not None and set(row["nodes"])==set(pids),"changed process inventory")
            if label=="empty":require(not killed and not audits,"empty observation after load")
            elif label=="offline-soak":require("before-source-loss" in passed and not killed,"soak outside offline boundary")
            elif label=="cold-before-retry":require(set(replacement)==set(pids) and not resumed,"cold observation after retry")
            elif label=="drained":require(final,"drained without final audit")
            else:require(False,"unknown resource stage")
            for node,value in row["nodes"].items():
                require(value["pid"]==replacement.get(node,pids[node]) and value["rss_bytes"]>0
                    and 0<=value["state"]["occupied"]<=128 and 0<=value["state"]["cached_routes"]<=256
                    and 0<=value["files"]<=10000 and value["logical_file_bytes"]==sum(value["family_bytes"].values()),"invalid resource observation")
            observations.add(label)
        elif event=="fleet_process_stopped":
            require(final and row["node"] not in stopped and row["exit_code"]==0 and len(row["reports"])==1
                and row["pid"]==replacement.get(row["node"])
                and all(row["reports"][0].get(key) is True for key in ("ingress_drained","application_work_drained","cells_drained")),"incomplete final shutdown")
            stopped.add(row["node"])
        elif event=="network_store_removed":
            require(store is not None and pids is not None and stopped==set(pids) and row["container"]==store["container"] and not removed,"foreign/early store removal");removed=True
    require(plan is not None and passed==audits=={"before-source-loss","final"} and len(accepted)==checkpoint==plan["count"] and final
        and observations.issuperset({"empty","cold-before-retry","drained"}) and (plan["soak_seconds"]==0 or "offline-soak" in observations)
        and stopped==set(pids) and (store is None or removed) and drain_seconds is not None,"incomplete many-source evidence")
    return dict(sources=plan["count"],accepted_records=len(accepted),checkpoint=checkpoint,sigkills=len(killed),drain_seconds=drain_seconds,backend="s3" if store else "sqlite")
