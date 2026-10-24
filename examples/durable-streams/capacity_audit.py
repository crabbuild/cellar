# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Independent saved-ledger oracle for the bounded single-source capacity case."""
import json
from pathlib import Path

from faults import integer, require, validate_event


def validate_source(operations,row,settled):
    count=len(operations);state=row["state"];page=state["outbox"]
    require(state["host"]=="producer" and state["commands"]==count and len(page["lanes"])==1,"source business/lane inventory changed")
    lane=page["lanes"][0];revision=lane["revision"]
    require(integer(revision["next_sequence"])==count+1 and integer(revision["settled_through"])==settled
        and integer(page["pending_rows"])==integer(lane["pending_rows"])==count-settled,"source lost/skipped obligations or consumed overflow sequence")
    require(set(row["commands"])==set(operations),"source command inventory changed")
    for index,(key,operation) in enumerate(operations.items(),1):
        command=row["commands"][key];receipt=operation.get("ack")
        require(receipt is not None and command["host"]=="producer" and command["order"]==operation["payload"]
            and command["receipt"]==receipt and receipt["payload_sha256"]==operation["sha256"] and integer(receipt["sequence"])==index,
            "original acknowledged source command/sequence changed")
        require(receipt["source"]==revision["source"] and receipt["incarnation"]==page["incarnation"]==revision["incarnation"]
            and receipt["lane"]==revision["lane"],"source incarnation/lane changed during recovery")
    require(row["rejected"]["order"] is None and row["rejected"]["receipt"] is None and row["rejected"]["commands"]==count,
        "overflow committed partial business state")
    require(len(row["events"])<=1,"stream exceeded captured one-record capacity")
    if settled==0:require(row["events"]==[],"destination accepted before recovered state audit")
    else:
        require(len(row["events"])==1 and integer(row["events"][0]["offset"])==count,"final retained history changed")
        record=row["events"][0];validate_event(operations[record["order_id"]],record)


def validate_effects(operations,effects,keys):
    require(effects["host"]=="consumer" and set(effects["receipts"])==set(keys),"consumer lost or added an effect")
    require(effects["projection"]==dict(orders=len(keys),total_cents=sum(operations[key]["payload"]["amount_cents"] for key in keys)),
        "consumer totals differ from distinct acknowledged commands")
    for key in keys:
        receipt=validate_event(operations[key],effects["receipts"][key])
        require(receipt==operations[key]["accepted"],"consumer effect differs from accepted envelope")


def audit(path:Path):
    operations=None;plan=None;pids=None;replacement=None;rejected=False;killed=False;resumed=False;stopped=set()
    source_rows={};passed=set();accepted=[];proven=[];checkpoints=[];before=set();observations=set();store=None;removed=False;final=False
    for line in path.read_text().splitlines():
        row=json.loads(line);event=row["event"]
        if event=="plan":
            require(operations is None,"multiple source plans");operations=row["operations"]
        elif event=="capacity_plan":
            require(plan is None and 1<=row["count"]<=256 and row["count"]==len(operations)==row["outbox_rows"]
                and row["stream_rows"]==1 and 0<=row["soak_seconds"]<=3600 and 0<=row["consumer_delay"]<=5,"invalid captured capacity policy")
            plan=row
        elif event=="network_store_started":
            require(store is None and pids is None and row["backend"]=="s3" and row["container"].startswith("cellar-stream-store-")
                and row["image_id"].startswith("sha256:"),"invalid owned store identity")
            store=row
        elif event=="capacity_started":
            require(pids is None and plan is not None and set(row["pids"])=={"producer","consumer","destination"}
                and len(set(row["pids"].values()))==3,"capacity processes were not independent")
            pids=row["pids"]
        elif event=="attempt":
            key=row["key"]
            require(key in operations and not row["replay"] and row["attempt"]==1 and operations[key]["attempts"]==0,
                "source replay could conceal quota/recovery loss")
            operations[key]["attempts"]=1
        elif event=="ack":
            key=row["key"]
            require(key in operations and operations[key]["attempts"]==1 and "ack" not in operations[key],"unissued/repeated source acknowledgment")
            operations[key]["ack"]=row["receipt"]
        elif event=="capacity_rejected":
            require(not rejected and row["command"]=="overflow" and row["error"]=="OutboxFull" and all("ack" in op for op in operations.values()),
                "quota rejection did not follow a full acknowledged source")
            rejected=True
        elif event=="capacity_source_audit":
            label=row["label"]
            require(label not in source_rows and set(row["operations"])==set(operations),"repeated/foreign source audit")
            if label=="full-before-soak":require(rejected and not killed and not resumed,"full audit did not precede fault")
            elif label=="before-source-loss":require("full-before-soak" in passed and not killed and not resumed,"loss audit did not follow full source")
            elif label=="recovered-before-retry":require(killed and replacement is not None and not resumed,"recovery audit did not precede retry")
            elif label=="final":require(resumed and len(checkpoints)==plan["count"],"final source audit preceded all consumer progress")
            else:require(False,"unknown capacity audit stage")
            for key,operation in operations.items():
                for field in ("payload","sha256","offset","attempts","ack","accepted"):
                    require(row["operations"][key].get(field)==operation.get(field),"audit changed original planned command/receipt")
            validate_source(operations,row,plan["count"] if label=="final" else 0)
            source_rows[label]=row
        elif event=="capacity_source_audit_passed":
            require(row["label"] in source_rows and row["label"] not in passed,"audit pass without original snapshot")
            passed.add(row["label"])
        elif event=="process_killed":
            require(not killed and "before-source-loss" in passed and row["label"]=="capacity-producer-pending"
                and row["pid"]==pids["producer"] and row["exit_code"]==-9,"source loss names wrong process or preceded audit")
            killed=True
        elif event=="capacity_producer_restarted":
            require(killed and replacement is None and row["pid"] not in pids.values() and not resumed,"replacement did not recover killed source")
            replacement=row["pid"]
        elif event=="retry_resumed":
            require("recovered-before-retry" in passed and not resumed,"retry concealed original acknowledged source loss")
            resumed=True
        elif event=="retry_blocked":require(not resumed and len(row["request_sha256"])==64,"offline append escaped its pause boundary")
        elif event=="capacity_acceptance":
            key=row["key"]
            require(resumed and len(accepted)==len(checkpoints) and key==list(operations)[len(accepted)],"non-FIFO/duplicate/unaudited acceptance")
            receipt=validate_event(operations[key],row["record"])
            operations[key]["accepted"]=receipt;accepted.append(key)
        elif event=="capacity_effects_before":
            key=row["key"]
            require(accepted and key==accepted[-1] and key not in before and len(accepted)==len(checkpoints)+1,"consumer observation did not precede processing")
            validate_effects(operations,row["effects"],checkpoints);before.add(key)
        elif event=="capacity_effect_proven":
            key=row["key"]
            require(accepted and key==accepted[-1] and key in before and key not in proven,"effect did not follow original acceptance/observation")
            validate_effects(operations,row["effects"],accepted);proven.append(key)
        elif event=="capacity_checkpoint":
            key=row["key"]
            require(key==proven[-1] and len(proven)==len(checkpoints)+1 and row["checkpoint"]==len(proven)
                and integer(row["subscription"]["checkpoint"])==len(proven) and not row["subscription"]["observation"]["lease_active"],
                "checkpoint skipped proof or retained a live acknowledged lease")
            checkpoints.append(key)
        elif event=="capacity_final_effects":
            require("final" in passed and not final,"final effects preceded final source proof")
            validate_effects(operations,row["effects"],list(operations));final=True
        elif event=="capacity_observation":
            label=row["label"];require(pids is not None and set(row["nodes"])==set(pids),"resource observation changed process inventory")
            if label=="empty":require(not rejected and not killed,"empty observation followed source load")
            elif label=="offline-soak":require("full-before-soak" in passed and not killed,"soak observation outside offline full source")
            elif label=="recovered-full":require("recovered-before-retry" in passed and not resumed,"recovered observation followed replay")
            elif label=="drained":require(final,"drained observation lacked final audit")
            else:require(False,"unknown capacity resource stage")
            for node,value in row["nodes"].items():
                require(value["pid"]==(replacement if node=="producer" and killed else pids[node]) and value["rss_bytes"]>0
                    and 0<=value["state"]["occupied"]<=128 and 0<=value["state"]["cached_routes"]<=256
                    and 0<=value["files"]<=10000 and value["logical_file_bytes"]==sum(value["family_bytes"].values())
                    and value["allocated_file_bytes"]>=0,"invalid memory/file/residency observation")
            observations.add(label)
        elif event=="fleet_process_stopped":
            require(final and row["node"] not in stopped and row["exit_code"]==0 and len(row["reports"])==1
                and row["pid"]==(replacement if row["node"]=="producer" else pids.get(row["node"]))
                and all(row["reports"][0].get(key) is True for key in ("ingress_drained","application_work_drained","cells_drained")),"incomplete capacity fleet shutdown")
            stopped.add(row["node"])
        elif event=="network_store_removed":
            require(store is not None and pids is not None and not removed and stopped==set(pids) and row["container"]==store["container"],"capacity store removed before complete shutdown")
            removed=True
    require(plan is not None and set(source_rows)==passed=={"full-before-soak","before-source-loss","recovered-before-retry","final"}
        and accepted==proven==checkpoints==list(operations) and final and observations.issuperset({"empty","recovered-full","drained"})
        and (plan["soak_seconds"]==0 or "offline-soak" in observations) and stopped==set(pids) and (store is None or removed),"incomplete capacity evidence")
    return dict(commands=plan["count"],audits=len(passed),sigkills=1,accepted_records=len(accepted),effect_receipts=len(proven),checkpoint=len(checkpoints),
        backend="s3" if store else "sqlite")
