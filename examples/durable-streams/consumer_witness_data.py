"""Damage and return only real committed batches while witness processes are joined."""
import hashlib
import json
import os
from pathlib import Path
import shutil

from faults import require
from whole_fleet import inventory


def validate_damage(mode, original, damaged):
    require(mode in ("missing", "corrupt", "state-missing", "state-corrupt"), "unknown consumer witness fault")
    before={entry["path"]:entry for entry in original}
    after={entry["path"]:entry for entry in damaged}
    batches={path for path in before if path.endswith(".batch")}
    require(batches and len(before)==len(original) and len(after)==len(damaged), "missing/duplicate witness inventory")
    if mode.startswith("state-"):
        require("state.json" in before and {key:value for key,value in after.items() if key!="state.json"}=={key:value for key,value in before.items() if key!="state.json"},"metadata fault changed committed batches")
        require(set(after)==set(before)-{"state.json"} if mode=="state-missing" else set(after)==set(before) and after["state.json"]["sha256"]!=before["state.json"]["sha256"],"metadata fault was not exercised")
        return
    require({path:value for path,value in after.items() if path not in batches}=={path:value for path,value in before.items() if path not in batches}, "witness fault changed state or unrelated files")
    if mode=="missing":
        require(set(after)==set(before)-batches, "missing-witness fault retained a committed batch")
    else:
        require(set(after)==set(before) and all(after[path]["bytes"]==before[path]["bytes"] and after[path]["sha256"]!=before[path]["sha256"] for path in batches), "corrupt-witness fault did not damage each batch")


def damage(directories, root, mode):
    result={}
    for node,directory in directories.items():
        original=inventory(directory)
        if mode.startswith("state-"):
            path=directory/"state.json"
            if mode=="state-missing":path.unlink()
            else:
                require(mode=="state-corrupt","unknown metadata fault")
                value=json.loads(path.read_bytes());state=value["state"]
                require(state["base"]<state["end"],"metadata corruption must hide actual retained work")
                state["base"]=state["end"]
                with path.open("wb") as file:file.write(json.dumps(value).encode());file.flush();os.fsync(file.fileno())
        for entry in ([] if mode.startswith("state-") else original):
            if not entry["path"].endswith(".batch"):continue
            path=directory/entry["path"]
            if mode=="missing":path.unlink()
            else:
                require(mode=="corrupt", "unknown consumer witness fault")
                raw=bytearray(path.read_bytes());require(len(raw)>=72, "short original witness batch")
                raw[-1]^=1
                with path.open("wb") as file:file.write(raw);file.flush();os.fsync(file.fileno())
        after=inventory(directory);validate_damage(mode,original,after)
        archive=root/"damaged-consumer-followers"/node;shutil.copytree(directory,archive)
        require(inventory(archive)==after,"damaged witness archive changed")
        result[node]=after
    return result


def restore(directories, root, retained, mode="missing", damaged=None):
    result={}
    for node,directory in directories.items():
        before=inventory(directory)
        # Preserve the live implementation's persisted seal floor. Returning an
        # old state.json would roll it back and could conceal a fencing bug.
        names=["state.json"] if mode.startswith("state-") else [batch["path"] for batch in retained[node]["batches"]]
        if mode.startswith("state-"):require(before==damaged[node],"metadata refusal changed witness files before return")
        for name in names:
            require(Path(name).name==name and (name.endswith(".batch") or name=="state.json"),"unsafe witness return path")
            original=root/"consumer-follower-images"/node/name
            with (directory/name).open("wb") as file:file.write(original.read_bytes());file.flush();os.fsync(file.fileno())
        after=inventory(directory)
        validate_return(retained[node]["files"],before,after,mode,damaged[node] if damaged else None)
        archive=root/"returned-consumer-followers"/node;shutil.copytree(directory,archive)
        require(inventory(archive)==after,"returned witness archive changed")
        result[node]=dict(before=before,after=after,sha256={batch["path"]:hashlib.sha256((directory/batch["path"]).read_bytes()).hexdigest() for batch in retained[node]["batches"]})
    return result


def validate_return(original,before,after,mode="missing",damaged=None):
    if mode.startswith("state-"):
        require(before==damaged and after==original,"metadata return changed bytes or rolled back a newer state")
        return
    def batches(files):return {entry["path"]:entry for entry in files if entry["path"].endswith(".batch")}
    require(batches(after)==batches(original),"returned witness batch differs from original bytes")
    require([entry for entry in before if not entry["path"].endswith(".batch")]==[entry for entry in after if not entry["path"].endswith(".batch")],"witness return rewrote seal state")
