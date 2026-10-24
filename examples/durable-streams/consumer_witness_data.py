# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Damage and return retained follower files only after witness processes join."""
import hashlib
import json
import os
from pathlib import Path
import shutil

from faults import require
from whole_fleet import inventory

OVERSIZED_STATE_BYTES = 64 << 10
OVERSIZED_BATCH_BYTES = (64 << 20) + 73


def validate_damage(mode, original, damaged):
    require(mode in ("missing", "corrupt", "oversize", "state-missing", "state-corrupt", "state-oversize"), "unknown consumer witness fault")
    before={entry["path"]:entry for entry in original}
    after={entry["path"]:entry for entry in damaged}
    batches={path for path in before if path.endswith(".batch")}
    require(batches and len(before)==len(original) and len(after)==len(damaged), "missing/duplicate witness inventory")
    if mode.startswith("state-"):
        require("state.json" in before and {key:value for key,value in after.items() if key!="state.json"}=={key:value for key,value in before.items() if key!="state.json"},"metadata fault changed committed batches")
        require(set(after)==set(before)-{"state.json"} if mode=="state-missing" else set(after)==set(before) and after["state.json"]["sha256"]!=before["state.json"]["sha256"],"metadata fault was not exercised")
        if mode=="state-oversize":require(after["state.json"]["bytes"]==OVERSIZED_STATE_BYTES,"wrong oversized-state fault length")
        return
    require({path:value for path,value in after.items() if path not in batches}=={path:value for path,value in before.items() if path not in batches}, "witness fault changed state or unrelated files")
    if mode=="missing":
        require(set(after)==set(before)-batches, "missing-witness fault retained a committed batch")
    elif mode=="oversize":
        require(set(after)==set(before) and all(before[path]["bytes"]<OVERSIZED_BATCH_BYTES and after[path]["bytes"]==OVERSIZED_BATCH_BYTES and after[path]["sha256"]!=before[path]["sha256"] for path in batches), "wrong oversized-batch fault length or untouched batch")
    else:
        require(set(after)==set(before) and all(after[path]["bytes"]==before[path]["bytes"] and after[path]["sha256"]!=before[path]["sha256"] for path in batches), "corrupt-witness fault did not damage each batch")


def damage(directories, root, mode):
    result={}
    for node,directory in directories.items():
        original=inventory(directory)
        if mode.startswith("state-"):
            path=directory/"state.json"
            if mode=="state-missing":path.unlink()
            elif mode=="state-oversize":
                raw=path.read_bytes();require(len(raw)<=4096,"original follower state exceeds its format bound")
                with path.open("wb") as file:file.write(raw+b" "*(OVERSIZED_STATE_BYTES-len(raw)));file.flush();os.fsync(file.fileno())
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
            elif mode=="oversize":
                require(entry["bytes"]<OVERSIZED_BATCH_BYTES,"original follower batch exceeds its format bound")
                with path.open("r+b") as file:file.truncate(OVERSIZED_BATCH_BYTES);file.flush();os.fsync(file.fileno())
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
        # Batch return preserves the live seal floor. Metadata return separately
        # requires that failed calls wrote no intervening state at all.
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


def validate_oversized_batch(original, altered):
    """Inspect the exact original prefix and zero extension with bounded reads."""
    require(original.stat().st_size<OVERSIZED_BATCH_BYTES and altered.stat().st_size==OVERSIZED_BATCH_BYTES,"wrong oversized-batch archive length")
    with original.open("rb") as expected,altered.open("rb") as actual:
        while block:=expected.read(64<<10):
            require(actual.read(len(block))==block,"oversized-batch fault changed the original prefix")
        while block:=actual.read(64<<10):
            require(not block.strip(b"\0"),"oversized-batch fault added nonzero padding")
