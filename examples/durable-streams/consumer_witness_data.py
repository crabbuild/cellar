"""Damage and return only real committed batches while witness processes are joined."""
import hashlib
import os
from pathlib import Path
import shutil

from faults import require
from whole_fleet import inventory


def validate_damage(mode, original, damaged):
    require(mode in ("missing", "corrupt"), "unknown consumer witness fault")
    before={entry["path"]:entry for entry in original}
    after={entry["path"]:entry for entry in damaged}
    batches={path for path in before if path.endswith(".batch")}
    require(batches and len(before)==len(original) and len(after)==len(damaged), "missing/duplicate witness inventory")
    require({path:value for path,value in after.items() if path not in batches}=={path:value for path,value in before.items() if path not in batches}, "witness fault changed state or unrelated files")
    if mode=="missing":
        require(set(after)==set(before)-batches, "missing-witness fault retained a committed batch")
    else:
        require(set(after)==set(before) and all(after[path]["bytes"]==before[path]["bytes"] and after[path]["sha256"]!=before[path]["sha256"] for path in batches), "corrupt-witness fault did not damage each batch")


def damage(directories, root, mode):
    result={}
    for node,directory in directories.items():
        original=inventory(directory)
        for entry in original:
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


def restore(directories, root, retained):
    result={}
    for node,directory in directories.items():
        before=inventory(directory)
        # Preserve the live implementation's persisted seal floor. Returning an
        # old state.json would roll it back and could conceal a fencing bug.
        for batch in retained[node]["batches"]:
            name=batch["path"];require(Path(name).name==name and name.endswith(".batch"),"unsafe witness batch path")
            original=root/"consumer-follower-images"/node/name
            with (directory/name).open("wb") as file:file.write(original.read_bytes());file.flush();os.fsync(file.fileno())
        after=inventory(directory)
        validate_return(retained[node]["files"],before,after)
        archive=root/"returned-consumer-followers"/node;shutil.copytree(directory,archive)
        require(inventory(archive)==after,"returned witness archive changed")
        result[node]=dict(before=before,after=after,sha256={batch["path"]:hashlib.sha256((directory/batch["path"]).read_bytes()).hexdigest() for batch in retained[node]["batches"]})
    return result


def validate_return(original,before,after):
    def batches(files):return {entry["path"]:entry for entry in files if entry["path"].endswith(".batch")}
    require(batches(after)==batches(original),"returned witness batch differs from original bytes")
    require([entry for entry in before if not entry["path"].endswith(".batch")]==[entry for entry in after if not entry["path"].endswith(".batch")],"witness return rewrote seal state")
