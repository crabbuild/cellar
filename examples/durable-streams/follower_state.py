# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Independent reader for the checksummed, identity-bound follower state record."""
import hashlib
import json
import struct

from faults import require


def read_state(directory, leader=None, node=None):
    raw=(directory/"state.json").read_bytes()
    require(len(raw)<=4096,"follower state exceeds its format bound")
    def unique(pairs):
        result={}
        for key,value in pairs:
            require(key not in result,"duplicate follower state field")
            result[key]=value
        return result
    value=json.loads(raw,object_pairs_hook=unique)
    require(set(value)=={"format","leader","node","state","checksum"} and type(value["format"]) is int and value["format"]==1,"unsupported follower state format")
    require(type(value["leader"]) is str and type(value["node"]) is str and value["node"] and (leader is None or value["leader"]==leader) and (node is None or value["node"]==node),"follower state identity mismatch")
    parts=value["leader"].split("/")
    require(len(parts)==3 and all(parts) and len(parts[1])==32 and all(c in "0123456789abcdef" for c in parts[1]),"invalid follower state session")
    state=value["state"];keys=("fragment_epoch","base","end","sealed_to")
    require(set(state)==set(keys) and all(type(state[key]) is int and 0<=state[key]<(1<<64) for key in keys) and state["base"]<=state["end"] and (state["fragment_epoch"]!=0 or state["base"]==state["end"]==0),"invalid follower state range")
    payload=b"cellar-follower-state\0"+struct.pack("<I",1)
    for key in ("leader","node"):
        encoded=value[key].encode();require(len(encoded)<=1024,"follower identity exceeds bound")
        payload+=struct.pack("<I",len(encoded))+encoded
    payload+=struct.pack("<QQQQ",*(state[key] for key in keys))
    require(value["checksum"]==hashlib.sha256(payload).hexdigest(),"follower state checksum mismatch")
    return state
