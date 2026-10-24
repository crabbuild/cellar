# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""The archive reader rejects corrupt, foreign and unsupported state records."""
import copy
import hashlib
import json
from pathlib import Path
import struct
import tempfile
import unittest

from faults import AuditError
from follower_state import read_state

LEADER="consumer/"+"c"*32+"/"+"c"*64


def state_bytes(state,leader=LEADER,node="destination"):
    payload=b"cellar-follower-state\0"+struct.pack("<I",1)
    for identity in (leader,node):payload+=struct.pack("<I",len(identity.encode()))+identity.encode()
    payload+=struct.pack("<QQQQ",*(state[key] for key in ("fragment_epoch","base","end","sealed_to")))
    return json.dumps(dict(format=1,leader=leader,node=node,state=state,checksum=hashlib.sha256(payload).hexdigest())).encode()


class FollowerStateTests(unittest.TestCase):
    def check(self,raw,leader=LEADER,node="destination"):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);(root/"state.json").write_bytes(raw)
            return read_state(root,leader,node)

    def test_valid_record_binds_every_state_and_identity_field(self):
        state=dict(fragment_epoch=1,base=1,end=2,sealed_to=1)
        raw=state_bytes(state);self.assertEqual(self.check(raw),state)
        original=json.loads(raw)
        for key in state:
            changed=copy.deepcopy(original);changed["state"][key]+=1
            with self.subTest(key=key),self.assertRaises(AuditError):self.check(json.dumps(changed).encode())
        for key in ("leader","node","checksum"):
            changed=copy.deepcopy(original);changed[key]+="x"
            with self.subTest(key=key),self.assertRaises(AuditError):self.check(json.dumps(changed).encode())
        with self.assertRaises(AuditError):self.check(raw,node="producer")
        with self.assertRaises(AuditError):self.check(raw,leader=LEADER+"x")

    def test_no_legacy_reader_or_unknown_format(self):
        state=dict(fragment_epoch=1,base=0,end=2,sealed_to=0)
        with self.assertRaises(AuditError):self.check(json.dumps(state).encode())
        for version in (0,2,True):
            value=json.loads(state_bytes(state));value["format"]=version
            with self.subTest(version=version),self.assertRaises(AuditError):self.check(json.dumps(value).encode())

    def test_invalid_shapes_duplicates_and_oversize_are_rejected(self):
        original=json.loads(state_bytes(dict(fragment_epoch=1,base=0,end=2,sealed_to=0)))
        for kind in ("extra","extra_state","number","range","zero_epoch","duplicate","large"):
            value=copy.deepcopy(original)
            if kind=="extra":value["unknown"]=1
            elif kind=="extra_state":value["state"]["unknown"]=1
            elif kind=="number":value["state"]["base"]=True
            elif kind=="range":value["state"]["base"]=3
            elif kind=="zero_epoch":value["state"]["fragment_epoch"]=0
            raw=json.dumps(value).encode()
            if kind=="duplicate":raw=raw[:-1]+b',"format":1}'
            if kind=="large":raw+=b" "*4096
            with self.subTest(kind=kind),self.assertRaises(AuditError):self.check(raw)


if __name__=="__main__":unittest.main()
