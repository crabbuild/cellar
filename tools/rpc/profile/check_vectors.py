# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Check the shared corpus with the independent protoc Python message runtime."""
import json
import sys
from pathlib import Path

from google.protobuf import duration_pb2, empty_pb2, json_format, timestamp_pb2

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "fixtures/profile-generated/python"))
from profile_pb2 import AllTypes, RuntimeMappings

MESSAGES = {
    "Echo": AllTypes,
    "Ping": empty_pb2.Empty,
    "EchoChild": AllTypes.Child,
    "EchoTimestamp": timestamp_pb2.Timestamp,
    "EchoDuration": duration_pb2.Duration,
    "EchoMappings": RuntimeMappings,
}


def check():
    vectors = json.loads((ROOT / "fixtures/profile-vectors.json").read_text())
    assert len(vectors) == 18
    assert {vector["method"] for vector in vectors} == MESSAGES.keys()
    for vector in vectors:
        message = MESSAGES[vector["method"]]()
        json_format.ParseDict(vector["json"], message)
        assert json_format.MessageToDict(message) == vector["canonical"], vector["id"]
        wire = MESSAGES[vector["method"]].FromString(bytes(vector["bytes"]))
        assert json_format.MessageToDict(wire) == vector["canonical"], vector["id"]
        # Verify Python's own writer independently; map entry ordering may differ.
        decoded = MESSAGES[vector["method"]].FromString(message.SerializeToString())
        assert json_format.MessageToDict(decoded) == vector["canonical"], vector["id"]
    print(f"Verified {len(vectors)} portable-profile vectors with protoc Python")


if __name__ == "__main__":
    check()
