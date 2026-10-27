# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Import actual generated messages and the frozen, independently checked corpus."""
import json
from pathlib import Path

from google.protobuf import duration_pb2, empty_pb2, timestamp_pb2
from profile_pb2 import AllTypes, RuntimeMappings

MESSAGES = {"Echo": AllTypes, "Ping": empty_pb2.Empty, "EchoChild": AllTypes.Child,
            "EchoTimestamp": timestamp_pb2.Timestamp, "EchoDuration": duration_pb2.Duration,
            "EchoMappings": RuntimeMappings}
VECTORS = {vector["id"]: vector for vector in json.loads(
    (Path(__file__).resolve().parents[1] / "fixtures/profile-vectors.json").read_text())}
