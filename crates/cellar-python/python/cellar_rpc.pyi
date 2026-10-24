# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Static surface for the `cellar_rpc` module installed by the embedding host.

The runtime source is rpc.py. Type variables keep generated service method
results concrete while registrations can hold several request/response pairs.
"""
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any, Generic, TypeVar

from cellar import Context
from google.protobuf.message import Message

_Request = TypeVar("_Request", bound=Message)
_Response = TypeVar("_Response", bound=Message)

@dataclass(frozen=True)
class RpcMethod(Generic[_Request, _Response]):
    name: str
    digest: str
    handler: str
    input: type[_Request]
    output: type[_Response]
    max_message_bytes: int = ...
    max_depth: int = ...
    max_elements: int = ...
    max_decoded_slots: int = ...

class RpcRegistration:
    methods: Mapping[str, RpcMethod[Any, Any]]
    def __init__(self, methods: Iterable[RpcMethod[Any, Any]]) -> None: ...

def encode_message(message: Message, method: RpcMethod[Any, Any]) -> bytes: ...

class RpcClient:
    def __init__(self, context: Context, binding: str, name: str) -> None: ...
    async def _invoke(self, method: RpcMethod[_Request, _Response], request: _Request, *, timeout_ms: int | None = None) -> _Response: ...
