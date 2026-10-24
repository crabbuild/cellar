"""Runtime support for generated, invocation-scoped Cellar Protobuf services.

The native host owns the contract catalog, wire preflight and SDK authority.
Importing this module does not require Protobuf unless a service uses it.
"""
from __future__ import annotations
from dataclasses import dataclass, replace
from types import MappingProxyType
from typing import TYPE_CHECKING, Generic, TypeVar

if TYPE_CHECKING:
    from google.protobuf.message import Message

_Request = TypeVar("_Request", bound="Message")
_Response = TypeVar("_Response", bound="Message")

from cellar import SDKError, ValidationError


@dataclass(frozen=True)
class RpcMethod(Generic[_Request, _Response]):
    name: str
    digest: str
    handler: str
    input: type[_Request]
    output: type[_Response]
    max_message_bytes: int = 1024 * 1024
    max_depth: int = 64
    max_elements: int = 100_000
    max_decoded_slots: int = 1_000_000


class RpcRegistration:
    def __init__(self, methods):
        table = {}
        for method in methods:
            if not isinstance(method, RpcMethod) or method.name in table:
                raise TypeError("Invalid or duplicate RPC method registration")
            table[method.name] = method
        if not table:
            raise TypeError("RPC registration must contain methods")
        self.methods = MappingProxyType(table)


class _LayoutLimit(Exception):
    pass


def _check_message(message, method):
    """Bound recursive serialization before calling an allocating codec."""
    from google.protobuf.message import Message
    if not isinstance(message, Message):
        raise ValidationError("RPC values must be Protobuf messages", "InvalidArgument", "not_committed")
    remaining = method.max_elements
    slots = method.max_decoded_slots
    if type(slots) is not int or not 1 <= slots <= 4_000_000:
        raise ValidationError("Invalid RPC slot limit", "InvalidArgument", "not_committed")
    payload = 0

    def layout(count):
        nonlocal slots
        slots -= count
        if slots < 0:
            raise _LayoutLimit()

    def charge(count):
        nonlocal remaining
        remaining -= count
        layout(count)
        if remaining < 0:
            raise ValueError("RPC graph exceeds element limit")

    def scalar(field, value):
        nonlocal payload
        if field.type == field.TYPE_STRING:
            if len(value) > method.max_message_bytes:
                raise ValueError("RPC string exceeds limit")
            payload += len(value.encode("utf-8", errors="strict"))
        elif field.type == field.TYPE_BYTES:
            payload += len(value)
        if payload > method.max_message_bytes:
            raise ValueError("RPC payload exceeds limit")

    def visit(value, depth):
        if depth > method.max_depth:
            raise ValueError("RPC graph exceeds depth limit")
        layout(len(value.DESCRIPTOR.fields) + 1)
        for field, item in value.ListFields():
            if field.is_repeated:
                charge(len(item) + int(field.is_packed))
                if field.message_type is not None and field.message_type.GetOptions().map_entry:
                    if depth + 1 > method.max_depth:
                        raise ValueError("RPC map entry exceeds depth limit")
                    key_field, value_field = field.message_type.fields
                    for key in item:
                        charge(2)
                        layout(3)
                        scalar(key_field, key)
                        if value_field.type == value_field.TYPE_MESSAGE:
                            visit(item[key], depth + 2)
                        else:
                            scalar(value_field, item[key])
                elif field.type == field.TYPE_MESSAGE:
                    for child in item:
                        visit(child, depth + 1)
                else:
                    for child in item:
                        scalar(field, child)
            else:
                charge(1)
                if field.type == field.TYPE_MESSAGE:
                    visit(item, depth + 1)
                else:
                    scalar(field, item)

    try:
        visit(message, 0)
    except _LayoutLimit:
        raise ValidationError("RPC decoded layout exceeds slot limit", "ResourceExhausted", "not_committed") from None
    except (ValueError, TypeError, UnicodeError, RecursionError) as error:
        raise ValidationError("Invalid RPC message", "InvalidArgument", "not_committed") from error


def encode_message(message, method):
    _check_message(message, method)
    try:
        if message.ByteSize() > method.max_message_bytes:
            raise ValueError("RPC message exceeds limit")
        return message.SerializeToString()
    except (ValueError, TypeError, RecursionError) as error:
        raise ValidationError("Invalid RPC message", "InvalidArgument", "not_committed") from error


class RpcClient:
    """Internal transport base; generated subclasses expose concrete methods."""
    def __init__(self, context, binding, name):
        context._check()
        self._context = context
        self._binding = binding
        self._name = name

    async def _invoke(self, method, request, *, timeout_ms=None):
        self._context._check()
        budget = self._context._rpc_budget()
        if not isinstance(request, method.input):
            raise ValidationError("Incorrect RPC request type", "InvalidArgument", "not_committed")
        if timeout_ms is not None and (type(timeout_ms) is not int or not 0 <= timeout_ms <= 0xffffffff):
            raise ValidationError("Invalid RPC timeout", "InvalidArgument", "not_committed")
        if timeout_ms is not None:
            budget = budget.limit(timeout_ms)
        _remaining(budget)
        limits = self._context._verify_rpc(method)
        method = replace(method, max_message_bytes=limits["max_message_bytes"],
                         max_depth=limits["max_depth"], max_elements=limits["max_elements"],
                         max_decoded_slots=limits["max_decoded_slots"])
        _remaining(budget)
        body = encode_message(request, method)
        remaining = _remaining(budget)
        _, reply = await self._context._call(
            "cell.call", _body=body, _rpc_budget=budget, binding=self._binding, name=self._name,
            method=method.name, contract_digest=method.digest, timeout_ms=min(remaining, 0xffffffff),
        )
        _remaining(budget, "unknown")
        try:
            response = method.output.FromString(reply)
        except Exception:
            _remaining(budget, "unknown")
            raise SDKError("InvalidResponse", "InvalidResponse", "unknown") from None
        _remaining(budget, "unknown")
        return response


def _remaining(budget, outcome="not_committed"):
    """The native owner supplies time; never use a monkeypatchable guest clock."""
    try:
        return budget.remaining_ms()
    except RuntimeError as error:
        code = str(error)
        if code not in ("ContextExpired", "DeadlineExceeded", "Unavailable"):
            code = "Unavailable"
        raise SDKError(code, code, outcome) from None
