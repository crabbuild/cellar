"""Async, invocation-scoped Cellar SDK. Installed by the native embedding host."""
from __future__ import annotations
import asyncio
import base64
import json as _json
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, TypeAlias, TypedDict
from urllib.parse import urlsplit


JSON: TypeAlias = type(None) | bool | int | float | str | list["JSON"] | dict[str, "JSON"]
Headers: TypeAlias = Iterable[tuple[str, str]]
Body: TypeAlias = str | bytes | bytearray | memoryview


class LocalSQLResult(TypedDict):
    columns: list[str]
    rows: list[list[JSON]]
    written: int


@dataclass(frozen=True)
class CellIdentity:
    name: str
    class_name: str
    scope: str
    epoch: int


# This is an additional caller-scope check. Authority still comes from the
# original Rust invocation and its native SDK checks, never this variable.
_current_invocation = ContextVar("cellar_invocation", default=None)


# Reuse the validator: a per-call recursive closure creates cyclic garbage.
def _check_value(item, depth=0):
    if depth > 64:
        raise ValueError("Cellar values are limited to 64 nesting levels")
    kind = type(item)
    if kind is str or kind is bool or item is None or kind is float:
        return
    if isinstance(item, int) and not -(2**63) <= item < 2**64:
        raise ValueError("Cellar integers must fit signed or unsigned 64 bits")
    # A container can advertise an integer __class__. Only exact scalars may
    # skip the original container checks; subclasses retain their validation.
    if kind is int:
        return
    if isinstance(item, dict):
        if any(not isinstance(key, str) for key in item):
            raise TypeError("Cellar object keys must be strings")
        for child in item.values():
            _check_value(child, depth + 1)
    elif isinstance(item, (list, tuple)):
        for child in item:
            _check_value(child, depth + 1)


def _encode(value):
    try:
        _check_value(value)
        return _json.dumps(value, allow_nan=False, separators=(",", ":"))
    except (ValueError, TypeError, RecursionError) as error:
        raise ValidationError("Unsupported Cellar value") from error


def _encode_response(value):
    """Encode ordinary HTTP metadata without recursively checking every scalar."""
    if type(value) is not dict:
        return _encode(value)
    snapshot = value.copy()
    if any(type(key) is not str for key in snapshot) or snapshot.keys() != {"status", "headers", "body"}:
        return _encode(value)
    status, headers, body = snapshot["status"], snapshot["headers"], snapshot["body"]
    if (type(status) is not int or not -(2**63) <= status < 2**64
            or type(body) is not str
            or (type(headers) is not list and type(headers) is not tuple) or len(headers) > 128):
        return _encode(value)
    # Own each container before inspecting its members. Another application
    # thread may mutate the original headers on a free-threaded interpreter.
    rows = []
    for row in tuple(headers):
        if type(row) is not list and type(row) is not tuple:
            return _encode(value)
        pair = tuple(row)
        if len(pair) != 2 or type(pair[0]) is not str or type(pair[1]) is not str:
            return _encode(value)
        rows.append(pair)
    snapshot["headers"] = rows
    # This shape is shallow and contains only checked, immutable scalars.
    # Keep key order and ASCII escaping: native admission charges these bytes.
    # HTTP status/header validation still belongs to the native output boundary.
    return _json.dumps(snapshot, allow_nan=False, separators=(",", ":"))


class SDKError(Exception):
    """A host operation failed. Mutations are never automatically retried."""

    def __init__(self, message: str, code: str = "HostError", outcome: str | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.outcome = outcome or ("not_committed" if code in ("Conflict", "ContextExpired", "ContextMismatch", "NotSubmitted", "ValidationError") else "unknown")


class ValidationError(SDKError, ValueError):
    """An invalid value or operation was refused before applying effects."""

    def __init__(self, message: str = "Invalid Cellar operation", code: str = "ValidationError", outcome: str | None = None) -> None:
        super().__init__(message, code, outcome)


class Overloaded(SDKError):
    """Capacity was exhausted. Inspect outcome before deciding how to retry."""

    def __init__(self, message: str = "Cellar capacity exhausted", code: str = "Overloaded", outcome: str | None = None) -> None:
        super().__init__(message, code, outcome)


class Ambiguous(SDKError):
    """A submitted mutation failed without establishing whether it committed."""

    def __init__(self, message: str = "Cellar mutation outcome is unknown", code: str = "Ambiguous", outcome: str | None = None) -> None:
        super().__init__(message, code, outcome)


class OwnershipLost(SDKError):
    """Native ownership was lost; this does not establish mutation rollback."""

    def __init__(self, message: str = "Cellar ownership lost", code: str = "OwnershipLost", outcome: str | None = None) -> None:
        super().__init__(message, code, outcome)


class ContextMismatch(SDKError):
    """A live context was used by a different invocation."""

    def __init__(self, message: str = "Cellar context belongs to another invocation") -> None:
        super().__init__(message, "ContextMismatch")


class Conflict(SDKError):
    """A checked storage batch failed without applying its mutations."""


@dataclass(frozen=True)
class Snapshot:
    present: bool
    value: JSON = None


MISSING = Snapshot(False)


class Request:
    def __init__(self, url: str, method: str = "GET", body: Body = b"", headers: Headers = ()) -> None:
        self.url = url
        self.method = method
        self.body = body.encode() if isinstance(body, str) else bytes(body)
        self.headers = list(headers)

    @property
    def path(self) -> str:
        return urlsplit(self.url).path

    async def json(self) -> JSON:
        return _json.loads(self.body)

    async def text(self) -> str:
        return self.body.decode()

    def _wire(self):
        return dict(url=self.url, method=self.method, headers=self.headers,
                    body=base64.b64encode(self.body).decode())

    @classmethod
    def _from_wire(cls, value, body):
        return cls(value["url"], value["method"], body, value["headers"])


class Response:
    def __init__(self, body: Body = b"", status: int = 200, headers: Headers = ()) -> None:
        self.body = body.encode() if isinstance(body, str) else bytes(body)
        self.status = status
        self.headers = list(headers)

    @classmethod
    def json(cls, value: JSON, status: int = 200) -> Response:
        return cls(_encode(value), status, [("content-type", "application/json")])

    def _wire(self):
        return dict(status=self.status, headers=self.headers,
                    body=base64.b64encode(self.body).decode())

    def _completion(self):
        body = self.body
        if not isinstance(body, bytes):
            body = memoryview(body).tobytes()
        return dict(status=self.status, headers=self.headers, body=""), body

    @classmethod
    def _from_wire(cls, value):
        return cls(base64.b64decode(value["body"]), value["status"], value["headers"])


class Storage:
    def __init__(self, context: Context) -> None:
        self._context = context

    async def read_json(self, key: str) -> Snapshot:
        value = await self._context._call("storage.read", key=key)
        return Snapshot(value["present"], value["value"])

    async def get_json(self, key: str, default: JSON = None) -> JSON:
        snapshot = await self.read_json(key)
        return snapshot.value if snapshot.present else default

    async def apply(self, *, checks: Mapping[str, Snapshot] | None = None, puts: Mapping[str, JSON] | None = None, deletes: Iterable[str] = (), alarm_at_ms: int | None = None, delete_alarm: bool = False) -> None:
        checks = checks or {}
        if any(not isinstance(value, Snapshot) for value in checks.values()):
            raise ValidationError("checks must contain Snapshot values from read_json or MISSING")
        return await self._context._call(
            "storage.apply", checks={key: dict(present=s.present, value=s.value) for key, s in checks.items()},
            puts=puts or {}, deletes=list(deletes), alarm_at_ms=alarm_at_ms, delete_alarm=delete_alarm)

    async def put_json(self, key: str, value: JSON) -> None:
        await self.apply(puts={key: value})

    async def delete(self, key: str) -> None:
        await self.apply(deletes=[key])

    async def sql(self, sql: str, params: Iterable[JSON] = ()) -> LocalSQLResult:
        return await self._context._call("storage.sql", sql=sql, params=list(params))

    async def sync(self) -> None:
        await self._context._call("storage.sync")

    async def get_alarm(self) -> int | None:
        return await self._context._call("alarm.get")

    async def set_alarm(self, time_ms: int) -> None:
        await self.apply(alarm_at_ms=time_ms)

    async def delete_alarm(self) -> None:
        await self.apply(delete_alarm=True)


class _Cell:
    def __init__(self, context: Context, binding: str, name: str) -> None:
        self._context, self._binding, self._name = context, binding, name

    async def fetch(self, request: Request) -> Response:
        return Response._from_wire(await self._context._call(
            "cell.fetch", binding=self._binding, name=self._name, request=request._wire()))


class _Namespace:
    def __init__(self, context: Context, binding: str) -> None:
        self._context, self._binding = context, binding

    def get_by_name(self, name: str) -> _Cell:
        return _Cell(self._context, self._binding, name)


class _KV:
    def __init__(self, context: Context, binding: str) -> None:
        self._context, self._binding = context, binding

    async def get(self, key: str) -> JSON:
        return await self._context._call("kv.get", binding=self._binding, key=key)

    async def put(self, key: str, value: JSON, *, ttl_seconds: int | None = None, metadata: JSON = None) -> None:
        await self._context._call("kv.put", binding=self._binding, key=key, value=value,
                                  ttl_seconds=ttl_seconds, metadata=metadata)

    async def delete(self, key: str) -> None:
        await self._context._call("kv.delete", binding=self._binding, key=key)

    async def list(self, *, prefix: str = "", limit: int | None = None, cursor: str | None = None) -> dict[str, JSON]:
        return await self._context._call("kv.list", binding=self._binding, prefix=prefix, limit=limit, cursor=cursor)


class _SQL:
    def __init__(self, context: Context, binding: str) -> None:
        self._context, self._binding = context, binding

    async def query(self, sql: str, params: Iterable[JSON] = ()) -> dict[str, JSON]:
        return await self._context._call("sql.query", binding=self._binding, sql=sql, params=list(params))


class _Queue:
    def __init__(self, context: Context, binding: str) -> None:
        self._context, self._binding = context, binding

    async def send(self, body: JSON, *, delay_seconds: int | None = None) -> None:
        await self._context._call("queue.send", binding=self._binding, body=body, delay_seconds=delay_seconds)


class Message:
    def __init__(self, context: Context, value: dict[str, Any]) -> None:
        self._context = context
        self.id, self.body = value["id"], value["body"]
        self.attempts, self.timestamp_ms = value["attempts"], value["timestamp_ms"]

    async def ack(self) -> None:
        await self._context._call("queue.settle", id=self.id, retry=False)

    async def retry(self, delay_seconds: int | None = None) -> None:
        await self._context._call("queue.settle", id=self.id, retry=True, delay_seconds=delay_seconds)


class Batch:
    def __init__(self, context: Context, value: dict[str, Any]) -> None:
        self.queue = value["queue"]
        self.messages = tuple(Message(context, message) for message in value["messages"])
        self._context = context

    async def ack_all(self) -> None:
        await self._context._call("queue.settle", retry=False)

    async def retry_all(self, delay_seconds: int | None = None) -> None:
        await self._context._call("queue.settle", retry=True, delay_seconds=delay_seconds)


class Scheduled:
    def __init__(self, context: Context, value: dict[str, Any]) -> None:
        self.scheduled_ms, self.cron = value["scheduled_ms"], value["cron"]
        self._context = context

    async def no_retry(self) -> None:
        await self._context._call("cron.no_retry")


class Context:
    def __init__(self, bridge: Any, identity: dict[str, Any] | None) -> None:
        self._bridge = bridge
        self._loop = asyncio.get_running_loop()
        self._pending = {}
        self._sequence = 0
        self.identity: CellIdentity | None = CellIdentity(identity["name"], identity["class"], identity["scope"], identity["epoch"]) if identity else None
        self.storage = Storage(self)

    async def _call(self, operation: str, **payload: Any) -> Any:
        if asyncio.get_running_loop() is not self._loop:
            raise SDKError("Cellar SDK calls must use the invocation's asyncio loop", "NotSubmitted")
        if not self._bridge.active():
            raise SDKError("ContextExpired", "ContextExpired")
        if _current_invocation.get() is not self._bridge:
            raise ContextMismatch()
        self._sequence += 1
        sequence = self._sequence
        future = asyncio.get_running_loop().create_future()
        self._pending[sequence] = future
        try:
            encoded = _encode(payload)
            try:
                self._bridge.submit(sequence, operation, encoded)
            except BlockingIOError as error:
                raise Overloaded(outcome="not_committed") from error
            except ValueError as error:
                raise ValidationError() from error
            except RuntimeError as error:
                raise SDKError("Cellar operation was not submitted", "NotSubmitted") from error
            return await future
        finally:
            self._pending.pop(sequence, None)

    def _reply(self, sequence, payload):
        future = self._pending.get(sequence)
        if future is None or future.done():
            return
        value = _json.loads(payload)
        if value["ok"]:
            future.set_result(value["value"])
        else:
            error = value["error"]
            code = value["code"]
            # The native envelope carries an explicit outcome. Never infer a
            # rollback from a submitted mutation's generic backend failure.
            error_type = {"Conflict": Conflict, "ValidationError": ValidationError,
                          "Overloaded": Overloaded, "Ambiguous": Ambiguous,
                          "OwnershipLost": OwnershipLost}.get(code, SDKError)
            future.set_exception(error_type(error, code, value["outcome"]))

    def _close(self):
        self._bridge.expire()
        for future in tuple(self._pending.values()):
            future.cancel()
        self._pending.clear()

    async def now_ms(self) -> int:
        return await self._call("clock.now")

    async def var(self, name: str) -> str | None:
        return await self._call("var", name=name)

    def namespace(self, binding: str) -> _Namespace:
        return _Namespace(self, binding)

    def kv(self, binding: str) -> _KV:
        return _KV(self, binding)

    def sql(self, binding: str) -> _SQL:
        return _SQL(self, binding)

    def queue(self, binding: str) -> _Queue:
        return _Queue(self, binding)
