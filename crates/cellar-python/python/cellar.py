# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Async, invocation-scoped Cellar SDK. Installed by the native embedding host."""
from __future__ import annotations
import asyncio
import base64
import json as _json
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, TypeAlias, TypedDict
from types import MappingProxyType
from urllib.parse import urlsplit


# Configure once. CPython allocates recursion/circular-reference state inside
# each encode call; the encoder retains neither request values nor results.
_value_encoder = _json.JSONEncoder(allow_nan=False, separators=(",", ":"))


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
        return _value_encoder.encode(value)
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
    return _value_encoder.encode(snapshot)


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
_EMPTY_INITIAL_STORAGE = MappingProxyType({})


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

    def _parts(self):
        body = self.body
        if not isinstance(body, bytes):
            body = memoryview(body).tobytes()
        return dict(url=self.url, method=self.method, headers=self.headers, body=""), body

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
        if hasattr(self, "_forward"):
            return {"forward": self._forward}, b""
        body = self.body
        if not isinstance(body, bytes):
            body = memoryview(body).tobytes()
        return dict(status=self.status, headers=self.headers, body=""), body

    @classmethod
    def _from_parts(cls, value, body):
        return cls(body, value["status"], value["headers"])


class StreamingResponse(Response):
    """An async iterable of text/bytes, pulled one chunk at a time by the host."""
    def __init__(self, chunks, status=200, headers=()):
        super().__init__(b"", status, headers)
        self.chunks = chunks

    @classmethod
    def sse(cls, events, headers=()):
        return cls(events, headers=[("content-type", "text/event-stream"), ("cache-control", "no-cache"), *headers])


def sse_event(data: str, *, event: str | None = None, id: str | None = None, retry: int | None = None) -> str:
    result = ""
    for key, value in (("event", event), ("id", id)):
        if value is not None:
            if not isinstance(value, str) or any(c in value for c in "\r\n\0"):
                raise ValueError("Invalid SSE field")
            result += f"{key}: {value}\n"
    if retry is not None:
        if type(retry) is not int or retry < 0:
            raise ValueError("Invalid SSE retry")
        result += f"retry: {retry}\n"
    return result + "".join(f"data: {line}\n" for line in str(data).replace("\r\n", "\n").replace("\r", "\n").split("\n")) + "\n"


class WebSocket:
    """Invocation-bound facade; obtain a fresh facade in each cell event."""
    def __init__(self, context, id):
        self._context, self.id = context, id

    async def accept(self, *, hibernating=False, tags=()):
        await self._context._call("websocket.accept", id=self.id, hibernating=hibernating, tags=list(tags))

    async def send(self, data: Body):
        if isinstance(data, str):
            await self._context._call("websocket.send", id=self.id, text=data)
        elif isinstance(data, (bytes, bytearray, memoryview)):
            await self._context._call("websocket.send", id=self.id, body=base64.b64encode(bytes(data)).decode())
        else:
            raise TypeError("WebSocket data must be text or bytes")

    async def close(self, code=1000, reason=""):
        await self._context._call("websocket.close", id=self.id, code=code, reason=reason)

    async def next(self):
        value = await self._context._call("websocket.next", id=self.id)
        if "body" in value:
            value["data"] = base64.b64decode(value["body"])
        elif "text" in value:
            value["data"] = value["text"]
        return value

    async def set_attachment(self, value):
        await self._context._call("websocket.attachment.set", id=self.id, value=value)

    async def get_attachment(self):
        return await self._context._call("websocket.attachment.get", id=self.id)

    async def tags(self):
        return await self._context._call("websocket.tags", id=self.id)

    async def auto_response_timestamp(self):
        return await self._context._call("websocket.auto_response_timestamp", id=self.id)

    async def info(self):
        return await self._context._call("websocket.info", id=self.id)


class WebSocketResponse(Response):
    def __init__(self, socket: WebSocket, headers=(), run=None):
        super().__init__(b"", 101, headers)
        self.socket, self.run = socket, run

    def _completion(self):
        return {"websocket": self.socket.id, "headers": self.headers}, b""


class Storage:
    def __init__(self, context: Context) -> None:
        self._context = context

    async def read_json(self, key: str) -> Snapshot:
        value = await self._context._call("storage.read", key=key)
        return Snapshot(value["present"], value["value"])

    async def read_json_many(self, keys: Iterable[str]) -> dict[str, Snapshot]:
        """Read bounded snapshots in one host call; duplicate keys share one entry."""
        if isinstance(keys, (str, bytes)):
            raise ValidationError("keys must be an iterable of strings")
        value = await self._context._call("storage.read_many", keys=list(keys))
        return {key: Snapshot(row["present"], row["value"]) for key, row in value.items()}

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


@dataclass(frozen=True)
class ContainerCapabilities:
    """Declared operations and limits; querying does not start a sandbox."""
    format: int
    provider: str
    environment: str
    operations: tuple[str, ...]
    max_body_bytes: int
    max_resources: int | None


@dataclass(frozen=True)
class ContainerExecOutput:
    exit_code: int
    stdout: bytes
    stderr: bytes


class ContainerExec:
    """Invocation-owned streams; drain stdout and stderr concurrently."""
    def __init__(self, context: Context, resource_id: str) -> None:
        self._context, self._id = context, resource_id

    async def read_stdout(self) -> bytes:
        return base64.b64decode(await self._context._call("container.exec.read_stdout", id=self._id))

    async def read_stderr(self) -> bytes:
        return base64.b64decode(await self._context._call("container.exec.read_stderr", id=self._id))

    async def write(self, data: bytes) -> None:
        await self._context._call("container.exec.write", id=self._id, data=base64.b64encode(data).decode())

    async def close_stdin(self) -> None:
        await self._context._call("container.exec.close_stdin", id=self._id)

    async def wait(self) -> int:
        return await self._context._call("container.exec.wait", id=self._id)

    async def kill(self, signal: int = 9) -> None:
        await self._context._call("container.exec.kill", id=self._id, signal=signal)

    async def release(self) -> None:
        """Release streams without killing the process."""
        await self._context._call("container.release", id=self._id)


class ContainerSocket:
    def __init__(self, context: Context, resource_id: str) -> None:
        self._context, self._id = context, resource_id

    async def read(self) -> bytes:
        """An empty chunk marks EOF."""
        return base64.b64decode(await self._context._call("container.socket.read", id=self._id))

    async def write(self, data: bytes) -> None:
        await self._context._call("container.socket.write", id=self._id, data=base64.b64encode(data).decode())

    async def close_write(self) -> None:
        await self._context._call("container.socket.close_write", id=self._id)

    async def close(self) -> None:
        await self._context._call("container.socket.close", id=self._id)

    async def release(self) -> None:
        await self._context._call("container.release", id=self._id)


class ContainerWebSocket:
    """Event-owned outbound WebSocket, closed when the invocation ends."""
    def __init__(self, context: Context, resource_id: str, protocol: str) -> None:
        self._context, self._id, self.protocol = context, resource_id, protocol

    async def receive(self) -> dict[str, Any]:
        value = await self._context._call("container.websocket.receive", id=self._id)
        if value["type"] == "binary":
            value["data"] = base64.b64decode(value["data"])
        return value

    async def send_text(self, text: str) -> None:
        await self._context._call("container.websocket.send_text", id=self._id, text=text)

    async def send_binary(self, data: bytes) -> None:
        await self._context._call("container.websocket.send_binary", id=self._id, data=base64.b64encode(data).decode())

    async def close(self, code: int = 1000, reason: str = "") -> None:
        await self._context._call("container.websocket.close", id=self._id, code=code, reason=reason)

    async def release(self) -> None:
        await self._context._call("container.release", id=self._id)


class ContainerPort:
    def __init__(self, context: Context, port: int) -> None:
        self._context, self._port = context, port

    async def wait_ready(self, timeout_ms: int = 10000) -> None:
        await self._context._call("container.port.wait_ready", port=self._port, timeout_ms=timeout_ms)

    async def fetch(self, request: Request) -> Response:
        value = await self._context._call("container.port.fetch", port=self._port, request=request._wire())
        return Response(base64.b64decode(value["body"]), value["status"], value["headers"])

    async def connect_websocket(self, url: str, headers: Headers = ()) -> ContainerWebSocket | Response:
        value = await self._context._call("container.port.connect_websocket", port=self._port, url=url, headers=list(headers))
        if value["upgraded"]:
            return ContainerWebSocket(self._context, value["id"], value["protocol"])
        response = value["response"]
        return Response(base64.b64decode(response["body"]), response["status"], response["headers"])

    async def connect(self) -> ContainerSocket:
        resource_id = await self._context._call("container.port.connect", port=self._port)
        return ContainerSocket(self._context, resource_id)


class Container:
    """Control a root cell's configured provider through native ownership gates."""
    def __init__(self, context: Context) -> None:
        self._context = context

    async def capabilities(self) -> ContainerCapabilities:
        value = await self._context._call("container.capabilities")
        return ContainerCapabilities(value["format"], value["provider"], value["environment"],
            tuple(value["operations"]), value["max_body_bytes"], value["max_resources"])

    async def running(self) -> bool:
        return await self._context._call("container.running")

    async def start(self, *, entrypoint: list[str] | None = None, env: Headers = (),
                    enable_internet: bool = False, labels: Headers = ()) -> str:
        return await self._context._call("container.start", options=dict(
            entrypoint=entrypoint, env=list(env), enable_internet=enable_internet, labels=list(labels)))

    async def monitor(self) -> int:
        return await self._context._call("container.monitor")

    async def destroy(self) -> None:
        await self._context._call("container.destroy")

    async def signal(self, signal: int) -> None:
        await self._context._call("container.signal", signal=signal)

    async def set_inactivity_timeout(self, timeout_ms: int) -> None:
        await self._context._call("container.set_inactivity_timeout", timeout_ms=timeout_ms)

    async def exec(self, cmd: list[str], *, env: Headers = (), cwd: str | None = None,
                   user: str | None = None, combined: bool = False) -> ContainerExec:
        resource_id = await self._context._call("container.exec", options=dict(
            cmd=cmd, env=list(env), cwd=cwd, user=user, combined=combined))
        return ContainerExec(self._context, resource_id)

    async def exec_output(self, cmd: list[str], *, max_bytes: int = 1048576,
                          env: Headers = (), cwd: str | None = None,
                          user: str | None = None, combined: bool = False) -> ContainerExecOutput:
        value = await self._context._call("container.exec_output", max_bytes=max_bytes,
            options=dict(cmd=cmd, env=list(env), cwd=cwd, user=user, combined=combined))
        return ContainerExecOutput(value["exit_code"], base64.b64decode(value["stdout"]), base64.b64decode(value["stderr"]))

    def port(self, port: int) -> ContainerPort:
        return ContainerPort(self._context, port)


class _Cell:
    def __init__(self, context: Context, binding: str, name: str) -> None:
        self._context, self._binding, self._name = context, binding, name

    async def fetch_stream(self, request: Request) -> Response:
        value = await self._context._call("cell.fetch_stream", binding=self._binding, name=self._name, request=request._wire())
        return self._context._live_response(value)

    async def fetch(self, request: Request) -> Response:
        metadata, body = request._parts()
        value, body = await self._context._call(
            "cell.fetch", _body=body, binding=self._binding, name=self._name, request=metadata)
        return Response._from_parts(value, body)


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
    def __init__(self, bridge: Any, identity: dict[str, Any] | None, initial_storage=None) -> None:
        self._bridge = bridge
        self._loop = asyncio.get_running_loop()
        self._pending = {}
        self._sequence = 0
        self.identity: CellIdentity | None = CellIdentity(identity["name"], identity["class"], identity["scope"], identity["epoch"]) if identity else None
        self.initial_storage: Mapping[str, Snapshot] = (MappingProxyType({
            key: Snapshot(row["present"], row["value"]) for key, row in initial_storage.items()
        }) if initial_storage else _EMPTY_INITIAL_STORAGE)

    @property
    def storage(self) -> Storage:
        # A handle retains its invocation, but the invocation must not retain
        # the handle: that cycle otherwise delays every request's reclamation.
        return Storage(self)

    @property
    def container(self) -> Container:
        return Container(self)

    async def _call(self, operation: str, *, _body: bytes | None = None, **payload: Any) -> Any:
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
                if _body is None:
                    self._bridge.submit(sequence, operation, encoded)
                else:
                    self._bridge.submit(sequence, operation, encoded, _body)
            except BlockingIOError as error:
                raise Overloaded(outcome="not_committed") from error
            except ValueError as error:
                raise ValidationError() from error
            except RuntimeError as error:
                raise SDKError("Cellar operation was not submitted", "NotSubmitted") from error
            return await future
        finally:
            self._pending.pop(sequence, None)

    def _reply(self, sequence, payload, body=None):
        future = self._pending.get(sequence)
        if future is None or future.done():
            return
        value = _json.loads(payload)
        if value["ok"]:
            future.set_result(value["value"] if body is None else (value["value"], body))
        else:
            error = value["error"]
            code = value["code"]
            # The native envelope carries an explicit outcome. Never infer a
            # rollback from a submitted mutation's generic backend failure.
            error_type = {"Conflict": Conflict, "ValidationError": ValidationError,
                          "Overloaded": Overloaded, "Ambiguous": Ambiguous,
                          "OwnershipLost": OwnershipLost}.get(code, SDKError)
            future.set_exception(error_type(error, code, value["outcome"]))

    def _close(self, finished=False):
        if not self._pending:
            self._bridge.expire(finished)
            return
        # Revoke authority before cancelling work. Only successful managed
        # cleanup may suppress the native driver's final cancellation wakeup.
        self._bridge.expire()
        for future in tuple(self._pending.values()):
            future.cancel()
        self._pending.clear()
        if finished:
            self._bridge.expire(True)

    async def fetch_stream(self, request: Request) -> Response:
        return self._live_response(await self._call("http.fetch_stream", request=request._wire()))

    async def accept_websocket(self, response: Response) -> WebSocket:
        value = await self._call("websocket.from_response", response=response._forward)
        return self.websocket(value["id"])

    def _live_response(self, value):
        response = Response(base64.b64decode(value["body"]), value["status"], value["headers"])
        response._forward = value["forward"]
        async def chunks():
            try:
                if response.body:
                    yield response.body
                while value["stream"]:
                    chunk = await self._call("response.read", id=value["forward"])
                    if chunk["done"]:
                        break
                    yield base64.b64decode(chunk["body"])
            finally:
                await self._call("response.cancel", id=value["forward"])
        response.chunks = chunks
        async def cancel():
            await self._call("response.cancel", id=value["forward"])
        response.cancel = cancel
        return response

    async def websocket_pair(self) -> WebSocket:
        value = await self._call("websocket.pair")
        return self.websocket(value["id"])

    def websocket(self, id: str) -> WebSocket:
        return WebSocket(self, id)

    async def connect_websocket(self, url: str, protocols=()) -> WebSocket:
        value = await self._call("websocket.connect", url=url, protocols=list(protocols))
        return self.websocket(value["id"])

    async def websockets(self, tag=None) -> list[WebSocket]:
        return [self.websocket(id) for id in await self._call("websocket.list", tag=tag)]

    async def get_websocket_auto_response(self):
        return await self._call("websocket.auto_response.get")

    async def set_websocket_auto_response(self, pair):
        await self._call("websocket.auto_response", pair=pair)

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
