# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Async, invocation-scoped Cellar SDK. Installed by the native embedding host."""
from __future__ import annotations
import asyncio
import base64
import json as _json
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any, Iterable, Literal, Mapping, TypeAlias, TypedDict
from types import MappingProxyType
from urllib.parse import urlsplit


# Configure once. CPython allocates recursion/circular-reference state inside
# each encode call; the encoder retains neither request values nor results.
_value_encoder = _json.JSONEncoder(allow_nan=False, separators=(",", ":"))
_rpc_control_encoder = _json.JSONEncoder(allow_nan=False, ensure_ascii=False, separators=(",", ":"))


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


def _encode_rpc_control(value):
    """Use UTF-8 wire bytes for the shared RPC control limit across languages."""
    try:
        _check_value(value)
        return _rpc_control_encoder.encode(value)
    except (ValueError, TypeError, RecursionError) as error:
        raise ValidationError("Unsupported RPC control", "InvalidArgument", "not_committed") from error


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


@dataclass(frozen=True)
class FileStat:
    inode: int
    kind: str
    mode: int
    uid: int
    gid: int
    size: int
    nlink: int
    atime_ns: int
    mtime_ns: int
    ctime_ns: int
    version: int
    allocated_bytes: int

    @classmethod
    def _decode(cls, value):
        value = {key: value[key] for key in cls.__dataclass_fields__}
        for key in ("inode", "size", "nlink", "atime_ns", "mtime_ns", "ctime_ns", "version", "allocated_bytes"):
            value[key] = int(value[key])
        return cls(**value)


@dataclass(frozen=True)
class DirectoryEntry:
    name: bytes
    inode: int
    kind: str


def _filesystem_argument(operation, message):
    error = ValidationError(message)
    error.errno = "EINVAL"
    return _filesystem_progress(error, 0, operation, False)


def _filesystem_buffer(data, operation):
    try:
        return memoryview(data).cast("B")
    except (TypeError, ValueError, BufferError) as error:
        raise _filesystem_argument(operation, "Filesystem data must be contiguous bytes") from error


def _filesystem_encode(data, operation):
    return base64.b64encode(_filesystem_buffer(data, operation)).decode("ascii")


def _filesystem_path(path, operation):
    if isinstance(path, str):
        return path
    if isinstance(path, (bytes, bytearray, memoryview)):
        return {"bytes_base64": _filesystem_encode(path, operation)}
    raise _filesystem_argument(operation, "Filesystem paths must be text or bytes")


def _filesystem_integer(value, operation):
    if type(value) is int or type(value) is str:
        return str(value)
    raise _filesystem_argument(operation, "Filesystem integers must be exact integers or decimal strings")


def _filesystem_handle(directory, operation):
    handle = getattr(directory, "_handle", None) if isinstance(directory, (Directory, File)) else None
    if not isinstance(handle, str):
        raise _filesystem_argument(operation, "Filesystem directory argument must be a descriptor")
    return handle


def _filesystem_progress(error, completed, operation, committed):
    if not isinstance(error, SDKError) or type(error.code) is not str or not error.code or error.outcome not in ("not_committed", "locally_committed", "unknown") or type(getattr(error, "completed_bytes", 0)) is not int or not 0 <= getattr(error, "completed_bytes", 0) <= 2**53-1:
        error = SDKError("Filesystem host reply failed", "HostError", "unknown")
        error.errno = "EIO"
    if error.code == "ValidationError" and error.outcome == "not_committed" and not hasattr(error, "errno"):
        error.errno = "EINVAL"
    error.completed_bytes = completed + getattr(error, "completed_bytes", 0)
    error.operation = "filesystem." + operation
    if completed and error.outcome == "not_committed":
        error.outcome = "locally_committed" if committed else "unknown"
    return error


def _filesystem_cancelled(error, completed, operation):
    # Cancellation is control flow: retain its type, message and identity. A
    # cancelled wait cannot establish whether its last native call committed.
    previous = getattr(error, "completed_bytes", 0)
    if type(previous) is not int or not 0 <= previous <= 2**53-1:
        previous = 0
    error.code = "Cancelled"
    error.outcome = "unknown"
    error.operation = "filesystem." + operation
    error.completed_bytes = completed + previous


def _filesystem_reply(operation, value, request):
    """Validate acknowledgements before exposing values or advancing helpers."""
    def require(valid):
        if not valid:
            raise ValueError("Invalid filesystem reply")

    def record(value):
        require(type(value) is dict)

    def integer(value, maximum=2**53-1):
        require(type(value) is int and 0 <= value <= maximum)

    def decimal(value, signed=False):
        require(type(value) is str and len(value) <= 20)
        digits = value[1:] if signed and value.startswith("-") else value
        require(bool(digits) and digits.isascii() and digits.isdecimal())
        number = int(value)
        require((-(2**63) if signed else 0) <= number <= (2**63-1 if signed else 2**64-1))

    def binary(value):
        require(type(value) is str)
        decoded = base64.b64decode(value, validate=True)
        require(base64.b64encode(decoded).decode("ascii") == value)
        return len(decoded)

    def entry(value):
        record(value)
        require(0 < binary(value["name_base64"]) <= 255)
        decimal(value["inode"])
        require(value["kind"] in ("File", "Directory"))

    if operation in ("open", "creat", "openat", "opendir", "dup"):
        record(value)
        require(type(value["handle"]) is str and bool(value["handle"]))
    elif operation in ("read", "pread", "read_file"):
        record(value)
        count = binary(value["data_base64"])
        if operation != "read_file":
            integer(value["count"], request["count"])
            require(count == value["count"])
        elif request["max_bytes"] is not None:
            require(count <= request["max_bytes"])
    elif operation in ("write", "pwrite"):
        integer(value, binary(request["data_base64"]))
    elif operation == "lseek":
        decimal(value)
    elif operation in ("stat", "lstat", "fstat", "fstatat"):
        record(value)
        for key in ("inode", "size", "nlink", "version", "allocated_bytes"):
            decimal(value[key])
        for key in ("atime_ns", "mtime_ns", "ctime_ns"):
            decimal(value[key], True)
        for key in ("mode", "uid", "gid"):
            integer(value[key], 2**32-1)
        require(value["kind"] in ("File", "Directory"))
    elif operation == "edit_file":
        record(value)
        decimal(value["inode"])
        decimal(value["version"])
    elif operation == "readdir":
        if value is not None:
            entry(value)
    elif operation in ("read_page", "read_dir"):
        if operation == "read_dir":
            record(value)
        values = value if operation == "read_page" else value["entries"]
        require(type(values) is list and len(values) <= request["limit"])
        for item in values:
            entry(item)
        if operation == "read_dir" and value["next"] is not None:
            cursor = value["next"]
            record(cursor)
            require(type(cursor["scope"]) is str)
            decimal(cursor["inode"])
            decimal(cursor["position"])
    elif operation == "telldir":
        require(binary(value) == 16)
    elif operation == "collect_garbage":
        integer(value)
    elif operation == "statvfs":
        record(value)
        for key in ("logical_bytes", "allocated_bytes", "inodes", "available_logical_bytes", "available_allocated_bytes", "available_inodes"):
            decimal(value[key])
        integer(value["chunk_bytes"])
        require(value["chunk_bytes"] > 0)
    elif operation == "capabilities":
        record(value)
        record(value["limits"])
        record(value["transport"])
        for key in ("logical_uid", "logical_gid", "creation_mask"):
            integer(value[key], 2**32-1)
        for key in ("read_only", "regular_files", "directories", "positional_io", "directory_relative", "transactions", "symlinks", "hard_links", "advisory_locks", "atime"):
            require(type(value[key]) is bool)
        for key in ("max_file_bytes", "max_logical_bytes", "max_allocated_bytes", "max_inodes"):
            decimal(value["limits"][key])
        for key in ("max_transfer_bytes", "max_file_helper_bytes", "max_handles", "max_host_handles", "max_path_bytes", "max_path_depth", "max_discarded_ranges", "gc_batch_chunks", "max_transaction_operations", "max_transaction_bytes", "max_directory_entries", "max_namespace_directories"):
            integer(value["limits"][key], 2**31-1)
        for key in ("max_value_bytes", "max_body_bytes", "max_batch_operations"):
            integer(value["transport"][key])
    else:
        require(value is None)


class File:
    """Invocation-local cell descriptor; closing never implicitly fsyncs."""
    def __init__(self, filesystem, handle):
        self._fs, self._handle = filesystem, handle

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        await self.close()

    async def read(self, count):
        value = await self._fs._call("read", handle=self._handle, count=count)
        return base64.b64decode(value["data_base64"], validate=True)

    async def pread(self, count, offset):
        value = await self._fs._call("pread", handle=self._handle, count=count, offset=_filesystem_integer(offset, "pread"))
        return base64.b64decode(value["data_base64"], validate=True)

    async def write(self, data):
        return await self._fs._call("write", handle=self._handle, data_base64=_filesystem_encode(data, "write"))

    async def pwrite(self, data, offset):
        return await self._fs._call("pwrite", handle=self._handle, data_base64=_filesystem_encode(data, "pwrite"), offset=_filesystem_integer(offset, "pwrite"))

    async def write_all(self, data):
        completed = 0
        try:
            data = _filesystem_buffer(data, "write_all")
            limits = (await self._fs.capabilities())["limits"]
            limit = limits["max_file_helper_bytes"]
            if data.nbytes > limit:
                raise ValidationError("Filesystem write_all exceeds helper limit")
            if not data:
                await self.write(data)
            while data:
                count = await self.write(data[:min(65536, limits["max_transfer_bytes"])])
                if count <= 0:
                    raise SDKError("Filesystem write made no progress", "HostError", "unknown")
                completed += count
                data = data[count:]
        except asyncio.CancelledError as error:
            _filesystem_cancelled(error, completed, "write_all")
            raise
        except Exception as error:
            failure = _filesystem_progress(error, completed, "write_all", True)
            if failure is error:
                raise
            raise failure from error

    async def read_exact(self, count):
        completed = 0
        try:
            limits = (await self._fs.capabilities())["limits"]
            if type(count) is not int or not 0 <= count <= limits["max_file_helper_bytes"]:
                raise ValidationError("Filesystem read_exact exceeds helper limit")
            chunks = []
            if not count:
                await self.read(0)
            while count:
                chunk = await self.read(min(count, 65536))
                if not chunk:
                    error = SDKError("Filesystem read reached end of file", "UnexpectedEof", "not_committed")
                    error.errno = "EIO"
                    raise error
                completed += len(chunk)
                chunks.append(chunk)
                count -= len(chunk)
            return b"".join(chunks)
        except asyncio.CancelledError as error:
            _filesystem_cancelled(error, completed, "read_exact")
            raise
        except Exception as error:
            failure = _filesystem_progress(error, completed, "read_exact", False)
            if failure is error:
                raise
            raise failure from error

    async def lseek(self, offset, whence="start"):
        return int(await self._fs._call("lseek", handle=self._handle, offset=_filesystem_integer(offset, "lseek"), whence=whence))

    async def fstat(self):
        return FileStat._decode(await self._fs._call("fstat", handle=self._handle))

    async def ftruncate(self, size):
        return await self._fs._call("ftruncate", handle=self._handle, size=_filesystem_integer(size, "ftruncate"))

    async def fchmod(self, mode):
        return await self._fs._call("fchmod", handle=self._handle, mode=mode)

    async def dup(self):
        return File(self._fs, (await self._fs._call("dup", handle=self._handle))["handle"])

    async def close(self):
        return await self._fs._call("close", handle=self._handle)

    async def fsync(self):
        return await self._fs._call("fsync", handle=self._handle)

    async def fdatasync(self):
        return await self._fs._call("fdatasync", handle=self._handle)


@dataclass(frozen=True)
class FileVersion:
    inode: int
    version: int


@dataclass(frozen=True)
class DirectoryPage:
    entries: list[DirectoryEntry]
    next: dict | None


class Directory:
    def __init__(self, filesystem, handle):
        self._fs, self._handle = filesystem, handle

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        await self.closedir()

    async def readdir(self):
        value = await self._fs._call("readdir", handle=self._handle)
        return None if value is None else DirectoryEntry(base64.b64decode(value["name_base64"], validate=True), int(value["inode"]), value["kind"])

    async def read_page(self, limit):
        return [DirectoryEntry(base64.b64decode(value["name_base64"], validate=True), int(value["inode"]), value["kind"]) for value in await self._fs._call("read_page", handle=self._handle, limit=limit)]

    async def telldir(self):
        return await self._fs._call("telldir", handle=self._handle)

    async def seekdir(self, cookie):
        return await self._fs._call("seekdir", handle=self._handle, cookie=cookie)

    async def rewinddir(self):
        return await self._fs._call("rewinddir", handle=self._handle)

    async def closedir(self):
        return await self._fs._call("closedir", handle=self._handle)

    async def fstat(self):
        return FileStat._decode(await self._fs._call("fstat", handle=self._handle))

    async def fsync(self):
        return await self._fs._call("fsync", handle=self._handle)


class Filesystem:
    def __init__(self, context):
        self._context = context

    async def _call(self, operation, **value):
        try:
            reply = await self._context._call("filesystem." + operation, **value)
            _filesystem_reply(operation, reply, value)
            return reply
        except asyncio.CancelledError as error:
            _filesystem_cancelled(error, 0, operation)
            raise
        except Exception as error:
            failure = _filesystem_progress(error, 0, operation, False)
            if failure is error:
                raise
            raise failure from error

    async def capabilities(self):
        value = await self._call("capabilities")
        for key in ("max_file_bytes", "max_logical_bytes", "max_allocated_bytes", "max_inodes"):
            value["limits"][key] = int(value["limits"][key])
        return value

    async def statvfs(self):
        value = await self._call("statvfs")
        for key in ("logical_bytes", "allocated_bytes", "inodes", "available_logical_bytes", "available_allocated_bytes", "available_inodes"):
            value[key] = int(value[key])
        return value

    async def open(self, path, **options):
        return File(self, (await self._call("open", path=_filesystem_path(path, "open"), options=options))["handle"])

    async def creat(self, path, mode=0o666):
        return File(self, (await self._call("creat", path=_filesystem_path(path, "creat"), mode=mode))["handle"])

    async def openat(self, directory, path, **options):
        return File(self, (await self._call("openat", directory=_filesystem_handle(directory, "openat"), path=_filesystem_path(path, "openat"), options=options))["handle"])

    async def stat(self, path):
        return FileStat._decode(await self._call("stat", path=_filesystem_path(path, "stat")))

    async def lstat(self, path):
        return FileStat._decode(await self._call("lstat", path=_filesystem_path(path, "lstat")))

    async def fstatat(self, directory, path, no_follow=False):
        return FileStat._decode(await self._call("fstatat", directory=_filesystem_handle(directory, "fstatat"), path=_filesystem_path(path, "fstatat"), no_follow=no_follow))

    async def truncate(self, path, size):
        return await self._call("truncate", path=_filesystem_path(path, "truncate"), size=_filesystem_integer(size, "truncate"))

    async def mkdir(self, path, mode=0o777):
        return await self._call("mkdir", path=_filesystem_path(path, "mkdir"), mode=mode)

    async def mkdirat(self, directory, path, mode=0o777):
        return await self._call("mkdirat", directory=_filesystem_handle(directory, "mkdirat"), path=_filesystem_path(path, "mkdirat"), mode=mode)

    async def rmdir(self, path):
        return await self._call("rmdir", path=_filesystem_path(path, "rmdir"))

    async def unlink(self, path):
        return await self._call("unlink", path=_filesystem_path(path, "unlink"))

    async def unlinkat(self, directory, path, remove_directory=False):
        return await self._call("unlinkat", directory=_filesystem_handle(directory, "unlinkat"), path=_filesystem_path(path, "unlinkat"), remove_directory=remove_directory)

    async def rename(self, old, new):
        return await self._call("rename", old=_filesystem_path(old, "rename"), new=_filesystem_path(new, "rename"))

    async def renameat(self, old_directory, old, new_directory, new):
        return await self._call("renameat", old_directory=_filesystem_handle(old_directory, "renameat"), old=_filesystem_path(old, "renameat"), new_directory=_filesystem_handle(new_directory, "renameat"), new=_filesystem_path(new, "renameat"))

    async def chmod(self, path, mode):
        return await self._call("chmod", path=_filesystem_path(path, "chmod"), mode=mode)

    async def utimens(self, path, mtime_ns="now"):
        value = mtime_ns if mtime_ns is None or mtime_ns == "now" else _filesystem_integer(mtime_ns, "utimens")
        return await self._call("utimens", path=_filesystem_path(path, "utimens"), mtime_ns=value)

    async def opendir(self, path):
        return Directory(self, (await self._call("opendir", path=_filesystem_path(path, "opendir")))["handle"])

    async def read_file(self, path, max_bytes=None):
        return base64.b64decode((await self._call("read_file", path=_filesystem_path(path, "read_file"), max_bytes=max_bytes))["data_base64"], validate=True)

    async def write_file(self, path, data):
        return await self._call("write_file", path=_filesystem_path(path, "write_file"), data_base64=_filesystem_encode(data, "write_file"))

    async def sync(self):
        return await self._call("sync")

    async def replace_file(self, path, data):
        return await self._call("replace_file", path=_filesystem_path(path, "replace_file"), data_base64=_filesystem_encode(data, "replace_file"))

    async def edit_file(self, path, expected, *, offset, delete_bytes, insert):
        value = await self._call("edit_file", path=_filesystem_path(path, "edit_file"), inode=_filesystem_integer(getattr(expected, "inode", None), "edit_file"), version=_filesystem_integer(getattr(expected, "version", None), "edit_file"), offset=_filesystem_integer(offset, "edit_file"), delete_bytes=_filesystem_integer(delete_bytes, "edit_file"), data_base64=_filesystem_encode(insert, "edit_file"))
        return FileVersion(int(value["inode"]), int(value["version"]))

    async def read_dir(self, path, limit=100, cursor=None):
        page = await self._call("read_dir", path=_filesystem_path(path, "read_dir"), limit=limit, cursor=cursor)
        return DirectoryPage([DirectoryEntry(base64.b64decode(value["name_base64"], validate=True), int(value["inode"]), value["kind"]) for value in page["entries"]], page["next"])

    async def collect_garbage(self):
        return await self._call("collect_garbage")


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

    async def apply(self, *, checks: Mapping[str, Snapshot] | None = None, puts: Mapping[str, JSON] | None = None, deletes: Iterable[str] = (), alarm_at_ms: int | None = None, delete_alarm: bool = False, filesystem_checks=(), filesystem_mutations=()) -> None:
        checks = checks or {}
        if any(not isinstance(value, Snapshot) for value in checks.values()):
            raise ValidationError("checks must contain Snapshot values from read_json or MISSING")
        return await self._context._call(
            "storage.apply", checks={key: dict(present=s.present, value=s.value) for key, s in checks.items()},
            puts=puts or {}, deletes=list(deletes), alarm_at_ms=alarm_at_ms, delete_alarm=delete_alarm,
            filesystem_checks=list(filesystem_checks), filesystem_mutations=list(filesystem_mutations))

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
    synchronous_running: bool
    queued_lifecycle: bool
    fencing: str
    exec_pty: bool


@dataclass(frozen=True)
class ContainerExecOutput:
    exit_code: int
    stdout: bytes
    stderr: bytes


@dataclass(frozen=True)
class ContainerExecInfo:
    """Allocation metadata; pid is a verified container PID or None."""
    pid: int | None
    is_pty: bool


class ContainerExec:
    """Invocation-owned streams; drain stdout and stderr concurrently."""
    def __init__(self, context: Context, resource_id: str) -> None:
        self._context, self._id = context, resource_id

    async def info(self) -> ContainerExecInfo:
        value = await self._context._call("container.exec.info", id=self._id)
        return ContainerExecInfo(value["pid"], value["is_pty"])

    async def resize(self, cols: int, rows: int) -> None:
        await self._context._call("container.exec.resize", id=self._id, cols=cols, rows=rows)

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
    def __init__(self, context: Context, resource_id: str, protocol: str, headers: Headers = ()) -> None:
        self._context, self._id, self.protocol = context, resource_id, protocol
        self.headers = tuple(tuple(pair) for pair in headers)

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


class ContainerResponse:
    """Event-owned HTTP prefix and pull reader. Finish or cancel in this event."""

    def __init__(self, context: Context, value: dict) -> None:
        self._context, self._id = context, value["stream"]
        self.status = value["status"]
        self.headers = tuple(tuple(pair) for pair in value["headers"])
        self.body = base64.b64decode(value["body"])
        self._ended = self._id is None

    async def read(self) -> bytes | None:
        if self._ended:
            return None
        value = await self._context._call("container.response.read", id=self._id)
        self._ended = value["done"]
        return None if self._ended else base64.b64decode(value["body"])

    async def cancel(self) -> None:
        if not self._ended:
            self._ended = True
            await self._context._call("container.response.cancel", id=self._id)

    async def chunks(self):
        try:
            if self.body:
                yield self.body
            while not self._ended:
                chunk = await self.read()
                if chunk is not None:
                    yield chunk
        finally:
            await self.cancel()


class ContainerPort:
    def __init__(self, context: Context, port: int) -> None:
        self._context, self._port = context, port

    async def wait_ready(self, timeout_ms: int = 10000) -> None:
        await self._context._call("container.port.wait_ready", port=self._port, timeout_ms=timeout_ms)

    async def fetch(self, request: Request) -> Response:
        value = await self._context._call("container.port.fetch", port=self._port, request=request._wire())
        return Response(base64.b64decode(value["body"]), value["status"], value["headers"])

    async def fetch_stream(self, request: Request) -> ContainerResponse:
        value = await self._context._call("container.port.fetch_stream", port=self._port, request=request._wire())
        return ContainerResponse(self._context, value)

    async def connect_websocket(self, url: str, headers: Headers = ()) -> ContainerWebSocket | Response:
        value = await self._context._call("container.port.connect_websocket", port=self._port, url=url, headers=list(headers))
        if value["upgraded"]:
            return ContainerWebSocket(self._context, value["id"], value["protocol"], value["headers"])
        response = value["response"]
        return Response(base64.b64decode(response["body"]), response["status"], response["headers"])

    async def connect(self) -> ContainerSocket:
        resource_id = await self._context._call("container.port.connect", port=self._port)
        return ContainerSocket(self._context, resource_id)


@dataclass(frozen=True)
class ContainerFileMetadata:
    name: str
    kind: str
    size: int
    mode: int
    modified_ms: int | None
    link_target: str | None

    @classmethod
    def _from_wire(cls, value: dict) -> ContainerFileMetadata:
        return cls(value["name"], value["kind"], value["size"], value["mode"],
                   value.get("modified_ms"), value.get("link_target"))


@dataclass(frozen=True)
class ContainerReconciliation:
    """Current lifecycle evidence; native execution has no remote generation."""
    state: Literal["unbound", "running", "stopped", "deleting", "deleted"]
    generation: str | None


class Container:
    """Control a root cell's configured provider through native ownership gates."""
    def __init__(self, context: Context) -> None:
        self._context = context

    async def capabilities(self) -> ContainerCapabilities:
        value = await self._context._call("container.capabilities")
        return _container_capabilities(value)

    async def reconcile(self) -> ContainerReconciliation:
        """Observe recovery/expiry without creating, resuming or deleting compute."""
        value = await self._context._call("container.reconcile")
        return ContainerReconciliation(value["state"], value["generation"])

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

    async def intercept_outbound_http(self, target: str | None, binding: str) -> None:
        """Route plain HTTP through a declared service; requires the native helper profile."""
        await self._context._call("container.intercept_outbound_http", target=target,
                                  service=dict(kind="binding", binding=binding))

    async def exec(self, cmd: list[str], *, env: Headers = (), cwd: str | None = None,
                   user: str | None = None, combined: bool = False,
                   pty: tuple[int, int] | None = None) -> ContainerExec:
        resource_id = await self._context._call("container.exec", options=dict(
            cmd=cmd, env=list(env), cwd=cwd, user=user, combined=combined,
            pty=None if pty is None else dict(cols=pty[0], rows=pty[1])))
        return ContainerExec(self._context, resource_id)

    async def exec_output(self, cmd: list[str], *, max_bytes: int = 1048576,
                          env: Headers = (), cwd: str | None = None,
                          user: str | None = None, combined: bool = False,
                          pty: tuple[int, int] | None = None) -> ContainerExecOutput:
        value = await self._context._call("container.exec_output", max_bytes=max_bytes,
            options=dict(cmd=cmd, env=list(env), cwd=cwd, user=user, combined=combined,
                         pty=None if pty is None else dict(cols=pty[0], rows=pty[1])))
        return ContainerExecOutput(value["exit_code"], base64.b64decode(value["stdout"]), base64.b64decode(value["stderr"]))

    def port(self, port: int) -> ContainerPort:
        return ContainerPort(self._context, port)

    async def read_file(self, path: str, *, max_bytes: int = 1048576) -> bytes:
        """Read a bounded regular file from an absolute container path."""
        value = await self._context._call("container.file.read", path=path, max_bytes=max_bytes)
        return base64.b64decode(value)

    async def write_file(self, path: str, data: bytes, *, mode: int = 0o644) -> None:
        """Create/replace a file in an existing directory; not transactional."""
        await self._context._call("container.file.write", path=path,
            data=base64.b64encode(data).decode("ascii"), mode=mode)

    async def stat_file(self, path: str) -> ContainerFileMetadata:
        value = await self._context._call("container.file.stat", path=path)
        return ContainerFileMetadata._from_wire(value)

    async def list_files(self, path: str, *, max_bytes: int = 1048576,
                         max_entries: int = 128) -> tuple[ContainerFileMetadata, ...]:
        """List direct children; oversized archives/listings refuse in full."""
        value = await self._context._call("container.file.list", path=path,
                                         max_bytes=max_bytes, max_entries=max_entries)
        return tuple(ContainerFileMetadata._from_wire(entry) for entry in value)


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


class _ContainerContext:
    """Select a host-declared slot without replacing invocation authority."""
    def __init__(self, context: Context, name: str) -> None:
        self._context, self._name = context, name

    async def _call(self, operation, **value):
        return await self._context._call(operation, binding=self._name, **value)


@dataclass(frozen=True)
class ContainerBindingCapabilities:
    name: str
    capabilities: ContainerCapabilities


def _container_capabilities(value):
    return ContainerCapabilities(value["format"], value["provider"], value["environment"],
        tuple(value["operations"]), value["max_body_bytes"], value["max_resources"],
        value["synchronous_running"], value["queued_lifecycle"], value["fencing"], value["exec_pty"])


class ContainerBindings:
    def __init__(self, context: Context) -> None:
        self._context = context

    def get(self, name: str) -> Container:
        if (not isinstance(name, str) or not 1 <= len(name) <= 128
                or not all(c.isascii() and (c.isalnum() or c in "-_.") for c in name)):
            raise ValidationError("Invalid container binding", "ValidationError", "not_committed")
        return Container(self._context if name == "container" else _ContainerContext(self._context, name))

    async def list(self) -> tuple[ContainerBindingCapabilities, ...]:
        values = await self._context._call("container.bindings")
        return tuple(ContainerBindingCapabilities(value["name"], _container_capabilities(value["capabilities"])) for value in values)


class Context:
    def __init__(self, bridge: Any, identity: dict[str, Any] | None, initial_storage=None) -> None:
        self._bridge = bridge
        self._loop = asyncio.get_running_loop()
        self._pending = {}
        self._sequence = 0
        self._rpc_codecs = None
        self.identity: CellIdentity | None = CellIdentity(identity["name"], identity["class"], identity["scope"], identity["epoch"]) if identity else None
        self.initial_storage: Mapping[str, Snapshot] = (MappingProxyType({
            key: Snapshot(row["present"], row["value"]) for key, row in initial_storage.items()
        }) if initial_storage else _EMPTY_INITIAL_STORAGE)

    @property
    def fs(self) -> Filesystem:
        return Filesystem(self)

    @property
    def storage(self) -> Storage:
        # A handle retains its invocation, but the invocation must not retain
        # the handle: that cycle otherwise delays every request's reclamation.
        return Storage(self)

    @property
    def container(self) -> Container:
        return Container(self)

    @property
    def containers(self) -> ContainerBindings:
        return ContainerBindings(self)

    def _check(self):
        if asyncio.get_running_loop() is not self._loop:
            raise SDKError("Cellar SDK calls must use the invocation's asyncio loop", "NotSubmitted")
        if not self._bridge.active():
            raise SDKError("ContextExpired", "ContextExpired")
        if _current_invocation.get() is not self._bridge:
            raise ContextMismatch()

    def _verify_rpc(self, method):
        try:
            if self._rpc_codecs is None:
                raise TypeError("No native RPC contract catalog")
            return self._rpc_codecs.verify(method)
        except Exception:
            raise ValidationError("ContractMismatch", "ContractMismatch", "not_committed") from None

    def _rpc_budget(self):
        self._check()
        try:
            return self._bridge.rpc_budget()
        except RuntimeError as error:
            code = str(error)
            if code == "ContextMismatch":
                raise ContextMismatch() from None
            if code not in ("ContextExpired", "DeadlineExceeded", "Unavailable"):
                code = "Unavailable"
            raise SDKError(code, code, "not_committed") from None

    async def _call(self, operation: str, *, _body: bytes | None = None, _rpc_budget=None, **payload: Any) -> Any:
        self._check()
        self._sequence += 1
        sequence = self._sequence
        future = asyncio.get_running_loop().create_future()
        self._pending[sequence] = future
        try:
            encoded = _encode_rpc_control(payload) if operation == "cell.call" else _encode(payload)
            try:
                if _rpc_budget is not None:
                    self._bridge.submit(sequence, operation, encoded, _body, rpc_budget=_rpc_budget)
                elif _body is None:
                    self._bridge.submit(sequence, operation, encoded)
                else:
                    self._bridge.submit(sequence, operation, encoded, _body)
            except BlockingIOError as error:
                raise Overloaded(outcome="not_committed") from error
            except ValueError as error:
                if operation == "cell.call":
                    raise ValidationError("Invalid RPC control", "InvalidArgument", "not_committed") from error
                raise ValidationError() from error
            except RuntimeError as error:
                if str(error) == "ContextMismatch":
                    raise ContextMismatch() from None
                raise SDKError("Cellar operation was not submitted", "NotSubmitted") from error
            if operation != "cell.call":
                return await future
            # A task's cancellation must reach the original native call. Keep
            # its reply future alive until native cleanup releases admission;
            # cancelling a local Future alone would leave the mutation running.
            try:
                await asyncio.wait((future,))
                return future.result()
            except asyncio.CancelledError as cancelled:
                if not self._bridge.active():
                    future.cancel()
                    raise
                try:
                    self._bridge.cancel_operation(sequence)
                except RuntimeError:
                    # Native retirement can race the active check. It already
                    # owns revocation and no longer promises another reply.
                    future.cancel()
                    raise cancelled from None
                while not future.done():
                    try:
                        await asyncio.wait((future,))
                    except asyncio.CancelledError:
                        # Repeated Task.cancel() must not abandon owned native
                        # cleanup. Whole-invocation retirement is authoritative.
                        if not self._bridge.active():
                            future.cancel()
                if not future.cancelled():
                    # Retrieve even an already-ready failure. The Python task
                    # still reports cancellation, which makes no rollback claim.
                    future.exception()
                raise
        finally:
            self._pending.pop(sequence, None)

    def _reply(self, sequence, payload, body=None):
        future = self._pending.get(sequence)
        if future is None or future.done():
            return
        try:
            value = _json.loads(payload)
            if type(value) is not dict or type(value["ok"]) is not bool:
                raise ValueError("Invalid Cellar reply")
            if value["ok"]:
                result = value["value"] if body is None else (value["value"], body)
            else:
                error, code, outcome = value["error"], value["code"], value["outcome"]
                completed = value.get("completed_bytes", 0)
                if type(error) is not str or type(code) is not str or not code or outcome not in ("not_committed", "locally_committed", "unknown") or type(completed) is not int or not 0 <= completed < 2**64 or (value.get("errno") is not None and type(value["errno"]) is not str) or (value.get("operation") is not None and type(value["operation"]) is not str):
                    raise ValueError("Invalid Cellar failure reply")
                # A valid native envelope owns its evidenced outcome and cause.
                error_type = {"Conflict": Conflict, "ValidationError": ValidationError,
                              "Overloaded": Overloaded, "Ambiguous": Ambiguous,
                              "OwnershipLost": OwnershipLost}.get(code, SDKError)
                failure = error_type(error, code, outcome)
                failure.errno = value.get("errno")
                failure.operation = value.get("operation")
                failure.completed_bytes = completed
        except Exception as cause:
            # Settlement must reach the waiter even when metadata decoding
            # fails. A corrupt reply cannot establish mutation rollback.
            failure = SDKError("Invalid Cellar host reply", "HostError", "unknown")
            failure.errno = "EIO"
            failure.__cause__ = cause
            future.set_exception(failure)
            return
        if value["ok"]:
            future.set_result(result)
        else:
            future.set_exception(failure)

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
