# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Bounded cell file tools with atomic receipts or an existing agent journal."""
import base64
import json
import math
import re
from types import MappingProxyType

from cellar import SDKError, Snapshot, ValidationError, _encode


def _require(condition, message):
    if not condition:
        raise ValidationError(message)


def _clone(value, depth=0, *, floats=False):
    _require(depth <= 16, "File tool value exceeds nesting limit")
    kind = type(value)
    if value is None or kind in (str, bool):
        return value
    if kind is int:
        _require(-(2**63) <= value < 2**64, "File tool integer exceeds wire bounds")
        return value
    if kind is float and floats:
        _require(math.isfinite(value), "File tool values must be finite")
        return value
    if kind in (bytes, bytearray, memoryview):
        return {"bytes_base64": base64.b64encode(bytes(value)).decode("ascii")}
    if kind is list:
        _require(len(value) <= 128, "File tool array exceeds limit")
        return [_clone(child, depth + 1, floats=floats) for child in value.copy()]
    _require(kind is dict and len(value) <= 64, "File tool values must be bounded JSON objects")
    snapshot = value.copy()
    _require(all(type(key) is str for key in snapshot), "File tool object keys must be strings")
    return {key: _clone(snapshot[key], depth + 1, floats=floats) for key in sorted(snapshot)}


def _json(value):
    return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":"), sort_keys=True)


def _utf8(value):
    try:
        return value.encode("utf-8")
    except UnicodeEncodeError:
        raise ValidationError("File tool text must be valid UTF-8") from None


def _decimal(value=0):
    _require(type(value) in (str, int) and re.fullmatch(r"[0-9]+", str(value)) is not None,
             "Expected a nonnegative exact filesystem integer")
    digits = str(value).lstrip("0") or "0"
    _require(len(digits) <= 19, "Filesystem integer exceeds i64")
    number = int(digits)
    _require(number <= 2**63 - 1, "Filesystem integer exceeds i64")
    return number


def _path(value):
    if type(value) is dict and value.keys() == {"bytes_base64"}:
        try:
            return base64.b64decode(value["bytes_base64"], validate=True)
        except (ValueError, TypeError):
            raise ValidationError("Invalid base64 byte path") from None
    _require(type(value) in (str, bytes), "Expected a text or byte path")
    return value


def _wire_path(value):
    return value if type(value) is str else {"bytes_base64": base64.b64encode(value).decode("ascii")}


def _text(value):
    try:
        return value.decode("utf-8")
    except UnicodeDecodeError:
        return None


def _file_check(path, stat):
    return {"path": _wire_path(path), "present": False} if stat is None else {
        "path": _wire_path(path), "present": True, "inode": str(stat.inode), "version": str(stat.version)}


def _stat(stat):
    return dict(inode=str(stat.inode), version=str(stat.version), kind=stat.kind,
                size=str(stat.size), mode=stat.mode, mtime_ns=str(stat.mtime_ns))


def _entry(entry):
    text = _text(entry.name)
    return dict(name_base64=base64.b64encode(entry.name).decode("ascii"), inode=str(entry.inode),
                kind=entry.kind, **({"name": text} if text is not None else {}))


def filesystem_tool_refusal(error):
    """Return a receipt value only when the SDK proves this operation committed nothing."""
    if (isinstance(error, SDKError) and error.outcome == "not_committed"
            and error.code in ("Filesystem", "Conflict", "ValidationError")):
        return {"error": {"code": error.code, "errno": getattr(error, "errno", None), "outcome": "not_committed"}}
    return None


def validate_filesystem_checkpoint(batch, transport, *, journal_keys=(), reserve_bytes=1024):
    """Admit JSON metadata and future reads before submitting file effects.

    Pending monolithic journals reserve space for a checked terminal update;
    terminal writes omit journal_keys. max_body_bytes bounds binary bodies,
    while storage.apply JSON is charged against max_value_bytes.
    """
    maximum = transport.get("max_value_bytes")
    _require(type(maximum) is int and maximum >= 4096, "Invalid filesystem checkpoint transport budget")
    _require(type(reserve_bytes) is int and reserve_bytes >= 0, "Invalid filesystem checkpoint reserve")
    def size(value):
        return len(_utf8(_json(value)))

    # Match Storage.apply's emitted defaults and Snapshot conversion exactly.
    wire = dict(checks={}, puts={}, deletes=[], alarm_at_ms=None, delete_alarm=False,
                filesystem_checks=[], filesystem_mutations=[])
    wire.update(batch)
    _require(all(isinstance(value, Snapshot) for value in wire["checks"].values()),
             "Checkpoint checks must contain storage snapshots")
    wire["checks"] = {key: dict(present=value.present, value=value.value)
                      for key, value in wire["checks"].items()}
    # The Python bridge escapes non-ASCII in request JSON. Native read replies
    # use UTF-8 JSON; account for those two encodings separately.
    _require(len(_utf8(_encode(wire))) <= maximum, "Filesystem checkpoint exceeds the transport budget")
    puts = batch.get("puts", {})
    for value in puts.values():
        _require(size({"ok": True, "value": {"present": True, "value": value}}) <= maximum,
                 "Filesystem checkpoint exceeds the storage read budget")
    journal = {}
    for key in journal_keys:
        _require(key in puts, "Filesystem journal key must be written by the checkpoint")
        journal[key] = puts[key]
    if journal:
        checks = {key: {"present": True, "value": value} for key, value in journal.items()}
        _require(len(_utf8(_encode({"checks": checks, "puts": journal}))) + reserve_bytes <= maximum,
                 "Filesystem journal leaves no room for a checked recovery checkpoint")


class FilesystemTools:
    """An invocation-scoped facade. Stable IDs originate in saved model decisions.

    execute commits effects, local checkpoint and receipt atomically, then syncs.
    prepare commits nothing: its result and ALL returned checks/effects must join
    the caller's existing journal transaction. Replay the journal first. Unknown
    outcomes propagate; neither path retries a submitted file mutation.
    """
    def __init__(self, context, **options):
        limits = dict(prefix="filesystem-tool:", max_receipts=256, max_read_bytes=32768,
                      max_write_bytes=32768, max_entries=128, max_search_files=16,
                      max_search_directories=16, max_search_bytes=131072, max_search_depth=8,
                      max_request_bytes=131072, max_result_bytes=131072)
        _require(options.keys() <= limits.keys(), "Unknown file tool limit")
        limits.update(options)
        prefix = limits["prefix"]
        _require(type(prefix) is str and 0 < len(prefix) <= 128 and not prefix.startswith("__sys_"),
                 "Invalid file tool receipt prefix")
        for key, value in limits.items():
            if key != "prefix":
                _require(type(value) is int and value > 0, "Invalid file tool limit")
        self._ctx = context
        self.limits = MappingProxyType(limits)

    def _request(self, request):
        saved = _clone(request)
        _require(type(saved) is dict and len(_utf8(_json(saved))) <= self.limits["max_request_bytes"],
                 "File tool request exceeds receipt limit")
        return saved

    async def _stat_or_missing(self, path):
        try:
            return await self._ctx.fs.stat(path)
        except SDKError as error:
            if getattr(error, "errno", None) == "ENOENT":
                return None
            raise

    async def _read(self, path, count, offset=0):
        async with await self._ctx.fs.open(path) as file:
            stat = await file.fstat()
            data = await file.pread(count, offset)
        return stat, data, _file_check(path, stat)

    async def _prepare(self, request):
        path = _path(request.get("path", "/"))
        operation = request.get("operation")
        checks, mutations = [], []
        if operation == "read":
            count = request.get("max_bytes", self.limits["max_read_bytes"])
            _require(type(count) is int and 0 < count <= self.limits["max_read_bytes"], "Read tool exceeds byte limit")
            offset = _decimal(request.get("offset", 0))
            stat, data, check = await self._read(path, count, offset)
            checks.append(check)
            text = _text(data)
            result = dict(_stat(stat), offset=str(offset), data_base64=base64.b64encode(data).decode("ascii"),
                          truncated=offset + len(data) < stat.size, **({"text": text} if text is not None else {}))
        elif operation in ("write", "replace"):
            content = request.get("content")
            _require(type(content) is str, "Write tool requires text content")
            data = _utf8(content)
            _require(len(data) <= self.limits["max_write_bytes"], "Write tool exceeds byte limit")
            checks.append(_file_check(path, await self._stat_or_missing(path)))
            mutations.append(dict(op="write_file" if operation == "write" else "replace_file",
                                  path=_wire_path(path), data_base64=base64.b64encode(data).decode("ascii")))
            result = {"written_bytes": len(data)}
        elif operation == "edit":
            insert = request.get("insert")
            _require(type(insert) is str, "Edit tool requires text insertion")
            data = _utf8(insert)
            _require(len(data) <= self.limits["max_write_bytes"], "Edit tool exceeds byte limit")
            inode, version = _decimal(request.get("inode", 0)), _decimal(request.get("version", 0))
            mutations.append(dict(op="edit_file", path=_wire_path(path), inode=str(inode), version=str(version),
                                  offset=str(_decimal(request.get("offset", 0))),
                                  delete_bytes=str(_decimal(request.get("delete_bytes", 0))),
                                  data_base64=base64.b64encode(data).decode("ascii")))
            result = dict(expected_inode=str(inode), expected_version=str(version), inserted_bytes=len(data))
        elif operation == "stat":
            stat = await self._ctx.fs.stat(path)
            checks.append(_file_check(path, stat))
            result = _stat(stat)
        elif operation == "list":
            count = request.get("limit", self.limits["max_entries"])
            _require(type(count) is int and 0 < count <= self.limits["max_entries"], "List tool exceeds entry limit")
            stat = await self._ctx.fs.stat(path)
            page = await self._ctx.fs.read_dir(path, count, request.get("cursor"))
            checks.append(_file_check(path, stat))
            result = dict(_stat(stat), entries=[_entry(entry) for entry in page.entries], next=page.next)
        elif operation == "rename":
            destination = _path(request.get("destination"))
            checks.extend([_file_check(path, await self._ctx.fs.stat(path)),
                           _file_check(destination, await self._stat_or_missing(destination))])
            mutations.append(dict(op="rename", old=_wire_path(path), new=_wire_path(destination)))
            result = {"renamed": True}
        elif operation == "unlink":
            checks.append(_file_check(path, await self._ctx.fs.stat(path)))
            mutations.append(dict(op="unlink", path=_wire_path(path)))
            result = {"removed": True}
        elif operation == "search":
            result = await self._search(path, request.get("pattern"), checks)
        else:
            raise ValidationError("Unknown filesystem tool")
        _require(len(_utf8(_json(result))) <= self.limits["max_result_bytes"], "File tool result exceeds receipt limit")
        return dict(result=result, filesystem_checks=checks, filesystem_mutations=mutations)

    async def _search(self, root, pattern, checks):
        _require(type(pattern) is str and 0 < len(_utf8(pattern)) <= 512, "Search requires 1-512 literal bytes")
        queue, matches = [(root, 0)], []
        files = directories = size = entries = 0
        truncated = False
        limits = self.limits
        while (queue and directories < limits["max_search_directories"] and files < limits["max_search_files"]
               and size < limits["max_search_bytes"] and entries < limits["max_entries"]):
            parent, depth = queue.pop(0)
            checks.append(_file_check(parent, await self._ctx.fs.stat(parent)))
            directories += 1
            page = await self._ctx.fs.read_dir(parent, limits["max_entries"] - entries)
            entries += len(page.entries)
            truncated |= page.next is not None
            for entry in page.entries:
                location = (_utf8(parent) if type(parent) is str else parent).rstrip(b"/") + b"/" + entry.name
                if entry.kind == "Directory":
                    if depth < limits["max_search_depth"] and len(queue) < limits["max_search_directories"]:
                        queue.append((location, depth + 1))
                    else:
                        truncated = True
                    continue
                if files >= limits["max_search_files"] or size >= limits["max_search_bytes"]:
                    truncated = True
                    break
                stat, data, check = await self._read(location, min(limits["max_read_bytes"], limits["max_search_bytes"] - size))
                checks.append(check)
                files += 1
                size += len(data)
                truncated |= len(data) < stat.size
                text = _text(data)
                if text is not None and (index := text.find(pattern)) >= 0:
                    display = _text(location)
                    matches.append(dict(_stat(stat), path_base64=base64.b64encode(location).decode("ascii"),
                                        excerpt=text[max(0, index - 64):index + len(pattern) + 128],
                                        **({"path": display} if display is not None else {})))
        return dict(matches=matches, visited_files=files, visited_directories=directories,
                    visited_bytes=size, truncated=truncated or bool(queue))

    async def prepare(self, request):
        saved = self._request(request)
        await self._ctx.fs.capabilities()
        try:
            return await self._prepare(saved)
        except Exception as error:
            result = filesystem_tool_refusal(error)
            if result is None:
                raise
            return dict(result=result, filesystem_checks=[], filesystem_mutations=[])

    async def execute(self, operation_id, request, *, checks=None, puts=None):
        _require(type(operation_id) is str and re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", operation_id)
                 and operation_id != "count", "Invalid stable file tool operation ID")
        saved = self._request(request)
        _require((checks is None or type(checks) is dict) and (puts is None or type(puts) is dict),
                 "File tool checkpoints must be dictionaries")
        saved_checks = {}
        for key, snapshot in (checks or {}).copy().items():
            _require(type(key) is str and isinstance(snapshot, Snapshot), "Invalid checkpoint snapshot")
            saved_checks[key] = Snapshot(snapshot.present, _clone(snapshot.value, floats=True))
        saved_puts = _clone(puts or {}, floats=True)
        prefix = self.limits["prefix"]
        _require(not any(key.startswith(prefix) for key in (*saved_checks, *saved_puts)), "Checkpoint collides with tool receipts")
        identity = _json(saved)
        capabilities = await self._ctx.fs.capabilities()
        maximum = capabilities.get("transport", {}).get("max_value_bytes")
        _require(type(maximum) is int and maximum >= 4096, "Missing filesystem receipt transport budget")
        key, count_key = prefix + operation_id, prefix + "count"
        prior = await self._ctx.storage.read_json(key)
        if prior.present:
            _require(prior.value["format"] == 1 and prior.value["request"] == identity,
                     "File tool operation ID reused with different input")
            await self._ctx.fs.sync()
            return dict(result=_clone(prior.value["result"], floats=True), replayed=True)
        count = await self._ctx.storage.read_json(count_key)
        used = count.value if count.present else 0
        _require(type(used) is int and 0 <= used < self.limits["max_receipts"], "File tool receipt capacity reached")
        try:
            prepared = await self._prepare(saved)
        except Exception as error:
            refusal = filesystem_tool_refusal(error)
            if refusal is None:
                raise
            prepared = dict(result=refusal, filesystem_checks=[], filesystem_mutations=[])

        def batch(result):
            receipt = dict(format=1, request=identity, result=result)
            envelope = {"ok": True, "value": {"present": True, "value": receipt}}
            _require(len(_utf8(_json(envelope))) <= maximum, "File tool receipt exceeds the storage read transport budget")
            return dict(checks={**saved_checks, key: prior, count_key: count},
                        puts={**saved_puts, key: receipt, count_key: used + 1})

        async def submit(value):
            validate_filesystem_checkpoint(value, capabilities["transport"])
            await self._ctx.storage.apply(**value)

        try:
            await submit(dict(**batch(prepared["result"]),
                              filesystem_checks=prepared["filesystem_checks"],
                              filesystem_mutations=prepared["filesystem_mutations"]))
        except Exception as error:
            refusal = filesystem_tool_refusal(error)
            if refusal is None:
                raise
            prepared["result"] = refusal
            await submit(batch(refusal))
        await self._ctx.fs.sync()
        return dict(result=prepared["result"], replayed=False)
