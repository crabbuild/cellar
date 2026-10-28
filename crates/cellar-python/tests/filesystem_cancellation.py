# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Real byte calls with controlled coroutine suspension at SDK boundaries."""
import asyncio
import base64
import hashlib
import json
import sys
import sysconfig
import traceback
from cellar import Request, Response, SDKError

SEED = bytes(range(32))
EDIT = bytes(range(128, 140))


async def exercise(ctx, name, recovered):
    assert sys._is_gil_enabled() is not bool(sysconfig.get_config_var("Py_GIL_DISABLED"))
    operation, phase = name.split("-")
    fs = ctx.fs
    if recovered:
        saved = await ctx.storage.get_json("cancelled")
        assert saved is not None
        data = await fs.read_file("/ledger.bin")
        assert base64.b64encode(data).decode() == saved["bytes"]
        stat = await fs.stat("/ledger.bin")
        assert (str(stat.inode), str(stat.version)) == (saved["inode"], saved["version"])
        assert hashlib.sha256(data).hexdigest() == saved["sha256"]
        assert (saved["code"], saved["outcome"], saved["operation"]) == ("Cancelled", "unknown", "filesystem." + operation)
        assert saved["completed_bytes"] == (4 if phase in ("before_second", "after_second") else 0)
        assert await ctx.storage.get_json("started") == name
        return saved
    await ctx.storage.apply(filesystem_mutations=[dict(op="write_file", path="/ledger.bin", data_base64=base64.b64encode(SEED).decode())], puts={"started": name})
    await fs.sync()
    initial = await fs.stat("/ledger.bin")
    file = await fs.open("/ledger.bin", access="read_write")
    duplicate = await file.dup()
    directory = await fs.opendir("/")
    cookie = await directory.telldir()
    await file.lseek(3)
    ready = asyncio.Event()
    gate = asyncio.get_running_loop().create_future()
    submitted = 0
    entered = 0
    helper = operation in ("read_exact", "write_all")
    wire = "read" if operation == "read_exact" else "write" if operation == "write_all" else operation
    original_call = ctx._call
    original_method = getattr(file, wire)

    async def delayed_call(method, **payload):
        result = await original_call(method, **payload)
        if method == "filesystem." + ("capabilities" if phase == "capabilities" else wire):
            nonlocal submitted
            if phase != "capabilities":
                submitted += 1
            ready.set()
            await gate
        return result

    async def transfer(*args):
        nonlocal entered, submitted
        entered += 1
        if (phase == "before_first" and entered == 1) or (phase == "before_second" and entered == 2):
            ready.set()
            await gate
        result = await original_method(*args)
        submitted += 1
        if phase == "after_second" and entered == 2:
            ready.set()
            await gate
        return result

    if helper and phase != "capabilities":
        setattr(file, wire, transfer)
    else:
        # Only reply delivery is held. Native parsing, authority, SQLite work
        # and the real Context pending-reply machinery remain in use.
        ctx._call = delayed_call

    calls = dict(read=lambda: file.read(12), pread=lambda: file.pread(12, 3),
                 write=lambda: file.write(EDIT), pwrite=lambda: file.pwrite(EDIT, 3),
                 read_exact=lambda: file.read_exact(12), write_all=lambda: file.write_all(EDIT))
    task = asyncio.create_task(calls[operation]())
    await ready.wait()
    assert not task.done()
    message = "stop filesystem helper"
    task.cancel(message)
    cancellation = None
    try:
        await task
    except asyncio.CancelledError as error:
        cancellation = error
    finally:
        ctx._call = original_call
        setattr(file, wire, original_method)
    assert cancellation is not None and task.cancelled(), "SDK swallowed task cancellation"
    assert str(cancellation) == message
    completed = 4 if phase in ("before_second", "after_second") else 0
    actual = tuple(getattr(cancellation, key, None) for key in ("code", "outcome", "operation", "completed_bytes"))
    expected_calls = 0 if phase in ("before_first", "capabilities") else 2 if phase == "after_second" else 1
    assert submitted == expected_calls, (name, submitted)
    offset = 3 if operation in ("pread", "pwrite") else 3 + submitted * 4
    assert await file.lseek(0, "current") == await duplicate.lseek(0, "current") == offset
    assert await directory.telldir() == cookie
    expected = bytearray(SEED)
    writes = submitted if operation in ("write", "pwrite", "write_all") else 0
    expected[3:3 + writes * 4] = EDIT[:writes * 4]
    stat = await file.fstat()
    assert stat.inode == initial.inode and stat.version == initial.version + writes
    if not writes:
        assert stat == initial
    assert await fs.read_file("/ledger.bin") == expected
    assert actual == ("Cancelled", "unknown", "filesystem." + operation, completed), (name, actual,
        dict(native_replies=submitted, entered=entered, offset=offset, version=str(stat.version)))
    # The parent invocation and original descriptor still have authority after
    # cancelling one child task; cleanup and subsequent calls must remain usable.
    fourth = await fs.open("/ledger.bin")
    try:
        await fs.open("/ledger.bin")
    except SDKError as error:
        assert error.errno == "ENFILE" and error.outcome == "not_committed"
    else:
        raise AssertionError("cancellation changed host descriptor admission")
    await fourth.close()
    await duplicate.close()
    await directory.closedir()
    await file.close()
    receipt = dict(code=cancellation.code, outcome=cancellation.outcome, operation=cancellation.operation,
                   completed_bytes=cancellation.completed_bytes, submitted=submitted, bytes=base64.b64encode(expected).decode(),
                   inode=str(stat.inode), version=str(stat.version), sha256=hashlib.sha256(expected).hexdigest())
    await ctx.storage.apply(filesystem_checks=[dict(path="/ledger.bin", present=True, inode=str(stat.inode), version=str(stat.version))], puts={"cancelled": receipt})
    await fs.sync()
    assert sys._is_gil_enabled() is not bool(sysconfig.get_config_var("Py_GIL_DISABLED"))
    return receipt


class Counter:
    async def fetch(self, req, ctx):
        name, action = req.url.rsplit("/", 2)[-2:]
        try:
            receipt = await exercise(ctx, name, action == "recovered")
            return Response(json.dumps(dict(checked=True, case=name, recovered=action == "recovered",
                gil_enabled=sys._is_gil_enabled(), free_threaded=bool(sysconfig.get_config_var("Py_GIL_DISABLED")), receipt=receipt)))
        except Exception:
            return Response(traceback.format_exc(), status=500)


async def fetch(req, ctx):
    name = req.url.rsplit("/", 2)[-2]
    return await ctx.namespace("COUNTERS").get_by_name(name).fetch(Request(req.url))


cells = {"Counter": Counter}
