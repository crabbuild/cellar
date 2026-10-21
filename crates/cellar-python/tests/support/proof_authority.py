"""Retain an invocation across actual native durability proof cancellation."""
import asyncio
import threading
from cellar import Ambiguous, Response, SDKError

_lock = threading.RLock()
_owner = None
_ctx = None
_loop = None
_release = None
_task = None
_attempts = 0
_committed = False
_returned = False
_cancelled = False
_late_entered = False
_late_done = False
_state_reads = 0


def snapshot():
    with _lock:
        return dict(owner=_owner, attempts=_attempts, committed=_committed, returned=_returned,
                    cancelled=_cancelled, late_entered=_late_entered, late_done=_late_done,
                    active=_ctx._bridge.active() if _ctx else False, state_reads=_state_reads)


def release():
    with _lock:
        loop, event = _loop, _release
    loop.call_soon_threadsafe(event.set)


def clear():
    global _ctx, _loop, _release, _task
    with _lock:
        _ctx = _loop = _release = _task = None


async def late(ctx, event):
    global _late_entered, _late_done
    with _lock:
        _late_entered = True
    while not event.is_set():
        try:
            await event.wait()
        except asyncio.CancelledError:
            pass
    try:
        await ctx.storage.put_json("late", True)
    except SDKError as error:
        assert error.code == "ContextExpired"
    else:
        raise AssertionError("expired proof context admitted a mutation")
    try:
        ctx._bridge.submit(10000, "storage.apply", '{"puts":{"late-raw":true}}')
    except RuntimeError as error:
        assert str(error) == "ContextExpired"
    else:
        raise AssertionError("expired proof sender admitted a mutation")
    with _lock:
        _late_done = True


async def fetch(request, ctx):
    global _state_reads
    if request.path == "/state":
        with _lock:
            _state_reads += 1
    cell = ctx.namespace("COUNTERS").get_by_name("proof-authority")
    if request.path == "/failure":
        try:
            await cell.fetch(request)
        except SDKError as error:
            assert isinstance(error, Ambiguous)
            assert error.outcome == "unknown"
            # The caller remains live after its callee's proof fails.
            assert (await ctx.var("DOMAIN_MARKER")).startswith("authority-proof-")
            return Response.json(dict(code=error.code, outcome=error.outcome))
        raise AssertionError("failed native proof returned success to the caller")
    return await cell.fetch(request)


class Counter:
    async def fetch(self, request, ctx):
        global _owner, _ctx, _loop, _release, _task
        global _attempts, _committed, _returned, _cancelled
        owner = await ctx.var("DOMAIN_MARKER")
        with _lock:
            _owner = owner
        if request.path == "/seed":
            await ctx.storage.put_json("seed", 7)
            await ctx.storage.sync()
            return Response.json(True)
        if request.path == "/state":
            return Response.json({key: await ctx.storage.get_json(key) for key in ("seed", "pending", "late", "late-raw")})
        assert request.path in ("/sync", "/output", "/failure")
        event = asyncio.Event()
        with _lock:
            _attempts += 1
            _ctx, _loop, _release = ctx, asyncio.get_running_loop(), event
        await ctx.storage.put_json("pending", 1)
        with _lock:
            _committed = True
        if request.path == "/sync":
            try:
                await ctx.storage.sync()
            except asyncio.CancelledError:
                with _lock:
                    _cancelled = True
                await late(ctx, event)
                return Response.json(False)
            raise AssertionError("sync completed through withheld native proofs")
        task = asyncio.create_task(late(ctx, event))
        with _lock:
            _task = task
            _returned = True
        return Response.json(True)
