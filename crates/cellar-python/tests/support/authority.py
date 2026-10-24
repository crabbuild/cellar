# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Submit a real SDK mutation while its native invocation driver is paused."""
import asyncio
import threading
from cellar import Response, SDKError

_lock = threading.RLock()
_context = None
_loop = None
_submit = None
_release = None
_cancelled = False
_refused = False
_finished = False


def snapshot():
    with _lock:
        return dict(entered=_context is not None, active=_context._bridge.active() if _context else False,
                    cancelled=_cancelled, refused=_refused, finished=_finished)


def submit():
    with _lock:
        loop, event = _loop, _submit
    loop.call_soon_threadsafe(event.set)


def release():
    with _lock:
        loop, event = _loop, _release
    loop.call_soon_threadsafe(event.set)


def clear():
    global _context, _loop, _submit, _release
    with _lock:
        _context = _loop = _submit = _release = None


async def fetch(request, ctx):
    return await ctx.namespace("COUNTERS").get_by_name("authority").fetch(request)


class Counter:
    async def fetch(self, request, ctx):
        global _context, _loop, _submit, _release, _cancelled, _refused, _finished
        if request.path == "/seed":
            await ctx.storage.put_json("seed", 7)
            await ctx.storage.sync()
            return Response.json(True)
        if request.path == "/state":
            return Response.json({key: await ctx.storage.get_json(key) for key in ("seed", "queued", "late", "late-raw")})
        assert request.path == "/queued"
        assert ctx.initial_storage["seed"].present and ctx.initial_storage["seed"].value == 7
        submit, release = asyncio.Event(), asyncio.Event()
        with _lock:
            _context, _loop, _submit, _release = ctx, asyncio.get_running_loop(), submit, release
        try:
            await submit.wait()
            await ctx.storage.put_json("queued", True)
            raise AssertionError("queued operation executed through a closed native poll gate")
        except asyncio.CancelledError:
            with _lock:
                _cancelled = True
            while not release.is_set():
                try:
                    await release.wait()
                except asyncio.CancelledError:
                    pass
            try:
                await ctx.storage.apply(checks={"seed": ctx.initial_storage["seed"]}, puts={"late": True})
            except SDKError as error:
                assert error.code == "ContextExpired"
            else:
                raise AssertionError("expired invocation admitted a late mutation")
            try:
                ctx._bridge.submit(10000, "storage.apply", '{"puts":{"late-raw":true}}')
            except RuntimeError as error:
                assert str(error) == "ContextExpired"
            else:
                raise AssertionError("expired native sender admitted a late mutation")
            try:
                await ctx.storage.read_json_many(["seed"])
            except SDKError as error:
                assert error.code == "ContextExpired"
            else:
                raise AssertionError("expired invocation admitted a batch read")
            try:
                ctx._bridge.submit(10001, "storage.read_many", '{"keys":["seed"]}')
            except RuntimeError as error:
                assert str(error) == "ContextExpired"
            else:
                raise AssertionError("expired native sender admitted a batch read")
            with _lock:
                _refused = True
            return Response.json(False)
        finally:
            with _lock:
                _finished = True
