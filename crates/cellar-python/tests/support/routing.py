# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""A typed core refusal before the target handler enters Python."""
import asyncio
import threading
from cellar import Overloaded, Request, Response, SDKError

_lock = threading.RLock()
_loop = None
_release = None
_entered = False
_cold_visits = 0


def snapshot():
    with _lock:
        return dict(entered=_entered, cold_visits=_cold_visits)


def release():
    with _lock:
        loop, event = _loop, _release
    loop.call_soon_threadsafe(event.set)


async def fetch(request, ctx):
    name = "held" if request.path == "/hold" else "cold"
    cell = ctx.namespace("COUNTERS").get_by_name(name)
    if request.path == "/probe":
        try:
            await cell.fetch(Request("http://cell/PRIVATE-ROUTING-SENTINEL"))
        except SDKError as error:
            assert isinstance(error, Overloaded)
            assert error.code == "Overloaded" and error.outcome == "unknown"
            assert "PRIVATE-ROUTING-SENTINEL" not in str(error)
            assert await ctx.var("DOMAIN_MARKER") == "routing"
            # A second mutation-class SDK call on this same caller proves the
            # failed call released its reservation, while A remains occupied.
            reused = await ctx.namespace("COUNTERS").get_by_name("held").fetch(Request("http://cell/reuse"))
            assert reused.status == 200 and reused.body == b"true"
            return Response.json(dict(code=error.code, outcome=error.outcome))
        raise AssertionError("capacity-refused handler entered Python")
    return await cell.fetch(request)


class Counter:
    async def fetch(self, request, ctx):
        global _loop, _release, _entered, _cold_visits
        if request.path == "/hold":
            event = asyncio.Event()
            with _lock:
                _loop, _release, _entered = asyncio.get_running_loop(), event, True
            await event.wait()
        elif request.path != "/reuse":
            with _lock:
                _cold_visits += 1
        return Response.json(True)
