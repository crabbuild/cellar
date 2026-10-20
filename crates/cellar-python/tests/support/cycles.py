"""Live same-cell reentry and nested-call cancellation/fencing barriers."""
import asyncio
import json
import threading
from cellar import Request, Response, SDKError

_lock = threading.RLock()
_contexts = {}
_cancelled = set()
_completed = set()
_refused = {}
_release = False
_outer = None


def snapshot():
    with _lock:
        return dict(entered=sorted(_contexts), cancelled=sorted(_cancelled),
                    completed=sorted(_completed), refused=dict(_refused),
                    active={key: ctx._bridge.active() for key, ctx in _contexts.items()})


def release():
    global _release
    with _lock:
        _release = True


def clear():
    with _lock:
        _contexts.clear()


async def held(label, ctx, work):
    with _lock:
        _contexts[label] = ctx
    try:
        return await work
    except asyncio.CancelledError:
        with _lock:
            _cancelled.add(label)
        # Deliberately retain the frame/context after native cancellation.
        # Only the supervisor releases it, after observing expired authority.
        while True:
            with _lock:
                released = _release
            if released:
                break
            try:
                await asyncio.sleep(0.001)
            except asyncio.CancelledError:
                pass
        try:
            if label == "root":
                await ctx.var("DOMAIN_MARKER")
            else:
                await ctx.storage.put_json("late-" + label, True)
        except SDKError as error:
            assert error.code == "ContextExpired", error.code
        else:
            raise AssertionError("cancelled nested context retained SDK authority")
        try:
            ctx._bridge.submit(10000, "var" if label == "root" else "storage.apply",
                               '{"name":"DOMAIN_MARKER"}' if label == "root" else '{"puts":{"late-raw":true}}')
        except RuntimeError as error:
            assert str(error) == "ContextExpired"
        else:
            raise AssertionError("cancelled nested sender retained native authority")
        with _lock:
            _refused[label] = "ContextExpired"
        return Response.json(False)
    finally:
        with _lock:
            _completed.add(label)


async def fetch(request, ctx):
    if request.path == "/cycle":
        return await ctx.namespace("COUNTERS").get_by_name("cycle-a").fetch(Request("http://cell/cycle/outer"))
    if request.path == "/held":
        return await held("root", ctx, ctx.namespace("COUNTERS").get_by_name("cycle-a").fetch(Request("http://cell/held/outer")))
    if request.path == "/state":
        result = {}
        for name in ("a", "b"):
            response = await ctx.namespace("COUNTERS").get_by_name("cycle-" + name).fetch(Request("http://cell/state"))
            result[name] = json.loads(response.body)
        return Response.json(result)
    raise AssertionError("unknown cycle root route")


class Counter:
    async def fetch(self, request, ctx):
        label = request.path.rsplit("/", 1)[-1]
        if request.path.startswith("/held/"):
            return await held(label, ctx, self.run(label, ctx, True))
        if request.path == "/state":
            return Response.json({key: await ctx.storage.get_json(key) for key in
                                  ("cycle-outer", "cycle-middle", "cycle-inner", "late-outer", "late-middle", "late-inner", "late-raw")})
        return await self.run(label, ctx, False)

    async def run(self, label, ctx, holding):
        global _outer
        if label == "outer":
            if not holding:
                _outer = (ctx, self)
            try:
                response = await ctx.namespace("COUNTERS").get_by_name("cycle-b").fetch(Request("http://cell/" + ("held" if holding else "cycle") + "/middle"))
                assert json.loads(response.body) is True
                await ctx.storage.put_json("cycle-outer", 1)
            finally:
                if not holding:
                    _outer = None
        elif label == "middle":
            response = await ctx.namespace("COUNTERS").get_by_name("cycle-a").fetch(Request("http://cell/" + ("held" if holding else "cycle") + "/inner"))
            assert json.loads(response.body) is True
            await ctx.storage.put_json("cycle-middle", 1)
        elif label == "inner":
            if holding:
                await asyncio.Event().wait()
            else:
                original, instance = _outer
                assert instance is self and original.identity == ctx.identity
                assert original._bridge is not ctx._bridge
                try:
                    await original.storage.put_json("late-raw", True)
                except SDKError as error:
                    assert error.code == "ContextMismatch"
                else:
                    raise AssertionError("same-cell reentry inherited outer authority")
                try:
                    original._bridge.submit(10001, "storage.apply", '{"puts":{"late-raw":true}}')
                except RuntimeError as error:
                    assert str(error) == "ContextMismatch"
                else:
                    raise AssertionError("same-cell reentry reused the outer native sender")
                await ctx.storage.put_json("cycle-inner", 1)
        else:
            raise AssertionError("unknown cycle cell route")
        await ctx.storage.sync()
        return Response.json(True)
