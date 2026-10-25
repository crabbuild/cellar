# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Actual activation collection and authority over eviction/adoption/shutdown."""
import asyncio
import gc
import weakref
from cellar import Request, Response, SDKError

created = 0
references = {}
dead = set()
retained = {}
holding = {}
release = {}
cancelled = 0
late_refused = 0
cancelled_contexts = []


def referrers(serial):
    target = references[serial]()
    if target is None:
        return []
    names = []
    for referrer in gc.get_referrers(target):
        name = type(referrer).__name__
        if name == "frame":
            name += ":" + referrer.f_code.co_name
        elif name == "tuple":
            name += ":" + ",".join(type(item).__name__ for item in referrer)
        names.append(name)
    return names


async def refused(call, code):
    try:
        await call()
    except SDKError as error:
        assert error.code == code and error.outcome == "not_committed"
    else:
        raise AssertionError("retained context acquired another invocation's authority")


async def fetch(request, ctx):
    if request.path == "/cancel-root":
        cancelled_contexts.append(ctx)
        raise asyncio.CancelledError
    if request.path == "/cancelled-expired":
        for old in cancelled_contexts:
            assert not old._bridge.active()
            await refused(lambda: old.storage.apply(puts={"forbidden": True}), "ContextExpired")
        return Response.json(len(cancelled_contexts))
    if request.path == "/fatal":
        import threading
        def fail():
            raise RuntimeError("PRIVATE-UNMANAGED-EXCEPTION-SENTINEL")
        thread = threading.Thread(target=fail)
        thread.start()
        thread.join()
        return Response.json(True)
    if request.path == "/inspect":
        return Response.json(observation())
    if request.path.startswith("/release/"):
        release[request.path.removeprefix("/release/")].set()
        return Response.json(True)
    if request.path == "/pressure":
        return await ctx.namespace("COUNTERS").get_by_name("pressure").fetch(Request("http://cell/cell/state"))
    if request.path.startswith("/cell/"):
        return await ctx.namespace("COUNTERS").get_by_name("one").fetch(request)
    return Response("unknown route", 404)


def observation():
    gc.collect()
    return {"created": created, "dead": sorted(dead),
            "live": sorted(k for k, ref in references.items() if ref() is not None),
            "owners": {str(k): value._bridge.owner_active() for k, value in retained.items()},
            "retired_referrers": {str(k): referrers(k) for k, value in retained.items() if not value._bridge.owner_active() and references[k]() is not None},
            "holding": sorted(holding), "cancelled": cancelled, "late_refused": late_refused}


class Counter:
    def __init__(self):
        global created
        created += 1
        self.serial = created
        references[self.serial] = weakref.ref(self, lambda _, serial=self.serial: dead.add(serial))

    async def fetch(self, request, ctx):
        global cancelled, late_refused
        retained[self.serial] = ctx
        marker = await ctx.var("DOMAIN_MARKER")
        if request.path in ("/cell/cancel", "/cell/cancel-after-write"):
            if request.path.endswith("after-write"):
                await ctx.storage.apply(puts={"value": 44})
            cancelled_contexts.append(ctx)
            raise asyncio.CancelledError
        elif request.path == "/cell/seed":
            await ctx.storage.apply(puts={"value": await request.json()})
        elif request.path in ("/cell/hold", "/cell/hold-shutdown"):
            key = "shutdown" if request.path.endswith("shutdown") else "cross"
            holding[key] = ctx
            release[key] = asyncio.Event()
            try:
                await release[key].wait()
            except asyncio.CancelledError:
                cancelled += 1
                assert not ctx._bridge.active()
                for call in (lambda: ctx.storage.apply(puts={"forbidden": True}),
                             lambda: ctx.namespace("COUNTERS").get_by_name("late").fetch(Request("http://cell/cell/state"))):
                    await refused(call, "ContextExpired")
                    late_refused += 1
                raise
            finally:
                holding.pop(key, None)
        elif request.path == "/cell/cross":
            old = holding["cross"]
            assert old._bridge.active() and old._bridge.owner_active()
            # Public identity metadata can be copied, but cannot select the
            # Rust sender's original domain, event, activation or storage.
            original_identity = ctx.identity
            ctx.identity = old.identity
            assert ctx.identity == old.identity
            await refused(lambda: old.storage.get_json("value"), "ContextMismatch")
            await refused(lambda: old.storage.apply(puts={"forbidden": True}), "ContextMismatch")
            await refused(lambda: old.namespace("COUNTERS").get_by_name("late").fetch(Request("http://cell/cell/state")), "ContextMismatch")
            for call in (lambda: old._bridge.submit(99999, "storage.apply", '{"puts":{"forbidden":true}}'),
                         lambda: old._bridge.complete("null", False),
                         lambda: old._bridge.complete('{"status":200,"headers":[],"body":""}', False, b"forbidden")):
                try:
                    call()
                except RuntimeError as error:
                    assert str(error) == "ContextMismatch"
                else:
                    raise AssertionError("native sender accepted a foreign live invocation")
            await ctx.storage.apply(puts={"metadata": marker})
            ctx.identity = original_identity
        elif request.path == "/cell/expired":
            for old in tuple(retained.values()):
                if old is not ctx:
                    assert not old._bridge.active()
                    await refused(lambda: old.storage.apply(puts={"forbidden": True}), "ContextExpired")
        elif request.path != "/cell/state":
            return Response("unknown cell route", 404)
        return Response.json({"serial": self.serial, "marker": marker,
                              "name": ctx.identity.name, "scope": ctx.identity.scope, "epoch": ctx.identity.epoch,
                              "value": await ctx.storage.get_json("value"),
                              "metadata": await ctx.storage.get_json("metadata"),
                              "forbidden": await ctx.storage.get_json("forbidden")})
