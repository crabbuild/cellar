# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import asyncio
import gc
import time
from cellar import Response

# Completed bridge release must not depend on periodic cyclic collection.
gc.disable()
retired = cancelled = created = 0
_bridge_type = None


def snapshot():
    return {"retired": retired, "cancelled": cancelled, "created": created,
            "bridges": sum(type(value) is _bridge_type for value in gc.get_objects())}


async def fetch(request, ctx):
    global cancelled
    if request.path == "/lag":
        until = time.perf_counter_ns() + 60_000_000
        while time.perf_counter_ns() < until:
            pass
        return Response.json(True)
    if request.path == "/held":
        try:
            await asyncio.Event().wait()
        finally:
            cancelled += 1
    return await ctx.namespace("COUNTERS").get_by_name("one").fetch(request)


class Counter:
    def __init__(self):
        global created
        created += 1

    def __del__(self):
        global retired
        retired += 1

    async def fetch(self, request, ctx):
        global _bridge_type
        _bridge_type = type(ctx._bridge)
        assert gc.is_tracked(ctx._bridge), "bridge census must include native senders"
        await ctx.storage.apply(puts={"value": 42})
        value = await ctx.storage.read_json("value")
        return Response.json({"value": value.value, "scope": ctx.identity.scope,
                              "epoch": ctx.identity.epoch})
