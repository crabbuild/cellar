import asyncio
import time
from cellar import Response

retired = cancelled = 0


def snapshot():
    return {"retired": retired, "cancelled": cancelled}


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
    def __del__(self):
        global retired
        retired += 1

    async def fetch(self, request, ctx):
        await ctx.storage.apply(puts={"value": 42})
        value = await ctx.storage.read_json("value")
        return Response.json({"value": value.value, "scope": ctx.identity.scope,
                              "epoch": ctx.identity.epoch})
