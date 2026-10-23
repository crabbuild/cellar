"""Explicit dispatch snapshots; live reads and mutation authority stay separate."""
import asyncio
from cellar import Conflict, Response

ready = asyncio.Event()
release = asyncio.Event()
entered = 0


async def fetch(request, ctx):
    assert not ctx.initial_storage  # No storage authority on the root handler.
    if request.path == "/ready":
        return Response.json(ready.is_set())
    if request.path == "/release":
        release.set()
        return Response.json(True)
    if request.path == "/entered":
        return Response.json(entered)
    return await ctx.namespace("COUNTERS").get_by_name("initial").fetch(request)


class Counter:
    async def fetch(self, request, ctx):
        global entered
        entered += 1
        initial = ctx.initial_storage
        assert set(initial) == {"value", "nullable", "absent"}
        assert initial["nullable"].present and initial["nullable"].value is None
        assert not initial["absent"].present and initial["absent"].value is None
        try:
            initial["value"] = initial["absent"]
            raise AssertionError("initial snapshot map was mutable")
        except TypeError:
            pass
        if request.path == "/hold":
            assert initial["value"].value == 1
            ready.set()
            await release.wait()
            try:
                await ctx.storage.apply(checks={"value": initial["value"]},
                                        puts={"value": 99, "must-not-land": True})
                raise AssertionError("stale dispatch snapshot overwrote native writer")
            except Conflict:
                pass
            assert not (await ctx.storage.read_json("must-not-land")).present
            return Response.json({"initial": initial["value"].value,
                                  "current": await ctx.storage.get_json("value")})
        if request.path == "/own-write":
            await ctx.storage.apply(checks={"value": initial["value"]}, puts={"value": 42})
            return Response.json({"initial": initial["value"].value,
                                  "current": await ctx.storage.get_json("value")})
        return Response.json({key: {"present": row.present, "value": row.value}
                              for key, row in initial.items()})
