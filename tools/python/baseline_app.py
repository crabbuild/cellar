"""Equivalent echo and checked counter operations for the measured Rust host."""
from cellar import Response
import asyncio
import sys

held = None


async def fetch(request, ctx):
    global held
    if request.path == "/runtime":
        return Response.json({"version": sys.version, "free_threaded": "t" in sys.abiflags,
                              "gil_enabled": sys._is_gil_enabled()})
    if request.path == "/echo" and request.method == "POST":
        return Response(request.body, headers=[("content-type", "application/octet-stream")])
    if request.path == "/cell-echo" and request.method == "POST":
        return await ctx.namespace("ENTITIES").get_by_name("echo").fetch(request)
    if request.path == "/defaults/cpu":
        return Response.json({"value": sum((index ^ 7) % 97 for index in range(2000))})
    if request.path.startswith("/defaults/held/"):
        # Private measurement fixture only. Hold all requested participants so
        # admission/byte/memory observations cannot depend on a transient burst.
        target = int(request.path.removeprefix("/defaults/held/"))
        if not 1 <= target <= 4096:
            raise ValueError("invalid qualification participant count")
        if held is None:
            held = {"target": target, "entered": 0, "active": 0, "gate": asyncio.Event()}
        batch = held
        if batch["target"] != target or batch["entered"] >= target:
            raise ValueError("qualification batches overlap")
        batch["entered"] += 1
        batch["active"] += 1
        if batch["entered"] == target:
            asyncio.get_running_loop().call_later(2, batch["gate"].set)
        try:
            await batch["gate"].wait()
            return Response(request.body)
        finally:
            batch["active"] -= 1
            if batch["active"] == 0:
                held = None
    if request.path.startswith("/cells/"):
        name = request.path.removeprefix("/cells/")
        return await ctx.namespace("ENTITIES").get_by_name(name).fetch(request)
    return Response("unknown route", 404)


class Entity:
    async def fetch(self, request, ctx):
        if request.path == "/cell-echo" and request.method == "POST":
            return Response(request.body, headers=[("content-type", "application/octet-stream")])
        previous = ctx.initial_storage.get("count")
        if previous is None:
            previous = await ctx.storage.read_json("count")
        value = previous.value if previous.present else 0
        if request.method == "POST":
            value += 1
            await ctx.storage.apply(checks={"count": previous}, puts={"count": value})
        elif request.method != "GET":
            return Response("invalid method", 405)
        return Response.json({"value": value})
