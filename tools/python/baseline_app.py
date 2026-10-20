"""Equivalent echo and checked counter operations for the measured Rust host."""
from cellar import Response
import sys


async def fetch(request, ctx):
    if request.path == "/runtime":
        return Response.json({"version": sys.version, "free_threaded": "t" in sys.abiflags,
                              "gil_enabled": sys._is_gil_enabled()})
    if request.path == "/echo" and request.method == "POST":
        return Response(request.body, headers=[("content-type", "application/octet-stream")])
    if request.path.startswith("/cells/"):
        name = request.path.removeprefix("/cells/")
        return await ctx.namespace("ENTITIES").get_by_name(name).fetch(request)
    return Response("unknown route", 404)


class Entity:
    async def fetch(self, request, ctx):
        previous = await ctx.storage.read_json("count")
        value = previous.value if previous.present else 0
        if request.method == "POST":
            value += 1
            await ctx.storage.apply(checks={"count": previous}, puts={"count": value})
        elif request.method != "GET":
            return Response("invalid method", 405)
        return Response.json({"value": value})
