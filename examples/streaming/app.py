# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""SSE progress, worker echo, and a hibernating cell with a durable frame count."""

import json
import re

from cellar import Response, StreamingResponse, WebSocketResponse, sse_event


async def progress():
    for step in range(1, 4):
        # Replace this with awaited application work. IDs label this response;
        # they do not provide a durable log or Last-Event-ID replay.
        yield sse_event(json.dumps({"step": step}, separators=(",", ":")), event="progress", id=str(step))

    yield sse_event("complete", event="done")


async def echo(socket, ctx):
    """A worker retains this invocation while it consumes text/binary frames."""
    while True:
        event = await socket.next()

        if event["type"] == "close":
            return

        if event["type"] == "message":
            await socket.send(event["data"])


class Room:
    async def fetch(self, request, ctx):
        if request.path.endswith("/state"):
            snapshot = await ctx.storage.read_json("messages")
            return Response.json({"messages": snapshot.value if snapshot.present else 0})

        socket = await ctx.websocket_pair()
        await socket.accept(hibernating=True, tags=["echo"])
        return WebSocketResponse(socket)

    async def websocket_open(self, event, ctx):
        # No application work is needed when the native connection opens.
        pass

    async def websocket_close(self, event, ctx):
        # The host releases the connection; the message count stays in storage.
        pass

    async def websocket_message(self, event, ctx):
        # Each callback receives fresh authority; never cache a Context or socket
        # facade on the Python object for use in a later invocation.
        snapshot = await ctx.storage.read_json("messages")
        count = snapshot.value if snapshot.present else 0
        await ctx.storage.apply(
            checks={"messages": snapshot},
            puts={"messages": count + 1},
        )
        await ctx.storage.sync()
        await ctx.websocket(event["id"]).send(event["data"])


async def fetch(request, ctx):
    if request.method != "GET":
        return Response("Use GET", 405, [("allow", "GET")])

    path = request.path
    is_socket = path == "/ws" or re.fullmatch(r"/rooms/[\w-]{1,64}/ws", path)
    upgrade = any(
        name.lower() == "upgrade" and value.lower() == "websocket"
        for name, value in request.headers
    )

    if is_socket and not upgrade:
        return Response("Use a WebSocket client", 426)

    if path == "/events":
        return StreamingResponse.sse(progress())

    if path == "/ws":
        socket = await ctx.websocket_pair()
        await socket.accept()
        return WebSocketResponse(socket, run=echo)

    room = re.fullmatch(r"/rooms/([\w-]{1,64})/(ws|state)", path, flags=re.ASCII)

    if room:
        # Preserve the native live response so upgrades cross the namespace hop.
        return await ctx.namespace("ROOMS").get_by_name(room[1]).fetch_stream(request)

    return Response("Use /events, /ws, /rooms/demo/ws or /rooms/demo/state", 404)
