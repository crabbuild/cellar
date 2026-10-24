# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import asyncio
from cellar import Response, StreamingResponse, WebSocketResponse, Request, sse_event

gate = asyncio.Event()

async def events(ctx, fail=False):
    if ctx.identity:
        await ctx.storage.put_json("phase", 1)
    yield sse_event("first", id="1", event="update")
    if fail:
        raise ValueError("private producer failure")
    await gate.wait()
    if ctx.identity:
        await ctx.storage.put_json("phase", 2)
    yield sse_event("second", id="2")

class Counter:
    async def fetch(self, req, ctx):
        if "/ws" in req.url:
            socket = await ctx.websocket_pair()
            await socket.set_attachment({"room": "test"})
            await socket.accept(hibernating=True, tags=["test"])
            return WebSocketResponse(socket)
        if "/phase" in req.url:
            return Response.json(await ctx.storage.get_json("phase"))
        return StreamingResponse.sse(events(ctx, "error" in req.url))

    async def websocket_open(self, event, ctx):
        pass

    async def websocket_message(self, event, ctx):
        socket = ctx.websocket(event["id"])
        assert (await socket.get_attachment())["room"] == "test"
        await ctx.storage.put_json("message", event.get("text", "binary"))
        await socket.send(event["data"])

    async def websocket_close(self, event, ctx):
        pass

async def echo(socket, ctx):
    while True:
        event = await socket.next()
        if event["type"] == "close":
            break
        if event["type"] == "message":
            await socket.send(event["data"])

async def fetch(req, ctx):
    global gate
    if "/cancel-read" in req.url:
        from cellar import SDKError
        upstream = await ctx.fetch_stream(Request(req.url.replace("/cancel-read", "/sse")))
        reader = upstream.chunks()
        await anext(reader)
        pending = asyncio.create_task(anext(reader))
        await asyncio.sleep(0)
        await upstream.cancel()
        try:
            await pending
            raise AssertionError("read was not cancelled")
        except SDKError as error:
            assert error.code == "HostError"
        return Response("cancelled")
    if "/large" in req.url:
        async def large():
            for i in range(5):
                yield bytes([i]) * 2048
        return StreamingResponse(large())
    if "/release" in req.url:
        gate.set()
        return Response("ok")
    if "/reset" in req.url:
        gate = asyncio.Event()
        return Response("ok")
    if "/outbound/sse" in req.url:
        return await ctx.fetch_stream(Request(req.url.replace("/outbound/", "/")))
    if "/consume/sse" in req.url:
        upstream = await ctx.namespace("COUNTERS").get_by_name("test").fetch_stream(Request(req.url))
        return StreamingResponse.sse(upstream.chunks())
    if "/outbound-ws" in req.url:
        socket = await ctx.connect_websocket(req.url.replace("http", "ws", 1).replace("/outbound-ws", "/worker-ws"))
        event = await socket.next()
        assert event["type"] == "open"
        receive = asyncio.create_task(socket.next())
        await asyncio.sleep(0)
        await socket.send("outbound")
        event = await receive
        await socket.close()
        return Response(event["data"])
    if "/cell/" in req.url:
        return await ctx.namespace("COUNTERS").get_by_name("test").fetch_stream(Request(req.url, headers=req.headers))
    if "/worker-ws" in req.url:
        socket = await ctx.websocket_pair()
        await socket.accept()
        return WebSocketResponse(socket, run=echo)
    return StreamingResponse.sse(events(ctx, "error" in req.url))
