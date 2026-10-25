# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import asyncio
from cellar import ContainerWebSocket, Request, Response, SDKError


async def rejects(action, outcome):
    try:
        await action()
    except SDKError as error:
        assert error.outcome == outcome, (error.code, error.outcome)
    else:
        raise AssertionError("expected rejection")


async def drain(read):
    result = b""
    while chunk := await read():
        result += chunk
    return result


async def receive(socket):
    event = await socket.receive()
    while event["type"] == "open":
        event = await socket.receive()
    return event


class Counter:
    async def fetch(self, request, ctx):
        if request.path == "/borrow":
            await rejects(lambda: self.old.running(), "not_committed")
            return Response("ok")
        container = ctx.container
        capabilities = await container.capabilities()
        assert capabilities.provider == "audited-native" and capabilities.environment == "linux"
        assert capabilities.max_resources == 64 and "container.exec" in capabilities.operations
        assert capabilities.synchronous_running
        assert isinstance(capabilities.operations, tuple)
        if not await container.running():
            await container.start(env=[("HELLO", "world")], labels=[("app", "adapter")])
        port = container.port(int(await ctx.var("HTTP_PORT")))
        if request.path in ("/leak", "/cancel"):
            socket = await port.connect_websocket("http://container/ws")
            assert isinstance(socket, ContainerWebSocket)
            if request.path == "/cancel":
                while True:
                    await socket.receive()
            return Response("ok")
        self.old = container
        await rejects(lambda: container.signal(0), "not_committed")
        await rejects(lambda: container.exec([]), "not_committed")
        await rejects(lambda: container.port(0).connect(), "not_committed")
        await rejects(lambda: container.exec_output(["echo-stdin"], max_bytes=-1), "not_committed")
        await container.set_inactivity_timeout(60000)
        await container.signal(15)
        process = await container.exec(["echo-stdin"], env=[("EXEC", "yes")], cwd="/tmp", user="1000")
        await process.write(b"\x00\xff*")
        await process.close_stdin()
        stdout, stderr, code = await asyncio.gather(drain(process.read_stdout), drain(process.read_stderr), process.wait())
        assert (stdout, stderr, code) == (b"\x00\xff*", b"stderr", 7)
        await process.release()
        await rejects(process.wait, "not_committed")
        output = await container.exec_output(["echo-stdin"], max_bytes=64)
        assert (output.exit_code, output.stdout, output.stderr) == (7, b"", b"stderr")
        await rejects(lambda: container.exec_output(["echo-stdin"], max_bytes=2), "unknown")
        await port.wait_ready(1000)
        response = await port.fetch(Request("https://untrusted.invalid/path?query=yes"))
        assert (response.status, response.body) == (302, b"container response")
        socket = await container.port(int(await ctx.var("TCP_PORT"))).connect()
        await socket.write(b"\x00\xff*")
        await socket.close_write()
        assert await drain(socket.read) == b"\x00\xff*"
        await socket.close()
        declined = await port.connect_websocket("http://container/decline")
        assert isinstance(declined, Response) and declined.status == 302
        upgraded = await port.connect_websocket("ws://container/ws")
        assert isinstance(upgraded, ContainerWebSocket)
        await upgraded.send_text("hello")
        assert await receive(upgraded) == {"type": "text", "text": "hello"}
        await upgraded.send_binary(b"\x00\xff*")
        assert await receive(upgraded) == {"type": "binary", "data": b"\x00\xff*"}
        await upgraded.close()
        old = await container.exec(["echo-stdin"])
        await container.destroy()
        assert isinstance(await container.start(), str)
        await rejects(old.wait, "unknown")
        await old.release()
        monitor = asyncio.create_task(container.monitor())
        # Let the monitor enter the native invocation before destruction.
        await asyncio.sleep(0)
        await container.destroy()
        assert await monitor == 137
        assert not await container.running()
        return Response("ok")


async def fetch(request, ctx):
    await rejects(ctx.container.running, "unknown")
    return await ctx.namespace("COUNTERS").get_by_name("alice").fetch(Request(request.url))
