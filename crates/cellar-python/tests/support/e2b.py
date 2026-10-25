# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

from cellar import Response, SDKError


async def refuses(action):
    try:
        await action()
    except SDKError as error:
        assert error.outcome == "not_committed"
    else:
        raise AssertionError("expected refusal")


class Counter:
    async def fetch(self, request, ctx):
        container = ctx.container
        capabilities = await container.capabilities()
        assert capabilities.provider == "e2b"
        assert capabilities.fencing == "local_checks_and_expiry"
        assert capabilities.max_resources == 0
        assert not capabilities.queued_lifecycle and not capabilities.synchronous_running
        path = "/tmp/é-?.bin"

        if request.path == "/destroy":
            await container.destroy()
            assert not await container.running()
            return Response("ok")
        if request.path == "/second":
            await refuses(lambda: self.old.running())
            assert await container.running()
            assert await container.read_file(path, max_bytes=3) == b"\x00\xff*"
            return Response("ok")

        await refuses(lambda: container.signal(9))
        assert await container.start(env=[("HELLO", "world")]) == "1"
        self.old = container
        await container.write_file(path, b"\x00\xff*", mode=0o640)
        assert await container.read_file(path, max_bytes=3) == b"\x00\xff*"
        metadata = await container.stat_file(path)
        assert (metadata.kind, metadata.size, metadata.mode) == ("file", 3, 0o640)
        entries = await container.list_files("/tmp", max_bytes=4096, max_entries=1)
        assert len(entries) == 1 and entries[0].name == "é-?.bin"
        output = await container.exec_output(["child-exit"], max_bytes=64)
        assert (output.exit_code, output.stdout, output.stderr) == (7, b"\x00\xff*", b"diagnostic")
        return Response("ok")


cells = {"Counter": Counter}


async def fetch(request, ctx):
    return await ctx.namespace("COUNTERS").get_by_name("alice").fetch(request)
