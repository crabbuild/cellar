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
        backup = ctx.containers.get("backup")
        bindings = await ctx.containers.list()
        assert tuple(binding.name for binding in bindings) == ("container", "backup")
        assert all(binding.capabilities.provider == request.path.strip("/").split("/")[0] for binding in bindings)
        await refuses(lambda: ctx.containers.get("missing").running())
        await refuses(lambda: ctx.containers.get("../escape").running())
        capabilities = await container.capabilities()
        assert not capabilities.exec_pty
        await refuses(lambda: container.exec_output(["child-exit"], max_bytes=64, pty=(80, 24)))
        assert capabilities.provider == request.path.strip("/").split("/")[0]
        assert capabilities.fencing == "local_checks_and_expiry"
        assert capabilities.max_resources == 0
        assert not capabilities.queued_lifecycle and not capabilities.synchronous_running
        path = "/tmp/é-?.bin"

        if request.path.endswith("/expired"):
            for slot in (container, backup):
                try:
                    await slot.exec_output(["child-exit"], max_bytes=64)
                except SDKError as error:
                    assert error.code == "ResourceExpired"
                    assert error.outcome == "not_committed"
                else:
                    raise AssertionError("expired resource admitted a command")
            return Response("ok")
        if request.path.endswith("/destroy"):
            await container.destroy()
            assert not await container.running()
            assert await backup.running()
            await backup.destroy()
            retired = await container.reconcile()
            assert (retired.state, retired.generation) == ("deleted", "1")
            return Response("ok")
        if request.path.endswith("/second"):
            await refuses(lambda: self.old.running())
            await refuses(lambda: self.old.reconcile())
            await refuses(lambda: self.old_backup.running())
            assert await backup.read_file(path, max_bytes=3) == b"\x01\x02\x03"
            current = await container.reconcile()
            assert (current.state, current.generation) == ("running", "1")
            assert await container.running()
            assert await container.read_file(path, max_bytes=3) == b"\x00\xff*"
            return Response("ok")

        assert "container.reconcile" in capabilities.operations
        empty = await container.reconcile()
        assert (empty.state, empty.generation) == ("unbound", None)
        await refuses(lambda: container.signal(9))
        assert await container.start(env=[("HELLO", "world")]) == "1"
        assert (await backup.reconcile()).state == "unbound"
        assert await backup.start(env=[("HELLO", "world")]) == "1"
        await backup.write_file(path, b"\x01\x02\x03", mode=0o640)
        self.old = container
        self.old_backup = backup
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
