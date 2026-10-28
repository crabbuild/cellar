# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

from cellar import Request, Response

retained = None
original = None


async def refuses(action):
    try:
        await action()
    except Exception:
        return
    raise AssertionError("Expected refusal")


async def fetch(request, ctx):
    if request.url.endswith("/outside"):
        await refuses(lambda: ctx.sandboxes.get("build"))
        return Response("ok")
    return await ctx.namespace("COUNTERS").get_by_name("alice").fetch(Request(request.url))


class Counter:
    async def fetch(self, request, ctx):
        global retained, original
        sandboxes = ctx.sandboxes
        path = request.url.rsplit("/", 1)[-1]
        if path == "start":
            retained = sandboxes
            original = await sandboxes.start("build", "SANDBOXES")
            repeated = await sandboxes.start("build", "SANDBOXES")
            assert original["execution_scope"] == repeated["execution_scope"]
            assert isinstance(original["generation"], str)
            original = repeated
            await sandboxes.start("preview", "SANDBOXES")
            page = await sandboxes.list(limit=1)
            assert len(page["allocations"]) == 1 and page["next"] is not None
            assert (await sandboxes.inspect(original))["state"] == "ready"
            output = await sandboxes.execute(original, "container.exec_output", {
                "options": {"cmd": ["echo", "ok"]}, "max_bytes": 1024,
            })
            assert output["exit_code"] == 7
            await refuses(lambda: sandboxes.execute(original, "container.exec", {}))
            await refuses(lambda: sandboxes.destroy({**original, "generation": "999"}))
        elif path == "borrow":
            await refuses(lambda: retained.get("build"))
        elif path == "destroy":
            current = await sandboxes.get("build")
            assert (await sandboxes.destroy(current))["state"] == "deleted"
        elif path == "recreate":
            current = await sandboxes.start("build", "SANDBOXES")
            assert current["execution_scope"] != original["execution_scope"]
            await refuses(lambda: sandboxes.execute(original, "container.running", {}))
        elif path == "retire":
            await sandboxes.retire()
            await refuses(lambda: sandboxes.start("late", "SANDBOXES"))
        elif path == "status":
            return Response((await sandboxes.parent())["state"])
        else:
            raise AssertionError("Unknown route")
        return Response("ok")


cells = {"Counter": Counter}
