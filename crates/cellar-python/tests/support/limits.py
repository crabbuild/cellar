"""Exact native codec and batch-byte boundaries, including refused effects."""
from cellar import Request, Response, ValidationError, _encode

visits = 0


async def fetch(request, ctx):
    if request.path == "/echo":
        return Response(request.body)
    if request.path.startswith("/body/"):
        return Response(b"x" * (4096 if request.path.endswith("exact") else 4097))
    if request.path.startswith("/header-count/"):
        count = 128 if request.path.endswith("exact") else 129
        return Response(headers=[("x-boundary", "y")] * count)
    if request.path.startswith("/header-bytes/"):
        count = 32767 if request.path.endswith("exact") else 32768
        return Response(headers=[("x", "y" * count)])
    if request.path.startswith("/completion/"):
        response = Response(b"x" * 4096, headers=[("x", "")])
        remaining = 65536 - len(_encode(response._wire()).encode())
        # Quotes occupy one HTTP header byte but two encoded envelope bytes.
        # Both individual HTTP limits fit while the envelope reaches its bound.
        response.headers[0] = ("x", '"' * (remaining // 2) + "a" * (remaining % 2))
        assert len(response.headers[0][1]) + 1 <= 32768
        if request.path.endswith("over"):
            response.headers[0] = ("x", response.headers[0][1] + "a")
        assert len(_encode(response._wire()).encode()) == (65537 if request.path.endswith("over") else 65536)
        return response
    if request.path == "/nested-input":
        cell = ctx.namespace("COUNTERS").get_by_name("limits")
        before = visits
        for good, bad in [
            (Request("http://cell/", body=b"x" * 4096), Request("http://cell/", body=b"x" * 4097)),
            (Request("http://cell/", headers=[("x", "y")] * 128), Request("http://cell/", headers=[("x", "y")] * 129)),
            (Request("http://cell/", headers=[("x", "y" * 32767)]), Request("http://cell/", headers=[("x", "y" * 32768)])),
        ]:
            await cell.fetch(good)
            try:
                await cell.fetch(bad)
            except ValidationError as error:
                assert error.outcome == "not_committed"
            else:
                raise AssertionError("over-limit request entered a native cell")
        assert visits == before + 3
        return Response.json(True)
    return await ctx.namespace("COUNTERS").get_by_name("limits").fetch(request)


class Counter:
    async def fetch(self, request, ctx):
        global visits
        visits += 1
        if request.path == "/batch-value":
            original_alarm = await ctx.storage.get_alarm()
            payload = dict(checks={}, puts={"byte-limit": ""}, deletes=[], alarm_at_ms=None, delete_alarm=False)
            size = 65536 - len(_encode(payload).encode())
            payload["puts"]["byte-limit"] = "x" * size
            assert len(_encode(payload).encode()) == 65536
            await ctx._call("storage.apply", **payload)
            assert await ctx.storage.get_json("byte-limit") == "x" * size
            payload["puts"]["byte-limit"] += "y"
            assert len(_encode(payload).encode()) == 65537
            try:
                await ctx._call("storage.apply", **payload)
            except ValidationError as error:
                assert error.outcome == "not_committed"
            else:
                raise AssertionError("over-limit batch changed storage")
            assert await ctx.storage.get_json("byte-limit") == "x" * size
            assert await ctx.storage.get_alarm() == original_alarm
            return Response.json(True)
        return Response.json(True)
