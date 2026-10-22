"""Malformed handler output must not become a diagnostic data channel."""
from cellar import Ambiguous, Request, Response, SDKError

PRIVATE = "PRIVATE-UNMANAGED-EXCEPTION-SENTINEL"


class Malformed(Response):
    def __init__(self, kind):
        super().__init__(b"unused")
        self.kind = kind

    def _completion(self):
        value, body = super()._completion()
        if self.kind == "/headers":
            value["headers"] = [PRIVATE]
        elif self.kind == "/header-value":
            value["headers"] = [["x-private", PRIVATE + "\r\n"]]
        elif self.kind == "/body":
            body = PRIVATE
        elif self.kind == "/metadata-body":
            value["body"] = PRIVATE
        elif self.kind == "/missing-body":
            body = None
        elif self.kind == "/payload":
            return PRIVATE, body
        return value, body


async def fetch(request, ctx):
    cell = ctx.namespace("COUNTERS").get_by_name("privacy")
    if request.path == "/state":
        return await cell.fetch(Request("http://cell/state"))
    if request.path.startswith("/nested/"):
        try:
            response = await cell.fetch(Request("http://cell/" + request.path.removeprefix("/nested/"), body=request.body))
        except SDKError as error:
            assert isinstance(error, Ambiguous) and error.outcome == "unknown"
            assert PRIVATE not in str(error)
            assert await ctx.var("DOMAIN_MARKER") == "python-test"
            return Response.json(dict(code=error.code, outcome=error.outcome))
        if request.path == "/nested/header-value":
            # A local native fetch transports raw response headers. HTTP wire
            # validation belongs to public ingress; this is application data,
            # not a parser diagnostic or proof of mutation refusal.
            return Response.json(dict(status=response.status, private_header=any(PRIVATE in value for _, value in response.headers)))
        raise AssertionError("malformed cell output succeeded")
    if request.path == "/healthy":
        return Response.json(True)
    if request.path == "/json":
        await request.json()
        raise AssertionError("invalid JSON was accepted")
    return Malformed(request.path)


class Counter:
    async def fetch(self, request, ctx):
        if request.path == "/state":
            return Response.json(await ctx.storage.get_json("attempts", 0))
        count = await ctx.storage.get_json("attempts", 0)
        await ctx.storage.apply(puts={"attempts": count + 1})
        return await fetch(request, ctx)
