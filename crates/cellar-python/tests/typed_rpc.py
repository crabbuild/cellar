from urllib.parse import urlsplit, parse_qsl
from cellar import Conflict, Overloaded, Response, SDKError
from counter_pb2 import (
    Count, GetCountRequest, GetCountResponse, IncrementRequest,
    IncrementResponse, OperationConflict, OutOfRange,
)
from counter_cellar import CounterClient, CounterRegistration

constructed = 0
saved_client = None


class Counter:
    def __init__(self):
        global constructed
        constructed += 1

    async def get_count(self, request, ctx):
        value = await ctx.storage.read_json("count")
        return GetCountResponse(count=Count(value=int(value.value) if value.present else 0))

    async def increment(self, request, ctx):
        if not request.HasField("delta") or request.delta <= 0:
            return IncrementResponse(out_of_range=OutOfRange(reason="positive delta is required"))
        if not request.operation_id or len(request.operation_id.encode("utf-8")) > 128:
            return IncrementResponse(out_of_range=OutOfRange(reason="operation ID is required"))
        receipt_key = "receipt:" + request.operation_id
        for _ in range(16):
            current = await ctx.storage.read_json("count")
            receipt = await ctx.storage.read_json(receipt_key)
            if receipt.present:
                if receipt.value["delta"] != str(request.delta):
                    return IncrementResponse(operation_conflict=OperationConflict())
                return IncrementResponse(count=Count(value=int(receipt.value["value"])))
            value = (int(current.value) if current.present else 0) + request.delta
            if value > 2**63 - 1:
                return IncrementResponse(out_of_range=OutOfRange(reason="counter overflow"))
            try:
                await ctx.storage.apply(
                    checks={"count": current, receipt_key: receipt},
                    puts={"count": str(value), receipt_key: {
                        "delta": str(request.delta), "value": str(value),
                    }},
                )
            except Conflict:
                continue
            await ctx.storage.sync()
            if request.operation_id == "invalid-reply":
                # Deliberate application bug after a committed write. The native
                # RPC boundary must reject it without claiming rollback.
                return GetCountResponse(count=Count(value=value))
            return IncrementResponse(count=Count(value=value))
        raise Overloaded()

    async def private_helper(self, request, ctx):
        raise AssertionError("private method must never be exposed")


__cellar_rpc__ = {"Counter": CounterRegistration}


async def fetch(request, ctx):
    global saved_client
    url = urlsplit(request.url)
    query = dict(parse_qsl(url.query))
    if url.path.endswith("/constructors"):
        return Response.json({"constructed": constructed})
    target = "RUST" if query.get("target") == "rust" else "COUNTERS"
    counter = CounterClient.bind(ctx, target, "alice")
    if url.path.endswith("/capture"):
        saved_client = counter
        return Response.json({"captured": True})
    try:
        if url.path.endswith("/expired"):
            await saved_client.get_count(GetCountRequest())
            raise AssertionError("retained RPC client remained usable")
        if url.path.endswith("/read"):
            reply = await counter.get_count(GetCountRequest())
            return Response.json({"value": str(reply.count.value)})
        reply = await counter.increment(IncrementRequest(
            delta=int(query["delta"]) if "delta" in query else None,
            operation_id=query.get("id", ""),
        ))
        result = reply.WhichOneof("result")
        if result == "count":
            return Response.json({"value": str(reply.count.value)})
        return Response.json({"error": result})
    except SDKError as error:
        return Response.json({"code": error.code, "outcome": error.outcome})
