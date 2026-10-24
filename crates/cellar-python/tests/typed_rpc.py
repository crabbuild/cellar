from urllib.parse import urlsplit, parse_qsl
from cellar import Conflict, Overloaded, Response, SDKError
from counter_pb2 import (
    Count, GetCountRequest, GetCountResponse, IncrementRequest,
    IncrementResponse, OperationConflict, OutOfRange,
)
from counter_cellar import CounterClient, CounterRegistration
import time
import cellar_rpc

constructed = 0
saved_client = None
saved_budget = None


async def codec_budget(ctx, target, phase):
    """Real native clock, codecs, provider writes and receipt recovery."""
    counter = CounterClient.bind(ctx, target, "codec-budget-" + phase)
    verify = ctx._verify_rpc
    encode = cellar_rpc.encode_message
    decode = IncrementResponse.FromString
    entered = []

    def slow_verify(method):
        entered.append("verify")
        time.sleep(0.04)
        return verify(method)

    def slow_encode(message, method):
        entered.append("encode")
        time.sleep(0.04)
        return encode(message, method)

    def slow_decode(body):
        entered.append("decode")
        time.sleep(0.12)
        return decode(body)

    try:
        if phase == "verify":
            ctx._verify_rpc = slow_verify
        elif phase == "encode":
            cellar_rpc.encode_message = slow_encode
        else:
            assert phase == "decode"
            IncrementResponse.FromString = slow_decode
        try:
            await counter.increment(IncrementRequest(delta=1, operation_id="codec-budget"),
                                    timeout_ms=100 if phase == "decode" else 20)
        except SDKError as error:
            failure = dict(code=error.code, outcome=error.outcome)
        else:
            raise AssertionError("Slow codec escaped the original call budget")
    finally:
        ctx._verify_rpc = verify
        cellar_rpc.encode_message = encode
        IncrementResponse.FromString = decode

    assert entered == [phase]
    saved = await counter.get_count(GetCountRequest())
    assert saved.count.value == int(phase == "decode")
    if phase == "decode":
        # Decoder failure follows a committed mutation. An explicit same-ID
        # retry must recover the receipt without applying another increment.
        replay = await counter.increment(IncrementRequest(delta=1, operation_id="codec-budget"))
        assert replay.count.value == 1
    return Response.json(dict(failure, codec_entered=phase, count=str(saved.count.value)))


class Counter:
    def __init__(self):
        global constructed
        constructed += 1

    async def get_count(self, request, ctx):
        if ctx.identity.name == "codec-parent":
            # A guest cannot extend the real incoming native RPC's deadline.
            budget = ctx._rpc_budget().limit(60_000)
            remaining = budget.remaining_ms()
            assert 0 < remaining <= 1000
            return GetCountResponse(count=Count(value=remaining))
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
    global saved_client, saved_budget
    url = urlsplit(request.url)
    query = dict(parse_qsl(url.query))
    if url.path.endswith("/constructors"):
        return Response.json({"constructed": constructed})
    target = "RUST" if query.get("target") == "rust" else "COUNTERS"
    if url.path.endswith("/codec-budget"):
        return await codec_budget(ctx, target, query["phase"])
    if url.path.endswith("/budget-capture"):
        saved_budget = ctx._rpc_budget()
        return Response.json({"captured": True})
    if url.path.endswith("/budget-foreign"):
        try:
            await ctx._call("cell.call", _body=b"", _rpc_budget=saved_budget,
                            binding=target, name="never-construct", method="example.counter.v1.Counter/GetCount",
                            contract_digest=CounterRegistration.methods["example.counter.v1.Counter/GetCount"].digest)
        except SDKError as error:
            return Response.json({"code": error.code, "outcome": error.outcome})
        raise AssertionError("Foreign invocation replayed an opaque budget")
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
