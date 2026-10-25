# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Real-host fault probes; production budgets always read the native clock."""
import asyncio
import time

import cellar_rpc
from cellar import Response, SDKError
from counter_cellar import CounterClient
from counter_pb2 import Count, GetCountRequest, GetCountResponse, IncrementRequest, IncrementResponse

_nested = {"armed": False, "child": "", "observation": {}}


def control(action, child):
    global _nested
    if action == "arm":
        assert not _nested["armed"], "Nested deadline probe already armed"
        _nested = dict(armed=True, child=child, observation={"late_done": False})
    elif action == "disarm":
        assert _nested["observation"]["late_done"]
        _nested["armed"] = False
    else:
        assert action == "status"
    return Response.json(dict(_nested["observation"]))


async def _expired(operation):
    try:
        await operation()
    except SDKError as error:
        return error.code == "ContextExpired" and error.outcome == "not_committed"
    return False


async def nested(ctx):
    """Run as a typed provider with the real incoming parent deadline."""
    if not _nested["armed"]:
        late = await ctx.storage.read_json("deadline-parent-late")
        return GetCountResponse(count=Count(value=int(late.present)))
    observed = _nested["observation"]
    observed["remaining_ms"] = ctx._rpc_budget().remaining_ms()
    verify = ctx._verify_rpc

    def slow_verify(method):
        observed["verify_entered"] = True
        time.sleep(0.12)
        ctx._verify_rpc = verify
        return verify(method)

    child = CounterClient.bind(ctx, "RUST", _nested["child"])
    cached = CounterClient.bind(ctx, "RUST", "guest-followup")
    ctx._verify_rpc = slow_verify
    try:
        return await child.get_count(GetCountRequest(), timeout_ms=60_000)
    except (asyncio.CancelledError, SDKError):
        observed["storage_refused"] = await _expired(lambda: ctx.storage.put_json("deadline-parent-late", True))
        observed["cached_call_refused"] = await _expired(lambda: cached.get_count(GetCountRequest()))
        observed["new_call_refused"] = await _expired(lambda: CounterClient.bind(ctx, "RUST", "guest-followup").get_count(GetCountRequest()))
        observed["late_done"] = True
        raise
    finally:
        ctx._verify_rpc = verify


async def call(ctx, binding, name, timeout):
    node = await ctx.var("DOMAIN_MARKER")
    client = CounterClient.bind(ctx, binding, name)
    try:
        reply = await client.get_count(GetCountRequest(), timeout_ms=int(timeout) if timeout is not None else None)
        result = {"writes": reply.count.value}
    except SDKError as error:
        result = {"code": error.code, "outcome": error.outcome}
    # Warmup only establishes ownership; reserve the bounded followup audit for
    # deliberately timed calls whose mutation admission must be released.
    followup = None if timeout is None else await CounterClient.bind(ctx, "RUST", "guest-followup").get_count(GetCountRequest())
    return Response.json(dict(node=node, result=result, followup_writes=followup.count.value if followup is not None else None))


async def codec_budget(ctx, binding, phase, operation_id):
    assert phase in ("verify", "encode", "decode")
    client = CounterClient.bind(ctx, binding, "codec-budget-" + phase)
    verify, encode, decode = ctx._verify_rpc, cellar_rpc.encode_message, IncrementResponse.FromString
    entered = []

    def slow_verify(method):
        entered.append("verify")
        time.sleep(0.12)
        ctx._verify_rpc = verify
        # Install only after the deliberate block, while this caller owns the
        # interpreter turn; no provider may be submitted from this expired call.
        cellar_rpc.encode_message = unexpected_encode
        return verify(method)

    def unexpected_encode(message, method):
        raise AssertionError("Expired verification reached encoding")

    def slow_encode(message, method):
        entered.append("encode")
        time.sleep(0.12)
        # Restore before any await: remote Python providers use the same module.
        cellar_rpc.encode_message = encode
        return encode(message, method)

    def slow_decode(body):
        entered.append("decode")
        time.sleep(0.6)
        return decode(body)

    try:
        if phase == "verify":
            ctx._verify_rpc = slow_verify
        elif phase == "encode":
            cellar_rpc.encode_message = slow_encode
        else:
            IncrementResponse.FromString = slow_decode
        try:
            await client.increment(IncrementRequest(delta=1, operation_id=operation_id),
                                   timeout_ms=400 if phase == "decode" else 20)
        except SDKError as error:
            failure = dict(code=error.code, outcome=error.outcome)
        else:
            raise AssertionError("Slow remote codec escaped its original deadline")
    finally:
        ctx._verify_rpc = verify
        cellar_rpc.encode_message = encode
        IncrementResponse.FromString = decode
    assert entered == [phase], entered
    saved = await client.get_count(GetCountRequest())
    replay = None
    if phase == "decode":
        receipt = await client.increment(IncrementRequest(delta=1, operation_id=operation_id))
        replay = str(receipt.count.value)
    return Response.json(dict(failure, codec_entered=phase, count=str(saved.count.value), replay=replay))
