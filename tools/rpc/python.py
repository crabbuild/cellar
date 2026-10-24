"""Reference Python provider and consumer for the all-backend qualification."""
import asyncio
from urllib.parse import urlsplit, parse_qsl
from cellar import Conflict, Response, SDKError
from counter_pb2 import Count, GetCountRequest, GetCountResponse, IncrementRequest, IncrementResponse, OperationConflict, OutOfRange
from counter_cellar import CounterClient, CounterRegistration
import live
import deadline

constructed = 0
retained = None
cancel_releases = {}


class PythonCounter:
    def __init__(self):
        global constructed
        constructed += 1

    async def get_count(self, request, ctx):
        if ctx.identity.name == "deadline-nested":
            return await deadline.nested(ctx)
        current = await ctx.storage.read_json("count")
        return GetCountResponse(count=Count(value=int(current.value) if current.present else 0))

    async def increment(self, request, ctx):
        if not request.HasField("delta") or request.delta <= 0:
            return IncrementResponse(out_of_range=OutOfRange(reason="positive delta is required"))
        if not request.operation_id or len(request.operation_id.encode("utf-8")) > 128:
            return IncrementResponse(out_of_range=OutOfRange(reason="operation ID is required"))
        key = "receipt:" + request.operation_id
        for _ in range(16):
            current = await ctx.storage.read_json("count")
            saved = await ctx.storage.read_json(key)
            if saved.present:
                if saved.value["delta"] != str(request.delta):
                    return IncrementResponse(operation_conflict=OperationConflict())
                return IncrementResponse(count=Count(value=int(saved.value["value"])))
            value = (int(current.value) if current.present else 0) + request.delta
            if value > 2**63 - 1:
                return IncrementResponse(out_of_range=OutOfRange(reason="counter overflow"))
            try:
                await ctx.storage.apply(checks={"count": current, key: saved}, puts={
                    "count": str(value), key: {"delta": str(request.delta), "value": str(value)},
                })
            except Conflict:
                continue
            # Exercise host output proof independently of an explicit sync await.
            await live.sync(ctx, request.operation_id)
            if not request.operation_id.startswith("proof-no-sync-"):
                await ctx.storage.sync()
            if request.operation_id == "invalid-reply":
                # Intentionally wrong after commit. Retry must use its receipt.
                return GetCountResponse(count=Count(value=value))
            return IncrementResponse(count=Count(value=value))
        raise RuntimeError("Counter conflict budget exhausted")

    async def private_helper(self, request, ctx):
        raise AssertionError("Unregistered helper was invoked")


__cellar_rpc__ = {"PythonCounter": CounterRegistration}


async def fetch(request, ctx):
    global retained
    url = urlsplit(request.url)
    query = dict(parse_qsl(url.query))
    if url.path.endswith("/cancel-release"):
        cancel_releases[query["name"]][query.get("phase", "cancel")].set()
        return Response.json({"released": True})
    if url.path.endswith("/cancel-state"):
        control = cancel_releases[query["name"]]
        return Response.json({key: control[key] for key in ("settled", "result", "followup_writes")})
    if url.path.endswith("/cancel-attack"):
        original = cancel_releases[query["name"]]["context"]
        try:
            original._bridge.cancel_operation(original._sequence)
        except RuntimeError as error:
            assert str(error) == "ContextMismatch"
            return Response.json({"scope_refused": True})
        raise AssertionError("Another invocation cancelled the original call")
    if url.path.endswith("/cancel") or url.path.endswith("/proof-cancel"):
        proof = url.path.endswith("/proof-cancel")
        name = query["id"] if proof else query["name"]
        deadline_trigger = proof and query.get("trigger") == "deadline"
        controls = cancel_releases[name] = {"cancel": asyncio.Event(), "finish": asyncio.Event(), "context": ctx,
                                           "settled": False, "result": None, "followup_writes": None}
        binding = {"rust": "RUST", "python": "PYTHON", "v8": "V8", "node": "NODE"}[query["target"]] if proof else "RUST"
        counter = CounterClient.bind(ctx, binding, "alice" if proof else name)
        pending = counter.increment(IncrementRequest(delta=int(query["delta"]), operation_id=name), timeout_ms=1000 if deadline_trigger else 5000) if proof else counter.get_count(GetCountRequest(), timeout_ms=5000)
        task = asyncio.create_task(pending)
        try:
            # The host sends this control only after observing the target's
            # uncommitted SQLite write. No timer guesses when to cancel.
            if not deadline_trigger:
                await controls["cancel"].wait()
                task.cancel()
            try:
                await task
                raise AssertionError("RPC task ignored cancellation")
            except asyncio.CancelledError:
                assert not deadline_trigger
                result = {"cancelled": True}
            except SDKError as error:
                assert deadline_trigger and error.code == "DeadlineExceeded" and error.outcome == "unknown"
                result = {"code": error.code, "outcome": error.outcome}
            assert ctx._bridge.cancel_operation(1) is False
            # Awaiting cancellation must settle the original native operation
            # and its mutation permit before another call can be admitted.
            followup = CounterClient.bind(ctx, "RUST", "cancel-followup")
            try:
                reply = await followup.get_count(GetCountRequest())
                followup_writes = reply.count.value
            except SDKError as error:
                followup_writes = {"code": error.code, "outcome": error.outcome}
            controls.update(settled=True, result=result, followup_writes=followup_writes)
            # Keep the parent alive while the blocked target attempts late SDK
            # work, so parent retirement cannot stand in for task cancellation.
            await controls["finish"].wait()
            final_followup = await followup.get_count(GetCountRequest()) if proof else None
            return Response.json({"node": await ctx.var("DOMAIN_MARKER"),
                                  "result": result,
                                  "followup_writes": followup_writes,
                                  "final_followup_writes": final_followup.count.value if final_followup is not None else followup_writes})
        finally:
            cancel_releases.pop(name, None)
    if url.path.endswith("/constructors"):
        return Response.json({"constructed": constructed})
    if url.path.endswith("/probe"):
        return Response.json(live.snapshot())
    if url.path.endswith("/probe-release"):
        return Response.json(live.control("release"))
    if url.path.endswith("/probe-clear"):
        return Response.json(live.control("clear"))
    binding = {"rust": "RUST", "python": "PYTHON", "v8": "V8", "node": "NODE"}[query.get("target", "python")]
    if url.path.endswith("/deadline-control"):
        return deadline.control(query["action"], query.get("child", ""))
    if url.path.endswith("/deadline"):
        return await deadline.call(ctx, binding, query["name"], query.get("timeout"))
    if url.path.endswith("/codec-budget"):
        return await deadline.codec_budget(ctx, binding, query["phase"], query["id"])
    counter = CounterClient.bind(ctx, binding, "alice")
    if url.path.endswith("/capture"):
        retained = counter
        return Response.json({"captured": True})
    try:
        if url.path.endswith("/expired"):
            await retained.get_count(GetCountRequest())
            raise AssertionError("Expired client remained usable")
        if url.path.endswith("/read"):
            reply = await counter.get_count(GetCountRequest())
            return Response.json({"value": str(reply.count.value)})
        reply = await counter.increment(IncrementRequest(
            delta=int(query["delta"]) if "delta" in query else None,
            operation_id=query.get("id", ""),
        ))
        case = reply.WhichOneof("result")
        return Response.json({"value": str(reply.count.value)} if case == "count" else {"error": case})
    except SDKError as error:
        return Response.json({"code": error.code, "outcome": error.outcome})
