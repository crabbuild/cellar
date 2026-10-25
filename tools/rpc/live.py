# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""A noncooperative typed handler, resumed only after its native owner fences."""
import asyncio
from cellar import SDKError
from counter_cellar import CounterClient
from counter_pb2 import IncrementRequest

_probe = None
_release = None


def snapshot():
    return dict(_probe) if _probe else {"ready": False}


def control(action):
    global _probe, _release
    if not _probe:
        raise AssertionError("Live probe does not exist")
    if action == "release":
        assert _probe["cancelled"], "Live handler was not cancelled by its fence"
        _release.set()
        return {"released": True}
    assert action == "clear" and _probe["late_done"]
    _probe = _release = None
    return {"cleared": True}


async def _expired(operation):
    try:
        await operation()
    except SDKError as error:
        return error.code == "ContextExpired" and error.outcome == "not_committed"
    return False


async def sync(ctx, operation_id):
    global _probe, _release
    if not operation_id.startswith(("proof-fence-sync-", "proof-cancel-sync-")):
        return False
    node = await ctx.var("DOMAIN_MARKER")
    if node != "proof-owner":
        return False  # Explicit retries run with a new owner's authority.
    assert _probe is None and ctx.identity is not None
    count = await ctx.storage.read_json("count")
    receipt = await ctx.storage.read_json("receipt:" + operation_id)
    assert count.present and receipt.present
    client = CounterClient.bind(ctx, "RUST", "alice")
    event = asyncio.Event()
    _release = event
    current = dict(ready=True, id=operation_id, node=node,
                   **{"class": ctx.identity.class_name}, scope=ctx.identity.scope,
                   epoch=str(ctx.identity.epoch), count=count.value, receipt=receipt.value,
                   active_before=ctx._bridge.active(), committed=True, waiting=True,
                   cancelled=False, late_done=False, storage_refused=False,
                   read_refused=False, call_refused=False)
    _probe = current
    try:
        await ctx.storage.sync()
    except (asyncio.CancelledError, SDKError):
        current["cancelled"] = True
        current["waiting"] = False
        # The owning driver may cancel repeatedly. Retain only this original
        # task, never an unowned SDK task, until the coordinator resumes it.
        while not event.is_set():
            try:
                await event.wait()
            except asyncio.CancelledError:
                pass
        current["storage_refused"] = await _expired(lambda: ctx.storage.put_json("late-fenced", True))
        current["read_refused"] = await _expired(lambda: ctx.storage.read_json("count"))
        current["call_refused"] = await _expired(lambda: client.increment(IncrementRequest(delta=1, operation_id="fenced-late")))
        current["bridge_inactive"] = not ctx._bridge.active()
        try:
            ctx._bridge.submit(10000, "storage.apply", '{"puts":{"late-raw":true}}')
        except RuntimeError as error:
            current["raw_refused"] = str(error) == "ContextExpired"
        else:
            current["raw_refused"] = False
        current["late_done"] = True
        raise asyncio.CancelledError()
    raise AssertionError("Sync completed through withheld proof")
