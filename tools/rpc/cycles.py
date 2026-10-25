# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Qualification-only named cycles; retained frames use their original context."""
import asyncio
from cellar import SDKError
from counter_pb2 import GetCountRequest, IncrementRequest
from counter_cellar import CounterClient

_probes = {}


def control(action):
    if action == "release":
        for probe in _probes.values():
            probe["release"].set()
    elif action == "clear":
        assert all(probe["late_done"] for probe in _probes.values()), "Cycle work has not settled"
        _probes.clear()
    else:
        assert action == "status", "Unknown cycle control"
    return [{key: value for key, value in probe.items() if key != "release"}
            for probe in _probes.values()]


async def _expired(work):
    try:
        await work()
    except SDKError as error:
        return error.code == "ContextExpired" and error.outcome == "not_committed"
    return False


async def after_commit(ctx, operation_id):
    if not operation_id.startswith("t15|"):
        return
    prefix, a, b, shape, role = operation_id.split("|")
    assert shape in ("self", "aba") and role in ("outer", "middle", "inner")
    assert role not in _probes and len(_probes) < 3, "Duplicate cycle frame"
    cached = CounterClient.bind(ctx, b if role == "middle" else a,
                                "cycle-b" if role == "middle" else "cycle-a")
    probe = dict(role=role, waiting=True, late_done=False, storage_refused=False,
                 read_refused=False, call_refused=False, release=asyncio.Event())
    _probes[role] = probe
    try:
        if role == "inner":
            await CounterClient.bind(ctx, "RUST", "cycle-gate").get_count(
                GetCountRequest(), timeout_ms=60000)
        else:
            child = "middle" if role == "outer" and shape == "aba" else "inner"
            await CounterClient.bind(ctx, b if child == "middle" else a,
                                     "cycle-b" if child == "middle" else "cycle-a").increment(
                IncrementRequest(delta=1, operation_id="|".join((prefix, a, b, shape, child))),
                timeout_ms=60000)
    except (SDKError, asyncio.CancelledError):
        probe["waiting"] = False
        # Retain the original frame despite native cancellation. No detached
        # task is created; the runtime must supervise this continuation.
        while not probe["release"].is_set():
            try:
                await probe["release"].wait()
            except asyncio.CancelledError:
                pass
        probe["storage_refused"] = await _expired(lambda: ctx.storage.apply(puts={"count": "999"}))
        probe["read_refused"] = await _expired(lambda: ctx.storage.read_json("count"))
        probe["call_refused"] = await _expired(lambda: cached.increment(
            IncrementRequest(delta=100, operation_id="cycle-late")))
        probe["late_done"] = True
        raise
    raise AssertionError("Held cycle unexpectedly completed")
