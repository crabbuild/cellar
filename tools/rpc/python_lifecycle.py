# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Keep an expired typed handler alive without preserving native authority."""
import asyncio
from cellar import SDKError
from counter_cellar import CounterClient
from counter_pb2 import GetCountRequest, IncrementRequest

_state = None
_wake = None


def snapshot():
    if _state is None:
        return {"armed": False}
    return {key: value for key, value in _state.items() if key != "context_id"}


def control(query):
    global _state, _wake
    action = query["action"]
    if action == "arm":
        assert _state is None, "Previous suppression task was not cleared"
        _state = dict(armed=True, id=query["id"], after=query["after"] == "true",
                      stubborn=query.get("stubborn") == "true", claimed=False,
                      cancellations=0, probes=0, requested=0, finish=False,
                      finished=False, refusals=None)
        _wake = asyncio.Event()
    else:
        assert _state is not None, "No suppression task"
        if action == "probe":
            assert _state["cancellations"] > 0 and not _state["finished"]
            _state["requested"] += 1
            _wake.set()
        elif action == "finish":
            assert not _state["stubborn"], "Persistent suppression must fail shutdown"
            _state["finish"] = True
            _wake.set()
        elif action == "clear":
            assert _state["finished"], "Suppression task is still alive"
            _state = _wake = None
        else:
            assert action == "state", "Unknown suppression control"
    return snapshot()


async def _refused(operation):
    try:
        await operation()
    except SDKError as error:
        return dict(code=error.code, outcome=error.outcome)
    raise AssertionError("Expired Python authority was accepted")


async def _wait(context):
    state = _state
    late = CounterClient.bind(context, "RUST", "python-late")
    try:
        await CounterClient.bind(context, "RUST", "worker-control").get_count(GetCountRequest())
        raise AssertionError("Native suppression gate returned before cancellation")
    except asyncio.CancelledError:
        state["cancellations"] += 1
        while not state["finish"]:
            try:
                await _wake.wait()
                _wake.clear()
            except asyncio.CancelledError:
                state["cancellations"] += 1
                continue
            if state["requested"] > state["probes"]:
                refusals = {
                    "write": await _refused(lambda: context.storage.put_json("python-late", True)),
                    "read": await _refused(lambda: context.storage.read_json("count")),
                    "rpc": await _refused(lambda: late.increment(IncrementRequest(delta=1, operation_id="python-late"))),
                }
                try:
                    context._bridge.submit(10000, "storage.apply", '{"puts":{"python-late":true}}')
                except RuntimeError as error:
                    refusals["raw"] = str(error)
                else:
                    raise AssertionError("Expired raw Python bridge was accepted")
                state["refusals"] = refusals
                state["probes"] += 1
        state["finished"] = True
        # A stale business method must not continue its normal mutation path.
        raise asyncio.CancelledError()


async def before(context, operation_id):
    if _state is None or _state["id"] != operation_id or _state["claimed"]:
        return
    _state["claimed"] = True
    _state["context_id"] = id(context)
    if not _state["after"]:
        await _wait(context)


async def after(context, operation_id):
    if (_state is not None and _state["id"] == operation_id and _state["after"]
            and _state.get("context_id") == id(context)):
        await _wait(context)
