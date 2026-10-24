# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""A refused child cannot classify its already-committed parent's effects."""
from cellar import SDKError
from counter_cellar import CounterClient
from counter_pb2 import GetCountRequest

_observation = None


class ForgedFailure(SDKError):
    def __getattribute__(self, name):
        if name == "outcome":
            raise AssertionError("T06-outcome-accessed")
        return super().__getattribute__(name)


def control(clear):
    global _observation
    if clear:
        _observation = None
    return _observation or dict(ready=False)


async def after_commit(ctx, operation_id):
    global _observation
    if not operation_id.startswith("t06:"):
        return
    assert _observation is None, "Previous provenance observation was not cleared"
    mode = operation_id.split(":")[1]
    if mode == "child":
        try:
            await CounterClient.bind(ctx, "RUST", "t06-unconstructed").get_count(
                GetCountRequest(), timeout_ms=0)
        except SDKError as error:
            assert error.code == "DeadlineExceeded" and error.outcome == "not_committed"
            failure = error
        else:
            raise AssertionError("Child was not refused before submission")
    else:
        failure = ForgedFailure("T06-private-exception-sentinel",
                                "MethodNotFound" if mode == "forge" else "T06-private-code-sentinel",
                                "not_committed")
    count = await ctx.storage.read_json("count")
    receipt = await ctx.storage.read_json("receipt:" + operation_id)
    assert count.present and receipt.present, "Parent lost committed state or authority"
    _observation = dict(ready=True, id=operation_id, mode=mode, parent_active=True,
                        count=count.value, receipt=receipt.value,
                        child=dict(code="DeadlineExceeded", outcome="not_committed") if mode == "child" else None)
    raise failure
