"""Concrete generated clients and structural handler typing, checked by mypy."""
from typing import assert_type

from cellar import Context
from counter_cellar import CounterClient, CounterHandler
from counter_pb2 import Count, GetCountRequest, GetCountResponse, IncrementRequest, IncrementResponse
from profile_pb2 import AllTypes
from profile_cellar import ProfileClient
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.duration_pb2 import Duration
from google.protobuf.empty_pb2 import Empty


class Counter:
    async def increment(self, request: IncrementRequest, ctx: Context) -> IncrementResponse:
        return IncrementResponse(count=Count(value=request.delta))

    async def get_count(self, request: GetCountRequest, ctx: Context) -> GetCountResponse:
        return GetCountResponse(count=Count(value=2**53 + 1))


handler: CounterHandler = Counter()


async def typed_calls(context: Context) -> None:
    client = CounterClient.bind(context, "COUNTERS", "alice")
    reply = await client.increment(IncrementRequest(delta=2**53 + 1), timeout_ms=1000)
    assert_type(reply, IncrementResponse)
    assert_type(reply.count.value, int)
    assert_type(await client.get_count(GetCountRequest()), GetCountResponse)
    # Each suppression must match an actual error (`warn_unused_ignores`). This
    # also catches accidental Any return/input types in generated APIs.
    await client.increment(GetCountRequest())  # type: ignore[arg-type]
    await client.get_count(GetCountRequest(), timeout_ms="1000")  # type: ignore[arg-type]
    await client.private_helper()  # type: ignore[attr-defined]
    wrong: str = reply.count.value  # type: ignore[assignment]
    profile = ProfileClient.bind(context, "PROFILES", "types")
    assert_type(await profile.echo_child(AllTypes.Child()), AllTypes.Child)
    assert_type(await profile.echo_timestamp(Timestamp()), Timestamp)
    assert_type(await profile.echo_duration(Duration()), Duration)
    assert_type(await profile.ping(Empty()), Empty)
    await profile.echo_child(AllTypes())  # type: ignore[arg-type]


class IncorrectHandler:
    async def increment(self, request: GetCountRequest, ctx: Context) -> GetCountResponse:
        return GetCountResponse()


invalid: CounterHandler = IncorrectHandler()  # type: ignore[assignment]
