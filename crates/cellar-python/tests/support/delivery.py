# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Real broker delivery: decisions, replay, cancellation and retained authority."""
import asyncio
from cellar import Response, SDKError

holding = asyncio.Event()
release = asyncio.Event()
retained = None
cancelled = []


async def refused(call, code):
    try:
        await call()
    except SDKError as error:
        assert error.code == code and error.outcome == "not_committed"
    else:
        raise AssertionError("settlement escaped its invocation")


async def fetch(request, ctx):
    database = ctx.sql("DB")
    if request.path == "/setup":
        await database.query("CREATE TABLE effects (command TEXT PRIMARY KEY)")
        await database.query("CREATE TABLE attempts (message TEXT, attempt INTEGER, command TEXT, PRIMARY KEY(message, attempt))")
    elif request.path == "/send":
        await ctx.queue("JOBS").send(await request.json())
    elif request.path == "/burst":
        async def send():
            try:
                await ctx.queue("JOBS").send({"kind": "burst"})
                return "accepted"
            except SDKError as error:
                assert error.code == "Overloaded" and error.outcome == "not_committed"
                return "refused"
        results = await asyncio.gather(*(send() for _ in range(32)))
        return Response.json({"accepted": results.count("accepted"), "refused": results.count("refused")})
    elif request.path == "/state":
        return Response.json({"effects": (await database.query("SELECT command FROM effects ORDER BY command"))["results"],
                              "attempts": (await database.query("SELECT message, attempt, command FROM attempts ORDER BY command, attempt"))["results"],
                              "holding": holding.is_set(), "cancelled": cancelled})
    elif request.path == "/release":
        release.set()
    elif request.path in ("/cross", "/expired"):
        message, batch = retained
        if request.path == "/cross":
            assert message._context._bridge.active()
            code = "ContextMismatch"
        else:
            assert not message._context._bridge.active()
            code = "ContextExpired"
        for call in (message.ack, message.retry, batch.ack_all, batch.retry_all):
            await refused(call, code)
    elif request.path == "/wrong-event":
        await refused(lambda: ctx._call("queue.settle", retry=False), "ValidationError")
        await refused(lambda: ctx._call("cron.no_retry"), "ValidationError")
    else:
        return Response("unknown route", 404)
    return Response.json(True)


async def queue(batch, ctx):
    global retained
    assert len(batch.messages) == 1
    message = batch.messages[0]
    kind = message.body["kind"]
    database = ctx.sql("DB")
    # A business receipt, not a promise of exactly-once queue delivery.
    await database.query("INSERT OR IGNORE INTO effects VALUES (?)", [kind])
    await database.query("INSERT INTO attempts VALUES (?, ?, ?)", [message.id, message.attempts, kind])
    if kind == "hold":
        retained = (message, batch)
        holding.set()
        await release.wait()
    elif kind == "cancel" and message.attempts == 1:
        try:
            await asyncio.Event().wait()
        finally:
            # Native timeout revoked this original turn before Python cleanup.
            await refused(message.ack, "ContextExpired")
            cancelled.append(kind)
    elif kind == "error" and message.attempts == 1:
        raise RuntimeError("retry without settlement")
    elif kind == "poison":
        raise RuntimeError("bounded retry exhaustion")
    elif kind == "ack_error":
        await message.ack()
        raise RuntimeError("explicit acknowledgement survives handler failure")
    elif kind == "retry_ack" and message.attempts == 1:
        await message.retry(0)
        await message.ack()
        return
    elif kind == "ack_retry":
        await message.ack()
        await message.retry(0)
        return
    elif kind == "ackall_retry":
        await batch.ack_all()
        await batch.retry_all(0)
        raise RuntimeError("explicit batch acknowledgement survives failure")
    elif kind == "retryall_ack" and message.attempts == 1:
        await batch.retry_all(0)
        await batch.ack_all()
        raise RuntimeError("first batch decision wins")
    elif kind == "invalid":
        await refused(lambda: ctx._call("queue.settle", id="absent", retry=False), "ValidationError")
        await refused(lambda: message.retry(-1), "ValidationError")
    elif kind == "success":
        return  # The successful batch defaults to acknowledgement.
    await message.ack()
