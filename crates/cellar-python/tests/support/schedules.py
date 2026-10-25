# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""The real reserved cron cell invokes Python and retains its own history."""
import asyncio
from cellar import Response, SDKError

holding = asyncio.Event()
release = asyncio.Event()
retained = None
HOLD = "* * * * *"
RETRY = "*/1 * * * *"
DECLINE = "* 0-23 * * *"
SUCCESS = "* * 1-31 * *"


async def refused(call, code):
    try:
        await call()
    except SDKError as error:
        assert error.code == code and error.outcome == "not_committed"
    else:
        raise AssertionError("scheduled handle escaped its original invocation")


async def fetch(request, ctx):
    database = ctx.sql("DB")
    if request.path == "/setup":
        await database.query("CREATE TABLE effects (cron TEXT, occurrence INTEGER, PRIMARY KEY(cron, occurrence))")
        await database.query("CREATE TABLE attempts (cron TEXT, occurrence INTEGER, attempt INTEGER, PRIMARY KEY(cron, occurrence, attempt))")
    elif request.path == "/state":
        return Response.json({"holding": holding.is_set(), "now": await ctx.now_ms(),
                              "effects": (await database.query("SELECT cron, occurrence FROM effects ORDER BY cron, occurrence"))["results"],
                              "attempts": (await database.query("SELECT cron, occurrence, attempt FROM attempts ORDER BY cron, occurrence, attempt"))["results"]})
    elif request.path == "/release":
        release.set()
    elif request.path in ("/cross", "/expired"):
        code = "ContextMismatch" if request.path == "/cross" else "ContextExpired"
        assert retained._context._bridge.active() == (code == "ContextMismatch")
        await refused(retained.no_retry, code)
    else:
        return Response("unknown route", 404)
    return Response.json(True)


async def scheduled(event, ctx):
    global retained
    database = ctx.sql("DB")
    key = [event.cron, event.scheduled_ms]
    await database.query("INSERT OR IGNORE INTO effects VALUES (?, ?)", key)
    attempt = (await database.query("SELECT COUNT(*) AS n FROM attempts WHERE cron=? AND occurrence=?", key))["results"][0]["n"] + 1
    await database.query("INSERT INTO attempts VALUES (?, ?, ?)", key + [attempt])
    if event.cron == HOLD:
        retained = event
        holding.set()
        await release.wait()
    elif event.cron == RETRY and attempt == 1:
        raise RuntimeError("retry this same occurrence")
    elif event.cron == DECLINE:
        await event.no_retry()
        raise RuntimeError("explicitly decline this occurrence's retry")
