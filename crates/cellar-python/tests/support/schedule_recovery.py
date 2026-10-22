"""Business idempotency and authority under native cron timeout/owner loss."""
import asyncio
from cellar import Response, SDKError

entered = False
cancelled = 0
refused = 0


async def schema(database):
    await database.query("CREATE TABLE IF NOT EXISTS effects (cron TEXT, occurrence INTEGER, PRIMARY KEY(cron, occurrence))")
    await database.query("CREATE TABLE IF NOT EXISTS attempts (cron TEXT, occurrence INTEGER, attempt INTEGER, PRIMARY KEY(cron, occurrence, attempt))")
    await database.query("CREATE TABLE IF NOT EXISTS expired (value INTEGER)")


async def fetch(request, ctx):
    database = ctx.sql("DB")
    if request.path == "/ready":
        return Response.json(True)
    if request.path == "/setup":
        await schema(database)
        return Response.json(True)
    if request.path == "/state":
        return Response.json({"entered": entered, "cancelled": cancelled, "refused": refused,
                              "effects": (await database.query("SELECT * FROM effects ORDER BY occurrence"))["results"],
                              "attempts": (await database.query("SELECT * FROM attempts ORDER BY occurrence, attempt"))["results"],
                              "expired": (await database.query("SELECT * FROM expired"))["results"]})
    return Response("unknown route", 404)


async def scheduled(event, ctx):
    global entered, cancelled, refused
    database = ctx.sql("DB")
    await schema(database)
    key = [event.cron, event.scheduled_ms]
    await database.query("INSERT OR IGNORE INTO effects VALUES (?, ?)", key)
    attempt = (await database.query("SELECT COUNT(*) AS n FROM attempts WHERE cron=? AND occurrence=?", key))["results"][0]["n"] + 1
    await database.query("INSERT INTO attempts VALUES (?, ?, ?)", key + [attempt])
    if FAULT != "recovered" and attempt == 1:
        entered = True
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled += 1
            assert not ctx._bridge.active()
            for call in (event.no_retry, lambda: database.query("INSERT INTO expired VALUES (1)")):
                try:
                    await call()
                except SDKError as error:
                    assert error.code == "ContextExpired" and error.outcome == "not_committed"
                    refused += 1
                else:
                    raise AssertionError("cancelled cron retained invocation authority")
            raise
