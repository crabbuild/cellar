"""A producer's unknown outcome must not replay a live recovered queue lease."""
from cellar import Response, SDKError

producer_calls = 0
deliveries = []


async def fetch(request, ctx):
    global producer_calls
    if request.path == "/setup":
        await ctx.sql("DB").query(
            "CREATE TABLE receipts (command TEXT PRIMARY KEY, message TEXT, attempt INTEGER)"
        )
        await ctx.queue("JOBS").send({"command": "seed"})
    elif request.path == "/send":
        producer_calls += 1
        try:
            await ctx.queue("JOBS").send({"command": "lost-ack"})
        except SDKError as error:
            return Response.json({"code": error.code, "outcome": error.outcome})
    elif request.path == "/progress":
        return Response.json({"calls": producer_calls, "deliveries": deliveries})
    elif request.path == "/state":
        return Response.json((await ctx.sql("DB").query(
            "SELECT command, message, attempt FROM receipts ORDER BY command"
        ))["results"])
    else:
        return Response("unknown route", 404)
    return Response.json(True)


async def queue(batch, ctx):
    for message in batch.messages:
        observation = {"command": message.body["command"], "id": message.id,
                       "attempt": message.attempts, "at_ms": None}
        deliveries.append(observation)
        observation["at_ms"] = await ctx.now_ms()
        await ctx.sql("DB").query(
            "INSERT OR IGNORE INTO receipts VALUES (?, ?, ?)",
            [message.body["command"], message.id, message.attempts],
        )
        await message.ack()
