# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""In-flight faults on real KV, shared SQL and queue producer SDK calls."""
import asyncio
from cellar import Response, SDKError

progress = {}
releases = {}
calls = {}


async def execute(ctx, binding, mode):
    command = binding + "-" + mode
    calls[command] = calls.get(command, 0) + 1
    if binding == "kv":
        await ctx.kv("CACHE").put(command, {"receipt": command})
    elif binding == "sql":
        await ctx.sql("DB").query("INSERT OR IGNORE INTO receipts VALUES (?)", [command])
    elif binding == "queue":
        await ctx.queue("JOBS").send({"command": command})
    else:
        raise AssertionError("unknown binding")


async def fetch(request, ctx):
    path = request.path.split("/")
    if request.path == "/setup":
        await ctx.kv("CACHE").put("seed", True)
        await ctx.sql("DB").query("CREATE TABLE receipts (command TEXT PRIMARY KEY)")
        await ctx.sql("DB").query("CREATE TABLE deliveries (command TEXT PRIMARY KEY)")
        # Warm the real producer/broker before introducing storage faults.
        await ctx.queue("JOBS").send({"command": "seed"})
    elif request.path == "/progress":
        return Response.json({"progress": progress, "calls": calls})
    elif request.path == "/state":
        return Response.json({"kv": {mode: await ctx.kv("CACHE").get("kv-" + mode)
                                      for mode in ("before", "cancel", "failure", "burst")},
                              "sql": (await ctx.sql("DB").query("SELECT command FROM receipts ORDER BY command"))["results"],
                              "queue": (await ctx.sql("DB").query("SELECT command FROM deliveries ORDER BY command"))["results"],
                              "calls": calls})
    elif path[1] == "cancel":
        releases[path[2]].set()
    elif path[1] == "reconcile":
        await execute(ctx, path[2], path[3])
    elif path[1] == "burst":
        binding = path[2]
        async def send():
            try:
                await execute(ctx, binding, "burst")
                return "accepted"
            except SDKError as error:
                assert error.code == "Overloaded" and error.outcome == "not_committed"
                return "refused"
        results = await asyncio.gather(*(send() for _ in range(32)))
        return Response.json({"accepted": results.count("accepted"), "refused": results.count("refused")})
    elif path[1] == "attempt":
        binding, mode = path[2:4]
        command = binding + "-" + mode
        releases[command] = asyncio.Event()
        task = asyncio.create_task(execute(ctx, binding, mode))
        if mode == "before":
            task.cancel()
        else:
            progress[command] = {"phase": "pending"}
            if mode == "cancel":
                await releases[command].wait()
                task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            progress[command] = {"phase": "cancelled"}
        except SDKError as error:
            progress[command] = {"phase": "failed", "code": error.code, "outcome": error.outcome}
        else:
            progress[command] = {"phase": "succeeded"}
        return Response.json(progress[command])
    else:
        return Response("unknown route", 404)
    return Response.json(True)


async def queue(batch, ctx):
    for message in batch.messages:
        # Queue delivery can repeat after an unknown producer outcome or an
        # explicit reconciliation. Only the business key deduplicates effects.
        await ctx.sql("DB").query("INSERT OR IGNORE INTO deliveries VALUES (?)", [message.body["command"]])
        await message.ack()
