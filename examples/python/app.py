"""Durable Python counter. Cellar owns storage, routing and recovery."""
import hashlib
import asyncio
import os
import numpy as np
from typing_inspection.introspection import get_literal_values
from typing import Literal
from cellar import Conflict, Response
from pydantic import BaseModel, Field, ValidationError


class Increment(BaseModel):
    amount: int = Field(default=1, ge=1, le=1000, strict=True)


async def fetch(request, ctx):
    if request.path == "/libraries":
        def numerical_work():
            values = np.arange(9, dtype=np.float64).reshape(3, 3)
            return (values @ values.T).tolist()
        matrix, checksum = await asyncio.gather(
            asyncio.to_thread(numerical_work),
            asyncio.to_thread(lambda: hashlib.sha256(b"cellar").hexdigest()))
        assert list(get_literal_values(Literal["durable", "python"])) == ["durable", "python"]
        return Response.json({"matrix": matrix, "sha256": checksum, "numpy": np.__version__})
    if request.path == "/health":
        response = Response.json({"runtime": "cpython", "sha256": hashlib.sha256(b"cellar").hexdigest()})
        if instance := os.environ.get("CELLAR_SUPERVISOR_INSTANCE"):
            response.headers.append(("x-cellar-supervisor-instance", instance))
        return response
    if request.path.startswith("/counters/"):
        name = request.path.split("/")[2]
        return await ctx.namespace("COUNTERS").get_by_name(name).fetch(request)
    if request.path == "/kv":
        cache = ctx.kv("CACHE")
        if request.method == "POST":
            await cache.put("example", await request.json())
        return Response.json(await cache.get("example"))
    if request.path == "/sql":
        db = ctx.sql("DB")
        await db.query("CREATE TABLE IF NOT EXISTS messages (body TEXT NOT NULL)")
        if request.method == "POST":
            await db.query("INSERT INTO messages (body) VALUES (?)", [await request.text()])
        return Response.json(await db.query("SELECT body FROM messages ORDER BY rowid"))
    if request.path == "/jobs":
        listing = await ctx.kv("CACHE").list(prefix="job:")
        return Response.json([await ctx.kv("CACHE").get(key["name"]) for key in listing["keys"]])
    if request.path == "/cron":
        return Response.json(await ctx.kv("CACHE").get("last-cron"))
    if request.path == "/queue" and request.method == "POST":
        await ctx.queue("JOBS").send(await request.json())
        return Response.json({"queued": True}, 202)
    return Response("use /counters/NAME, /kv, /sql or /queue", 404)


async def queue(batch, ctx):
    for message in batch.messages:
        if isinstance(message.body, dict) and message.body.get("retry_once") and message.attempts == 1:
            await message.retry(delay_seconds=1)
            continue
        # At-least-once delivery: choose an idempotency key for business effects.
        await ctx.kv("CACHE").put("job:" + message.id, message.body)
        await message.ack()


async def scheduled(event, ctx):
    await ctx.kv("CACHE").put("last-cron", {"scheduled_ms": event.scheduled_ms, "cron": event.cron})


class Counter:
    async def fetch(self, request, ctx):
        storage = ctx.storage
        if request.path.endswith("/alarm") and request.method == "POST":
            await storage.set_alarm(await ctx.now_ms() + 500)
        if request.method == "POST" and not request.path.endswith("/alarm"):
            try:
                change = Increment.model_validate(await request.json() if request.body else {})
            except (ValidationError, ValueError):
                return Response.json({"error": "amount must be an integer from 1 to 1000"}, 400)
            keys = [value for name, value in request.headers if name.lower() == "idempotency-key"]
            if len(keys) > 1 or (keys and (not keys[0] or len(keys[0]) > 128 or
                    any(not c.isascii() or not (c.isalnum() or c in "-_.") for c in keys[0]))):
                return Response.json({"error": "idempotency-key must be 1..128 ASCII letters, digits, -, _ or ."}, 400)
            receipt_key = "receipt:" + keys[0] if keys else None
            receipt = await storage.read_json(receipt_key) if receipt_key else None
            if receipt and receipt.present:
                if receipt.value["amount"] != change.amount:
                    return Response.json({"error": "idempotency-key already used for a different increment"}, 409)
                sealed = receipt.value["response"]
                return Response(sealed["body"], sealed["status"], sealed["headers"])
            snapshot = await storage.read_json("count")
            count = (snapshot.value if snapshot.present else 0) + change.amount
            response = Response.json({"count": count, "alarms": await storage.get_json("alarms", 0)})
            checks, puts = {"count": snapshot}, {"count": count}
            if receipt_key:
                checks[receipt_key] = receipt
                # Persist the rendered application response, not an object
                # whose JSON key order/encoding can change on replay.
                puts[receipt_key] = {"amount": change.amount, "response": {
                    "status": response.status, "headers": response.headers,
                    "body": response.body.decode("ascii")}}
            # The receipt and its effect commit in one checked transaction. A
            # lost response can be resolved by retrying the same key and amount.
            try:
                await storage.apply(checks=checks, puts=puts)
            except Conflict:
                return Response.json({"error": "conflicting update; reconcile using the same idempotency-key"}, 409)
            return response
        return Response.json({"count": await storage.get_json("count", 0),
                              "alarms": await storage.get_json("alarms", 0)})

    async def alarm(self, scheduled_ms, ctx):
        receipt_key = "alarm:" + str(scheduled_ms)
        receipt = await ctx.storage.read_json(receipt_key)
        if receipt.present:
            return
        snapshot = await ctx.storage.read_json("alarms")
        await ctx.storage.apply(checks={receipt_key: receipt, "alarms": snapshot},
                                puts={receipt_key: True, "alarms": (snapshot.value if snapshot.present else 0) + 1})
