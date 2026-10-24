# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Binding-specific authority and retry checks over real native backends."""
_base_fetch = fetch
binding_handles = None
binding_holding = asyncio.Event()
binding_release = asyncio.Event()

async def fetch(request, ctx):
    global binding_handles
    cache, database, producer = ctx.kv("CACHE"), ctx.sql("DB"), ctx.queue("JOBS")
    if request.path == "/bindings/setup":
        marker = await ctx.var("DOMAIN_MARKER")
        await cache.put("authority", marker)
        await database.query("CREATE TABLE IF NOT EXISTS authority (marker TEXT)")
        await database.query("DELETE FROM authority")
        await database.query("INSERT INTO authority VALUES (?)", [marker])
        return Response.json(marker)
    if request.path == "/bindings/hold":
        binding_handles = (cache, database, producer)
        binding_holding.set()
        await binding_release.wait()
        return Response.json(True)
    if request.path == "/bindings/holding":
        return Response.json(binding_holding.is_set())
    if request.path == "/bindings/release":
        binding_release.set()
        return Response.json(True)
    if request.path == "/bindings/cross":
        old_cache, old_database, old_producer = binding_handles
        assert old_cache._context._bridge.active()
        calls = [lambda: old_cache.get("authority"),
                 lambda: old_cache.put("forbidden", True),
                 lambda: old_database.query("INSERT INTO authority VALUES ('forbidden')"),
                 lambda: old_producer.send({"forbidden": True})]
        for call in calls:
            try:
                await call()
                raise AssertionError("retained binding crossed its live invocation/domain")
            except SDKError as error:
                assert error.code == "ContextMismatch" and error.outcome == "not_committed"
        return Response.json(True)
    if request.path == "/bindings/state":
        return Response.json({"authority": await cache.get("authority"),
                              "forbidden": await cache.get("forbidden"),
                              "sql": (await database.query("SELECT marker FROM authority"))["results"]})
    if request.path == "/bindings/expired":
        old_cache, old_database, old_producer = binding_handles
        for call in [lambda: old_cache.put("forbidden", True),
                     lambda: old_database.query("INSERT INTO authority VALUES ('forbidden')"),
                     lambda: old_producer.send({"forbidden": True})]:
            try:
                await call()
                raise AssertionError("expired binding accepted new work")
            except SDKError as error:
                assert error.code == "ContextExpired" and error.outcome == "not_committed"
        return Response.json(True)
    return await _base_fetch(request, ctx)
