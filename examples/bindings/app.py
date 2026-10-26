# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Configured aliases route to named cells with independent durable counters."""

import re

from cellar import Conflict, Response


MAX_COUNT = 9_007_199_254_740_991


class Counter:
    async def fetch(self, request, ctx):
        if request.method not in ("GET", "POST"):
            return Response.json({"error": "use GET or POST"}, 405)

        snapshot = await ctx.storage.read_json("count")
        count = snapshot.value if snapshot.present else 0

        if type(count) is not int or not 0 <= count <= MAX_COUNT:
            raise ValueError("stored count is not a nonnegative safe integer")

        if request.method == "POST":
            if count == MAX_COUNT:
                return Response.json({"error": "counter limit"}, 409)

            try:
                # The native transaction checks the snapshot before changing it.
                # Concurrent calls through either alias share this same check.
                await ctx.storage.apply(
                    checks={"count": snapshot},
                    puts={"count": count + 1},
                )
            except Conflict as error:
                if error.outcome != "not_committed":
                    raise
                return Response.json(
                    {"error": "concurrent update; increment not applied"}, 409,
                )

            # Unknown failures escape; retrying an ambiguous mutation is unsafe.
            await ctx.storage.sync()
            count += 1

        if ctx.identity is None:
            raise RuntimeError("counter requires a cell identity")

        return Response.json({
            "class": ctx.identity.class_name,
            "name": ctx.identity.name,
            "scope": ctx.identity.scope,
            "count": count,
        })


class OtherCounter(Counter):
    """Reuse the behavior under a separately configured class namespace."""


async def fetch(request, ctx):
    if request.method not in ("GET", "POST"):
        return Response.json({"error": "use GET or POST"}, 405)

    route = re.fullmatch(r"/bindings/([^/]+)/([^/]*)", request.path)

    if route is None:
        return Response.json({"error": "use /bindings/COUNTERS/demo"}, 404)

    binding, name = route.groups()

    # Public route selection is explicit; native configuration supplies targets.
    if binding not in ("COUNTERS", "COUNTER_ALIAS", "OTHER_COUNTERS"):
        return Response.json({"error": "unknown binding"}, 404)

    if re.fullmatch(r"[A-Za-z0-9_-]{1,64}", name) is None:
        return Response.json({"error": "invalid cell name"}, 400)

    namespace = ctx.namespace(binding)
    cell = namespace.get_by_name(name)
    return await cell.fetch(request)
