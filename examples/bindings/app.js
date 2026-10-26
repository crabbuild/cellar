// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.
import { jsonResponse, request, response } from "@cellar/sdk";
/** Each selected cell gets its own durable count, regardless of binding alias. */
export class Counter {
  async fetch(req, ctx) {
    if (req.method !== "GET" && req.method !== "POST") {
      return jsonResponse({ error: "use GET or POST" }, { status: 405 });
    }
    const snapshot = await ctx.storage.read("count");
    let count = snapshot.present ? snapshot.value : 0;
    if (
      typeof count !== "number" ||
      !Number.isSafeInteger(count) ||
      count < 0
    ) {
      throw new Error("stored count is not a nonnegative safe integer");
    }
    if (req.method === "POST") {
      if (count === Number.MAX_SAFE_INTEGER) {
        return jsonResponse({ error: "counter limit" }, { status: 409 });
      }
      try {
        // Compare and update in one native transaction. An await between a
        // read and write must not allow concurrent callers to lose increments.
        await ctx.storage.apply({
          checks: { count: snapshot },
          puts: { count: count + 1 },
        });
      } catch (error) {
        const failure = error;
        if (
          failure.code === "Conflict" &&
          failure.outcome === "not_committed"
        ) {
          return jsonResponse(
            { error: "concurrent update; increment not applied" },
            { status: 409 },
          );
        }
        // A lost response may follow a committed write. Do not retry it here.
        throw error;
      }
      await ctx.storage.sync();
      count += 1;
    }
    if (!ctx.identity) {
      throw new Error("counter requires a cell identity");
    }
    return jsonResponse({
      class: ctx.identity.class,
      name: ctx.identity.name,
      scope: ctx.identity.scope,
      count,
    });
  }
}
// The implementation is shared; the configured class gives it separate state.
export class OtherCounter extends Counter {}
export const cells = { Counter, OtherCounter };
/** Resolve an application-approved binding name and forward to its named cell. */
export async function fetch(req, ctx) {
  if (req.method !== "GET" && req.method !== "POST") {
    return jsonResponse({ error: "use GET or POST" }, { status: 405 });
  }
  const path = req.url.split("?")[0].replace(/^https?:\/\/[^/]+/, "");
  const route = /^\/bindings\/([^/]+)\/([^/]*)$/.exec(path);
  if (!route) {
    return jsonResponse(
      { error: "use /bindings/COUNTERS/demo" },
      { status: 404 },
    );
  }
  const [, binding, name] = route;
  if (!["COUNTERS", "COUNTER_ALIAS", "OTHER_COUNTERS"].includes(binding)) {
    return jsonResponse({ error: "unknown binding" }, { status: 404 });
  }
  if (!/^[A-Za-z0-9_-]{1,64}$/.test(name)) {
    return jsonResponse({ error: "invalid cell name" }, { status: 400 });
  }
  const cell = ctx.cell(binding, name);
  const result = await cell.fetch(
    request(req.url, {
      method: req.method,
      headers: req.headers,
      body: req.body,
    }),
  );
  // Nested fetch returns decoded bytes; encode them as a handler response.
  return response(result.body, {
    status: result.status,
    headers: result.headers,
  });
}
