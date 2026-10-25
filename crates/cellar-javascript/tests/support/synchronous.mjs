// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

let saved;
export class Counter {
  async fetch(req, ctx) {
    const execute = (sql, params = []) => ctx._sync("storage.sql", { sql, params });
    saved ??= ctx;
    if (saved !== ctx) {
      try { saved._sync("storage.read", { key: "count" }); throw new Error("borrowed authority"); }
      catch (error) { if (error.code !== "ContextMismatch") throw error; }
    }
    execute("CREATE TABLE IF NOT EXISTS counters (value INTEGER NOT NULL)");
    execute("INSERT INTO counters (value) SELECT 0 WHERE NOT EXISTS (SELECT 1 FROM counters)");
    execute("UPDATE counters SET value = value + 1");
    const count = execute("SELECT value FROM counters").rows[0][0];
    ctx._sync("storage.apply", { puts: { count } });
    if (ctx._sync("storage.read", { key: "count" }).value !== count) throw new Error("wrong invocation storage");
    const now = ctx._sync("clock.now");
    if (typeof now !== "number") throw new Error("invalid native clock");
    if (ctx._sync("alarm.get") !== null) throw new Error("unexpected alarm");
    // An unresolved asynchronous mutation must fence synchronous SQL as well.
    const write = ctx.storage.apply({ puts: { other: count } });
    try { execute("UPDATE counters SET value = 999"); throw new Error("overlapping mutation"); }
    catch (error) { if (error.code !== "ConcurrentMutation" || error.outcome !== "not_committed") throw error; }
    await write;
    try { ctx._sync("cell.fetch", {}); throw new Error("synchronous external I/O"); }
    catch (error) { if (error.code !== "UnsupportedOperation" || error.outcome !== "not_committed") throw error; }
    await ctx.storage.sync();
    return { status: 200, headers: [], body: ctx._encode(String(count)) };
  }
}
export const cells = { Counter };
export async function fetch(req, ctx) {
  const result = await ctx.cell("COUNTERS", req.url.split("/").pop()).fetch({ url: req.url, method: "GET", headers: [], body: "" });
  return { status: result.status, headers: result.headers, body: ctx._encode(result.body) };
}
