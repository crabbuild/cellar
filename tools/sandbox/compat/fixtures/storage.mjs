// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { DurableObject, RpcTarget } from "cloudflare:workers";
import { activateDurableObject } from "../../../../packages/cellar-cloudflare/index.js";
import { application, check, refuses } from "./adapter.mjs";

class Probe extends DurableObject {
  initialized = false;
  constructor(ctx, env) {
    super(ctx, env);
    ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS counts (id INTEGER PRIMARY KEY, value INTEGER); INSERT OR IGNORE INTO counts VALUES(1,0)").toArray();
    ctx.blockConcurrencyWhile(async () => {
      await ctx.storage.get("initialized");
      this.initialized = true;
    });
  }
  async inspect(command, nativeContext) {
    this.current = nativeContext;
    check(this.initialized, "constructor admission did not finish");
    const { storage } = this.ctx;
    const { sql, kv } = storage;
    switch (command) {
      case "identity": return { ok: true, id: this.ctx.id.toString() };
      case "crypto": {
        const bytes = new Uint8Array(32);
        check(crypto.getRandomValues(bytes) === bytes && bytes.some(n => n !== 0), "native random values");
        check(/^[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/.test(crypto.randomUUID()), "native UUID");
        const tooLarge = new Uint8Array(65537);
        check((await refuses(() => crypto.getRandomValues(tooLarge))).name === "QuotaExceededError" && tooLarge.every(n => n === 0), "random quota before effects");
        check((await refuses(() => crypto.getRandomValues(new Float32Array(1)))).name === "TypeMismatchError", "integer typed arrays only");
        const spoofed = new Float32Array(1);
        Object.defineProperty(spoofed, Symbol.toStringTag, { value: "Uint8Array" });
        check((await refuses(() => crypto.getRandomValues(spoofed))).name === "TypeMismatchError", "typed array brand was spoofed");
        return { ok: true };
      }
      case "failure": {
        let invoked = 0;
        const native = this.ctx.storage; // Keep the real owning native facade in the callback.
        const current = this.current;
        const fail = class extends DurableObject {
          constructor(ctx, env) { super(ctx, env); ctx.blockConcurrencyWhile(async () => { await ctx.storage.get("original"); throw undefined; }); }
        };
        const activation = activateDurableObject(fail, current);
        for (let i=0; i<2; i++) await refuses(() => activation.invoke(() => { invoked++; }));
        check(invoked === 0, "failed initialization admitted a handler");
        await native.get("original");
        return { ok: true };
      }
      case "tick": {
        const row = sql.exec("UPDATE counts SET value=value+1 WHERE id=1 RETURNING value").one();
        await storage.put("count", row.value);
        check(kv.get("count") === row.value, "sync/async KV mismatch");
        check(/^[a-f0-9]{64}$/.test(this.ctx.id.toString()), "native object ID");
        await storage.sync();
        return { count: row.value };
      }
      case "read-types": {
        const result = kv.get("types");
        check(result.self === result && result.binary[1] === 255 && result.bigint === 2n ** 100n, "persisted structured graph");
        return { ok: true };
      }
      case "types": {
        const buffer = new Uint8Array([0,255,42]).buffer;
        const value = { undefined: undefined, bigint: 2n ** 100n, date: new Date(123456789),
          map: new Map([["key", new Set([1,2])]]), binary: new Uint8Array(buffer), view: new DataView(buffer),
          regexp: /cellar/gi, error: new Error("stored"), lone: "\ud800", nan: NaN, infinity: Infinity,
          holes: [,1], boxed: new Number(7) };
        value.self = value;
        await storage.put("types", value);
        const result = kv.get("types");
        check(result.self === result && result.bigint === value.bigint, "cycles/bigint");
        check(result.date.getTime() === 123456789 && result.map.get("key").has(2), "date/map/set");
        check(result.binary[1] === 255 && result.binary.buffer === result.view.buffer, "shared binary backing store");
        check(result.regexp.source === "cellar" && result.regexp.flags === "gi" && result.error.message === "stored", "regexp/error");
        check(result.lone === "\ud800" && Number.isNaN(result.nan) && result.infinity === Infinity, "UTF-16/nonfinite values");
        check(!(0 in result.holes) && result.holes[1] === 1 && result.boxed.valueOf() === 7 && "undefined" in result, "sparse/boxed/undefined");
        await storage.put({ "list/a": "first", "list/b": null, "list/c": undefined });
        check((await storage.get(["list/a", "missing", "list/b"])).size === 2, "multi-get missing/null");
        const list = await storage.list({ prefix: "list/", startAfter: "list/a", limit: 2, reverse: true });
        check([...list.keys()].join(",") === "list/c,list/b", "ordered KV list");
        check(await storage.delete(["list/a", "list/b", "missing"]) === 2, "delete count");
        check(kv.delete("list/c") && !kv.delete("list/c"), "delete boolean");
        await storage.setAlarm(new Date(Date.now() + 60000));
        check(await storage.getAlarm() > Date.now(), "alarm persistence");
        await storage.deleteAlarm();
        check(await storage.getAlarm() === null, "alarm deletion");
        return { ok: true };
      }
      case "sql": {
        const cursor = sql.exec("SELECT 1 AS value UNION ALL SELECT 2 UNION ALL SELECT 3");
        check(cursor.raw().next().value[0] === 1 && cursor.toArray().map(x => x.value).join(",") === "2,3", "shared cursor consumption");
        check(cursor.columnNames[0] === "value" && cursor.rowsRead === 3, "cursor metadata");
        check(sql.exec("SELECT ? AS bytes", new Uint8Array([0,255])).one().bytes instanceof ArrayBuffer, "SQL blob type");
        check(new Uint8Array(sql.exec("SELECT ? AS bytes", new Uint8Array([0,255])).one().bytes)[1] === 255, "SQL blob bytes");
        check(sql.exec("SELECT 9223372036854775807 AS value").one().value === Number(9223372036854775807n), "explicit SQL number conversion");
        check(sql.exec("SELECT 9e999 AS value").one().value === Infinity, "SQL infinity");
        check(sql.databaseSize > 0, "database size");
        await refuses(() => sql.exec("SELECT 1 WHERE 0").one());
        await refuses(() => sql.exec("SELECT 1 UNION ALL SELECT 2").one());
        await refuses(() => sql.exec("SELECT * FROM __sys_cell_kv").toArray());
        await refuses(() => sql.exec("BEGIN").toArray());
        return { ok: true };
      }
      case "save": this.cursor = sql.exec("SELECT 1"); return { ok: true };
      case "borrow": await refuses(() => this.cursor.next(), "ContextMismatch"); return { ok: true };
      case "buffers": {
        const first = sql.exec("SELECT zeroblob(600000) AS data");
        const error = await refuses(() => sql.exec("SELECT zeroblob(600000) AS data"), "Ambiguous");
        check(error.outcome === "unknown", "post-execution quota invented rollback");
        first.return();
        const second = sql.exec("SELECT zeroblob(600000) AS data");
        check(second.one().data.byteLength === 600000, "cursor cleanup did not release buffer quota");
        return { ok: true };
      }
      case "bounds": {
        for (let i=0; i<160; i++) { for (const _row of sql.exec("SELECT 1")) break; }
        const opened = [];
        try { for (let i=0; i<128; i++) opened.push(sql.exec("SELECT 1"));
          const error = await refuses(() => sql.exec("INSERT INTO counts VALUES(2,999)"), "ValidationError");
          check(error.outcome === "not_committed", "cursor admission provenance");
        } finally { for (const cursor of opened) cursor.return(); }
        check(sql.exec("SELECT count(*) AS n FROM counts WHERE id=2").one().n === 0, "refused statement had effects");
        return { ok: true };
      }
      case "refuse": {
        await storage.put("original", 1);
        await refuses(() => storage.put({ original: 2, bad: () => {} }));
        await refuses(() => storage.put({ original: 3, bad: { get target() { return new RpcTarget(); } } }));
        check(kv.get("original") === 1, "batch validation partially wrote");
        check([...kv.list().keys()].every(key => !key.startsWith("__sys_")), "private key leaked through list");
        await storage.put({ "0-before": 0, "z-after": 1 });
        check([...kv.list({ limit: 1 }).keys()][0] === "0-before", "private key charged against ascending limit");
        check([...kv.list({ reverse: true, limit: 1 }).keys()][0] === "z-after", "private key charged against descending limit");
        await refuses(() => storage.put("__sys_private", 1), "ValidationError");
        await refuses(() => storage.list({ limit: 129 }), "ValidationError");
        await refuses(() => storage.transaction(() => storage.put("original", 4)));
        check(kv.get("original") === 1, "unsupported transaction ran callback");
        return { ok: true };
      }
      default: throw new Error("Unknown fixture operation");
    }
  }
}
const app = application(Probe);
export const { cells, fetch } = app;
