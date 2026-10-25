// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { DurableObject } from "cloudflare:workers";
import { activateDurableObject, durableObjectNamespace } from "../../../../packages/cellar-cloudflare/index.js";
import { check, refuses } from "./adapter.mjs";
const gates = new Map();
let saved;
let deepFailure;
function hold(key) {
  check(!gates.has(key), `duplicate gate ${key}`);
  let release;
  const promise = new Promise(done => { release = done; });
  gates.set(key, release);
  return promise;
}
class Probe extends DurableObject {
  async inspect(command, native) {
    const key = this.ctx.id.name;
    if (command === "wait") {
      saved = native;
      this.ctx.waitUntil(hold(key).then(() => {
        this.ctx.storage.kv.put("wait", 1n);
        // Registration by a completing job must join the same native lifetime.
        this.ctx.waitUntil(Promise.resolve().then(() => this.ctx.storage.kv.put("child", 2n)));
      }));
    } else if (command === "foreign") {
      await refuses(() => saved._waitUntil(Promise.resolve()), "ContextMismatch");
    } else if (command === "transaction") {
      const gate = hold(key);
      // Deliberately drop the result. The native savepoint and its JS callback
      // must remain connected until completion, with no root-scope fallback.
      this.ctx.storage.transaction(async tx => {
        await gate;
        await tx.put("transaction", 3n);
      });
    } else if (command === "rollback") {
      const gate = hold(key);
      this.ctx.storage.transaction(async tx => {
        await gate;
        await tx.put("rollback", 4n);
        throw undefined;
      });
    } else if (command === "reject") {
      this.ctx.waitUntil(hold(key).then(() => { throw new Error("background failure"); }));
    } else if (command === "quota") {
      const gate = hold(key);
      for (let i = 0; i < 128; i++) this.ctx.waitUntil(gate.then(() => {}));
      let refused = false;
      try { this.ctx.waitUntil(gate); } catch (error) { refused = error.name === "QuotaExceededError"; }
      check(refused, "waitUntil quota");
    } else if (command === "shutdown") {
      this.ctx.waitUntil(durableObjectNamespace(native, "COUNTERS").getByName("gate").nativeGate()
        .then(() => this.ctx.storage.kv.put("shutdown", 8n)));
    } else if (command === "disconnect-before-head") {
      this.ctx.waitUntil(hold(`${key}-before-background`).then(() => this.ctx.storage.kv.put("before", 7n)));
      await hold(`${key}-before-response`);
    } else if (command === "stream") {
      this.ctx.waitUntil(hold(`${key}-background`).then(() => this.ctx.storage.kv.put("disconnected", 5n)));
      const gate = hold(`${key}-stream`);
      return { chunks: (async function* () { yield "head"; await gate; yield "tail"; })() };
    } else if (command === "deep") {
      // The I/O's derived continuation runs after several promise checkpoints.
      // It intentionally has no explicit waitUntil wrapper.
      native.now().then(async () => {
        for (let i = 0; i < 8; i++) await Promise.resolve();
        const response = await native.cell("COUNTERS", "deep-target").fetch({ url: "http://cell/deep-write", method: "GET", headers: [], body: "" });
        check(response.status === 200, "deep native continuation");
      }).catch(error => { deepFailure = `${error.code}: ${error.message}`; });
    } else if (command === "deep-write") {
      this.ctx.storage.kv.put("deep", 10n);
    } else if (command === "oversized-write") {
      this.ctx.storage.kv.put("oversized", 9n);
    } else if (command === "read") {
      return { ...Object.fromEntries([...this.ctx.storage.kv.list()].map(([key, value]) => [key, String(value)])), deepFailure };
    }
    return { ok: true };
  }
  beginRpc() {
    this.ctx.waitUntil(hold("rpc").then(() => this.ctx.storage.kv.put("rpc", 6n)));
    return 101n;
  }
}
export const cells = { Counter: class {
  initialize(native) { this.activation ??= activateDurableObject(Probe, native); }
  async fetch(request, native) {
    this.initialize(native);
    const result = await this.activation.invoke(target => target.inspect(request.url.split("/").pop(), native));
    return result.chunks ? { status: 200, headers: [], chunks: result.chunks } : { status: 200, headers: [], body: native._encode(JSON.stringify(result)) };
  }
  async cell_rpc(event, native) { this.initialize(native); return this.activation.rpc(event.method, event.args); }
  async alarm(_event, native) { await native.storage.apply({ delete_alarm: true }); }
} };
export async function fetch(request, native) {
  const [name, command] = request.url.split("/").slice(3);
  if (name === "oversized") {
    native._waitUntil(durableObjectNamespace(native, "COUNTERS").getByName("gate").nativeGate().then(async () => {
      const response = await native.cell("COUNTERS", "alice").fetch({ url: "http://cell/oversized-write", method: "GET", headers: [], body: "" });
      check(response.status === 200, "background write after refused result");
    }));
    return { status: 200, headers: [], body: native._encode("x".repeat(32 * 1024)) };
  }
  if (name === "budget") {
    if (command === "total") {
      for (let i = 0; i < 3; i++) await native.now();
      await refuses(() => native.now(), "OperationLimit");
    } else {
      // ID derivation, binding validation, then one held native RPC exhaust the
      // exact three-operation budget and the only ordinary pending slot.
      native._waitUntil(durableObjectNamespace(native, "COUNTERS").getByName("gate").nativeGate());
      await refuses(() => native.now(), "OperationLimit");
    }
    return { status: 200, headers: [], body: native._encode('{"ok":true}') };
  }
  if (name === "release") {
    const release = gates.get(command);
    check(release, `missing gate ${command}`);
    gates.delete(command);
    release();
    return { status: 200, headers: [], body: native._encode('{"ok":true}') };
  }
  if (name === "pending") {
    return { status: 200, headers: [], body: native._encode(JSON.stringify({ pending: gates.has(command) })) };
  }
  if (name === "rpc") {
    const result = await durableObjectNamespace(native, "COUNTERS").getByName(command).beginRpc();
    return { status: 200, headers: [], body: native._encode(JSON.stringify({ value: String(result) })) };
  }
  const response = await native.cell("COUNTERS", name).fetchStream({ url: `http://cell/${command}`, method: "GET", headers: [], body: "" });
  return { status: response.status, headers: response.headers, chunks: response.chunks() };
}
