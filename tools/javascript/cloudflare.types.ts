// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import {
  activateDurableObject,
  durableObjectNamespace,
  durableObjectContainer,
  workersApplication,
  type DurableObjectStorage,
} from "../../packages/cellar-cloudflare/index.js";
import { DurableObject } from "../../packages/cellar-cloudflare/workers.js";
import type { Cell, Context } from "../../packages/cellar-sdk/index.js";
class Counter extends DurableObject {
  increment(by: bigint): bigint {
    return by + 1n;
  }
  background(): void {
    this.ctx.waitUntil(Promise.resolve(3));
  }
  output(): ReadableStream<Uint8Array> {
    return new ReadableStream<Uint8Array>({ type: "bytes" });
  }
  nestedOutput() {
    return { body: this.output() };
  }
  tools() {
    return { add: (by: bigint) => by + 1n };
  }
}
declare const context: Context;
const container = durableObjectContainer(context);
const running: boolean = container.running;
void running;
const queued: void = container.start({
  env: { MODE: "worker" },
  enableInternet: false,
});
const monitored: Promise<void> = container.monitor();
const stopped: Promise<void> = container.destroy(new Error("reason"));
container.signal(15);
void queued;
void monitored;
void stopped;
// @ts-expect-error: image selection requires a separate versioned profile
container.start({ image: "unconfigured" });
// @ts-expect-error: Workers running is a property, not a Promise or method
container.running();
// @ts-expect-error: Native state is read-only
container.running = false;
const namespace = durableObjectNamespace<Counter>(context, "COUNTERS");
const stub = namespace.getByName("alice");
const result: Promise<bigint> = stub.increment(3n);
const tools = stub.tools().then((tools) => {
  const value: Promise<bigint> = tools.add(1n);
  const duplicate = tools.add.dup();
  const other: Promise<bigint> = duplicate(2n);
  // @ts-expect-error: delegated calls return promises
  const immediate: bigint = tools.add(1n);
  // @ts-expect-error: delegated argument types remain intact
  tools.add("1");
  tools[Symbol.dispose]();
  duplicate[Symbol.dispose]();
  return [value, other, immediate];
});
void tools;
const streamed: Promise<ReadableStream<Uint8Array>> = stub.output();
const consumed = stub.nestedOutput().then(({ body }) => {
  const bytes: Promise<ReadableStreamReadResult<Uint8Array>> = body
    .getReader()
    .read();
  body[Symbol.dispose]();
  return bytes;
});
void [streamed, consumed];
// @ts-expect-error: instance fields are not RPC methods
stub.env();
// @ts-expect-error: argument types remain intact
stub.increment("3");
const fetched: Promise<Response> = stub.fetch("http://example.com");
void fetched;
const module = { default: { fetch: async () => new Response("ok") }, Counter };
workersApplication(module, { namespaces: { COUNTERS: "Counter" } });
// @ts-expect-error: namespace classes must be exports of the supplied module
workersApplication(module, { namespaces: { COUNTERS: "Missing" } });
const activation = activateDurableObject(Counter, context);
const local: Promise<bigint> = activation.invoke((target) =>
  target.increment(1n),
);
const cell: Cell = {
  cell_rpc: (event, context) => activation.rpc(event.method, event.args),
};
declare const storage: DurableObjectStorage;
const count: Promise<number | undefined> = storage.get<number>("count");
const counters: Map<string, number> = storage.kv.get<number>(["count"]);
const row: { value: number } = storage.sql
  .exec<{ value: number }>("SELECT 1 AS value")
  .one();
const transaction: Promise<bigint> = storage.transaction(async (tx) => {
  await tx.put("test", 3n);
  tx.rollback();
  return 4n;
});
const synchronous: bigint = storage.transactionSync(() => 9n);
// @ts-expect-error: synchronous transactions cannot return a Promise
storage.transactionSync(async () => 1);
void [result, local, cell, count, counters, row, transaction, synchronous];
