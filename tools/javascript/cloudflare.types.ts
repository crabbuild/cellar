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
  download(): Response {
    return new Response(this.output());
  }
  sink(): WritableStream<Uint8Array> {
    return new WritableStream<Uint8Array>();
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
const downloaded = stub.download().then(async (response) => {
  const status: number = response.status;
  const bytes: ArrayBuffer = await response.arrayBuffer();
  response[Symbol.dispose]();
  return [status, bytes];
});
void downloaded;
const consumed = stub.nestedOutput().then(({ body }) => {
  const bytes: Promise<ReadableStreamReadResult<Uint8Array>> = body
    .getReader()
    .read();
  body[Symbol.dispose]();
  return bytes;
});
const written = stub.sink().then(async (sink) => {
  const writer = sink.getWriter();
  await writer.write(new Uint8Array([0, 255]));
  // @ts-expect-error: byte writers refuse text chunks
  writer.write("text");
  sink[Symbol.dispose]();
});
void [streamed, consumed, written];
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

// Optional Node/DOM Workers globals, installed by the admitted runtime profile.
import type {} from "../../packages/cellar-cloudflare/globals.js";
import type {
  DurableObjectContainer,
  WorkerWebSocket,
} from "../../packages/cellar-cloudflare/index.js";
async function socketTypes(portContainer: DurableObjectContainer) {
  const response = await portContainer
    .getTcpPort(3000)
    .fetch("http://container/rpc", { headers: { Upgrade: "websocket" } });
  const socket: WorkerWebSocket | null = response.webSocket;
  if (socket) {
    socket.accept();
    socket.send(Uint8Array.of(0, 255));
    socket.close(1011, "failure");
  }
  const pair = new WebSocketPair();
  pair[1].accept();
  pair[1].addEventListener("message", (event) => {
    const data: string | ArrayBuffer | Blob = event.data;
    void data;
  });
  return new Response(null, { status: 101, webSocket: pair[0] });
}
void socketTypes;

interface HttpRpcMethods {
  exchange(request: Request, response: Response): Promise<Request>;
}
async function httpMessageTypes(
  httpRpc: import("../../packages/cellar-cloudflare/index.js").DurableObjectStub<HttpRpcMethods>,
) {
  const result = await httpRpc.exchange(
    new Request("https://example.test/input"),
    new Response("input"),
  );
  const method: string = result.method;
  result.signal.throwIfAborted();
  const body: ArrayBuffer = await result.arrayBuffer();
  result[Symbol.dispose]();
  return { method, body };
}
void httpMessageTypes;

const equalPreviewTokens: boolean = crypto.subtle.timingSafeEqual(
  new Uint8Array([1]),
  new Uint8Array([1]),
);
void equalPreviewTokens;

async function processTypes(container: DurableObjectContainer) {
  const process = await container.exec(["tool", "literal; argument"], {
    stdin: "pipe",
    stderr: "combined",
    env: { HELLO: "world" },
  });
  await process.stdin?.getWriter().close();
  const output = await process.output();
  const bytes: ArrayBuffer = output.stdout;
  const code: number = await process.exitCode;
  await container.setInactivityTimeout(30_000);
  const input = new ReadableStream<Uint8Array>();
  const streamed = await container.exec(["tool"], { stdin: input });
  await streamed.output();
  // @ts-expect-error: process stdin accepts byte streams, not text streams
  await container.exec(["tool"], { stdin: new ReadableStream<string>() });
  const terminal = await container.exec(["tool"], { pty: true });
  const pid: number | undefined = terminal.pid;
  const isPty: boolean = terminal.isPty;
  const resize: void = terminal.resize(132, 40);
  void [pid, isPty, resize];
  // @ts-expect-error: terminal dimensions are numeric
  await container.exec(["tool"], { pty: { cols: "wide" } });
  return { bytes, code };
}
void processTypes;
