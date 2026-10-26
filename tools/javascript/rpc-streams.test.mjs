// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

const source = await readFile(
  new URL(
    "../../crates/cellar-javascript/runtime/rpc_streams.js",
    import.meta.url,
  ),
  "utf8",
);
const make = (0, eval)(`(${source})`);
const context = { _sync: () => ({ max_body_bytes: 256 }) };

test("RPC bytes transfer ownership, pull on demand and support bounded BYOB reads", async () => {
  const codec = make();
  let pulls = 0,
    releases = 0;
  const original = new ReadableStream({
    type: "bytes",
    pull(controller) {
      if (pulls++ === 0)
        controller.enqueue(new Uint8Array(768).map((_, i) => i % 256));
      else {
        const pending = controller.byobRequest;
        controller.close();
        pending?.respond(0);
      }
    },
  });
  const sent = codec.send(original, context);
  const sizes = [];
  const received = codec.receive(
    async (...args) => {
      const chunk = await sent.invoke(...args);
      if (chunk?.value) sizes.push(chunk.value.byteLength);
      return chunk;
    },
    async () => {
      releases++;
    },
  );
  assert.equal(original.locked, true);
  assert.equal(pulls, 0);
  const reader = received.stream.getReader({ mode: "byob" });
  const output = [];
  for (;;) {
    const chunk = await reader.read(new Uint8Array(77));
    if (chunk.done) break;
    output.push(...chunk.value);
  }
  assert.deepEqual(
    output,
    Array.from({ length: 768 }, (_, i) => i % 256),
  );
  assert.ok(sizes.every((size) => size <= 128));
  assert.equal(releases, 1);
  assert.equal(original.locked, true);
  await sent.close();
});

test("cancelling a pending RPC read joins the source and releases once", async () => {
  const codec = make();
  const entered = Promise.withResolvers();
  let cancelled,
    releases = 0;
  const original = new ReadableStream({
    type: "bytes",
    pull() {
      entered.resolve();
    },
    cancel(reason) {
      cancelled = reason;
    },
  });
  const sent = codec.send(original, context);
  const received = codec.receive(sent.invoke, async () => {
    releases++;
  });
  const reader = received.stream.getReader();
  const pending = reader.read();
  await entered.promise;
  await reader.cancel("consumer left");
  assert.equal(cancelled, "consumer left");
  assert.equal((await pending).done, true);
  await received.dispose();
  assert.equal(releases, 1);
});

test("source errors and failed native EOF release remain reader errors", async () => {
  const codec = make();
  for (const failedRelease of [false, true]) {
    let releases = 0;
    const original = new ReadableStream({
      type: "bytes",
      pull(controller) {
        if (!failedRelease) throw new Error("producer error");
        const pending = controller.byobRequest;
        controller.close();
        pending?.respond(0);
      },
    });
    const sent = codec.send(original, context);
    const received = codec.receive(sent.invoke, async () => {
      releases++;
      if (failedRelease) throw new Error("native release failed");
    });
    const reader = received.stream.getReader();
    await assert.rejects(
      reader.read(),
      failedRelease ? /native release/ : /producer error/,
    );
    await new Promise(setImmediate);
    assert.ok(releases >= 1);
  }
});

test("refused exports restore locks and object streams refuse before pulling", async () => {
  const codec = make();
  const original = new ReadableStream({ type: "bytes" });
  const sent = codec.send(original, context);
  sent.restore();
  assert.equal(original.locked, false);
  assert.throws(
    () => codec.send(new ReadableStream(), context),
    /byte stream/i,
  );
  assert.throws(
    () => codec.send(original, { _sync: () => ({ max_body_bytes: 128 }) }),
    /budget/,
  );
  assert.equal(original.locked, false);
  await original.cancel();
});

test("disposing a locked imported stream wakes its reader and joins cancellation", async () => {
  const codec = make();
  const entered = Promise.withResolvers();
  let cancelled = 0,
    releases = 0;
  const sent = codec.send(
    new ReadableStream({
      type: "bytes",
      pull() {
        entered.resolve();
      },
      cancel() {
        cancelled++;
      },
    }),
    context,
  );
  const received = codec.receive(sent.invoke, async () => {
    releases++;
  });
  const reader = received.stream.getReader();
  const pending = reader.read();
  await entered.promise;
  await received.dispose();
  await assert.rejects(pending, /disposed/);
  assert.equal(cancelled, 1);
  assert.equal(releases, 1);
});

test("an untransferable cancellation reason still cancels and joins a pending source", async () => {
  const codec = make();
  const entered = Promise.withResolvers();
  let cancelled = false,
    releases = 0;
  const sent = codec.send(
    new ReadableStream({
      type: "bytes",
      pull() {
        entered.resolve();
      },
      cancel(reason) {
        assert.equal(reason, undefined);
        cancelled = true;
      },
    }),
    context,
  );
  const received = codec.receive(
    sent.invoke,
    async () => {
      releases++;
    },
    () => {
      throw new TypeError("reason cannot be cloned");
    },
  );
  const reader = received.stream.getReader();
  const pending = reader.read();
  await entered.promise;
  await assert.rejects(
    reader.cancel(() => {}),
    /cannot be cloned/,
  );
  assert.equal((await pending).done, true);
  assert.equal(cancelled, true);
  assert.equal(releases, 1);
});

test("RPC pulls respect the smaller producer and consumer byte budgets", async () => {
  const codec = make();
  for (const [producer, consumer] of [
    [256, 1024],
    [1024, 256],
  ]) {
    let sentChunk = false;
    const source = new ReadableStream({
      type: "bytes",
      pull(controller) {
        if (!sentChunk) {
          sentChunk = true;
          controller.enqueue(new Uint8Array(2048));
        } else {
          const pending = controller.byobRequest;
          controller.close();
          pending?.respond(0);
        }
      },
    });
    const sent = codec.send(source, {
      _sync: () => ({ max_body_bytes: producer }),
    });
    const sizes = [];
    const received = codec.receive(
      async (...args) => {
        const chunk = await sent.invoke(...args);
        if (chunk.value) sizes.push(chunk.value.byteLength);
        return chunk;
      },
      async () => {},
      undefined,
      Math.min(producer, consumer),
    );
    assert.equal(
      (await new Response(received.stream).arrayBuffer()).byteLength,
      2048,
    );
    assert.ok(
      sizes.every((size) => size <= Math.min(producer, consumer) - 128),
    );
    await sent.close();
  }
});
