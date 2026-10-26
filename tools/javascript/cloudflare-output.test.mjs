// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import test from "node:test";
import assert from "node:assert/strict";
import { encodeStreamResponse } from "../../packages/cellar-cloudflare/output.js";

function fixture(limit = 4) {
  const watches = new Set();
  const context = {
    _sync: () => ({ max_body_bytes: limit }),
    _call(name, _value, _body, signal) {
      assert.equal(name, "response.closed");
      return new Promise((resolve, reject) => {
        const finish = (error) => {
          if (!watches.delete(close)) return;
          signal.removeEventListener("abort", abort);
          if (error) reject(error);
          else resolve(null);
        };
        const close = () => finish();
        const abort = () => finish(new Error("native waiter cancelled"));
        watches.add(close);
        signal.addEventListener("abort", abort, { once: true });
        if (signal.aborted) abort();
      });
    },
  };
  return {
    context,
    watches,
    disconnect: () => {
      for (const close of watches) close();
    },
  };
}

test("module response conversion publishes metadata before pulling and bounds each chunk", async () => {
  const { context, watches } = fixture();
  let pulls = 0;
  const source = new ReadableStream(
    {
      pull(controller) {
        if (pulls++ === 3) controller.close();
        else controller.enqueue(Uint8Array.of(0, 255, 42));
      },
    },
    { highWaterMark: 0 },
  );
  const frame = encodeStreamResponse(
    new Response(source, {
      status: 201,
      headers: [
        ["set-cookie", "a=1"],
        ["set-cookie", "b=2"],
      ],
    }),
    context,
  );
  assert.equal(frame.status, 201);
  assert.equal(pulls, 0);
  assert.equal(watches.size, 0);
  const bytes = [];
  for await (const chunk of frame.chunks) bytes.push(...chunk);
  assert.deepEqual(bytes, [0, 255, 42, 0, 255, 42, 0, 255, 42]);
  assert.equal(watches.size, 0);
  assert.equal(source.locked, false);
  assert.deepEqual(frame.headers, [
    ["set-cookie", "a=1"],
    ["set-cookie", "b=2"],
  ]);
});

test("consumer disconnect wakes a pending producer and releases its reader and native waiter", async () => {
  const { context, watches, disconnect } = fixture();
  const entered = Promise.withResolvers();
  let cancelled = 0;
  const source = new ReadableStream(
    {
      pull() {
        entered.resolve();
        return new Promise(() => {});
      },
      cancel() {
        cancelled++;
      },
    },
    { highWaterMark: 0 },
  );
  const frame = encodeStreamResponse(new Response(source), context);
  const iterator = frame.chunks[Symbol.asyncIterator]();
  const pending = iterator.next();
  await entered.promise;
  assert.equal(watches.size, 1);
  disconnect();
  assert.equal((await pending).done, true);
  assert.equal(cancelled, 1);
  assert.equal(source.locked, false);
  assert.equal(watches.size, 0);
  await frame.cancel();
  assert.equal(cancelled, 1);
});

test("oversized chunks and producer failures remain errors and join cleanup", async () => {
  for (const tooLarge of [false, true]) {
    const { context, watches } = fixture();
    const reason = new Error("producer failed");
    let cancelled = 0;
    const source = new ReadableStream(
      {
        pull(controller) {
          if (tooLarge) controller.enqueue(new Uint8Array(5));
          else controller.error(reason);
        },
        cancel() {
          cancelled++;
        },
      },
      { highWaterMark: 0 },
    );
    const frame = encodeStreamResponse(new Response(source), context);
    await assert.rejects(
      frame.chunks[Symbol.asyncIterator]().next(),
      tooLarge ? RangeError : (error) => error === reason,
    );
    assert.equal(watches.size, 0);
    assert.equal(source.locked, false);
    assert.equal(cancelled, Number(tooLarge));
  }
});

test("failed head admission can cancel an unstarted iterator and empty bodies need no watcher", async () => {
  let cancelled = 0;
  const source = new ReadableStream({
    cancel() {
      cancelled++;
    },
  });
  const { context, watches } = fixture();
  const frame = encodeStreamResponse(new Response(source), context);
  await frame.cancel();
  assert.equal(source.locked, false);
  assert.equal(cancelled, 1);
  assert.equal(watches.size, 0);
  assert.deepEqual(
    encodeStreamResponse(new Response(null, { status: 204 }), context),
    { status: 204, headers: [], body: "" },
  );
});

test("native watch admission failure still cancels a pending source", async () => {
  const { context } = fixture();
  let cancelled = 0;
  context._call = () => {
    throw new Error("OperationLimit");
  };
  const source = new ReadableStream({
    cancel() {
      cancelled++;
    },
  });
  const frame = encodeStreamResponse(new Response(source), context);
  await assert.rejects(
    frame.chunks[Symbol.asyncIterator]().next(),
    /OperationLimit/,
  );
  assert.equal(cancelled, 1);
  assert.equal(source.locked, false);
});

test("a disturbed Response cannot silently become a fresh partial reply", async () => {
  const { context } = fixture();
  let cancelled = 0;
  const response = new Response(
    new ReadableStream(
      {
        pull(controller) {
          controller.enqueue(Uint8Array.of(1));
        },
        cancel() {
          cancelled++;
        },
      },
      { highWaterMark: 0 },
    ),
  );
  const reader = response.body.getReader();
  await reader.read();
  reader.releaseLock();
  assert.equal(response.bodyUsed, true);
  assert.throws(
    () => encodeStreamResponse(response, context),
    /already been used/,
  );
  assert.equal(cancelled, 1);
});
