// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import test from "node:test";
import assert from "node:assert/strict";
import {
  decodeRequest,
  decodeResponse,
  encodeResponse,
  headerPairs,
} from "../../packages/cellar-cloudflare/http.js";
import { workersApplication } from "../../packages/cellar-cloudflare/index.js";
import { IdentityTransformStream } from "../../packages/cellar-cloudflare/streams.js";

const context = {
  _sync: () => ({ max_body_bytes: 4 }),
  _encode: (bytes) => Buffer.from(bytes).toString("base64"),
  _decode: (bytes) => Uint8Array.from(Buffer.from(bytes, "base64")),
};

test("module HTTP codecs retain binary bodies and separate Set-Cookie fields", async () => {
  const request = decodeRequest({
    url: "http://cell/",
    method: "POST",
    headers: [],
    body: Uint8Array.of(0, 255),
  });
  assert.deepEqual(
    new Uint8Array(await request.arrayBuffer()),
    Uint8Array.of(0, 255),
  );
  const original = new Response(Uint8Array.of(0, 255), {
    headers: [
      ["set-cookie", "a=1"],
      ["set-cookie", "b=2"],
    ],
  });
  const frame = await encodeResponse(original, context);
  assert.deepEqual(
    frame.headers.filter(([name]) => name === "set-cookie"),
    [
      ["set-cookie", "a=1"],
      ["set-cookie", "b=2"],
    ],
  );
  const response = decodeResponse(frame, context);
  assert.equal(response.webSocket, null);
  assert.deepEqual(response.headers.getSetCookie(), ["a=1", "b=2"]);
  assert.deepEqual(
    new Uint8Array(await response.arrayBuffer()),
    Uint8Array.of(0, 255),
  );
});

test("oversized module replies cancel a still-open producer", async () => {
  let cancelled = false;
  const response = new Response(
    new ReadableStream({
      start(controller) {
        controller.enqueue(new Uint8Array(5));
      },
      cancel() {
        cancelled = true;
      },
    }),
  );
  await assert.rejects(encodeResponse(response, context), RangeError);
  assert.equal(cancelled, true);
});

test("HTTP header bounds use UTF-8 bytes and count distinct fields", () => {
  assert.throws(
    () => headerPairs(new Headers({ large: "é".repeat(16384) })),
    RangeError,
  );
  assert.throws(
    () =>
      headerPairs(
        new Headers(
          Array.from({ length: 129 }, (_, n) => [`header-${n}`, "v"]),
        ),
      ),
    RangeError,
  );
});

test("module registration refuses missing classes and undeclared container owners", () => {
  const module = { default: { fetch: () => new Response() } };
  assert.throws(
    () => workersApplication(module, { namespaces: { CELLS: "Missing" } }),
    /Missing Durable Object export/,
  );
  assert.throws(
    () => workersApplication(module, { containers: ["Missing"] }),
    /no declared namespace/,
  );
});

test("identity byte streams apply backpressure, preserve buffers and support BYOB EOF", async () => {
  const stream = new IdentityTransformStream();
  const writer = stream.writable.getWriter();
  const reader = stream.readable.getReader({ mode: "byob" });
  const bytes = Uint8Array.of(0, 255, 42);
  let finished = false;
  const writing = writer.write(bytes).then(() => {
    finished = true;
  });
  await Promise.resolve();
  assert.equal(finished, false);
  const chunk = await reader.read(new Uint8Array(8));
  await writing;
  assert.deepEqual(chunk.value, Uint8Array.of(0, 255, 42));
  assert.equal(bytes.byteLength, 3);
  const closing = writer.close();
  const end = await reader.read(new Uint8Array(8));
  await closing;
  assert.equal(end.done, true);
  assert.equal(end.value.byteLength, 0);
});

test("cancelling identity output rejects blocked writers with the same reason", async () => {
  const { readable, writable } = new IdentityTransformStream();
  const writer = writable.getWriter();
  const reason = new Error("consumer closed");
  const writing = writer.write(Uint8Array.of(7));
  const refused = assert.rejects(writing, (error) => error === reason);
  await readable.cancel(reason);
  await refused;
});

function deferred() {
  let resolve, reject;
  const promise = new Promise((done, fail) => {
    resolve = done;
    reject = fail;
  });
  return { promise, resolve, reject };
}

function alarmCell(invoke) {
  const app = workersApplication(
    { Counter: class {}, default: { fetch: () => new Response() } },
    { namespaces: { COUNTERS: "Counter" } },
  );
  const cell = new app.cells.Counter();
  cell.owner = () => ({ invoke: (call) => call({ alarm: invoke }) });
  return cell;
}

test("cancelling a queued alarm cannot let its successor overtake the active alarm", async () => {
  const entered = deferred();
  const release = deferred();
  let calls = 0;
  const cell = alarmCell(async () => {
    calls++;
    if (calls === 1) {
      entered.resolve();
      await release.promise;
    }
  });
  const alive = { _revoked: () => new Promise(() => {}) };
  const first = cell.alarm(null, alive);
  await entered.promise;
  const cancelled = deferred();
  const second = cell.alarm(null, { _revoked: () => cancelled.promise });
  const third = cell.alarm(null, alive);
  const reason = new Error("queued event revoked");
  cancelled.reject(reason);
  await assert.rejects(second, (error) => error === reason);
  assert.equal(calls, 1);
  release.resolve();
  await Promise.all([first, third]);
  assert.equal(calls, 2);
});

test("revoking an active alarm releases its queue without reporting success", async () => {
  const entered = deferred();
  const release = deferred();
  const revoked = deferred();
  let calls = 0;
  const cell = alarmCell(async () => {
    calls++;
    if (calls === 1) {
      entered.resolve();
      await release.promise;
    }
  });
  const first = cell.alarm(null, { _revoked: () => revoked.promise });
  await entered.promise;
  const reason = Object.assign(new Error("event revoked"), {
    code: "ContextExpired",
    outcome: "unknown",
  });
  revoked.reject(reason);
  await assert.rejects(first, (error) => error === reason);
  await cell.alarm(null, { _revoked: () => new Promise(() => {}) });
  assert.equal(calls, 2);
  release.resolve();
});
