// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import assert from "node:assert/strict";
import test from "node:test";
import { testRunner } from "../../../tools/javascript/test-runner.mjs";
import {
  sseEvent,
  sseResponse,
  streamResponse,
  webSocketResponse,
} from "../index.js";

test("SSE preserves multiline data and rejects field injection", () => {
  assert.equal(
    sseEvent("a\r\nb\rc\n", { event: "change", id: "42", retry: 1000 }),
    "event: change\nid: 42\nretry: 1000\ndata: a\ndata: b\ndata: c\ndata: \n\n",
  );

  for (const value of ["bad\nfield", "bad\rfield", "bad\0field"]) {
    assert.throws(() => sseEvent("x", { id: value }), TypeError);
    assert.throws(() => sseEvent("x", { event: value }), TypeError);
  }

  for (const retry of [-1, 0.1, Infinity]) {
    assert.throws(() => sseEvent("x", { retry }), TypeError);
  }

  assert.equal(sseResponse([]).headers[0][1], "text/event-stream");
});

test("shared runtime awaits native backpressure before pulling the next chunk", { timeout: 5000 }, async () => {
  let unblock;
  let signalFirstWrite;
  const firstWrite = new Promise(resolve => {
    signalFirstWrite = resolve;
  });
  let pulled = 0;
  const calls = [];
  const runner = testRunner(async (id, name, payload) => {
    calls.push([name, JSON.parse(payload)]);

    // Hold the first write to emulate a native consumer applying backpressure.
    if (
      name === "response.write" &&
      calls.filter(([name]) => name === "response.write").length === 1
    ) {
      await new Promise(resolve => {
        unblock = resolve;
        signalFirstWrite();
      });
    }

    // The private bridge carries JSON metadata and an optional raw attachment.
    return [JSON.stringify({ ok: true, value: null }), undefined];
  })(
    {
      fetch: () =>
        streamResponse(
          (async function* () {
            pulled++;
            yield new Uint8Array([0, 255]);
            pulled++;
            yield "é";
          })(),
        ),
    },
    JSON.stringify({ events: ["fetch"], cells: {} }),
    4,
  );
  const completion = runner.dispatch(
    1,
    JSON.stringify({
      kind: "fetch",
      object: null,
      identity: null,
      event: { body: "", headers: [], method: "GET", url: "http://test" },
    }),
  );

  // The producer must stay on its first chunk until the native write resolves.
  await Promise.race([
    firstWrite,
    completion.then(() => {
      throw new Error("Stream completed before its first native write");
    }),
  ]);

  assert.equal(pulled, 1);
  assert.deepEqual(
    calls.map(([name]) => name),
    ["response.start", "response.write"],
  );

  unblock();
  assert.deepEqual(JSON.parse(await completion), { streamed: true });
  assert.equal(pulled, 2);
  assert.deepEqual(
    calls
      .filter(([name]) => name === "response.write")
      .map(([, value]) => value.body),
    ["AP8=", "w6k="],
  );
});

test("socket response builders preserve opaque string capabilities", () => {
  assert.deepEqual(webSocketResponse({ id: "18446744073709551615" }), {
    websocket: "18446744073709551615",
    headers: [],
    run: undefined,
  });
});
