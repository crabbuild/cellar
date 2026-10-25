// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import test from "node:test";
import assert from "node:assert/strict";
import { containerPort } from "../../packages/cellar-cloudflare/port.js";

function fixture(limit = 4) {
  const requests = [];
  let admitted = true;
  const context = {
    _sync: (name) => {
      assert.equal(name, "workers.http.limits");
      return { max_body_bytes: limit };
    },
    _encode: (body) => Buffer.from(body).toString("base64"),
    _call: async (name, { port, request }, body, signal) => {
      assert.equal(name, "workers.container.fetch");
      assert.equal(body, undefined);
      assert.ok(signal instanceof AbortSignal);
      requests.push({ port, ...request });
      return {
        status: 302,
        headers: [["location", "https://unrelated.invalid"]],
        body: Uint8Array.of(0, 255),
      };
    },
  };
  const current = () => {
    if (!admitted) throw new Error("ContextMismatch");
    return context;
  };
  return {
    port: containerPort(current, 8080, async () => {}),
    requests,
    retire: () => {
      admitted = false;
    },
  };
}

test("Workers port keeps binary bodies and returns redirects without egress", async () => {
  const { port, requests } = fixture();
  const response = await port.fetch("https://untrusted.invalid/a?b=c", {
    method: "POST",
    body: Uint8Array.of(0, 255, 42),
    headers: { "x-test": "value" },
  });
  assert.equal(response.status, 302);
  assert.equal(response.headers.get("location"), "https://unrelated.invalid");
  assert.deepEqual(
    new Uint8Array(await response.arrayBuffer()),
    Uint8Array.of(0, 255),
  );
  assert.equal(requests[0].port, 8080);
  assert.equal(requests[0].url, "https://untrusted.invalid/a?b=c");
  assert.deepEqual(
    Buffer.from(requests[0].body, "base64"),
    Buffer.from([0, 255, 42]),
  );
});

test("oversized streamed bodies cancel their source before provider submission", async () => {
  const { port, requests } = fixture();
  let cancelled = false;
  const body = new ReadableStream({
    start(controller) {
      controller.enqueue(new Uint8Array(5));
    },
    cancel() {
      cancelled = true;
    },
  });
  await assert.rejects(
    port.fetch("http://container/", { method: "POST", body, duplex: "half" }),
    RangeError,
  );
  assert.equal(cancelled, true);
  assert.deepEqual(requests, []);
});

test("abort interrupts a pending request-body read without submitting HTTP", async () => {
  const { port, requests } = fixture();
  const controller = new AbortController();
  const reason = new Error("caller cancelled upload");
  const { promise, resolve } = Promise.withResolvers();
  let cancelled;
  const body = new ReadableStream(
    {
      pull() {
        resolve();
        return new Promise(() => {});
      },
      cancel(value) {
        cancelled = value;
      },
    },
    { highWaterMark: 0 },
  );
  const pending = port.fetch("http://container/", {
    method: "POST",
    body,
    duplex: "half",
    signal: controller.signal,
  });
  await promise;
  controller.abort(reason);
  await assert.rejects(pending, (error) => error === reason);
  assert.deepEqual(requests, []);
  assert.equal(cancelled, reason);
});

test("retained ports cannot borrow retired invocation authority", async () => {
  const { port, retire, requests } = fixture();
  retire();
  await assert.rejects(port.fetch("http://container/"), /ContextMismatch/);
  assert.deepEqual(requests, []);
});
