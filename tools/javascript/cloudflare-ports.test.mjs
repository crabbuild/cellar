// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import test from "node:test";
import assert from "node:assert/strict";
import { containerPort } from "../../packages/cellar-cloudflare/port.js";
import { streamedResponse } from "../../packages/cellar-cloudflare/response.js";

function fixture(limit = 4) {
  const requests = [];
  let admitted = true;
  const context = {
    _sync: (name) => {
      assert.equal(name, "workers.http.limits");
      return { max_body_bytes: limit };
    },
    _decode: (body) => Uint8Array.from(Buffer.from(body, "base64")),
    _encode: (body) => Buffer.from(body).toString("base64"),
    _call: async (name, { port, request }, body, signal) => {
      assert.equal(name, "workers.container.fetch_stream");
      assert.equal(body, undefined);
      assert.ok(signal instanceof AbortSignal);
      requests.push({ port, ...request });
      return {
        stream: null,
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

test("HEAD port replies expose no response body", async () => {
  const { port, requests } = fixture();
  const response = await port.fetch("http://container/", { method: "HEAD" });
  assert.equal(response.body, null);
  assert.equal((await response.arrayBuffer()).byteLength, 0);
  assert.equal(requests[0].method, "HEAD");
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

function streamFixture(chunks) {
  const calls = [];
  const controller = new AbortController();
  let pending;
  let admitted = true;
  const context = {
    _decode: (body) => Uint8Array.from(Buffer.from(body, "base64")),
    async _call(name, { id }, _body, signal) {
      if (!admitted) throw new Error("ContextMismatch");
      calls.push(name);
      assert.equal(id, "response-1");
      if (name === "container.response.cancel") {
        pending?.({ done: true, body: "" });
        return null;
      }
      assert.equal(name, "container.response.read");
      assert.equal(signal, controller.signal);
      const chunk = chunks.shift();
      if (chunk === undefined)
        return new Promise((resolve) => {
          pending = resolve;
        });
      return {
        done: chunk === null,
        body: chunk === null ? "" : Buffer.from(chunk).toString("base64"),
      };
    },
  };
  const response = streamedResponse(
    {
      status: 200,
      headers: [["x-test", "stream"]],
      body: "AP8=",
      stream: "response-1",
    },
    context,
    controller.signal,
  );
  return {
    response,
    calls,
    controller,
    retire: () => {
      admitted = false;
    },
  };
}

test("port responses pull on demand, preserve binary prefixes and empty chunks, and finish BYOB reads", async () => {
  const { response, calls } = streamFixture([[], [42, 255], null]);
  assert.deepEqual(calls, []);
  assert.equal(response.headers.get("x-test"), "stream");
  const reader = response.body.getReader({ mode: "byob" });
  assert.deepEqual(
    (await reader.read(new Uint8Array(8))).value,
    Uint8Array.of(0, 255),
  );
  assert.deepEqual(calls, []);
  assert.deepEqual(
    (await reader.read(new Uint8Array(8))).value,
    Uint8Array.of(42, 255),
  );
  assert.deepEqual(calls, [
    "container.response.read",
    "container.response.read",
  ]);
  assert.equal((await reader.read(new Uint8Array(8))).done, true);
  reader.releaseLock();
  assert.deepEqual(calls, Array(3).fill("container.response.read"));
});

test("abort after headers cancels a pending native pull and preserves the caller reason", async () => {
  const { response, calls, controller } = streamFixture([]);
  const reader = response.body.getReader();
  await reader.read();
  const pending = reader.read();
  await Promise.resolve();
  assert.deepEqual(calls, ["container.response.read"]);
  const reason = new Error("cancel running Sandbox stream");
  controller.abort(reason);
  await assert.rejects(pending, (error) => error === reason);
  await assert.rejects(reader.read(), (error) => error === reason);
  assert.deepEqual(calls, [
    "container.response.read",
    "container.response.cancel",
  ]);
});

test("early consumer cancellation releases once and retained bodies reject retired authority", async () => {
  const early = streamFixture([]);
  await early.response.body.cancel();
  early.controller.abort();
  assert.deepEqual(early.calls, ["container.response.cancel"]);
  const retained = streamFixture([[42]]);
  const reader = retained.response.body.getReader();
  await reader.read();
  retained.retire();
  await assert.rejects(reader.read(), /ContextMismatch/);
  assert.deepEqual(retained.calls, []);
});

test("an activation-owned port cannot move a response reader into a later event", async () => {
  const calls = [];
  let context;
  const original = {
    _sync: () => ({ max_body_bytes: 4 }),
    _encode: (body) => Buffer.from(body).toString("base64"),
    _decode: (body) => Uint8Array.from(Buffer.from(body, "base64")),
    async _call(name) {
      calls.push(name);
      if (name === "workers.container.fetch_stream")
        return { status: 200, headers: [], body: "", stream: "1" };
      assert.ok(
        ["container.response.read", "container.response.cancel"].includes(name),
      );
      throw new Error("original event retired");
    },
  };
  context = original;
  const port = containerPort(
    () => context,
    3000,
    async () => {},
  );
  const response = await port.fetch("http://container/");
  context = {
    _call: () => assert.fail("reader borrowed a later event's handle table"),
  };
  await assert.rejects(
    response.body.getReader().read(),
    /original event retired/,
  );
  assert.ok(calls.includes("container.response.read"));
});
