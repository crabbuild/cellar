// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import { serialize, deserialize } from "node:v8";
import { AsyncLocalStorage } from "node:async_hooks";

async function factory(name) {
  return (0, eval)(
    `(${await readFile(new URL(`../../crates/cellar-javascript/runtime/${name}.js`, import.meta.url), "utf8")})`,
  );
}
const [makeMessages, makeReadables, makeRequests] = await Promise.all(
  ["rpc_messages", "rpc_streams", "rpc_requests"].map(factory),
);
const context = {
  identity: null,
  _sync: () => ({ max_body_bytes: 1024 }),
  _current: () => context,
};

function connection(request, ctx = context) {
  const codec = makeMessages(makeReadables(), makeRequests);
  const sender = codec.send(request, ctx);
  let released = 0;
  const received = codec.receive(
    sender.metadata,
    sender.invoke,
    async () => {
      released++;
      await sender.close();
    },
    undefined,
    1024,
    {
      watch: async () => deserialize(serialize(await sender.invoke("signal"))),
      cleanup: () => () => {},
    },
  );
  return { sender, received, releases: () => released };
}

test("RPC requests preserve metadata and stream bytes while retaining their live abort signal", async () => {
  const controller = new AbortController();
  const request = new Request("https://example.test/upload?q=1", {
    method: "PATCH",
    headers: { "x-input": "binary" },
    body: Uint8Array.of(0, 255, 42),
    redirect: "manual",
    credentials: "omit",
    cache: "no-store",
    referrer: "https://example.test/source",
    referrerPolicy: "no-referrer",
    integrity: "sha256-example",
    signal: controller.signal,
  });
  const { received, releases } = connection(request);
  const copy = received.stream;
  assert.ok(copy instanceof Request);
  for (const key of [
    "method",
    "url",
    "redirect",
    "credentials",
    "cache",
    "referrer",
    "referrerPolicy",
    "integrity",
    "mode",
    "keepalive",
    "destination",
  ])
    assert.equal(copy[key], request[key], key);
  assert.equal(copy.headers.get("x-input"), "binary");
  assert.equal(request.body.locked, true);
  assert.deepEqual(
    new Uint8Array(await copy.arrayBuffer()),
    Uint8Array.of(0, 255, 42),
  );
  assert.equal(
    releases(),
    0,
    "EOF must not discard the client's abort channel",
  );
  controller.abort("client disconnected after upload");
  await new Promise(setImmediate);
  assert.equal(copy.signal.aborted, true);
  assert.equal(copy.signal.reason, "client disconnected after upload");
  await received.dispose();
  assert.equal(releases(), 1);
});

test("bodyless requests keep their signal lease until explicit disposal", async () => {
  const { received, releases } = connection(
    new Request("https://example.test/status"),
  );
  assert.equal(received.stream.body, null);
  assert.equal(received.stream.signal.aborted, false);
  assert.equal(releases(), 0);
  await received.dispose();
  assert.equal(received.stream.signal.aborted, true);
  assert.equal(releases(), 1);
});

test("request transfer refuses used or locked bodies and pre-aborted requests without taking ownership", async () => {
  const codec = makeMessages(makeReadables(), makeRequests);
  const used = new Request("https://example.test/", {
    method: "POST",
    body: "used",
  });
  await used.text();
  assert.throws(() => codec.send(used, context), /used/);
  const locked = new Request("https://example.test/", {
    method: "POST",
    body: "locked",
  });
  const reader = locked.body.getReader();
  assert.throws(() => codec.send(locked, context), /used|locked/);
  reader.releaseLock();
  const controller = new AbortController();
  controller.abort("before admission");
  const aborted = new Request("https://example.test/", {
    method: "POST",
    body: "untouched",
    signal: controller.signal,
  });
  assert.throws(() => codec.send(aborted, context), /abort/i);
  assert.equal(aborted.body.locked, false);
});

test("failed request publication restores untouched body ownership and removes its signal subscription", async () => {
  const codec = makeMessages(makeReadables(), makeRequests);
  const request = new Request("https://example.test/", {
    method: "POST",
    body: "restore",
  });
  const sent = codec.send(request, context);
  assert.equal(request.body.locked, true);
  sent.restore();
  assert.equal(request.body.locked, false);
  assert.equal(await request.text(), "restore");
});

test("abort exception name and message survive the native clone boundary", async () => {
  const controller = new AbortController();
  const { received } = connection(
    new Request("https://example.test/", { signal: controller.signal }),
  );
  controller.abort(new DOMException("custom cancellation", "TimeoutError"));
  await new Promise(setImmediate);
  assert.equal(received.stream.signal.aborted, true);
  assert.ok(received.stream.signal.reason instanceof DOMException);
  assert.equal(received.stream.signal.reason.name, "TimeoutError");
  assert.equal(received.stream.signal.reason.message, "custom cancellation");
  await received.dispose();
});

test("later-event abort proves that caller's writes and cancels the source in its original scope", async () => {
  const scope = new AsyncLocalStorage();
  const proof = Promise.withResolvers();
  const controller = new AbortController();
  let cancelled;
  const later = {
    identity: { cell: "caller" },
    storage: {
      sync() {
        assert.equal(scope.getStore(), "later");
        return proof.promise;
      },
    },
  };
  const ctx = { ...context, _current: () => later };
  const link = scope.run("producer", () =>
    connection(
      new Request("https://example.test/", {
        method: "POST",
        duplex: "half",
        signal: controller.signal,
        body: new ReadableStream({
          pull() {},
          cancel() {
            cancelled = scope.getStore();
          },
        }),
      }),
      ctx,
    ),
  );
  scope.run("later", () => controller.abort("after write"));
  await new Promise(setImmediate);
  assert.equal(link.received.stream.signal.aborted, false);
  assert.equal(cancelled, undefined);
  proof.resolve();
  await new Promise(setImmediate);
  assert.equal(link.received.stream.signal.reason, "after write");
  assert.equal(cancelled, "producer");
  await link.received.dispose();
});
