// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import assert from "node:assert/strict";
import test from "node:test";
import { peer } from "./support/rpc-input-peer.mjs";

test("HTTP arguments preserve Request/Response identity, metadata and binary bodies", async () => {
  const remote = peer(async (request, alias, response, responseAlias) => {
    assert.ok(request instanceof Request);
    assert.ok(response instanceof Response);
    assert.equal(request, alias);
    assert.equal(response, responseAlias);
    assert.equal(request.method, "POST");
    assert.equal(request.headers.get("x-input"), "bytes");
    assert.equal(response.status, 206);
    return {
      input: [...new Uint8Array(await request.arrayBuffer())],
      output: await response.text(),
    };
  });
  const request = new Request("https://example.test/input", {
    method: "POST",
    headers: { "x-input": "bytes" },
    body: Uint8Array.of(0, 255, 42),
  });
  const response = new Response("streamed output", { status: 206 });
  assert.deepEqual(await remote.call([request, request, response, response]), {
    input: [0, 255, 42],
    output: "streamed output",
  });
  await remote.close();
  assert.equal(request.body.locked, true);
  assert.equal(response.body.locked, true);
});

test("a request argument forwards caller abort after body EOF without retaining the event forever", async () => {
  const entered = Promise.withResolvers();
  const controller = new AbortController();
  const remote = peer(async (request) => {
    assert.equal(await request.text(), "upload");
    entered.resolve();
    await new Promise((resolve) =>
      request.signal.addEventListener("abort", resolve, { once: true }),
    );
    return request.signal.reason;
  });
  const result = remote.call([
    new Request("https://example.test/upload", {
      method: "POST",
      body: "upload",
      signal: controller.signal,
    }),
  ]);
  await entered.promise;
  controller.abort("client disconnected");
  assert.equal(await result, "client disconnected");
  await remote.close();
});

test("unconsumed bodyless Request and Response arguments release their sessions", async () => {
  const remote = peer(async (request, response) => {
    assert.equal(request.body, null);
    assert.equal(response.body, null);
    return true;
  });
  assert.equal(
    await remote.call([
      new Request("https://example.test/"),
      new Response(null, { status: 204 }),
    ]),
    true,
  );
  await remote.close();
});

test("HTTP argument refusal restores earlier body locks before entering the callee", async () => {
  let invoked = 0;
  const remote = peer(async () => {
    invoked++;
  });
  const response = new Response("untouched");
  await assert.rejects(remote.call([response, () => {}]));
  assert.equal(invoked, 0);
  assert.equal(response.body.locked, false);
  await remote.close();
});
