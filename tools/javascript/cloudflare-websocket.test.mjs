// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import test from "node:test";
import assert from "node:assert/strict";
import { AsyncLocalStorage } from "node:async_hooks";
import {
  eventWebSocket,
  websocketResponse,
} from "../../packages/cellar-cloudflare/websocket.js";
import { installSocketGlobals } from "../../packages/cellar-cloudflare/websocket-pair.js";
import { containerPort } from "../../packages/cellar-cloudflare/port.js";

function fixture(maximum = 256) {
  const scope = new AsyncLocalStorage(),
    jobs = new Set(),
    cleanups = [],
    calls = [];
  const frames = [];
  let receiver;
  function push(frame) {
    if (receiver) {
      const value = receiver;
      receiver = undefined;
      value(frame);
    } else frames.push(frame);
  }
  const makeContext = (name) => ({
    storage: {
      async sync() {
        assert.equal(scope.getStore(), name);
        if (name === "second") await proof.promise;
      },
    },
    _sync: () => ({ max_body_bytes: maximum }),
    _encode: (data) => Buffer.from(data).toString("base64"),
    _decode: (data) => Uint8Array.from(Buffer.from(data, "base64")),
    _rpcCleanup: (close) => cleanups.push(close),
    _rpcRetain(make) {
      assert.equal(scope.getStore(), name, "borrowed a foreign event");
      const pending = Promise.resolve(make());
      jobs.add(pending);
      pending.then(
        () => jobs.delete(pending),
        () => jobs.delete(pending),
      );
      return pending;
    },
    async _call(operation, input, _body, signal) {
      assert.equal(
        scope.getStore(),
        "first",
        "native socket I/O borrowed a later event",
      );
      if (operation === "container.websocket.receive") {
        if (frames.length) return frames.shift();
        return new Promise((resolve, reject) => {
          receiver = resolve;
          signal.addEventListener(
            "abort",
            () => {
              receiver = undefined;
              reject(new Error("cancelled"));
            },
            { once: true },
          );
        });
      }
      calls.push([operation, input]);
      if (operation === "container.websocket.send_text")
        push({ type: "text", text: input.text });
      if (operation === "container.websocket.send_binary")
        push({ type: "binary", data: input.data });
      if (operation === "container.websocket.close")
        push({
          type: "close",
          code: input.code,
          reason: input.reason,
          clean: true,
        });
    },
  });
  const proof = Promise.withResolvers(),
    contexts = { first: makeContext("first"), second: makeContext("second") };
  const current = () => {
    const result = contexts[scope.getStore()];
    if (!result) throw new Error("ContextMismatch");
    return result;
  };
  const socket = scope.run("first", () =>
    eventWebSocket({ id: "native", protocol: "test" }, contexts.first, current),
  );
  return {
    socket,
    calls,
    push,
    proof,
    run: (name, make) => scope.run(name, make),
    async drain() {
      await scope.run("first", () =>
        Promise.all(cleanups.map((close) => close())),
      );
      await Promise.allSettled([...jobs]);
      assert.equal(jobs.size, 0);
    },
  };
}

test("port upgrade returns a native-backed 101 with handshake headers", async () => {
  const f = fixture();
  const socket = f.socket;
  const response = websocketResponse(
    {
      headers: [
        ["x-upgrade", "yes"],
        ["set-cookie", "a=1"],
      ],
    },
    socket,
  );
  assert.ok(response instanceof Response);
  assert.equal(response.status, 101);
  assert.equal(response.ok, false);
  assert.equal(response.headers.get("x-upgrade"), "yes");
  assert.equal(response.webSocket, socket);
  assert.equal(response.body, null);
  assert.throws(() => response.clone(), /cannot be cloned/);
  await f.drain();
  assert.equal(f.calls[0][0], "container.release");
});

test("same-activation sends prove the caller before original-event socket I/O", async () => {
  const f = fixture();
  f.run("first", () => f.socket.accept());
  let received;
  const message = Promise.withResolvers();
  f.socket.addEventListener("message", (event) => {
    assert.equal(event.target, f.socket);
    assert.ok(event instanceof MessageEvent);
    received = event.data;
    message.resolve();
  });
  f.run("second", () => f.socket.send("hello"));
  await new Promise(setImmediate);
  assert.deepEqual(f.calls, []);
  assert.equal(f.socket.bufferedAmount, 5);
  f.proof.resolve();
  await message.promise;
  assert.equal(received, "hello");
  assert.equal(f.calls[0][0], "container.websocket.send_text");
  f.run("second", () => f.socket.close(3000, "done"));
  await new Promise(setImmediate);
  await f.drain();
  assert.equal(f.socket.readyState, 3);
});

test("binary frames copy exact bytes and close events keep native clean status", async () => {
  const f = fixture();
  f.run("first", () => f.socket.accept());
  const message = Promise.withResolvers(),
    closed = Promise.withResolvers();
  f.socket.onmessage = (event) => message.resolve(event.data);
  f.socket.onclose = (event) =>
    closed.resolve({
      code: event.code,
      reason: event.reason,
      clean: event.wasClean,
    });
  const input = Uint8Array.of(9, 0, 255, 8);
  f.run("first", () => f.socket.send(input.subarray(1, 3)));
  input.fill(1);
  assert.deepEqual(
    new Uint8Array(await message.promise),
    Uint8Array.of(0, 255),
  );
  f.push({ type: "close", code: 1000, reason: "peer", clean: true });
  assert.deepEqual(await closed.promise, {
    code: 1000,
    reason: "peer",
    clean: true,
  });
  await f.drain();
});

test("foreign sends, invalid frames and queue overflow refuse before socket writes", async () => {
  const f = fixture(8);
  f.run("first", () => f.socket.accept());
  assert.throws(
    () => f.run("foreign", () => f.socket.send("x")),
    /ContextMismatch/,
  );
  assert.throws(() => f.run("first", () => f.socket.close(1005)), /code/);
  assert.throws(
    () => f.run("first", () => f.socket.send("123456789")),
    /limit/,
  );
  f.run("second", () => {
    f.socket.send("12345678");
    f.socket.send("12345678");
  });
  assert.throws(() => f.run("second", () => f.socket.send("x")), /queue/);
  assert.deepEqual(f.calls, []);
  f.proof.resolve();
  await new Promise(setImmediate);
  await f.drain();
});

test("upgrade routing keeps declines and refuses non-GET upgrades before dispatch", async () => {
  const calls = [];
  const context = {
    _sync: () => ({ max_body_bytes: 256 }),
    _encode: (data) => Buffer.from(data).toString("base64"),
    _decode: (data) => Uint8Array.from(Buffer.from(data, "base64")),
    async _call(name, input, _body, signal) {
      calls.push([name, input]);
      assert.ok(signal instanceof AbortSignal);
      return {
        upgraded: false,
        response: { status: 403, headers: [["x-denied", "yes"]], body: "bm8=" },
      };
    },
  };
  const port = containerPort(
    () => context,
    3000,
    async () => {},
  );
  const declined = await port.fetch("http://untrusted.invalid/rpc", {
    headers: { Upgrade: "websocket" },
  });
  assert.equal(declined.status, 403);
  assert.equal(await declined.text(), "no");
  assert.equal(calls[0][0], "container.port.connect_websocket");
  assert.equal(calls[0][1].port, 3000);
  await assert.rejects(
    port.fetch("http://untrusted.invalid/rpc", {
      method: "POST",
      headers: { Upgrade: "websocket" },
      body: "x",
    }),
    /bodyless GET/,
  );
  assert.equal(calls.length, 1);
});

test("local WebSocketPair supports the published proxy shape and both directions", async () => {
  const scope = new AsyncLocalStorage(),
    jobs = new Set(),
    cleanups = [];
  let active = true;
  const owner = {
    identity: null,
    _sync: () => ({ max_body_bytes: 256 }),
    _encode: (data) => Buffer.from(data).toString("base64"),
    _decode: (data) => Uint8Array.from(Buffer.from(data, "base64")),
    _rpcCleanup: (close) => cleanups.push(close),
    _rpcRetain(make) {
      assert.equal(scope.getStore(), "pair");
      const job = Promise.resolve(make());
      jobs.add(job);
      job.then(
        () => jobs.delete(job),
        () => jobs.delete(job),
      );
      return job;
    },
  };
  installSocketGlobals(() => {
    if (!active || scope.getStore() !== "pair")
      throw new Error("ContextMismatch");
    return owner;
  });
  assert.throws(() => new WebSocketPair(), /ContextMismatch/);
  const pair = scope.run("pair", () => new WebSocketPair());
  const [client, server] = Object.values(pair);
  const response = new Response(null, {
    status: 101,
    webSocket: client,
    headers: { "x-proxy": "yes" },
  });
  assert.equal(response.status, 101);
  assert.equal(response.webSocket, client);
  assert.ok(response instanceof Response);
  const left = Promise.withResolvers(),
    right = Promise.withResolvers();
  client.onmessage = (event) => left.resolve(event.data);
  server.onmessage = (event) => right.resolve(event.data);
  scope.run("pair", () => {
    client.accept();
    server.accept();
    client.send("request");
    server.send("reply");
  });
  assert.equal(await left.promise, "reply");
  assert.equal(await right.promise, "request");
  scope.run("pair", () => client.close(1011, "proxy failed"));
  await new Promise(setImmediate);
  await scope.run("pair", () => Promise.all(cleanups.map((close) => close())));
  await Promise.allSettled([...jobs]);
  assert.equal(jobs.size, 0);
  active = false;
  assert.equal(client.readyState, 3);
  assert.equal(server.readyState, 3);
});

test("post-upgrade admission refusal releases the socket and stays an unknown outcome", async () => {
  const calls = [];
  const context = {
    _sync: () => ({ max_body_bytes: 256 }),
    _encode: (data) => Buffer.from(data).toString("base64"),
    _rpcCleanup() {
      throw Object.assign(new Error("cleanup quota"), {
        code: "OperationLimit",
        outcome: "not_committed",
      });
    },
    async _call(name) {
      calls.push(name);
      if (name === "container.release") return;
      return { upgraded: true, id: "owned", protocol: "", headers: [] };
    },
  };
  const port = containerPort(
    () => context,
    3000,
    async () => {},
  );
  await assert.rejects(
    port.fetch("http://container/rpc", { headers: { Upgrade: "websocket" } }),
    (error) => {
      assert.equal(error.code, "Ambiguous");
      assert.equal(error.outcome, "unknown");
      assert.equal(error.cause.outcome, "not_committed");
      return true;
    },
  );
  assert.deepEqual(calls, [
    "container.port.connect_websocket",
    "container.release",
  ]);
});
