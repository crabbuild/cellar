// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import assert from "node:assert/strict";
import test from "node:test";
import { encodeStreamResponse } from "../../packages/cellar-cloudflare/output.js";
import { installSocketGlobals } from "../../packages/cellar-cloudflare/websocket-pair.js";
import { forwardedWebSocket } from "../../packages/cellar-cloudflare/websocket-forward.js";

let activeOwner;
installSocketGlobals(() => activeOwner);

function fixture() {
  const cleanups = [],
    jobs = new Set(),
    calls = [],
    frames = [],
    writes = [];
  let receiver;
  const disconnected = Promise.withResolvers();
  const owner = {
    identity: null,
    _sync: () => ({ max_body_bytes: 256 }),
    _encode: (data) => Buffer.from(data).toString("base64"),
    _decode: (data) => Uint8Array.from(Buffer.from(data, "base64")),
    _rpcCleanup: (close) => cleanups.push(close),
    _rpcRetain(make) {
      const work = Promise.resolve(make());
      jobs.add(work);
      work.then(
        () => jobs.delete(work),
        () => jobs.delete(work),
      );
      return work;
    },
    async _call(name, input, _body, signal) {
      calls.push([name, input]);
      if (name === "websocket.pair") return { id: "network" };
      if (name === "websocket.closed") {
        return new Promise((resolve, reject) => {
          const abort = () => reject(new Error("cancelled"));
          signal.addEventListener("abort", abort, { once: true });
          disconnected.promise.then(() => {
            signal.removeEventListener("abort", abort);
            resolve();
          });
        });
      }
      if (name === "websocket.next") {
        if (frames.length) return frames.shift();
        return new Promise((resolve, reject) => {
          const abort = () => {
            receiver = undefined;
            reject(new Error("cancelled"));
          };
          signal.addEventListener("abort", abort, { once: true });
          receiver = (frame) => {
            signal.removeEventListener("abort", abort);
            receiver = undefined;
            resolve(frame);
          };
          if (signal.aborted) abort();
        });
      }
      if (name === "websocket.send") writes.push(input);
    },
  };
  activeOwner = owner;
  const [client, server] = Object.values(new WebSocketPair());
  return {
    owner,
    client,
    server,
    calls,
    writes,
    disconnect() {
      disconnected.resolve();
      this.push({ type: "close", code: 1006, reason: "", was_clean: false });
    },
    push(frame) {
      if (receiver) receiver(frame);
      else frames.push(frame);
    },
    async drain() {
      await Promise.all(cleanups.map((close) => close()));
      await Promise.allSettled([...jobs]);
      assert.equal(jobs.size, 0);
    },
  };
}

test("module output transfers one unaccepted pair endpoint and joins unused upgrade cleanup", async () => {
  const f = fixture();
  try {
    const response = new Response(null, {
      status: 101,
      webSocket: f.client,
      headers: { "x-upgrade": "pair" },
    });
    const frame = await encodeStreamResponse(response, f.owner);
    assert.equal(frame.websocket, "network");
    assert.deepEqual(frame.headers, [["x-upgrade", "pair"]]);
    assert.equal(typeof frame.run, "function");
    assert.throws(() => f.client.accept(), /transferred/);
    await assert.rejects(
      Promise.resolve().then(() => encodeStreamResponse(response, f.owner)),
      /transferred/,
    );
    await frame.cancel();
  } finally {
    await f.drain();
  }
});

test("a namespace upgrade forwards its native capability and edited headers exactly once", async () => {
  const f = fixture();
  try {
    const response = forwardedWebSocket(
      { forward: "retained", headers: [["x-origin", "yes"]] },
      f.owner,
      () => f.owner,
    );
    response.headers.set("x-relay", "worker");
    assert.deepEqual(await encodeStreamResponse(response, f.owner), {
      forward: "retained",
      headers: [
        ["x-origin", "yes"],
        ["x-relay", "worker"],
      ],
    });
    await assert.rejects(
      encodeStreamResponse(response, f.owner),
      /transferred/,
    );
    assert.deepEqual(f.calls, []);
  } finally {
    await f.drain();
  }
});

test("an invalid upgrade head closes its owned pair without leaking accepted pumps", async () => {
  const f = fixture();
  try {
    f.server.accept();
    const headers = Array.from({ length: 129 }, (_, index) => [
      `x-${index}`,
      "value",
    ]);
    const response = new Response(null, {
      status: 101,
      webSocket: f.client,
      headers,
    });
    await assert.rejects(
      encodeStreamResponse(response, f.owner),
      /headers exceed/,
    );
    await new Promise(setImmediate);
    assert.equal(f.server.readyState, 3);
    assert.deepEqual(f.calls, []);
  } finally {
    await f.drain();
  }
});

test("same-event facade aliases transfer without admitting a later canonical event", async () => {
  const f = fixture();
  try {
    const alias = { ...f.owner, _current: () => f.owner };
    const response = forwardedWebSocket(
      { forward: "aliased", headers: [] },
      alias,
      () => ({ ...alias }),
    );
    await assert.rejects(
      encodeStreamResponse(response, { _current: () => ({}) }),
      (error) => error.code === "ContextMismatch",
    );
    assert.deepEqual(await encodeStreamResponse(response, f.owner), {
      forward: "aliased",
      headers: [],
    });
    assert.deepEqual(f.calls, []);
  } finally {
    await f.drain();
  }
});

test("disconnect joins an upgrade even while delivery to an unaccepted server is held", async () => {
  const f = fixture();
  let work;
  try {
    const output = await encodeStreamResponse(
      new Response(null, { status: 101, webSocket: f.client }),
      f.owner,
    );
    work = output.run();
    work.catch(() => {});
    f.push({ type: "message", text: "held" });
    await new Promise(setImmediate);
    f.disconnect();
    const settled = await Promise.race([
      work.then(() => true),
      new Promise(setImmediate).then(() => false),
    ]);
    assert.equal(
      settled,
      true,
      "disconnect must wake the bridge without waiting for application accept",
    );
  } finally {
    await f.drain();
    await Promise.allSettled([work]);
  }
});

test("upgrade pumps preserve bidirectional binary frames, close and native acknowledgements", async () => {
  const f = fixture();
  try {
    const received = Promise.withResolvers();
    f.server.onmessage = (event) => received.resolve(event.data);
    f.server.accept();
    const output = await encodeStreamResponse(
      new Response(null, { status: 101, webSocket: f.client }),
      f.owner,
    );
    const work = output.run();
    f.push({ type: "message", body: "AP8q" });
    assert.deepEqual(
      new Uint8Array(await received.promise),
      Uint8Array.of(0, 255, 42),
    );
    f.server.send("outbound");
    await new Promise(setImmediate);
    assert.deepEqual(f.writes, [{ id: "network", text: "outbound" }]);
    f.push({ type: "close", code: 3001, reason: "peer", was_clean: true });
    await work;
    assert.equal(f.server.readyState, 3);
    assert.ok(
      f.calls.some(
        ([name, input]) =>
          name === "websocket.close" &&
          input.code === 3001 &&
          input.reason === "peer",
      ),
    );
  } finally {
    await f.drain();
  }
});

test("foreign, synthetic and accepted upgrade endpoints refuse before creating a native pair", async () => {
  const f = fixture();
  try {
    const response = new Response(null, { status: 101, webSocket: f.client });
    await assert.rejects(
      encodeStreamResponse(response, {}),
      (error) => error.code === "ContextMismatch",
    );
    f.client.accept();
    await assert.rejects(encodeStreamResponse(response, f.owner), /unaccepted/);
    const forged = new Response(null);
    Object.defineProperties(forged, {
      status: { value: 101 },
      webSocket: { value: {} },
    });
    await assert.rejects(encodeStreamResponse(forged, f.owner), /owned/);
    assert.deepEqual(f.calls, []);
  } finally {
    await f.drain();
  }
});
