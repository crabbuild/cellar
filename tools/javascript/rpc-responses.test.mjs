// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { serialize, deserialize } from "node:v8";
import test from "node:test";

async function factory(name) {
  const source = await readFile(
    new URL(
      `../../crates/cellar-javascript/runtime/${name}.js`,
      import.meta.url,
    ),
    "utf8",
  );
  return (0, eval)(`(${source})`);
}
const [makeMessages, makeReadables, makeWritables, makeDelegates] =
  await Promise.all(
    ["rpc_messages", "rpc_streams", "rpc_writable", "delegates"].map(factory),
  );
const context = { _sync: () => ({ max_body_bytes: 1024 }) };

test("RPC responses retain HTTP metadata and stream default byte chunks under bounded demand", async () => {
  const codec = makeMessages(makeReadables());
  let pulls = 0,
    released = 0;
  const response = new Response(
    new ReadableStream(
      {
        pull(controller) {
          if (pulls++ === 0)
            controller.enqueue(new Uint8Array(900).map((_, i) => i % 256));
          else controller.close();
        },
      },
      { highWaterMark: 0 },
    ),
    {
      status: 206,
      statusText: "Partial Content",
      headers: [
        ["set-cookie", "a=1"],
        ["set-cookie", "b=2"],
        ["x-body", "bytes"],
      ],
    },
  );
  Object.defineProperties(response, {
    url: { value: "https://example.test/file" },
    redirected: { value: true },
  });
  const sent = codec.send(response, context);
  assert.equal(pulls, 0);
  assert.equal(response.body.locked, true);
  const received = codec.receive(
    sent.metadata,
    sent.invoke,
    async () => {
      released++;
    },
    undefined,
    256,
  );
  const copy = received.stream;
  assert.equal(copy.status, 206);
  assert.equal(copy.statusText, "Partial Content");
  assert.equal(copy.url, response.url);
  assert.equal(copy.redirected, true);
  assert.deepEqual(copy.headers.getSetCookie(), ["a=1", "b=2"]);
  const reader = copy.body.getReader({ mode: "byob" });
  const chunks = [];
  for (;;) {
    const chunk = await reader.read(new Uint8Array(71));
    if (chunk.done) break;
    chunks.push(...chunk.value);
  }
  assert.deepEqual(
    chunks,
    Array.from({ length: 900 }, (_, i) => i % 256),
  );
  assert.equal(released, 1);
  assert.equal(response.body.locked, true);
});

test("bodyless and error responses release their lease without acquiring a reader", async () => {
  const codec = makeMessages(makeReadables());
  for (const original of [
    new Response(null, { status: 204 }),
    Response.error(),
  ]) {
    let released = 0;
    const sent = codec.send(original, context);
    const received = codec.receive(sent.metadata, sent.invoke, async () => {
      released++;
    });
    assert.equal(received.stream.status, original.status);
    assert.equal(received.stream.type, original.type);
    assert.equal(received.stream.body, null);
    assert.equal(released, 1);
  }
});

test("response disposal joins pending reads and malformed byte chunks fail after their prefix", async () => {
  const codec = makeMessages(makeReadables());
  for (const bad of ["not bytes", new Uint8Array(1025)]) {
    let phase = 0,
      cancelled = false;
    const sent = codec.send(
      new Response(
        new ReadableStream(
          {
            pull(controller) {
              controller.enqueue(phase++ ? bad : Uint8Array.of(1, 2));
            },
            cancel() {
              cancelled = true;
            },
          },
          { highWaterMark: 0 },
        ),
      ),
      context,
    );
    const received = codec.receive(sent.metadata, sent.invoke, async () => {});
    const reader = received.stream.body.getReader();
    assert.deepEqual((await reader.read()).value, Uint8Array.of(1, 2));
    await assert.rejects(reader.read(), /bounded byte chunks/);
    await received.dispose();
    assert.equal(cancelled, true);
  }
  const entered = Promise.withResolvers();
  let reason;
  const sent = codec.send(
    new Response(
      new ReadableStream(
        {
          pull() {
            entered.resolve();
          },
          cancel(value) {
            reason = value;
          },
        },
        { highWaterMark: 0 },
      ),
    ),
    context,
  );
  const received = codec.receive(sent.metadata, sent.invoke, async () => {});
  const reader = received.stream.body.getReader();
  const pending = reader.read();
  await entered.promise;
  await reader.cancel("download left");
  assert.equal((await pending).done, true);
  assert.equal(reason, "download left");
});

test("response publication preserves aliases, refuses upgrades and restores locks on admission failure", async () => {
  const codec = makeDelegates(
    makeReadables(),
    makeWritables(),
    undefined,
    makeMessages,
  );
  const jobs = [],
    calls = [];
  const ctx = {
    ...context,
    _serialize: (value) => serialize(value).toString("base64"),
    _deserialize: (value) => deserialize(Buffer.from(value, "base64")),
    _rpcRetain: (make) => jobs.push(make()),
    async _call(name, input) {
      calls.push([name, input]);
      if (name === "workers.rpc.export") return { group: "native-lease" };
      if (name === "workers.rpc.poll") return { closed: true };
      if (name === "workers.rpc.release") return null;
      throw new Error("unexpected call");
    },
  };
  const response = new Response(null, { status: 204 });
  const frame = await codec.result({ response, alias: response }, ctx);
  assert.equal(frame.exports.count, 1);
  const copy = codec.reply(
    { ...frame, handles: [1], max_body_bytes: 1024 },
    ctx,
  );
  assert.ok(copy.response instanceof Response);
  assert.equal(copy.response, copy.alias);
  assert.equal(
    codec.reply({ value: frame.value, error: false }, ctx)[0]
      .response instanceof Response,
    false,
  );
  await Promise.all(jobs);
  const body = new Response(
    new ReadableStream({ pull() {} }, { highWaterMark: 0 }),
  );
  await assert.rejects(
    codec.result(body, {
      ...ctx,
      _rpcRetain() {
        throw new Error("quota");
      },
    }),
    /quota/,
  );
  assert.equal(body.body.locked, false);
  Object.defineProperty(response, "webSocket", { value: {} });
  await assert.rejects(codec.result(response, ctx), /WebSocket/);
});
