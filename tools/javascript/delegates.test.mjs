// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { Serializer, Deserializer } from "node:v8";
import test from "node:test";

const source = await readFile(
  new URL(
    "../../crates/cellar-javascript/runtime/delegates.js",
    import.meta.url,
  ),
  "utf8",
);
const makeDelegates = (0, eval)(`(${source})`);
const streamSource = await readFile(
  new URL(
    "../../crates/cellar-javascript/runtime/rpc_streams.js",
    import.meta.url,
  ),
  "utf8",
);
const writableSource = await readFile(
  new URL(
    "../../crates/cellar-javascript/runtime/rpc_writable.js",
    import.meta.url,
  ),
  "utf8",
);
const make = () =>
  makeDelegates(
    (0, eval)(`(${streamSource})`)(),
    (0, eval)(`(${writableSource})`)(),
  );

function context() {
  const calls = [],
    jobs = [];
  const ctx = {
    _decode: (value) => Buffer.from(value, "base64"),
    _serialize(value) {
      const serializer = new Serializer();
      serializer.writeHeader();
      serializer.writeValue(value);
      return serializer.releaseBuffer().toString("base64");
    },
    _deserialize(value) {
      const decoder = new Deserializer(Buffer.from(value, "base64"));
      decoder.readHeader();
      return decoder.readValue();
    },
    _waitUntil: (job) => jobs.push(job),
    _rpcRetain: (make) => jobs.push(make()),
    _sync(name, value) {
      calls.push([name, value]);
      if (name === "workers.http.limits") return { max_body_bytes: 4096 };
      assert.equal(name, "workers.rpc.dup");
      return { id: 99 };
    },
    async _call(name, value) {
      calls.push([name, value]);
      if (name === "workers.rpc.export") return { group: "native-owned" };
      if (name === "workers.rpc.poll") return { closed: true };
      if (name === "workers.rpc.invoke")
        return {
          value: ctx._serialize(ctx._deserialize(value.args)),
          error: false,
        };
      assert.equal(name, "workers.rpc.release");
      return null;
    },
  };
  return { ctx, calls, jobs };
}

test("callable marker graph preserves cycles, aliases, maps and real empty objects", async () => {
  const codec = make();
  const { ctx, calls, jobs } = context();
  const fn = () => {};
  const buffer = new ArrayBuffer(8);
  const input = {
    fn,
    alias: fn,
    empty: {},
    buffer,
    view: new Uint8Array(buffer, 2, 3),
  };
  input.self = input;
  input.map = new Map([[fn, input]]);
  input.sparse = [];
  input.sparse[3] = input;
  const encoded = await codec.result(input, ctx);
  assert.equal(encoded.exports.count, 1);
  const reply = codec.reply({ ...encoded, handles: [7] }, ctx);
  assert.equal(reply.self, reply);
  assert.equal(Object.getPrototypeOf(reply), Object.prototype);
  assert.equal(reply.fn, reply.alias);
  assert.equal(reply.map.get(reply.fn), reply);
  assert.equal(reply.sparse[3], reply);
  assert.equal(0 in reply.sparse, false);
  assert.ok(
    reply.view.buffer === reply.buffer,
    "view retains its shared backing buffer",
  );
  assert.deepEqual(Object.keys(reply.empty), []);
  assert.equal(typeof reply.empty, "object");
  assert.deepEqual(await reply.fn(1n, new Uint8Array([9])), [
    1n,
    new Uint8Array([9]),
  ]);
  const duplicate = reply.fn.dup();
  reply[Symbol.dispose]();
  reply[Symbol.dispose]();
  await assert.rejects(reply.fn(), /disposed/);
  assert.throws(() => reply.fn.dup(), /disposed/);
  assert.deepEqual(await duplicate("still owned"), ["still owned"]);
  duplicate[Symbol.dispose]();
  assert.equal(
    calls.filter(([name]) => name === "workers.rpc.release").length,
    2,
  );
  await Promise.all(jobs);
});

test("invalid graphs fail before exporting authority and malformed handle sets refuse", async () => {
  const codec = make();
  const { ctx, calls } = context();
  let input = () => {};
  for (let index = 0; index < 70; index++) input = { input };
  await assert.rejects(codec.result(input, ctx), /quota/);
  class Private {
    secret = () => {};
  }
  await assert.rejects(codec.result(new Private(), ctx), /RpcTarget/);
  assert.equal(calls.length, 0);
  assert.throws(
    () =>
      codec.reply(
        { value: ctx._serialize([{}, []]), handles: [1], error: false },
        ctx,
      ),
    /Invalid callable/,
  );
});

test("value objects have no-op disposers and stale disposal handles its rejected cleanup", async () => {
  const codec = make();
  const { ctx, jobs } = context();
  const plain = codec.reply(
    { value: ctx._serialize({ value: 1 }), error: false },
    ctx,
  );
  assert.equal(typeof plain[Symbol.dispose], "function");
  plain[Symbol.dispose]();
  const encoded = await codec.result(() => {}, ctx);
  const callable = codec.reply({ ...encoded, handles: [7] }, ctx);
  const call = ctx._call;
  ctx._call = async (name, value) => {
    if (name === "workers.rpc.release")
      throw new Error("context already ended");
    return call(name, value);
  };
  callable[Symbol.dispose]();
  callable[Symbol.dispose]();
  await Promise.all(jobs);
});

test("implicit-job admission happens before exporting authority", async () => {
  const codec = make();
  const { ctx, calls } = context();
  ctx._rpcRetain = () => {
    throw new Error("job quota");
  };
  await assert.rejects(
    codec.result(() => {}, ctx),
    /job quota/,
  );
  assert.equal(calls.length, 0);
});

test("RPC stream markers preserve aliases and persisted bytes cannot mint a stream", async () => {
  const codec = make();
  const { ctx, jobs } = context();
  let cancelled = 0;
  const body = new ReadableStream({
    type: "bytes",
    cancel() {
      cancelled++;
    },
  });
  const encoded = await codec.result(
    { body, alias: body, map: new Map([[body, 1]]) },
    ctx,
  );
  assert.equal(encoded.exports.count, 1);
  assert.equal(body.locked, true);
  const reply = codec.reply(
    { ...encoded, handles: [7], max_body_bytes: 4096 },
    ctx,
  );
  assert.ok(reply.body instanceof ReadableStream);
  assert.equal(reply.body, reply.alias);
  assert.equal(reply.map.get(reply.body), 1);
  const stored = codec.reply({ value: encoded.value, error: false }, ctx);
  assert.equal(stored[0].body instanceof ReadableStream, false);
  reply[Symbol.dispose]();
  await Promise.all(jobs);
  assert.equal(cancelled, 1);
});

test("failed native export or implicit admission restores an untouched stream", async () => {
  const codec = make();
  for (const failAdmission of [false, true]) {
    const { ctx, calls, jobs } = context();
    const body = new ReadableStream({ type: "bytes" });
    if (failAdmission)
      ctx._rpcRetain = () => {
        throw new Error("job quota");
      };
    else
      ctx._call = async () => {
        throw new Error("export refusal");
      };
    await assert.rejects(
      codec.result(body, ctx),
      failAdmission ? /job quota/ : /export refusal/,
    );
    assert.equal(body.locked, false);
    assert.equal(
      calls.some(([name]) => name === "workers.rpc.export"),
      false,
    );
    await Promise.all(jobs);
    await body.cancel();
  }
});

test("writable markers retain aliases and value bytes cannot recreate a writer", async () => {
  const codec = make();
  const { ctx, jobs } = context();
  let aborted = 0;
  const stream = new WritableStream({
    abort() {
      aborted++;
    },
  });
  const encoded = await codec.result({ stream, alias: stream }, ctx);
  assert.equal(encoded.exports.count, 1);
  assert.equal(stream.locked, true);
  const value = codec.reply(
    { ...encoded, handles: [7], max_body_bytes: 4096 },
    ctx,
  );
  assert.ok(value.stream instanceof WritableStream);
  assert.equal(value.stream, value.alias);
  const stored = codec.reply({ value: encoded.value, error: false }, ctx);
  assert.equal(stored[0].stream instanceof WritableStream, false);
  value[Symbol.dispose]();
  await Promise.all(jobs);
  assert.equal(aborted, 1);
});

test("a refused writable export restores the sender before any sink effect", async () => {
  const codec = make();
  const { ctx, jobs } = context();
  const stream = new WritableStream({
    write() {
      assert.fail("refused writer was used");
    },
  });
  ctx._call = async () => {
    throw new Error("native export refused");
  };
  await assert.rejects(codec.result(stream, ctx), /native export refused/);
  assert.equal(stream.locked, false);
  await Promise.all(jobs);
});
