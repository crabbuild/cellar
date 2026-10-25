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
const make = (0, eval)(`(${source})`);

function context() {
  const calls = [],
    jobs = [];
  const ctx = {
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
