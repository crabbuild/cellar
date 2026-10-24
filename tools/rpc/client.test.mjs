import assert from "node:assert/strict";
import test from "node:test";
import { readFileSync } from "node:fs";
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import { configureTextEncoding } from "@bufbuild/protobuf/wire";
import { createCodec, createMethod, createRegistration, RpcClient } from "../../packages/cellar-sdk/rpc/index.js";
import {
  IncrementRequestSchema, IncrementResponseSchema,
  GetCountRequestSchema, GetCountResponseSchema,
} from "./fixtures/generated/typescript/counter_pb.ts";

const codec = createCodec({ create, fromBinary, toBinary, configureTextEncoding });
const contract = JSON.parse(readFileSync(new URL("./fixtures/generated/rpc.json", import.meta.url)));
const increment = createMethod({
  method:"example.counter.v1.Counter/Increment", digest:contract.digest, handler:"increment",
  inputSchema:IncrementRequestSchema, outputSchema:IncrementResponseSchema,
}, codec);
const getCount = createMethod({
  method:"example.counter.v1.Counter/GetCount", digest:contract.digest, handler:"getCount",
  inputSchema:GetCountRequestSchema, outputSchema:GetCountResponseSchema,
}, codec);

function context(reply, expected = contract) {
  const calls = [];
  let active = true;
  return {
    calls,
    expire: () => { active = false; },
    [Symbol.for("cellar.rpc")]: {
      check() { if (!active) throw Error("ContextExpired"); },
      verify(method) {
        method.verify(expected);
        return {maxMessageBytes:expected.limits.max_message_bytes,
          maxDepth:expected.limits.max_depth,maxElements:expected.limits.max_elements,
          maxDecodedSlots:expected.limits.max_decoded_slots};
      },
      async call(metadata, body) { calls.push({metadata, body}); return reply; },
    },
  };
}

test("client submits exact binary method identity and preserves i64 business replies", async () => {
  const value = 9007199254740993n;
  const ctx = context(codec.encodeMessage(IncrementResponseSchema, {result:{case:"count", value:{value}}}));
  const client = new RpcClient(ctx, "COUNTERS", "alice");
  const result = await client.call(increment, {delta:1n, operationId:"id"}, {timeoutMs:17});
  assert.equal(result.result.value.value, value);
  assert.deepEqual(ctx.calls[0].body, Uint8Array.of(8,1,18,2,105,100));
  assert.deepEqual(ctx.calls[0].metadata, {binding:"COUNTERS", name:"alice",
    method:increment.method, contract_digest:increment.digest, timeout_ms:17});
});

test("empty messages remain present attachments", async () => {
  const ctx = context(new Uint8Array());
  const client = new RpcClient(ctx, "COUNTERS", "");
  await client.call(getCount, {});
  assert.deepEqual(ctx.calls[0].body, new Uint8Array());
});

test("an already aborted native signal refuses before encoding or submission", async () => {
  const ctx = context(new Uint8Array());
  const controller = new AbortController();
  controller.abort("private reason must remain local");
  const request = {get operationId() { throw Error("encoder must not run"); }};
  await assert.rejects(new RpcClient(ctx, "COUNTERS", "alice").call(increment, request,
    {signal:controller.signal}), {code:"Cancelled", outcome:"not_committed"});
  assert.equal(ctx.calls.length, 0);
});

test("call controls are captured once before validation and encoding", async () => {
  const ctx = context(new Uint8Array());
  const controller = new AbortController();
  let timeouts = 0, signals = 0;
  await new RpcClient(ctx, "COUNTERS", "alice").call(getCount, {}, {
    get timeoutMs() { timeouts++; return 17; },
    get signal() { signals++; return controller.signal; },
  });
  assert.equal(timeouts, 1);
  assert.equal(signals, 1);
  assert.equal(ctx.calls[0].metadata.timeout_ms, 17);
});

test("cancellation during encoding is refused before native submission", async () => {
  const ctx = context(new Uint8Array());
  const controller = new AbortController();
  let encoded = 0;
  const method = createMethod({method:getCount.method, digest:contract.digest, handler:"getCount",
    inputSchema:GetCountRequestSchema, outputSchema:GetCountResponseSchema}, {
    encodeMessage() { encoded++; controller.abort(); return new Uint8Array(); },
    decodeMessage() { throw Error("must not decode"); },
  });
  await assert.rejects(new RpcClient(ctx, "COUNTERS", "alice").call(method, {},
    {signal:controller.signal}), {code:"Cancelled", outcome:"not_committed"});
  assert.equal(encoded, 1);
  assert.equal(ctx.calls.length, 0);
});

test("invalid inputs, unsupported options and expired clients never submit", async () => {
  const ctx = context(new Uint8Array());
  const client = new RpcClient(ctx, "COUNTERS", "alice");
  await assert.rejects(client.call(getCount, null), {code:"InvalidArgument", outcome:"not_committed"});
  for (const options of [null, {timeoutMs:-1}, {timeoutMs:2**32}, {signal:{}}, {timeoutMs:NaN}]) {
    await assert.rejects(client.call(getCount, {}, options), {code:"InvalidArgument", outcome:"not_committed"});
  }
  ctx.expire();
  await assert.rejects(client.call(getCount, {}), {code:"ContextExpired", outcome:"not_committed"});
  assert.equal(ctx.calls.length, 0);
});

test("malformed replies and wrong handler response types retain unknown effects", async () => {
  const client = new RpcClient(context(Uint8Array.of(10,255)), "COUNTERS", "alice");
  await assert.rejects(client.call(getCount, {}), {code:"InvalidResponse", outcome:"unknown"});
  assert.throws(() => getCount.encode(create(IncrementResponseSchema)), {code:"InvalidResponse", outcome:"unknown"});
});

test("business rejections remain response values", async () => {
  const ctx = context(codec.encodeMessage(IncrementResponseSchema, {result:{case:"operationConflict", value:{}}}));
  const result = await new RpcClient(ctx, "COUNTERS", "alice").call(increment, {delta:1n});
  assert.equal(result.result.case, "operationConflict");
});

test("registration refuses duplicate methods and freezes the explicit table", () => {
  const registration = createRegistration([increment, getCount]);
  assert.equal(Object.keys(registration.methods).length, 2);
  assert.equal(Object.getPrototypeOf(registration.methods), null);
  assert(Object.isFrozen(registration.methods));
  assert.throws(() => createRegistration([increment, increment]), TypeError);
  assert.throws(() => createRegistration([]), TypeError);
});

test("client encoding uses the limits registered in its native contract", async () => {
  const expected = {...contract,limits:{...contract.limits,max_message_bytes:128}};
  const ctx = context(new Uint8Array(),expected);
  await assert.rejects(new RpcClient(ctx,"COUNTERS","alice").call(increment,
    {delta:1n,operationId:"k".repeat(128)}),{code:"ResourceExhausted",outcome:"not_committed"});
  assert.equal(ctx.calls.length,0);
});

test("client default object layouts use the native decoded-slot budget", async () => {
  const expected = {...contract,limits:{...contract.limits,max_decoded_slots:4}};
  const ctx = context(new Uint8Array(),expected);
  await assert.rejects(new RpcClient(ctx,"COUNTERS","alice").call(increment,
    {delta:1n,operationId:"id"}),{code:"ResourceExhausted",outcome:"not_committed"});
  assert.equal(ctx.calls.length,0);
});
