import assert from "node:assert/strict";
import test from "node:test";
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import { configureTextEncoding } from "@bufbuild/protobuf/wire";
import { createCodec } from "../../packages/cellar-sdk/rpc/codec.js";
import {
  CountSchema,
  IncrementRequestSchema,
  IncrementResponseSchema,
} from "./fixtures/generated/typescript/counter_pb.ts";
import { AllTypesSchema } from "./fixtures/profile-generated/javascript/profile_pb.js";

const { encodeMessage, decodeMessage } = createCodec({ create, fromBinary, toBinary, configureTextEncoding });

test("generated messages preserve optional presence and empty responses", () => {
  const missing = create(IncrementRequestSchema);
  const zero = create(IncrementRequestSchema, { delta: 0n });

  assert.deepEqual(toBinary(IncrementRequestSchema, missing), new Uint8Array());
  assert.deepEqual(toBinary(IncrementRequestSchema, zero), new Uint8Array([8, 0]));
  assert.equal(fromBinary(IncrementRequestSchema, new Uint8Array()).delta, undefined);
  assert.equal(fromBinary(IncrementRequestSchema, new Uint8Array([8, 0])).delta, 0n);

  const conflict = create(IncrementResponseSchema, {
    result: { case: "operationConflict", value: {} },
  });
  const wire = toBinary(IncrementResponseSchema, conflict);
  assert.deepEqual(wire, new Uint8Array([26, 0]));
  assert.equal(fromBinary(IncrementResponseSchema, wire).result.case, "operationConflict");
});

test("signed 64-bit values survive binary encoding without number conversion", () => {
  for (const value of [0n, 1n, -1n, 9007199254740993n, -(1n << 63n), (1n << 63n) - 1n]) {
    const count = create(CountSchema, { value });
    const wire = toBinary(CountSchema, count);
    assert.equal(fromBinary(CountSchema, wire).value, value);
  }

  const request = create(IncrementRequestSchema, { delta: 1n, operationId: "id" });
  assert.equal(Buffer.from(toBinary(IncrementRequestSchema, request)).toString("hex"), "080112026964");
});

test("the RPC input guard refuses lossy Unicode before message encoding", () => {
  const input = create(IncrementRequestSchema, { operationId: "\ud800" });
  assert.throws(() => encodeMessage(IncrementRequestSchema, input), { code: "InvalidArgument", outcome: "not_committed" });
});

test("the message codec rejects malformed UTF-8 and truncated fields", () => {
  assert.throws(() => fromBinary(IncrementRequestSchema, new Uint8Array([18, 1, 255])));
  assert.throws(() => fromBinary(IncrementRequestSchema, new Uint8Array([8])));
});

test("input graph budgets reject cycles, nesting, size and shared bytes", () => {
  for (const input of [null, undefined, 1, "request", [], new Uint8Array()]) {
    assert.throws(() => encodeMessage(IncrementRequestSchema, input), { code: "InvalidArgument" });
  }
  const cycle = {};
  cycle.children = [{recursive:cycle}];
  assert.throws(() => encodeMessage(AllTypesSchema, cycle), { code: "InvalidArgument" });
  assert.throws(() => encodeMessage(AllTypesSchema, {children:[{recursive:{}}]}, {maxDepth:1}), {code:"ResourceExhausted"});
  assert.throws(() => encodeMessage(AllTypesSchema, {packed:[1,2,3]}, {maxElements:3}), {code:"ResourceExhausted"});
  assert.throws(() => encodeMessage(AllTypesSchema, {data:new Uint8Array(129)}, {maxMessageBytes:128}), {code:"ResourceExhausted"});
  assert.throws(() => encodeMessage(AllTypesSchema, {data:new Uint8Array(new SharedArrayBuffer(8))}), {code:"InvalidArgument"});
  encodeMessage(IncrementRequestSchema, {operationId:"😀",delta:9007199254740993n});
});

test("Cellar's codec preserves BOMs and exact Unicode across the RPC boundary", () => {
  for (const operationId of ["\ufeffhello", "😀", "\u0000hello"]) {
    const bytes = encodeMessage(IncrementRequestSchema, { delta: 1n, operationId });
    const reply = decodeMessage(IncrementRequestSchema, bytes);
    assert.equal(reply.operationId, operationId);
    assert.equal(reply.delta, 1n);
  }
  assert.throws(() => encodeMessage(IncrementRequestSchema, { operationId: "\ud800" }), {
    code: "InvalidArgument", outcome: "not_committed",
  });
  assert.throws(() => decodeMessage(IncrementRequestSchema, new Uint8Array([8])), {
    code: "InvalidResponse", outcome: "unknown",
  });
});
