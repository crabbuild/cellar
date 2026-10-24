import assert from "node:assert/strict";
import test from "node:test";
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import { configureTextEncoding } from "@bufbuild/protobuf/wire";
import { createCodec } from "../../packages/cellar-sdk/rpc/codec.js";
import { AllTypesSchema } from "./fixtures/profile-generated/javascript/profile_pb.js";
import { ProfileRegistration } from "./fixtures/profile-generated/javascript/profile_cellar.js";

const codec = createCodec({create, fromBinary, toBinary, configureTextEncoding});

test("the portable profile preserves scalar boundaries, presence and maps", () => {
  const input = {
    signed32: -(2 ** 31), unsigned32: 2 ** 32 - 1, zigzag32: -(2 ** 31),
    fixedUnsigned32: 2 ** 32 - 1, fixedSigned32: -(2 ** 31),
    signed64: -(1n << 63n), unsigned64: (1n << 64n) - 1n,
    zigzag64: -(1n << 63n), fixedUnsigned64: (1n << 64n) - 1n,
    fixedSigned64: -(1n << 63n),
    float32: Infinity, float64: -Infinity, enabled: true,
    text: "\ufeff😀\0", data: Uint8Array.of(0, 255, 128), state: 777,
    present: 0n, choice: {case:"selectedText", value:""},
    packed: [-(2 ** 31), -1, 0, 2 ** 31 - 1],
    unpacked: [-(2 ** 31), -1, 0, 2 ** 31 - 1],
    byName: {"\ufeff😀": {name:"nested"}},
    byID: {"-9223372036854775808": true, "9223372036854775807": false},
    timestamp: {seconds:-1n, nanos:1}, duration: {seconds:-1n, nanos:-1},
  };
  const reply = codec.decodeMessage(AllTypesSchema, codec.encodeMessage(AllTypesSchema, input));
  assert.deepEqual(reply, create(AllTypesSchema, input));
});

test("non-finite protobuf floats and nested messages keep their actual values", () => {
  const input = {
    float32: NaN, float64: NaN,
    children: [{name:"first"}, {recursive:{text:"nested"}}],
    choice: {case:"selectedChild", value:{name:"choice"}},
  };
  const reply = codec.decodeMessage(AllTypesSchema, codec.encodeMessage(AllTypesSchema, input));
  assert.deepEqual(reply, create(AllTypesSchema, input));
});

test("generated services handle Empty and imported well-known message schemas", () => {
  const ping = ProfileRegistration.methods["cellar.rpc.conformance.v1.Profile/Ping"];
  assert.equal(ping.input, "google.protobuf.Empty");
  assert.deepEqual(ping.encodeRequest({}), new Uint8Array());
  assert.equal(ping.decodeResponse(new Uint8Array()).$typeName, "google.protobuf.Empty");
});
