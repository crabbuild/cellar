import assert from "node:assert/strict";
import test from "node:test";
import { readFileSync } from "node:fs";
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import { configureTextEncoding } from "@bufbuild/protobuf/wire";
import { Int64ValueSchema, StructSchema, ValueSchema } from "@bufbuild/protobuf/wkt";
import { createCodec } from "../../packages/cellar-sdk/rpc/codec.js";
import { prepareMessage } from "../../packages/cellar-sdk/rpc/message.js";
import { AllTypesSchema, RuntimeMappingsSchema } from "./fixtures/profile-generated/javascript/profile_pb.js";
import { IncrementRequestSchema, IncrementResponseSchema } from "./fixtures/generated/javascript/counter_pb.js";

const runtime = {create, fromBinary, toBinary, configureTextEncoding};
const codec = createCodec(runtime);

function checked(schema, input, limits) {
  const planned = prepareMessage(schema, input, limits);
  const actual = codec.encodeMessage(schema, input, limits);
  assert.equal(actual.byteLength, planned.size);
  return actual;
}

test("plain JS types and field names are checked before any codec allocation", () => {
  let calls = 0;
  const guarded = createCodec({...runtime,
    create() {calls++; throw Error("must not allocate");},
    toBinary() {calls++; throw Error("must not allocate");},
  });
  for (const input of [
    {delta:0}, {delta:9007199254740993}, {delta:"1"}, {delta:null},
    {delta:1n, operationID:"typo"}, {operationId:17}, {operationId:false},
    {$typeName:"other.Request", delta:1n},
  ]) {
    assert.throws(() => guarded.encodeMessage(IncrementRequestSchema, input), {code:"InvalidArgument",outcome:"not_committed"});
  }
  assert.equal(calls, 0);
});

test("every signed/unsigned scalar boundary is exact and out-of-range is refused", () => {
  const fields = [
    ["signed32", -(2**31), 2**31-1], ["unsigned32", 0, 2**32-1],
    ["zigzag32", -(2**31), 2**31-1], ["fixedUnsigned32", 0, 2**32-1],
    ["fixedSigned32", -(2**31), 2**31-1],
    ["signed64", -(1n<<63n), (1n<<63n)-1n], ["unsigned64", 0n, (1n<<64n)-1n],
    ["zigzag64", -(1n<<63n), (1n<<63n)-1n], ["fixedUnsigned64", 0n, (1n<<64n)-1n],
    ["fixedSigned64", -(1n<<63n), (1n<<63n)-1n], ["state", -(2**31), 2**31-1],
  ];
  for (const [name, minimum, maximum] of fields) {
    const step = typeof minimum === "bigint" ? 1n : 1;
    for (const value of [minimum, maximum]) {
      const reply = codec.decodeMessage(AllTypesSchema, checked(AllTypesSchema, {[name]:value}));
      assert.equal(reply[name], value);
    }
    for (const value of [minimum-step, maximum+step, "0", null]) {
      assert.throws(() => codec.encodeMessage(AllTypesSchema, {[name]:value}), {code:"InvalidArgument"}, name);
    }
  }
  for (const input of [{signed32:1.5}, {state:NaN}, {enabled:1}, {float64:"3"}, {float32:3.5e38}]) {
    assert.throws(() => codec.encodeMessage(AllTypesSchema, input), {code:"InvalidArgument"});
  }
});

test("float signs, non-finite values, optional zero and selected defaults are sized correctly", () => {
  for (const input of [
    {}, {float32:-0,float64:-0}, {float32:NaN,float64:NaN},
    {float32:Infinity,float64:-Infinity}, {float32:1e-50}, {present:0n},
    {choice:{case:"selectedText",value:""}}, {choice:{case:undefined}},
  ]) checked(AllTypesSchema, input);
  assert.equal(prepareMessage(AllTypesSchema, {present:0n}).size, 3);
  assert(Object.is(codec.decodeMessage(AllTypesSchema, checked(AllTypesSchema, {float32:-0})).float32, -0));
});

test("maps validate canonical keys, message values and their synthetic wire costs", () => {
  for (const input of [
    {byName:{"":{}}}, {byName:{"\ufeff😀":{name:"child"}}},
    {byID:{"-9223372036854775808":true,"9223372036854775807":false}},
    {byName:Object.fromEntries([["__proto__", {name:"safe"}]])},
  ]) checked(AllTypesSchema, input);
  for (const input of [
    {byID:{"01":true}}, {byID:{"-0":true}}, {byID:{"1e2":true}},
    {byID:{"9223372036854775808":true}}, {byID:{["9".repeat(100_000)]:true}},
    {byID:{"1":1}}, {byName:{bad:undefined}}, {byName:{"\ud800":{}}},
  ]) assert.throws(() => codec.encodeMessage(AllTypesSchema,input), {code:"InvalidArgument"});
  checked(AllTypesSchema, {byName:{x:{}}}, {maxDepth:2,maxElements:3});
  assert.throws(() => codec.encodeMessage(AllTypesSchema,{byName:{x:{}}},{maxDepth:1}),{code:"ResourceExhausted"});
  assert.throws(() => codec.encodeMessage(AllTypesSchema,{byName:{x:{}}},{maxElements:2}),{code:"ResourceExhausted"});
});

test("oneofs reject unknown cases, absent selected values and foreign nested messages", () => {
  for (const input of [
    {choice:{case:"typo",value:"x"}}, {choice:{case:"selectedText"}},
    {choice:{case:undefined,value:"ignored"}}, {choice:{case:"selectedText",value:1}},
    {selectedText:"wrong shape"}, {choice:{case:"selectedChild",value:{$typeName:"other.Child"}}},
    {choice:{case:"selectedText",value:"x",extra:true}},
  ]) assert.throws(() => codec.encodeMessage(AllTypesSchema,input), {code:"InvalidArgument"});
  checked(IncrementResponseSchema, {result:{case:"operationConflict",value:{}}});
});

test("wire budgets count repeated packed elements and map/message depth", () => {
  checked(AllTypesSchema, {packed:[-1,0,1]}, {maxElements:4});
  checked(AllTypesSchema, {unpacked:[-1,0,1]}, {maxElements:3});
  assert.throws(() => codec.encodeMessage(AllTypesSchema,{packed:[-1,0,1]},{maxElements:3}), {code:"ResourceExhausted"});
  const cycle = {};
  cycle.children = [{recursive:cycle}];
  assert.throws(() => codec.encodeMessage(AllTypesSchema,cycle), {code:"InvalidArgument"});
  const holes = new Array(3);
  assert.throws(() => codec.encodeMessage(AllTypesSchema,{packed:holes}), {code:"InvalidArgument"});
  const extended = [1]; extended.extra = 2;
  assert.throws(() => codec.encodeMessage(AllTypesSchema,{packed:extended}), {code:"InvalidArgument"});
});

test("exact wire size refuses overhead amplification before create or toBinary", () => {
  let calls = 0;
  const guarded = createCodec({...runtime,create() {calls++;throw Error("must not allocate");}});
  const inputs = [
    {data:new Uint8Array(128)}, // Raw bytes fit; the tag/length do not.
    {byName:{["k".repeat(125)]:{}}}, // Map key and synthetic message overhead.
    {unpacked:Array(13).fill(-1)}, // Negative int32 values consume ten-byte varints.
    {packed:Array(128).fill(1)},
  ];
  for (const input of inputs) {
    assert.throws(() => guarded.encodeMessage(AllTypesSchema,input,{maxMessageBytes:128}), {code:"ResourceExhausted"});
  }
  assert.equal(calls, 0);
});

test("getters never execute and input containers cannot alter the encoding pass", () => {
  let reads = 0;
  const input = {get delta() {reads++; return 1n;}};
  assert.throws(() => codec.encodeMessage(IncrementRequestSchema,input),{code:"InvalidArgument"});
  assert.equal(reads,0);
  const prepared = prepareMessage(AllTypesSchema,{children:[{name:"saved"}]});
  const original = {children:[{name:"original"}]};
  const own = prepareMessage(AllTypesSchema,original);
  original.children[0].name = "changed";
  assert.equal(own.input.children[0].name,"original");
  assert.equal(prepared.input.children[0].name,"saved");
});

test("bytes use exact private slices and reject shared, resizable and detached storage", () => {
  const source = Uint8Array.of(9,0,255,128,8);
  const slice = source.subarray(1,4);
  Object.defineProperty(slice,"byteLength",{value:9000});
  const reply = codec.decodeMessage(AllTypesSchema,checked(AllTypesSchema,{data:slice}));
  assert.deepEqual(reply.data,Uint8Array.of(0,255,128));
  for (const view of [
    new Uint8Array(new SharedArrayBuffer(8)),
    new Uint8Array(new ArrayBuffer(8,{maxByteLength:16})),
  ]) assert.throws(() => codec.encodeMessage(AllTypesSchema,{data:view}),{code:"InvalidArgument"});
  const detached = new Uint8Array(8);
  structuredClone(detached.buffer,{transfer:[detached.buffer]});
  assert.throws(() => codec.encodeMessage(AllTypesSchema,{data:detached}),{code:"InvalidArgument"});
});

test("unknown wire fields are accepted and discarded without forwarding caller metadata", () => {
  const decoded = codec.decodeMessage(IncrementRequestSchema, Uint8Array.of(0x7a,1,42));
  assert.equal(decoded.$unknown,undefined);
  assert.deepEqual(codec.encodeMessage(IncrementRequestSchema,decoded),new Uint8Array());
  assert.deepEqual(codec.encodeMessage(IncrementRequestSchema,{$unknown:[{no:1,wireType:2,data:new Uint8Array(2048)}]}, {maxMessageBytes:1}),new Uint8Array());
});

test("well-known scalar wrappers keep presence and their field-specific representations", () => {
  for (const input of [
    {wrapped64:0n,wrappedBytes:new Uint8Array(),wrappedDouble:-0},
    {wrapped64:-(1n<<63n),wrappedBytes:Uint8Array.of(0,255),wrappedDouble:NaN},
    {wrappedFloat:Infinity,wrappedUnsigned64:(1n<<64n)-1n},
    {wrapped32:-(2**31),wrappedUnsigned32:2**32-1,wrappedBool:false,wrappedString:""},
    {wrapperList:[{}, {value:1n}],wrapperMap:{false:{},true:{value:-1n}}},
    {choice:{case:"wrappedChoice",value:{value:0n}}},
  ]) checked(RuntimeMappingsSchema,input);

  // A present scalar wrapper's default value is an empty message, not an
  // absent field. Its inner value costs an element only when serialized.
  assert.deepEqual(checked(RuntimeMappingsSchema,{wrapped64:0n},{maxDepth:1,maxElements:1}),Uint8Array.of(10,0));
  checked(RuntimeMappingsSchema,{wrapped64:1n},{maxElements:2});
  assert.throws(() => codec.encodeMessage(RuntimeMappingsSchema,{wrapped64:1n},{maxElements:1}),{code:"ResourceExhausted"});
  checked(Int64ValueSchema,{value:1n});
  for (const input of [
    {wrapped64:{value:1n}}, {wrapped64:1}, {wrappedBytes:[]},
    {wrapperList:[1n]}, {wrapperMap:{true:1n}},
    {wrapperMap:{TRUE:{}}}, {choice:{case:"wrappedChoice",value:1n}},
  ]) assert.throws(() => codec.encodeMessage(RuntimeMappingsSchema,input),{code:"InvalidArgument"});
});

test("Struct fields snapshot JSON values and measure the expanded wire graph", () => {
  for (const input of [
    {object:{}},
    {object:{text:"\ufeff😀",nil:null,enabled:false,number:-0}},
    {object:{nested:{list:[1,"two",null,{three:true}]}}},
    {objects:[{one:1},{}],objectsByName:{x:{two:2}}},
    {choice:{case:"objectChoice",value:{nested:[true,false]}}},
    {explicitValue:{kind:{case:"structValue",value:{fields:{x:{kind:{case:"numberValue",value:1}}}}}}},
    {explicitList:{values:[{kind:{case:"nullValue",value:0}}]}},
  ]) checked(RuntimeMappingsSchema,input);
  checked(StructSchema,{fields:{x:{kind:{case:"numberValue",value:7}}}});
  checked(ValueSchema,{kind:{case:"listValue",value:{values:[]}}});
  checked(RuntimeMappingsSchema,{object:{x:7}},{maxDepth:3,maxElements:5});
  checked(RuntimeMappingsSchema,{object:{x:[7]}},{maxDepth:5,maxElements:7});
  for (const limits of [{maxDepth:2},{maxElements:4}]) {
    assert.throws(() => codec.encodeMessage(RuntimeMappingsSchema,{object:{x:7}},limits),{code:"ResourceExhausted"});
  }
  for (const limits of [{maxDepth:4},{maxElements:6}]) {
    assert.throws(() => codec.encodeMessage(RuntimeMappingsSchema,{object:{x:[7]}},limits),{code:"ResourceExhausted"});
  }
  for (const input of [
    {object:[]}, {object:{x:undefined}}, {object:{x:1n}},
    {object:{x:new Date()}}, {object:{"\ud800":"bad"}},
  ]) assert.throws(() => codec.encodeMessage(RuntimeMappingsSchema,input),{code:"InvalidArgument"});
  const cycle = {}; cycle.self = cycle;
  assert.throws(() => codec.encodeMessage(RuntimeMappingsSchema,{object:cycle}),{code:"InvalidArgument"});
});

test("JS wire sizes and graph budgets agree with the shared Rust/Python vectors", () => {
  const inputs = {
    "present-wrapper-zero": {wrapped64:0n},
    "wrapper-map": {wrapperMap:{true:{value:1n}}},
    "struct-number": {object:{x:7}},
  };
  const vectors = JSON.parse(readFileSync(new URL("./fixtures/message-vectors.json",import.meta.url),"utf8"));
  assert.equal(vectors.length,Object.keys(inputs).length);
  for (const vector of vectors) {
    const limits = {maxDepth:vector.max_depth,maxElements:vector.max_elements,maxDecodedSlots:vector.max_decoded_slots};
    const input = inputs[vector.name];
    assert(input, vector.name);
    assert.deepEqual(checked(RuntimeMappingsSchema,input,limits),Uint8Array.from(vector.bytes));
    assert.throws(() => codec.encodeMessage(RuntimeMappingsSchema,input,{...limits,maxDecodedSlots:limits.maxDecodedSlots-1}),{code:"ResourceExhausted"});
    if (limits.maxElements > 1) {
      assert.throws(() => codec.encodeMessage(RuntimeMappingsSchema,input,{...limits,maxElements:limits.maxElements-1}),{code:"ResourceExhausted"});
    }
    if (limits.maxDepth > 1) {
      assert.throws(() => codec.encodeMessage(RuntimeMappingsSchema,input,{...limits,maxDepth:limits.maxDepth-1}),{code:"ResourceExhausted"});
    }
  }
});

test("valid Rust map defaults decode using incoming wire costs, not JS re-encoding costs", () => {
  // Prost encodes RuntimeMappings.wrapper_map[false] = Int64Value(0) as an
  // empty synthetic entry. Native preflight sees one occurrence and depth 1.
  const reply = codec.decodeMessage(RuntimeMappingsSchema,Uint8Array.of(42,0),{maxDepth:1,maxElements:1});
  assert.equal(reply.wrapperMap.false.value,0n);
  // Protobuf-ES writes the default key and value explicitly, which legitimately
  // needs a larger outgoing budget. Decode must not apply that different cost.
  assert.throws(() => codec.encodeMessage(RuntimeMappingsSchema,reply,{maxDepth:1,maxElements:1}),{code:"ResourceExhausted"});
  checked(RuntimeMappingsSchema,reply);
});

test("default field layouts are bounded before allocating message constructors", () => {
  let created = 0;
  const guarded = createCodec({...runtime,create() {created++;throw Error("must not allocate");}});
  const input = {children:Array.from({length:256},() => ({}))};
  assert.throws(() => guarded.encodeMessage(AllTypesSchema,input,{maxDecodedSlots:1050}),{code:"ResourceExhausted",outcome:"not_committed"});
  assert.equal(created,0);
  checked(AllTypesSchema,input,{maxDecodedSlots:1051});
});
