import assert from "node:assert/strict";
import test from "node:test";
import { testBudget } from "./test-budgets.mjs";
import { readFileSync } from "node:fs";
import { create, fromBinary } from "@bufbuild/protobuf";
import { CodeGeneratorRequestSchema, FileDescriptorSetSchema } from "@bufbuild/protobuf/wkt";
import { plugin } from "./protoc-gen-cellar.mjs";
import { CounterClient } from "./fixtures/generated/javascript/counter_cellar.js";
import { CounterRegistration } from "./fixtures/generated/javascript/counter_cellar.js";
import { GetCountResponseSchema } from "./fixtures/generated/javascript/counter_pb.js";

const bytes = readFileSync(new URL("./fixtures/counter.binpb", import.meta.url));
function request(target = "ts") {
  return create(CodeGeneratorRequestSchema, {
    fileToGenerate:["counter.proto"],
    protoFile:fromBinary(FileDescriptorSetSchema, bytes).file,
    parameter:`target=${target},import_extension=js,digest=${"a".repeat(64)}`,
  });
}

test("the service plugin generates concrete TS and JS declaration APIs", () => {
  const ts = plugin.run(request("ts")).file;
  assert.equal(ts.length, 1);
  assert.match(ts[0].content, /MessageInitShape<typeof IncrementRequestSchema>/);
  assert.match(ts[0].content, /Promise<IncrementResponse>/);
  const js = plugin.run(request("js+dts")).file;
  assert.deepEqual(js.map(file => file.name).sort(), ["counter_cellar.d.ts", "counter_cellar.js"]);
  const declarations = js.find(file => file.name.endsWith(".d.ts")).content;
  assert.match(declarations, /CounterRegistration: RpcRegistration/);
  assert.doesNotMatch(declarations, /\bany\b/);
});

test("unsupported streaming and missing/invalid digest fail generation", () => {
  const streaming = request();
  streaming.protoFile[0].service[0].method[0].serverStreaming = true;
  assert.throws(() => plugin.run(streaming), /unary/);
  for (const parameter of ["target=ts", "target=ts,digest=invalid"]) {
    const invalid = request();
    invalid.parameter = parameter;
    assert.throws(() => plugin.run(invalid), /digest/);
  }
});

test("the generated plain-JS client and server registration execute actual codecs", async () => {
  const method = CounterRegistration.methods["example.counter.v1.Counter/GetCount"];
  let calls = 0;
  const context = {[Symbol.for("cellar.rpc")]: {
    check() {},
    budget: () => testBudget(),
    verify(method) { method.verify(JSON.parse(readFileSync(new URL("./fixtures/generated/rpc.json",import.meta.url)))); },
    async call(metadata, bytes) {
      calls++;
      assert.equal(metadata.method, method.method);
      assert.equal(bytes.byteLength, 0);
      return Uint8Array.of(10,2,8,42);
    },
  }};
  const result = await CounterClient.bind(context, "COUNTERS", "alice").getCount({});
  assert.equal(result.count.value, 42n);
  assert.equal(result.$typeName, GetCountResponseSchema.typeName);
  assert.equal(calls, 1);
  assert.deepEqual(method.encode(result), Uint8Array.of(10,2,8,42));
});
