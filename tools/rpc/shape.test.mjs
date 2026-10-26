// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import assert from "node:assert/strict";
import test from "node:test";
import { testBudget, testBudgetControl } from "./test-budgets.mjs";
import { testRunner } from "./test-runner.mjs";
import { readFileSync } from "node:fs";
import { createFileRegistry, fromBinary } from "@bufbuild/protobuf";
import { FileDescriptorSetSchema } from "@bufbuild/protobuf/wkt";
import { verifyCodecs } from "../../packages/cellar-sdk/rpc/shape.js";
import { createMethod, RpcClient } from "../../packages/cellar-sdk/rpc/index.js";
import { CounterClient } from "./fixtures/generated/javascript/counter_cellar.js";

function fixture(name = "profile", mutate = () => {}) {
  const directory = name === "profile" ? "profile-generated" : "generated";
  const contract = JSON.parse(readFileSync(new URL(`./fixtures/${directory}/rpc.json`,import.meta.url)));
  const image = fromBinary(FileDescriptorSetSchema,readFileSync(new URL(`./fixtures/${name}.binpb`,import.meta.url)));
  const file = image.file.find(file => file.package === (name === "profile" ? "cellar.rpc.conformance.v1" : "example.counter.v1"));
  mutate(file);
  return {contract,registry:createFileRegistry(image)};
}

test("live codec shapes match all native roots, including recursive maps, wrappers and Struct", () => {
  const {contract,registry} = fixture();
  for (const method of contract.methods) {
    verifyCodecs(registry.getMessage(method.input),registry.getMessage(method.output),contract.types);
  }
  const schema = registry.getMessage("cellar.rpc.conformance.v1.AllTypes");
  assert(Object.isFrozen(schema));
  assert.throws(() => {schema.fields[0].number = 99;},TypeError);
  assert.throws(() => schema.fields.pop(),TypeError);
  assert(Object.isFrozen(schema.fields[0].proto));
});

test("copied names and digest cannot hide stale scalar fields or optional presence", () => {
  for (const mutate of [
    field => {field.number = 99;}, field => {field.name = "renamed";},
    field => {field.type = 4;},
    field => {field.proto3Optional = false; delete field.oneofIndex;},
  ]) {
    const {contract,registry} = fixture("counter",file => {
      mutate(file.messageType.find(message => message.name === "IncrementRequest").field[0]);
      if (!file.messageType[0].field[0].proto3Optional) file.messageType[0].oneofDecl = [];
    });
    const schema = registry.getMessage("example.counter.v1.IncrementRequest");
    assert.throws(() => verifyCodecs(schema,schema,contract.types),TypeError);
  }
});

test("actual packing, cardinality, map keys, enum values and oneof membership are checked", () => {
  const mutations = [
    message => {message.field.find(field => field.name === "packed").options = {packed:false};},
    message => {message.field.find(field => field.name === "unpacked").label = 1;},
    message => {message.nestedType.find(type => type.options?.mapEntry).field[0].type = 5;},
    message => {message.enumType[0].value[1].number = 77;},
    message => {message.field.find(field => field.name === "selected_text").oneofIndex = undefined;},
  ];
  for (const mutate of mutations) {
    const {contract,registry} = fixture("profile",file => mutate(file.messageType[0]));
    const schema = registry.getMessage("cellar.rpc.conformance.v1.AllTypes");
    assert.throws(() => verifyCodecs(schema,schema,contract.types),TypeError);
  }
});

test("mutated reflection members and referenced descriptors cannot bypass a cached match", () => {
  for (const mutate of [
    schema => {schema.members = [];},
    schema => {schema.fields.find(field => field.name === "timestamp").message = schema;},
    schema => {schema.fields.find(field => field.name === "signed64").longAsString = true;},
  ]) {
    const {contract,registry} = fixture();
    const schema = registry.getMessage("cellar.rpc.conformance.v1.AllTypes");
    mutate(schema);
    assert.throws(() => verifyCodecs(schema,schema,contract.types),TypeError);
  }
});

test("a stale client fails before its allocating encoder or transport is called", async () => {
  const {contract,registry} = fixture("counter",file => {file.messageType[0].field[0].type = 4;});
  let encoded = 0, submitted = 0;
  const rule = createMethod({method:contract.methods[1].name,digest:contract.digest,handler:"increment",
    inputSchema:registry.getMessage(contract.methods[1].input),outputSchema:registry.getMessage(contract.methods[1].output)}, {
    encodeMessage() {encoded++;throw Error("must not encode");},
    decodeMessage() {throw Error("must not decode");},
  });
  const client = new RpcClient({[Symbol.for("cellar.rpc")]:{
    check() {}, budget: () => testBudget(), verify(method) {method.verify(contract);},
    async call() {submitted++;},
  }},"COUNTERS","alice");
  await assert.rejects(client.call(rule,{delta:1n}),{code:"ContractMismatch",outcome:"not_committed"});
  assert.equal(encoded,0);
  assert.equal(submitted,0);
});

test("the real runner supplies codec identity and limits to a consumer-only app", async () => {
  const {contract} = fixture("counter");
  let submitted = 0;
  const runner = testRunner((id,name,payload,body) => {
    assert.equal(id,1);
    if (name === "$check") return;
    if (name === "$rpc_budget") return testBudgetControl();
    assert.equal(name,"cell.call");
    assert.equal(JSON.parse(payload).method,"example.counter.v1.Counter/GetCount");
    assert.equal(body.length,0);
    submitted++;
    return [Promise.resolve([JSON.stringify({ok:true,value:null}),Uint8Array.of(10,2,8,42)]), () => {}];
  })({async fetch(_request,ctx) {
    const response = await CounterClient.bind(ctx,"COUNTERS","alice").getCount({});
    return {value:response.count.value.toString()};
  }},JSON.stringify({events:["fetch"],cells:{},rpc_contracts:{[contract.digest]:contract}}),16);
  const response = await runner.dispatch(1,JSON.stringify({kind:"fetch",identity:null,object:null,class:null,
    event:{url:"http://test/",method:"GET",headers:[],body:""}}));
  assert.deepEqual(JSON.parse(response),{value:"42"});
  assert.equal(submitted,1);
});

test("the real runner enforces native layout limits before a guest submits", async () => {
  const {contract} = fixture("counter");
  contract.limits.max_decoded_slots = 4; // IncrementRequest with two fields needs five.
  let submitted = 0;
  const runner = testRunner((_id,name) => {
    if (name === "$check") return;
    if (name === "$rpc_budget") return testBudgetControl();
    submitted++;
    throw Error("must not submit");
  })({async fetch(_request,ctx) {
    try {
      await CounterClient.bind(ctx,"COUNTERS","alice").increment({delta:1n,operationId:"id"});
      throw Error("must refuse");
    } catch (error) {
      return {code:error.code,outcome:error.outcome};
    }
  }},JSON.stringify({events:["fetch"],cells:{},rpc_contracts:{[contract.digest]:contract}}),16);
  const response = await runner.dispatch(1,JSON.stringify({kind:"fetch",identity:null,object:null,class:null,
    event:{url:"http://test/",method:"GET",headers:[],body:""}}));
  assert.deepEqual(JSON.parse(response),{code:"ResourceExhausted",outcome:"not_committed"});
  assert.equal(submitted,0);
});
