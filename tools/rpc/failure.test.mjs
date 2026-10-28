// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import assert from "node:assert/strict";
import test from "node:test";
import { testRunner } from "../javascript/test-runner.mjs";

const wire = "test.Counter/Fail";
const defaults = {max_message_bytes:1048576,max_depth:64,max_elements:100000,max_decoded_slots:1000000};

async function failure(error) {
  class Counter { async fail() { throw error; } }
  const rule = {digest:"a".repeat(64), input:"test.Request", output:"test.Response", handler:"fail",
    decode:() => ({}), encode:() => new Uint8Array(), verify() {}};
  const runner = testRunner(() => { throw Error("unexpected SDK operation"); })({cells:{Counter}, rpc:{Counter:{methods:{[wire]:rule}}}},
    JSON.stringify({events:[], cells:{Counter:[]}, rpc:{Counter:{[wire]:{...rule,limits:{}}}}}), 1);
  try {
    await runner.dispatch(1, JSON.stringify({kind:"rpc",identity:null,object:1,class:"Counter",event:{method:wire},rpc_limits:{["a".repeat(64)]:defaults}}), new Uint8Array());
    assert.fail("entered handler did not fail");
  } catch (value) {
    return {encoded:value, payload:runner.failurePayload(value), runner};
  }
}

test("entered RPC failures preserve only fixed categories without consulting claimed outcomes", async () => {
  for (const code of ["InvalidArgument","MethodNotFound","ContractMismatch","ResourceExhausted",
    "ContextExpired","Cancelled","DeadlineExceeded","Unavailable","OwnershipLost","DurabilityUnproven","InvalidResponse","Internal"]) {
    const error = new Error("private-sentinel");
    error.code = code;
    Object.defineProperty(error, "outcome", {get() { throw Error("must not read outcome"); }});
    const result = await failure(error);
    assert.equal(result.encoded, JSON.stringify({rpc_error:code}));
    assert.equal(result.payload, result.encoded);
    assert.equal(result.runner.failurePayload(JSON.stringify({rpc_error:code,outcome:"not_committed"})), "null");
  }
});

test("unknown codes, accessors and proxy exceptions cannot leak private text", async () => {
  const accessor = new Error("private-sentinel");
  Object.defineProperty(accessor, "code", {get() { throw Error("code getter executed"); }});
  const proxy = new Proxy({}, {getOwnPropertyDescriptor() { throw Error("private-proxy-sentinel"); }});
  for (const error of [{code:"private-code-sentinel"}, {code:"x".repeat(1024 * 1024)}, accessor, proxy, "private-string-sentinel", null]) {
    const result = await failure(error);
    assert.equal(result.payload, '{"rpc_error":"Internal"}');
    assert(!result.payload.includes("private"));
  }
});


test("provider codecs use native invocation limits without mutating another host", async () => {
  const seen = [];
  class Counter { async fail() { return {}; } }
  const rule = {digest:"a".repeat(64), input:"test.Request", output:"test.Response", handler:"fail",
    decode:(_, limits) => {seen.push(["decode",limits.maxDecodedSlots]); return {};},
    encode:(_, limits) => {seen.push(["encode",limits.maxDecodedSlots]); return new Uint8Array();}, verify() {}};
  const runner = testRunner(() => {throw Error("unexpected SDK operation");})({cells:{Counter},rpc:{Counter:{methods:{[wire]:rule}}}},
    JSON.stringify({events:[],cells:{Counter:[]},rpc:{Counter:{[wire]:{...rule,limits:defaults}}}}),1);
  for (const slots of [7,1000000,7]) {
    await runner.dispatch(1,JSON.stringify({kind:"rpc",identity:null,object:1,class:"Counter",event:{method:wire},
      rpc_limits:{[rule.digest]:{...defaults,max_decoded_slots:slots}}}),new Uint8Array());
  }
  assert.deepEqual(seen,[["decode",7],["encode",7],["decode",1000000],["encode",1000000],["decode",7],["encode",7]]);
});


test("suspended provider codecs keep their own host policy", async () => {
  const release = [];
  const encoded = [];
  class Counter { async fail(request) { await new Promise(resolve => {release[request.id]=resolve;}); return request; } }
  const rule = {digest:"a".repeat(64),input:"test.Request",output:"test.Response",handler:"fail",verify() {},
    decode:body => ({id:body[0]}),encode:(value,limits) => {encoded.push([value.id,limits.maxDecodedSlots]); return new Uint8Array();}};
  const runner = testRunner(() => {throw Error("unexpected SDK operation");})({cells:{Counter},rpc:{Counter:{methods:{[wire]:rule}}}},
    JSON.stringify({events:[],cells:{Counter:[]},rpc:{Counter:{[wire]:{...rule,limits:defaults}}}}),1);
  const start = (id,slots) => runner.dispatch(id,JSON.stringify({kind:"rpc",identity:null,object:1,class:"Counter",event:{method:wire},
    rpc_limits:{[rule.digest]:{...defaults,max_decoded_slots:slots}}}),Uint8Array.of(id));
  const strict = start(1,7), loose = start(2,1000000);
  release[1](); await strict;
  release[2](); await loose;
  assert.deepEqual(encoded,[[1,7],[2,1000000]]);
});
