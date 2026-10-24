import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import { Script } from "node:vm";

const factory = new Script(`(${readFileSync(new URL("../../crates/cellar-javascript/runtime/runner.js", import.meta.url), "utf8")})`).runInThisContext();
const wire = "test.Counter/Fail";

async function failure(error) {
  class Counter { async fail() { throw error; } }
  const rule = {digest:"a".repeat(64), input:"test.Request", output:"test.Response", handler:"fail",
    decode:() => ({}), encode:() => new Uint8Array(), verify() {}};
  const runner = factory(() => { throw Error("unexpected SDK operation"); })({cells:{Counter}, rpc:{Counter:{methods:{[wire]:rule}}}},
    JSON.stringify({events:[], cells:{Counter:[]}, rpc:{Counter:{[wire]:{...rule,limits:{}}}}}), 1);
  try {
    await runner.dispatch(1, JSON.stringify({kind:"rpc",identity:null,object:1,class:"Counter",event:{method:wire}}), new Uint8Array());
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
