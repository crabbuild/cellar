import assert from "node:assert/strict";
import test from "node:test";
import { createMethod, RpcClient } from "../../packages/cellar-sdk/rpc/index.js";
import { GetCountRequestSchema, GetCountResponseSchema } from "./fixtures/generated/typescript/counter_pb.ts";
import { testBudget } from "./test-budgets.mjs";

function probe({cap = 5000, parent = Infinity, verify = 0, encode = 0, deliver = 0, decode = 0} = {}) {
  const clock = {now: 0};
  const budget = testBudget(clock, cap, parent);
  let encoded = 0, decoded = 0;
  const calls = [];
  const method = createMethod({
    method:"example.counter.v1.Counter/GetCount", digest:"a".repeat(64), handler:"getCount",
    inputSchema:GetCountRequestSchema, outputSchema:GetCountResponseSchema,
  }, {
    encodeMessage() { encoded++; clock.now += encode; return new Uint8Array(); },
    decodeMessage() { decoded++; clock.now += decode; return {ok:true}; },
  });
  const context = {[Symbol.for("cellar.rpc")]: {
    check() {}, budget() { return budget; },
    verify() { clock.now += verify; return {}; },
    async call(metadata, bytes, signal, original) {
      calls.push({metadata, bytes, signal, original});
      clock.now += deliver;
      return new Uint8Array();
    },
  }};
  const client = new RpcClient(context, "COUNTERS", "alice");
  return {clock, budget, calls, context, client, method, encoded:() => encoded, decoded:() => decoded};
}

test("zero budget refuses before verification or encoding and releases its control", async () => {
  const p = probe();
  await assert.rejects(p.client.call(p.method, {}, {timeoutMs:0}), {code:"DeadlineExceeded", outcome:"not_committed"});
  assert.equal(p.encoded(), 0);
  assert.equal(p.calls.length, 0);
  assert.equal(p.budget.released, 1);
});

test("verification and option getters share the requested, host and parent cap", async () => {
  for (const [settings, timeoutMs] of [[{verify:11}, 10], [{verify:11, cap:10}, undefined], [{verify:11, parent:10}, 60000]]) {
    const p = probe(settings);
    await assert.rejects(p.client.call(p.method, {}, {timeoutMs}), {code:"DeadlineExceeded", outcome:"not_committed"});
    assert.equal(p.encoded(), 0);
    assert.equal(p.calls.length, 0);
    assert.equal(p.budget.released, 1);
  }
  const p = probe();
  await assert.rejects(p.client.call(p.method, {}, {get timeoutMs() { p.clock.now += 11; return 10; }}),
    {code:"DeadlineExceeded", outcome:"not_committed"});
  assert.equal(p.calls.length, 0);
});

test("encoding forwards remaining time and the original opaque budget separately", async () => {
  const p = probe({encode:7});
  await p.client.call(p.method, {}, {timeoutMs:10});
  assert.equal(p.calls[0].metadata.timeout_ms, 3);
  assert.equal(p.calls[0].original, p.budget);
  assert.deepEqual(Object.keys(p.calls[0].metadata).sort(), ["binding","contract_digest","method","name","timeout_ms"]);
  assert.equal(p.budget.released, 1);
});

test("expired encoding never submits while late delivery and decoding stay unknown", async () => {
  const p = probe({encode:10});
  await assert.rejects(p.client.call(p.method, {}, {timeoutMs:10}), {code:"DeadlineExceeded", outcome:"not_committed"});
  assert.equal(p.calls.length, 0);
  for (const settings of [{deliver:10}, {decode:10}]) {
    const q = probe(settings);
    await assert.rejects(q.client.call(q.method, {}, {timeoutMs:10}), {code:"DeadlineExceeded", outcome:"unknown"});
    assert.equal(q.calls.length, 1);
    assert.equal(q.decoded(), settings.deliver ? 0 : 1);
    assert.equal(q.budget.released, 1);
  }
});

test("controls snapshot before asynchronous native admission and failures still join it", async () => {
  for (const invalid of [false, true]) {
    const p = probe();
    let admit;
    p.context[Symbol.for("cellar.rpc")].budget = () => new Promise(resolve => { admit = resolve; });
    let reads = 0;
    const options = {get timeoutMs() { reads++; if (invalid) throw Error("private getter"); return 17; }};
    const call = p.client.call(p.method, {}, options);
    assert.equal(reads, 1);
    Object.defineProperty(options, "timeoutMs", {value:60000});
    admit(p.budget);
    if (invalid) await assert.rejects(call, {code:"InvalidArgument", outcome:"not_committed"});
    else { await call; assert.equal(p.calls[0].metadata.timeout_ms, 17); }
    assert.equal(p.budget.released, 1);
  }
});
