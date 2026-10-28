// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { addAbortListener } from "node:events";
import test from "node:test";
import { testBudgetControl } from "./test-budgets.mjs";
import { testRunner } from "../javascript/test-runner.mjs";
import { CounterClient } from "./fixtures/generated/javascript/counter_cellar.js";

const contract = JSON.parse(readFileSync(new URL("./fixtures/generated/rpc.json", import.meta.url)));

function start(signal, subscribe) {
  let resolve, current = 1, admitted = 0, cancelled = 0, context;
  const acknowledgement = new Promise(ready => { resolve = ready; });
  const runner = testRunner((invocation, name, payload) => {
    if (invocation !== current) throw Error("ContextMismatch");
    if (name === "$check") return;
    if (name === "$rpc_budget") return testBudgetControl();
    assert.equal(name, "cell.call");
    assert.equal(Object.hasOwn(JSON.parse(payload), "signal"), false);
    admitted++;
    return [acknowledgement, () => { cancelled++; }];
  }, subscribe, () => current)({async fetch(_request, ctx) {
    context = ctx;
    try {
      const response = await CounterClient.bind(ctx, "COUNTERS", "alice").getCount({}, {signal});
      return {value:response.count.value.toString()};
    } catch (error) {
      return {code:error.code, outcome:error.outcome};
    }
  }}, JSON.stringify({events:["fetch"], cells:{}, rpc_contracts:{[contract.digest]:contract}}), 16);
  const call = runner.dispatch(1, JSON.stringify({kind:"fetch", identity:null, object:null, class:null, rpc_limits:{[contract.digest]:contract.limits},
    event:{url:"http://test/", method:"GET", headers:[], body:""}})).then(JSON.parse);
  return {
    call, context:() => context,
    get admitted() { return admitted; },
    get cancelled() { return cancelled; },
    foreign() { current = 2; },
    acknowledge(ok = false) {
      resolve([JSON.stringify(ok ? {ok:true, value:null} :
        {ok:false, code:"Cancelled", outcome:"unknown"}), ok ? Uint8Array.of(10,2,8,42) : undefined]);
    },
  };
}

test("signal cancellation waits for the original native acknowledgement", async () => {
  const controller = new AbortController();
  let listeners = 0;
  const signal = {
    get aborted() { return controller.signal.aborted; },
    addEventListener(...args) { listeners++; controller.signal.addEventListener(...args); },
    removeEventListener(...args) { listeners--; controller.signal.removeEventListener(...args); },
  };
  const probe = start(signal);
  assert.equal(probe.admitted, 1);
  probe.foreign();
  controller.abort("private cancellation reason");
  assert.equal(probe.cancelled, 1);
  let settled = false;
  probe.call.then(() => { settled = true; });
  await Promise.resolve();
  assert.equal(settled, false);
  assert.equal(listeners, 1);
  probe.acknowledge();
  assert.deepEqual(await probe.call, {code:"Cancelled", outcome:"unknown"});
  assert.equal(listeners, 0);
});

test("a foreign context cannot acquire SDK authority by possessing a signal", async () => {
  const controller = new AbortController();
  const probe = start(controller.signal);
  probe.foreign();
  await assert.rejects(probe.context().variable("forbidden"), {code:"ContextMismatch", outcome:"not_committed"});
  controller.abort();
  probe.acknowledge();
  assert.deepEqual(await probe.call, {code:"Cancelled", outcome:"unknown"});
  assert.equal(probe.admitted, 1);
});

test("dispatching a synthetic abort event does not abort a live signal", async () => {
  const controller = new AbortController();
  const probe = start(controller.signal);
  controller.signal.dispatchEvent(new Event("abort"));
  assert.equal(probe.cancelled, 0);
  // Synthetic events must not consume the real cancellation subscription.
  controller.abort();
  assert.equal(probe.cancelled, 1);
  probe.acknowledge();
  assert.deepEqual(await probe.call, {code:"Cancelled", outcome:"unknown"});
});

test("Node's protected subscription resists stopImmediatePropagation", async () => {
  const controller = new AbortController();
  controller.signal.addEventListener("abort", event => event.stopImmediatePropagation());
  const probe = start(controller.signal, addAbortListener);
  controller.abort();
  assert.equal(probe.cancelled, 1);
  probe.acknowledge();
  assert.deepEqual(await probe.call, {code:"Cancelled", outcome:"unknown"});
});

test("a synthetic event does not consume Node's protected subscription", async () => {
  const controller = new AbortController();
  const probe = start(controller.signal, addAbortListener);
  controller.signal.dispatchEvent(new Event("abort"));
  assert.equal(probe.cancelled, 0);
  controller.abort();
  assert.equal(probe.cancelled, 1);
  probe.acknowledge();
  assert.deepEqual(await probe.call, {code:"Cancelled", outcome:"unknown"});
});

test("native composed signals use the same per-operation control", async () => {
  const controller = new AbortController();
  const probe = start(AbortSignal.any([controller.signal]));
  controller.abort();
  assert.equal(probe.cancelled, 1);
  probe.acknowledge();
  assert.deepEqual(await probe.call, {code:"Cancelled", outcome:"unknown"});
});

test("a native timeout signal reaches the owned control", async () => {
  const signal = AbortSignal.timeout(1);
  const probe = start(signal, addAbortListener);
  await new Promise((resolve, reject) => {
    const watchdog = setTimeout(reject, 1000, Error("timeout signal did not fire"));
    signal.addEventListener("abort", () => { clearTimeout(watchdog); resolve(); }, {once:true});
  });
  assert.equal(probe.cancelled, 1);
  probe.acknowledge();
  assert.deepEqual(await probe.call, {code:"Cancelled", outcome:"unknown"});
});

test("a signal aborted before admission never creates native work", async () => {
  const controller = new AbortController();
  controller.abort();
  const probe = start(controller.signal);
  assert.deepEqual(await probe.call, {code:"Cancelled", outcome:"not_committed"});
  assert.equal(probe.admitted, 0);
});

test("cancellation after a successful native reply retains unknown effects", async () => {
  const controller = new AbortController();
  const probe = start(controller.signal);
  probe.acknowledge(true);
  controller.abort();
  assert.deepEqual(await probe.call, {code:"Cancelled", outcome:"unknown"});
});
