// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import assert from "node:assert/strict";
import test from "node:test";
import { readFileSync } from "node:fs";
import { Script } from "node:vm";

const make = new Script(`(${readFileSync(new URL("../../crates/cellar-javascript/runtime/node/budget.js", import.meta.url), "utf8")})`).runInThisContext();

function probe(maximum = 1) {
  let now = 0n, current = 1, next = 0;
  const state = {budgets:new Map()};
  const active = new Map([[1, state]]), sent = [];
  const budgets = make(() => now, () => current, active, maximum, value => sent.push(value), () => ++next);
  return {state, active, sent, budgets,
    advance(ms) { now += BigInt(ms) * 1000000n; },
    foreign() { current = 2; },
    reply(remaining = 100) { budgets.receive({id:next, invocation:1, ok:true, remaining_ms:remaining}); },
  };
}

test("Node charges the preflight round trip and retains the original native handle", async () => {
  const p = probe();
  const pending = p.budgets.start(1, p.state);
  p.advance(10);
  p.reply();
  const [id, read, limit, release] = await pending;
  assert(read() > 0 && read() <= 90);
  limit(50);
  const control = p.budgets.parameters(1, id);
  assert.equal(control.budget_id, id);
  assert(control.budget_ms <= 40);
  p.advance(50);
  assert.throws(read, {code:"DeadlineExceeded"});
  release(); release();
  assert.equal(p.state.budgets.size, 0);
  assert.equal(p.sent.filter(value => value.type === "budget_end").length, 1);
});

test("budget controls are bounded separately and a released slot is reusable", async () => {
  const p = probe();
  const pending = p.budgets.start(1, p.state);
  assert.throws(() => p.budgets.start(1, p.state), {code:"ResourceExhausted"});
  p.reply();
  const control = await pending;
  control[3]();
  const next = p.budgets.start(1, p.state);
  p.reply();
  (await next)[3]();
  assert.equal(p.state.budgets.size, 0);
});

test("an expired preflight refuses before effects and foreign scopes cannot use its control", async () => {
  const p = probe();
  const pending = p.budgets.start(1, p.state);
  p.advance(100);
  p.reply(20);
  const [id, read, , release] = await pending;
  assert.throws(read, {code:"DeadlineExceeded"});
  p.foreign();
  assert.throws(() => p.budgets.parameters(1, id), {code:"ContextMismatch"});
  release();
  assert.equal(p.state.budgets.size, 0);
});

test("invocation retirement settles pending controls and removes acknowledged controls", async () => {
  for (const acknowledged of [false, true]) {
    const p = probe();
    const pending = p.budgets.start(1, p.state);
    let control;
    if (acknowledged) { p.reply(); control = await pending; }
    else pending.catch(() => {});
    p.active.delete(1);
    p.budgets.revoke(1, p.state);
    if (acknowledged) {
      assert.throws(control[1], {code:"ContextExpired"});
      control[3]();
    } else await assert.rejects(pending, {code:"ContextExpired"});
    assert.equal(p.state.budgets.size, 0);
  }
});
