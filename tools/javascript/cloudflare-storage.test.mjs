// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import assert from "node:assert/strict";
import test from "node:test";
import { durableStorage } from "../../packages/cellar-cloudflare/storage.js";

const checkpoint = () => new Promise(setImmediate);

test("async alarm APIs yield while native admission is held", async () => {
  const calls = [];
  let pending;
  const context = {
    _sync() {
      throw new Error("alarm blocked the language thread");
    },
    _call(name, input) {
      calls.push([name, input]);
      pending = Promise.withResolvers();
      return pending.promise;
    },
  };
  const storage = durableStorage(() => context);
  for (const [invoke, expected, result] of [
    [
      () => storage.setAlarm(123),
      ["storage.apply", { alarm_at_ms: 123 }],
      null,
    ],
    [() => storage.getAlarm(), ["alarm.get", undefined], 123],
    [
      () => storage.deleteAlarm(),
      ["storage.apply", { delete_alarm: true }],
      null,
    ],
  ]) {
    let settled = false;
    const work = invoke();
    work.then(
      () => {
        settled = true;
      },
      () => {
        settled = true;
      },
    );
    assert.deepEqual(calls.at(-1), expected);
    await checkpoint();
    assert.equal(settled, false, "native admission must settle before the API");
    pending.resolve(result);
    assert.equal(await work, result ?? undefined);
  }
});
