// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import assert from "node:assert/strict";
import { AsyncLocalStorage } from "node:async_hooks";
import { readFile } from "node:fs/promises";
import test from "node:test";

const make = (0, eval)(
  `(${await readFile(new URL("../../crates/cellar-javascript/runtime/node/rejections.js", import.meta.url), "utf8")})`,
);
function setup() {
  const scope = new AsyncLocalStorage();
  const logs = [],
    global = {};
  const events = make(
    () => scope.getStore(),
    scope.run.bind(scope),
    (...args) => logs.push(args),
    global,
  );
  return { scope, logs, global, events };
}

test("Workers rejection events preserve values, scope and default reporting", () => {
  const { scope, logs, global, events } = setup();
  const promise = Promise.resolve(),
    reason = { failure: true },
    owner = { invocation: 1 };
  global.addEventListener("unhandledrejection", (event) => {
    assert.ok(event instanceof global.PromiseRejectionEvent);
    assert.equal(event.promise, promise);
    assert.equal(event.reason, reason);
    assert.equal(event.cancelable, true);
    assert.equal(scope.getStore(), owner);
  });
  scope.run(owner, () => events.unhandled(reason, promise));
  assert.deepEqual(logs, [["Uncaught (in promise)", reason]]);
});

test("preventDefault suppresses logging and late handling uses the original promise scope", () => {
  const { scope, logs, global, events } = setup();
  const promise = Promise.resolve(),
    owner = { invocation: 1, callback: 2 };
  const listener = (event) => event.preventDefault();
  global.addEventListener("unhandledrejection", listener);
  scope.run(owner, () => events.unhandled(undefined, promise));
  assert.deepEqual(logs, []);
  let handled = 0;
  global.addEventListener("rejectionhandled", (event) => {
    assert.equal(scope.getStore(), owner);
    assert.equal(event.promise, promise);
    assert.equal(event.reason, undefined);
    assert.equal(event.cancelable, false);
    handled++;
  });
  scope.run({ invocation: 3 }, () => events.handled(promise));
  events.handled(promise);
  assert.equal(handled, 1);
  global.removeEventListener("unhandledrejection", listener);
  scope.run(owner, () => events.unhandled(undefined, Promise.resolve()));
  assert.equal(logs.length, 1);
});

test("bootstrap failures retain fatal Node behavior", () => {
  const { events, logs } = setup();
  const error = new Error("bootstrap");
  assert.throws(
    () => events.unhandled(error, Promise.resolve()),
    (e) => e === error,
  );
  assert.deepEqual(logs, []);
});
