// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { durableStorage } from "./storage.js";
import { nativeId } from "./id.js";
export { durableStorage } from "./storage.js";

// The future Worker/DO event dispatcher uses this activation owner. Native SDK
// contexts retain invocation lifetime; only the state facade reacquires authority
// for the current continuation of this exact native activation.
export function activateDurableObject(Target, nativeContext, { env = {}, container } = {}) {
  if (typeof Target !== "function") throw new TypeError("Durable Object export must be a class");
  const current = nativeContext._activation();
  if (globalThis.crypto === undefined) Object.defineProperty(globalThis, "crypto", { value: nativeContext._crypto, configurable: true });
  const blocks = new Set();
  let failed = false;
  let failure;
  const unsupported = name => { throw new Error(`Unsupported Workers API: ${name}`); };
  const storage = durableStorage(current);
  const id = nativeId(current()._sync("workers.identity"));
  const state = Object.freeze({
    storage,
    container,
    id,
    // Namespace bindings and RPC exports still need native dispatch support.
    get exports() { return unsupported("DurableObjectState.exports"); },
    blockConcurrencyWhile(callback) {
      current();
      if (typeof callback !== "function") throw new TypeError("blockConcurrencyWhile requires a callback");
      const promise = Promise.resolve().then(() => { current(); if (failed) throw failure; return callback(); });
      blocks.add(promise);
      promise.then(() => blocks.delete(promise), error => { blocks.delete(promise); if (!failed) { failed = true; failure = error; } });
      return promise;
    },
    waitUntil() { return unsupported("waitUntil"); },
    abort() { return unsupported("abort"); },
  });
  let target;
  try { target = new Target(state, env); } catch (error) { failed = true; failure = error; }
  async function admitted() {
    while (blocks.size) await Promise.all([...blocks]);
    current();
    if (failed) throw failure;
  }
  return Object.freeze({
    async invoke(callback) {
      current();
      await admitted();
      const result = await callback(target);
      await admitted();
      return result;
    },
  });
}
