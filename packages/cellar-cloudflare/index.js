// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { durableStorage } from "./storage.js";
import { nativeId } from "./id.js";
import { DurableObject, RpcTarget } from "./workers.js";
export { durableStorage } from "./storage.js";
export { durableObjectNamespace } from "./namespace.js";

const reserved = new Set(["constructor", "dup", "fetch", "connect", "alarm", "webSocketMessage", "webSocketClose", "webSocketError"]);
function rpcMethod(target, method, args) {
  if (!(target instanceof DurableObject) || typeof method !== "string" || reserved.has(method) || !Array.isArray(args)) throw new TypeError("Invalid Durable Object RPC method");
  for (let prototype = Object.getPrototypeOf(target); prototype && prototype !== DurableObject.prototype && prototype !== RpcTarget.prototype && prototype !== Object.prototype; prototype = Object.getPrototypeOf(prototype)) {
    const descriptor = Object.getOwnPropertyDescriptor(prototype, method);
    if (descriptor) {
      if (typeof descriptor.value !== "function") throw new TypeError("RPC property access requires a property-capable profile");
      return Reflect.apply(descriptor.value, target, args);
    }
  }
  throw new TypeError("No public Durable Object RPC method");
}

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
      const promise = current()._callback("block", () => { current(); if (failed) throw failure; return callback(); });
      blocks.add(promise);
      promise.then(() => blocks.delete(promise), error => { blocks.delete(promise); if (!failed) { failed = true; failure = error; } });
      return promise;
    },
    waitUntil(promise) { current()._waitUntil(promise); },
    abort() { return unsupported("abort"); },
  });
  let target;
  try { target = new Target(state, env); } catch (error) { failed = true; failure = error; }
  async function admitted() {
    while (blocks.size) await Promise.all([...blocks]);
    current();
    if (failed) throw failure;
  }
  async function invoke(callback) {
    current();
    await admitted();
    const result = await callback(target);
    await admitted();
    return result;
  }
  return Object.freeze({ invoke, rpc: (method, args) => invoke(target => rpcMethod(target, method, args)) });
}
