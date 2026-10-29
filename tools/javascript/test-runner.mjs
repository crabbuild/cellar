// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { readFileSync } from "node:fs";
import { Script } from "node:vm";
import { serialize, deserialize } from "node:v8";

function load(name) {
  const url = new URL(
    `../../crates/cellar-javascript/runtime/${name}.js`,
    import.meta.url,
  );
  return new Script(`(${readFileSync(url, "utf8")})`, {
    filename: url.pathname,
  }).runInThisContext();
}

const runner = load("runner");
const delegates = load("delegates");
const streams = load("rpc_streams");
const writables = load("rpc_writable");
const inputs = load("rpc_inputs");
const messages = load("rpc_messages");

// Supply the production delegate factories just as both native bootstraps do.
// Each test still controls host admission, replies and cancellation explicitly.
export function testRunner(host, subscribe, currentHost = () => 1) {
  const create = runner(
    host,
    undefined,
    currentHost,
    serialize,
    (bytes) => deserialize(Buffer.from(bytes)),
    (_id, _scope, callback) => callback(),
    subscribe,
    delegates,
    streams,
    writables,
    inputs,
    messages,
  );
  return (app, descriptor, maximum, lifetimeLimit = 16) =>
    create(app, descriptor, maximum, lifetimeLimit);
}
