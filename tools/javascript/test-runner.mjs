// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { readFileSync } from "node:fs";
import { Script } from "node:vm";

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
export function testRunner(host, subscribe) {
  return runner(
    host,
    undefined,
    undefined,
    undefined,
    undefined,
    undefined,
    subscribe,
    delegates,
    streams,
    writables,
    inputs,
    messages,
  );
}
