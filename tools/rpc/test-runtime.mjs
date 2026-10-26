// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// The real facade requires its captured stream/delegate factories, even when
// a particular fixture only exercises unary RPC. Host authority stays explicit.
import { readFileSync } from "node:fs";
import { Script } from "node:vm";
import { serialize, deserialize } from "node:v8";

const evaluate = (name) =>
  new Script(
    `(${readFileSync(
      new URL(
        `../../crates/cellar-javascript/runtime/${name}`,
        import.meta.url,
      ),
      "utf8",
    )})`,
  ).runInThisContext();

export function runnerFactory(
  host,
  synchronousHost,
  currentHost = () => 1,
  encodeClone = serialize,
  decodeClone = (bytes) => deserialize(Buffer.from(bytes)),
  scopedHost = (_id, _scope, callback) => callback(),
  subscribe,
) {
  const create = evaluate("runner.js")(
    host,
    synchronousHost,
    currentHost,
    encodeClone,
    decodeClone,
    scopedHost,
    subscribe,
    evaluate("delegates.js"),
    evaluate("rpc_streams.js"),
    evaluate("rpc_writable.js"),
    evaluate("rpc_inputs.js"),
    evaluate("rpc_messages.js"),
  );
  return (app, descriptor, maximum, lifetimeLimit = 16) =>
    create(app, descriptor, maximum, lifetimeLimit);
}
