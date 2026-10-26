// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { Serializer, Deserializer } from "node:v8";

async function factory(name) {
  const source = await readFile(
    new URL(
      `../../../crates/cellar-javascript/runtime/${name}.js`,
      import.meta.url,
    ),
    "utf8",
  );
  return (0, eval)(`(${source})`);
}
const [
  makeInputs,
  makeDelegates,
  makeReadables,
  makeWritables,
  makeMessages,
  makeRequests,
] = await Promise.all(
  [
    "rpc_inputs",
    "delegates",
    "rpc_streams",
    "rpc_writable",
    "rpc_messages",
    "rpc_requests",
  ].map(factory),
);

// A bounded protocol peer, not a native authority substitute. Native-host
// integration separately verifies routing, leases, quotas and shutdown.
export function peer(invoke, maximum = 2048, beforeControl = () => {}) {
  let implementation,
    released = 0;
  const cleanups = new Set(),
    reservations = new Set(),
    frames = [];
  const base = {
    identity: null,
    _current: () => base,
    _serialize(value) {
      const codec = new Serializer();
      codec.writeHeader();
      codec.writeValue(value);
      return codec.releaseBuffer().toString("base64");
    },
    _deserialize(value) {
      const codec = new Deserializer(
        new Uint8Array(Buffer.from(value, "base64")),
      );
      codec.readHeader();
      return codec.readValue();
    },
    _decode: (value) => Buffer.from(value, "base64"),
    _sync: () => ({ max_body_bytes: maximum }),
    _rpcCleanup(close) {
      cleanups.add(close);
      return () => cleanups.delete(close);
    },
    async _call(name, input) {
      if (name === "workers.rpc.error") {
        const value = base._deserialize(input.value);
        if (value[0] instanceof Error) value[0].stack = undefined;
        return { ...input, value: base._serialize(value) };
      }
      if (name === "workers.rpc.inputs") {
        if (input.count > 6) throw new Error("input quota");
        const id = reservations.size + 1;
        reservations.add(id);
        return { id, max_body_bytes: maximum };
      }
      if (name === "workers.rpc.inputs_release") {
        reservations.delete(input.id);
        return;
      }
      assert.equal(name, "workers.rpc");
      return implementation.accept(
        base._deserialize(input.args),
        server,
        (_method, args) => invoke(...args),
      );
    },
  };
  const server = {
    ...base,
    _rpcResult(controls, close) {
      return { controls, close };
    },
  };
  let graphHelpers;
  const helpers = makeDelegates(
    makeReadables(),
    makeWritables(),
    (dependencies) => {
      graphHelpers = dependencies;
      return {};
    },
    (readables) => makeMessages(readables, makeRequests),
  );
  implementation = makeInputs({
    ...graphHelpers,
    reply(frame, ctx, _soft, signal) {
      if (!frame.controls) return helpers.reply(frame, ctx);
      return frame.controls.map((control, index) => {
        const call = async (...args) => {
          const encoded = ctx._serialize(args);
          const bytes = Buffer.byteLength(
            JSON.stringify({
              format: 1,
              group: "g".repeat(36),
              target: index,
              args: encoded,
              inputs: false,
            }),
          );
          frames.push(bytes);
          beforeControl(index, ctx._deserialize(encoded));
          assert.ok(
            bytes <= maximum,
            `input control request ${bytes} exceeds ${maximum}`,
          );
          const result = Promise.resolve().then(() =>
            control(...ctx._deserialize(encoded)),
          );
          if (index !== 0 || !signal) return result;
          const abort = new Promise((_, reject) =>
            signal.addEventListener(
              "abort",
              () => reject(new Error("cancelled")),
              { once: true },
            ),
          );
          return Promise.race([result, abort]);
        };
        call[Symbol.dispose] = () => {
          if (++released === 4) frame.close();
        };
        return call;
      });
    },
    readables: makeReadables(),
    writables: makeWritables(),
  });
  return {
    call: (args) => implementation.call({ method: "consume" }, args, base),
    frames,
    async close() {
      for (const close of [...cleanups]) if (cleanups.has(close)) await close();
      assert.equal(reservations.size, 0);
    },
  };
}
