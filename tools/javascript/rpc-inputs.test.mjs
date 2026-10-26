// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { Serializer, Deserializer } from "node:v8";
import test from "node:test";

async function factory(name) {
  const source = await readFile(
    new URL(
      `../../crates/cellar-javascript/runtime/${name}.js`,
      import.meta.url,
    ),
    "utf8",
  );
  return (0, eval)(`(${source})`);
}
const [makeInputs, makeDelegates, makeReadables, makeWritables] =
  await Promise.all(
    ["rpc_inputs", "delegates", "rpc_streams", "rpc_writable"].map(factory),
  );

// A bounded protocol peer, not a native authority substitute. Native-host
// integration separately verifies routing, leases, quotas and shutdown.
function peer(invoke, maximum = 2048, beforeControl = () => {}) {
  let implementation,
    released = 0;
  const cleanups = new Set(),
    reservations = new Set(),
    frames = [];
  const base = {
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
      for (const close of [...cleanups]) await close();
      assert.equal(reservations.size, 0);
    },
  };
}

test("input reads preserve aliases and bound the nested completion envelope", async () => {
  let count = 0;
  const remote = peer(async (stream, alias) => {
    assert.equal(stream, alias);
    const reader = stream.getReader({ mode: "byob" });
    for (;;) {
      const chunk = await reader.read(new Uint8Array(4096));
      if (chunk.done) break;
      for (const byte of chunk.value) assert.equal(byte, count++ % 256);
    }
    return count;
  });
  const bytes = new Uint8Array(8192).map((_, i) => i % 256);
  const source = new Blob([bytes]).stream();
  assert.equal(await remote.call([source, source]), bytes.length);
  assert.ok(remote.frames.length > 10);
  await remote.close();
  assert.equal(source.locked, true);
});

test("writable inputs wait for sinks and preserve bytes across bounded frames", async () => {
  const gate = Promise.withResolvers(),
    entered = Promise.withResolvers();
  let bytes = 0,
    closed = false;
  const sink = new WritableStream({
    async write(chunk) {
      entered.resolve();
      await gate.promise;
      for (const byte of chunk) assert.equal(byte, bytes++ % 256);
    },
    close() {
      closed = true;
    },
  });
  const remote = peer(async (stream) => {
    const writer = stream.getWriter();
    await writer.write(new Uint8Array(8192).map((_, i) => i % 256));
    await writer.close();
    return true;
  });
  let finished = false;
  const pending = remote.call([sink]).then((value) => {
    finished = true;
    return value;
  });
  await entered.promise;
  assert.equal(finished, false);
  gate.resolve();
  assert.equal(await pending, true);
  assert.equal(bytes, 8192);
  assert.equal(closed, true);
  await remote.close();
  assert.equal(sink.locked, true);
});

test("unused input sessions cancel sources without retaining the caller", async () => {
  let cancelled = false;
  const source = new ReadableStream({
    type: "bytes",
    cancel() {
      cancelled = true;
    },
  });
  const remote = peer(() => true);
  assert.equal(await remote.call([source]), true);
  assert.equal(cancelled, false);
  await remote.close();
  assert.equal(cancelled, true);
});

test("cancellation can overtake a pending input read and keeps its reason", async () => {
  const entered = Promise.withResolvers();
  let reason;
  const source = new ReadableStream({
    type: "bytes",
    pull() {
      entered.resolve();
    },
    cancel(value) {
      reason = value;
    },
  });
  const remote = peer(async (stream) => {
    const reader = stream.getReader();
    const read = reader.read();
    await entered.promise;
    await reader.cancel("consumer left");
    assert.equal((await read).done, true);
    return true;
  });
  assert.equal(await remote.call([source]), true);
  assert.equal(reason, "consumer left");
  await remote.close();
});

test("source errors survive concurrent cancellation cleanup after accepted bytes", async () => {
  for (let attempt = 0; attempt < 8; attempt++) {
    let sent = false,
      bytes = 0;
    const source = new ReadableStream({
      type: "bytes",
      pull(controller) {
        if (sent) {
          const error = new Error("upload failed");
          error.stack = ""; // Keep this failure inside the negotiated frame budget.
          throw error;
        }
        sent = true;
        controller.enqueue(new Uint8Array(4096));
      },
    });
    const remote = peer(async (stream) => {
      const reader = stream.getReader();
      for (;;) {
        const chunk = await reader.read();
        if (chunk.done) return;
        bytes += chunk.value.length;
      }
    });
    await assert.rejects(remote.call([source]), /upload failed/);
    assert.equal(bytes, 4096);
    await remote.close();
  }
});

test("a lost input acknowledgement preserves accepted prefixes and never replays data", async () => {
  let accepted = 0,
    invocations = 0,
    reads = 0;
  const source = new Blob([new Uint8Array(8192)]).stream();
  const remote = peer(
    async (stream) => {
      invocations++;
      const reader = stream.getReader();
      for (;;) {
        const chunk = await reader.read();
        if (chunk.done) return;
        accepted += chunk.value.length;
      }
    },
    2048,
    (target, args) => {
      if (target === 1 && ++reads === 2) {
        const error = new Error("acknowledgement refused");
        error.code = "ValidationError";
        error.outcome = "not_committed";
        throw error;
      }
    },
  );
  await assert.rejects(remote.call([source]), (error) => {
    assert.equal(error.code, "Ambiguous");
    assert.equal(error.outcome, "unknown");
    assert.equal(error.cause.code, "ValidationError");
    return true;
  });
  assert.ok(accepted > 0 && accepted < 8192);
  assert.equal(invocations, 1);
  assert.equal(reads, 2);
  await remote.close();
});
