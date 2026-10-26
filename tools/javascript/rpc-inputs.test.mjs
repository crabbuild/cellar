// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import assert from "node:assert/strict";
import test from "node:test";
import { peer } from "./support/rpc-input-peer.mjs";

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
