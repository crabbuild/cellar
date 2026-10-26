// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

const source = await readFile(
  new URL(
    "../../crates/cellar-javascript/runtime/rpc_writable.js",
    import.meta.url,
  ),
  "utf8",
);
const make = (0, eval)(`(${source})`);
const context = (maximum) => ({ _sync: () => ({ max_body_bytes: maximum }) });

test("RPC writable transfers ownership and splits bytes to the smaller host budget", async () => {
  const codec = make();
  for (const [producer, consumer] of [
    [256, 1024],
    [1024, 256],
  ]) {
    const chunks = [];
    let closed = 0,
      released = 0;
    const original = new WritableStream({
      write(bytes) {
        chunks.push(bytes);
      },
      close() {
        closed++;
      },
    });
    const sent = codec.send(original, context(producer));
    const received = codec.receive(
      sent.invoke,
      async () => {
        released++;
      },
      undefined,
      Math.min(producer, consumer),
    );
    assert.equal(original.locked, true);
    const writer = received.stream.getWriter();
    await writer.write(new Uint8Array(0));
    const bytes = new Uint8Array(768).map((_, i) => i % 256);
    await writer.write(bytes);
    await writer.close();
    assert.equal(chunks[0].byteLength, 0);
    assert.ok(chunks.every((chunk) => chunk.byteLength <= 32));
    assert.deepEqual(Buffer.concat(chunks), Buffer.from(bytes));
    assert.equal(closed, 1);
    assert.equal(released, 1);
    assert.equal(original.locked, true);
    await sent.close();
    assert.equal(closed, 1);
  }
});

test("writes and close await the actual remote sink", async () => {
  const codec = make();
  const entered = Promise.withResolvers(),
    writing = Promise.withResolvers(),
    closing = Promise.withResolvers(),
    closeEntered = Promise.withResolvers();
  let writeDone = false,
    closeDone = false;
  const sent = codec.send(
    new WritableStream({
      write() {
        entered.resolve();
        return writing.promise;
      },
      close() {
        closeEntered.resolve();
        return closing.promise;
      },
    }),
    context(1024),
  );
  const received = codec.receive(sent.invoke, async () => {}, undefined, 1024);
  const writer = received.stream.getWriter();
  const write = writer.write(Uint8Array.of(1)).then(() => {
    writeDone = true;
  });
  await entered.promise;
  await new Promise(setImmediate);
  assert.equal(writeDone, false);
  writing.resolve();
  await write;
  const close = writer.close().then(() => {
    closeDone = true;
  });
  await closeEntered.promise;
  assert.equal(closeDone, false);
  closing.resolve();
  await close;
});

test("sink failures preserve accepted prefixes and never become successful close", async () => {
  const codec = make();
  let accepted = 0,
    closed = 0,
    released = 0;
  const sent = codec.send(
    new WritableStream({
      write(bytes) {
        if (accepted) throw new Error("sink rejected");
        accepted += bytes.byteLength;
      },
      close() {
        closed++;
      },
    }),
    context(256),
  );
  const received = codec.receive(
    sent.invoke,
    async () => {
      released++;
    },
    undefined,
    256,
  );
  const writer = received.stream.getWriter();
  await assert.rejects(writer.write(new Uint8Array(64)), /sink rejected/);
  await assert.rejects(writer.closed, /sink rejected/);
  await assert.rejects(writer.close());
  assert.equal(accepted, 32);
  assert.equal(closed, 0);
  assert.equal(released, 1);
});

test("abort joins the sink, keeps its reason and releases the native handle", async () => {
  const codec = make();
  const entered = Promise.withResolvers(),
    gate = Promise.withResolvers();
  let reason,
    released = 0,
    finished = false;
  const sent = codec.send(
    new WritableStream({
      abort(value) {
        reason = value;
        entered.resolve();
        return gate.promise;
      },
    }),
    context(1024),
  );
  const received = codec.receive(
    sent.invoke,
    async () => {
      released++;
    },
    undefined,
    1024,
  );
  const abort = received.stream.abort("caller left").then(() => {
    finished = true;
  });
  await entered.promise;
  assert.equal(finished, false);
  assert.equal(released, 0);
  gate.resolve();
  await abort;
  assert.equal(reason, "caller left");
  assert.equal(released, 1);
});

test("disposal of an imported locked writer joins an accepted write before abort", async () => {
  const codec = make();
  const entered = Promise.withResolvers(),
    gate = Promise.withResolvers();
  let aborted = 0,
    released = 0,
    finished = false;
  const sent = codec.send(
    new WritableStream({
      write() {
        entered.resolve();
        return gate.promise;
      },
      abort() {
        aborted++;
      },
    }),
    context(1024),
  );
  const received = codec.receive(
    sent.invoke,
    async () => {
      released++;
    },
    undefined,
    1024,
  );
  const writer = received.stream.getWriter();
  const written = writer.write(Uint8Array.of(3));
  // Disposal errors the controller and its closed promise, while an accepted
  // in-flight write can still finish. It cannot claim rollback of that write.
  const closed = assert.rejects(writer.closed, /disposed/);
  await entered.promise;
  const disposal = received.dispose().then(() => {
    finished = true;
  });
  await new Promise(setImmediate);
  assert.equal(finished, false);
  assert.equal(aborted, 0);
  gate.resolve();
  await Promise.all([written, disposal, closed]);
  assert.equal(aborted, 1);
  assert.equal(released, 1);
});

test("failed publication restores locks and invalid chunks/reasons still clean up", async () => {
  const codec = make();
  const original = new WritableStream();
  codec.send(original, context(1024)).restore();
  assert.equal(original.locked, false);
  for (const maximum of [128, Infinity, "1024"])
    assert.throws(() => codec.send(original, context(maximum)), /budget/);
  assert.equal(original.locked, false);
  let aborted = 0,
    released = 0;
  const sent = codec.send(
    new WritableStream({
      abort(reason) {
        assert.equal(reason, undefined);
        aborted++;
      },
    }),
    context(1024),
  );
  const received = codec.receive(
    sent.invoke,
    async () => {
      released++;
    },
    () => {
      throw new Error("uncloneable reason");
    },
    1024,
  );
  await assert.rejects(
    received.stream.abort(() => {}),
    /uncloneable reason/,
  );
  assert.equal(aborted, 1);
  assert.equal(released, 1);
  let invalidAborted = false;
  const invalid = codec.receive(
    async (operation) => {
      if (operation === "abort") invalidAborted = true;
      else assert.fail("invalid bytes were sent");
    },
    async () => {},
    undefined,
    1024,
  );
  const writer = invalid.stream.getWriter();
  await assert.rejects(writer.write("text"), /Uint8Array/);
  await assert.rejects(writer.closed, /Uint8Array/);
  assert.equal(invalidAborted, true);
});

test("failed sink close and abort stay failures while native handles release", async () => {
  const codec = make();
  for (const operation of ["close", "abort"]) {
    let releases = 0;
    const original = new WritableStream({
      close() {
        throw new Error("close failed");
      },
      abort() {
        throw new Error("abort failed");
      },
    });
    const sent = codec.send(original, context(1024));
    const received = codec.receive(
      sent.invoke,
      async () => {
        releases++;
      },
      undefined,
      1024,
    );
    const writer = received.stream.getWriter();
    const closed = assert.rejects(writer.closed);
    await assert.rejects(
      writer[operation]("reason"),
      new RegExp(`${operation} failed`),
    );
    await closed;
    assert.equal(releases, 1);
    assert.equal(original.locked, true);
  }
});

test("a later frame refusal cannot mark the composite write not committed", async () => {
  const codec = make();
  for (const acceptedPrefix of [false, true]) {
    let accepted = 0,
      released = 0;
    const refusal = Object.assign(new Error("operation quota"), {
      code: "OperationLimit",
      outcome: "not_committed",
    });
    const received = codec.receive(
      async (operation, bytes) => {
        if (operation !== "write") return;
        if (acceptedPrefix && accepted === 0) {
          accepted += bytes.byteLength;
          return;
        }
        throw refusal;
      },
      async () => {
        released++;
      },
      undefined,
      256,
    );
    const writer = received.stream.getWriter();
    await assert.rejects(writer.write(new Uint8Array(64)), (error) => {
      assert.equal(error.outcome, acceptedPrefix ? "unknown" : "not_committed");
      if (acceptedPrefix) {
        assert.equal(error.cause, refusal);
        assert.equal(error.code, "Ambiguous");
      } else assert.equal(error, refusal);
      return true;
    });
    await assert.rejects(writer.closed);
    assert.equal(accepted, acceptedPrefix ? 32 : 0);
    assert.equal(released, 1);
  }
});
