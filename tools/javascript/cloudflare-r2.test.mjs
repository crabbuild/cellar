// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import assert from "node:assert/strict";
import test from "node:test";
import { r2Binding } from "../../packages/cellar-cloudflare/r2.js";
import { workersApplication } from "../../packages/cellar-cloudflare/index.js";
import { FixedLengthStream } from "../../packages/cellar-cloudflare/streams.js";

test("R2 known-length parts cross bounded bridge chunks with backpressure", async () => {
  const bytes = Uint8Array.from({ length: 19 }, (_, index) => index);
  const writes = [];
  let active = 0;
  const context = {
    _encode(value) {
      return Buffer.from(value).toString("base64");
    },
    _sync() {
      return { max_body_bytes: 4 };
    },
    async _call(name, input) {
      if (name === "workers.r2.part_begin") {
        assert.equal(input.length, bytes.length);
        return { id: "part", max_chunk_bytes: 4 };
      }
      if (name === "workers.r2.part_write") {
        assert.equal(active++, 0, "part writes overlap");
        await Promise.resolve();
        const value = Buffer.from(input.body, "base64");
        assert.ok(value.length <= 4);
        writes.push(value);
        active--;
        return null;
      }
      assert.equal(name, "workers.r2.part_finish");
      assert.deepEqual(Buffer.concat(writes), Buffer.from(bytes));
      return { partNumber: 1, etag: "receipt" };
    },
  };
  const part = await r2Binding(() => context, "BACKUPS")
    .resumeMultipartUpload("key", "upload")
    .uploadPart(1, bytes);
  assert.deepEqual(part, { partNumber: 1, etag: "receipt" });
  assert.equal(writes.length, 5);
});

test("R2 interrupted stages join scratch retirement and preserve the initiating failure", async () => {
  for (const failing of ["part_write", "part_finish"]) {
    const failure = new Error(`lost ${failing} acknowledgement`);
    const calls = [];
    let retired = false;
    const context = {
      _encode(value) {
        return Buffer.from(value).toString("base64");
      },
      async _call(name) {
        calls.push(name);
        if (name.endsWith("part_begin"))
          return { id: "stage", max_chunk_bytes: 4 };
        if (name.endsWith(failing)) throw failure;
        if (name.endsWith("part_cancel")) {
          await Promise.resolve();
          retired = true;
          throw new Error("cleanup response lost too");
        }
        return null;
      },
    };
    await assert.rejects(
      r2Binding(() => context, "BACKUPS")
        .resumeMultipartUpload("key", "upload")
        .uploadPart(1, Uint8Array.of(1)),
      (error) => error === failure,
    );
    assert.equal(retired, true);
    assert.equal(calls.filter((name) => name.endsWith(failing)).length, 1);
    assert.equal(calls.filter((name) => name.endsWith("part_begin")).length, 1);
    assert.equal(
      calls.filter((name) => name.endsWith("part_cancel")).length,
      1,
    );
  }
});

test("R2 streamed text preserves Unicode across bounded UTF-16 windows", async () => {
  const text = "a".repeat(16383) + "😀" + "é\ud800".repeat(20);
  const expected = Buffer.from(text);
  const chunks = [];
  const context = {
    _encode(value) {
      return Buffer.from(value).toString("base64");
    },
    async _call(name, input) {
      if (name.endsWith("part_begin")) {
        assert.equal(input.length, expected.length);
        return { id: "stage", max_chunk_bytes: 1024 };
      }
      if (name.endsWith("part_write")) {
        const bytes = Buffer.from(input.body, "base64");
        assert.ok(bytes.length <= 1024);
        chunks.push(bytes);
        return null;
      }
      assert.equal(name, "workers.r2.part_finish");
      assert.deepEqual(Buffer.concat(chunks), expected);
      return { partNumber: 1, etag: "receipt" };
    },
  };
  await r2Binding(() => context, "BACKUPS")
    .resumeMultipartUpload("key", "upload")
    .uploadPart(1, text);
});

test("R2 short fixed-length sources discard the stage and release their reader", async () => {
  const { readable, writable } = new FixedLengthStream(3);
  let retired = false;
  const context = {
    _encode(value) {
      return Buffer.from(value).toString("base64");
    },
    async _call(name) {
      if (name.endsWith("part_begin"))
        return { id: "stage", max_chunk_bytes: 4 };
      if (name.endsWith("part_write")) return null;
      assert.equal(name, "workers.r2.part_cancel");
      retired = true;
    },
  };
  const upload = r2Binding(() => context, "BACKUPS")
    .resumeMultipartUpload("key", "upload")
    .uploadPart(1, readable);
  const produce = (async () => {
    const writer = writable.getWriter();
    try {
      await writer.write(Uint8Array.of(1, 2));
      await writer.close();
    } finally {
      writer.releaseLock();
    }
  })();
  const results = await Promise.allSettled([upload, produce]);
  assert.ok(
    results.every(
      (result) =>
        result.status === "rejected" &&
        result.reason.message.includes("too few bytes"),
    ),
  );
  assert.equal(retired, true);
  assert.equal(readable.locked, false);
});

test("R2 unsupported options and invalid parts refuse before native effects", async () => {
  let calls = 0;
  const bucket = r2Binding(
    () => ({
      _call() {
        calls++;
        throw new Error("unexpected native effect");
      },
    }),
    "BACKUPS",
  );
  for (const options of [
    { ssecKey: "unsupported" },
    { httpMetadata: new Headers() },
    { customMetadata: new Map() },
    { httpMetadata: { cacheExpiry: 42 } },
  ])
    await assert.rejects(
      bucket.createMultipartUpload("key", options),
      TypeError,
    );
  for (const options of [
    { onlyIf: {} },
    { range: new Headers({ range: "bytes=1-2" }) },
  ])
    await assert.rejects(bucket.get("key", options), TypeError);
  const upload = bucket.resumeMultipartUpload("key", "opaque-id");
  for (const number of [0, 1.5, 10001, NaN])
    await assert.rejects(
      upload.uploadPart(number, Uint8Array.of(1)),
      RangeError,
    );
  await assert.rejects(upload.uploadPart(1, {}), TypeError);
  await assert.rejects(
    upload.uploadPart(1, "bytes", { ssecKey: "unsupported" }),
    TypeError,
  );
  await assert.rejects(
    upload.complete([{ partNumber: 2, etag: "receipt" }]),
    TypeError,
  );
  assert.equal(calls, 0);
});

test("R2 binding declarations refuse invalid names and collisions", () => {
  const module = {
    default: {
      fetch() {
        return new Response();
      },
    },
  };
  for (const options of [
    { r2: "BACKUPS" },
    { r2: [""] },
    { r2: ["a\0b"] },
    { r2: ["SAME"], services: ["SAME"] },
    { r2: ["SAME"], variables: ["SAME"] },
  ])
    assert.throws(() => workersApplication(module, options), TypeError);
});
