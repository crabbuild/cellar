// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import assert from "node:assert/strict";
import test from "node:test";
import { r2Binding } from "../../packages/cellar-cloudflare/r2.js";
import { workersApplication } from "../../packages/cellar-cloudflare/index.js";

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
