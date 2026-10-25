// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { digest, root, verify, verifyFiles } from "./inventory.mjs";

test("real published packages and upstream reference retain exact identities", async () => {
  const manifest = JSON.parse(await readFile(join(root, "manifest.json"), "utf8"));
  const report = await verify(manifest);
  assert.deepEqual(report.packages, ["@cloudflare/containers@0.3.7", "@cloudflare/sandbox@0.12.9"]);
  assert.equal(report.conformance, "not_run");
  assert.deepEqual(report.externals, ["cloudflare:workers", "node:path/posix"]);
});

test("source drift and fixture path escape refuse verification", async () => {
  const directory = await mkdtemp(join(tmpdir(), "cellar-sandbox-fixture-"));
  try {
    await writeFile(join(directory, "source.js"), "original");
    const files = [{ path: "source.js", sha256: digest("original") }];
    await verifyFiles(files, directory);
    await writeFile(join(directory, "source.js"), "changed");
    await assert.rejects(verifyFiles(files, directory), /identity changed/);
    await assert.rejects(verifyFiles([{ path: "../outside", sha256: "" }], directory), /escapes/);
  } finally { await rm(directory, { recursive: true, force: true }); }
});

test("unreviewed dependency lock and package version cannot inherit compatibility inventory", async () => {
  const manifest = JSON.parse(await readFile(join(root, "manifest.json"), "utf8"));
  await assert.rejects(verify({ ...manifest, lock_sha256: "0".repeat(64) }), /lock changed/);
  const wrong = structuredClone(manifest);
  wrong.packages[1].version = "1.0.0";
  await assert.rejects(verify(wrong), /identity changed/);
});

test("a different image digest cannot reuse the captured sandbox image evidence", async () => {
  const manifest = JSON.parse(await readFile(join(root, "manifest.json"), "utf8"));
  manifest.image.reference = "docker.io/cloudflare/sandbox@sha256:" + "0".repeat(64);
  await assert.rejects(verify(manifest), /image manifest changed/);
});
