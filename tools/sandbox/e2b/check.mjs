#!/usr/bin/env node
// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

export async function check(root = new URL("./", import.meta.url)) {
  const manifest = JSON.parse(
    await readFile(new URL("manifest.json", root), "utf8"),
  );
  assert.equal(manifest.format, 1);
  assert.equal(manifest.provider, "e2b");
  assert.equal(manifest.repository, "https://github.com/e2b-dev/E2B");
  assert.match(manifest.commit, /^[a-f0-9]{40}$/);
  assert.equal(manifest.qualification, "not_run");
  assert.equal(manifest.files.length, 5);
  const paths = new Set();
  for (const file of manifest.files) {
    assert.match(file.path, /^[a-zA-Z0-9_.-]+$/);
    assert(!paths.has(file.path), "duplicate pinned file");
    paths.add(file.path);
    assert.match(file.git_blob, /^[a-f0-9]{40}$/);
    const bytes = await readFile(new URL(`reference/${file.path}`, root));
    const blob = createHash("sha1")
      .update(`blob ${bytes.length}\0`)
      .update(bytes)
      .digest("hex");
    assert.equal(blob, file.git_blob, `upstream bytes changed: ${file.path}`);
  }
  return manifest;
}

if (
  process.argv[1] &&
  resolve(process.argv[1]) === fileURLToPath(import.meta.url)
) {
  const manifest = await check();
  console.log(
    `Verified ${manifest.files.length} E2B protocol inputs at ${manifest.commit}; live qualification: ${manifest.qualification}.`,
  );
}
