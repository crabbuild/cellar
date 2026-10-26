// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import test from "node:test";
import assert from "node:assert/strict";
import { cp, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { check } from "./check.mjs";

test("the E2B reference inputs retain the reviewed upstream identity", async () => {
  const manifest = await check();
  assert.equal(manifest.commit, "a2b2caea4498766aee8fd33d3280b56744070e99");
  assert.equal(manifest.sdk_reference_version, "2.59.0");
});

test("the E2B check refuses changed or missing protocol sources", async () => {
  const directory = await mkdtemp(join(tmpdir(), "cellar-e2b-pin-"));
  try {
    await cp(new URL("./", import.meta.url), directory, { recursive: true });
    const root = pathToFileURL(directory + "/");
    await writeFile(
      join(directory, "reference/envd_process_process.proto"),
      "changed",
    );
    await assert.rejects(check(root), /upstream bytes changed/);
    await rm(join(directory, "reference/envd_process_process.proto"));
    await assert.rejects(check(root), /ENOENT/);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
