// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import test from "node:test";
import assert from "node:assert/strict";
import { cp, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { check } from "./check.mjs";

test("the Vercel reference inputs retain the reviewed upstream identity", async () => {
  const manifest = await check();
  assert.equal(manifest.commit, "8910e6205b1c72e1ad6ebaa2ceae236a3aa4d87f");
  assert.equal(manifest.sdk_reference_version, "3.9.0");
});

test("the Vercel check refuses changed or missing protocol sources", async () => {
  const directory = await mkdtemp(join(tmpdir(), "cellar-vercel-pin-"));
  try {
    await cp(new URL("./", import.meta.url), directory, { recursive: true });
    const root = pathToFileURL(directory + "/");
    await writeFile(join(directory, "reference/api-client.ts"), "changed");
    await assert.rejects(check(root), /upstream bytes changed/);
    await rm(join(directory, "reference/api-client.ts"));
    await assert.rejects(check(root), /ENOENT/);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
