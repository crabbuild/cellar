// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import assert from "node:assert/strict";
import test from "node:test";
import { manifest, verify } from "./inventory.mjs";
test("Sandbox 1.x profile pins its SDK, helper, base image and input bytes", () =>
  verify());
test("SDK, helper, lock and source drift refuse before fixture build", async () => {
  for (const change of [
    (value) => (value.sdk.version = "0.12.9"),
    (value) => (value.helper_image += "-changed"),
    (value) => (value.lock_sha256 = "0".repeat(64)),
    (value) => (value.files["fixture.mjs"] = "0".repeat(64)),
    (value) => (value.files["terminal-program.mjs"] = "0".repeat(64)),
  ]) {
    const value = structuredClone(manifest);
    change(value);
    await assert.rejects(verify(value));
  }
});
