// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import test from "node:test";
import assert from "node:assert/strict";
import {spawnSync} from "node:child_process";
import {fileURLToPath} from "node:url";

test("TypeScript container APIs cover binary streams and upgrade discrimination", () => {
  const checked = spawnSync(process.execPath, [
    fileURLToPath(new URL("node_modules/typescript/bin/tsc", import.meta.url)),
    "--noEmit", "--strict", "--target", "ES2023", "--module", "NodeNext",
    "--moduleResolution", "NodeNext",
    fileURLToPath(new URL("containers.types.ts", import.meta.url)),
  ], {encoding: "utf8"});
  assert.equal(checked.status, 0, checked.stdout + checked.stderr);
});

test("TypeScript Workers profile preserves RPC and storage types", () => {
  const checked = spawnSync(process.execPath, [
    fileURLToPath(new URL("node_modules/typescript/bin/tsc", import.meta.url)),
    "--noEmit", "--strict", "--target", "ES2023", "--module", "NodeNext",
    "--moduleResolution", "NodeNext",
    fileURLToPath(new URL("cloudflare.types.ts", import.meta.url)),
  ], {encoding: "utf8"});
  assert.equal(checked.status, 0, checked.stdout + checked.stderr);
});
