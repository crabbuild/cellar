// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, writeFile, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";
test("Node compiler accepts normal extended JSONC configs and removes stale output on errors", async () => {
  const root = await mkdtemp(join(tmpdir(), "cellar-node-build-"));
  try {
    await writeFile(
      join(root, "base.json"),
      JSON.stringify({
        compilerOptions: {
          target: "ES2023",
          module: "NodeNext",
          moduleResolution: "NodeNext",
          strict: true,
          types: [],
          outDir: "dist",
        },
      }),
    );
    await writeFile(
      join(root, "tsconfig.json"),
      '{\n// Standard TypeScript JSONC configuration\n"extends":"./base.json", "include":["app.ts"],\n}',
    );
    await writeFile(join(root, "app.ts"), "export const value: number = 4;");
    const build = () =>
      spawnSync(process.execPath, [
        fileURLToPath(new URL("node-build.mjs", import.meta.url)),
        join(root, "tsconfig.json"),
      ]);
    const passed = build();
    assert.equal(passed.status, 0, passed.stderr.toString());
    assert.match(await readFile(join(root, "dist", "app.js"), "utf8"), /value/);
    await writeFile(
      join(root, "app.ts"),
      'export const value: number = "invalid";',
    );
    assert.notEqual(build().status, 0);
    await assert.rejects(readFile(join(root, "dist", "app.js")), {
      code: "ENOENT",
    });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
