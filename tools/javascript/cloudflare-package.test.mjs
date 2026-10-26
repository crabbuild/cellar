// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";
import { execFileSync, spawnSync } from "node:child_process";
import { fileURLToPath, pathToFileURL } from "node:url";

test("the packed Cloudflare facade imports and dispatches from its published files", async () => {
  const directory = await mkdtemp(join(tmpdir(), "cellar-cloudflare-package-"));
  try {
    const packed = JSON.parse(
      execFileSync(
        "npm",
        [
          "pack",
          "--ignore-scripts",
          "--json",
          "--pack-destination",
          directory,
          "--cache",
          join(directory, "cache"),
        ],
        {
          cwd: fileURLToPath(
            new URL("../../packages/cellar-cloudflare", import.meta.url),
          ),
          encoding: "utf8",
        },
      ),
    );
    assert.equal(packed.length, 1);
    const filename = packed[0].filename;
    assert.equal(basename(filename), filename);
    execFileSync("tar", ["-xzf", join(directory, filename), "-C", directory]);
    const entry = pathToFileURL(join(directory, "package/index.js")).href;
    const imported = spawnSync(
      process.execPath,
      [
        "--input-type=module",
        "--eval",
        `
      const { workersApplication } = await import(${JSON.stringify(entry)});
      const app = workersApplication({ default: { fetch: () => new Response(null) } }, { namespaces: {} });
      const result = await app.fetch({ url: "http://worker/", method: "GET", headers: [], body: new Uint8Array() }, {});
      if (result.status !== 200 || result.body !== "") throw new Error("packed module dispatch failed");
    `,
      ],
      { encoding: "utf8" },
    );
    assert.equal(imported.status, 0, imported.stdout + imported.stderr);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
