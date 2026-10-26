// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, mkdir, writeFile, rm, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawnSync } from "node:child_process";
import { pack } from "./node-package.mjs";
test("relocatable Node package verifies every input and rejects tampering", async () => {
  const root = await mkdtemp(join(tmpdir(), "cellar-node-package-"));
  try {
    await mkdir(join(root, "app"));
    await writeFile(
      join(root, "app", "index.mjs"),
      "export const fetch=()=>null;",
    );
    await writeFile(join(root, "host"), "fixture native executable");
    const config = join(root, "package.json"),
      output = join(root, "delivered");
    await writeFile(
      config,
      JSON.stringify({
        host: "host",
        application: "app",
        entry: "index.mjs",
        files: ["index.mjs"],
      }),
    );
    const result = await pack(config, output);
    assert.equal(result.node, process.versions.node);
    assert.equal(
      spawnSync(join(output, "bin", "node"), [join(output, "verify.mjs")])
        .status,
      0,
    );
    await assert.rejects(pack(config, output), /already exists/);
    await writeFile(join(output, "app", "index.mjs"), "changed");
    const verified = spawnSync(join(output, "bin", "node"), [
      join(output, "verify.mjs"),
    ]);
    assert.notEqual(verified.status, 0);
    assert.match(verified.stderr.toString(), /checksum mismatch/);
    await writeFile(
      config,
      JSON.stringify({
        host: "host",
        application: "app",
        entry: "../escape",
        files: ["index.mjs"],
      }),
    );
    await assert.rejects(pack(config, join(root, "refused")), /contained/);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("runtime-only preparation is verified without embedding a Rust host", async () => {
  const root = await mkdtemp(join(tmpdir(), "cellar-node-runtime-"));
  try {
    await mkdir(join(root, "app"));
    await writeFile(
      join(root, "app", "index.mjs"),
      "export const fetch=()=>null;",
    );
    const config = join(root, "runtime.json"),
      output = join(root, "runtime");
    await writeFile(
      config,
      JSON.stringify({
        application: "app",
        entry: "index.mjs",
        files: ["index.mjs"],
      }),
    );
    const result = await pack(config, output, { runtimeOnly: true });
    assert.ok(result.files["bin/node"]);
    assert.equal(result.files["bin/cellar-host"], undefined);
    assert.equal(result.files["launch.sh"], undefined);
    assert.equal(
      spawnSync(join(output, "bin", "node"), [join(output, "verify.mjs")])
        .status,
      0,
    );
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
