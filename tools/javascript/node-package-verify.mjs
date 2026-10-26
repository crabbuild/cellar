// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { readFile, readdir, lstat } from "node:fs/promises";
import { createHash } from "node:crypto";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
const root = dirname(fileURLToPath(import.meta.url));
const manifest = JSON.parse(
  await readFile(join(root, "node-package.json"), "utf8"),
);
if (
  manifest.format !== 1 ||
  manifest.kind !== "cellar-node-directory" ||
  manifest.platform !== process.platform ||
  manifest.arch !== process.arch ||
  manifest.node !== process.versions.node
)
  throw new Error("Node package runtime or platform mismatch");
const seen = new Set();
async function walk(base) {
  for (const entry of await readdir(join(root, base), {
    withFileTypes: true,
  })) {
    const path = base ? base + "/" + entry.name : entry.name;
    if (path === "node-package.json") continue;
    const full = join(root, path);
    const info = await lstat(full);
    if (info.isDirectory()) {
      await walk(path);
      continue;
    }
    if (!info.isFile())
      throw new Error("Package contains unsupported file: " + path);
    const expected = manifest.files[path];
    if (!expected) throw new Error("Untracked package file: " + path);
    const actual = createHash("sha256")
      .update(await readFile(full))
      .digest("hex");
    if (actual !== expected.sha256 || info.size !== expected.bytes)
      throw new Error("Package checksum mismatch: " + path);
    seen.add(path);
  }
}
await walk("");
if (seen.size !== Object.keys(manifest.files).length)
  throw new Error("Package file missing");
