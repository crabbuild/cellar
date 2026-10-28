// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { resolve } from "node:path";
export const root = fileURLToPath(new URL("./", import.meta.url));
export const manifest = JSON.parse(
  await readFile(resolve(root, "manifest.json"), "utf8"),
);
const sha = (bytes) => createHash("sha256").update(bytes).digest("hex");
export async function verify(value = manifest) {
  assert.equal(value.format, 1);
  assert.equal(value.profile, "cloudflare-sandbox-1.0.3-files");
  const pkg = JSON.parse(await readFile(resolve(root, "package.json"), "utf8"));
  assert.equal(pkg.dependencies["@cloudflare/sandbox"], value.sdk.version);
  const lockBytes = await readFile(resolve(root, "package-lock.json"));
  assert.equal(sha(lockBytes), value.lock_sha256, "lock drift");
  const lock =
    JSON.parse(lockBytes).packages["node_modules/@cloudflare/sandbox"];
  assert.equal(lock.version, value.sdk.version);
  assert.equal(lock.integrity, value.sdk.integrity);
  assert.equal(lock.resolved, value.sdk.tarball);
  const installed = JSON.parse(
    await readFile(
      resolve(root, "node_modules/@cloudflare/sandbox/package.json"),
      "utf8",
    ),
  );
  assert.equal(installed.version, value.sdk.version);
  for (const [path, digest] of Object.entries(value.files))
    assert.equal(
      sha(await readFile(resolve(root, path))),
      digest,
      `${path} drift`,
    );
  const dockerfile = await readFile(resolve(root, "Dockerfile"), "utf8");
  assert.ok(dockerfile.includes(`FROM ${value.base_image}`));
  assert.ok(dockerfile.includes(`COPY --from=${value.helper_image} `));
  assert.equal(value.platform, "linux/amd64");
}
