// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, writeFile, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { bundle } from "./build.mjs";
import { response, request, utf8 } from "../../packages/cellar-sdk/index.js";

async function project(source, action) {
  const directory = await mkdtemp(join(tmpdir(), "cellar-javascript-"));
  try {
    await writeFile(join(directory, "app.ts"), source);
    await writeFile(
      join(directory, "tsconfig.json"),
      JSON.stringify({
        compilerOptions: {
          target: "ES2023",
          module: "ESNext",
          moduleResolution: "Bundler",
          strict: true,
          noEmit: true,
          lib: ["ES2023"],
        },
        include: ["app.ts"],
      }),
    );
    await writeFile(
      join(directory, "package-lock.json"),
      '{"lockfileVersion":3}',
    );
    await writeFile(
      join(directory, "javascript.json"),
      JSON.stringify({
        entry: "app.ts",
        output: "app.js",
        tsconfig: "tsconfig.json",
      }),
    );
    await action(directory);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
}
test("unicode and binary bodies have exact bytes", () => {
  assert.equal(response("hé😀").body, Buffer.from("hé😀").toString("base64"));
  for (let length = 0; length < 20; length++) {
    const bytes = Uint8Array.from({ length }, (_, i) => i * 11);
    assert.equal(
      request("http://test/", { body: bytes }).body,
      Buffer.from(bytes).toString("base64"),
    );
  }
  assert.deepEqual(utf8("\ud800"), new Uint8Array([239, 191, 189]));
});
test("bundle is reproducible and failed typecheck removes stale executable input", async () => {
  await project("export const count: number = 1;", async (directory) => {
    const file = join(directory, "javascript.json");
    const first = await bundle(file),
      second = await bundle(file);
    assert.equal(first.bundle_sha256, second.bundle_sha256);
    assert.match(
      await readFile(join(directory, "app.js"), "utf8"),
      /CellarApp/,
    );
    await writeFile(
      join(directory, "app.ts"),
      'export const count: number = "bad";',
    );
    await assert.rejects(bundle(file), /TypeScript validation failed/);
    await assert.rejects(readFile(join(directory, "app.js")), {
      code: "ENOENT",
    });
  });
});
test("Node imports are rejected even when application supplies ambient declarations", async () => {
  await project(
    '// @ts-ignore\nimport {readFile} from "node:fs"; export {readFile};',
    async (directory) => {
      await assert.rejects(
        bundle(join(directory, "javascript.json")),
        /Unsupported runtime import/,
      );
    },
  );
});
test("nonliteral runtime imports are rejected", async () => {
  await project(
    'const name = "./later.js"; export async function load() {return import(name)}',
    async (directory) => {
      await assert.rejects(
        bundle(join(directory, "javascript.json")),
        /argument is not a string literal/,
      );
    },
  );
});
