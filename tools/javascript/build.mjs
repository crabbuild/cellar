#!/usr/bin/env node
// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// Build-time tooling only. Serving requires neither Node nor node_modules.
import {build} from "esbuild";
import {spawnSync} from "node:child_process";
import {readFile, writeFile, rename, rm} from "node:fs/promises";
import {dirname, resolve} from "node:path";
import {createHash} from "node:crypto";
import {fileURLToPath} from "node:url";

export async function bundle(projectFile) {
  const base = dirname(resolve(projectFile));
  const config = JSON.parse(await readFile(projectFile, "utf8"));
  for (const key of Object.keys(config)) if (!["entry", "output", "tsconfig"].includes(key)) throw new Error(`Unknown build field: ${key}`);
  for (const key of ["entry", "output", "tsconfig"]) if (typeof config[key] !== "string") throw new Error(`Missing ${key}`);
  const output = resolve(base, config.output);
  // Failed typecheck/build must not leave a stale executable input available.
  await rm(output, {force: true});
  await rm(output + ".json", {force: true});
  const tsc = fileURLToPath(new URL("node_modules/typescript/bin/tsc", import.meta.url));
  const checked = spawnSync(process.execPath, [tsc, "--noEmit", "--project", resolve(base, config.tsconfig)], {stdio: "inherit"});
  if (checked.error || checked.status !== 0) throw new Error("TypeScript validation failed");
  const result = await build({
    absWorkingDir: base, entryPoints: [config.entry], outfile: output,
    bundle: true, write: false, format: "iife", globalName: "CellarApp",
    platform: "neutral", target: "es2023", mainFields: ["module", "main"],
    conditions: ["cellar", "production"], metafile: true, sourcemap: false,
    logOverride: {"unsupported-dynamic-import": "error", "unsupported-require-call": "error"},
    plugins: [{name: "cellar-runtime-contract", setup(build) {
      build.onResolve({filter: /^(node:|https?:|npm:)|\.node$/}, args => ({errors: [{text: `Unsupported runtime import: ${args.path}`}]}));
    }}],
  });
  for (const info of Object.values(result.metafile.outputs)) {
    if (info.imports.length) throw new Error("Unresolved or dynamic runtime import");
  }
  // Non-literal require/import cannot be resolved into a closed executable.
  // esbuild emits these warnings rather than errors, so make them fatal.
  if (result.warnings.length) throw new Error("Bundle contains warnings; resolve all runtime dependencies");
  const bytes = result.outputFiles[0].contents;
  const lock = await readFile(resolve(base, "package-lock.json"));
  const inventory = {format: 1, engine: "v8@152.2.0", profile: "cellar-buffered-v1",
    bundle_sha256: createHash("sha256").update(bytes).digest("hex"),
    package_lock_sha256: createHash("sha256").update(lock).digest("hex"),
    inputs: Object.keys(result.metafile.inputs).sort()};
  await writeFile(output + ".tmp", bytes);
  await writeFile(output + ".json", JSON.stringify(inventory, null, 2) + "\n");
  await rename(output + ".tmp", output);
  return inventory;
}
if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  if (process.argv.length !== 3) throw new Error("Usage: node tools/javascript/build.mjs path/to/javascript.json");
  await bundle(process.argv[2]);
}
