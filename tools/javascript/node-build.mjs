#!/usr/bin/env node
// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// Preserve the module graph and Node's package resolution, including native
// addons, dynamic imports, package exports and CommonJS. Do not bundle packages.
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { rm } from "node:fs/promises";
import { dirname, resolve, relative, isAbsolute } from "node:path";
const project = resolve(process.argv[2] ?? "tsconfig.json");
const tsc = fileURLToPath(
  new URL("node_modules/typescript/bin/tsc", import.meta.url),
);
// Let the compiler resolve JSONC, extends and relative paths instead of parsing
// a reduced configuration language that differs from ordinary TypeScript.
const inspected = spawnSync(
  process.execPath,
  [tsc, "--showConfig", "--project", project],
  { encoding: "utf8", maxBuffer: 8 * 1024 * 1024 },
);
if (inspected.error || inspected.status !== 0)
  throw new Error(
    "TypeScript configuration failed: " + (inspected.stdout ?? ""),
  );
const config = JSON.parse(inspected.stdout),
  options = config.compilerOptions ?? {};
if (
  typeof options.outDir !== "string" ||
  options.noEmit ||
  options.emitDeclarationOnly
)
  throw new Error("Node builds require outDir and JavaScript emission");
const base = dirname(project),
  output = resolve(base, options.outDir),
  location = relative(base, output);
if (!location || location.startsWith("..") || isAbsolute(location))
  throw new Error("outDir must be inside the project");
// A failed typecheck must not leave stale generated code in a deliverable.
await rm(output, { recursive: true, force: true });
const result = spawnSync(
  process.execPath,
  [tsc, "--project", project, "--noEmitOnError"],
  { stdio: "inherit" },
);
if (result.error || result.status !== 0)
  throw new Error("Node TypeScript build failed");
