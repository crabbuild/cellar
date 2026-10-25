// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// Reuse Cellar's closed V8 bundler and the pinned TypeScript CLI. A failed
// compile removes old outputs so a stale fixture cannot pass an integration test.
import { bundle } from "../javascript/build.mjs";
import { spawnSync } from "node:child_process";
import { cp, rm } from "node:fs/promises";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("./", import.meta.url));
const output = fileURLToPath(new URL("dist/node", import.meta.url));
await rm(output, {recursive:true, force:true});
const tsc = fileURLToPath(new URL("node_modules/typescript/bin/tsc", import.meta.url));
const result = spawnSync(process.execPath, [tsc, "--project", "tsconfig.json", "--noEmitOnError"], {cwd:root, stdio:"inherit"});
if (result.error || result.status !== 0) throw Error("RPC fixture typecheck/build failed");
await cp(new URL("fixtures/generated/javascript/", import.meta.url), new URL("dist/node/fixtures/generated/javascript/", import.meta.url), {recursive:true});
await bundle(fileURLToPath(new URL("javascript.json", import.meta.url)));
await bundle(fileURLToPath(new URL("javascript-stale.json", import.meta.url)));
await bundle(fileURLToPath(new URL("javascript-worker.json", import.meta.url)));
