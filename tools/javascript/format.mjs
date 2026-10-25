#!/usr/bin/env node
// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { readdir, readFile, writeFile } from "node:fs/promises";
import { resolve, relative } from "node:path";
import { fileURLToPath } from "node:url";
import { format } from "prettier";

const mode = process.argv[2];
if (process.argv.length !== 3 || !["--check", "--write"].includes(mode)) {
  throw new Error("Usage: node tools/javascript/format.mjs --check|--write");
}

const root = fileURLToPath(new URL("../../", import.meta.url));
const config = JSON.parse(
  await readFile(new URL(".prettierrc.json", import.meta.url), "utf8"),
);
const sources = [
  "crates/cellar-javascript/runtime",
  "crates/cellar-javascript/tests/support",
  "packages/cellar-cloudflare",
  "tools/javascript",
  "tools/sandbox",
];
const upstream = resolve(root, "tools/sandbox/compat/fixtures/celld");
const factories = new Set(
  [
    "crates/cellar-javascript/runtime/runner.js",
    "crates/cellar-javascript/runtime/delegates.js",
    "crates/cellar-javascript/runtime/encoding.js",
    "crates/cellar-javascript/runtime/node/budget.js",
  ].map((path) => resolve(root, path)),
);

async function* files(directory) {
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const path = resolve(directory, entry.name);
    // Generated bundles and pinned upstream inputs retain their original bytes.
    if (["node_modules", "dist"].includes(entry.name) || path === upstream)
      continue;
    if (entry.isDirectory()) yield* files(path);
    else if (entry.isFile() && /\.(?:[cm]?[jt]s|[jt]sx)$/.test(entry.name))
      yield path;
  }
}

let checked = 0;
let changed = 0;
for (const source of sources) {
  for await (const path of files(resolve(root, source))) {
    const original = await readFile(path, "utf8");
    let formatted = await format(original, { ...config, filepath: path });
    if (factories.has(path)) {
      // Rust and the JS harness embed these files inside parentheses. Keep
      // Prettier's internal statement semicolons, but omit the expression's
      // final semicolon so it remains valid in that surrounding expression.
      if (!formatted.endsWith(";\n"))
        throw new Error(`Expected factory expression: ${path}`);
      formatted = formatted.slice(0, -2) + "\n";
    }
    checked++;
    if (original === formatted) continue;
    changed++;
    console.log(relative(root, path));
    if (mode === "--write") await writeFile(path, formatted);
  }
}
console.log(
  `${checked} sources checked; ${changed} ${mode === "--write" ? "formatted" : "need formatting"}.`,
);
if (mode === "--check" && changed) process.exitCode = 1;
