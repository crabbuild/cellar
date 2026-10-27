// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { spawnSync } from "node:child_process";
import test from "node:test";

const launcher = await readFile(
  new URL(
    "../../crates/cellar-javascript/runtime/node/bootstrap.mjs",
    import.meta.url,
  ),
  "utf8",
);

function launch(input) {
  return spawnSync(
    process.execPath,
    ["--input-type=module", "--eval", launcher],
    {
      input,
      encoding: "utf8",
    },
  );
}

test("large bootstrap code travels on the private pipe without URL or argv credentials", () => {
  const source = `export default async function(config) {
    const source = Buffer.from(import.meta.url.split(",")[1], "base64").toString("utf8");
    if (source.includes(config.secret) || process.execArgv.some(value => value.includes(config.secret)))
      throw new Error("authentication escaped its private argument");
    console.log(JSON.stringify({probe:config.probe, argvBytes:Math.max(...process.execArgv.map(value=>Buffer.byteLength(value)))}));
  }\n/*${"x".repeat(256 * 1024)}*/`;
  const result = launch(
    JSON.stringify({
      source,
      config: { secret: "private-bootstrap-fixture-token", probe: "é\u0000😀" },
    }),
  );
  assert.equal(result.status, 0, result.stderr);
  const observed = JSON.parse(result.stdout);
  assert.equal(observed.probe, "é\u0000😀");
  assert.ok(observed.argvBytes < 4096);
});

test("bootstrap parse and initialization failures cannot print private configuration", () => {
  const secret = "private-bootstrap-fixture-token";
  for (const input of [
    `{"source":"", "config":{"secret":"${secret}", broken}`,
    JSON.stringify({
      source:
        "export default function(config) {throw new Error(config.secret)}",
      config: { secret },
    }),
  ]) {
    const result = launch(input);
    assert.equal(result.status, 70);
    assert.equal(result.stdout, "");
    assert.equal(result.stderr, "");
  }
});

test("runtime error frames stay bounded before native RPC stack stripping", () => {
  const source = `import {serialize} from 'node:v8';
    export default async function() {
      try { await frameworkCall(); }
      catch(error) {
        const bytes=serialize(error);
        if(bytes.byteLength>2048) throw new Error('runtime error exceeded clone budget');
        console.log(JSON.stringify({message:error.message, stack:error.stack, bytes:bytes.byteLength}));
      }
    }
    async function frameworkCall(){throw new Error('deliberate upload failure')}
    /*${"x".repeat(256 * 1024)}*/`;
  const result = launch(JSON.stringify({ source, config: {} }));
  assert.equal(result.status, 0, result.stderr);
  const error = JSON.parse(result.stdout);
  assert.equal(error.message, "deliberate upload failure");
  assert.ok(error.bytes < 2048);
  assert.match(error.stack, /cellar-runtime\.mjs/);
  assert.doesNotMatch(error.stack, /data:text\/javascript/);
});
