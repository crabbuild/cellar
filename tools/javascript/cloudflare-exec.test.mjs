// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import assert from "node:assert/strict";
import test from "node:test";
import { containerExec } from "../../packages/cellar-cloudflare/exec.js";

function fixture({
  limit = 32,
  stdout = [[0, 255]],
  stderr = [[42]],
  code = 7,
} = {}) {
  const calls = [],
    writes = [];
  const chunks = { read_stdout: [...stdout, []], read_stderr: [...stderr, []] };
  let closed;
  const eof = new Promise((resolve) => {
    closed = resolve;
  });
  const context = {
    _sync: () => ({ max_body_bytes: limit }),
    _decode: (value) => Uint8Array.from(Buffer.from(value, "base64")),
    _encode: (value) => Buffer.from(value).toString("base64"),
    async _call(name, input) {
      calls.push([name, input]);
      if (name === "container.exec") return 1;
      if (name === "container.release") return null;
      assert.equal(input.id, 1);
      const method = name.split(".").at(-1);
      if (chunks[method])
        return Buffer.from(chunks[method].shift()).toString("base64");
      if (method === "write") writes.push(input.data);
      if (method === "close_stdin") closed();
      if (method === "wait") {
        await eof;
        return code;
      }
      return null;
    },
  };
  return { context, calls, writes };
}

test("Workers exec closes omitted stdin and preserves binary output and nonzero exits", async () => {
  const { context, calls } = fixture();
  const process = await containerExec(context, ["tool", "literal; argument"]);
  const output = await process.output();
  assert.deepEqual(new Uint8Array(output.stdout), Uint8Array.of(0, 255));
  assert.deepEqual(new Uint8Array(output.stderr), Uint8Array.of(42));
  assert.equal(output.exitCode, 7);
  assert.equal(await process.exitCode, 7);
  assert.deepEqual(calls[0][1].options.cmd, ["tool", "literal; argument"]);
  assert.equal(
    calls.filter(([name]) => name === "container.release").length,
    1,
  );
  await assert.rejects(process.output(), TypeError);
});

test("Workers exec pipes stdin in bounded chunks and drains ignored stderr", async () => {
  const { context, writes } = fixture({ limit: 2 });
  const process = await containerExec(context, ["tool"], {
    stdin: "pipe",
    stderr: "ignore",
  });
  const writer = process.stdin.getWriter();
  await writer.write(Uint8Array.of(0, 1, 2, 3, 4));
  await writer.close();
  writer.releaseLock();
  const output = await process.output();
  assert.equal(process.stderr, null);
  assert.equal(output.stderr.byteLength, 0);
  assert.deepEqual(
    writes.map((value) => Array.from(Buffer.from(value, "base64"))),
    [[0, 1], [2, 3], [4]],
  );
});

test("invalid modes and pre-aborted exec refuse before native allocation", async () => {
  const { context, calls } = fixture();
  for (const [cmd, options] of [
    [[], {}],
    [[""], {}],
    [["-tool"], {}],
    [["tool"], { pty: true }],
    [["tool"], { stdout: "ignore", stderr: "combined" }],
    [["tool"], { unknown: true }],
    [["tool"], { user: "1000" }],
    [["tool"], { user: "root:root" }],
  ]) {
    await assert.rejects(containerExec(context, cmd, options), TypeError);
  }
  const controller = new AbortController();
  controller.abort();
  await assert.rejects(
    containerExec(context, ["tool"], { signal: controller.signal }),
    { name: "AbortError" },
  );
  assert.deepEqual(calls, []);
});

test("output quota failure releases the process without replay or successful truncation", async () => {
  const { context, calls } = fixture({ limit: 2 });
  const process = await containerExec(context, ["tool"]);
  await assert.rejects(process.output(), RangeError);
  assert.equal(calls.filter(([name]) => name === "container.exec").length, 1);
  assert.equal(
    calls.filter(([name]) => name === "container.release").length,
    1,
  );
});

test("an abort after observed process exit never signals a replacement", async () => {
  const { context, calls } = fixture();
  const controller = new AbortController();
  const process = await containerExec(context, ["tool"], {
    signal: controller.signal,
  });
  await process.output();
  controller.abort();
  await Promise.resolve();
  assert.equal(
    calls.filter(([name]) => name === "container.exec.kill").length,
    0,
  );
});

test("argv and stream policy are captured before allocation acknowledgement", async () => {
  const { context, calls } = fixture();
  let acknowledge;
  const gate = new Promise((resolve) => {
    acknowledge = resolve;
  });
  const original = context._call.bind(context);
  context._call = async (name, input, ...rest) => {
    if (name === "container.exec") await gate;
    return original(name, input, ...rest);
  };
  const cmd = ["original"],
    options = { stdout: "pipe" };
  const starting = containerExec(context, cmd, options);
  cmd[0] = "changed";
  options.stdout = "ignore";
  acknowledge();
  const process = await starting;
  assert.ok(process.stdout instanceof ReadableStream);
  assert.deepEqual(calls[0][1].options.cmd, ["original"]);
  await process.output();
});
