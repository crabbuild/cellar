// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import assert from "node:assert/strict";
import test from "node:test";
import { containerExec } from "../../packages/cellar-cloudflare/exec.js";

function fixture({
  limit = 32,
  stdout = [[0, 255]],
  stderr = [[42]],
  code = 7,
  killFinishes = true,
  blockOutput = false,
} = {}) {
  const calls = [],
    writes = [],
    work = [];
  const chunks = { read_stdout: [...stdout, []], read_stderr: [...stderr, []] };
  let closed;
  const eof = new Promise((resolve) => {
    closed = resolve;
  });
  async function waitForExit(signal) {
    let abort;
    try {
      await Promise.race([
        eof,
        new Promise((_, reject) => {
          abort = () => reject(signal.reason);
          signal?.addEventListener("abort", abort, { once: true });
          if (signal?.aborted) abort();
        }),
      ]);
    } finally {
      signal?.removeEventListener("abort", abort);
    }
  }
  const context = {
    _waitUntil(pending) {
      work.push(pending);
      pending.catch(() => {});
    },
    _sync: () => ({ max_body_bytes: limit }),
    _decode: (value) => Uint8Array.from(Buffer.from(value, "base64")),
    _encode: (value) => Buffer.from(value).toString("base64"),
    _containerMonitor(make) {
      const pending = make();
      work.push(pending);
      return pending;
    },
    _containerControl() {},
    async _call(name, input, _body, signal) {
      calls.push([name, input]);
      if (name === "container.exec") return 1;
      if (name === "container.release") return null;
      assert.equal(input.id, 1);
      const method = name.split(".").at(-1);
      if (method === "info")
        return { pid: 42, is_pty: Boolean(calls[0][1].options.pty) };
      if (chunks[method]) {
        if (blockOutput) await waitForExit(signal);
        return Buffer.from(chunks[method].shift()).toString("base64");
      }
      if (method === "write") writes.push(input.data);
      if (method === "close_stdin") closed();
      if (method === "kill" && killFinishes) closed();
      if (method === "wait") {
        await waitForExit(signal);
        return code;
      }
      return null;
    },
  };
  return { context, calls, writes, work, finish: () => closed() };
}

test("Workers exec closes omitted stdin and preserves binary output and nonzero exits", async () => {
  const { context, calls } = fixture();
  const process = await containerExec(context, ["tool", "literal; argument"]);
  assert.equal(process.isPty, false);
  assert.throws(() => process.resize(80, 24), TypeError);
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
    [["tool"], { pty: "terminal" }],
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

test("terminal exec defaults, metadata and resize preserve process control ordering", async () => {
  const { context, calls, finish } = fixture({
    stdout: [[65, 13, 10]],
    stderr: [],
  });
  const process = await containerExec(context, ["tool"], { pty: true });
  assert.deepEqual(calls[0][1].options.pty, { cols: 80, rows: 24 });
  assert.equal(calls[0][1].options.combined, true);
  assert.equal(process.pid, 42);
  assert.equal(process.isPty, true);
  assert.equal(process.stderr, null);
  assert.equal(process.resize(132, 40), undefined);
  const writer = process.stdin.getWriter();
  await writer.write(Uint8Array.of(42));
  writer.releaseLock();
  const resize = calls.findIndex(([name]) => name === "container.exec.resize");
  const write = calls.findIndex(([name]) => name === "container.exec.write");
  assert.ok(resize >= 0 && resize < write);
  assert.deepEqual(calls[resize][1], { id: 1, cols: 132, rows: 40 });
  finish();
  const output = await process.output();
  assert.deepEqual(new Uint8Array(output.stdout), Uint8Array.of(65, 13, 10));
  assert.equal(output.stderr.byteLength, 0);
  assert.equal(
    calls.filter(([name]) => name === "container.release").length,
    1,
  );
});

test("terminal shape and dimensions refuse before allocation; resize validates synchronously", async () => {
  const { context, calls, finish } = fixture();
  for (const options of [
    { pty: { unknown: 1 } },
    { pty: true, stderr: "pipe" },
    { pty: null },
  ])
    await assert.rejects(containerExec(context, ["tool"], options), TypeError);
  for (const pty of [
    { cols: 0 },
    { rows: 65536 },
    { cols: 1.5 },
    { rows: null },
  ])
    await assert.rejects(containerExec(context, ["tool"], { pty }), RangeError);
  assert.deepEqual(calls, []);
  const process = await containerExec(context, ["tool"], {
    pty: { cols: 100 },
  });
  assert.deepEqual(calls[0][1].options.pty, { cols: 100, rows: 24 });
  for (const dims of [
    [0, 1],
    [1, 65536],
    [undefined, undefined],
  ])
    assert.throws(() => process.resize(...dims), RangeError);
  assert.equal(
    calls.filter(([name]) => name === "container.exec.resize").length,
    0,
  );
  finish();
  await process.output();
});

test("metadata failure retires the accepted handle without replay or consuming source input", async () => {
  const { context, calls } = fixture();
  const call = context._call.bind(context);
  context._call = (name, ...args) =>
    name === "container.exec.info"
      ? Promise.reject(new Error("metadata unavailable"))
      : call(name, ...args);
  let pulled = 0;
  const input = new ReadableStream(
    {
      pull() {
        pulled++;
      },
    },
    { highWaterMark: 0 },
  );
  await assert.rejects(
    containerExec(context, ["tool"], { stdin: input }),
    /metadata unavailable/,
  );
  assert.equal(pulled, 0);
  assert.equal(input.locked, false);
  assert.equal(calls.filter(([name]) => name === "container.exec").length, 1);
  assert.equal(
    calls.filter(([name]) => name === "container.exec.kill").length,
    1,
  );
  assert.equal(
    calls.filter(([name]) => name === "container.release").length,
    1,
  );
});

test("void resize reserves event and callback ownership before native dispatch", async () => {
  for (const hook of ["_waitUntil", "_containerControl"]) {
    const { context, calls, finish, work } = fixture();
    const terminal = await containerExec(context, ["tool"], { pty: true });
    context[hook] = () => {
      throw new Error("retention refused");
    };
    assert.throws(() => terminal.resize(100, 30), /retention refused/);
    await Promise.allSettled(work);
    assert.equal(
      calls.filter(([name]) => name === "container.exec.resize").length,
      0,
    );
    finish();
    await terminal.output();
  }
});

test("void resize reports a lost acknowledgement through its event without replay", async () => {
  const { context, calls, finish, work } = fixture();
  const call = context._call.bind(context);
  context._call = async (name, ...args) => {
    const result = await call(name, ...args);
    if (name === "container.exec.resize")
      throw new Error("resize acknowledgement lost");
    return result;
  };
  const terminal = await containerExec(context, ["tool"], { pty: true });
  assert.equal(terminal.resize(100, 30), undefined);
  await assert.rejects(work.at(-1), /resize acknowledgement lost/);
  assert.equal(
    calls.filter(([name]) => name === "container.exec.resize").length,
    1,
  );
  finish();
  await terminal.output();
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

test("exec kill defaults to SIGTERM while abort remains an explicit SIGKILL", async () => {
  const { context, calls } = fixture();
  const process = await containerExec(context, ["tool"], { stdin: "pipe" });
  await process.kill();
  await process.stdin.close();
  await process.output();
  assert.deepEqual(
    calls
      .filter(([name]) => name === "container.exec.kill")
      .map(([, input]) => input),
    [{ id: 1, signal: 15 }],
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

test("exec source streams bytes through bounded native writes and closes stdin", async () => {
  const { context, calls, writes, work } = fixture({ limit: 3 });
  const source = new ReadableStream({
    start(controller) {
      controller.enqueue(Uint8Array.of(0, 255, 2, 3, 4, 5, 6));
      controller.enqueue(new Uint8Array());
      controller.enqueue(Uint8Array.of(7));
      controller.close();
    },
  });
  const process = await containerExec(context, ["tool"], { stdin: source });
  assert.equal(process.stdin, null);
  assert.equal((await process.output()).exitCode, 7);
  await Promise.all(work);
  assert.deepEqual(
    writes.map((value) => [...Buffer.from(value, "base64")]),
    [[0, 255, 2], [3, 4, 5], [6], [7]],
  );
  assert.equal(source.locked, false);
  assert.equal(
    calls.filter(([name]) => name === "container.exec.close_stdin").length,
    1,
  );
  assert.equal(
    calls.filter(([name]) => name === "container.release").length,
    1,
  );
});

test("exec source stops pulling while a native write awaits acknowledgement", async () => {
  const { context, work } = fixture();
  const entered = Promise.withResolvers(),
    release = Promise.withResolvers();
  const original = context._call.bind(context);
  context._call = async (name, ...args) => {
    if (name === "container.exec.write") {
      entered.resolve();
      await release.promise;
    }
    return original(name, ...args);
  };
  let pulls = 0;
  const source = new ReadableStream(
    {
      pull(controller) {
        pulls++;
        if (pulls === 1) controller.enqueue(Uint8Array.of(1));
        else controller.close();
      },
    },
    { highWaterMark: 0 },
  );
  const process = await containerExec(context, ["tool"], { stdin: source });
  await entered.promise;
  assert.equal(pulls, 1);
  assert.equal(source.locked, true);
  release.resolve();
  await process.output();
  await Promise.all(work);
  assert.equal(pulls, 2);
  assert.equal(source.locked, false);
});

test("exec refuses locked and invalid sources before allocation", async () => {
  const { context, calls } = fixture();
  const source = new ReadableStream();
  const reader = source.getReader();
  await assert.rejects(
    containerExec(context, ["tool"], { stdin: source }),
    TypeError,
  );
  reader.releaseLock();
  for (const stdin of [null, 1, "text", new Uint8Array(1)])
    await assert.rejects(
      containerExec(context, ["tool"], { stdin }),
      TypeError,
    );
  const controller = new AbortController();
  controller.abort();
  await assert.rejects(
    containerExec(context, ["tool"], {
      stdin: source,
      signal: controller.signal,
    }),
    { name: "AbortError" },
  );
  assert.equal(source.locked, false);
  assert.deepEqual(calls, []);
});

test("early process exit cancels a pending source and joins its cancellation", async () => {
  const { context, calls, work, finish } = fixture();
  const entered = Promise.withResolvers(),
    release = Promise.withResolvers();
  const source = new ReadableStream(
    {
      pull() {},
      async cancel() {
        entered.resolve();
        await release.promise;
      },
    },
    { highWaterMark: 0 },
  );
  const process = await containerExec(context, ["tool"], { stdin: source });
  finish();
  assert.equal(await process.exitCode, 7);
  await entered.promise;
  let completed = false;
  const output = process.output().then((value) => {
    completed = true;
    return value;
  });
  await Promise.resolve();
  assert.equal(completed, false);
  assert.equal(source.locked, true);
  release.resolve();
  assert.equal((await output).exitCode, 7);
  await Promise.all(work);
  assert.equal(source.locked, false);
  assert.equal(
    calls.filter(([name]) => name === "container.release").length,
    1,
  );
});

test("allocation failure unlocks a source without consuming or cancelling it", async () => {
  const { context } = fixture();
  let reads = 0,
    cancelled = false;
  const source = new ReadableStream(
    {
      pull() {
        reads++;
      },
      cancel() {
        cancelled = true;
      },
    },
    { highWaterMark: 0 },
  );
  const failure = new Error("allocation response lost");
  context._call = async () => {
    throw failure;
  };
  await assert.rejects(
    containerExec(context, ["tool"], { stdin: source }),
    (error) => error === failure,
  );
  assert.equal(reads, 0);
  assert.equal(cancelled, false);
  assert.equal(source.locked, false);
});

test("source lifetime admission refuses before allocation and returns its reader", async () => {
  for (const method of ["_containerMonitor", "_containerControl"]) {
    const { context, calls, work } = fixture();
    const failure = Object.assign(new Error("lifetime admission refused"), {
      code: "OperationLimit",
      outcome: "not_committed",
    });
    context[method] = () => {
      throw failure;
    };
    const source = new ReadableStream({}, { highWaterMark: 0 });
    await assert.rejects(
      containerExec(context, ["tool"], { stdin: source }),
      (error) => error === failure,
    );
    await Promise.all(work);
    assert.equal(source.locked, false);
    assert.deepEqual(calls, []);
  }
});

test("invalid source chunks preserve their error and retire native observation", async () => {
  const { context, calls, work } = fixture({
    killFinishes: false,
    blockOutput: true,
  });
  let cancelled = false;
  const source = new ReadableStream({
    start(controller) {
      controller.enqueue("not bytes");
    },
    cancel() {
      cancelled = true;
      throw new Error("secondary cancellation failure");
    },
  });
  const process = await containerExec(context, ["tool"], { stdin: source });
  await assert.rejects(process.output(), {
    name: "TypeError",
    message: "Exec stdin requires Uint8Array chunks",
  });
  await Promise.all(work);
  assert.equal(cancelled, true);
  assert.equal(source.locked, false);
  assert.equal(calls.filter(([name]) => name === "container.exec").length, 1);
  assert.equal(
    calls.filter(([name]) => name === "container.exec.kill").length,
    1,
  );
  assert.equal(
    calls.filter(([name]) => name === "container.release").length,
    1,
  );
});

test("abort cancels a pending source and signals only the original process", async () => {
  const { context, calls, work } = fixture();
  const entered = Promise.withResolvers();
  let cancellation;
  const source = new ReadableStream(
    {
      pull() {
        entered.resolve();
      },
      cancel(reason) {
        cancellation = reason;
      },
    },
    { highWaterMark: 0 },
  );
  const controller = new AbortController();
  const process = await containerExec(context, ["tool"], {
    stdin: source,
    signal: controller.signal,
  });
  await entered.promise;
  const reason = new Error("stop this input");
  controller.abort(reason);
  await process.output();
  await Promise.all(work);
  assert.equal(cancellation, reason);
  assert.equal(source.locked, false);
  assert.deepEqual(
    calls
      .filter(([name]) => name === "container.exec.kill")
      .map(([, input]) => input),
    [{ id: 1, signal: 9 }],
  );
});

test("lost native authority cancels an input read without another effect", async () => {
  const { context, calls, work } = fixture();
  const lost = Promise.withResolvers();
  const original = context._call.bind(context);
  context._call = (name, ...args) =>
    name === "container.exec.wait" ? lost.promise : original(name, ...args);
  let cancelled = false;
  const source = new ReadableStream(
    {
      pull() {},
      cancel() {
        cancelled = true;
      },
    },
    { highWaterMark: 0 },
  );
  const process = await containerExec(context, ["tool"], { stdin: source });
  const failure = Object.assign(new Error("event retired"), {
    code: "ContextExpired",
    outcome: "unknown",
  });
  lost.reject(failure);
  await assert.rejects(process.output(), (error) => error === failure);
  await Promise.all(work);
  assert.equal(cancelled, true);
  assert.equal(source.locked, false);
  assert.equal(
    calls.filter(([name]) =>
      [
        "container.exec.write",
        "container.exec.kill",
        "container.exec.close_stdin",
      ].includes(name),
    ).length,
    0,
  );
  assert.equal(
    calls.filter(([name]) => name === "container.release").length,
    1,
  );
});

test("a lost write acknowledgement cancels its source without replay", async () => {
  const { context, calls, work } = fixture();
  const failure = Object.assign(new Error("write acknowledgement lost"), {
    code: "Ambiguous",
    outcome: "unknown",
  });
  const original = context._call.bind(context);
  context._call = async (name, ...args) => {
    const result = await original(name, ...args);
    if (name === "container.exec.write") throw failure;
    return result;
  };
  let reason;
  const source = new ReadableStream({
    start(controller) {
      controller.enqueue(Uint8Array.of(1));
    },
    cancel(value) {
      reason = value;
    },
  });
  const process = await containerExec(context, ["tool"], { stdin: source });
  await assert.rejects(process.output(), (error) => error === failure);
  await Promise.all(work);
  assert.equal(reason, failure);
  assert.equal(source.locked, false);
  assert.equal(
    calls.filter(([name]) => name === "container.exec.write").length,
    1,
  );
  assert.equal(calls.filter(([name]) => name === "container.exec").length, 1);
  assert.equal(
    calls.filter(([name]) => name === "container.release").length,
    1,
  );
});

test("abort during an accepted write prevents the remaining source chunks", async () => {
  const { context, calls, work } = fixture({
    limit: 2,
    stdout: [],
    stderr: [],
  });
  const entered = Promise.withResolvers(),
    release = Promise.withResolvers();
  const original = context._call.bind(context);
  context._call = async (name, ...args) => {
    if (name === "container.exec.write") {
      entered.resolve();
      await release.promise;
    }
    return original(name, ...args);
  };
  const source = new ReadableStream({
    start(controller) {
      controller.enqueue(new Uint8Array(9));
    },
  });
  const controller = new AbortController();
  const process = await containerExec(context, ["tool"], {
    stdin: source,
    signal: controller.signal,
  });
  await entered.promise;
  controller.abort();
  release.resolve();
  await process.output();
  await Promise.all(work);
  assert.equal(source.locked, false);
  assert.equal(
    calls.filter(([name]) => name === "container.exec.write").length,
    1,
  );
  assert.equal(
    calls.filter(([name]) => name === "container.exec.kill").length,
    1,
  );
  assert.equal(
    calls.filter(([name]) => name === "container.release").length,
    1,
  );
});

test("source failure retires native handles while source cancellation is still pending", async () => {
  const { context, calls, work } = fixture();
  const entered = Promise.withResolvers(),
    release = Promise.withResolvers();
  const source = new ReadableStream({
    start(controller) {
      controller.enqueue("not bytes");
    },
    async cancel() {
      entered.resolve();
      await release.promise;
    },
  });
  const process = await containerExec(context, ["tool"], { stdin: source });
  const output = process.output();
  output.catch(() => {});
  try {
    await entered.promise;
    await new Promise((resolve) => setImmediate(resolve));
    assert.equal(source.locked, true);
    assert.equal(
      calls.filter(([name]) => name === "container.exec.kill").length,
      1,
    );
    assert.equal(
      calls.filter(([name]) => name === "container.release").length,
      1,
    );
  } finally {
    release.resolve();
    await Promise.allSettled([output, ...work]);
  }
  await assert.rejects(output, TypeError);
  assert.equal(source.locked, false);
});

test("output quota keeps its failure when source teardown also fails", async () => {
  const { context, calls, work } = fixture({ limit: 2 });
  const source = new ReadableStream(
    {
      pull() {},
      cancel() {
        throw new Error("secondary source cancellation error");
      },
    },
    { highWaterMark: 0 },
  );
  const process = await containerExec(context, ["tool"], { stdin: source });
  await assert.rejects(process.output(), RangeError);
  await Promise.all(work);
  assert.equal(source.locked, false);
  assert.equal(
    calls.filter(([name]) => name === "container.release").length,
    1,
  );
});

test("ignored output failures refuse output even after process exit resolves", async () => {
  const { context, calls, work } = fixture();
  const failure = Object.assign(new Error("lost stderr observation"), {
    code: "ContextExpired",
    outcome: "unknown",
  });
  const gate = Promise.withResolvers();
  const original = context._call;
  context._call = async (name, ...args) => {
    if (name === "container.exec.read_stderr") {
      await gate.promise;
      throw failure;
    }
    return original(name, ...args);
  };
  const process = await containerExec(context, ["tool"], { stderr: "ignore" });
  assert.equal(await process.exitCode, 7);
  gate.resolve();
  await assert.rejects(process.output(), (error) => error === failure);
  await Promise.all(work);
  assert.equal(
    calls.filter(([name]) => name === "container.release").length,
    1,
  );
});
