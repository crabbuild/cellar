// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import {
  mkdtemp,
  rm,
  readFile,
  access,
  writeFile,
  mkdir,
  readdir,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { setTimeout as delay } from "node:timers/promises";
import { createDemoServer } from "../script/server.mjs";
import { recoveryStore, workerState } from "./recovery-store.mjs";

const host = process.env.CELLAR_AI_HOST;
assert.ok(host, "set CELLAR_AI_HOST");
const live = process.env.CELLAR_AI_TEST_LIVE === "1";
if (live) assert.ok(process.env.OPENAI_API_KEY && process.env.CELLAR_AI_MODEL);
const root = await mkdtemp(join(tmpdir(), "cellar-tool-recovery-"));
const evidence = [];
const children = new Set();
const prompt =
  "Use all six tools in this turn: calculate 24 times 7; look up durability; increment the counter by 13 exactly once; save a note titled Recovery with the reference summary; create one normal-priority ticket titled Verify recovery; then readState after the writes. Finally explain all verified results in a Markdown table and four short paragraphs. Do not add any text before the tool calls.";

async function until(fn, description, timeout = 120000) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    const result = await fn();
    if (result) return result;
    await delay(50);
  }
  throw new Error(`Timed out: ${description}`);
}

async function scenario(boundary) {
  const filesystem = boundary.startsWith("filesystem");
  const taskPrompt = filesystem
    ? "Exercise the bounded cell filesystem tools, including a stale-version refusal."
    : prompt;
  const directory = join(root, boundary);
  await mkdir(directory);
  const store = await recoveryStore(directory, boundary);
  const restarts = [];
  let backend,
    generation = 0,
    token = randomUUID(),
    current;
  const events = [];
  let streamRestart,
    interrupted = false;
  const demo = createDemoServer({
    directory,
    backend: () => backend,
    token: () => token,
    provider: live ? "openai" : "fixture",
    model: process.env.CELLAR_AI_MODEL ?? "fixture",
    restart: async () => {
      throw new Error("test owns restarts");
    },
  });
  await new Promise((resolve) => demo.server.listen(0, "127.0.0.1", resolve));
  const origin = `http://127.0.0.1:${demo.server.address().port}`;
  const marker = join(directory, "marker");
  const completions = join(directory, "completed");
  const calls = join(directory, "calls");

  async function start() {
    generation++;
    token = randomUUID();
    backend = undefined;
    const data = join(directory, `node-${generation}`);
    await mkdir(data);
    assert.deepEqual(await readdir(data), [], "replacement must start empty");
    const child = spawn(
      resolve(host),
      ["--listen", "127.0.0.1:0", "--internal-listen", "127.0.0.1:0"],
      {
        env: {
          ...store.env,
          CELLAR_NODE_ENTRY: fileURLToPath(
            new URL("./tool-crash-fixture.mjs", import.meta.url),
          ),
          CELLAR_NODE_EXECUTABLE: process.execPath,
          CELLAR_NODE: `tools-${boundary}-${generation}`,
          CELLAR_AI_PROVIDER: live ? "openai" : "fixture",
          CELLAR_AI_MODEL: process.env.CELLAR_AI_MODEL ?? "gpt-6-luna",
          CELLAR_AI_TEST_LIVE: filesystem ? "0" : live ? "1" : "0",
          CELLAR_AI_FILESYSTEM: filesystem ? "1" : "0",
          CELLAR_TEST_DATA_DIR: data,
          CELLAR_TOKIO_THREADS: "2",
          CELLAR_PLACEMENT_WEIGHT: "1",
          CELLAR_REBALANCE_INTERVAL_MS: "0",
          CELLAR_WAKER_TICK_MS: "250",
          CELLAR_HANDLER_BUDGET_S: "120",
          CELLAR_SHUTDOWN_TOTAL_MS: "8000",
          CELLAR_AI_EVENTS_URL: `${origin}/internal/events`,
          CELLAR_AI_EVENTS_TOKEN: token,
          CELLAR_AI_SERVICES_URL: origin,
          CELLAR_AI_SERVICES_TOKEN: token,
          CELLAR_AI_CALLS: calls,
          CELLAR_AI_COMPLETIONS: completions,
          CELLAR_AI_MARKER: marker,
          CELLAR_AI_PAUSE_AT:
            generation === 1 && (boundary === "external" || filesystem)
              ? boundary
              : "",
          RUST_LOG: "warn",
        },
        stdio: ["ignore", "pipe", "pipe"],
      },
    );
    children.add(child);
    let output = "",
      stderr = "",
      error;
    child.stdout.on("data", (value) => {
      output = (output + value).slice(-32000);
    });
    child.stderr.on("data", (value) => {
      stderr = (stderr + value).slice(-32000);
    });
    child.on("error", (value) => {
      error = value;
    });
    const exited = new Promise((resolve) =>
      child.once("close", (code, signal) => resolve({ code, signal })),
    );
    current = { child, exited, data, logs: () => output + stderr };
    await until(
      async () => {
        if (error) throw error;
        if (child.exitCode !== null || child.signalCode !== null)
          throw new Error("host exited during startup");
        const address = /cellar listening on (\S+)/.exec(output)?.[1];
        if (!address) return;
        try {
          if (
            (
              await fetch(`http://${address}/.well-known/cellar/health`, {
                signal: AbortSignal.timeout(1000),
              })
            ).ok
          ) {
            backend = `http://${address}`;
            return true;
          }
        } catch {}
      },
      "host ready",
      30000,
    );
  }
  async function kill() {
    backend = undefined;
    current.child.kill("SIGKILL");
    await current.exited;
    children.delete(current.child);
  }

  async function coldRestart() {
    await kill();
    const beforeRestart = await store.inspect();
    const deleted = current.data;
    const rows = await workerState(deleted, { filesystem });
    assert.ok(
      (await readdir(deleted)).length > 0,
      "old worker must have local files to erase",
    );
    await rm(deleted, { recursive: true });
    await assert.rejects(access(deleted), { code: "ENOENT" });
    restarts.push({
      localStateDeleted: true,
      replacementStartedEmpty: true,
      beforeRestart,
      workerState: rows,
    });
    await start();
  }
  const abort = new AbortController();
  let consuming;
  try {
    await start();
    const stream = await fetch(`${origin}/api/tasks/task/events`, {
      signal: abort.signal,
    });
    consuming = (async () => {
      const decoder = new TextDecoder();
      let buffer = "";
      for await (const chunk of stream.body) {
        buffer += decoder.decode(chunk, { stream: true });
        let at;
        while ((at = buffer.indexOf("\n\n")) >= 0) {
          const frame = buffer.slice(0, at);
          buffer = buffer.slice(at + 2);
          const line = frame
            .split("\n")
            .find((line) => line.startsWith("data: "));
          if (!line) continue;
          const event = JSON.parse(line.slice(6));
          events.push(event);
          if (boundary === "stream" && event.type === "text" && !interrupted) {
            interrupted = true;
            streamRestart = coldRestart();
            streamRestart.catch(() => {});
          }
        }
      }
    })();
    consuming.catch(() => {});
    const post = await fetch(`${origin}/api/tasks/task`, {
      method: "POST",
      headers: { origin },
      body: JSON.stringify({ prompt: taskPrompt }),
    });
    assert.equal(post.status, 202);
    if (boundary === "external" || filesystem) {
      await until(async () => {
        try {
          await access(marker);
          return true;
        } catch {}
      }, "remote effect committed");
      if (!filesystem)
        assert.equal(
          (await (await fetch(`${origin}/api/tickets`)).json()).effects,
          1,
        );
      await coldRestart();
    }
    // Wait on a test observation file, without a task GET that could activate it.
    await until(async () => {
      // S3 inspection and physical deletion can outlast a polling interval.
      // Join the deliberate restart before checking the replacement's health.
      if (streamRestart) await streamRestart;
      if (current.child.exitCode !== null || current.child.signalCode !== null)
        throw new Error("recovery host stopped");
      try {
        return (await readFile(completions, "utf8")).includes("completed");
      } catch {}
    }, "alarm-driven completion without task HTTP");
    if (boundary === "stream") {
      assert.ok(interrupted, "no live text arrived");
      await streamRestart;
    }
    const result = await (await fetch(`${origin}/api/tasks/task`)).json();
    assert.equal(result.status, "completed", JSON.stringify(result));
    assert.equal(result.count, filesystem ? 0 : 13);
    assert.equal(result.attempts, 2);
    assert.equal(result.toolReceipts, filesystem ? 9 : 6);
    assert.deepEqual(
      result.receipts.map((item) => item.name).sort(),
      filesystem
        ? [
            "deleteFile",
            "editFile",
            "editFile",
            "listFiles",
            "readFile",
            "readFile",
            "renameFile",
            "searchFiles",
            "writeFile",
          ]
        : [
            "calculate",
            "createTicket",
            "increment",
            "lookup",
            "readState",
            "saveNote",
          ],
    );
    if (!filesystem)
      assert.equal(
        result.receipts.find((item) => item.name === "calculate").result.value,
        168,
      );
    const tickets = await (await fetch(`${origin}/api/tickets`)).json();
    assert.equal(tickets.effects, filesystem ? 0 : 1);
    assert.equal(
      tickets.requests,
      filesystem ? 0 : boundary === "external" ? 2 : 1,
    );
    if (filesystem) {
      const reads = result.receipts.filter((item) => item.name === "readFile");
      assert.equal(reads[0].result.text, "first contents");
      assert.equal(reads[1].result.text, "other contents");
      const edits = result.receipts.filter((item) => item.name === "editFile");
      assert.equal(edits[0].result.inserted_bytes, 5);
      assert.deepEqual(edits[1].result.error, {
        code: "Conflict",
        errno: "EBUSY",
        outcome: "not_committed",
      });
      assert.equal(
        result.receipts.find((item) => item.name === "listFiles").result.entries
          .length,
        1,
      );
      assert.equal(
        result.receipts.find((item) => item.name === "searchFiles").result
          .matches.length,
        1,
      );
      const captured = restarts[0].workerState[0].files[0];
      assert.equal(captured.inode, reads[0].result.inode);
      if (boundary === "filesystem") {
        assert.equal(
          captured.version,
          reads[0].result.version,
          "recovery must replay the write receipt without rewriting the file",
        );
        assert.equal(
          Buffer.from(captured.prefix, "base64").toString(),
          "first contents",
        );
      } else {
        assert.ok(BigInt(captured.version) > BigInt(reads[0].result.version));
        assert.equal(
          Buffer.from(captured.prefix, "base64").toString(),
          "other contents",
        );
      }
    }
    if (boundary === "stream") {
      assert.ok(
        events.some((event) => event.type === "text" && event.attempt === 1),
      );
      assert.ok(
        events.some((event) => event.type === "text" && event.attempt === 2),
      );
      assert.ok(
        events.some(
          (event) =>
            event.type === "tool" && event.replayed && event.attempt === 2,
        ),
      );
      const step = events.find(
        (event) => event.type === "text" && event.attempt === 1,
      ).step;
      assert.ok(
        !events.some(
          (event) =>
            event.type === "checkpoint" &&
            event.step === step &&
            event.attempt === 1,
        ),
        "interruption must precede the final model checkpoint",
      );
    }
    if (live && !filesystem)
      assert.ok(
        result.usage.every(
          (item) => item.inputTokens.total > 0 && item.outputTokens.total > 0,
        ),
      );
    const beforeCalls = (await readFile(calls, "utf8"))
      .trim()
      .split("\n").length;
    assert.equal(
      beforeCalls,
      result.modelSteps + (boundary === "stream" ? 1 : 0),
      "only the interrupted, uncheckpointed model response may repeat",
    );
    const recoveredRows = await workerState(current.data, { filesystem });
    assert.equal(recoveredRows.length, 1);
    assert.equal(recoveredRows[0].count, filesystem ? 0 : 13);
    assert.deepEqual(
      recoveredRows[0].notes,
      filesystem
        ? []
        : [result.receipts.find((item) => item.name === "saveNote").input],
    );
    if (filesystem) assert.deepEqual(recoveredRows[0].files, []);
    if (boundary === "stream")
      assert.deepEqual(recoveredRows, restarts[0].workerState);
    assert.equal(
      (
        await fetch(`${origin}/api/tasks/task`, {
          method: "POST",
          headers: { origin },
          body: JSON.stringify({ prompt: taskPrompt }),
        })
      ).status,
      200,
    );
    assert.deepEqual(
      await (await fetch(`${origin}/api/tasks/task`)).json(),
      result,
    );
    assert.equal(
      (await readFile(calls, "utf8")).trim().split("\n").length,
      beforeCalls,
    );
    const afterRestart = await store.inspect();
    if (afterRestart.backend === "s3") {
      const previous = restarts[0].beforeRestart.ownership[0];
      const current = afterRestart.ownership.find(
        (item) => item.key === previous.key,
      );
      assert.ok(
        current.epoch > previous.epoch,
        "cold recovery must acquire a new epoch",
      );
      assert.notEqual(current.node, previous.node);
    }
    evidence.push({
      boundary,
      model: process.env.CELLAR_AI_MODEL ?? "fixture",
      tickets,
      providerCalls: beforeCalls,
      result,
      events,
      restarts,
      afterRestart,
      workerState: recoveredRows,
    });
    console.log(
      `PASS ${boundary}: ${result.toolReceipts} receipts, two attempts, count ${result.count}, ${tickets.effects} external effects, ${tickets.requests} downstream requests, unchanged duplicate`,
    );
  } finally {
    abort.abort();
    await consuming?.catch(() => {});
    await streamRestart?.catch(() => {});
    if (
      current &&
      current.child.exitCode === null &&
      current.child.signalCode === null
    )
      await kill();
    await demo.close();
  }
}

try {
  await scenario("external");
  await scenario("filesystem");
  await scenario("filesystem-read");
  // A provider fixture can finish synchronously before a socket round trip.
  // Streaming interruption is qualified against the real provider only.
  if (live) await scenario("stream");
  if (process.env.CELLAR_AI_TEST_REPORT)
    await writeFile(
      process.env.CELLAR_AI_TEST_REPORT,
      JSON.stringify({ live, evidence }, null, 2),
    );
} finally {
  await Promise.all(
    [...children].map(async (child) => {
      if (child.exitCode !== null || child.signalCode !== null) return;
      const exited = new Promise((resolve) => child.once("close", resolve));
      child.kill("SIGKILL");
      await exited;
    }),
  );
  await rm(root, { recursive: true, force: true });
}
