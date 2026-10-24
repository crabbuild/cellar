import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdtemp, readFile, rm, access, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { setTimeout as delay } from "node:timers/promises";

const program = process.env.CELLAR_AI_HOST;
assert.ok(
  program,
  "CELLAR_AI_HOST must name the built cellar-vercel-agent-example executable",
);
const live = process.env.CELLAR_AI_TEST_LIVE === "1";
if (live) {
  assert.equal(
    process.env.CELLAR_AI_PROVIDER,
    "openai",
    "live test requires CELLAR_AI_PROVIDER=openai",
  );
  assert.ok(
    process.env.OPENAI_API_KEY && process.env.CELLAR_AI_MODEL,
    "live test needs a key and model",
  );
}
const evidence = [];
const entry = fileURLToPath(new URL("./crash-fixture.mjs", import.meta.url));
const productionEntry = fileURLToPath(
  new URL("../dist/app.js", import.meta.url),
);
const root = await mkdtemp(join(tmpdir(), "cellar-ai-"));
const children = new Set();

async function until(check, description, timeout = 60_000) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    const value = await check();
    if (value) return value;
    await delay(50);
  }
  throw new Error(`Timed out: ${description}`);
}

async function start(boundary, generation) {
  let stdout = "",
    stderr = "";
  const child = spawn(
    resolve(program),
    ["--listen", "127.0.0.1:0", "--internal-listen", "127.0.0.1:0"],
    {
      env: {
        ...process.env,
        CELLAR_NODE_ENTRY: boundary === "concurrent" ? productionEntry : entry,
        CELLAR_NODE_EXECUTABLE: process.execPath,
        CELLAR_AI_PROVIDER: live ? "openai" : "fixture",
        CELLAR_NODE: `ai-${boundary}-${generation}`,
        CELLAR_TEST_DATA_DIR: join(root, `${boundary}-${generation}-data`),
        CELLAR_BUCKET: "cellar-dev",
        CELLAR_INTERNAL_DEV_STORE: join(root, `${boundary}-bucket.sqlite`),
        CELLAR_DURABILITY: "bucket",
        CELLAR_TOKIO_THREADS: "2",
        CELLAR_PLACEMENT_WEIGHT: "1",
        CELLAR_WAKER_TICK_MS: "250",
        CELLAR_REBALANCE_INTERVAL_MS: "0",
        CELLAR_HANDLER_BUDGET_S: "60",
        CELLAR_SHUTDOWN_TOTAL_MS: "8000",
        CELLAR_AI_PAUSE_AT: generation === 1 ? boundary : "",
        CELLAR_AI_MARKER: join(root, `${boundary}.marker`),
        CELLAR_AI_CALLS: join(root, `${boundary}.calls`),
        CELLAR_AI_COMPLETIONS: join(root, `${boundary}.completed`),
        RUST_LOG: "warn",
      },
      stdio: ["ignore", "pipe", "pipe"],
    },
  );
  children.add(child);
  const exited = new Promise((resolve) =>
    child.once("exit", (code, signal) => resolve({ code, signal })),
  );
  let spawnError;
  child.on("error", (error) => {
    spawnError = error;
  });
  child.stdout.on("data", (chunk) => {
    stdout += chunk;
  });
  child.stderr.on("data", (chunk) => {
    stderr += chunk;
  });
  const addresses = await until(async () => {
    if (spawnError) throw spawnError;
    if (child.exitCode !== null || child.signalCode !== null)
      throw new Error(`Host exited: ${stdout}\n${stderr}`);
    const ingress = /cellar listening on (\S+)/.exec(stdout)?.[1];
    const internal = /cellar internal listening on (\S+)/.exec(stdout)?.[1];
    return ingress && internal
      ? { ingress: `http://${ingress}`, internal: `http://${internal}` }
      : null;
  }, "host startup");
  await until(async () => {
    try {
      return (await fetch(`${addresses.ingress}/.well-known/cellar/health`)).ok;
    } catch {
      return false;
    }
  }, "host readiness");
  return { ...addresses, child, exited, logs: () => `${stdout}\n${stderr}` };
}

async function stop(node) {
  assert.ok((await fetch(`${node.internal}/shutdown`, { method: "POST" })).ok);
  const outcome = await Promise.race([
    node.exited,
    delay(15_000, undefined, { ref: false }).then(() => {
      throw new Error("shutdown timeout");
    }),
  ]);
  assert.equal(outcome.code, 0, node.logs());
  children.delete(node.child);
}

async function completed(url) {
  return until(async () => {
    const reply = await fetch(url);
    if (!reply.ok) return null;
    const value = await reply.json();
    assert.notEqual(value.status, "failed", JSON.stringify(value));
    return value.status === "completed" ? value : null;
  }, `completed ${url}`);
}

try {
  const boundaries =
    live && process.env.CELLAR_AI_TEST_ALL_BOUNDARIES !== "1"
      ? ["tool", "completed"]
      : ["accepted", "provider", "model", "tool", "final-model", "completed"];
  for (const boundary of boundaries) {
    const first = await start(boundary, 1);
    const url = `${first.ingress}/agents/task`;
    // Acceptance can itself lose its response. Recovery uses the same task ID.
    const submission = fetch(url, {
      method: "POST",
      body: JSON.stringify({ prompt: "Add three" }),
    }).then(
      (reply) => {
        assert.ok(reply.ok);
      },
      () => {},
    );
    await until(async () => {
      if (first.child.exitCode !== null) throw new Error(first.logs());
      try {
        await access(join(root, `${boundary}.marker`));
        return true;
      } catch {
        return false;
      }
    }, `checkpoint ${boundary}`);
    first.child.kill("SIGKILL");
    await first.exited;
    await submission;
    children.delete(first.child);

    // A different node and empty local directory must recover from the bucket.
    const second = await start(boundary, 2);
    const recoveredUrl = `${second.ingress}/agents/task`;
    const expectedCalls = boundary === "provider" ? 3 : 2;
    // Do not route a task request to wake the cell. The durable alarm must
    // resume it independently after the former owner's lease expires.
    await until(async () => {
      try {
        return (
          (
            await readFile(join(root, `${boundary}.completed`), "utf8")
          ).trim() === "completed"
        );
      } catch {
        return false;
      }
    }, "recovery without a task HTTP request");
    const result = await until(async () => {
      const reply = await fetch(recoveredUrl);
      if (!reply.ok) return null; // Prior owner lease may still need to expire.
      const value = await reply.json();
      if (value.status === "failed")
        throw new Error(
          `Agent failed: ${JSON.stringify(value)}\n${second.logs()}`,
        );
      return value.status === "completed" ? value : null;
    }, "alarm-driven recovery");
    assert.equal(result.count, 3);
    assert.equal(result.toolReceipts, 1);
    assert.equal(result.modelSteps, 2);
    assert.ok(result.messages.some((message) => message.role === "tool"));
    if (live) {
      const assistants = result.messages.filter(
        (message) => message.role === "assistant",
      );
      assert.equal(assistants.length, 2);
      assert.ok(
        result.usage.every(
          (usage) =>
            usage.inputTokens.total > 0 && usage.outputTokens.total > 0,
        ),
      );
      assert.ok(
        assistants
          .at(-1)
          .content.some(
            (part) => part.type === "text" && /3|three/i.test(part.text),
          ),
      );
    }
    const calls = (await readFile(join(root, `${boundary}.calls`), "utf8"))
      .trim()
      .split("\n");
    assert.equal(
      calls.length,
      expectedCalls,
      "only an uncheckpointed provider request may repeat",
    );
    const repeat = await fetch(recoveredUrl, {
      method: "POST",
      body: JSON.stringify({ prompt: "Add three" }),
    });
    assert.equal(repeat.status, 200);
    const conflict = await fetch(recoveredUrl, {
      method: "POST",
      body: JSON.stringify({ prompt: "Add four" }),
    });
    assert.equal(conflict.status, 409);
    assert.equal((await (await fetch(recoveredUrl)).json()).count, 3);
    assert.equal((await fetch(`${second.ingress}/agents/other`)).status, 404);
    await stop(second);
    evidence.push({ boundary, providerCalls: calls.length, result });
    console.log(
      `PASS ${boundary}: SIGKILL, cold bucket recovery, one tool effect, ${expectedCalls} provider calls, idempotent retry`,
    );
  }
  if (!live) {
    // Exercise the production entry with concurrent independent cells. This is
    // a bounded integration scenario, not a maximum-capacity benchmark.
    const first = await start("concurrent", 1);
    const missing = `${first.ingress}/agents/missing`;
    assert.equal((await fetch(missing)).status, 404);
    for (const body of [
      "{",
      "{}",
      '{"prompt":" "}',
      JSON.stringify({ prompt: "x".repeat(8001) }),
    ]) {
      assert.equal(
        (await fetch(missing, { method: "POST", body })).status,
        400,
      );
    }
    assert.equal((await fetch(missing, { method: "PUT" })).status, 405);
    assert.equal((await fetch(`${first.ingress}/wrong`)).status, 404);
    const started = performance.now();
    const results = await Promise.all(
      Array.from({ length: 16 }, async (_, id) => {
        const url = `${first.ingress}/agents/task-${id}`;
        assert.equal(
          (await fetch(url, { method: "POST", body: '{"prompt":"Add three"}' }))
            .status,
          202,
        );
        const result = await completed(url);
        assert.equal(result.count, 3);
        assert.equal(result.modelSteps, 2);
        assert.equal(result.toolReceipts, 1);
        assert.deepEqual(
          result.messages.map((message) => message.role),
          ["user", "assistant", "tool", "assistant"],
        );
        return result;
      }),
    );
    const elapsedMs = Math.round(performance.now() - started);
    first.child.kill("SIGKILL");
    await first.exited;
    children.delete(first.child);
    const second = await start("concurrent", 2);
    await Promise.all(
      Array.from({ length: 32 }, async (_, index) => {
        const id = index % results.length;
        const url = `${second.ingress}/agents/task-${id}`;
        const duplicate = await until(async () => {
          const reply = await fetch(url, {
            method: "POST",
            body: '{"prompt":"Add three"}',
          });
          return reply.ok ? reply : null;
        }, "duplicate submission on replacement owner");
        assert.equal(duplicate.status, 200);
        assert.deepEqual(await completed(url), results[id]);
      }),
    );
    await stop(second);
    evidence.push({
      boundary: "concurrent",
      tasks: 16,
      duplicates: 32,
      elapsedMs,
    });
    console.log(
      `PASS production entry: 16 concurrent tasks in ${elapsedMs} ms, cold recovery, 32 unchanged duplicate results`,
    );
  }
  if (process.env.CELLAR_AI_TEST_REPORT) {
    await writeFile(
      process.env.CELLAR_AI_TEST_REPORT,
      JSON.stringify({ live, evidence }, null, 2),
    );
  }
} finally {
  await Promise.all(
    [...children].map(async (child) => {
      if (child.exitCode === null && child.signalCode === null) {
        const exited = new Promise((resolve) => child.once("exit", resolve));
        child.kill("SIGKILL");
        await exited;
      }
    }),
  );
  await rm(root, { recursive: true, force: true });
}
