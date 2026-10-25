// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// Opt-in: runs a paid model task through an already running local demo.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { writeFile } from "node:fs/promises";
import { setTimeout as delay } from "node:timers/promises";

assert.equal(
  process.env.CELLAR_PI_TEST_LIVE,
  "1",
  "Set CELLAR_PI_TEST_LIVE=1 to authorize paid live qualification",
);
const origin = process.env.CELLAR_PI_DEMO_URL ?? "http://127.0.0.1:4317";
assert.equal(new URL(origin).hostname, "127.0.0.1");
const config = await (await fetch(`${origin}/api/config`)).json();
assert.equal(config.provider, "openai");
const task = `stream-${randomUUID()}`;
const prompt =
  "Add 13 to the counter once. Then explain the arithmetic and result in four short paragraphs.";
const events = [];
const abort = new AbortController();
const response = await fetch(`${origin}/api/tasks/${task}/events`, {
  signal: abort.signal,
});
assert.equal(response.status, 200);
let restart,
  restartRequested = false;
const consume = (async () => {
  let buffer = "";
  const decoder = new TextDecoder();
  for await (const bytes of response.body) {
    buffer += decoder.decode(bytes, { stream: true });
    let split;
    while ((split = buffer.indexOf("\n\n")) >= 0) {
      const frame = buffer.slice(0, split);
      buffer = buffer.slice(split + 2);
      const data = frame.split("\n").find((line) => line.startsWith("data: "));
      if (!data) continue;
      const event = JSON.parse(data.slice(6));
      events.push(event);
      // Interrupt directly on the first real delta, before a human/browser
      // round trip can allow the final response to finish.
      if (event.type === "text" && !restartRequested) {
        restartRequested = true;
        restart = fetch(`${origin}/api/restart`, {
          method: "POST",
          headers: { origin },
        }).then((reply) => assert.equal(reply.status, 200));
        restart.catch(() => {}); // Awaited below; prevent an interim unhandled rejection.
      }
    }
  }
})();
consume.catch(() => {});
try {
  const submit = await fetch(`${origin}/api/tasks/${task}`, {
    method: "POST",
    headers: { origin },
    body: JSON.stringify({ prompt }),
  });
  assert.equal(submit.status, 202);
  let result;
  const deadline = Date.now() + 150000;
  while (Date.now() < deadline) {
    try {
      const reply = await fetch(`${origin}/api/tasks/${task}`, {
        signal: AbortSignal.timeout(5000),
      });
      if (reply.ok) {
        const state = await reply.json();
        if (state.status === "failed") throw new Error("task failed");
        if (state.status === "completed") {
          result = state;
          break;
        }
      }
    } catch (error) {
      if (error.message === "task failed") throw error;
    }
    await delay(250);
  }
  assert.ok(result, "task did not complete");
  assert.ok(restartRequested, "no real provider text delta arrived");
  await restart;
  assert.equal(result.count, 13);
  assert.equal(result.toolReceipts, 1);
  assert.equal(result.modelSteps, 2);
  assert.equal(result.attempts, 2);
  assert.ok(events.some((e) => e.type === "text" && e.attempt === 1));
  assert.ok(events.some((e) => e.type === "text" && e.attempt === 2));
  assert.ok(
    events.some((e) => e.type === "tool" && e.replayed && e.attempt === 2),
  );
  assert.ok(
    !events.some(
      (e) => e.type === "checkpoint" && e.step === 1 && e.attempt === 1,
    ),
    "restart missed partial-generation boundary",
  );
  const duplicate = await fetch(`${origin}/api/tasks/${task}`, {
    method: "POST",
    headers: { origin },
    body: JSON.stringify({ prompt }),
  });
  assert.equal(duplicate.status, 200);
  assert.deepEqual(
    await (await fetch(`${origin}/api/tasks/${task}`)).json(),
    result,
  );
  const assistants = result.messages.filter((m) => m.role === "assistant");
  assert.ok(
    assistants.every(
      (m) => m.model === config.model && m.usage.totalTokens > 0,
    ),
  );
  if (process.env.CELLAR_PI_TEST_REPORT)
    await writeFile(
      process.env.CELLAR_PI_TEST_REPORT,
      JSON.stringify({ task, config, events, result }, null, 2),
    );
  console.log(
    `PASS ${task}: live deltas before checkpoint, interruption during generation, two attempts, one effect, receipt replay, identical duplicate result`,
  );
} finally {
  abort.abort();
  await consume.catch((error) => {
    if (error.name !== "AbortError") throw error;
  });
}
