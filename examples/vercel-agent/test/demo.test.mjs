// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import test from "node:test";
import assert from "node:assert/strict";
import { createDemoServer } from "../script/server.mjs";

async function setup(t) {
  let observerToken = "private-test-token";
  const demo = createDemoServer({
    backend: () => undefined,
    token: () => observerToken,
    provider: "openai",
    model: "test-model",
    storage: {
      backend: "s3",
      bucket: "test-bucket",
      endpoint: "http://127.0.0.1:19000",
    },
    restart: async (reset) => {
      observerToken = "new-token";
      reset();
    },
  });
  await new Promise((resolve) => demo.server.listen(0, "127.0.0.1", resolve));
  t.after(() => demo.close());
  const origin = `http://127.0.0.1:${demo.server.address().port}`;
  return {
    origin,
    post: (path, value, headers = {}) =>
      fetch(origin + path, {
        method: "POST",
        headers: { origin, ...headers },
        body: JSON.stringify(value),
      }),
    event: (value) =>
      fetch(origin + "/internal/events", {
        method: "POST",
        headers: { authorization: `Bearer ${observerToken}` },
        body: JSON.stringify({ task: "task-1", attempt: 1, ...value }),
      }),
  };
}
async function frame(reader) {
  let text = "";
  while (!text.includes("\n\n")) {
    const next = await reader.read();
    if (next.done) throw new Error("stream closed");
    text += new TextDecoder().decode(next.value);
  }
  return JSON.parse(
    text
      .split("\n")
      .find((x) => x.startsWith("data: "))
      .slice(6),
  );
}
test("SSE sends provider progress before completion and reconnect restores preview without triggering work", async (t) => {
  const { origin, event } = await setup(t);
  const controller = new AbortController();
  t.after(() => controller.abort());
  const reply = await fetch(origin + "/api/tasks/task-1/events", {
    signal: controller.signal,
  });
  assert.match(reply.headers.get("content-type"), /text\/event-stream/);
  const reader = reply.body.getReader();
  assert.equal((await frame(reader)).type, "waiting");
  assert.equal(
    (await event({ type: "text", step: 1, text: "Hello" })).status,
    202,
  );
  const preview = await frame(reader);
  assert.equal(preview.text, "Hello");
  assert.equal(preview.deltaEvents, 1);
  const reconnect = await fetch(origin + "/api/tasks/task-1/events", {
    signal: controller.signal,
  });
  assert.equal((await frame(reconnect.body.getReader())).text, "Hello");
  await event({ type: "attempt", attempt: 2 });
  assert.equal((await frame(reader)).attempt, 2);
  await event({ type: "text", attempt: 1, text: "stale" });
  const again = await fetch(origin + "/api/tasks/task-1/events", {
    signal: controller.signal,
  });
  assert.equal((await frame(again.body.getReader())).attempt, 2);
});
test("local observer authentication, origin protection and worker restart fencing", async (t) => {
  const { origin, event, post } = await setup(t);
  assert.equal(
    (
      await post("/internal/events", {
        task: "task-1",
        attempt: 1,
        type: "text",
        text: "secret",
      })
    ).status,
    403,
  );
  assert.equal(
    (await post("/api/restart", {}, { origin: "https://untrusted.example" }))
      .status,
    403,
  );
  assert.equal(
    (await event({ type: "text", text: "a".repeat(32001) })).status,
    400,
  );
  assert.equal((await post("/api/restart", {})).status, 200);
  assert.equal(
    (
      await post(
        "/internal/events",
        { task: "task-1", attempt: 1, type: "text", text: "stale" },
        { authorization: "Bearer private-test-token" },
      )
    ).status,
    403,
  );
  assert.equal((await fetch(origin + "/api/tasks/task-1")).status, 503);
  const html = await fetch(origin + "/");
  assert.equal(html.status, 200);
  assert.match(
    html.headers.get("content-security-policy"),
    /script-src 'self'/,
  );
  assert.ok(!(await html.text()).includes("private-test-token"));
  assert.deepEqual(await (await fetch(origin + "/api/config")).json(), {
    provider: "openai",
    model: "test-model",
    storage: {
      backend: "s3",
      bucket: "test-bucket",
      endpoint: "http://127.0.0.1:19000",
    },
  });
});

test("launcher releases its directory lock after a spawn failure", async () => {
  const { mkdtemp, access, rm } = await import("node:fs/promises");
  const { tmpdir } = await import("node:os");
  const { join } = await import("node:path");
  const { execFile } = await import("node:child_process");
  const { promisify } = await import("node:util");
  const directory = await mkdtemp(join(tmpdir(), "ai-demo-launcher-"));
  try {
    await assert.rejects(
      promisify(execFile)(
        process.execPath,
        [new URL("../script/launch.mjs", import.meta.url).pathname],
        {
          env: {
            ...process.env,
            CELLAR_AI_HOST: join(directory, "missing-host"),
            CELLAR_AI_PROVIDER: "fixture",
            CELLAR_AI_DEMO_DATA: directory,
            CELLAR_AI_DEMO_PORT: "0",
          },
          timeout: 5000,
        },
      ),
      (error) =>
        error.code === 1 && /Worker failed to start/.test(error.stderr),
    );
    await assert.rejects(access(join(directory, "demo.lock")), {
      code: "ENOENT",
    });
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("observer token rotation also fences a request whose body arrives after restart", async (t) => {
  const { request } = await import("node:http");
  const { origin, post } = await setup(t);
  const payload = JSON.stringify({
    task: "task-1",
    attempt: 1,
    type: "text",
    text: "stale",
  });
  let sent;
  const headerSent = new Promise((resolve) => {
    sent = resolve;
  });
  const reply = new Promise((resolve, reject) => {
    const req = request(
      origin + "/internal/events",
      {
        method: "POST",
        headers: {
          authorization: "Bearer private-test-token",
          "content-length": Buffer.byteLength(payload),
        },
      },
      (res) => {
        res.resume();
        resolve(res.statusCode);
      },
    );
    req.on("error", reject);
    req.flushHeaders();
    // Wait for a socket write callback, then rotate before completing the body.
    req.write(payload.slice(0, 1), sent);
    void headerSent.then(async () => {
      try {
        assert.equal((await post("/api/restart", {})).status, 200);
        req.end(payload.slice(1));
      } catch (error) {
        req.destroy();
        reject(error);
      }
    });
  });
  assert.equal(await reply, 403);
});
