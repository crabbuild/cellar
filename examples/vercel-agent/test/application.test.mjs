// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import test from "node:test";
import assert from "node:assert/strict";
import { application } from "../dist/application.js";
import { fixtureModel } from "../dist/provider.js";
import { accept, execute } from "../dist/durable.js";

function setup(parent) {
  const data = new Map();
  const ctx = {
    identity: { name: "child" },
    now: async () => 1000,
    cell: () => ({
      fetch: async () => ({ status: parent ? 200 : 404, json: () => parent }),
    }),
    storage: {
      read: async (key) =>
        data.has(key)
          ? { present: true, value: structuredClone(data.get(key)) }
          : { present: false, value: null },
      apply: async (batch) => {
        for (const [key, expected] of Object.entries(batch.checks ?? {}))
          assert.deepEqual(await ctx.storage.read(key), expected);
        for (const [key, value] of Object.entries(batch.puts ?? {}))
          data.set(key, structuredClone(value));
      },
      sync: async () => {},
    },
  };
  const model = fixtureModel();
  const cell = new (application(model).cells.DurableAgent)();
  return {
    ctx,
    model,
    data,
    fetch: (method, body) => cell.fetch({ method, json: () => body }, ctx),
  };
}
const decode = (reply) =>
  JSON.parse(Buffer.from(reply.body, "base64").toString());

test("follow-up history comes from a completed durable parent and participates in idempotency", async () => {
  const messages = [
    { role: "user", content: "Old prompt" },
    { role: "assistant", content: [{ type: "text", text: "Old answer" }] },
  ];
  const h = setup({ status: "completed", messages });
  assert.equal(
    (await h.fetch("POST", { prompt: "Follow up", previous: "parent" })).status,
    202,
  );
  assert.deepEqual(h.data.get("run").history, messages);
  assert.equal(
    (await h.fetch("POST", { prompt: "Follow up", previous: "another" }))
      .status,
    409,
  );
  assert.equal(
    (await h.fetch("POST", { prompt: "Follow up", previous: "parent" })).status,
    202,
  );
});

test("missing, pending, self-referencing and oversized parents refuse acceptance", async () => {
  for (const [parent, previous, status] of [
    [null, "parent", 409],
    [{ status: "pending", messages: [] }, "parent", 409],
    [{ status: "completed", messages: [] }, "child", 400],
    [
      {
        status: "completed",
        messages: [{ role: "assistant", content: "界".repeat(70000) }],
      },
      "parent",
      413,
    ],
  ]) {
    const h = setup(parent);
    assert.equal(
      (await h.fetch("POST", { prompt: "Follow up", previous })).status,
      status,
    );
    assert.equal(h.data.size, 0);
  }
});

test("status strips reasoning and provider metadata without modifying the durable journal", async () => {
  const h = setup();
  await accept(h.ctx, "Add three", h.model);
  await execute(h.ctx, h.model);
  h.data.get("run").messages.push({
    role: "assistant",
    content: [
      { type: "reasoning", text: "private reasoning" },
      {
        type: "text",
        text: "Public answer",
        providerOptions: { openai: { itemId: "private-id" } },
      },
    ],
  });
  const reply = decode(await h.fetch("GET"));
  assert.ok(!JSON.stringify(reply).includes("private"));
  assert.ok(JSON.stringify(h.data.get("run")).includes("private reasoning"));
  assert.equal(reply.count, 3);
});
