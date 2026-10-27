// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import assert from "node:assert/strict";
import test from "node:test";
import { accept, execute, readRun } from "../dist/durable.js";
import { fixtureModel } from "../dist/provider.js";

function storage() {
  const data = new Map();
  let alarm;
  const ctx = {
    now: async () => 1000,
    fs: {
      capabilities: async () => ({
        transport: { max_value_bytes: 2 * 1024 * 1024 },
      }),
    },
    storage: {
      read: async (key) =>
        data.has(key)
          ? { present: true, value: structuredClone(data.get(key)) }
          : { present: false, value: null },
      apply: async (batch) => {
        for (const [key, expected] of Object.entries(batch.checks ?? {})) {
          assert.deepEqual(
            await ctx.storage.read(key),
            expected,
            "storage check failed",
          );
        }
        for (const [key, value] of Object.entries(batch.puts ?? {}))
          data.set(key, structuredClone(value));
        if (batch.alarm_at_ms !== undefined) alarm = batch.alarm_at_ms;
        if (batch.delete_alarm) alarm = undefined;
      },
      sync: async () => {},
    },
  };
  return { ctx, data, alarm: () => alarm };
}

for (const boundary of ["model", "tool", "final-model", "completed"]) {
  test(`replay after interruption at ${boundary} keeps one effect and reuses decisions`, async () => {
    const { ctx, data, alarm } = storage();
    const provider = fixtureModel();
    const generate = provider.doGenerate;
    let calls = 0;
    provider.doGenerate = (...args) => {
      calls++;
      return generate(...args);
    };
    await accept(ctx, "Add three", provider);
    let models = 0;
    await assert.rejects(
      execute(ctx, provider, async (point) => {
        if (point === "tool")
          await new Promise((resolve) => setTimeout(resolve, 15));
        if (point === "model" && ++models === 2 && boundary === "final-model")
          throw new Error("injected interruption");
        if (point === boundary) throw new Error("injected interruption");
      }),
      /injected interruption/,
    );
    await execute(ctx, provider);
    const run = await readRun(ctx);
    assert.equal(run.status, "completed");
    assert.equal(data.get("count"), 3);
    assert.equal(Object.keys(run.tools).length, 1);
    assert.equal(calls, 2);
    assert.equal(alarm(), undefined);
    assert.ok(run.messages.some((item) => item.role === "tool"));
    await execute(ctx, provider);
    assert.equal(calls, 2);
  });
}

test("a lost acknowledgement after atomic tool commit is reconciled on replay", async () => {
  const { ctx, data } = storage();
  const provider = fixtureModel();
  await accept(ctx, "Add three", provider);
  let lost = false;
  ctx.storage.sync = async () => {
    if (data.has("count") && !lost) {
      lost = true;
      throw new Error("unknown outcome");
    }
  };
  await assert.rejects(execute(ctx, provider), /unknown outcome/);
  await execute(ctx, provider);
  assert.equal(data.get("count"), 3);
  assert.equal((await readRun(ctx)).status, "completed");
});

test("task acceptance is idempotent and rejects prompt reuse", async () => {
  const { ctx } = storage();
  const provider = fixtureModel();
  await accept(ctx, "Add three", provider);
  assert.equal(await accept(ctx, "Add four", provider), null);
  await execute(ctx, provider);
  assert.equal((await accept(ctx, "Add three", provider)).status, "completed");
});

test("activity time changes with durable checkpoints, never reads or duplicate acceptance", async () => {
  const { ctx, data } = storage();
  const provider = fixtureModel();
  let now = 1000;
  ctx.now = async () => now;
  await accept(ctx, "Add three", provider);
  assert.equal(data.get("updated_at"), 1000);
  now = 2000;
  await readRun(ctx);
  await accept(ctx, "Add three", provider);
  assert.equal(data.get("updated_at"), 1000);
  await execute(ctx, provider);
  assert.equal(data.get("updated_at"), 2000);
  now = 3000;
  await execute(ctx, provider);
  await readRun(ctx);
  await accept(ctx, "Add three", provider);
  assert.equal(data.get("updated_at"), 2000);
});

test("duplicate acceptance and status reads require a durability proof", async () => {
  const { ctx } = storage();
  const provider = fixtureModel();
  await accept(ctx, "Add three", provider);
  ctx.storage.sync = async () => {
    throw new Error("durability unavailable");
  };
  await assert.rejects(
    accept(ctx, "Add three", provider),
    /durability unavailable/,
  );
  await assert.rejects(readRun(ctx), /durability unavailable/);
});

test("retry exhaustion preserves journal and configuration changes refuse execution", async () => {
  const { ctx, alarm } = storage();
  const provider = fixtureModel();
  await accept(ctx, "Add three", provider);
  const different = fixtureModel();
  Object.defineProperty(different, "modelId", { value: "different-release" });
  await assert.rejects(execute(ctx, different), /configuration changed/);
  provider.doGenerate = async () => {
    throw new Error("unavailable");
  };
  for (let n = 0; n < 5; n++) await assert.rejects(execute(ctx, provider));
  await execute(ctx, provider);
  assert.equal((await readRun(ctx)).status, "failed");
  assert.equal(alarm(), undefined);
});

test("small transport refuses a large decision and still saves retry exhaustion", async () => {
  const { ctx, data, alarm } = storage();
  ctx.fs.capabilities = async () => ({ transport: { max_value_bytes: 8192 } });
  const apply = ctx.storage.apply;
  ctx.storage.apply = async (batch) => {
    assert.ok(Buffer.byteLength(JSON.stringify(batch)) <= 8192);
    return apply(batch);
  };
  const provider = fixtureModel();
  const generate = provider.doGenerate;
  provider.doGenerate = async (...args) => {
    const result = await generate(...args);
    result.content.push({ type: "text", text: "x".repeat(9000) });
    return result;
  };
  await accept(ctx, "Add three", provider);
  for (let n = 0; n < 5; n++)
    await assert.rejects(execute(ctx, provider), {
      code: "ValidationError",
      outcome: "not_committed",
    });
  await execute(ctx, provider);
  const run = await readRun(ctx);
  assert.equal(run.status, "failed");
  assert.equal(run.attempts, 5);
  assert.equal(run.models.length, 0);
  assert.equal(data.has("count"), false);
  assert.equal(alarm(), undefined);
});

test("replay rejects a changed recorded request before further effects", async () => {
  const { ctx, data } = storage();
  const provider = fixtureModel();
  await accept(ctx, "Add three", provider);
  await assert.rejects(
    execute(ctx, provider, async () => {
      throw new Error("stop");
    }),
  );
  data.get("run").models[0].request = "wrong";
  await assert.rejects(execute(ctx, provider), /replay diverged/);
  assert.equal(data.has("count"), false);
});

test("tool failures cannot become a checkpointed alternate conversation", async () => {
  const { ctx, data } = storage();
  const model = fixtureModel();
  await accept(ctx, "Add three", model);
  let calls = 0;
  const generate = model.doGenerate;
  model.doGenerate = (...args) => {
    calls++;
    return generate(...args);
  };
  ctx.storage.sync = async () => {
    if (data.has("count")) throw new Error("durability proof unavailable");
  };
  await assert.rejects(execute(ctx, model), /durability proof unavailable/);
  assert.equal(
    calls,
    1,
    "no further provider call after a tool durability failure",
  );
  assert.equal(data.get("run").status, "pending");
  assert.equal(data.get("run").models.length, 1);
  ctx.storage.sync = async () => {};
  await execute(ctx, model);
  assert.equal(calls, 2);
  assert.equal(data.get("count"), 3);
});

test("multiple tool calls execute sequentially and cannot increment twice", async () => {
  const { ctx, data } = storage();
  const model = fixtureModel();
  const generate = model.doGenerate;
  model.doGenerate = async (...args) => {
    const response = await generate(...args);
    if (response.finishReason.unified === "tool-calls") {
      response.content.push({
        ...response.content[0],
        toolCallId: "second-increment",
      });
    }
    return response;
  };
  await accept(ctx, "Add three", model);
  await assert.rejects(execute(ctx, model), /already has an increment/);
  assert.equal(data.get("count"), 3);
  assert.equal(Object.keys(data.get("run").tools).length, 1);
  await assert.rejects(execute(ctx, model), /already has an increment/);
  assert.equal(data.get("count"), 3);
});

test("invalid tool arguments never mutate the counter or complete the task", async () => {
  const { ctx, data } = storage();
  const model = fixtureModel();
  const generate = model.doGenerate;
  model.doGenerate = async (...args) => {
    const result = await generate(...args);
    if (result.finishReason.unified === "tool-calls")
      result.content[0].input = '{"amount":101}';
    return result;
  };
  await accept(ctx, "Add too much", model);
  await assert.rejects(execute(ctx, model));
  assert.equal(data.has("count"), false);
  assert.equal(data.get("run").status, "pending");
});

test("an ambiguous first tool refuses later tools from the same model decision", async () => {
  const { ctx, data } = storage();
  const model = fixtureModel();
  const generate = model.doGenerate;
  model.doGenerate = async (...args) => {
    const response = await generate(...args);
    if (response.finishReason.unified === "tool-calls")
      response.content.push({
        type: "tool-call",
        toolName: "saveNote",
        toolCallId: "after-ambiguous-write",
        input: '{"title":"Must not execute","content":"No further effects"}',
      });
    return response;
  };
  await accept(ctx, "Add three and save a note", model);
  ctx.storage.sync = async () => {
    if (data.has("count")) throw new Error("ambiguous durability outcome");
  };
  await assert.rejects(execute(ctx, model), /ambiguous durability outcome/);
  assert.equal(data.get("count"), 3);
  assert.equal(
    [...data.keys()].some((key) => key.startsWith("note:")),
    false,
  );
  assert.equal(Object.keys(data.get("run").tools).length, 1);
});

test("acceptance and checkpoints reject oversized journals before commit", async () => {
  const { ctx, data } = storage();
  const model = fixtureModel();
  await assert.rejects(accept(ctx, "x".repeat(512 * 1024), model), /exceeds/);
  assert.equal(data.size, 0);
  await accept(ctx, "Add three", model);
  const generate = model.doGenerate;
  model.doGenerate = async (...args) => {
    const result = await generate(...args);
    result.content = [{ type: "text", text: "x".repeat(512 * 1024) }];
    return result;
  };
  await assert.rejects(execute(ctx, model), /exceeds/);
  assert.equal(data.get("run").models.length, 0);
  assert.equal(data.has("count"), false);
});
