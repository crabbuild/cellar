import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { ticketStore } from "../script/tickets.mjs";
import { createDemoServer } from "../script/server.mjs";
import { agentTools } from "../dist/tools.js";

function harness(services, external) {
  const data = new Map();
  const state = { tools: {}, intents: {} };
  let snapshot;
  const ctx = {
    identity: {
      scope: "test",
      class: "DurableAgent",
      name: "test",
      epoch: "1",
    },
    storage: {
      read: async (key) =>
        data.has(key)
          ? { present: true, value: structuredClone(data.get(key)) }
          : { present: false, value: null },
    },
  };
  const save = async (batch) => {
    for (const [key, check] of Object.entries(batch?.checks ?? {}))
      assert.deepEqual(await ctx.storage.read(key), check);
    for (const [key, value] of Object.entries(batch?.puts ?? {}))
      data.set(key, value);
    snapshot = structuredClone(state);
  };
  const tools = () =>
    agentTools(ctx, state, save, {
      services,
      external: external ?? (async () => {}),
      emit: () => {},
      committed: async () => {},
      check: () => {},
      failed: () => {},
    });
  return {
    state,
    data,
    ctx,
    saved: () => snapshot,
    call: (name, args, id) =>
      tools()[name].execute(args, { toolCallId: id, messages: [] }),
  };
}

test("read, calculation and local note tools checkpoint their exact results", async () => {
  const h = harness();
  assert.deepEqual(
    await h.call("calculate", { operation: "multiply", a: 24, b: 7 }, "calc"),
    { value: 168 },
  );
  assert.deepEqual(
    await h.call("calculate", { operation: "divide", a: 2, b: 0 }, "zero"),
    { error: "division_by_zero" },
  );
  assert.match(
    (await h.call("lookup", { topic: "durability" }, "reference")).text,
    /checkpoint/,
  );
  assert.deepEqual(
    await h.call(
      "saveNote",
      { title: "Recovery", content: "Saved atomically" },
      "note",
    ),
    { saved: true, title: "Recovery" },
  );
  assert.equal(
    [...h.data.values()].find((item) => item.title === "Recovery").content,
    "Saved atomically",
  );
  assert.equal((await h.call("readState", {}, "state")).receipts.length, 4);
  assert.equal(Object.keys(h.saved().tools).length, 5);
  assert.deepEqual(
    await h.call("calculate", { operation: "multiply", a: 24, b: 7 }, "calc"),
    { value: 168 },
  );
  await assert.rejects(
    h.call("calculate", { operation: "multiply", a: 25, b: 7 }, "calc"),
    /replay diverged/,
  );
});

test("downstream ticket idempotency survives service restart and rejects key reuse with changed arguments", async () => {
  const root = await mkdtemp(join(tmpdir(), "cellar-ticket-"));
  const key = "a".repeat(64);
  try {
    let store = ticketStore(join(root, "tickets.sqlite"));
    const first = store.create(key, {
      title: "Verify recovery",
      priority: "normal",
    });
    assert.equal(first.status, 201);
    store.close();
    store = ticketStore(join(root, "tickets.sqlite"));
    try {
      assert.deepEqual(
        store.create(key, { title: "Verify recovery", priority: "normal" })
          .result,
        first.result,
      );
      assert.equal(
        store.create(key, { title: "Different", priority: "normal" }).status,
        409,
      );
      assert.deepEqual(store.stats(), { effects: 1, requests: 2 });
    } finally {
      store.close();
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("lost response after external effect reconciles the durable intent without creating a second ticket", async () => {
  const demo = createDemoServer({
    backend: () => undefined,
    token: () => "token",
    provider: "fixture",
    model: "fixture",
    restart: async () => {},
  });
  await new Promise((resolve) => demo.server.listen(0, "127.0.0.1", resolve));
  const origin = `http://127.0.0.1:${demo.server.address().port}`;
  let interrupted = false;
  const h = harness({ url: origin, token: "token" }, async () => {
    if (!interrupted) {
      interrupted = true;
      throw new Error("response lost after remote commit");
    }
  });
  try {
    await assert.rejects(
      h.call(
        "createTicket",
        { title: "Recover me", priority: "high" },
        "ticket",
      ),
      /response lost/,
    );
    assert.equal(Object.keys(h.saved().intents).length, 1);
    assert.equal(Object.keys(h.saved().tools).length, 0);
    // Activation epoch is deliberately absent from the downstream key.
    h.ctx.identity.epoch = "2";
    const result = await h.call(
      "createTicket",
      { title: "Recover me", priority: "high" },
      "ticket",
    );
    assert.match(result.id, /^ticket-/);
    assert.deepEqual(await (await fetch(`${origin}/api/tickets`)).json(), {
      effects: 1,
      requests: 2,
    });
    await h.call(
      "createTicket",
      { title: "Recover me", priority: "high" },
      "ticket",
    );
    assert.deepEqual(
      await (await fetch(`${origin}/api/tickets`)).json(),
      { effects: 1, requests: 2 },
      "local receipt prevents any third HTTP request",
    );
  } finally {
    await demo.close();
  }
});
