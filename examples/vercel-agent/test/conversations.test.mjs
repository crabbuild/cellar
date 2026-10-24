import test from "node:test";
import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";
import { ConversationDirectory } from "../dist/conversations.js";

function harness() {
  const db = new DatabaseSync(":memory:");
  const ctx = {
    storage: {
      sql: async (sql, params = []) => {
        const statement = db.prepare(sql);
        const columns = statement.columns().map((column) => column.name);
        let rows = [];
        if (columns.length) {
          rows = statement
            .all(...params)
            .map((row) => columns.map((column) => row[column]));
        } else {
          statement.run(...params);
        }
        return { columns, rows, written: 0 };
      },
      sync: async () => {},
    },
  };
  const cell = new ConversationDirectory();
  const call = (method, body) => cell.fetch({ method, json: () => body }, ctx);
  return {
    ctx,
    db,
    call,
    list: async () =>
      JSON.parse(Buffer.from((await call("GET")).body, "base64").toString())
        .chats,
  };
}
const entry = (task, previous = null) => ({
  task,
  previous,
  title: "Saved conversation",
  prompt: `Prompt ${task}`,
});

test("durable directory lists latest conversation turns without resurrecting replayed parents", async (t) => {
  const h = harness();
  t.after(() => h.db.close());
  await h.call("POST", entry("root"));
  await h.call("POST", entry("child", "root"));
  await h.call("POST", entry("root"));
  assert.deepEqual(
    (await h.list()).map((chat) => chat.task),
    ["child"],
  );
  await h.call("POST", entry("another"));
  assert.deepEqual(
    (await h.list()).map((chat) => chat.task),
    ["another", "child"],
  );
  assert.equal(
    (await h.call("POST", { ...entry("child", "root"), prompt: "Different" }))
      .status,
    409,
  );
  assert.equal((await h.call("POST", entry("../invalid"))).status, 400);
});

test("lost directory sync acknowledgement is reconciled before idempotent replay or listing", async (t) => {
  const h = harness();
  t.after(() => h.db.close());
  h.ctx.storage.sync = async () => {
    throw new Error("unknown outcome");
  };
  await assert.rejects(h.call("POST", entry("saved")), /unknown outcome/);
  await assert.rejects(h.list(), /unknown outcome/);
  h.ctx.storage.sync = async () => {};
  await h.call("POST", entry("saved"));
  assert.deepEqual(await h.list(), [entry("saved")]);
});

test("concurrent directory registrations retain every turn and deduplicate retries", async (t) => {
  const h = harness();
  t.after(() => h.db.close());
  await Promise.all(
    Array.from({ length: 16 }, (_, n) => h.call("POST", entry(`turn-${n}`))),
  );
  await Promise.all(
    Array.from({ length: 32 }, (_, n) =>
      h.call("POST", entry(`turn-${n % 16}`)),
    ),
  );
  assert.equal((await h.list()).length, 16);
  assert.equal(
    h.db.prepare("SELECT COUNT(*) AS count FROM conversation_turns").get()
      .count,
    16,
  );
});
