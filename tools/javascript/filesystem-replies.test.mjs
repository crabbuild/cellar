// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

const root = new URL("../../", import.meta.url);
const source = (path) => readFile(new URL(path, root), "utf8");
const factory = async (name) =>
  (0, eval)(await source(`crates/cellar-javascript/runtime/${name}.js`));
const factories = await Promise.all(
  [
    "runner",
    "delegates",
    "rpc_streams",
    "rpc_writable",
    "rpc_inputs",
    "rpc_messages",
    "rpc_requests",
  ].map(factory),
);
const fixture = JSON.parse(
  await source("crates/cellar/tests/support/filesystem_reply_cases.json"),
);
const probe = (0, eval)(
  `(${await source("crates/cellar-javascript/tests/support/filesystem-replies.js")}\n)`,
);

test("filesystem replies preserve uncertainty and confirmed helper progress", async () => {
  const replies = [
    { operation: "open", value: { handle: "file" } },
    { operation: "opendir", value: { handle: "directory" } },
    ...fixture.cases.flatMap((item) => item.replies),
  ];
  let submitted = 0;
  const host = async (_id, operation) => {
    const reply = replies[submitted++];
    assert.ok(reply, `unexpected ${operation}`);
    assert.equal(operation, `filesystem.${reply.operation}`);
    const value = reply.template
      ? { ...fixture.templates[reply.template], ...reply.patch }
      : reply.value;
    return [reply.raw ?? JSON.stringify(reply.envelope ?? { ok: true, value })];
  };
  const unused = () => {
    throw new Error("unexpected protocol capability");
  };
  const make = factories[0](
    host,
    unused,
    () => 1,
    unused,
    unused,
    unused,
    undefined,
    ...factories.slice(1),
    unused,
  );
  const runner = make(
    { fetch: (_request, ctx) => probe(ctx, fixture.cases) },
    JSON.stringify({ events: ["fetch"], cells: {} }),
    16,
    32,
  );
  const result = await runner.dispatch(
    1,
    JSON.stringify({
      kind: "fetch",
      event: { url: "http://test/", method: "GET", headers: [], body: "" },
      identity: null,
      object: null,
      class: null,
    }),
  );
  assert.deepEqual(JSON.parse(result), { checked: fixture.cases.length });
  assert.equal(submitted, replies.length);
});
