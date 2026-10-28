// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import assert from "node:assert/strict";
import test from "node:test";
import { durableObjectContainer } from "../../packages/cellar-cloudflare/container.js";
import { serviceBinding } from "../../packages/cellar-cloudflare/service.js";

function fixture(install = async () => null) {
  const calls = [];
  const work = [];
  const tickets = new Map();
  const state = {};
  let active = true;
  let next = 0;
  const current = () => {
    if (!active) throw new Error("ContextMismatch");
    return context;
  };
  const context = {
    _activation: () => current,
    _containerState: () => state,
    _containerControl(pending) {
      work.push(pending);
    },
    _rpcValue: (value) => Buffer.from(JSON.stringify(value)).toString("base64"),
    _containerAdmit(operation, input) {
      const ticket = String(++next);
      tickets.set(ticket, { operation, input });
      return ticket;
    },
    _sync(name, { ticket }) {
      assert.equal(name, "workers.container.discard");
      assert.equal(tickets.delete(ticket), true);
    },
    async _call(operation, input) {
      calls.push({ operation, input });
      if (
        [
          "container.intercept_outbound_http",
          "container.intercept_outbound_https",
        ].includes(operation)
      )
        return install(input);
      assert.equal(operation, "workers.container.perform");
      assert.equal(tickets.delete(input.ticket), true);
      return null;
    },
  };
  return {
    context,
    calls,
    work,
    tickets,
    container: durableObjectContainer(context),
    binding: serviceBinding(current, { kind: "binding", binding: "API" }),
    exported: serviceBinding(current, { kind: "export", name: "Gateway" }),
    retire() {
      active = false;
    },
  };
}

test("HTTP interception copies declared routes and props without retaining guest handlers", async () => {
  const { container, calls, binding, exported } = fixture();
  assert.equal(
    await container.interceptOutboundHttp("*.example.test", binding),
    undefined,
  );
  const props = { binding: "BACKUPS", key: "archive", mode: "read" };
  const handler = exported({ props });
  props.mode = "forged-after-capture";
  await container.interceptAllOutboundHttp(handler);
  assert.deepEqual(calls, [
    {
      operation: "container.intercept_outbound_http",
      input: {
        target: "*.example.test",
        service: { kind: "binding", binding: "API" },
      },
    },
    {
      operation: "container.intercept_outbound_http",
      input: {
        target: null,
        service: {
          kind: "export",
          name: "Gateway",
          props: Buffer.from(
            JSON.stringify({
              binding: "BACKUPS",
              key: "archive",
              mode: "read",
            }),
          ).toString("base64"),
        },
      },
    },
  ]);
});

test("HTTP interception rejects forged, foreign and retired service capabilities before native dispatch", () => {
  const own = fixture();
  const foreign = fixture();
  for (const value of [
    () => new Response("forged"),
    { kind: "binding", binding: "API" },
    foreign.binding,
  ]) {
    assert.throws(
      () => own.container.interceptAllOutboundHttp(value),
      TypeError,
    );
  }
  assert.throws(
    () => own.container.interceptOutboundHttp(null, own.binding),
    TypeError,
  );
  own.retire();
  assert.throws(
    () => own.container.interceptAllOutboundHttp(own.binding),
    /ContextMismatch/,
  );
  assert.deepEqual(own.calls, []);
});

test("pre-start interception completes before an admitted start reaches the provider", async () => {
  const install = Promise.withResolvers();
  const { container, binding, calls, work, tickets } = fixture(
    () => install.promise,
  );
  const installed = container.interceptAllOutboundHttp(binding);
  container.start();
  assert.equal(tickets.size, 1);
  assert.deepEqual(
    calls.map(({ operation }) => operation),
    ["container.intercept_outbound_http"],
  );
  install.resolve(null);
  await installed;
  await Promise.all(work);
  assert.deepEqual(
    calls.map(({ operation }) => operation),
    ["container.intercept_outbound_http", "workers.container.perform"],
  );
  assert.equal(tickets.size, 0);
});

test("an interception refusal prevents queued starts and releases unused admission", async () => {
  const refusal = new Error("Unsupported provider profile");
  const { container, binding, calls, work, tickets } = fixture(async () => {
    throw refusal;
  });
  const installed = container.interceptAllOutboundHttp(binding);
  container.start();
  await assert.rejects(installed, (error) => error === refusal);
  const outcomes = await Promise.allSettled(work);
  assert.ok(
    outcomes.every(
      ({ status, reason }) => status === "rejected" && reason === refusal,
    ),
  );
  assert.deepEqual(
    calls.map(({ operation }) => operation),
    ["container.intercept_outbound_http"],
  );
  assert.equal(tickets.size, 0);
});

test("HTTPS uses a separate operation, copies wildcard routes and precedes start", async () => {
  const { container, calls, binding, exported, work, tickets } = fixture();
  await container.interceptOutboundHttps("*.example.test", binding);
  await container.interceptOutboundHttps(
    "*",
    exported({ props: { phase: "tls" } }),
  );
  container.start();
  await Promise.all(work);
  assert.deepEqual(
    calls.map(({ operation }) => operation),
    [
      "container.intercept_outbound_https",
      "container.intercept_outbound_https",
      "workers.container.perform",
    ],
  );
  assert.equal(calls[0].input.target, "*.example.test");
  assert.equal(calls[1].input.target, "*");
  assert.equal(tickets.size, 0);
});

test("HTTPS refuses invalid, forged, foreign and retired capabilities before dispatch", () => {
  const own = fixture();
  const foreign = fixture();
  for (const target of [null, 443, {}]) {
    assert.throws(
      () => own.container.interceptOutboundHttps(target, own.binding),
      TypeError,
    );
  }
  for (const handler of [
    () => {},
    { kind: "binding", binding: "API" },
    foreign.binding,
  ]) {
    assert.throws(
      () => own.container.interceptOutboundHttps("*", handler),
      TypeError,
    );
  }
  own.retire();
  assert.throws(
    () => own.container.interceptOutboundHttps("*", own.binding),
    /ContextMismatch/,
  );
  assert.deepEqual(own.calls, []);
});

test("HTTPS refusal prevents queued start and releases admission", async () => {
  const refusal = new Error("HTTPS host profile unavailable");
  const { container, calls, binding, work, tickets } = fixture(async () => {
    throw refusal;
  });
  const installed = container.interceptOutboundHttps("*", binding);
  container.start();
  await assert.rejects(installed, (error) => error === refusal);
  const outcomes = await Promise.allSettled(work);
  assert.ok(
    outcomes.every(
      ({ status, reason }) => status === "rejected" && reason === refusal,
    ),
  );
  assert.deepEqual(
    calls.map(({ operation }) => operation),
    ["container.intercept_outbound_https"],
  );
  assert.equal(tickets.size, 0);
});
