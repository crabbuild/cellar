// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { DurableObject } from "cloudflare:workers";
import { application, check } from "./adapter.mjs";
const gate = {
  entered: false,
  release: undefined,
  reads: 0,
  brokenInvocations: 0,
};
class Broken extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    ctx.blockConcurrencyWhile(async () => {
      await ctx.storage.get("initialized");
      throw undefined;
    });
  }
  inspect() {
    gate.brokenInvocations++;
    throw new Error("failed constructor admitted a method");
  }
}
class Probe extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    ctx.blockConcurrencyWhile(async () => {
      await ctx.storage.put("initialized", true);
    });
  }
  async inspect(command) {
    check(
      (await this.ctx.storage.get("initialized")) === true,
      "native constructor admission",
    );
    if (command === "hold") {
      await this.ctx.blockConcurrencyWhile(async () => {
        await this.ctx.storage.transaction(async (tx) => {
          tx.kv.put("held", true);
          await new Promise((resolve) => {
            gate.release = resolve;
            gate.entered = true;
          });
          check(
            tx.kv.get("held") === true,
            "transaction lost callback view after gate release",
          );
        });
      });
      return { ok: true };
    }
    if (command === "fail") {
      await this.ctx.blockConcurrencyWhile(async () => {
        await this.ctx.storage.get("held");
        throw undefined;
      });
      throw new Error("failed native block returned success");
    }
    if (command === "read") gate.reads++;
    return { ok: true, id: this.ctx.id.toString() };
  }
}
class Selected extends DurableObject {
  constructor(ctx, env) {
    return ctx.id.name === "broken"
      ? new Broken(ctx, env)
      : new Probe(ctx, env);
  }
}
const app = application(Selected);
export const { cells } = app;
export async function fetch(req, ctx) {
  if (req.url.endsWith("/gate"))
    return {
      status: 200,
      headers: [],
      body: ctx._encode(
        JSON.stringify({
          entered: gate.entered,
          reads: gate.reads,
          brokenInvocations: gate.brokenInvocations,
        }),
      ),
    };
  if (req.url.endsWith("/release")) {
    check(gate.entered, "gate was not entered");
    gate.release();
    return { status: 200, headers: [], body: ctx._encode("{}") };
  }
  return app.fetch(req, ctx);
}
