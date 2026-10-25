// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import {
  activateDurableObject,
  durableObjectContainer,
} from "../../../../packages/cellar-cloudflare/index.js";
let saved;
let generations = 0;
export const check = (value, message) => {
  if (!value) throw new Error(message);
};
export async function refuses(callback, code) {
  try {
    await callback();
  } catch (error) {
    if (code)
      check(error.code === code, `${code}: ${error.code}: ${error.message}`);
    return error;
  }
  throw new Error("Expected refusal");
}
export function application(Target, usesContainer = false) {
  class Counter {
    async initialize(ctx) {
      this.initialization ??= (async () => {
        // This fixture qualifies constructors/storage and the live running
        // property. Queued lifecycle and forwarding remain separate work.
        const container = usesContainer
          ? durableObjectContainer(ctx)
          : undefined;
        if (usesContainer)
          check(
            !container.running,
            "constructor fixture requires a stopped engine",
          );
        this.activation = activateDurableObject(Target, ctx, {
          container,
          env: { SANDBOX_LOG_LEVEL: "none" },
        });
        this.original = ctx;
        this.generation = ++generations;
        saved ??= {
          activation: this.activation,
          container,
          name: ctx.identity.name,
        };
      })();
      await this.initialization;
      if (this.original !== ctx)
        await refuses(
          () => this.original.storage.read("forbidden"),
          "ContextMismatch",
        );
      if (saved.activation !== this.activation)
        await refuses(
          () => saved.activation.invoke(() => "forbidden"),
          "ContextMismatch",
        );
      if (usesContainer && saved.activation !== this.activation)
        await refuses(() => saved.container.running, "ContextMismatch");
    }
    async fetch(req, ctx) {
      try {
        await this.initialize(ctx);
        const result = await this.activation.invoke((target) =>
          target.inspect(req.url.split("/").pop(), ctx),
        );
        return {
          status: 200,
          headers: [],
          body: ctx._encode(
            JSON.stringify({
              ...result,
              generation: this.generation,
              epoch: ctx.identity.epoch,
            }),
          ),
        };
      } catch (error) {
        return {
          status: 500,
          headers: [],
          body: ctx._encode(
            JSON.stringify({
              code: error.code,
              name: error.name,
              message: error.message,
              stack: error.stack,
            }),
          ),
        };
      }
    }
    async alarm(_event, ctx) {
      await this.initialize(ctx);
      await this.activation.invoke(async (target) => {
        if (typeof target.alarm === "function")
          await target.alarm({ isRetry: false, retryCount: 0 });
        else await ctx.storage.apply({ delete_alarm: true });
      });
    }
  }
  return {
    cells: { Counter },
    async fetch(req, ctx) {
      const [name, command] = req.url.split("/").slice(3);
      const result = await ctx.cell("COUNTERS", name).fetch({
        url: `http://cell/${command}`,
        method: "GET",
        headers: [],
        body: "",
      });
      return {
        status: result.status,
        headers: result.headers,
        body: ctx._encode(result.body),
      };
    },
  };
}
