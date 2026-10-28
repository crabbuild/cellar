// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { Container } from "@cloudflare/containers";
import { workersApplication } from "../../../../packages/cellar-cloudflare/index.js";

// Only observe the published lifecycle. Its readiness, idle alarms, signal,
// state transitions and monitor callbacks run unchanged.
export class Counter extends Container {
  defaultPort = 8080;
  sleepAfter = "1s";

  async fetch(request) {
    console.log("lifecycle: Durable Object fetch entered");
    try {
      return await super.fetch(request);
    } finally {
      console.log("lifecycle: Durable Object fetch settled");
    }
  }
  async onStart() {
    console.log("lifecycle: onStart entered");
    await this.record("starts");
    console.log("lifecycle: onStart settled");
  }
  async onStop(params) {
    console.log("lifecycle: onStop entered");
    await this.record("stops");
    await this.ctx.storage.put("lastStop", params);
    console.log("lifecycle: onStop settled");
  }
  async onActivityExpired() {
    await this.record("expired");
    await super.onActivityExpired();
  }
  async record(key) {
    await this.ctx.storage.put(
      key,
      ((await this.ctx.storage.get(key)) ?? 0) + 1,
    );
  }
  async report() {
    return {
      running: this.container.running,
      status: (await this.getState()).status,
      starts: (await this.ctx.storage.get("starts")) ?? 0,
      stops: (await this.ctx.storage.get("stops")) ?? 0,
      expired: (await this.ctx.storage.get("expired")) ?? 0,
      lastStop: (await this.ctx.storage.get("lastStop")) ?? null,
      alarm: await this.ctx.storage.getAlarm(),
    };
  }
  async dispose() {
    const monitor = this.monitor;
    await this.destroy();
    await monitor;
    await this.ctx.blockConcurrencyWhile(async () => {});
    await this.syncPendingStoppedEvents();
  }
}

export default workersApplication(
  {
    Counter,
    default: {
      async fetch(request, env) {
        const url = new URL(request.url);
        const stub = env.COUNTERS.getByName("sleeper");
        if (url.pathname === "/state")
          return Response.json(await stub.report());
        if (url.pathname === "/destroy") {
          await stub.dispose();
          return new Response(null, { status: 204 });
        }
        console.log("lifecycle: Worker wake entered");
        return stub.fetch(new Request("http://container/", request));
      },
    },
  },
  { namespaces: { COUNTERS: "Counter" }, containers: ["Counter"] },
);
