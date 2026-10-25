// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { DurableObject } from "cloudflare:workers";
import { workersApplication } from "../../../../packages/cellar-cloudflare/index.js";
let saved;
let alarmDeliveries = 0;

class Counter extends DurableObject {
  constructor(state, env) {
    super(state, env);
    this.peer = env.COUNTERS.getByName("peer");
    saved ??= env.COUNTERS;
  }
  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname.endsWith("/alarms/start")) {
      await this.ctx.storage.setAlarm(Date.now());
      return new Response("scheduled");
    }
    if (url.pathname.endsWith("/alarms/release")) {
      this.releaseAlarm?.();
      return new Response("released");
    }
    if (url.pathname.endsWith("/alarms/state")) {
      return Response.json({
        deliveries: alarmDeliveries,
        started: this.alarmsStarted ?? 0,
        active: this.alarmsActive ?? 0,
        peak: this.alarmsPeak ?? 0,
      });
    }
    if (url.pathname.endsWith("/lost-response")) {
      this.ctx.storage.kv.put("lost-response", true);
      return new Response(new Uint8Array(257));
    }
    if (url.pathname.endsWith("/lost-effect")) {
      return Response.json({
        effect: this.ctx.storage.kv.get("lost-response"),
      });
    }
    if (url.pathname.endsWith("/retained")) {
      return this.peer.fetch("https://unrelated.invalid/echo", {
        method: "POST",
        body: Uint8Array.of(0, 255, 42),
      });
    }
    if (url.pathname.endsWith("/borrow")) {
      try {
        saved.idFromName("forbidden");
      } catch (error) {
        return Response.json({ code: error.code, outcome: error.outcome });
      }
      throw new Error("foreign activation borrowed environment binding");
    }
    if (url.pathname.endsWith("/echo")) {
      return new Response(request.body, {
        headers: [
          ["set-cookie", "a=1"],
          ["set-cookie", "b=2"],
          ["x-cell", this.ctx.id.name],
        ],
      });
    }
    const count = (this.ctx.storage.kv.get("count") ?? 0) + 1;
    this.ctx.storage.kv.put("count", count);
    return Response.json({
      count,
      name: this.ctx.id.name,
      marker: this.env.DOMAIN_MARKER,
    });
  }
  async alarm() {
    this.alarmsStarted = (this.alarmsStarted ?? 0) + 1;
    this.alarmsActive = (this.alarmsActive ?? 0) + 1;
    this.alarmsPeak = Math.max(this.alarmsPeak ?? 0, this.alarmsActive);
    try {
      if (this.alarmsStarted === 1) {
        await this.ctx.storage.setAlarm(Date.now());
        await new Promise((resolve) => {
          this.releaseAlarm = resolve;
        });
      }
    } finally {
      this.alarmsActive--;
    }
  }
}
const module = {
  Counter,
  default: {
    fetch(request, env) {
      if (new URL(request.url).pathname === "/parallel-http") {
        return Promise.all(
          ["left", "right"].map((name) =>
            env.COUNTERS.getByName(name).fetch("https://local.invalid/echo"),
          ),
        ).then((responses) =>
          Response.json(
            responses.map((response) => response.headers.get("x-cell")),
          ),
        );
      }
      const name = new URL(request.url).pathname.split("/")[1];
      const id = env.COUNTERS.idFromName(name);
      // The HTTP dispatcher must route opaque IDs as well as named stubs.
      return env.COUNTERS.get(env.COUNTERS.idFromString(id.toString())).fetch(
        request,
      );
    },
  },
};
const app = workersApplication(module, {
  namespaces: { COUNTERS: "Counter" },
  variables: ["DOMAIN_MARKER"],
});
export const fetch = app.fetch;
export const cells = {
  Counter: class extends app.cells.Counter {
    alarm(event, context) {
      alarmDeliveries++;
      return super.alarm(event, context);
    }
  },
};
