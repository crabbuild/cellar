// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.
import { DurableObject } from "cloudflare:workers";

const now = () => performance.now();
const timed = (name, duration) => `${name};dur=${Math.max(0, duration).toFixed(3)}`;
const json = (data, status = 200, headers = {}) => new Response(JSON.stringify(data), {
  status, headers: {"content-type":"application/json", "cache-control":"no-store", ...headers}
});

export class CounterContainer extends DurableObject {
  ready = false;
  starting;
  async ensureReady() {
    const c = this.ctx.container;
    const cold = !c.running;
    if (cold) {
      this.ready = false;
      if (!this.env.R2_ENDPOINT || !this.env.R2_ACCESS_KEY_ID || !this.env.R2_SECRET_ACCESS_KEY) throw new Error("R2 credentials are not configured");
      c.start({enableInternet:true, env:{
        R2_BUCKET:this.env.R2_BUCKET, R2_ENDPOINT:this.env.R2_ENDPOINT,
        AWS_ACCESS_KEY_ID:this.env.R2_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY:this.env.R2_SECRET_ACCESS_KEY,
        CELLAR_DURABILITY:"bucket", CELLAR_TOKIO_THREADS:"1", CELLAR_MAX_RESIDENT_CELLS:"8",
        CELLAR_ACTIVATIONS:"4", CELLAR_SHUTDOWN_TOTAL_MS:"5000"
      }});
    }
    if (!this.ready) {
      // The timeout is relative to Durable Object inactivity, not to this
      // call. Reconfigure after startup/object recreation, not on every read.
      await c.setInactivityTimeout(600_000);
      const port = c.getTcpPort(8080);
      for (let attempt = 0; attempt < 100; attempt++) {
        try {
          const health = await port.fetch("http://container/health", {signal:AbortSignal.timeout(1000)});
          await health.body?.cancel();
          if (health.ok) { this.ready = true; return cold; }
        } catch { /* Only health reads retry; a mutation is forwarded once. */ }
        await scheduler.wait(250);
      }
      throw new Error("Cellar did not become ready");
    }
    return cold;
  }
  async fetch(request) {
    const started = now();
    this.starting ??= this.ensureReady().finally(() => { this.starting = undefined; });
    const cold = await this.starting;
    const ready = now() - started;
    const u = new URL(request.url); u.host = "container"; u.protocol = "http:";
    const forwarded = new Request(u, request); forwarded.headers.delete("host");
    const execution = now();
    try {
      const response = await this.ctx.container.getTcpPort(8080).fetch(forwarded);
      const body = await response.arrayBuffer();
      if (body.byteLength > 16384) throw new Error("Unexpected counter response size");
      const headers = new Headers(response.headers);
      headers.delete("content-length");
      headers.append("server-timing", [timed("readiness", ready), timed("container", now()-execution)].join(", "));
      headers.set("x-counter-cold-start", String(cold));
      return new Response(body, {status:response.status, headers});
    } catch (error) {
      this.ready = false;
      // A connection error can follow commitment. Never replay a POST here.
      throw error;
    }
  }
}

async function r2Probe(env) {
  const key = `latency-probes/${crypto.randomUUID()}`;
  const timings = {};
  try {
    let t = now(); await env.PROBES.put(key, "Cellar R2 probe"); timings.r2_put = now()-t;
    t = now(); const object = await env.PROBES.get(key); if (!object || await object.text() !== "Cellar R2 probe") throw new Error("R2 readback did not match"); timings.r2_get = now()-t;
  } finally {
    const t = now(); await env.PROBES.delete(key); timings.r2_delete = now()-t;
  }
  return json({probe:true, timings}, 200, {"server-timing":Object.entries(timings).map(([n,d])=>timed(n,d)).join(", ")});
}

export default {
  async fetch(request, env) {
    const u = new URL(request.url);
    if (!u.pathname.startsWith("/api/")) return env.ASSETS.fetch(request);
    if (u.search.length > 2048) return json({error:"Action parameters are too long"},400);
    if (request.method === "POST" && request.body) {
      const reader = request.body.getReader(); const first = await reader.read(); await reader.cancel();
      if (!first.done) return json({error:"Actions use query parameters and an empty request body"},400);
    }
    const requestId = crypto.randomUUID();
    const started = now();
    if (!["GET","POST"].includes(request.method)) return json({error:"Use GET or POST"},405);
    if (request.method === "POST" && (request.headers.get("x-counter-action") !== "1" || request.headers.get("origin") && request.headers.get("origin") !== u.origin)) return json({error:"Same-origin action header required"},403);
    if (u.pathname === "/api/info") return json({cpu:0.5, memoryGiB:4, diskGB:8, durability:"R2 bucket", storage:"SQLite", revision:env.CELLAR_REVISION});
    try {
      let response;
      if (u.pathname === "/api/probe/r2" && request.method === "POST") response = await r2Probe(env);
      else if (/^\/api\/counters\/(demo|check)$/.test(u.pathname)) {
        const forwarded = new Request(request.url, {method:request.method, headers:request.headers, redirect:"manual"}); forwarded.headers.set("x-request-id",requestId);
        response = await env.COUNTER.getByName("counter-lab").fetch(forwarded);
      } else return json({error:"Unknown API route"},404);
      const headers = new Headers(response.headers);
      headers.set("x-request-id",requestId);
      headers.set("cache-control","no-store");
      headers.set("timing-allow-origin",u.origin);
      headers.append("server-timing",timed("edge",now()-started));
      return new Response(response.body,{status:response.status,headers});
    } catch {
      return json({error:"Container request could not be confirmed. Read the counter; retry a mutation only with its original identity.",requestId},503,{"x-request-id":requestId,"server-timing":timed("edge",now()-started)});
    }
  }
};
