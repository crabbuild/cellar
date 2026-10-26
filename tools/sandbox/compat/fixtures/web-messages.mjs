// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { DurableObject } from "cloudflare:workers";
import { workersApplication } from "../../../../packages/cellar-cloudflare/index.js";

export class Counter extends DurableObject {
  gates = new Map();
  response(token, mode) {
    const gate = Promise.withResolvers();
    this.gates.set(token, gate);
    let phase = 0,
      cancelled = false;
    const storage = this.ctx.storage;
    const stream = new ReadableStream(
      {
        async pull(controller) {
          if (phase > 0) await gate.promise;
          if (cancelled) return;
          if (phase === 1 && mode === "error")
            throw new Error("download failed after prefix");
          if (phase === 3) {
            storage.kv.put(token, "finished");
            controller.close();
            return;
          }
          storage.kv.put(token, ++phase);
          controller.enqueue(new Uint8Array(1024).fill(phase));
        },
        cancel() {
          cancelled = true;
          storage.kv.put(token, "cancelled");
          gate.resolve();
        },
      },
      { highWaterMark: 0 },
    );
    return new Response(stream, {
      status: 206,
      statusText: "Partial Content",
      headers: [
        ["x-download", token],
        ["set-cookie", "first=1"],
        ["set-cookie", "second=2"],
      ],
    });
  }
  release(token) {
    this.gates.get(token)?.resolve();
  }
  state(token) {
    return this.ctx.storage.kv.get(token) ?? null;
  }
  metadata() {
    const response = new Response(null, {
      status: 204,
      headers: { "x-empty": "yes" },
    });
    return { response, alias: response, error: Response.error() };
  }
}

export default workersApplication(
  {
    Counter,
    default: {
      async fetch(request, env) {
        const url = new URL(request.url);
        const token = url.searchParams.get("token");
        const stub = env.COUNTERS.getByName("downloads");
        if (url.pathname === "/release") {
          await stub.release(token);
          return new Response(null, { status: 204 });
        }
        if (url.pathname === "/state")
          return Response.json(await stub.state(token));
        if (url.pathname === "/metadata") {
          const value = await stub.metadata();
          return Response.json({
            response: value.response instanceof Response,
            alias: value.response === value.alias,
            status: value.response.status,
            header: value.response.headers.get("x-empty"),
            empty: value.response.body === null,
            error: value.error.type,
            errorStatus: value.error.status,
          });
        }
        const response = await stub.response(token, url.pathname.slice(1));
        if (url.pathname === "/unused") {
          response[Symbol.dispose]();
          return new Response("disposed");
        }
        return response;
      },
    },
  },
  { namespaces: { COUNTERS: "Counter" } },
);
