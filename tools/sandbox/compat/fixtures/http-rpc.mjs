// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { DurableObject } from "cloudflare:workers";
import { workersApplication } from "../../../../packages/cellar-cloudflare/index.js";

const uploads = new Map();
export class Counter extends DurableObject {
  async consume(token, request, alias, response, responseAlias) {
    if (request !== alias || response !== responseAlias)
      throw new Error("HTTP argument alias changed");
    if (!(request instanceof Request) || !(response instanceof Response))
      throw new Error("HTTP argument type changed");
    const reader = request.body.getReader({ mode: "byob" });
    let count = 0,
      valid = true;
    try {
      for (;;) {
        const { done, value } = await reader.read(new Uint8Array(513));
        if (done) break;
        for (const byte of value) if (byte !== count++ % 256) valid = false;
        this.ctx.storage.kv.put(token, count);
      }
    } finally {
      reader.releaseLock();
    }
    this.ctx.storage.kv.put(token, "uploaded");
    await new Promise((resolve) => {
      if (request.signal.aborted) resolve();
      else request.signal.addEventListener("abort", resolve, { once: true });
    });
    return {
      count,
      valid,
      method: request.method,
      url: request.url,
      header: request.headers.get("x-upload"),
      responseStatus: response.status,
      responseText: await response.text(),
      reason: request.signal.reason,
    };
  }
  state(token) {
    return this.ctx.storage.kv.get(token) ?? null;
  }
  makeRequest() {
    this.controller = new AbortController();
    return new Request("https://service.test/result", {
      method: "PUT",
      body: "returned body",
      headers: { "x-result": "request" },
      signal: this.controller.signal,
    });
  }
  async echoRequest(request) {
    return new Response(await request.text(), {
      headers: { "x-method": request.method },
    });
  }
  abortRequest() {
    this.ctx.storage.kv.put("abort-proof", "committed");
    this.controller.abort("producer abort");
    return true;
  }
  untouched(request) {
    this.ctx.storage.kv.put("entered", true);
    return request.url;
  }
}

export default workersApplication(
  {
    Counter,
    default: {
      async fetch(request, env) {
        const url = new URL(request.url),
          token = url.searchParams.get("token") ?? "upload";
        const stub = env.COUNTERS.getByName("http-rpc");
        if (url.pathname === "/state")
          return Response.json(await stub.state(token));
        if (url.pathname === "/release") {
          uploads.get(token).gate.resolve();
          return new Response(null, { status: 204 });
        }
        if (url.pathname === "/abort") {
          uploads.get(token).controller.abort("client abort");
          return new Response(null, { status: 204 });
        }
        if (url.pathname === "/abort-returned")
          return Response.json(await stub.abortRequest());
        if (url.pathname === "/refuse") {
          const controller = new AbortController();
          controller.abort("before effect");
          let refused = false;
          try {
            await stub.untouched(
              new Request("https://service.test/", {
                signal: controller.signal,
              }),
            );
          } catch {
            refused = true;
          }
          return Response.json({
            refused,
            entered: await stub.state("entered"),
          });
        }
        if (url.pathname === "/roundtrip") {
          const input = await stub.makeRequest();
          const output = await stub.echoRequest(input);
          return Response.json({
            body: await output.text(),
            method: output.headers.get("x-method"),
          });
        }
        if (url.pathname === "/unused") {
          const value = await stub.makeRequest();
          return Response.json({
            request: value instanceof Request,
            method: value.method,
          });
        }
        if (url.pathname === "/returned") {
          const value = await stub.makeRequest();
          const body = await value.text();
          uploads.set("returned", { ready: true });
          await new Promise((resolve) => {
            if (value.signal.aborted) resolve();
            else
              value.signal.addEventListener("abort", resolve, { once: true });
          });
          return Response.json({
            body,
            method: value.method,
            header: value.headers.get("x-result"),
            reason: value.signal.reason,
            proof: await stub.state("abort-proof"),
          });
        }
        if (url.pathname === "/returned-state")
          return Response.json(uploads.get("returned")?.ready ?? false);
        if (url.pathname === "/upload") {
          const gate = Promise.withResolvers(),
            controller = new AbortController();
          uploads.set(token, { gate, controller });
          let offset = 0,
            cancelled = false;
          const size = 32768 + 17;
          const body = new ReadableStream({
            type: "bytes",
            async pull(target) {
              if (offset >= 1024) await gate.promise;
              if (cancelled) return;
              if (offset === size) {
                const pending = target.byobRequest;
                target.close();
                pending?.respond(0);
                return;
              }
              const bytes = new Uint8Array(Math.min(1024, size - offset)).map(
                (_, i) => (offset + i) % 256,
              );
              offset += bytes.length;
              target.enqueue(bytes);
            },
            cancel() {
              cancelled = true;
              gate.resolve();
            },
          });
          const input = new Request("https://service.test/upload", {
            method: "POST",
            body,
            duplex: "half",
            headers: { "x-upload": "binary" },
            signal: controller.signal,
          });
          const output = new Response("response argument", { status: 206 });
          return Response.json(
            await stub.consume(token, input, input, output, output),
          );
        }
        return new Response("missing", { status: 404 });
      },
    },
  },
  { namespaces: { COUNTERS: "Counter" } },
);
