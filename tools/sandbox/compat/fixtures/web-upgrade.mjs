// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { DurableObject } from "cloudflare:workers";
import { workersApplication } from "../../../../packages/cellar-cloudflare/index.js";

function echo(context, accept = true) {
  const [client, server] = Object.values(new WebSocketPair());
  if (accept) server.accept();
  server.onmessage = (event) => {
    if (context)
      context.storage.kv.put(
        "messages",
        (context.storage.kv.get("messages") ?? 0) + 1,
      );
    server.send(event.data);
  };
  return new Response(null, {
    status: 101,
    webSocket: client,
    headers: { "x-upgrade-kind": "pair" },
  });
}

class Counter extends DurableObject {
  async fetch(request) {
    const mode = new URL(request.url).pathname.split("/").pop();
    if (mode === "pair") return echo(this.ctx);
    if (mode === "stalled") return echo(this.ctx, false);
    if (mode === "state")
      return Response.json({
        messages: this.ctx.storage.kv.get("messages") ?? 0,
      });
    if (mode === "destroy") {
      await this.ctx.container.destroy();
      return new Response(null, { status: 204 });
    }
    if (!this.ctx.container.running) this.ctx.container.start();
    return this.ctx.container
      .getTcpPort(8080)
      .fetch(`http://container/${mode === "port" ? "ws" : "decline"}`, {
        headers: request.headers,
      });
  }
}

export default workersApplication(
  {
    Counter,
    default: {
      async fetch(request, env) {
        const path = new URL(request.url).pathname;
        if (path === "/worker/pair") return echo();
        const stub = env.COUNTERS.getByName("relay");
        if (path === "/accept") {
          const response = await stub.fetch("http://cell/pair", {
            headers: { Upgrade: "websocket" },
          });
          const socket = response.webSocket;
          const received = new Promise((resolve) =>
            socket.addEventListener(
              "message",
              (event) => resolve([...new Uint8Array(event.data)]),
              { once: true },
            ),
          );
          socket.accept();
          socket.send(Uint8Array.of(0, 255, 42));
          const bytes = await received;
          socket.close(1000, "accepted");
          return Response.json(bytes);
        }
        const response = await stub.fetch(request);
        response.headers.set("x-relay", "worker");
        return response;
      },
    },
  },
  { namespaces: { COUNTERS: "Counter" }, containers: ["Counter"] },
);
