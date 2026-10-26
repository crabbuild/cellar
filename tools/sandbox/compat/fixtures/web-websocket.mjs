// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { DurableObject } from "cloudflare:workers";
import { workersApplication } from "../../../../packages/cellar-cloudflare/index.js";
let foreign;
export class Counter extends DurableObject {
  messages = [];
  async open(accept = true) {
    this.ctx.container.start();
    const response = await this.ctx.container
      .getTcpPort(8080)
      .fetch("http://untrusted.invalid/ws", {
        headers: { Upgrade: "websocket" },
      });
    if (response.status !== 101 || !response.webSocket)
      throw new Error("missing container upgrade");
    this.socket = response.webSocket;
    foreign = this.socket;
    this.socket.addEventListener("message", (event) => {
      const message =
        typeof event.data === "string"
          ? event.data
          : [...new Uint8Array(event.data)];
      this.messages.push(message);
      this.ctx.storage.kv.put("received", this.messages.length);
    });
    if (accept) this.socket.accept();
    return {
      status: response.status,
      accepted: accept,
      upgrade: response.headers.get("upgrade"),
    };
  }
  send(value) {
    this.ctx.storage.kv.put("sent", value);
    this.socket.send(value);
    return true;
  }
  binary() {
    this.socket.send(Uint8Array.of(0, 255, 42));
    return true;
  }
  close() {
    this.socket.close(1011, "joined");
    return true;
  }
  state() {
    return {
      messages: this.messages,
      ready: this.socket?.readyState,
      received: this.ctx.storage.kv.get("received") ?? 0,
    };
  }
  borrow() {
    try {
      foreign.send("forbidden");
    } catch (error) {
      return { code: error.code };
    }
    throw new Error("socket borrowed another activation");
  }
}
const app = workersApplication(
  {
    Counter,
    default: {
      async fetch(request, env) {
        const url = new URL(request.url),
          stub = env.COUNTERS.getByName(
            url.searchParams.get("owner") ?? "socket-owner",
          );
        const operation = url.pathname.slice(1);
        if (operation === "open") return Response.json(await stub.open());
        if (operation === "unused")
          return Response.json(await stub.open(false));
        if (operation === "send")
          return Response.json(await stub.send(url.searchParams.get("value")));
        if (operation === "binary") return Response.json(await stub.binary());
        if (operation === "close") return Response.json(await stub.close());
        if (operation === "borrow") return Response.json(await stub.borrow());
        return Response.json(await stub.state());
      },
    },
  },
  { namespaces: { COUNTERS: "Counter" }, containers: ["Counter"] },
);
export default app;
