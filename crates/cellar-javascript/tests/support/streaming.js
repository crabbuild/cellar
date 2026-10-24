// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

let release;
let gate = new Promise(resolve => {
  release = resolve;
});
async function* events(ctx, fail = false) {
  if (ctx.identity) await ctx.storage.apply({ puts: { phase: 1 } });
  yield sseEvent("first", { id: "1", event: "update" });
  if (fail) throw new Error("private producer failure");
  await gate;
  if (ctx.identity) await ctx.storage.apply({ puts: { phase: 2 } });
  yield sseEvent("second", { id: "2" });
}
class Counter {
  async fetch(req, ctx) {
    if (req.url.includes("/ws")) {
      const socket = await ctx.websocketPair();
      await socket.setAttachment({ room: "test" });
      await socket.accept({ hibernating: true, tags: ["test"] });
      return webSocketResponse(socket);
    }
    if (req.url.includes("/phase"))
      return jsonResponse((await ctx.storage.read("phase")).value);
    return sseResponse(events(ctx, req.url.includes("error")));
  }
  async websocket_open() {}
  async websocket_message(event, ctx) {
    const socket = ctx.websocket(event.id);
    if ((await socket.getAttachment()).room !== "test")
      throw new Error("missing attachment");
    await ctx.storage.apply({ puts: { message: event.text ?? "binary" } });
    await socket.send(event.data);
  }
  async websocket_close() {}
}
const app = {
  cells: { Counter },
  async fetch(req, ctx) {
    if (req.url.includes("/cancel-read")) {
      const upstream = await ctx.fetchStream(
        request(req.url.replace("/cancel-read", "/sse")),
      );
      const reader = upstream.chunks()[Symbol.asyncIterator]();
      await reader.next();
      const pending = reader.next();
      await ctx.now();
      await upstream.cancel();
      try {
        await pending;
        throw new Error("read was not cancelled");
      } catch (error) {
        if (error.code !== "HostError") throw error;
      }
      return response("cancelled");
    }
    if (req.url.includes("/large"))
      return streamResponse(
        (async function* () {
          for (let i = 0; i < 5; i++) yield new Uint8Array(2048).fill(i);
        })(),
      );
    if (req.url.includes("/release")) {
      release();
      return response("ok");
    }
    if (req.url.includes("/reset")) {
      gate = new Promise(resolve => {
        release = resolve;
      });
      return response("ok");
    }
    if (req.url.includes("/outbound/sse"))
      return ctx.fetchStream(request(req.url.replace("/outbound/", "/")));
    if (req.url.includes("/consume/sse")) {
      const upstream = await ctx
        .cell("COUNTERS", "test")
        .fetchStream(request(req.url));
      return sseResponse(upstream.chunks());
    }
    if (req.url.includes("/outbound-ws")) {
      const socket = await ctx.connectWebSocket(
        req.url.replace(/^http/, "ws").replace("/outbound-ws", "/worker-ws"),
      );
      let event = await socket.next();
      if (event.type !== "open") throw new Error("missing open");
      const receive = socket.next();
      await socket.send("outbound");
      event = await receive;
      await socket.close();
      return response(event.data);
    }
    if (req.url.includes("/cell/"))
      return ctx
        .cell("COUNTERS", "test")
        .fetchStream(request(req.url, { headers: req.headers }));
    if (req.url.includes("/worker-ws")) {
      const socket = await ctx.websocketPair();
      await socket.accept();
      return webSocketResponse(socket, {
        run: async socket => {
          for (;;) {
            const event = await socket.next();
            if (event.type === "close") break;
            if (event.type === "message") await socket.send(event.data);
          }
        },
      });
    }
    return sseResponse(events(ctx, req.url.includes("error")));
  },
};
