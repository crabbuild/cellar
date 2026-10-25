// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import {
  jsonResponse,
  request,
  response,
  sseEvent,
  sseResponse,
  webSocketResponse,
  type Cell,
  type Context,
  type FetchOutput,
  type Request,
  type WebSocketMessage,
} from "@cellar/sdk";

/** A finite progress feed; each yield is one event, not a buffered response. */
async function* progress() {
  for (let step = 1; step <= 3; step++) {
    // Replace this with awaited application work, such as a model token source.
    // IDs label this response only; this example has no replay log.
    yield sseEvent(JSON.stringify({ step }), {
      event: "progress",
      id: String(step),
    });
  }

  yield sseEvent("complete", { event: "done" });
}

/** Cell sockets use fresh event contexts, so no socket/context is kept in fields. */
export class Room implements Cell {
  async fetch(req: Request, ctx: Context): Promise<FetchOutput> {
    if (req.url.split("?")[0].endsWith("/state")) {
      const snapshot = await ctx.storage.read("messages");
      return jsonResponse({ messages: snapshot.present ? snapshot.value : 0 });
    }

    const socket = await ctx.websocketPair();
    await socket.accept({ hibernating: true, tags: ["echo"] });

    return webSocketResponse(socket);
  }

  async websocket_open(): Promise<void> {
    // No application work is needed when the native connection opens.
  }

  async websocket_close(): Promise<void> {
    // The host releases the connection; the message count stays in storage.
  }

  async websocket_message(
    event: WebSocketMessage,
    ctx: Context,
  ): Promise<void> {
    const snapshot = await ctx.storage.read("messages");
    const count = snapshot.present ? Number(snapshot.value) : 0;

    // Compare and write atomically. A conflicting operation fails without a
    // partial update; do not blindly replay a frame after an unknown outcome.
    await ctx.storage.apply({
      checks: { messages: snapshot },
      puts: { messages: count + 1 },
    });
    await ctx.storage.sync();
    await ctx.websocket(event.id).send(event.data);
  }
}

export const cells = { Room };

/** Both embedded V8 and Node run these same Cellar SDK contracts. */
export async function fetch(req: Request, ctx: Context): Promise<FetchOutput> {
  const path = req.url.split("?")[0].replace(/^https?:\/\/[^/]+/, "");

  if (req.method !== "GET") {
    return response("Use GET", { status: 405, headers: [["allow", "GET"]] });
  }

  const isSocket = path === "/ws" || /^\/rooms\/[\w-]{1,64}\/ws$/.test(path);

  if (
    isSocket &&
    !req.headers.some(
      ([name, value]) =>
        name.toLowerCase() === "upgrade" && value.toLowerCase() === "websocket",
    )
  ) {
    return response("Use a WebSocket client", { status: 426 });
  }

  if (path === "/events") {
    return sseResponse(progress());
  }

  if (path === "/ws") {
    const socket = await ctx.websocketPair();
    await socket.accept();

    return webSocketResponse(socket, {
      // A worker keeps this invocation alive while it consumes frames.
      run: async socket => {
        for (;;) {
          const event = await socket.next();

          if (event.type === "close") {
            return;
          }

          if (event.type === "message") {
            await socket.send(event.data);
          }
        }
      },
    });
  }

  const room = /^\/rooms\/([\w-]{1,64})\/(ws|state)$/.exec(path);

  if (room) {
    // Forward the live native response, including its upgrade capability.
    return ctx.cell("ROOMS", room[1]).fetchStream(
      request(req.url, {
        headers: req.headers,
      }),
    );
  }

  return response("Use /events, /ws, /rooms/demo/ws or /rooms/demo/state", {
    status: 404,
  });
}
