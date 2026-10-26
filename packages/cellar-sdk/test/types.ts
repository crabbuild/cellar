// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import {
  type Cell,
  type Context,
  type Request,
  type WebSocketMessage,
  type WebSocketClose,
  type FetchOutput,
  sseResponse,
  sseEvent,
  webSocketResponse,
  streamResponse,
  request,
} from "../index.js";

// Compile each streaming return shape and the native cell WebSocket callbacks.
class Room implements Cell {
  async fetch(req: Request, ctx: Context): Promise<FetchOutput> {
    if (req.url.endsWith("/ws")) {
      const socket = await ctx.websocketPair();
      await socket.accept({ hibernating: true, tags: ["room"] });

      return webSocketResponse(socket);
    }

    const upstream = await ctx
      .cell("ROOMS", "test")
      .fetchStream(request(req.url));

    if (req.url.endsWith("/forward")) {
      return upstream;
    }

    if (req.url.endsWith("/consume")) {
      return streamResponse(upstream.chunks());
    }

    return sseResponse([sseEvent("hello", { event: "update", id: "1" })]);
  }

  async websocket_message(
    event: WebSocketMessage,
    ctx: Context,
  ): Promise<void> {
    await ctx.websocket(event.id).send(event.data);
  }

  async websocket_close(_event: WebSocketClose, _ctx: Context): Promise<void> {
    // Verify the close callback signature without adding fixture behavior.
  }
}

const room: Cell = new Room();
void room;

async function consumeContainerStream(ctx: Context, signal: AbortSignal) {
  const response = await ctx.container.port(3000).fetchStream(request("http://container/events"), { signal });
  const status: number = response.status;
  const next: Uint8Array | null = await response.read();
  for await (const bytes of response.chunks()) {
    const owned: Uint8Array = bytes;
    void owned;
  }
  await response.cancel();
  return { status, next };
}
void consumeContainerStream;
