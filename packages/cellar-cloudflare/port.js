// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { fetchRequest } from "./http.js";
import { streamedResponse } from "./response.js";
import {
  eventWebSocket,
  websocketResponse,
  websocketRuntime,
} from "./websocket.js";

/** An activation-owned port; native routing grants only this port's authority. */
export function containerPort(current, port, beforeFetch) {
  if (!Number.isInteger(port) || port < 1 || port > 65535) {
    throw new RangeError("Container port must be an integer from 1 to 65535");
  }
  current();
  const capability = {
    async fetch(input, init) {
      const context = current();
      return fetchRequest(
        input,
        init,
        context,
        async (request, signal) => {
          const headers = new Headers(request.headers);
          if (headers.get("upgrade")?.toLowerCase() !== "websocket")
            return context._call(
              "workers.container.fetch_stream",
              { port, request },
              undefined,
              signal,
            );
          if (request.method !== "GET" || request.body !== "")
            throw new TypeError("WebSocket upgrades require a bodyless GET");
          websocketRuntime();
          const frame = await context._call(
            "container.port.connect_websocket",
            {
              port,
              url: request.url,
              headers: request.headers,
            },
            undefined,
            signal,
          );
          if (!frame.upgraded) return { ...frame.response, stream: null };
          return { websocket: frame };
        },
        () => beforeFetch(context),
        // A response reader keeps this event's context, even if the activation
        // later handles another event with colliding local resource IDs.
        (frame, _context, signal, request) => {
          if (!frame.websocket)
            return streamedResponse(frame, context, signal, {
              bodyless: request.method === "HEAD",
            });
          try {
            return websocketResponse(
              frame.websocket,
              eventWebSocket(frame.websocket, context, current),
            );
          } catch (error) {
            // Upgrade effects may already have occurred. Refusal to represent
            // its response cannot become a pre-effect failure of the fetch.
            context
              ._call("container.release", { id: frame.websocket.id })
              .catch(() => {});
            const failure = new Error(
              "WebSocket upgrade response unavailable",
              { cause: error },
            );
            failure.code = "Ambiguous";
            failure.outcome = "unknown";
            throw failure;
          }
        },
      );
    },
  };
  Object.defineProperty(capability, "__cellar_runtime_capability__", {
    value: () => {},
    enumerable: true,
  });
  return Object.freeze(capability);
}
