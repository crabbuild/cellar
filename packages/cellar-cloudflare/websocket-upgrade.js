// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { transferWebSocket } from "./websocket.js";

function closeFrame(frame) {
  const code = frame?.code;
  const valid =
    Number.isInteger(code) &&
    ((code >= 1000 && code <= 1014 && ![1004, 1005, 1006].includes(code)) ||
      (code >= 3000 && code <= 4999));
  const reason = frame?.reason;
  return valid &&
    typeof reason === "string" &&
    new TextEncoder().encode(reason).length <= 123
    ? { code, reason }
    : { code: 1001, reason: "" };
}

/** Transfer an unaccepted endpoint; pumps stay in the opening native event. */
export async function encodeWebSocketResponse(response, context, headers) {
  if (response.status !== 101 || response.body !== null)
    throw new TypeError("WebSocket upgrades require status 101 and no body");
  const source = transferWebSocket(response.webSocket, context);
  // A namespace response already owns a native upgrade. Forward it without
  // accepting it or creating another language/native proxy at every hop.
  if (source.forward !== undefined) return { forward: source.forward, headers };

  const stop = new AbortController();
  let id,
    closing,
    pumps = [],
    ran = false;
  const close = (frame) => {
    if (closing) return closing;
    stop.abort();
    const { code, reason } = closeFrame(frame);
    closing = (async () => {
      const results = await Promise.allSettled([
        source.close(code, reason, frame?.was_clean ?? frame?.clean ?? false),
        id === undefined
          ? undefined
          : context._call("websocket.close", { id, code, reason }),
      ]);
      await Promise.allSettled(pumps);
      const failed = results.find((result) => result.status === "rejected");
      if (failed) throw failed.reason;
    })();
    return closing;
  };
  try {
    id = (await context._call("websocket.pair")).id;
    await context._call("websocket.accept", {
      id,
      hibernating: false,
      polled: true,
      tags: [],
    });
  } catch (error) {
    await close().catch(() => {});
    throw error;
  }

  return {
    websocket: id,
    headers,
    cancel: () => close(),
    async run() {
      if (ran) throw new TypeError("WebSocket bridge already started");
      ran = true;
      async function outgoing() {
        for (;;) {
          const frame = await source.read(stop.signal);
          if (frame.type === "open") continue;
          if (frame.type === "close") return frame;
          if (frame.type !== "text" && frame.type !== "binary")
            throw new TypeError("Invalid WebSocket source frame");
          await context._call("websocket.send", {
            id,
            ...(frame.type === "text"
              ? { text: frame.text }
              : { body: frame.data }),
          });
        }
      }
      async function incoming() {
        for (;;) {
          const frame = await context._call(
            "websocket.next",
            { id },
            undefined,
            stop.signal,
          );
          if (frame.type === "open") continue;
          if (frame.type === "close") return frame;
          if (frame.type !== "message")
            throw new TypeError("Invalid native WebSocket frame");
          await source.send(
            frame.body === undefined
              ? { type: "text", text: frame.text }
              : { type: "binary", data: frame.body },
          );
        }
      }
      // A local peer may not have called accept(), leaving delivery held. The
      // native transport can still retire; observe it without another frame pull.
      pumps = [
        outgoing(),
        incoming(),
        context._call("websocket.closed", { id }, undefined, stop.signal),
      ];
      try {
        const frame = await Promise.race(pumps);
        await close(frame);
      } finally {
        await close();
      }
    },
  };
}
