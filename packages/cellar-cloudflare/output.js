// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { headerPairs, httpLimit, messageHeaders } from "./http.js";
import { encodeWebSocketResponse } from "./websocket-upgrade.js";
import { cancelUntransferredWebSocket } from "./websocket.js";

/** Convert a web Response without consuming its body before the native head. */
export function encodeStreamResponse(response, context) {
  if (!(response instanceof Response))
    throw new TypeError("Expected an HTTP Response");
  if (response.webSocket)
    return (async () => {
      try {
        return await encodeWebSocketResponse(
          response,
          context,
          headerPairs(response.headers),
        );
      } catch (error) {
        // A failed head must not retain an accepted local peer forever. A
        // foreign or already-transferred endpoint is never cancelled here.
        try {
          await cancelUntransferredWebSocket(response.webSocket, context);
        } catch {}
        throw error;
      }
    })();
  if (response.bodyUsed) {
    const error = new TypeError(
      "The returned Response body has already been used",
    );
    response.body?.cancel(error).catch(() => {});
    throw error;
  }
  let headers;
  try {
    headers = messageHeaders(response);
  } catch (error) {
    response.body?.cancel(error).catch(() => {});
    throw error;
  }
  if (!response.body) return { status: response.status, headers, body: "" };
  const limit = httpLimit(context);
  const reader = response.body.getReader();
  let released = false;
  const cancel = async (reason) => {
    if (released) return;
    released = true;
    try {
      await reader.cancel(reason);
    } finally {
      reader.releaseLock();
    }
  };
  return {
    status: response.status,
    headers,
    body: "",
    cancel,
    chunks: {
      async *[Symbol.asyncIterator]() {
        const stop = new AbortController();
        // This waiter is owned and joined, including after a finite response.
        // A disconnect must wake a pending reader, while independent waitUntil
        // work retains the original native event and can finish normally.
        let closed;
        try {
          closed = context
            ._call("response.closed", {}, undefined, stop.signal)
            .then(() => null)
            .catch((error) => {
              if (!stop.signal.aborted) throw error;
              return null;
            });
          for (;;) {
            const chunk = await Promise.race([reader.read(), closed]);
            if (chunk === null || chunk.done) return;
            if (
              !(chunk.value instanceof Uint8Array) ||
              chunk.value.byteLength > limit
            ) {
              throw new RangeError(
                "Workers HTTP response chunk exceeds host limit",
              );
            }
            if (chunk.value.byteLength) yield chunk.value;
          }
        } finally {
          stop.abort();
          try {
            if (closed) await closed;
          } finally {
            await cancel();
          }
        }
      },
    },
  };
}
