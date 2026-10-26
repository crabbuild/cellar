// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { eventWebSocket, websocketResponse } from "./websocket.js";

/** Namespace upgrades may be forwarded intact or accepted by the opening event. */
export function forwardedWebSocket(frame, owner, current) {
  let opening,
    forwarded = false;
  const open = () =>
    (opening ??= owner._call("websocket.from_response", {
      response: frame.forward,
    }));
  const adapter = {
    _sync: owner._sync,
    _encode: owner._encode,
    _decode: owner._decode,
    _rpcCleanup: owner._rpcCleanup,
    _rpcRetain: owner._rpcRetain,
    _forward() {
      forwarded = true;
    },
    async _call(operation, input, _body, signal) {
      if (operation === "container.release") {
        if (forwarded) return;
        if (!opening)
          return owner._call("response.cancel", { id: frame.forward });
        const { id } = await opening;
        return owner._call("websocket.close", { id, code: 1001, reason: "" });
      }
      const { id } = await open();
      if (operation === "container.websocket.receive") {
        const event = await owner._call(
          "websocket.next",
          { id },
          undefined,
          signal,
        );
        if (event.type !== "message")
          return { ...event, clean: event.was_clean };
        return event.body === undefined
          ? { type: "text", text: event.text }
          : { type: "binary", data: event.body };
      }
      if (operation === "container.websocket.close")
        return owner._call("websocket.close", {
          id,
          code: input.code,
          reason: input.reason,
        });
      if (operation === "container.websocket.send_text")
        return owner._call("websocket.send", { id, text: input.text });
      if (operation === "container.websocket.send_binary")
        return owner._call("websocket.send", { id, body: input.data });
      throw new TypeError("Invalid namespace WebSocket operation");
    },
  };
  return websocketResponse(
    frame,
    eventWebSocket(
      {
        ...frame,
        id: frame.forward,
        protocol:
          new Headers(frame.headers).get("sec-websocket-protocol") ?? "",
      },
      adapter,
      current,
      owner,
    ),
  );
}
