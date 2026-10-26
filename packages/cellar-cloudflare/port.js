// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { fetchRequest } from "./http.js";
import { streamedResponse } from "./response.js";

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
        (request, signal) =>
          context._call(
            "workers.container.fetch_stream",
            { port, request },
            undefined,
            signal,
          ),
        () => beforeFetch(context),
        // A response reader keeps this event's context, even if the activation
        // later handles another event with colliding local resource IDs.
        (frame, _context, signal) => streamedResponse(frame, context, signal),
      );
    },
  };
  Object.defineProperty(capability, "__cellar_runtime_capability__", {
    value: () => {},
    enumerable: true,
  });
  return Object.freeze(capability);
}
