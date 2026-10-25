// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// Buffer only up to the host's declared limit. Request body consumption occurs
// before native submission; cancellation here cannot have HTTP side effects.
async function bodyBytes(request, limit) {
  if (!request.body) return new Uint8Array();
  const reader = request.body.getReader();
  const chunks = [];
  let size = 0;
  const abort = () => {
    reader.cancel(request.signal.reason).catch(() => {});
  };
  request.signal.addEventListener("abort", abort, { once: true });
  try {
    request.signal.throwIfAborted();
    for (;;) {
      const { done, value } = await reader.read();
      request.signal.throwIfAborted();
      if (done) break;
      if (!(value instanceof Uint8Array) || value.byteLength > limit - size) {
        await reader.cancel("Container request body exceeds host limit");
        throw new RangeError("Container request body exceeds host limit");
      }
      // A source may reuse its chunk after the next pull.
      chunks.push(value.slice());
      size += value.byteLength;
    }
    const output = new Uint8Array(size);
    let offset = 0;
    for (const chunk of chunks) {
      output.set(chunk, offset);
      offset += chunk.byteLength;
    }
    return output;
  } finally {
    if (request.signal.aborted) abort();
    request.signal.removeEventListener("abort", abort);
    reader.releaseLock();
  }
}

/** An activation-owned port; native routing grants only this port's authority. */
export function containerPort(current, port, beforeFetch) {
  if (!Number.isInteger(port) || port < 1 || port > 65535) {
    throw new RangeError("Container port must be an integer from 1 to 65535");
  }
  current();
  const capability = {
    async fetch(input, init) {
      const context = current();
      if (typeof Request !== "function" || typeof Response !== "function") {
        throw new Error("Container HTTP requires the web runtime profile");
      }
      const request = new Request(input, init);
      request.signal.throwIfAborted();
      await beforeFetch(context);
      const { max_body_bytes: limit } = await context.container.capabilities();
      const body = await bodyBytes(request, limit);
      try {
        const response = await context.container.port(port).fetch(
          {
            url: request.url,
            method: request.method,
            headers: [...request.headers],
            body: context._encode(body),
          },
          { signal: request.signal },
        );
        return new Response(
          [204, 205, 304].includes(response.status) ? null : response.body,
          {
            status: response.status,
            headers: response.headers,
          },
        );
      } catch (error) {
        if (error.code === "Cancelled" && request.signal.aborted) {
          throw request.signal.reason;
        }
        throw error;
      }
    },
  };
  Object.defineProperty(capability, "__cellar_runtime_capability__", {
    value: () => {},
    enumerable: true,
  });
  return Object.freeze(capability);
}
