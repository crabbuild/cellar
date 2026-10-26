// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { decodeResponse } from "./http.js";

/** A port response retains its original event; pulls never borrow a later one. */
export function streamedResponse(
  frame,
  context,
  signal,
  {
    read = "container.response.read",
    cancel = "container.response.cancel",
    releaseOnEnd = false,
    bodyless = false,
  } = {},
) {
  if (bodyless || [204, 205, 304].includes(frame.status)) {
    if (frame.stream !== null)
      context._call(cancel, { id: frame.stream }).catch(() => {});
    return new Response(null, { status: frame.status, headers: frame.headers });
  }
  if (frame.stream === null) return decodeResponse(frame, context);
  const id = frame.stream;
  let finished = false;
  let controller;
  const detach = () => signal.removeEventListener("abort", abort);
  const release = async () => {
    if (finished) return;
    finished = true;
    detach();
    await context._call(cancel, { id });
  };
  const abort = () => {
    if (finished) return;
    controller.error(signal.reason);
    release().catch(() => {});
  };
  try {
    const body = new ReadableStream({
      type: "bytes",
      start(value) {
        controller = value;
        const prefix = context._decode(frame.body);
        if (prefix.byteLength) controller.enqueue(prefix);
        signal.addEventListener("abort", abort, { once: true });
        if (signal.aborted) abort();
      },
      async pull() {
        if (finished) return;
        try {
          while (!finished) {
            const chunk = await context._call(read, { id }, undefined, signal);
            if (finished) return;
            if (chunk.done) {
              if (releaseOnEnd) await context._call(cancel, { id });
              if (finished) return;
              finished = true;
              detach();
              const pending = controller.byobRequest;
              controller.close();
              pending?.respond(0);
              return;
            } else {
              const bytes = context._decode(chunk.body);
              if (bytes.byteLength) {
                controller.enqueue(bytes);
                return;
              }
            }
          }
        } catch (error) {
          if (finished) return;
          controller.error(signal.aborted ? signal.reason : error);
          await release();
        }
      },
      cancel: release,
    });
    return new Response(body, { status: frame.status, headers: frame.headers });
  } catch (error) {
    release().catch(() => {});
    throw error;
  }
}
