// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

/** Byte-only identity transform with standard backpressure and BYOB reads. */
export class IdentityTransformStream {
  constructor() {
    const inner = new TransformStream({
      transform(chunk, controller) {
        if (!ArrayBuffer.isView(chunk))
          throw new TypeError(
            "IdentityTransformStream requires a typed byte buffer",
          );
        const bytes = new Uint8Array(
          chunk.buffer,
          chunk.byteOffset,
          chunk.byteLength,
        );
        // A byte readable transfers its chunks. Keep the caller's buffer intact.
        if (bytes.byteLength) controller.enqueue(bytes.slice());
      },
    });
    const reader = inner.readable.getReader();
    const readable = new ReadableStream({
      type: "bytes",
      async pull(controller) {
        try {
          const { done, value } = await reader.read();
          if (done) {
            const pending = controller.byobRequest;
            controller.close();
            pending?.respond(0);
            reader.releaseLock();
          } else controller.enqueue(value);
        } catch (error) {
          controller.error(error);
          reader.releaseLock();
        }
      },
      cancel(reason) {
        return reader.cancel(reason).finally(() => reader.releaseLock());
      },
    });
    Object.defineProperties(this, {
      readable: { value: readable, enumerable: true },
      writable: { value: inner.writable, enumerable: true },
    });
  }
}

export function installWebExtensions() {
  if (typeof Response === "function" && !("webSocket" in Response.prototype)) {
    Object.defineProperty(Response.prototype, "webSocket", {
      get: () => null,
      configurable: true,
    });
  }
  if (
    globalThis.IdentityTransformStream === undefined &&
    typeof TransformStream === "function"
  ) {
    Object.defineProperty(globalThis, "IdentityTransformStream", {
      value: IdentityTransformStream,
      configurable: true,
      writable: true,
    });
  }
}
