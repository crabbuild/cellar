// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

const fixedLengths = new WeakMap();

function byteTransform(target, length) {
  let remaining = length;
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
      if (remaining !== undefined) {
        if (bytes.byteLength > remaining)
          throw new TypeError("FixedLengthStream received too many bytes");
        remaining -= bytes.byteLength;
      }
      // A byte readable transfers its chunks. Keep the caller's buffer intact.
      if (bytes.byteLength) controller.enqueue(bytes.slice());
    },
    flush() {
      if (remaining !== undefined && remaining !== 0)
        throw new TypeError("FixedLengthStream received too few bytes");
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
  if (length !== undefined) fixedLengths.set(readable, length);
  Object.defineProperties(target, {
    readable: { value: readable, enumerable: true },
    writable: { value: inner.writable, enumerable: true },
  });
}

/** Byte-only identity transform with standard backpressure and BYOB reads. */
export class IdentityTransformStream {
  constructor() {
    byteTransform(this);
  }
}

/** Byte-counted identity transform; a short close or overflowing write errors. */
export class FixedLengthStream {
  constructor(length) {
    if (typeof length !== "number" && typeof length !== "bigint")
      throw new TypeError(
        "FixedLengthStream requires a number or bigint length",
      );
    if (
      length < 0 ||
      length > Number.MAX_SAFE_INTEGER ||
      !Number.isSafeInteger(Number(length))
    )
      throw new RangeError(
        "FixedLengthStream length must be a nonnegative safe integer",
      );
    byteTransform(this, Number(length));
  }
}

// Only streams created here carry a trusted length. Guest properties never
// establish HTTP framing, and consuming a stream still validates its byte count.
export function fixedLength(readable) {
  return fixedLengths.get(readable);
}

/** Preserve a validated source's length through Request's body proxy. */
export function streamRequest(input, init) {
  let source = input instanceof Request ? input.body : undefined;
  // Read an init body's accessor only when the native constructor does. Reading
  // it twice could associate the first stream's length with a different body.
  const dictionary =
    init !== null && (typeof init === "object" || typeof init === "function");
  const options = !dictionary
    ? init
    : new Proxy(init, {
        get(target, key) {
          const value = Reflect.get(target, key, target);
          if (key === "body" && value != null) source = value;
          return value;
        },
      });
  const request = new Request(input, options);
  const length = fixedLength(source);
  if (length !== undefined && request.body)
    fixedLengths.set(request.body, length);
  return request;
}

export function installWebExtensions() {
  if (typeof Response === "function" && !("webSocket" in Response.prototype)) {
    Object.defineProperty(Response.prototype, "webSocket", {
      get: () => null,
      configurable: true,
    });
  }
  // Bundlers may rename a class when upstream uses the same global name.
  // Install protocol names explicitly rather than depending on Function.name.
  for (const [name, value] of [
    ["IdentityTransformStream", IdentityTransformStream],
    ["FixedLengthStream", FixedLengthStream],
  ]) {
    if (globalThis[name] !== undefined || typeof TransformStream !== "function")
      continue;
    Object.defineProperty(globalThis, name, {
      value,
      configurable: true,
      writable: true,
    });
  }
}
