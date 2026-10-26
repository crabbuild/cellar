// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// Captured before application initialization. Streams use the same native
// leases, original producer event and output proof as returned RPC functions.
() => {
  const Stream = globalThis.ReadableStream;
  const Reader = globalThis.ReadableStreamBYOBReader;
  const DefaultReader = globalThis.ReadableStreamDefaultReader;
  const Bytes = Uint8Array;
  const apply = Reflect.apply;
  const getReader = Stream?.prototype.getReader;
  const read = Reader?.prototype.read;
  const cancelReader = Reader?.prototype.cancel;
  const unlock = Reader?.prototype.releaseLock;
  const defaultRead = DefaultReader?.prototype.read;
  const defaultCancel = DefaultReader?.prototype.cancel;
  const defaultUnlock = DefaultReader?.prototype.releaseLock;

  function send(stream, context, allowDefault = false) {
    const maximum = context._sync("workers.http.limits").max_body_bytes;
    // Reserve space for the clone envelope and read-result fields. Native clone
    // and bridge budgets independently validate every completed delegate reply.
    if (!Number.isSafeInteger(maximum) || maximum <= 128)
      throw new RangeError("RPC stream byte budget is too small");
    const size = Math.min(64 * 1024, maximum - 128);
    // Input streams require a byte source before invoking the target. Returned
    // streams and Response bodies can use a default reader (e.g. Cap'n Web
    // pipes); validate each chunk before forwarding and retain at most one
    // chunk within the host body budget. Never coerce arbitrary objects.
    let reader,
      byob = true;
    try {
      reader = apply(getReader, stream, [{ mode: "byob" }]);
    } catch (error) {
      if (!allowDefault || stream.locked) throw error;
      byob = false;
      reader = apply(getReader, stream, []);
    }
    const readChunk = byob ? read : defaultRead;
    const cancelChunk = byob ? cancelReader : defaultCancel;
    const release = byob ? unlock : defaultUnlock;
    let buffered;
    let ended = false;
    let reading = false;
    let closing;
    const close = (reason) => {
      if (!closing) {
        ended = true;
        buffered = undefined;
        closing = apply(cancelChunk, reader, [reason]);
      }
      return closing;
    };
    return {
      restore() {
        // Publication failed before transferring ownership or issuing a read.
        apply(release, reader, []);
      },
      close,
      async invoke(operation, argument) {
        if (operation === "cancel") {
          await close(argument);
          return null;
        }
        if (operation !== "read" || reading)
          throw new TypeError("Invalid or concurrent RPC stream read");
        if (!Number.isSafeInteger(argument) || argument <= 0)
          throw new TypeError("Invalid RPC stream read size");
        if (ended) return { done: true };
        reading = true;
        try {
          const grant = Math.min(size, argument);
          const chunk = buffered
            ? { done: false, value: buffered }
            : await apply(readChunk, reader, byob ? [new Bytes(grant)] : []);
          if (chunk.done) {
            ended = true;
            return { done: true };
          }
          if (
            !(chunk.value instanceof Bytes) ||
            chunk.value.byteLength > maximum
          )
            throw new TypeError("RPC stream requires bounded byte chunks");
          // Own the unread suffix: a small view may otherwise retain a much
          // larger backing buffer supplied by a default source.
          const value = chunk.value.slice(0, grant);
          buffered =
            chunk.value.byteLength > grant
              ? chunk.value.slice(grant)
              : undefined;
          return { done: false, value };
        } catch (error) {
          ended = true;
          throw error;
        } finally {
          reading = false;
        }
        // The sender's stream stays locked after transfer, including at EOF.
        // Cancelling its native consumer is distinct from unlocking ownership.
      },
    };
  }

  function receive(
    call,
    release,
    prepareReason = (reason) => reason,
    maximum = 64 * 1024 + 128,
  ) {
    if (!Stream) throw new TypeError("RPC byte streams require web globals");
    if (!Number.isSafeInteger(maximum) || maximum <= 128)
      throw new RangeError("RPC stream byte budget is too small");
    const size = Math.min(64 * 1024, maximum - 128);
    let controller;
    let ended = false;
    let closing;
    const close = (reason) => {
      if (!closing) {
        ended = true;
        closing = (async () => {
          let refusal;
          try {
            reason = prepareReason(reason);
          } catch (error) {
            // A reason that cannot cross RPC must not strand a pending read.
            // Refuse that value locally, cancel without it, then report refusal.
            refusal = error;
            reason = undefined;
          }
          try {
            await call("cancel", reason);
          } finally {
            await release();
          }
          if (refusal) throw refusal;
        })();
      }
      return closing;
    };
    const stream = new Stream({
      type: "bytes",
      start(value) {
        controller = value;
      },
      async pull() {
        if (ended) return;
        try {
          while (!ended) {
            const chunk = await call("read", size);
            if (ended) return;
            if (chunk.done === true) {
              await release();
              if (ended) return;
              ended = true;
              const pending = controller.byobRequest;
              controller.close();
              pending?.respond(0);
              return;
            }
            if (chunk.done !== false || !(chunk.value instanceof Bytes))
              throw new TypeError("Invalid RPC byte stream frame");
            if (chunk.value.byteLength) {
              controller.enqueue(chunk.value);
              return;
            }
            // Empty default-stream chunks do not satisfy a pending byte read.
            // Each retry still consumes a native operation and its byte budget.
          }
        } catch (error) {
          if (ended) return;
          controller.error(error);
          await close(error);
        }
      },
      cancel: close,
    });
    return {
      stream,
      async dispose() {
        if (ended) return;
        controller.error(new Error("RPC stream disposed"));
        await close();
      },
    };
  }
  return {
    isReadable: (value) => Stream && value instanceof Stream,
    send,
    sendReturned: (stream, context) => send(stream, context, true),
    receive,
  };
}
