// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// Captured before application initialization. Streams use the same native
// leases, original producer event and output proof as returned RPC functions.
() => {
  const Stream = globalThis.ReadableStream;
  const Reader = globalThis.ReadableStreamBYOBReader;
  const Bytes = Uint8Array;
  const apply = Reflect.apply;
  const getReader = Stream?.prototype.getReader;
  const read = Reader?.prototype.read;
  const cancelReader = Reader?.prototype.cancel;
  const unlock = Reader?.prototype.releaseLock;

  function send(stream, context) {
    const maximum = context._sync("workers.http.limits").max_body_bytes;
    // Reserve space for the clone envelope and read-result fields. Native clone
    // and bridge budgets independently validate every completed delegate reply.
    if (!Number.isSafeInteger(maximum) || maximum <= 128)
      throw new RangeError("RPC stream byte budget is too small");
    const size = Math.min(64 * 1024, maximum - 128);
    // A BYOB reader refuses object streams and locked streams before any pull.
    const reader = apply(getReader, stream, [{ mode: "byob" }]);
    let ended = false;
    let reading = false;
    let closing;
    const close = (reason) => {
      if (!closing) {
        ended = true;
        closing = apply(cancelReader, reader, [reason]);
      }
      return closing;
    };
    return {
      restore() {
        // Publication failed before transferring ownership or issuing a read.
        apply(unlock, reader, []);
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
          const chunk = await apply(read, reader, [
            new Bytes(Math.min(size, argument)),
          ]);
          if (chunk.done) {
            ended = true;
            return { done: true };
          }
          return { done: false, value: chunk.value };
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
          const chunk = await call("read", size);
          if (ended) return;
          if (chunk.done === true) {
            await release();
            if (ended) return;
            ended = true;
            const pending = controller.byobRequest;
            controller.close();
            pending?.respond(0);
          } else if (chunk.done === false && chunk.value instanceof Bytes) {
            controller.enqueue(chunk.value);
          } else throw new TypeError("Invalid RPC byte stream frame");
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
    receive,
  };
}
