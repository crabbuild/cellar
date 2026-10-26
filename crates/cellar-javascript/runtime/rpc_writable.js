// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// Private factory captured before application initialization. The native RPC
// session owns authority, quotas and proof; these writers own only web streams.
() => {
  const Stream = globalThis.WritableStream;
  const Writer = globalThis.WritableStreamDefaultWriter;
  const Bytes = Uint8Array;
  const apply = Reflect.apply;
  const getWriter = Stream?.prototype.getWriter;
  const write = Writer?.prototype.write;
  const closeWriter = Writer?.prototype.close;
  const abortWriter = Writer?.prototype.abort;
  const unlock = Writer?.prototype.releaseLock;
  const slice = Bytes.prototype.slice;
  const ErrorClass = Error;
  const own = Object.getOwnPropertyDescriptor;

  function partialFailure(error) {
    try {
      if (
        error === null ||
        !["object", "function"].includes(typeof error) ||
        own(error, "outcome")?.value !== "not_committed"
      )
        return error;
      const message = own(error, "message")?.value;
      const partial = new ErrorClass(
        typeof message === "string" ? message : "RPC write incomplete",
        { cause: error },
      );
      partial.code = "Ambiguous";
      partial.outcome = "unknown";
      return partial;
    } catch {
      const partial = new ErrorClass("RPC write incomplete", { cause: error });
      partial.code = "Ambiguous";
      partial.outcome = "unknown";
      return partial;
    }
  }

  function chunkSize(maximum) {
    // Routed calls wrap base64 arguments and a native group identity. Reserve
    // 128 bytes for that envelope and 64 for clone-array/typed-buffer fields.
    if (!Number.isSafeInteger(maximum))
      throw new RangeError("Invalid RPC writable byte budget");
    const size = Math.min(
      64 * 1024,
      Math.floor(((maximum - 128) * 3) / 4) - 64,
    );
    if (!Number.isSafeInteger(size) || size <= 0)
      throw new RangeError("RPC writable byte budget is too small");
    return size;
  }

  function send(stream, context) {
    const size = chunkSize(context._sync("workers.http.limits").max_body_bytes);
    const writer = apply(getWriter, stream, []);
    let state = "open";
    let writing = false;
    let aborting;
    const abort = (reason) => {
      if (state === "closed" || state === "failed") return Promise.resolve();
      if (!aborting) {
        state = "aborting";
        // Standard writer abort waits for accepted in-flight writes. Neither
        // cancellation nor lease loss can establish that those writes rolled back.
        aborting = apply(abortWriter, writer, [reason]).then(
          () => {
            state = "closed";
          },
          (error) => {
            state = "failed";
            throw error;
          },
        );
      }
      return aborting;
    };
    return {
      restore() {
        apply(unlock, writer, []);
      },
      close: abort,
      async invoke(operation, argument) {
        if (operation === "abort") {
          await abort(argument);
          return null;
        }
        if (state !== "open" || writing)
          throw new TypeError("Invalid or concurrent RPC writable operation");
        if (operation !== "write" && operation !== "close")
          throw new TypeError("Invalid RPC writable operation");
        if (
          operation === "write" &&
          (!(argument instanceof Bytes) || argument.byteLength > size)
        )
          throw new TypeError("Invalid RPC writable byte chunk");
        writing = true;
        try {
          if (operation === "close") {
            state = "closing";
            await apply(closeWriter, writer, []);
            state = "closed";
          } else await apply(write, writer, [argument]);
          return null;
        } catch (error) {
          state = "failed";
          throw error;
        } finally {
          writing = false;
        }
        // Keep the original writer locked after transfer, including at close.
      },
    };
  }

  function receive(
    call,
    release,
    prepareReason = (reason) => reason,
    maximum = 64 * 1024,
  ) {
    if (!Stream)
      throw new TypeError("RPC writable streams require web globals");
    const size = chunkSize(maximum);
    let controller;
    let ending;
    let ended = false;
    const finish = (operation, reason) => {
      if (!ending) {
        ending = (async () => {
          let refusal;
          if (operation === "abort") {
            try {
              reason = prepareReason(reason);
            } catch (error) {
              refusal = error;
              reason = undefined;
            }
          }
          try {
            await call(operation, reason);
          } finally {
            ended = true;
            await release();
          }
          if (refusal) throw refusal;
        })();
      }
      return ending;
    };
    const stream = new Stream({
      start(value) {
        controller = value;
      },
      async write(bytes) {
        if (ending || ended) throw new Error("RPC writable disposed");
        if (!(bytes instanceof Bytes)) {
          const error = new TypeError(
            "RPC writable requires Uint8Array chunks",
          );
          try {
            await finish("abort", error);
          } catch {}
          throw error;
        }
        let accepted = false;
        try {
          if (!bytes.byteLength) {
            await call("write", new Bytes(0));
            accepted = true;
          }
          for (let offset = 0; offset < bytes.byteLength; offset += size) {
            if (ending || ended) throw new Error("RPC writable disposed");
            await call("write", apply(slice, bytes, [offset, offset + size]));
            accepted = true;
          }
        } catch (error) {
          // A later frame's pre-effect refusal cannot prove rollback of the
          // larger write, whose earlier frames have already been acknowledged.
          if (accepted) error = partialFailure(error);
          try {
            await finish("abort", error);
          } catch {}
          throw error;
        }
      },
      close: () => finish("close"),
      abort: (reason) => finish("abort", reason),
    });
    return {
      stream,
      async dispose() {
        if (ended) return;
        controller.error(new Error("RPC writable disposed"));
        await finish("abort");
      },
    };
  }
  return {
    isWritable: (value) => Stream && value instanceof Stream,
    send,
    receive,
  };
}
