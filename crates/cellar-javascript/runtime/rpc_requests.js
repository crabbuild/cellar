// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// HTTP request bodies and abort observation share the original native lease.
// A body reaching EOF does not retire a still-live client abort signal.
(readables, headerEntries) => {
  const RequestClass = globalThis.Request;
  const UrlClass = globalThis.URL;
  const AbortClass = globalThis.AbortController;
  const SignalClass = globalThis.AbortSignal;
  const DomException = globalThis.DOMException;
  const ErrorClass = Error;
  const PromiseClass = Promise;
  const allSettled = Promise.allSettled.bind(Promise);
  const define = Object.defineProperty;
  const exceptionName =
    DomException &&
    Object.getOwnPropertyDescriptor(DomException.prototype, "name").get;
  const exceptionMessage =
    DomException &&
    Object.getOwnPropertyDescriptor(DomException.prototype, "message").get;
  const apply = Reflect.apply;
  const add = globalThis.EventTarget?.prototype.addEventListener;
  const remove = globalThis.EventTarget?.prototype.removeEventListener;
  const aborted =
    SignalClass &&
    Object.getOwnPropertyDescriptor(SignalClass.prototype, "aborted").get;
  const signalReason =
    SignalClass &&
    Object.getOwnPropertyDescriptor(SignalClass.prototype, "reason").get;
  const encoder = globalThis.TextEncoder && new TextEncoder();
  const abortError = (message) =>
    DomException
      ? new DomException(message, "AbortError")
      : new ErrorClass(message);

  function send(request, context) {
    if (request.cf !== undefined)
      throw new TypeError("RPC Request.cf is not supported by this profile");
    const body = request.body;
    if (request.bodyUsed || body?.locked)
      throw new TypeError("RPC request body has already been used or locked");
    const signal = request.signal;
    if (apply(aborted, signal, []))
      throw abortError("RPC request is already aborted");
    const metadata = {
      method: request.method,
      url: request.url,
      headers: headerEntries(request),
      body: body !== null,
      redirect: request.redirect,
      cache: request.cache,
      credentials: request.credentials,
      integrity: request.integrity,
      keepalive: request.keepalive,
      mode: request.mode,
      referrer: request.referrer,
      referrerPolicy: request.referrerPolicy,
      destination: request.destination,
    };
    for (const field of ["url", "referrer", "integrity"])
      if (encoder.encode(metadata[field]).byteLength > 8192)
        throw new RangeError("RPC request metadata exceeds limit");
    if (!["http:", "https:"].includes(new UrlClass(metadata.url).protocol))
      throw new TypeError("RPC requests require an HTTP URL");
    if (body && metadata.keepalive)
      throw new TypeError("RPC streamed requests cannot use keepalive");
    // Validate the constructible HTTP head before locking any source. The native
    // clone validator independently checks its fields before routing the call.
    new RequestClass(metadata.url, { ...metadata, body: undefined });
    const source = body
      ? readables.sendReturned(body, context)
      : {
          restore() {},
          async close() {},
          async invoke(operation) {
            if (operation === "read") return { done: true };
            if (operation === "cancel") return null;
            throw new TypeError("Invalid RPC request operation");
          },
        };
    let resolveSignal,
      rejectSignal,
      aborting,
      restored = false;
    const notification = new PromiseClass((resolve, reject) => {
      resolveSignal = resolve;
      rejectSignal = reject;
    });
    // This continuation belongs to the original producer, even when a later
    // event aborts its controller. Never lend that event the source's SDK scope.
    const observed = notification.then((state) => {
      if (!state.aborted) return state;
      const reason = apply(signalReason, signal, []);
      source.close(reason).catch(() => {});
      return DomException && reason instanceof DomException
        ? {
            aborted: true,
            exception: {
              name: apply(exceptionName, reason, []),
              message: apply(exceptionMessage, reason, []),
            },
          }
        : { aborted: true, reason };
    });
    observed.catch(() => {});
    const onAbort = () => {
      if (!apply(aborted, signal, []) || aborting) return;
      try {
        const current = context._current();
        aborting =
          current.identity === null
            ? PromiseClass.resolve()
            : current.storage.sync();
        aborting.then(() => resolveSignal({ aborted: true }), rejectSignal);
      } catch (error) {
        rejectSignal(error);
      }
    };
    const unsubscribe = () => apply(remove, signal, ["abort", onAbort]);
    try {
      apply(add, signal, ["abort", onAbort, { once: true }]);
      onAbort();
    } catch (error) {
      source.restore();
      throw error;
    }
    const close = async () => {
      unsubscribe();
      resolveSignal({ aborted: false });
      if (!restored) await source.close();
    };
    return {
      metadata,
      restore() {
        restored = true;
        unsubscribe();
        resolveSignal({ aborted: false });
        source.restore();
      },
      close,
      async invoke(operation, argument) {
        if (operation === "dispose") {
          await close();
          return null;
        }
        if (operation === "signal") return observed;
        if (operation === "read" && apply(aborted, signal, [])) {
          await notification;
          throw apply(signalReason, signal, []);
        }
        const result = await source.invoke(operation, argument);
        if (operation === "read" && apply(aborted, signal, [])) {
          await notification;
          throw apply(signalReason, signal, []);
        }
        return result;
      },
    };
  }

  function receive(metadata, call, release, reason, maximum, options) {
    if (!RequestClass || !options?.watch || !options.cleanup)
      throw new TypeError(
        "RPC requests require web globals and owned signal cleanup",
      );
    const controller = new AbortClass();
    const watchAbort = new AbortClass();
    let closing, unregister, watching;
    const received = metadata.body
      ? readables.receive(call, async () => {}, reason, maximum)
      : { stream: null, async dispose() {} };
    const dispose = (cause = abortError("RPC request disposed")) =>
      (closing ??= (async () => {
        controller.abort(cause);
        // Revoke the source's signal waiter as well as its body. Both cleanup
        // paths are joined before releasing the one native request handle.
        const results = await allSettled([
          received.dispose(cause),
          call("dispose"),
        ]);
        watchAbort.abort();
        await allSettled([watching]);
        try {
          for (const result of results)
            if (result.status === "rejected") throw result.reason;
        } finally {
          await release();
          unregister?.();
        }
      })());
    try {
      const request = new RequestClass(metadata.url, {
        ...metadata,
        body: received.stream,
        duplex: "half",
        signal: controller.signal,
      });
      define(request, "destination", {
        value: metadata.destination,
      });
      unregister = options.cleanup(dispose);
      watching = options
        .watch(watchAbort.signal)
        .then(async (state) => {
          if (state.aborted === true) {
            const cause = state.exception
              ? new DomException(state.exception.message, state.exception.name)
              : state.reason;
            controller.abort(cause);
            await received.dispose(cause);
          } else if (state.aborted !== false)
            throw new TypeError("Invalid RPC request signal");
        })
        .catch(async (error) => {
          if (closing) return;
          controller.abort(error);
          await received.dispose(error).catch(() => {});
        });
      return { stream: request, dispose };
    } catch (error) {
      dispose(error).catch(() => {});
      throw error;
    }
  }
  return {
    isRequest: (value) => RequestClass && value instanceof RequestClass,
    send,
    receive,
  };
}
