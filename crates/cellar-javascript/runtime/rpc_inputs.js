// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// Private two-way input sessions over native returned-function leases. The
// caller never needs a routable object identity. Its source stays in the
// original invocation; the callee receives only bounded stream operations.
({ transform, errorResult, reply, readables, writables, messages }) => {
  const PromiseClass = Promise;
  const Abort = globalThis.AbortController;
  const MapClass = Map;
  const SetClass = Set;
  const ErrorClass = Error;
  const isArray = Array.isArray;
  const dispose = Symbol.dispose;
  const method = "__cellar_workers_rpc_inputs";

  function unwrap(frame, context) {
    const value = reply(frame, context);
    if (!frame.error) return value;
    const [thrown, properties] = value;
    if (thrown !== null && ["object", "function"].includes(typeof thrown))
      for (const key of Object.keys(properties))
        Object.defineProperty(thrown, key, {
          value: properties[key],
          enumerable: true,
          writable: true,
          configurable: true,
        });
    throw thrown;
  }

  function chunkBudget(maximum) {
    // A returned next-command and an outgoing submit both add control fields.
    // Completion values are themselves encoded clone frames, so submit adds
    // two base64 layers inside the routed request. Reserve both plus fields.
    const size = Math.min(
      64 * 1024,
      Math.floor(((maximum - 512) * 9) / 16) - 128,
    );
    if (!Number.isSafeInteger(size) || size <= 0)
      throw new RangeError("RPC input byte budget is too small");
    return size;
  }

  async function call(request, args, context) {
    const senders = [],
      markers = [],
      kinds = [];
    let copied, encoded;
    try {
      copied = transform(args, (value) => {
        let transfer, kind;
        if (messages?.isRequest(value) || messages?.isResponse(value)) {
          transfer = messages;
          kind = messages.isRequest(value) ? "request" : "response";
        } else if (readables.isReadable(value)) {
          transfer = readables;
          kind = "readable";
        } else if (writables.isWritable(value)) {
          transfer = writables;
          kind = "writable";
        } else return value;
        const sender = transfer.send(value, context);
        senders.push(sender);
        kinds.push(kind);
        const marker = sender.metadata ?? Object.create(null);
        markers.push(marker);
        return marker;
      });
      encoded = context._serialize(
        markers.length ? [copied, markers, kinds] : copied,
      );
    } catch (error) {
      for (const sender of senders) sender.restore();
      throw error;
    }
    const routed = (args, inputs = false) =>
      request.handle === undefined
        ? context._call("workers.rpc", { ...request, args })
        : context._call("workers.rpc.invoke", {
            id: request.handle,
            args,
            inputs,
          });
    if (!senders.length) return unwrap(await routed(encoded), context);

    const commandLimit =
      senders.length * 2 + kinds.filter((kind) => kind === "request").length;
    let reservation, controls, loop, closed, removeCleanup, transferFailure;
    const running = new SetClass();
    let abort;
    // Cleanup is separate from public/background jobs. An idle next() waiter
    // must not retain the caller forever, but native operations are still
    // joined before the invocation completes.
    const close = () =>
      (closed ??= (async () => {
        abort.abort();
        // Closing the callee first rejects queued operations and pending reads.
        const stopping = controls ? controls[3]() : PromiseClass.resolve();
        await PromiseClass.allSettled(senders.map((sender) => sender.close()));
        await PromiseClass.allSettled([stopping, loop]);
        // A next() reply can win its abort race and start one final answer.
        // Join the pump first, then every answer it actually admitted.
        while (running.size) await PromiseClass.allSettled([...running]);
        if (controls) for (const control of controls) control[dispose]();
        if (reservation)
          await context._call("workers.rpc.inputs_release", {
            id: reservation.id,
          });
        removeCleanup?.();
      })());
    try {
      abort = new Abort();
      removeCleanup = context._rpcCleanup(close);
      reservation = await context._call("workers.rpc.inputs", {
        count: senders.length,
        value: encoded,
      });
    } catch (error) {
      removeCleanup?.();
      for (const sender of senders) sender.restore();
      throw error;
    }
    try {
      const args = context._serialize([
        request.method ?? "__call__",
        encoded,
        senders.length,
        reservation.max_body_bytes,
      ]);
      const frame =
        request.handle === undefined
          ? await context._call("workers.rpc", { ...request, method, args })
          : await routed(args, true);
      if (frame.error) unwrap(frame, context);
      // Only these private controls use soft calls. Actual returned application
      // capabilities keep their ordinary original-event lifetime semantics.
      controls = reply(frame, context, true, abort.signal);
      if (
        !isArray(controls) ||
        controls.length !== 4 ||
        controls.some((control) => typeof control !== "function")
      )
        throw new TypeError("Invalid RPC input session");
      const next = controls[0];
      async function answer(command) {
        const [id, index, operation, argument] = command;
        let completion;
        try {
          const value = await senders[index].invoke(operation, argument);
          completion = { value: context._serialize(value), error: false };
        } catch (error) {
          completion = await context._call(
            "workers.rpc.error",
            errorResult(error, context),
          );
        }
        // Native invoke proves this original caller's cell writes before
        // forwarding data. Root Workers have no local SQLite state.
        await controls[1](id, completion);
      }
      loop = (async () => {
        try {
          for (;;) {
            const command = await next();
            if (command === null) return;
            if (
              !isArray(command) ||
              command.length !== 4 ||
              !Number.isSafeInteger(command[1]) ||
              !senders[command[1]] ||
              running.size >= commandLimit
            )
              throw new TypeError("Invalid RPC input command");
            const work = answer(command);
            running.add(work);
            work.then(
              () => running.delete(work),
              (error) => {
                if (!closed) transferFailure ??= error;
                running.delete(work);
                // A failed data acknowledgement is never replayed. Revoke the
                // callee session so its pending consumer observes failure.
                close().catch(() => {});
              },
            );
          }
        } catch (error) {
          if (!abort.signal.aborted) {
            transferFailure ??= error;
            // Do not wait for this loop from inside itself.
            PromiseClass.resolve()
              .then(close)
              .catch(() => {});
            throw error;
          }
        }
      })();
      loop.catch(() => {});
      return await controls[2]();
    } catch (error) {
      await close();
      if (transferFailure) {
        const failure = new ErrorClass(
          `RPC input transport failed: ${transferFailure.message}`,
          { cause: transferFailure },
        );
        failure.code = "Ambiguous";
        failure.outcome = "unknown";
        throw failure;
      }
      throw error;
    }
  }

  async function accept(input, context, invoke) {
    if (!isArray(input) || input.length !== 4)
      throw new TypeError("Invalid RPC input session request");
    const [target, encoded, count, callerBudget] = input;
    if (
      typeof target !== "string" ||
      target === method ||
      !Number.isSafeInteger(callerBudget) ||
      callerBudget <= 0
    )
      throw new TypeError("Invalid RPC input session metadata");
    const reservation = await context._call("workers.rpc.inputs", {
      count,
      value: encoded,
    });
    const maximum = Math.min(callerBudget, reservation.max_body_bytes);
    let commandLimit = count * 2;
    let stopped = false,
      closing,
      waiter,
      output,
      nextId = 0;
    const pending = new MapClass(),
      queue = [],
      received = [];
    const fail = () => new ErrorClass("RPC input session closed");
    const close = () =>
      (closing ??= (async () => {
        stopped = true;
        waiter?.(null);
        waiter = undefined;
        for (const { reject } of pending.values()) reject(fail());
        pending.clear();
        queue.length = 0;
        await PromiseClass.allSettled(received.map((value) => value.dispose()));
        await context._call("workers.rpc.inputs_release", {
          id: reservation.id,
        });
      })());
    function enqueue(index, operation, argument) {
      if (stopped) {
        // The session has revoked its queue and the original caller now owns
        // source cleanup. Do not enqueue new cleanup commands into that queue.
        if (["cancel", "abort", "dispose"].includes(operation))
          return PromiseClass.resolve(null);
        return PromiseClass.reject(fail());
      }
      if (pending.size >= commandLimit)
        return PromiseClass.reject(new ErrorClass("RPC input queue full"));
      if (++nextId > Number.MAX_SAFE_INTEGER)
        return PromiseClass.reject(fail());
      const id = nextId;
      const result = new PromiseClass((resolve, reject) =>
        pending.set(id, { resolve, reject }),
      );
      const command = [id, index, operation, argument];
      if (waiter) {
        const ready = waiter;
        waiter = undefined;
        ready(command);
      } else queue.push(command);
      return result;
    }
    try {
      const graph = context._deserialize(encoded);
      const replacements = new MapClass();
      commandLimit += graph[2].filter((kind) => kind === "request").length;
      const size = chunkBudget(maximum);
      for (let i = 0; i < count; i++) {
        const message = ["request", "response"].includes(graph[2][i]);
        const readable = message || graph[2][i] === "readable";
        const transfer = readable ? readables : writables;
        // Reuse normal stream ownership, backpressure and cancellation handling.
        // Force a smaller payload grant for the additional input envelope.
        const budget = readable
          ? size + 128
          : Math.floor(((size + 64) * 4) / 3) + 128;
        const receive = message
          ? (...args) => messages.receive(graph[1][i], ...args)
          : transfer.receive;
        const value = receive(
          (operation, argument) => enqueue(i, operation, argument),
          async () => {},
          (reason) => {
            const copy = context._serialize(reason);
            if (context._decode(copy).byteLength > size)
              throw new RangeError(
                "RPC input cancellation reason exceeds limit",
              );
            return context._deserialize(copy);
          },
          budget,
          {
            watch: () => enqueue(i, "signal"),
            cleanup: (close) => context._rpcCleanup(close),
          },
        );
        received.push(value);
        replacements.set(graph[1][i], value.stream);
      }
      const args = transform(
        graph[0],
        (value) => replacements.get(value) ?? value,
      );
      const controls = [
        () => {
          if (stopped) return null;
          if (queue.length) return queue.shift();
          if (waiter)
            throw new ErrorClass("RPC input session already has a waiter");
          return new PromiseClass((resolve) => {
            waiter = resolve;
          });
        },
        (id, completion) => {
          const slot = pending.get(id);
          if (!slot) throw new ErrorClass("RPC input command expired");
          pending.delete(id);
          try {
            slot.resolve(unwrap(completion, context));
          } catch (error) {
            slot.reject(error);
          }
        },
        () => {
          if (stopped) throw fail();
          return (output ??= PromiseClass.resolve().then(() =>
            invoke(target, args),
          ));
        },
        close,
      ];
      return await context._rpcResult(controls, close);
    } catch (error) {
      await close();
      throw error;
    }
  }
  return { call, accept };
}
