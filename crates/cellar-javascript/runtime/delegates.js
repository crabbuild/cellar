// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// Factory captured before application initialization. Native session handles,
// rather than serializable marker data, authorize every delegated operation.
(readables, writables, makeInputs, makeMessages) => {
  const messages = makeMessages?.(readables);
  const apply = Reflect.apply;
  const keys = Object.keys;
  const prototype = Object.getPrototypeOf;
  const define = Object.defineProperty;
  const ArrayClass = Array;
  const MapClass = Map;
  const SetClass = Set;
  const WeakMapClass = WeakMap;
  const ErrorClass = Error;
  const DateClass = Date;
  const RegExpClass = RegExp;
  const BufferClass = ArrayBuffer;
  const PromiseClass = Promise;
  const view = ArrayBuffer.isView;
  const dispose = Symbol.dispose;

  function errorResult(error, context) {
    const properties = Object.create(null);
    if (error instanceof ErrorClass) {
      properties.name = error.name;
      for (const key of keys(error)) {
        if (key !== "stack") properties[key] = error[key];
      }
    }
    return { value: context._serialize([error, properties]), error: true };
  }

  function transform(input, replace) {
    const seen = new WeakMapClass();
    let values = 0,
      objects = 0;
    function copy(value, depth = 0) {
      if (++values > 200_000 || depth > 64)
        throw new ErrorClass("RPC graph quota exceeded");
      if (value === null || !["object", "function"].includes(typeof value))
        return value;
      if (seen.has(value)) return seen.get(value);
      if (++objects > 100_000) throw new ErrorClass("RPC graph quota exceeded");
      const replacement = replace(value);
      if (replacement !== value) {
        seen.set(value, replacement);
        return replacement;
      }
      let output;
      if (ArrayClass.isArray(value)) output = new ArrayClass(value.length);
      else if (value instanceof MapClass) output = new MapClass();
      else if (value instanceof SetClass) output = new SetClass();
      else if (
        prototype(value) === Object.prototype ||
        prototype(value) === null
      )
        output = Object.create(prototype(value));
      else if (
        value instanceof DateClass ||
        value instanceof RegExpClass ||
        value instanceof BufferClass ||
        view(value) ||
        value instanceof ErrorClass
      )
        return value;
      else
        throw new TypeError("Custom RPC classes require RpcTarget transport");
      seen.set(value, output);
      if (value instanceof MapClass) {
        for (const [key, child] of value)
          output.set(copy(key, depth + 1), copy(child, depth + 1));
      } else if (value instanceof SetClass) {
        for (const child of value) output.add(copy(child, depth + 1));
      } else {
        for (const key of keys(value))
          define(output, key, {
            value: copy(value[key], depth + 1),
            enumerable: true,
            configurable: true,
            writable: true,
          });
      }
      return output;
    }
    return copy(input);
  }

  async function result(value, context, closed) {
    const functions = [],
      markers = [],
      kinds = [],
      senders = [];
    let encoded;
    const restore = () => {
      for (const sender of senders) sender.restore();
    };
    try {
      const copied = transform(value, (value) => {
        let invoke, kind, metadata;
        if (messages?.isResponse(value) || messages?.isRequest(value)) {
          const sender = messages.send(value, context);
          senders.push(sender);
          invoke = sender.invoke;
          metadata = sender.metadata;
          kind = messages.isRequest(value) ? "request" : "response";
        } else if (readables.isReadable(value)) {
          const sender = readables.sendReturned(value, context);
          senders.push(sender);
          invoke = sender.invoke;
          kind = "readable";
        } else if (writables.isWritable(value)) {
          const sender = writables.send(value, context);
          senders.push(sender);
          invoke = sender.invoke;
          kind = "writable";
        } else if (typeof value === "function") {
          invoke = value;
          kind = "function";
        } else return value;
        functions.push(invoke);
        kinds.push(kind);
        const marker = metadata ?? Object.create(null);
        markers.push(marker);
        return marker;
      });
      if (!functions.length)
        return { value: context._serialize(copied), error: false };
      // Kind metadata is private to this live native publication. Persisting
      // its clone graph never turns ordinary data back into a capability.
      encoded = context._serialize([copied, markers, kinds]);
    } catch (error) {
      restore();
      throw error;
    }
    let group, start;
    const ready = new PromiseClass((resolve) => {
      start = resolve;
    });
    async function answer(command) {
      let completion;
      try {
        let value,
          thrown = false,
          error;
        try {
          const args = context._deserialize(command.args);
          if (command.inputs) {
            completion = await inputs.accept(args, context, (_target, args) =>
              apply(functions[command.target], undefined, args),
            );
          } else
            value = await apply(functions[command.target], undefined, args);
        } catch (failure) {
          thrown = true;
          error = failure;
        }
        completion ??= thrown
          ? errorResult(error, context)
          : await result(value, context);
      } catch {
        // Result encoding can fail after effects. Native reply validation
        // reports a lost/unknown result, rather than a business exception.
        completion = null;
      }
      await context._call("workers.rpc.reply", {
        group,
        id: command.id,
        result: completion,
      });
    }
    // Reserve implicit background capacity before minting a native export or
    // starting a poll. Failed admission must not strand an unpublished lease.
    try {
      context._rpcRetain(async () => {
        if (!(await ready)) return;
        try {
          for (;;) {
            const command = await context._call("workers.rpc.poll", { group });
            if (command.closed) return;
            try {
              context._rpcRetain(() => answer(command));
            } catch {
              await context._call("workers.rpc.reply", {
                group,
                id: command.id,
                result: null,
              });
            }
          }
        } finally {
          // Native lease loss wakes the poll even with a read pending. Cancel
          // and join every transferred source before its producer can drain.
          await PromiseClass.allSettled([
            ...senders.map((sender) => sender.close()),
            ...(closed ? [PromiseClass.resolve().then(closed)] : []),
          ]);
        }
      });
    } catch (error) {
      restore();
      throw error;
    }
    try {
      ({ group } = await context._call("workers.rpc.export", {
        count: functions.length,
      }));
      start(true);
    } catch (error) {
      start(false);
      restore();
      throw error;
    }
    return {
      value: encoded,
      error: false,
      exports: { group, count: functions.length },
    };
  }

  function reply(frame, context, soft = false, signal) {
    const value = context._deserialize(frame.value);
    const handles = frame.handles;
    if (!handles) {
      if (!frame.error && value !== null && typeof value === "object")
        define(value, dispose, { value: () => {} });
      return value;
    }
    if (
      frame.error ||
      !ArrayClass.isArray(value) ||
      value.length !== 3 ||
      !ArrayClass.isArray(value[1]) ||
      value[1].length !== handles.length ||
      !ArrayClass.isArray(value[2]) ||
      value[2].length !== handles.length ||
      value[2].some(
        (kind) =>
          !["function", "readable", "writable", "response", "request"].includes(
            kind,
          ),
      )
    )
      throw new ErrorClass("Invalid callable RPC result");
    const replacements = new MapClass();
    const imported = [];
    function stub(id, callSignal) {
      let released = false;
      const check = () => {
        if (released) throw new ErrorClass("RPC stub disposed");
      };
      const invoke = async (args, untracked, signal) => {
        check();
        if (!untracked && inputs)
          return inputs.call({ handle: id }, args, context);
        const input = { id, args: context._serialize(args) };
        const frame = untracked
          ? await context._rpcSoftCall("workers.rpc.invoke", input, signal)
          : await context._call("workers.rpc.invoke", input);
        const value = reply(frame, context);
        if (frame.error) {
          const [thrown, properties] = value;
          if (
            thrown !== null &&
            (typeof thrown === "object" || typeof thrown === "function")
          )
            for (const key of keys(properties))
              define(thrown, key, {
                value: properties[key],
                enumerable: true,
                writable: true,
                configurable: true,
              });
          throw thrown;
        }
        return value;
      };
      const callable = (...args) => invoke(args, soft, callSignal);
      const watch = (signal) => invoke(["signal"], true, signal);
      const release = async () => {
        if (released) return;
        released = true;
        await context._call("workers.rpc.release", { id });
      };
      define(callable, dispose, {
        value: () => {
          release().catch(() => {});
        },
      });
      define(callable, "dup", {
        value: () => {
          check();
          return stub(context._sync("workers.rpc.dup", { id }).id).callable;
        },
      });
      return { callable, release, watch };
    }
    let copied;
    try {
      for (let index = 0; index < handles.length; index++) {
        const { callable, release, watch } = stub(
          handles[index],
          index === 0 ? signal : undefined,
        );
        let replacement = callable;
        if (value[2][index] !== "function") {
          const message = ["response", "request"].includes(value[2][index]);
          const readable = message || value[2][index] === "readable";
          const transfer = readable ? readables : writables;
          const receive = message
            ? (...args) => messages.receive(value[1][index], ...args)
            : transfer.receive;
          const received = receive(
            callable,
            release,
            (reason) => {
              const encoded = context._serialize([
                readable ? "cancel" : "abort",
                reason,
              ]);
              // The routed request wraps base64 arguments and group identity.
              // Account for that envelope against both hosts' negotiated budget.
              const maximum = Math.floor(
                ((frame.max_body_bytes - 128) * 3) / 4,
              );
              if (context._decode(encoded).byteLength > maximum)
                throw new RangeError(
                  "RPC stream cancellation reason exceeds limit",
                );
              // Use the validated copy so accessors cannot change the arguments
              // between local validation and the native cancellation operation.
              return context._deserialize(encoded)[1];
            },
            frame.max_body_bytes,
            { watch, cleanup: (close) => context._rpcCleanup(close) },
          );
          const close = () => {
            received.dispose().catch(() => {});
          };
          replacement = received.stream;
          define(replacement, dispose, { value: close, configurable: true });
          imported.push(close);
        } else imported.push(() => callable[dispose]());
        replacements.set(value[1][index], replacement);
      }
      copied = transform(value[0], (value) => replacements.get(value) ?? value);
      if (copied !== null && typeof copied === "object")
        define(copied, dispose, {
          value: () => {
            for (const close of imported) close();
          },
        });
    } catch (error) {
      for (const close of imported) close();
      for (const id of handles)
        context._call("workers.rpc.release", { id }).catch(() => {});
      throw error;
    }
    return copied;
  }
  const inputs = makeInputs?.({
    transform,
    errorResult,
    reply,
    readables,
    writables,
    messages,
  });
  return { result, reply, ...inputs };
}
