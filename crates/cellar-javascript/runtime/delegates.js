// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// Factory captured before application initialization. Native session handles,
// rather than serializable marker data, authorize every delegated operation.
() => {
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

  async function result(value, context) {
    const functions = [],
      markers = [];
    const copied = transform(value, (value) => {
      if (typeof value !== "function") return value;
      functions.push(value);
      const marker = Object.create(null);
      markers.push(marker);
      return marker;
    });
    if (!functions.length)
      return { value: context._serialize(copied), error: false };
    // Serialize before reserving authority. The marker array shares identity
    // with placeholders in the value graph; business data cannot collide with
    // these fresh objects or cause a persisted clone to become a capability.
    const encoded = context._serialize([copied, markers]);
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
          value = await apply(
            functions[command.target],
            undefined,
            context._deserialize(command.args),
          );
        } catch (failure) {
          thrown = true;
          error = failure;
        }
        completion = thrown
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
    context._rpcRetain(async () => {
      if (!(await ready)) return;
      for (;;) {
        const command = await context._call("workers.rpc.poll", { group });
        if (command.closed) return;
        try {
          context._rpcRetain(() => answer(command));
        } catch {
          // Admission failed before calling the function. The already accepted
          // routed call still needs a bounded, honest failure response.
          await context._call("workers.rpc.reply", {
            group,
            id: command.id,
            result: null,
          });
        }
      }
    });
    try {
      ({ group } = await context._call("workers.rpc.export", {
        count: functions.length,
      }));
      start(true);
    } catch (error) {
      start(false);
      throw error;
    }
    return {
      value: encoded,
      error: false,
      exports: { group, count: functions.length },
    };
  }

  function reply(frame, context) {
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
      value.length !== 2 ||
      !ArrayClass.isArray(value[1]) ||
      value[1].length !== handles.length
    )
      throw new ErrorClass("Invalid callable RPC result");
    const replacements = new MapClass();
    const imported = [];
    function stub(id) {
      let released = false;
      const check = () => {
        if (released) throw new ErrorClass("RPC stub disposed");
      };
      const callable = async (...args) => {
        check();
        const frame = await context._call("workers.rpc.invoke", {
          id,
          args: context._serialize(args),
        });
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
      define(callable, dispose, {
        value: () => {
          if (released) return;
          released = true;
          // Automatic native event cleanup may already have disposed it. A
          // synchronous disposer must not create an unhandled stale-context
          // rejection; forced cleanup still drops every native import.
          context._call("workers.rpc.release", { id }).catch(() => {});
        },
      });
      define(callable, "dup", {
        value: () => {
          check();
          return stub(context._sync("workers.rpc.dup", { id }).id);
        },
      });
      return callable;
    }
    for (let index = 0; index < handles.length; index++) {
      const callable = stub(handles[index]);
      imported.push(callable);
      replacements.set(value[1][index], callable);
    }
    const copied = transform(
      value[0],
      (value) => replacements.get(value) ?? value,
    );
    if (copied !== null && typeof copied === "object")
      define(copied, dispose, {
        value: () => {
          for (const callable of imported) callable[dispose]();
        },
      });
    return copied;
  }
  return { result, reply };
}
