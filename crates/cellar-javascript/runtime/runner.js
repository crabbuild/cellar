// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// Shared SDK facade for embedded V8 and Node. This file is a factory expression:
// the Rust bootstrap evaluates or embeds it, then supplies the private host call.
// Keep the expression free of a trailing semicolon for embedding in parentheses.
(host, synchronousHost, currentHost, encodeClone, decodeClone) => {
  "use strict";

  // Capture these builtins before importing/evaluating application code.
  const parse = JSON.parse;
  const stringify = JSON.stringify;
  const freeze = Object.freeze;
  const entries = Object.entries;
  const isArray = Array.isArray;
  const MapClass = Map;
  const ErrorClass = Error;
  const apply = Reflect.apply;
  const typedArrayPrototype = Object.getPrototypeOf(Uint8Array.prototype);
  const typedArrayTag = Object.getOwnPropertyDescriptor(typedArrayPrototype, Symbol.toStringTag).get;
  const typedArrayBytes = Object.getOwnPropertyDescriptor(typedArrayPrototype, "byteLength").get;
  const typedArrayOffset = Object.getOwnPropertyDescriptor(typedArrayPrototype, "byteOffset").get;
  const typedArrayBuffer = Object.getOwnPropertyDescriptor(typedArrayPrototype, "buffer").get;
  const alphabet =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

  // Copy only the bridge's JSON value domain. Reject values that JSON.stringify
  // would silently omit, coerce, or round, rather than changing their meaning.
  function json(value, depth = 0) {
    if (depth > 64) {
      throw new ErrorClass("ValueDepth");
    }

    if (
      value === null ||
      typeof value === "string" ||
      typeof value === "boolean"
    ) {
      return value;
    }

    if (
      typeof value === "number" &&
      Number.isFinite(value) &&
      (!Number.isInteger(value) || Number.isSafeInteger(value))
    ) {
      return value;
    }

    if (isArray(value)) {
      return value.map(v => json(v, depth + 1));
    }

    if (
      typeof value === "object" &&
      value !== null &&
      (Object.getPrototypeOf(value) === Object.prototype ||
        Object.getPrototypeOf(value) === null)
    ) {
      const result = Object.create(null);

      for (const [key, v] of entries(value)) {
        result[key] = json(v, depth + 1);
      }

      return result;
    }

    throw new ErrorClass("ValueType: use finite JSON values and safe integers");
  }

  // Decode bridge bodies without Buffer or browser globals. The same facade
  // runs in embedded V8, where those host APIs are not available.
  function decode(text) {
    const bytes = [];
    let bits = 0;
    let count = 0;

    for (const c of text) {
      if (c === "=") {
        break;
      }

      const n = alphabet.indexOf(c);
      if (n < 0) {
        throw new ErrorClass("InvalidBase64");
      }

      bits = (bits << 6) | n;
      count += 6;

      if (count >= 8) {
        count -= 8;
        bytes.push((bits >> count) & 255);
      }
    }

    return new Uint8Array(bytes);
  }

  // Encode each bounded body or socket frame for the JSON bridge.
  function encode(bytes) {
    if (!(bytes instanceof Uint8Array)) {
      const error = new ErrorClass("ValidationError");
      error.code = "ValidationError";
      error.outcome = "not_committed";
      throw error;
    }
    let result = "";

    for (let i = 0; i < bytes.length; i += 3) {
      const n =
        (bytes[i] << 16) | ((bytes[i + 1] ?? 0) << 8) | (bytes[i + 2] ?? 0);
      result +=
        alphabet[n >>> 18] +
        alphabet[(n >>> 12) & 63] +
        (i + 1 < bytes.length ? alphabet[(n >>> 6) & 63] : "=") +
        (i + 2 < bytes.length ? alphabet[n & 63] : "=");
    }
    return result;
  }

  // UTF-8 encoding must also work in V8 without Node or browser text encoders.
  function bytes(value) {
    if (typeof value === "string") {
      const encoded = encodeURIComponent(value.toWellFormed());
      const output = [];

      for (let i = 0; i < encoded.length; i++) {
        if (encoded[i] === "%") {
          output.push(parseInt(encoded.slice(i + 1, i + 3), 16));
          i += 2;
        } else {
          output.push(encoded.charCodeAt(i));
        }
      }

      return new Uint8Array(output);
    }

    if (!(value instanceof Uint8Array)) {
      throw new TypeError("Expected text or Uint8Array");
    }

    return value;
  }

  // Both incoming requests and nested-cell responses use this decoded view.
  // text() deliberately rejects invalid UTF-8 instead of replacing bad bytes.
  function request(value) {
    const body = decode(value.body);
    const text = () =>
      decodeURIComponent(
        Array.from(body, b => "%" + b.toString(16).padStart(2, "0")).join(""),
      );

    return freeze({ ...value, body, text, json: () => json(parse(text())) });
  }

  // Optional Workers crypto resolves the current event rather than capturing
  // the cell that first installed the global. Only these pure-value operations
  // have this global lifetime; ordinary saved SDK contexts still expire.
  const crypto = freeze({
    getRandomValues(array) {
      const type = apply(typedArrayTag, array, []);
      if (!["Int8Array", "Uint8Array", "Uint8ClampedArray", "Int16Array", "Uint16Array", "Int32Array", "Uint32Array", "BigInt64Array", "BigUint64Array"].includes(type)) {
        const error = new ErrorClass("Expected an integer typed array"); error.name = "TypeMismatchError"; throw error;
      }
      const length = apply(typedArrayBytes, array, []);
      if (length > 65536) { const error = new ErrorClass("Random-values quota exceeded"); error.name = "QuotaExceededError"; throw error; }
      const invocation = context(currentHost(), null, null);
      const bytes = decode(invocation._sync("workers.crypto.random", { length }));
      new Uint8Array(apply(typedArrayBuffer, array, []), apply(typedArrayOffset, array, []), length).set(bytes);
      return array;
    },
    randomUUID: () => context(currentHost(), null, null)._sync("workers.crypto.uuid"),
    get subtle() { throw new ErrorClass("Unsupported Workers API: crypto.subtle"); },
  });

  // Each facade closes over one invocation ID. The backend independently checks
  // continuation identity and liveness whenever that facade calls into Rust.
  function context(id, identity, activation) {
    const invoke = (name, value, synchronous) => {
      let encoded, pending;

      try {
        encoded = stringify(json(value));
      } catch {
        const error = new ErrorClass("ValidationError");
        error.code = "ValidationError";
        error.outcome = "not_committed";
        throw error;
      }

      // Synchronous admission failures occur before the operation is submitted.
      try {
        pending = (synchronous ? synchronousHost : host)(id, name, encoded);
      } catch (error) {
        error.code = error.message;
        error.outcome ??= "not_committed";
        throw error;
      }

      return synchronous ? finish(name, pending) : pending.then(reply => finish(name, reply));
    };

    const finish = (name, reply) => {
      const result = parse(reply);

      if (!result.ok) {
        const error = new ErrorClass(result.code);
        error.code = result.code;
        error.outcome = result.outcome;
        throw error;
      }

      // A result that cannot be represented safely may follow a committed write.
      // Refusing that result must not imply that the native mutation rolled back.
      try {
        return json(result.value);
      } catch {
        const read = [
          "clock.now",
          "workers.crypto.random",
          "workers.crypto.uuid",
          "workers.identity",
          "workers.sql.size",
          "workers.kv.get",
          "workers.kv.list",
          "storage.read",
          "alarm.get",
          "var",
          "kv.get",
          "kv.list",
          "container.capabilities",
          "container.running",
          "container.monitor",
          "container.exec.read_stdout",
          "container.exec.read_stderr",
          "container.exec.wait",
          "container.socket.read",
          "container.websocket.receive",
          "websocket.next",
          "response.read",
        ].includes(name);
        const error = new ErrorClass(read ? "HostError" : "Ambiguous");
        error.code = error.message;
        error.outcome = "unknown";
        throw error;
      }
    };

    const call = async (name, value = {}) => invoke(name, value, false);
    const synchronous = (name, value = {}) => invoke(name, value, true);

    const resource = (kind, resourceId) => {
      const operation = (method, args = {}) =>
        call(`container.${kind}.${method}`, { ...args, id: resourceId });
      const common = {
        release: () => call("container.release", { id: resourceId }),
      };
      return freeze(kind === "exec" ? {
        ...common,
        readStdout: async () => decode(await operation("read_stdout")),
        readStderr: async () => decode(await operation("read_stderr")),
        write: bytes => operation("write", { data: encode(bytes) }),
        closeStdin: () => operation("close_stdin"),
        wait: () => operation("wait"),
        kill: (signal = 9) => operation("kill", { signal }),
      } : kind === "websocket" ? {
        ...common,
        receive: async () => {
          const event = await operation("receive");
          return freeze(event.type === "binary" ? { ...event, data: decode(event.data) } : event);
        },
        sendText: text => operation("send_text", { text }),
        sendBinary: bytes => operation("send_binary", { data: encode(bytes) }),
        close: (code = 1000, reason = "") => operation("close", { code, reason }),
      } : {
        ...common,
        read: async () => decode(await operation("read")),
        write: bytes => operation("write", { data: encode(bytes) }),
        closeWrite: () => operation("close_write"),
        close: () => operation("close"),
      });
    };

    const container = freeze({
      capabilities: async () => {
        const value = await call("container.capabilities");
        return freeze({ ...value, operations: freeze(value.operations) });
      },
      running: () => call("container.running"),
      start: (options = {}) => call("container.start", { options }),
      monitor: () => call("container.monitor"),
      destroy: () => call("container.destroy"),
      signal: signal => call("container.signal", { signal }),
      setInactivityTimeout: timeout_ms => call("container.set_inactivity_timeout", { timeout_ms }),
      exec: async options => resource("exec", await call("container.exec", { options })),
      execOutput: async (options, max_bytes = 1048576) => {
        const output = await call("container.exec_output", { options, max_bytes });
        return freeze({ ...output, stdout: decode(output.stdout), stderr: decode(output.stderr) });
      },
      port: port => freeze({
        waitReady: (timeout_ms = 10000) => call("container.port.wait_ready", { port, timeout_ms }),
        fetch: async value => request(await call("container.port.fetch", { port, request: value })),
        connect: async () => resource("socket", await call("container.port.connect", { port })),
        connectWebSocket: async (url, headers = []) => {
          const result = await call("container.port.connect_websocket", { port, url, headers });
          return freeze(result.upgraded
            ? { upgraded: true, protocol: result.protocol, socket: resource("websocket", result.id) }
            : { upgraded: false, response: request(result.response) });
        },
      }),
    });

    // These objects contain bridge calls, never native storage handles. Binding
    // lookup and effect execution remain in the original Rust invocation.
    // Socket facades carry opaque IDs, never native handles. Each operation
    // resolves its authority through the current Rust invocation.
    const socket = id =>
      freeze({
        id,
        accept: (options = {}) =>
          call("websocket.accept", {
            id,
            hibernating: false,
            tags: [],
            ...options,
          }),
        send: data =>
          call(
            "websocket.send",
            typeof data === "string"
              ? { id, text: data }
              : { id, body: encode(bytes(data)) },
          ),
        close: (code = 1000, reason = "") =>
          call("websocket.close", { id, code, reason }),
        next: async () => {
          const event = await call("websocket.next", { id });

          return freeze(
            event.type !== "message"
              ? event
              : {
                  ...event,
                  data:
                    event.body === undefined ? event.text : decode(event.body),
                },
          );
        },
        setAttachment: value => call("websocket.attachment.set", { id, value }),
        getAttachment: () => call("websocket.attachment.get", { id }),
        tags: () => call("websocket.tags", { id }),
        autoResponseTimestamp: () =>
          call("websocket.auto_response_timestamp", { id }),
        info: () => call("websocket.info", { id }),
      });

    // Keep the native response capability intact so callers can either forward
    // it directly or pull chunks without buffering the complete response.
    const liveResponse = result => {
      const id = result.forward;

      return freeze({
        ...result,
        body: decode(result.body),
        async *chunks() {
          try {
            // A buffered prefix precedes any chunks from the native stream.
            if (result.body) {
              yield decode(result.body);
            }

            while (result.stream) {
              const chunk = await call("response.read", { id });

              if (chunk.done) {
                break;
              }

              yield decode(chunk.body);
            }
          } finally {
            // Release the native consumer even if iteration stops early.
            await call("response.cancel", { id });
          }
        },
        cancel: () => call("response.cancel", { id }),
      });
    };

    return freeze({
      fetchStream: async value =>
        liveResponse(await call("http.fetch_stream", { request: value })),
      acceptWebSocket: async response =>
        socket(
          (
            await call("websocket.from_response", {
              response: response.forward,
            })
          ).id,
        ),
      // Private hooks used by dispatch while retaining this invocation.
      _crypto: crypto,
      _call: call,
      // Optional Workers state has activation lifetime. This getter deliberately
      // grants no authority to a foreign cell, retired activation, cancelled
      // event, or ambient code outside an admitted native continuation.
      _activation: () => {
        let cachedId, cached;
        const current = () => {
          let invocation;
          try { invocation = currentHost(activation); }
          catch (error) {
            error.code = error.message;
            error.outcome = "not_committed";
            throw error;
          }
          if (invocation !== cachedId) {
            cachedId = invocation;
            cached = context(invocation, identity, activation);
          }
          return cached;
        };
        current();
        return current;
      },
      _sync: synchronous,
      _serialize: value => encode(encodeClone(value)),
      _deserialize: value => decodeClone(decode(value)),
      _decode: decode,
      _encode: data => encode(bytes(data)),

      websocket: socket,
      websocketPair: async () => socket((await call("websocket.pair")).id),
      connectWebSocket: async (url, protocols = []) =>
        socket((await call("websocket.connect", { url, protocols })).id),
      websockets: async (tag = null) =>
        (await call("websocket.list", { tag })).map(socket),
      getWebSocketAutoResponse: () => call("websocket.auto_response.get"),
      setWebSocketAutoResponse: pair =>
        call("websocket.auto_response", { pair }),

      identity: identity && freeze(identity),
      now: () => call("clock.now"),
      variable: name => call("var", { name }),
      container,

      storage: freeze({
        read: key => call("storage.read", { key }),
        apply: batch => call("storage.apply", batch),
        sync: () => call("storage.sync"),
        sql: (sql, params = []) => call("storage.sql", { sql, params }),
        getAlarm: () => call("alarm.get"),
      }),

      cell: (binding, name) =>
        freeze({
          fetch: async value =>
            request(
              await call("cell.fetch", { binding, name, request: value }),
            ),
          fetchStream: async value =>
            liveResponse(
              await call("cell.fetch_stream", {
                binding,
                name,
                request: value,
              }),
            ),
        }),

      kv: binding =>
        freeze({
          get: key => call("kv.get", { binding, key }),
          put: (key, value, options = {}) =>
            call("kv.put", { binding, key, value, ...options }),
          delete: key => call("kv.delete", { binding, key }),
          list: (options = {}) =>
            call("kv.list", { binding, prefix: "", ...options }),
        }),

      sql: binding =>
        freeze({
          query: (sql, params = []) =>
            call("sql.query", { binding, sql, params }),
        }),

      queue: binding =>
        freeze({
          send: (body, options = {}) =>
            call("queue.send", { binding, body, ...options }),
        }),

      settle: options => call("queue.settle", options),
      noRetry: () => call("cron.no_retry"),
    });
  }

  // Validate only the explicitly declared handlers before signaling readiness.
  // Objects are cached by activation ID; retirement discards activation-local
  // JavaScript state without changing the cell's durable Rust-owned storage.
  return (app, descriptor, maximum) => {
    const declared = parse(descriptor);
    const objects = new MapClass();

    for (const name of declared.events) {
      if (typeof app[name] !== "function") {
        throw new ErrorClass("Missing application handler");
      }
    }

    for (const [name, events] of entries(declared.cells)) {
      if (typeof app.cells?.[name] !== "function") {
        throw new ErrorClass("Missing cell class");
      }

      for (const event of events) {
        if (typeof app.cells[name].prototype[event] !== "function") {
          throw new ErrorClass("Missing cell handler");
        }
      }
    }

    return freeze({
      async dispatch(id, payload) {
        const invocation = parse(payload);
        const ctx = context(id, invocation.identity, invocation.object);
        let target = app;

        if (invocation.object !== null) {
          target = objects.get(invocation.object);

          if (!target) {
            if (objects.size >= maximum) {
              throw new ErrorClass("ObjectLimit");
            }

            target = new app.cells[invocation.class]();
            objects.set(invocation.object, target);
          }
        }

        let event =
          invocation.kind === "fetch"
            ? request(invocation.event)
            : json(invocation.event);

        if (invocation.kind === "websocket_message") {
          event = freeze({
            ...event,
            data: event.body === undefined ? event.text : decode(event.body),
          });
        }

        let result = await target[invocation.kind](event, ctx);

        // Forward the native capability without consuming its body or upgrade.
        if (invocation.kind === "fetch" && result?.forward !== undefined) {
          return stringify({ forward: result.forward });
        }

        if (invocation.kind === "fetch" && result?.chunks !== undefined) {
          // Publish headers before pulling the producer. Each awaited write
          // preserves native backpressure and keeps invocation authority alive.
          await ctx._call("response.start", {
            status: result.status,
            headers: result.headers,
            body: "",
          });

          for await (const chunk of result.chunks) {
            await ctx._call("response.write", { body: ctx._encode(chunk) });
          }

          result = { streamed: true };
        } else if (
          invocation.kind === "fetch" &&
          result?.websocket !== undefined
        ) {
          const response = {
            websocket: result.websocket,
            headers: result.headers ?? [],
          };

          if (result.run) {
            // Workers consume events in this retained invocation. Hibernating
            // cells omit run and receive separate native WebSocket callbacks.
            await ctx._call("response.upgrade", response);
            await result.run(ctx.websocket(result.websocket), ctx);
            result = { streamed: true };
          } else {
            result = response;
          }
        }

        // Non-HTTP handlers complete without an application return value.
        return stringify(json(invocation.kind === "fetch" ? result : null));
      },

      retire(id) {
        objects.delete(id);
      },
    });
  };
}
