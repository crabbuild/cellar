// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// Shared SDK facade for embedded V8 and Node. This file is a factory expression:
// the Rust bootstrap evaluates or embeds it, then supplies the private host call.
(host) => {
  "use strict";

  // Capture these builtins before importing/evaluating application code.
  const parse = JSON.parse,
    stringify = JSON.stringify,
    freeze = Object.freeze;
  const entries = Object.entries,
    isArray = Array.isArray;
  const MapClass = Map,
    ErrorClass = Error;
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
    let bits = 0,
      count = 0;

    for (const c of text) {
      if (c === "=") break;

      const n = alphabet.indexOf(c);
      if (n < 0) throw new ErrorClass("InvalidBase64");

      bits = (bits << 6) | n;
      count += 6;

      if (count >= 8) {
        count -= 8;
        bytes.push((bits >> count) & 255);
      }
    }

    return new Uint8Array(bytes);
  }

  function encode(bytes) {
    if (!(bytes instanceof Uint8Array)) {
      const error = new ErrorClass("ValidationError");
      error.code = "ValidationError";
      error.outcome = "not_committed";
      throw error;
    }
    let result = "";
    for (let i = 0; i < bytes.length; i += 3) {
      const n = (bytes[i] << 16) | ((bytes[i + 1] ?? 0) << 8) | (bytes[i + 2] ?? 0);
      result += alphabet[n >>> 18] + alphabet[(n >>> 12) & 63] +
        (i + 1 < bytes.length ? alphabet[(n >>> 6) & 63] : "=") +
        (i + 2 < bytes.length ? alphabet[n & 63] : "=");
    }
    return result;
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

  // Each facade closes over one invocation ID. The backend independently checks
  // continuation identity and liveness whenever that facade calls into Rust.
  function context(id, identity) {
    const call = async (name, value = {}) => {
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
        pending = host(id, name, encoded);
      } catch (error) {
        error.code = error.message;
        error.outcome = "not_committed";
        throw error;
      }

      const result = parse(await pending);

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
          "storage.read",
          "alarm.get",
          "var",
          "kv.get",
          "kv.list",
          "container.running", "container.monitor", "container.exec.read_stdout",
          "container.exec.read_stderr", "container.exec.wait", "container.socket.read", "container.websocket.receive",
        ].includes(name);
        const error = new ErrorClass(read ? "HostError" : "Ambiguous");
        error.code = error.message;
        error.outcome = "unknown";
        throw error;
      }
    };

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
    return freeze({
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
            request(await call("cell.fetch", { binding, name, request: value })),
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
    const declared = parse(descriptor),
      objects = new MapClass();

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
        const ctx = context(id, invocation.identity);
        let target = app;

        if (invocation.object !== null) {
          target = objects.get(invocation.object);

          if (!target) {
            if (objects.size >= maximum) throw new ErrorClass("ObjectLimit");

            target = new app.cells[invocation.class]();
            objects.set(invocation.object, target);
          }
        }

        const event =
          invocation.kind === "fetch"
            ? request(invocation.event)
            : json(invocation.event);
        const result = await target[invocation.kind](event, ctx);

        // Non-HTTP handlers complete without an application return value.
        return stringify(json(invocation.kind === "fetch" ? result : null));
      },

      retire(id) {
        objects.delete(id);
      },
    });
  };
}
