// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// Shared SDK facade for embedded V8 and Node. This file is a factory expression:
// the Rust bootstrap evaluates or embeds it, then supplies the private host call.
// Keep the expression free of a trailing semicolon for embedding in parentheses.
(host, addAbortListener) => {
  "use strict";

  // Capture these builtins before importing/evaluating application code.
  const parse = JSON.parse,
    stringify = JSON.stringify,
    freeze = Object.freeze;
  const entries = Object.entries,
    isArray = Array.isArray;
  const MapClass = Map,
    ErrorClass = Error;
  const budgetKeys = new WeakMap();
  const rpcKey = Symbol.for("cellar.rpc");
  const apply = Reflect.apply,
    own = Object.getOwnPropertyDescriptor;
  const rpcFailures = freeze({
    InvalidArgument:'{"rpc_error":"InvalidArgument"}', MethodNotFound:'{"rpc_error":"MethodNotFound"}',
    ContractMismatch:'{"rpc_error":"ContractMismatch"}', ResourceExhausted:'{"rpc_error":"ResourceExhausted"}',
    ContextExpired:'{"rpc_error":"ContextExpired"}', Cancelled:'{"rpc_error":"Cancelled"}',
    DeadlineExceeded:'{"rpc_error":"DeadlineExceeded"}', Unavailable:'{"rpc_error":"Unavailable"}',
    OwnershipLost:'{"rpc_error":"OwnershipLost"}', DurabilityUnproven:'{"rpc_error":"DurabilityUnproven"}',
    InvalidResponse:'{"rpc_error":"InvalidResponse"}', Internal:'{"rpc_error":"Internal"}',
  });
  function handlerFailure(error) {
    try {
      // Read a data property without invoking code/outcome/message getters.
      const descriptor = own(error, "code");
      const value = descriptor && own(descriptor, "value");
      const category = value && typeof value.value === "string" && value.value.length <= 32 && own(rpcFailures, value.value);
      if (category) return category.value;
    } catch { /* A proxy trap cannot manufacture outcome evidence. */ }
    return rpcFailures.Internal;
  }
  // Node supplies events.addAbortListener, which resists stopImmediatePropagation.
  // Closed V8 bundles may supply a standard AbortController polyfill.
  const dispose = Symbol.dispose;
  const subscribe = addAbortListener ? ((signal, listener) => {
    let subscription;
    const onAbort = () => {
      listener();
      // A synthetic abort event must not consume Node's one-shot protected
      // subscription. Native Node snapshots listeners for each dispatch.
      if (!signal.aborted) subscription = addAbortListener(signal, onAbort);
    };
    subscription = addAbortListener(signal, onAbort);
    return () => subscription[dispose]();
  }) : ((signal, listener) => {
    const add = signal.addEventListener, remove = signal.removeEventListener;
    apply(add, signal, ["abort", listener]);
    return () => apply(remove, signal, ["abort", listener]);
  });

  function rpcFailure(code, outcome) {
    const error = new ErrorClass(code);
    error.code = code;
    error.outcome = outcome;
    return error;
  }
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

  // Each facade closes over one invocation ID. The backend independently checks
  // continuation identity and liveness whenever that facade calls into Rust.
  function context(id, identity, contracts) {
    const call = async (name, value = {}, body, signal, budget) => {
      let encoded, pending;

      try {
        encoded = stringify(json(value));
      } catch {
        const error = new ErrorClass("ValidationError");
        error.code = "ValidationError";
        error.outcome = "not_committed";
        throw error;
      }

      let unsubscribe, cancel;
      let submitted = false, cancelled = false, invalidSignal = false;
      // Subscribe before admission. The native cancellation capability belongs
      // only to this operation; its signal may fire in another JS continuation.
      try {
        if (signal !== undefined) {
          const onAbort = () => {
            let aborted = true;
            try {
              aborted = signal.aborted;
              if (typeof aborted !== "boolean") throw new TypeError();
            } catch { invalidSignal = true; }
            if (aborted || invalidSignal) {
              cancelled = true;
              cancel?.();
            }
          };
          try {
            unsubscribe = subscribe(signal, onAbort);
            onAbort();
          } catch {
            throw rpcFailure("InvalidArgument", "not_committed");
          }
          if (invalidSignal) throw rpcFailure("InvalidArgument", "not_committed");
          if (cancelled) throw rpcFailure("Cancelled", "not_committed");
        }

        // Synchronous admission failures precede submission. RPC admission
        // returns a private [promise, cancel] pair; other operations keep their
        // existing promise shape. No cancellation authority crosses JSON.
        try {
          const key = budget === undefined ? undefined : budgetKeys.get(budget);
          if (budget !== undefined && key === undefined) throw new ErrorClass("ContextMismatch");
          const admitted = host(id, name, encoded, body, key);
          if (name === "cell.call") [pending, cancel] = admitted;
          else pending = admitted;
          submitted = true;
        } catch (error) {
          error.code = error.message;
          error.outcome = "not_committed";
          throw error;
        }
        if (cancelled) cancel();

        // Always await the original native acknowledgement. A local Promise
        // race would release the waiter before mutation/byte admission settles.
        const [metadata, replyBody] = await pending;
        if (invalidSignal) throw rpcFailure("InvalidArgument", "unknown");
        const result = parse(metadata);

        if (!result.ok) {
          throw rpcFailure(result.code, result.outcome);
        }

        if (name === "cell.call") {
          if (replyBody instanceof Uint8Array) return replyBody;
          throw rpcFailure("InvalidResponse", "unknown");
        }

        // A result that cannot be represented safely may follow a committed write.
        // Refusing it must not imply that the native mutation rolled back.
        try {
          return json(result.value);
        } catch {
          const read = [
            "clock.now", "storage.read", "alarm.get", "var", "kv.get", "kv.list",
            "websocket.next", "response.read",
          ].includes(name);
          throw rpcFailure(read ? "HostError" : "Ambiguous", "unknown");
        }
      } finally {
        try { unsubscribe?.(); }
        catch { throw rpcFailure("InvalidArgument", submitted ? "unknown" : "not_committed"); }
      }
    };

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
      [rpcKey]: freeze({
        check: () => host(id, "$check", "{}"),
        budget: () => {
          const wrap = control => {
            const budget = freeze({__proto__:null, remaining:control[1], limit:control[2], release:control[3]});
            budgetKeys.set(budget, control[0]);
            return budget;
          };
          const control = host(id, "$rpc_budget", "{}");
          return typeof control?.then === "function" ? control.then(wrap) : wrap(control);
        },
        verify: method => {
          try {
            const contract = contracts[method.digest];
            method.verify(contract);
            return freeze({maxMessageBytes:contract.limits.max_message_bytes,
              maxDepth:contract.limits.max_depth, maxElements:contract.limits.max_elements,
              maxDecodedSlots:contract.limits.max_decoded_slots});
          } catch {
            const error = new ErrorClass("ContractMismatch");
            error.code = "ContractMismatch";
            error.outcome = "not_committed";
            throw error;
          }
        },
        call: (metadata, body, signal, budget) => call("cell.call", metadata, body, signal, budget),
      }),
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
      _call: call,
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
    const declared = parse(descriptor),
      objects = new MapClass(),
      rpc = new MapClass();
    const contracts = declared.rpc_contracts ?? {};
    // Keep trusted native schema data immutable when passing it to generated
    // codec verification. The projection is flat; type recursion uses names.
    function lock(value) {
      if (value && typeof value === "object") {
        for (const child of Object.values(value)) lock(child);
        freeze(value);
      }
    }
    lock(contracts);

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

    const services = declared.rpc ?? {};
    if (Object.keys(app.rpc ?? {}).length !== Object.keys(services).length) {
      throw new ErrorClass("RPC classes differ from native registration");
    }
    for (const [name, methods] of entries(services)) {
      const registration = app.rpc?.[name]?.methods;
      if (!registration || Object.keys(registration).length !== Object.keys(methods).length) {
        throw new ErrorClass("RPC methods differ from native registration");
      }
      const table = new MapClass();
      for (const [wire, native] of entries(methods)) {
        const rule = registration[wire];
        const handler = rule && own(app.cells[name].prototype, rule.handler)?.value;
        if (!rule || rule.digest !== native.digest || rule.input !== native.input ||
            rule.output !== native.output || typeof handler !== "function" ||
            typeof rule.decode !== "function" || typeof rule.encode !== "function") {
          throw new ErrorClass("RPC contract differs from native registration");
        }
        if (typeof rule.verify !== "function") throw new ErrorClass("Missing RPC codec verification");
        rule.verify(contracts[native.digest]);
        const limits = freeze({
          maxMessageBytes: native.limits.max_message_bytes,
          maxDepth: native.limits.max_depth,
          maxElements: native.limits.max_elements,
          maxDecodedSlots: native.limits.max_decoded_slots,
        });
        table.set(wire, freeze({handler, decode:rule.decode, encode:rule.encode, limits}));
      }
      rpc.set(name, table);
    }

    return freeze({
      failurePayload(error) {
        if (typeof error === "string") {
          for (const [, payload] of entries(rpcFailures)) if (payload === error) return payload;
        }
        return "null";
      },
      async dispatch(id, payload, body) {
        const invocation = parse(payload);
        const ctx = context(id, invocation.identity, contracts);
        let rule, argument;
        if (invocation.kind === "rpc") {
          rule = rpc.get(invocation.class)?.get(invocation.event.method);
          if (!rule || !(body instanceof Uint8Array)) throw new ErrorClass("InvalidRpcInvocation");
          argument = rule.decode(body, rule.limits);
        } else if (body !== undefined) {
          throw new ErrorClass("UnexpectedAttachment");
        }
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

        if (rule) {
          let result;
          try { result = await apply(rule.handler, target, [argument, ctx]); }
          catch (error) { throw handlerFailure(error); }
          try {
            const encoded = rule.encode(result, rule.limits);
            if (!(encoded instanceof Uint8Array)) throw new ErrorClass("InvalidResponse");
            return encoded;
          } catch {
            // Missing response bytes become InvalidResponse/unknown in native
            // code. Never forward codec text or a claimed rollback outcome.
            return "null";
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
