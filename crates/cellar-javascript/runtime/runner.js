// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// Shared SDK facade for embedded V8 and Node. This file is a factory expression:
// the Rust bootstrap evaluates or embeds it, then supplies the private host call.
// Keep the expression free of a trailing semicolon for embedding in parentheses.
(
  host,
  synchronousHost,
  currentHost,
  encodeClone,
  decodeClone,
  scopedHost,
  addAbortListener,
  makeDelegates,
  makeRpcStreams,
  makeRpcWritables,
  makeRpcInputs,
  makeRpcMessages,
  makeRpcRequests,
  currentCallbackHost,
) => {
  "use strict";
  const delegates = makeDelegates(
    makeRpcStreams(),
    makeRpcWritables(),
    makeRpcInputs,
    (readables) => makeRpcMessages(readables, makeRpcRequests),
  );

  // Capture these builtins before importing/evaluating application code.
  const parse = JSON.parse;
  const stringify = JSON.stringify;
  const freeze = Object.freeze;
  const entries = Object.entries;
  const isArray = Array.isArray;
  const MapClass = Map;
  const WeakMapClass = WeakMap;
  const SetClass = Set;
  const PromiseClass = Promise;
  const resolve = Promise.resolve.bind(Promise);
  const race = Promise.race.bind(Promise);
  const allSettled = Promise.allSettled.bind(Promise);
  const lifetimes = new MapClass();
  function track(id, promise, report = false) {
    const state = lifetimes.get(id);
    if (!state) return promise;
    if (report && state.reported + state.retained >= state.limit) {
      const error = new ErrorClass("waitUntil job quota exceeded");
      error.name = "QuotaExceededError";
      throw error;
    }
    if (report) state.reported++;
    const settled = resolve(promise);
    state.jobs.add(settled);
    const release = () => {
      state.jobs.delete(settled);
      if (report) state.reported--;
    };
    settled.then(release, () => {
      release();
      if (report) state.failed = true;
    });
    return promise;
  }
  async function drain(id) {
    const state = lifetimes.get(id);
    if (!state) return;
    while (lifetimes.get(id) === state) {
      if (state.jobs.size) {
        await race([allSettled([...state.jobs]), state.cancelled]);
      } else if (state.cleanups.size) {
        const cleanups = [...state.cleanups];
        state.cleanups.clear();
        const results = await allSettled(
          cleanups.map((close) => resolve().then(close)),
        );
        if (results.some((result) => result.status === "rejected"))
          state.failed = true;
      } else return;
    }
  }
  const ErrorClass = Error;
  const Uint8ArrayClass = Uint8Array;
  const apply = Reflect.apply;
  const typedArrayPrototype = Object.getPrototypeOf(Uint8Array.prototype);
  const typedArrayTag = Object.getOwnPropertyDescriptor(
    typedArrayPrototype,
    Symbol.toStringTag,
  ).get;
  const typedArrayBytes = Object.getOwnPropertyDescriptor(
    typedArrayPrototype,
    "byteLength",
  ).get;
  const typedArrayOffset = Object.getOwnPropertyDescriptor(
    typedArrayPrototype,
    "byteOffset",
  ).get;
  const typedArrayBuffer = Object.getOwnPropertyDescriptor(
    typedArrayPrototype,
    "buffer",
  ).get;
  const budgetKeys = new WeakMap();
  const rpcKey = Symbol.for("cellar.rpc");
  const own = Object.getOwnPropertyDescriptor;
  const rpcFailures = freeze({
    InvalidArgument: '{"rpc_error":"InvalidArgument"}',
    MethodNotFound: '{"rpc_error":"MethodNotFound"}',
    ContractMismatch: '{"rpc_error":"ContractMismatch"}',
    ResourceExhausted: '{"rpc_error":"ResourceExhausted"}',
    ContextExpired: '{"rpc_error":"ContextExpired"}',
    Cancelled: '{"rpc_error":"Cancelled"}',
    DeadlineExceeded: '{"rpc_error":"DeadlineExceeded"}',
    Unavailable: '{"rpc_error":"Unavailable"}',
    OwnershipLost: '{"rpc_error":"OwnershipLost"}',
    DurabilityUnproven: '{"rpc_error":"DurabilityUnproven"}',
    InvalidResponse: '{"rpc_error":"InvalidResponse"}',
    Internal: '{"rpc_error":"Internal"}',
  });
  function handlerFailure(error) {
    try {
      // Read a data property without invoking code/outcome/message getters.
      const descriptor = own(error, "code");
      const value = descriptor && own(descriptor, "value");
      const category =
        value &&
        typeof value.value === "string" &&
        value.value.length <= 32 &&
        own(rpcFailures, value.value);
      if (category) return category.value;
    } catch {
      /* A proxy trap cannot manufacture outcome evidence. */
    }
    return rpcFailures.Internal;
  }
  // Node supplies events.addAbortListener, which resists stopImmediatePropagation.
  // Closed V8 bundles may supply a standard AbortController polyfill.
  const dispose = Symbol.dispose;
  const subscribe = addAbortListener
    ? (signal, listener) => {
        let subscription;
        const onAbort = () => {
          listener();
          // A synthetic abort event must not consume Node's one-shot protected
          // subscription. Native Node snapshots listeners for each dispatch.
          if (!signal.aborted) subscription = addAbortListener(signal, onAbort);
        };
        subscription = addAbortListener(signal, onAbort);
        return () => subscription[dispose]();
      }
    : (signal, listener) => {
        const add = signal.addEventListener,
          remove = signal.removeEventListener;
        apply(add, signal, ["abort", listener]);
        return () => apply(remove, signal, ["abort", listener]);
      };

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
      return value.map((v) => json(v, depth + 1));
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
  function incomingBody(context) {
    if (typeof ReadableStream !== "function")
      throw new ErrorClass("Incoming HTTP streams require the web runtime");
    let released = false;
    let remove;
    const release = async () => {
      if (released) return;
      released = true;
      remove?.();
      await context._call("request.body.cancel", {});
    };
    remove = context._rpcCleanup(release);
    return new ReadableStream(
      {
        type: "bytes",
        async pull(controller) {
          try {
            const chunk = await context._call("request.body.read", {});
            if (chunk.done) {
              await release();
              controller.close();
            } else {
              const body = context._decode(chunk.body);
              if (body.byteLength) controller.enqueue(body);
            }
          } catch (error) {
            await release().catch(() => {});
            controller.error(error);
          }
        },
        cancel: release,
      },
      { highWaterMark: 0 },
    );
  }

  function request(value, context) {
    const body = decode(value.body);
    const text = () =>
      decodeURIComponent(
        Array.from(body, (b) => "%" + b.toString(16).padStart(2, "0")).join(""),
      );

    if (value.body_stream) {
      const refuse = () => {
        throw new TypeError("Consume the incoming HTTP byte stream");
      };
      return freeze({
        ...value,
        body,
        body_stream: incomingBody(context),
        text: refuse,
        json: refuse,
      });
    }
    return freeze({ ...value, body, text, json: () => json(parse(text())) });
  }

  // Optional Workers crypto resolves the current event rather than capturing
  // the cell that first installed the global. Only these pure-value operations
  // have this global lifetime; ordinary saved SDK contexts still expire.
  const crypto = freeze({
    getRandomValues(array) {
      const type = apply(typedArrayTag, array, []);
      if (
        ![
          "Int8Array",
          "Uint8Array",
          "Uint8ClampedArray",
          "Int16Array",
          "Uint16Array",
          "Int32Array",
          "Uint32Array",
          "BigInt64Array",
          "BigUint64Array",
        ].includes(type)
      ) {
        const error = new ErrorClass("Expected an integer typed array");
        error.name = "TypeMismatchError";
        throw error;
      }
      const length = apply(typedArrayBytes, array, []);
      if (length > 65536) {
        const error = new ErrorClass("Random-values quota exceeded");
        error.name = "QuotaExceededError";
        throw error;
      }
      const invocation = context(currentHost(), null, null);
      const bytes = decode(
        invocation._sync("workers.crypto.random", { length }),
      );
      new Uint8Array(
        apply(typedArrayBuffer, array, []),
        apply(typedArrayOffset, array, []),
        length,
      ).set(bytes);
      return array;
    },
    randomUUID: () =>
      context(currentHost(), null, null)._sync("workers.crypto.uuid"),
    get subtle() {
      throw new ErrorClass("Unsupported Workers API: crypto.subtle");
    },
  });

  // Each facade closes over one invocation ID. The backend independently checks
  // continuation identity and liveness whenever that facade calls into Rust.
  function context(
    id,
    identity,
    activation,
    contracts = {},
    invocationLimits = {},
  ) {
    const retain = (make) => {
      if (
        (activation === null ? currentHost() : currentHost(activation)) !== id
      )
        throw rpcFailure("ContextMismatch", "not_committed");
      const state = lifetimes.get(id);
      if (!state) throw rpcFailure("UnsupportedOperation", "not_committed");
      if (state.reported + state.retained >= state.limit)
        throw rpcFailure("OperationLimit", "not_committed");
      state.retained++;
      let pending;
      try {
        pending = resolve(make());
      } catch (error) {
        state.retained--;
        throw error;
      }
      const release = () => {
        state.retained--;
      };
      pending.then(release, release);
      return track(id, pending);
    };
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

      return synchronous
        ? finish(name, pending)
        : track(
            id,
            pending.then((reply) => finish(name, reply)),
          );
    };

    const finish = (name, reply) => {
      const result = parse(reply);

      if (!result.ok) {
        const error = rpcFailure(result.code, result.outcome);
        if (result.errno !== undefined) error.errno = result.errno;
        if (result.operation !== undefined) error.operation = result.operation;
        if (result.completed_bytes !== undefined)
          error.completed_bytes = result.completed_bytes;
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
          "workers.namespace",
          "workers.http.limits",
          "workers.variable",
          "workers.container.running",
          "workers.container.admit",
          "workers.container.discard",
          "workers.callback.begin",
          "workers.sql.size",
          "workers.kv.get",
          "workers.kv.list",
          "workers.r2.head",
          "workers.r2.get",
          "storage.read",
          "alarm.get",
          "var",
          "kv.get",
          "kv.list",
          "container.bindings",
          "container.capabilities",
          "container.running",
          "container.monitor",
          "container.exec.info",
          "container.exec.read_stdout",
          "container.exec.read_stderr",
          "container.exec.wait",
          "container.socket.read",
          "container.response.read",
          "container.response.cancel",
          "container.websocket.receive",
          "websocket.next",
          "websocket.closed",
          "response.read",
          "response.cancel",
          "response.closed",
        ].includes(name);
        const error = new ErrorClass(read ? "HostError" : "Ambiguous");
        error.code = error.message;
        error.outcome = "unknown";
        throw error;
      }
    };

    const call = (name, value = {}, body, signal, budget, soft = false) =>
      (soft ? resolve : (promise) => track(id, promise))(
        (async () => {
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
          let submitted = false,
            cancelled = false,
            invalidSignal = false;
          // Subscribe before admission. The native cancellation capability belongs
          // only to this operation; its signal may fire in another JS continuation.
          try {
            if (signal !== undefined) {
              const onAbort = () => {
                let aborted = true;
                try {
                  aborted = signal.aborted;
                  if (typeof aborted !== "boolean") throw new TypeError();
                } catch {
                  invalidSignal = true;
                }
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
              if (invalidSignal)
                throw rpcFailure("InvalidArgument", "not_committed");
              if (cancelled) throw rpcFailure("Cancelled", "not_committed");
            }

            // Synchronous admission failures precede submission. Qualified
            // cancellable operations return a private [promise, cancel] pair;
            // no cancellation authority crosses JSON.
            try {
              const key =
                budget === undefined ? undefined : budgetKeys.get(budget);
              if (budget !== undefined && key === undefined)
                throw new ErrorClass("ContextMismatch");
              const admitted = host(id, name, encoded, body, key);
              if (
                [
                  "cell.call",
                  "workers.rpc.invoke",
                  "response.read",
                  "request.body.read",
                  "response.closed",
                  "websocket.next",
                  "websocket.closed",
                  "container.port.fetch",
                  "container.port.fetch_stream",
                  "container.response.read",
                  "container.exec.read_stdout",
                  "container.exec.read_stderr",
                  "container.exec.wait",
                  "container.port.connect_websocket",
                  "container.websocket.receive",
                  "workers.fetch",
                  "workers.fetch_stream",
                  "workers.container.fetch",
                  "workers.container.fetch_stream",
                ].includes(name)
              )
                [pending, cancel] = admitted;
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
            if (name.startsWith("filesystem.")) fsEnvelope(result);

            if (!result.ok) {
              const error = rpcFailure(result.code, result.outcome);
              if (result.errno !== undefined) error.errno = result.errno;
              if (result.operation !== undefined)
                error.operation = result.operation;
              if (result.completed_bytes !== undefined)
                error.completed_bytes = result.completed_bytes;
              throw error;
            }

            if (name === "cell.call") {
              if (replyBody instanceof Uint8Array) return replyBody;
              throw rpcFailure("InvalidResponse", "unknown");
            }

            // A result that cannot be represented safely may follow a committed write.
            // Refusing it must not imply that the native mutation rolled back.
            try {
              return json(result.value);
            } catch (error) {
              if (name.startsWith("filesystem.")) throw error;
              const read = [
                "clock.now",
                "workers.crypto.random",
                "workers.crypto.uuid",
                "workers.identity",
                "workers.namespace",
                "workers.http.limits",
                "workers.variable",
                "workers.container.running",
                "workers.container.admit",
                "workers.container.discard",
                "workers.callback.begin",
                "workers.sql.size",
                "workers.kv.get",
                "workers.kv.list",
                "workers.r2.head",
                "workers.r2.get",
                "storage.read",
                "alarm.get",
                "var",
                "kv.get",
                "kv.list",
                "container.bindings",
                "container.capabilities",
                "container.running",
                "container.monitor",
                "container.exec.info",
                "container.exec.read_stdout",
                "container.exec.read_stderr",
                "container.exec.wait",
                "container.socket.read",
                "container.response.read",
                "container.response.cancel",
                "container.websocket.receive",
                "websocket.next",
                "websocket.closed",
                "response.read",
                "response.cancel",
                "response.closed",
              ].includes(name);
              throw rpcFailure(read ? "HostError" : "Ambiguous", "unknown");
            }
          } finally {
            try {
              unsubscribe?.();
            } catch {
              throw rpcFailure(
                "InvalidArgument",
                submitted ? "unknown" : "not_committed",
              );
            }
          }
        })(),
      );

    const synchronous = (name, value = {}) => invoke(name, value, true);

    const makeContainer = (binding = null) => {
      const containerCall = (name, value = {}, ...options) =>
        call(
          name,
          binding === null ? value : { ...value, binding },
          ...options,
        );
      const resource = (kind, resourceId) => {
        const operation = (method, args = {}) =>
          containerCall(`container.${kind}.${method}`, {
            ...args,
            id: resourceId,
          });
        const common = {
          release: () => containerCall("container.release", { id: resourceId }),
        };
        return freeze(
          kind === "exec"
            ? {
                ...common,
                info: async () => freeze(await operation("info")),
                resize: (cols, rows) => operation("resize", { cols, rows }),
                readStdout: async () => decode(await operation("read_stdout")),
                readStderr: async () => decode(await operation("read_stderr")),
                write: (bytes) => operation("write", { data: encode(bytes) }),
                closeStdin: () => operation("close_stdin"),
                wait: () => operation("wait"),
                kill: (signal = 9) => operation("kill", { signal }),
              }
            : kind === "websocket"
              ? {
                  ...common,
                  receive: async () => {
                    const event = await operation("receive");
                    return freeze(
                      event.type === "binary"
                        ? { ...event, data: decode(event.data) }
                        : event,
                    );
                  },
                  sendText: (text) => operation("send_text", { text }),
                  sendBinary: (bytes) =>
                    operation("send_binary", { data: encode(bytes) }),
                  close: (code = 1000, reason = "") =>
                    operation("close", { code, reason }),
                }
              : {
                  ...common,
                  read: async () => decode(await operation("read")),
                  write: (bytes) => operation("write", { data: encode(bytes) }),
                  closeWrite: () => operation("close_write"),
                  close: () => operation("close"),
                },
        );
      };

      return freeze({
        capabilities: async () => {
          const value = await containerCall("container.capabilities");
          return freeze({ ...value, operations: freeze(value.operations) });
        },
        reconcile: async () =>
          freeze(await containerCall("container.reconcile")),
        running: () => containerCall("container.running"),
        start: (options = {}) => containerCall("container.start", { options }),
        monitor: () => containerCall("container.monitor"),
        destroy: () => containerCall("container.destroy"),
        signal: (signal) => containerCall("container.signal", { signal }),
        setInactivityTimeout: (timeout_ms) =>
          containerCall("container.set_inactivity_timeout", { timeout_ms }),
        interceptOutboundHttp: (target, binding) =>
          containerCall("container.intercept_outbound_http", {
            target,
            service: { kind: "binding", binding },
          }),
        interceptOutboundHttps: (target, binding) =>
          containerCall("container.intercept_outbound_https", {
            target,
            service: { kind: "binding", binding },
          }),
        exec: async (options) =>
          resource("exec", await containerCall("container.exec", { options })),
        execOutput: async (options, max_bytes = 1048576) => {
          const output = await containerCall("container.exec_output", {
            options,
            max_bytes,
          });
          return freeze({
            ...output,
            stdout: decode(output.stdout),
            stderr: decode(output.stderr),
          });
        },
        readFile: async (path, max_bytes = 1048576) =>
          decode(
            await containerCall("container.file.read", { path, max_bytes }),
          ),
        writeFile: (path, data, { mode = 0o644 } = {}) => {
          if (!(data instanceof Uint8Array)) {
            throw new TypeError("Container file data must be Uint8Array");
          }
          return containerCall("container.file.write", {
            path,
            data: encode(data),
            mode,
          });
        },
        statFile: async (path) =>
          freeze(await containerCall("container.file.stat", { path })),
        listFiles: async (
          path,
          { max_bytes = 1048576, max_entries = 128 } = {},
        ) =>
          freeze(
            (
              await containerCall("container.file.list", {
                path,
                max_bytes,
                max_entries,
              })
            ).map((value) => freeze(value)),
          ),
        port: (port) =>
          freeze({
            waitReady: (timeout_ms = 10000) =>
              containerCall("container.port.wait_ready", { port, timeout_ms }),
            fetch: async (value, options = {}) =>
              request(
                await containerCall(
                  "container.port.fetch",
                  { port, request: value },
                  undefined,
                  options.signal,
                ),
              ),
            fetchStream: async (value, options = {}) => {
              const result = await containerCall(
                "container.port.fetch_stream",
                { port, request: value },
                undefined,
                options.signal,
              );
              const id = result.stream;
              let ended = id === null;
              const cancel = async () => {
                if (ended) return;
                ended = true;
                await containerCall("container.response.cancel", { id });
              };
              const read = async () => {
                if (ended) return null;
                try {
                  const chunk = await containerCall(
                    "container.response.read",
                    { id },
                    undefined,
                    options.signal,
                  );
                  ended = chunk.done;
                  return chunk.done ? null : decode(chunk.body);
                } catch (error) {
                  await cancel().catch(() => {});
                  throw error;
                }
              };
              return freeze({
                status: result.status,
                headers: freeze(result.headers.map((pair) => freeze(pair))),
                body: decode(result.body),
                read,
                async *chunks() {
                  try {
                    if (result.body) yield decode(result.body);
                    while (!ended) {
                      const chunk = await read();
                      if (chunk !== null) yield chunk;
                    }
                  } finally {
                    await cancel();
                  }
                },
                cancel,
              });
            },
            connect: async () =>
              resource(
                "socket",
                await containerCall("container.port.connect", { port }),
              ),
            connectWebSocket: async (url, headers = []) => {
              const result = await containerCall(
                "container.port.connect_websocket",
                {
                  port,
                  url,
                  headers,
                },
              );
              return freeze(
                result.upgraded
                  ? {
                      upgraded: true,
                      protocol: result.protocol,
                      headers: freeze(
                        result.headers.map((pair) => freeze(pair)),
                      ),
                      socket: resource("websocket", result.id),
                    }
                  : { upgraded: false, response: request(result.response) },
              );
            },
          }),
      });
    };
    // Checkpoints contain decimal-string generations and revisions. Keep the
    // full checkpoint on every mutation so a stale name cannot select a successor.
    const sandboxes = freeze({
      start: async (name, binding, parameters = { options: {} }) =>
        freeze(await call("sandbox.start", { name, binding, parameters })),
      get: async (name) => {
        const value = await call("sandbox.get", { name });
        return value === null ? null : freeze(value);
      },
      list: async ({ after = null, limit = 100 } = {}) => {
        const page = await call("sandbox.list", { after, limit });
        return freeze({
          allocations: freeze(page.allocations.map(freeze)),
          next: page.next,
        });
      },
      inspect: (allocation) => call("sandbox.inspect", { allocation }),
      execute: (allocation, operation, input = {}) =>
        call("sandbox.execute", { allocation, command: operation, input }),
      destroy: async (allocation) =>
        freeze(await call("sandbox.destroy", { allocation })),
      parent: async () => {
        const value = await call("sandbox.parent");
        return value === null ? null : freeze(value);
      },
      retire: async () => freeze(await call("sandbox.retire")),
      requestErasure: () => call("sandbox.erase"),
    });
    const container = makeContainer();
    const containers = freeze({
      get: (name) => {
        if (typeof name !== "string" || !/^[A-Za-z0-9_.-]{1,128}$/.test(name))
          throw rpcFailure("ValidationError", "not_committed");
        return name === "container" ? container : makeContainer(name);
      },
      list: async () =>
        freeze(
          (await call("container.bindings")).map(({ name, capabilities }) =>
            freeze({
              name,
              capabilities: freeze({
                ...capabilities,
                operations: freeze(capabilities.operations),
              }),
            }),
          ),
        ),
    });

    // These objects contain bridge calls, never native storage handles. Binding
    // lookup and effect execution remain in the original Rust invocation.
    const fsEnvelope = (result) => {
      if (
        !result ||
        typeof result !== "object" ||
        isArray(result) ||
        typeof result.ok !== "boolean" ||
        (result.ok
          ? !("value" in result)
          : typeof result.error !== "string" ||
            typeof result.code !== "string" ||
            !result.code ||
            !["not_committed", "locally_committed", "unknown"].includes(
              result.outcome,
            ) ||
            (result.completed_bytes !== undefined &&
              (!Number.isSafeInteger(result.completed_bytes) ||
                result.completed_bytes < 0)) ||
            (result.errno != null && typeof result.errno !== "string") ||
            (result.operation != null && typeof result.operation !== "string"))
      )
        throw new ErrorClass("Invalid filesystem reply envelope");
    };
    const fsProgress = (error, completed, operation, committed) => {
      if (
        !error ||
        typeof error.code !== "string" ||
        !["not_committed", "locally_committed", "unknown"].includes(
          error.outcome,
        ) ||
        (error.completed_bytes !== undefined &&
          (!Number.isSafeInteger(error.completed_bytes) ||
            error.completed_bytes < 0))
      ) {
        const cause = error;
        error = rpcFailure("HostError", "unknown");
        error.errno = "EIO";
        error.cause = cause;
      }
      if (
        error.code === "ValidationError" &&
        error.outcome === "not_committed" &&
        error.errno === undefined
      )
        error.errno = "EINVAL";
      error.completed_bytes = completed + (error.completed_bytes ?? 0);
      error.operation = `filesystem.${operation}`;
      if (completed > 0 && error.outcome === "not_committed")
        error.outcome = committed ? "locally_committed" : "unknown";
      return error;
    };
    const fsCall = async (operation, prepare = () => ({})) => {
      try {
        const request = prepare();
        const value = await call(`filesystem.${operation}`, request);
        fsReply(operation, value, request);
        return value;
      } catch (error) {
        // Caller-scope admission can fail before a native filesystem reply.
        throw fsProgress(error, 0, operation, false);
      }
    };
    // A corrupt acknowledgement can follow committed work. Validate before
    // exposing values or using a count to advance a looping helper.
    const fsReply = (operation, value, request) => {
      const require = (valid) => {
        if (!valid) throw new ErrorClass("Invalid filesystem reply");
      };
      const object = (value) =>
        require(value !== null && typeof value === "object" && !isArray(value));
      const integer = (value, maximum = Number.MAX_SAFE_INTEGER) =>
        require(Number.isSafeInteger(value) && value >= 0 && value <= maximum);
      const decimal = (value, signed = false) => {
        require(
          typeof value === "string" &&
            value.length <= 20 &&
            (signed ? /^-?[0-9]+$/ : /^[0-9]+$/).test(value),
        );
        const number = BigInt(value);
        require(
          number >= (signed ? -(1n << 63n) : 0n) &&
            number <= (signed ? (1n << 63n) - 1n : (1n << 64n) - 1n),
        );
      };
      const binary = (value) => {
        require(typeof value === "string" && value.length % 4 === 0);
        const padding = value.endsWith("==") ? 2 : value.endsWith("=") ? 1 : 0;
        const body = value.slice(0, value.length - padding);
        require(!/[^A-Za-z0-9+/]/.test(body));
        require(
          !padding ||
            (body.length > 0 &&
              (alphabet.indexOf(body.at(-1)) & (padding === 2 ? 15 : 3)) === 0),
        );
        return (value.length / 4) * 3 - padding;
      };
      const entry = (value) => {
        object(value);
        const length = binary(value.name_base64);
        require(length > 0 && length <= 255);
        decimal(value.inode);
        require(value.kind === "File" || value.kind === "Directory");
      };
      if (["open", "creat", "openat", "opendir", "dup"].includes(operation)) {
        object(value);
        require(typeof value.handle === "string" && value.handle.length > 0);
      } else if (["read", "pread", "read_file"].includes(operation)) {
        object(value);
        const count = binary(value.data_base64);
        if (operation !== "read_file") {
          integer(value.count, request.count);
          require(count === value.count);
        } else if (request.max_bytes !== null)
          require(count <= request.max_bytes);
      } else if (operation === "write" || operation === "pwrite") {
        integer(value, binary(request.data_base64));
      } else if (operation === "lseek") decimal(value);
      else if (["stat", "lstat", "fstat", "fstatat"].includes(operation)) {
        object(value);
        for (const key of [
          "inode",
          "size",
          "nlink",
          "version",
          "allocated_bytes",
        ])
          decimal(value[key]);
        for (const key of ["atime_ns", "mtime_ns", "ctime_ns"])
          decimal(value[key], true);
        for (const key of ["mode", "uid", "gid"])
          integer(value[key], 0xffffffff);
        require(value.kind === "File" || value.kind === "Directory");
      } else if (operation === "edit_file") {
        object(value);
        decimal(value.inode);
        decimal(value.version);
      } else if (operation === "readdir") {
        if (value !== null) entry(value);
      } else if (operation === "read_page" || operation === "read_dir") {
        const values =
          operation === "read_page" ? value : (object(value), value.entries);
        require(isArray(values) && values.length <= request.limit);
        for (const item of values) entry(item);
        if (operation === "read_dir" && value.next !== null) {
          object(value.next);
          require(typeof value.next.scope === "string");
          decimal(value.next.inode);
          decimal(value.next.position);
        }
      } else if (operation === "telldir") require(binary(value) === 16);
      else if (operation === "collect_garbage") integer(value);
      else if (operation === "statvfs") {
        object(value);
        for (const key of [
          "logical_bytes",
          "allocated_bytes",
          "inodes",
          "available_logical_bytes",
          "available_allocated_bytes",
          "available_inodes",
        ])
          decimal(value[key]);
        integer(value.chunk_bytes);
        require(value.chunk_bytes > 0);
      } else if (operation === "capabilities") {
        object(value);
        object(value.limits);
        object(value.transport);
        for (const key of ["logical_uid", "logical_gid", "creation_mask"])
          integer(value[key], 0xffffffff);
        for (const key of [
          "read_only",
          "regular_files",
          "directories",
          "positional_io",
          "directory_relative",
          "transactions",
          "symlinks",
          "hard_links",
          "advisory_locks",
          "atime",
        ])
          require(typeof value[key] === "boolean");
        for (const key of [
          "max_file_bytes",
          "max_logical_bytes",
          "max_allocated_bytes",
          "max_inodes",
        ])
          decimal(value.limits[key]);
        for (const key of [
          "max_transfer_bytes",
          "max_file_helper_bytes",
          "max_handles",
          "max_host_handles",
          "max_path_bytes",
          "max_path_depth",
          "max_discarded_ranges",
          "gc_batch_chunks",
          "max_transaction_operations",
          "max_transaction_bytes",
          "max_directory_entries",
          "max_namespace_directories",
        ])
          integer(value.limits[key], 0x7fffffff);
        for (const key of [
          "max_value_bytes",
          "max_body_bytes",
          "max_batch_operations",
        ])
          integer(value.transport[key]);
      } else require(value === null);
    };
    const fsArgument = () => {
      const error = rpcFailure("ValidationError", "not_committed");
      error.errno = "EINVAL";
      return error;
    };
    const fsBytes = (value, maximum = Infinity) => {
      if (typeof value === "string") {
        if (value.length > maximum) throw fsArgument();
        return bytes(value);
      }
      if (
        value === null ||
        typeof value !== "object" ||
        apply(typedArrayTag, value, []) !== "Uint8Array"
      )
        throw fsArgument();
      const length = apply(typedArrayBytes, value, []);
      if (length > maximum) throw fsArgument();
      try {
        // Detached arrays also report zero bytes. Constructing a view rejects
        // them while retaining valid empty arrays and the exact byte range.
        return new Uint8ArrayClass(
          apply(typedArrayBuffer, value, []),
          apply(typedArrayOffset, value, []),
          length,
        );
      } catch (cause) {
        const error = fsArgument();
        error.cause = cause;
        throw error;
      }
    };
    const fsPath = (path) =>
      typeof path === "string" ? path : { bytes_base64: encode(fsBytes(path)) };
    const fsHandle = (directory) => {
      const handle = directory?.handle;
      if (typeof handle !== "string") throw fsArgument();
      return handle;
    };
    const fsInteger = (value) => {
      if (typeof value === "bigint" || typeof value === "string")
        return String(value);
      if (typeof value === "number" && Number.isSafeInteger(value))
        return String(value);
      throw fsArgument();
    };
    const fsStat = (value) => {
      const result = { ...value };
      for (const key of [
        "inode",
        "size",
        "nlink",
        "atime_ns",
        "mtime_ns",
        "ctime_ns",
        "version",
        "allocated_bytes",
      ])
        result[key] = BigInt(result[key]);
      return freeze(result);
    };
    const fsFile = (handle) =>
      freeze({
        handle,
        read: async (count) =>
          decode((await fsCall("read", () => ({ handle, count }))).data_base64),
        pread: async (count, offset) =>
          decode(
            (
              await fsCall("pread", () => ({
                handle,
                count,
                offset: fsInteger(offset),
              }))
            ).data_base64,
          ),
        write: (data) =>
          fsCall("write", () => ({
            handle,
            data_base64: encode(fsBytes(data)),
          })),
        pwrite: (data, offset) =>
          fsCall("pwrite", () => ({
            handle,
            data_base64: encode(fsBytes(data)),
            offset: fsInteger(offset),
          })),
        writeAll: async (data) => {
          let completed = 0;
          try {
            const caps = await fsCall("capabilities");
            let remaining = fsBytes(data, caps.limits.max_file_helper_bytes);
            if (remaining.length > caps.limits.max_file_helper_bytes)
              throw rpcFailure("ValidationError", "not_committed");
            if (!remaining.length) await fsFile(handle).write(remaining);
            while (remaining.length) {
              const count = await fsFile(handle).write(
                remaining.subarray(
                  0,
                  Math.min(65536, caps.limits.max_transfer_bytes),
                ),
              );
              if (count <= 0) throw rpcFailure("HostError", "unknown");
              completed += count;
              remaining = remaining.subarray(count);
            }
          } catch (error) {
            throw fsProgress(error, completed, "write_all", true);
          }
        },
        readExact: async (count) => {
          let offset = 0;
          try {
            const caps = await fsCall("capabilities");
            if (
              !Number.isSafeInteger(count) ||
              count < 0 ||
              count > caps.limits.max_file_helper_bytes
            )
              throw rpcFailure("ValidationError", "not_committed");
            const result = new Uint8Array(count);
            if (!count) await fsFile(handle).read(0);
            while (offset < count) {
              const chunk = await fsFile(handle).read(
                Math.min(65536, count - offset),
              );
              if (!chunk.length) {
                const error = rpcFailure("UnexpectedEof", "not_committed");
                error.errno = "EIO";
                throw error;
              }
              result.set(chunk, offset);
              offset += chunk.length;
            }
            return result;
          } catch (error) {
            throw fsProgress(error, offset, "read_exact", false);
          }
        },
        lseek: async (offset, whence = "start") =>
          BigInt(
            await fsCall("lseek", () => ({
              handle,
              offset: fsInteger(offset),
              whence,
            })),
          ),
        fstat: async () => fsStat(await fsCall("fstat", () => ({ handle }))),
        ftruncate: (size) =>
          fsCall("ftruncate", () => ({ handle, size: fsInteger(size) })),
        fchmod: (mode) => fsCall("fchmod", () => ({ handle, mode })),
        dup: async () =>
          fsFile((await fsCall("dup", () => ({ handle }))).handle),
        close: () => fsCall("close", () => ({ handle })),
        fsync: () => fsCall("fsync", () => ({ handle })),
        fdatasync: () => fsCall("fdatasync", () => ({ handle })),
      });
    const fsDirectory = (handle) =>
      freeze({
        handle,
        readdir: async () => {
          const value = await fsCall("readdir", () => ({ handle }));
          return value === null
            ? null
            : freeze({
                name: decode(value.name_base64),
                inode: BigInt(value.inode),
                kind: value.kind,
              });
        },
        readPage: async (limit) =>
          (await fsCall("read_page", () => ({ handle, limit }))).map((value) =>
            freeze({
              name: decode(value.name_base64),
              inode: BigInt(value.inode),
              kind: value.kind,
            }),
          ),
        telldir: () => fsCall("telldir", () => ({ handle })),
        seekdir: (cookie) => fsCall("seekdir", () => ({ handle, cookie })),
        rewinddir: () => fsCall("rewinddir", () => ({ handle })),
        closedir: () => fsCall("closedir", () => ({ handle })),
        fstat: async () => fsStat(await fsCall("fstat", () => ({ handle }))),
        fsync: () => fsCall("fsync", () => ({ handle })),
      });
    const filesystem = freeze({
      capabilities: async () => {
        const value = await fsCall("capabilities");
        const limits = { ...value.limits };
        for (const key of [
          "max_file_bytes",
          "max_logical_bytes",
          "max_allocated_bytes",
          "max_inodes",
        ])
          limits[key] = BigInt(limits[key]);
        return freeze({
          ...value,
          limits: freeze(limits),
          transport: freeze(value.transport),
        });
      },
      statvfs: async () => {
        const value = await fsCall("statvfs");
        const result = { ...value };
        for (const key of [
          "logical_bytes",
          "allocated_bytes",
          "inodes",
          "available_logical_bytes",
          "available_allocated_bytes",
          "available_inodes",
        ])
          result[key] = BigInt(result[key]);
        return freeze(result);
      },
      open: async (path, options = {}) =>
        fsFile(
          (await fsCall("open", () => ({ path: fsPath(path), options })))
            .handle,
        ),
      creat: async (path, mode = 0o666) =>
        fsFile(
          (await fsCall("creat", () => ({ path: fsPath(path), mode }))).handle,
        ),
      openat: async (directory, path, options = {}) =>
        fsFile(
          (
            await fsCall("openat", () => ({
              directory: fsHandle(directory),
              path: fsPath(path),
              options,
            }))
          ).handle,
        ),
      stat: async (path) =>
        fsStat(await fsCall("stat", () => ({ path: fsPath(path) }))),
      lstat: async (path) =>
        fsStat(await fsCall("lstat", () => ({ path: fsPath(path) }))),
      fstatat: async (directory, path, no_follow = false) =>
        fsStat(
          await fsCall("fstatat", () => ({
            directory: fsHandle(directory),
            path: fsPath(path),
            no_follow,
          })),
        ),
      truncate: (path, size) =>
        fsCall("truncate", () => ({
          path: fsPath(path),
          size: fsInteger(size),
        })),
      mkdir: (path, mode = 0o777) =>
        fsCall("mkdir", () => ({ path: fsPath(path), mode })),
      mkdirat: (directory, path, mode = 0o777) =>
        fsCall("mkdirat", () => ({
          directory: fsHandle(directory),
          path: fsPath(path),
          mode,
        })),
      rmdir: (path) => fsCall("rmdir", () => ({ path: fsPath(path) })),
      unlink: (path) => fsCall("unlink", () => ({ path: fsPath(path) })),
      unlinkat: (directory, path, remove_directory = false) =>
        fsCall("unlinkat", () => ({
          directory: fsHandle(directory),
          path: fsPath(path),
          remove_directory,
        })),
      rename: (old, path) =>
        fsCall("rename", () => ({ old: fsPath(old), new: fsPath(path) })),
      renameat: (old_directory, old, new_directory, path) =>
        fsCall("renameat", () => ({
          old_directory: fsHandle(old_directory),
          old: fsPath(old),
          new_directory: fsHandle(new_directory),
          new: fsPath(path),
        })),
      chmod: (path, mode) =>
        fsCall("chmod", () => ({ path: fsPath(path), mode })),
      utimens: (path, mtime_ns = "now") =>
        fsCall("utimens", () => ({
          path: fsPath(path),
          mtime_ns:
            mtime_ns === null || mtime_ns === "now"
              ? mtime_ns
              : fsInteger(mtime_ns),
        })),
      opendir: async (path) =>
        fsDirectory(
          (await fsCall("opendir", () => ({ path: fsPath(path) }))).handle,
        ),
      readFile: async (path, max_bytes = null) =>
        decode(
          (await fsCall("read_file", () => ({ path: fsPath(path), max_bytes })))
            .data_base64,
        ),
      writeFile: (path, data) =>
        fsCall("write_file", () => ({
          path: fsPath(path),
          data_base64: encode(fsBytes(data)),
        })),
      replaceFile: (path, data) =>
        fsCall("replace_file", () => ({
          path: fsPath(path),
          data_base64: encode(fsBytes(data)),
        })),
      editFile: async (path, expected, edit) => {
        const value = await fsCall("edit_file", () => ({
          path: fsPath(path),
          inode: fsInteger(expected?.inode),
          version: fsInteger(expected?.version),
          offset: fsInteger(edit?.offset),
          delete_bytes: fsInteger(edit?.delete_bytes),
          data_base64: encode(fsBytes(edit?.insert)),
        }));
        return freeze({
          inode: BigInt(value.inode),
          version: BigInt(value.version),
        });
      },
      readDir: async (path, limit = 100, cursor = null) => {
        const page = await fsCall("read_dir", () => ({
          path: fsPath(path),
          limit,
          cursor,
        }));
        return freeze({
          entries: freeze(
            page.entries.map((value) =>
              freeze({
                name: decode(value.name_base64),
                inode: BigInt(value.inode),
                kind: value.kind,
              }),
            ),
          ),
          next: page.next === null ? null : freeze(page.next),
        });
      },
      sync: () => fsCall("sync"),
      collectGarbage: () => fsCall("collect_garbage"),
    });

    // Socket facades carry opaque IDs, never native handles. Each operation
    // resolves its authority through the current Rust invocation.
    const socket = (id) =>
      freeze({
        id,
        accept: (options = {}) =>
          call("websocket.accept", {
            id,
            hibernating: false,
            tags: [],
            ...options,
          }),
        send: (data) =>
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
        setAttachment: (value) =>
          call("websocket.attachment.set", { id, value }),
        getAttachment: () => call("websocket.attachment.get", { id }),
        tags: () => call("websocket.tags", { id }),
        autoResponseTimestamp: () =>
          call("websocket.auto_response_timestamp", { id }),
        info: () => call("websocket.info", { id }),
      });

    // Keep the native response capability intact so callers can either forward
    // it directly or pull chunks without buffering the complete response.
    const liveResponse = (result) => {
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
          const wrap = (control) => {
            const budget = freeze({
              __proto__: null,
              remaining: control[1],
              limit: control[2],
              release: control[3],
            });
            budgetKeys.set(budget, control[0]);
            return budget;
          };
          const control = host(id, "$rpc_budget", "{}");
          return typeof control?.then === "function"
            ? control.then(wrap)
            : wrap(control);
        },
        verify: (method) => {
          try {
            const contract = contracts[method.digest];
            method.verify(contract);
            return freeze({
              maxMessageBytes:
                invocationLimits[method.digest].max_message_bytes,
              maxDepth: invocationLimits[method.digest].max_depth,
              maxElements: invocationLimits[method.digest].max_elements,
              maxDecodedSlots:
                invocationLimits[method.digest].max_decoded_slots,
            });
          } catch {
            const error = new ErrorClass("ContractMismatch");
            error.code = "ContractMismatch";
            error.outcome = "not_committed";
            throw error;
          }
        },
        call: (metadata, body, signal, budget) =>
          call("cell.call", metadata, body, signal, budget),
      }),
      fetchStream: async (value) =>
        liveResponse(await call("http.fetch_stream", { request: value })),
      acceptWebSocket: async (response) =>
        socket(
          (
            await call("websocket.from_response", {
              response: response.forward,
            })
          ).id,
        ),
      // Private hooks used by dispatch while retaining this invocation.
      _crypto: crypto,
      _current() {
        const active = lifetimes.get(currentHost());
        if (!active?.context)
          throw rpcFailure("ContextExpired", "not_committed");
        return active.context;
      },
      _call: call,
      _rpcSoftCall(name, value, signal) {
        if (name !== "workers.rpc.invoke")
          throw new TypeError("Invalid RPC session operation");
        return call(name, value, undefined, signal, undefined, true);
      },
      _rpcCleanup(close) {
        if (
          (activation === null ? currentHost() : currentHost(activation)) !== id
        )
          throw rpcFailure("ContextMismatch", "not_committed");
        const state = lifetimes.get(id);
        if (!state || state.cleanups.size >= state.limit)
          throw rpcFailure("OperationLimit", "not_committed");
        state.cleanups.add(close);
        return () => state.cleanups.delete(close);
      },
      _containerState(owner) {
        const lifetime = lifetimes.get(id);
        if (!lifetime) {
          const error = new ErrorClass(
            "Queued containers require Workers lifetimes",
          );
          error.code = "UnsupportedOperation";
          error.outcome = "not_committed";
          throw error;
        }
        // Activation getters may create a new context facade after interleaved
        // events. Store queue ordering on the actual invocation lifetime rather
        // than using a facade object's identity as the invocation identity.
        const states = (lifetime.containerStates ??= new WeakMapClass());
        let state = states.get(owner);
        if (!state) {
          state = { tail: undefined, failure: undefined };
          states.set(owner, state);
        }
        return state;
      },
      _containerAdmit(operation, input) {
        if (!lifetimes.has(id)) {
          const error = new ErrorClass(
            "Queued containers require Workers lifetimes",
          );
          error.code = "UnsupportedOperation";
          error.outcome = "not_committed";
          throw error;
        }
        const scope = currentCallbackHost();
        if (scope !== null) {
          const callback = lifetimes.get(id).containerCallbacks?.get(scope);
          if (!callback || callback.closing)
            throw rpcFailure("ContextExpired", "not_committed");
        }
        return synchronous("workers.container.admit", { operation, input });
      },
      _containerControl(pending) {
        const scope = currentCallbackHost();
        if (scope === null) return;
        const callback = lifetimes.get(id)?.containerCallbacks?.get(scope);
        if (!callback || callback.closing)
          throw rpcFailure("ContextExpired", "not_committed");
        callback.controls.add(pending);
        const release = () => callback.controls.delete(pending);
        pending.then(release, release);
      },
      _revoked() {
        if (
          (activation === null ? currentHost() : currentHost(activation)) !== id
        )
          throw rpcFailure("ContextMismatch", "not_committed");
        const state = lifetimes.get(id);
        if (!state) throw rpcFailure("UnsupportedOperation", "not_committed");
        return state.cancelled.then(() => {
          throw rpcFailure("ContextExpired", "unknown");
        });
      },
      _containerMonitor(make) {
        if (
          (activation === null ? currentHost() : currentHost(activation)) !== id
        )
          throw rpcFailure("ContextMismatch", "not_committed");
        const state = lifetimes.get(id);
        if (!state) throw rpcFailure("UnsupportedOperation", "not_committed");
        if (state.reported + state.retained >= state.limit) {
          const error = rpcFailure("OperationLimit", "not_committed");
          error.name = "QuotaExceededError";
          throw error;
        }
        state.retained++;
        let pending;
        try {
          pending = resolve(make());
        } catch (error) {
          state.retained--;
          throw error;
        }
        const release = () => {
          state.retained--;
        };
        pending.then(release, release);
        // The wrapper may await a destroy acknowledgement in another event
        // after its native read completed. Retain that entire monitor promise.
        return track(id, pending);
      },
      _waitUntil(promise) {
        try {
          if (
            (activation === null ? currentHost() : currentHost(activation)) !==
            id
          )
            throw new ErrorClass("ContextMismatch");
        } catch (error) {
          error.code = error.message;
          error.outcome = "not_committed";
          throw error;
        }
        if (!lifetimes.has(id))
          throw new ErrorClass("Workers lifetimes are not enabled");
        track(id, promise, true);
      },
      // Optional Workers state has activation lifetime. This getter deliberately
      // grants no authority to a foreign cell, retired activation, cancelled
      // event, or ambient code outside an admitted native continuation.
      _activation: () => {
        let cachedId, cached;
        const current = () => {
          let invocation;
          try {
            invocation = currentHost(activation);
          } catch (error) {
            error.code = error.message;
            error.outcome = "not_committed";
            throw error;
          }
          if (invocation !== cachedId) {
            cachedId = invocation;
            // Independent facade getters share the native event's canonical
            // context. currentHost(activation) above still verifies that this
            // continuation belongs to the original, live activation.
            cached =
              lifetimes.get(invocation)?.context ??
              context(invocation, identity, activation, contracts);
          }
          return cached;
        };
        current();
        return current;
      },
      _sync: synchronous,
      _callbackSync(callback) {
        if (typeof callback !== "function")
          throw new TypeError("Expected a callback");
        const token = synchronous("workers.callback.start_sync", {});
        let success = false;
        try {
          const result = scopedHost(id, token.id, callback, true);
          success = true;
          return result;
        } finally {
          synchronous("workers.callback.finish_sync", {
            id: token.id,
            success,
          });
        }
      },
      _callback(kind, callback) {
        if (typeof callback !== "function")
          throw new TypeError("Expected a callback");
        return track(
          id,
          (async () => {
            const token = await call("workers.callback.begin", { kind });
            const lifetime = lifetimes.get(id);
            const callbacks =
              lifetime && (lifetime.containerCallbacks ??= new MapClass());
            const boundary = { controls: new SetClass(), closing: false };
            callbacks?.set(token.id, boundary);
            let result,
              success = false;
            try {
              const current = () => {
                if (currentHost(activation, token.id) !== id)
                  throw new ErrorClass("ContextMismatch");
                return context(
                  id,
                  identity,
                  activation,
                  contracts,
                  invocationLimits,
                );
              };
              result = await scopedHost(id, token.id, async () => {
                try {
                  return await callback(current);
                } finally {
                  // Void controls retain this native callback until their
                  // provider acknowledgement or failure. Do not admit new
                  // controls from continuations after the callback returned.
                  boundary.closing = true;
                  await allSettled([...boundary.controls]);
                }
              });
              success = true;
            } finally {
              callbacks?.delete(token.id);
              await call("workers.callback.finish", { id: token.id, success });
            }
            return result;
          })(),
        );
      },
      _serialize: (value) => encode(encodeClone(value)),
      _rpcValue: (value) =>
        delegates.valueOnly(
          value,
          context(id, identity, activation, contracts, invocationLimits),
        ),
      _deserialize: (value) => decodeClone(decode(value)),
      _rpcResult: (value, closed) =>
        delegates.result(
          value,
          context(id, identity, activation, contracts, invocationLimits),
          closed,
        ),
      _rpcCall: (request, args) =>
        delegates.call(
          request,
          args,
          context(id, identity, activation, contracts, invocationLimits),
        ),
      _rpcDecode: (reply) =>
        delegates.reply(
          reply,
          context(id, identity, activation, contracts, invocationLimits),
        ),
      _rpcRetain: retain,
      _decode: decode,
      _encode: (data) => encode(bytes(data)),

      websocket: socket,
      websocketPair: async () => socket((await call("websocket.pair")).id),
      connectWebSocket: async (url, protocols = []) =>
        socket((await call("websocket.connect", { url, protocols })).id),
      websockets: async (tag = null) =>
        (await call("websocket.list", { tag })).map(socket),
      getWebSocketAutoResponse: () => call("websocket.auto_response.get"),
      setWebSocketAutoResponse: (pair) =>
        call("websocket.auto_response", { pair }),

      identity: identity && freeze(identity),
      now: () => call("clock.now"),
      variable: (name) => call("var", { name }),
      container,
      containers,
      sandboxes,

      fs: filesystem,

      storage: freeze({
        read: (key) => call("storage.read", { key }),
        apply: (batch) => call("storage.apply", batch),
        sync: () => call("storage.sync"),
        sql: (sql, params = []) => call("storage.sql", { sql, params }),
        getAlarm: () => call("alarm.get"),
      }),

      cell: (binding, name) =>
        freeze({
          fetch: async (value) =>
            request(
              await call("cell.fetch", { binding, name, request: value }),
            ),
          fetchStream: async (value) =>
            liveResponse(
              await call("cell.fetch_stream", {
                binding,
                name,
                request: value,
              }),
            ),
        }),

      kv: (binding) =>
        freeze({
          get: (key) => call("kv.get", { binding, key }),
          put: (key, value, options = {}) =>
            call("kv.put", { binding, key, value, ...options }),
          delete: (key) => call("kv.delete", { binding, key }),
          list: (options = {}) =>
            call("kv.list", { binding, prefix: "", ...options }),
        }),

      sql: (binding) =>
        freeze({
          query: (sql, params = []) =>
            call("sql.query", { binding, sql, params }),
        }),

      queue: (binding) =>
        freeze({
          send: (body, options = {}) =>
            call("queue.send", { binding, body, ...options }),
        }),

      settle: (options) => call("queue.settle", options),
      noRetry: () => call("cron.no_retry"),
    });
  }

  // Validate only the explicitly declared handlers before signaling readiness.
  // Objects are cached by activation ID; retirement discards activation-local
  // JavaScript state without changing the cell's durable Rust-owned storage.
  return (app, descriptor, maximum, lifetimeLimit) => {
    const declared = parse(descriptor);
    const objects = new MapClass(),
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
      if (
        !registration ||
        Object.keys(registration).length !== Object.keys(methods).length
      ) {
        throw new ErrorClass("RPC methods differ from native registration");
      }
      const table = new MapClass();
      for (const [wire, native] of entries(methods)) {
        const rule = registration[wire];
        const handler =
          rule && own(app.cells[name].prototype, rule.handler)?.value;
        if (
          !rule ||
          rule.digest !== native.digest ||
          rule.input !== native.input ||
          rule.output !== native.output ||
          typeof handler !== "function" ||
          typeof rule.decode !== "function" ||
          typeof rule.encode !== "function"
        ) {
          throw new ErrorClass("RPC contract differs from native registration");
        }
        if (typeof rule.verify !== "function")
          throw new ErrorClass("Missing RPC codec verification");
        rule.verify(contracts[native.digest]);
        table.set(
          wire,
          freeze({
            handler,
            decode: rule.decode,
            encode: rule.encode,
            digest: native.digest,
          }),
        );
      }
      rpc.set(name, table);
    }

    async function dispatch(id, payload, body) {
      const invocation = parse(payload);
      const ctx = context(
        id,
        invocation.identity,
        invocation.object,
        contracts,
        invocation.rpc_limits,
      );
      const lifetime = lifetimes.get(id);
      if (lifetime) lifetime.context = ctx;
      let rule, argument, limits;
      if (invocation.kind === "rpc") {
        rule = rpc.get(invocation.class)?.get(invocation.event.method);
        if (!rule || !(body instanceof Uint8Array))
          throw new ErrorClass("InvalidRpcInvocation");
        // Policy belongs to this native invocation, not the runtime's static
        // generated registration. Never mutate the table shared by other hosts.
        const native = invocation.rpc_limits[rule.digest];
        limits = freeze({
          maxMessageBytes: native.max_message_bytes,
          maxDepth: native.max_depth,
          maxElements: native.max_elements,
          maxDecodedSlots: native.max_decoded_slots,
        });
        argument = rule.decode(body, limits);
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
        try {
          result = await apply(rule.handler, target, [argument, ctx]);
        } catch (error) {
          throw handlerFailure(error);
        }
        try {
          const encoded = rule.encode(result, limits);
          if (!(encoded instanceof Uint8Array))
            throw new ErrorClass("InvalidResponse");
          return encoded;
        } catch {
          // Missing response bytes become InvalidResponse/unknown in native
          // code. Never forward codec text or a claimed rollback outcome.
          return "null";
        }
      }

      let event =
        invocation.kind === "fetch"
          ? request(invocation.event, ctx)
          : json(invocation.event);

      if (invocation.kind === "websocket_message") {
        event = freeze({
          ...event,
          data: event.body === undefined ? event.text : decode(event.body),
        });
      }

      if (invocation.kind === "cell_rpc" || invocation.kind === "worker_rpc") {
        const workerRpc = invocation.kind === "worker_rpc";
        const args =
          workerRpc && event.operation.kind === "get"
            ? undefined
            : ctx._deserialize(workerRpc ? event.operation.args : event.args);
        if ((!workerRpc || event.operation.kind !== "get") && !isArray(args))
          throw new ErrorClass("Invalid RPC arguments");
        if (!workerRpc && event.method === "__cellar_workers_rpc_inputs") {
          return stringify(
            await delegates.accept(args, ctx, (method, args) =>
              target.cell_rpc(freeze({ method, args }), ctx),
            ),
          );
        }
        let value;
        try {
          value = workerRpc
            ? await target.worker_rpc(
                freeze({
                  ...event,
                  operation: freeze({ ...event.operation, args }),
                }),
                ctx,
              )
            : await target.cell_rpc(
                freeze({ method: event.method, args }),
                ctx,
              );
        } catch (error) {
          // Remote throws are successful native RPC responses. Preserve the
          // thrown value and own data properties, without leaking its stack.
          const properties = Object.create(null);
          if (error instanceof ErrorClass) {
            properties.name = error.name;
            for (const [key, value] of entries(error)) {
              if (key !== "stack") properties[key] = value;
            }
          }
          return stringify({
            value: workerRpc
              ? ctx._rpcValue([error, properties])
              : ctx._serialize([error, properties]),
            error: true,
          });
        }
        // Encoding can fail after the handler committed writes. Such a
        // failure is a lost result, not a business exception from the method.
        return stringify(
          workerRpc
            ? { value: ctx._rpcValue(value), error: false }
            : await ctx._rpcResult(value),
        );
      }

      let result = await target[invocation.kind](event, ctx);

      // Forward the native capability without consuming its body or upgrade.
      if (invocation.kind === "fetch" && result?.forward !== undefined) {
        return stringify({
          forward: result.forward,
          ...(result.headers === undefined ? {} : { headers: result.headers }),
        });
      }

      if (invocation.kind === "fetch" && result?.chunks !== undefined) {
        // Publish headers before pulling the producer. Each awaited write
        // preserves native backpressure and keeps invocation authority alive.
        try {
          await ctx._call("response.start", {
            status: result.status,
            headers: result.headers,
            body: "",
          });
          for await (const chunk of result.chunks) {
            await ctx._call("response.write", { body: ctx._encode(chunk) });
          }
          await ctx._call("response.finish");
        } finally {
          // A failed head must release a web reader even before iteration starts.
          if (typeof result.cancel === "function") await result.cancel();
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
          try {
            await ctx._call("response.upgrade", response);
            await result.run(ctx.websocket(result.websocket), ctx);
          } finally {
            if (typeof result.cancel === "function") await result.cancel();
          }
          result = { streamed: true };
        } else {
          result = response;
        }
      }

      // Non-HTTP handlers complete without an application return value.
      return stringify(json(invocation.kind === "fetch" ? result : null));
    }

    return freeze({
      failurePayload(error) {
        if (typeof error === "string") {
          for (const [, payload] of entries(rpcFailures))
            if (payload === error) return payload;
        }
        return "null";
      },
      async dispatch(id, payload, body) {
        if (!declared.workers_lifetimes || parse(payload).kind === "rpc")
          return dispatch(id, payload, body);
        let cancel;
        const cancelled = new PromiseClass((done) => {
          cancel = done;
        });
        const state = {
          jobs: new SetClass(),
          cleanups: new SetClass(),
          reported: 0,
          retained: 0,
          limit: lifetimeLimit,
          failed: false,
          cancel,
          cancelled,
        };
        lifetimes.set(id, state);
        let encoded = "null",
          failed = false;
        try {
          encoded = await dispatch(id, payload, body);
        } catch {
          failed = true;
        }
        const publish = (value) =>
          context(id, null, null)._call("workers.lifetime.response", value);
        try {
          await publish({ value: encoded, failed });
        } catch {
          failed = true;
          // A refused oversized result must not abandon accepted background
          // work. A compact error can use publication capacity if the first
          // attempt was refused before admission. Both hosts admit it once.
          try {
            await publish({ value: "null", failed: true });
          } catch {}
        }
        await drain(id);
        if (failed || state.failed)
          throw new ErrorClass("Workers event/background work failed");
        // The backend owns final cleanup after a full microtask checkpoint.
        // Derived continuations may register more SDK work after this resolves.
        return "null";
      },
      drain,
      background(id) {
        const state = lifetimes.get(id);
        return !state ? 1 : state.jobs.size ? 0 : state.failed ? 2 : 1;
      },
      cancel(id) {
        const state = lifetimes.get(id);
        if (state) {
          lifetimes.delete(id);
          state.cancel();
        }
      },
      retire(id) {
        objects.delete(id);
      },
    });
  };
}
