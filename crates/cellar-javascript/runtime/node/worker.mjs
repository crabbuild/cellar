// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// Injected by the Rust bootstrap. No application code runs before the private
// transport and context factory have captured their dependencies.
import { readFileSync } from "node:fs";
import { Worker } from "node:worker_threads";
import { Serializer, Deserializer } from "node:v8";
import { types } from "node:util";
import { addAbortListener } from "node:events";
import { setImmediate as checkpoint } from "node:timers/promises";
import { AsyncLocalStorage } from "node:async_hooks";
import { pathToFileURL } from "node:url";

if (Number(process.versions.node.split(".")[0]) < 24) {
  throw new Error("Cellar requires Node 24 or later");
}

// Bootstrap configuration arrives on stdin, keeping the authentication secret
// out of argv and the environment. stdout/stderr remain available to the app.
const config = JSON.parse(readFileSync(0, "utf8"));
const parse = JSON.parse;
const stringify = JSON.stringify;
const isPromise = types.isPromise;
const scope = new AsyncLocalStorage();
const workersLifetimes = parse(config.descriptor).workers_lifetimes;
// Track invocation authority separately from operations waiting for Rust replies.
const active = new Map();
const pending = new Map();
const budgets = makeBudgets(
  process.hrtime.bigint.bind(process.hrtime),
  () => scope.getStore()?.invocation,
  active,
  config.pendingLimit,
  (value) => send(value),
  () => {
    const id = ++next;
    if (!Number.isSafeInteger(id)) reject("OperationLimit");
    return id;
  },
);
let next = 0;
let runner;
const exit = process.exit.bind(process);
// One slot suffices: the application event loop can execute only one blocking
// callback at a time. Keep replies bounded independently of MessagePort queues.
const synchronousSlot = new SharedArrayBuffer(config.valueLimit + 8);
const queued = new BigInt64Array(new SharedArrayBuffer(8));
const maximum = Math.min(config.valueLimit * 6 + 4096, config.bridgeLimit);
const synchronousHeader = new Int32Array(synchronousSlot, 0, 2);
const synchronousBytes = new Uint8Array(synchronousSlot, 8);
const io = new Worker(
  new URL(`data:text/javascript,${encodeURIComponent(ioSource)}`),
  {
    workerData: { config, synchronousSlot, queueSlot: queued.buffer },
  },
);
io.on("error", () => exit(70));
io.on("exit", () => exit(70));
function send(value, body, synchronous = false) {
  const encoded = stringify({ ...value, has_body: body !== undefined });
  const size = Buffer.byteLength(encoded) + (body?.byteLength ?? 0) + 8;
  if (
    size > maximum + 8 ||
    Atomics.add(queued, 0, BigInt(size)) + BigInt(size) >
      BigInt(config.bridgeLimit)
  )
    exit(70);
  io.postMessage({ type: synchronous ? "synchronous" : "send", encoded, body });
}
await new Promise((resolve) =>
  io.once("message", (message) => {
    if (message.type !== "connected") exit(70);
    resolve();
  }),
);
delete config.secret;

function reject(code) {
  throw new Error(code);
}

function validBody(body) {
  return (
    body instanceof Uint8Array &&
    body.buffer instanceof ArrayBuffer &&
    !body.buffer.resizable &&
    !body.buffer.detached
  );
}

// AsyncLocalStorage binds promises, timers and Node I/O continuations to their
// invocation. Rust repeats the liveness/limit checks before accepting effects.
function operation(
  invocation,
  name,
  payload,
  body,
  budget,
  synchronous = false,
) {
  const continuation = scope.getStore();
  if (continuation?.invocation !== invocation) {
    reject("ContextMismatch");
  }
  if (continuation.synchronous && !synchronous) reject("UnsupportedOperation");

  const state = active.get(invocation);

  if (!state) {
    reject("ContextExpired");
  }

  if (name === "$check") return;
  if (name === "$rpc_budget") return budgets.start(invocation, state);
  if ((name === "cell.call") !== (body !== undefined))
    reject("ValidationError");
  if (body !== undefined && !validBody(body)) reject("ValidationError");
  if (
    synchronous &&
    ![
      "clock.now",
      "storage.read",
      "storage.apply",
      "storage.sql",
      "alarm.get",
      "workers.callback.start_sync",
      "workers.callback.finish_sync",
      "workers.callback.rollback",
      "workers.rpc.dup",
      "workers.crypto.random",
      "workers.crypto.uuid",
      "workers.identity",
      "workers.namespace",
      "workers.http.limits",
      "workers.variable",
      "workers.container.running",
      "workers.container.admit",
      "workers.container.discard",
      "workers.sql.exec",
      "workers.sql.next",
      "workers.sql.close",
      "workers.sql.size",
      "workers.kv.get",
      "workers.kv.put",
      "workers.kv.delete",
      "workers.kv.list",
      "workers.kv.delete_all",
    ].includes(name)
  ) {
    reject("UnsupportedOperation");
  }

  if (
    Buffer.byteLength(payload) + (body?.byteLength ?? 0) >
    config.valueLimit
  ) {
    reject("ValueLimit");
  }

  const completion = name === "workers.lifetime.response";
  const ordinaryPending = state.pending - Number(pending.has(state.response));
  if (
    (completion && state.response !== undefined) ||
    (!completion &&
      (state.count >= config.operationLimit ||
        ordinaryPending >= config.pendingLimit))
  ) {
    reject("OperationLimit");
  }

  // Queued controls serialize in the native provider, and must not occupy the
  // storage mutation slot while startup awaits I/O. All retain effect provenance.
  // Workers routed calls own independent native futures/output gates. Keep
  // their mutation failure provenance, without the local storage mutation slot.
  // Body output can await backpressure while independent jobs mutate storage.
  // Ordinary SQL and fetch operations remain conservative mutations.
  const mutation =
    !name.startsWith("workers.rpc.") &&
    ![
      "storage.sync",
      "response.write",
      "response.finish",
      "workers.container.perform",
      "workers.rpc",
      "workers.fetch",
      "workers.fetch_stream",
      "workers.container.fetch",
      "workers.container.fetch_stream",
    ].includes(name) &&
    ![
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
      "workers.lifetime.response",
      "workers.callback.begin",
      "workers.callback.finish",
      "workers.callback.finish_sync",
      "workers.sql.size",
      "workers.kv.get",
      "workers.kv.list",
      "storage.read",
      "alarm.get",
      "var",
      "kv.get",
      "kv.list",
      "websocket.next",
      "response.read",
      "response.cancel",
      "response.closed",
      "container.capabilities",
      "container.running",
      "container.monitor",
      "container.exec.read_stdout",
      "container.exec.read_stderr",
      "container.exec.wait",
      "container.socket.read",
      "container.response.read",
      "container.response.cancel",
      "container.websocket.receive",
    ].includes(name);

  if (mutation && state.mutation) {
    reject("ConcurrentMutation");
  }

  const budgetFields = budgets.parameters(invocation, budget);
  const id = ++next;

  if (!Number.isSafeInteger(id)) {
    reject("OperationLimit");
  }

  if (completion) state.response = id;
  else state.count++;
  state.pending++;
  state.mutation ||= mutation;

  if (synchronous) {
    Atomics.store(synchronousHeader, 0, 0);
    send(
      {
        type: "operation",
        invocation,
        id,
        name,
        payload,
        callback_scope: continuation.callback_scope,
        synchronous: true,
        ...budgetFields,
      },
      undefined,
      true,
    );
    // The I/O worker may wake this slot with a reply, revocation, or failure.
    // Rust's event-loop watchdog remains authoritative; no worker heartbeats
    // pretend the application thread is responsive while it waits.
    Atomics.wait(synchronousHeader, 0, 0, config.synchronousTimeout);
    if (Atomics.load(synchronousHeader, 0) !== 1) exit(70);
    const length = Atomics.load(synchronousHeader, 1);
    if (length < 0 || length > config.valueLimit) exit(70);
    const result = Buffer.from(synchronousBytes.subarray(0, length)).toString(
      "utf8",
    );
    state.pending--;
    if (mutation) state.mutation = false;
    return result;
  }

  const promise = new Promise((resolve) => {
    pending.set(id, { invocation, mutation, resolve });
    send(
      {
        type: "operation",
        invocation,
        id,
        name,
        payload,
        callback_scope: continuation.callback_scope,
        ...budgetFields,
      },
      body,
    );
  });
  if (
    ![
      "cell.call",
      "workers.rpc.invoke",
      "response.read",
      "response.closed",
      "container.port.fetch",
      "container.port.fetch_stream",
      "container.response.read",
      "container.port.connect_websocket",
      "container.websocket.receive",
      "workers.fetch",
      "workers.fetch_stream",
      "workers.container.fetch",
      "workers.container.fetch_stream",
    ].includes(name)
  )
    return promise;
  let cancelled = false;
  const cancel = () => {
    if (cancelled || active.get(invocation) !== state || !pending.has(id))
      return;
    cancelled = true;
    send({ type: "cancel_operation", invocation, id });
  };
  return [promise, cancel];
}
const factory = makeRunner(
  operation,
  (invocation, name, payload) =>
    operation(invocation, name, payload, undefined, undefined, true),
  (...requested) => {
    const invocation = scope.getStore()?.invocation;
    const state = active.get(invocation);
    if (!state) reject("ContextExpired");
    if (
      requested.length &&
      (requested[0] === null || state.activation !== requested[0])
    )
      reject("ContextMismatch");
    if (
      requested.length > 1 &&
      scope.getStore()?.callback_scope !== requested[1]
    )
      reject("ContextMismatch");
    return invocation;
  },
  (value) => {
    const serializer = new Serializer();
    serializer.writeHeader();
    serializer.writeValue(value);
    const bytes = serializer.releaseBuffer();
    if (bytes.length > config.cloneLimit) reject("ValueLimit");
    return bytes;
  },
  (bytes) => {
    if (!(bytes instanceof Uint8Array) || bytes.length > config.cloneLimit)
      reject("ValueLimit");
    const deserializer = new Deserializer(bytes);
    if (!deserializer.readHeader()) reject("DataCloneError");
    return deserializer.readValue();
  },
  (invocation, callback_scope, callback, synchronous = false) => {
    if (scope.getStore()?.invocation !== invocation) reject("ContextMismatch");
    if (
      !Number.isSafeInteger(callback_scope) ||
      callback_scope < 1 ||
      typeof callback !== "function"
    )
      reject("ValidationError");
    return scope.run({ invocation, callback_scope, synchronous }, () => {
      const result = callback();
      if (synchronous && isPromise(result))
        throw new TypeError(
          "transactionSync callback must not return a Promise",
        );
      return result;
    });
  },
  addAbortListener,
  makeDelegates,
  makeRpcStreams,
  makeRpcWritables,
  makeRpcInputs,
);

// Revocation prevents new Cellar calls. It cannot undo effects already submitted
// to Rust, or stop arbitrary application work using Node's own OS capabilities.
function revoke(id) {
  const state = active.get(id);
  active.delete(id);
  runner?.cancel(id);
  budgets.revoke(id, state);

  for (const [key, operation] of pending) {
    if (operation.invocation === id) {
      pending.delete(key);
      operation.resolve([
        stringify({ ok: false, code: "ContextExpired", outcome: "unknown" }),
        undefined,
      ]);
    }
  }
}

function receive(message, body) {
  if (
    body !== undefined &&
    message.type !== "start" &&
    message.type !== "reply"
  )
    exit(70);
  switch (message.type) {
    case "start": {
      if (
        !runner ||
        active.has(message.id) ||
        active.size >= config.invocationLimit
      ) {
        exit(70);
      }

      const invocation = parse(message.payload);
      const state = {
        count: 0,
        pending: 0,
        mutation: false,
        activation: invocation.object,
        budgets: new Map(),
        workers_lifetimes: workersLifetimes && invocation.kind !== "rpc",
      };
      active.set(message.id, state);

      scope
        .run({ invocation: message.id }, async () => {
          let payload = "null";
          let failed = false,
            failurePayload = "null",
            outputBody;
          try {
            const result = await runner.dispatch(
              message.id,
              message.payload,
              body,
            );
            if (result instanceof Uint8Array) {
              if (validBody(result)) outputBody = result;
            } else payload = result;
          } catch (error) {
            failed = true;
            failurePayload = runner.failurePayload(error);
          }

          if (state.workers_lifetimes) {
            // A dispatcher result is not the end of its microtask queue. Wait
            // through a complete Node checkpoint, then re-drain any derived
            // native I/O or callback work registered by that queue.
            do {
              await runner.drain(message.id);
              await checkpoint();
            } while (
              active.get(message.id) === state &&
              (state.pending !== 0 || runner.background(message.id) === 0)
            );
            failed ||= runner.background(message.id) === 2;
          }
          // Cancellation may already have removed this invocation while it awaited.
          if (active.get(message.id) !== state) {
            return;
          }

          // Returning with outstanding SDK operations is a handler failure.
          failed ||=
            state.pending !== 0 ||
            state.budgets.size !== 0 ||
            Buffer.byteLength(payload) + (outputBody?.byteLength ?? 0) >
              config.valueLimit;
          revoke(message.id);
          send(
            {
              type: "complete",
              id: message.id,
              payload: failed ? failurePayload : payload,
              failed,
            },
            failed ? undefined : outputBody,
          );
        })
        .catch(() => exit(70));
      break;
    }

    case "budget_reply":
      budgets.receive(message);
      break;
    case "reply": {
      const operation = pending.get(message.id);

      if (!operation) {
        break;
      }

      if (operation.invocation !== message.invocation) {
        exit(70);
      }

      pending.delete(message.id);
      const state = active.get(operation.invocation);

      if (!state) {
        break;
      }

      state.pending--;

      if (operation.mutation) {
        state.mutation = false;
      }

      operation.resolve([message.payload, body]);
      break;
    }

    case "cancel":
      revoke(message.id);
      break;

    case "retire":
      runner.retire(message.id);
      break;

    default:
      exit(70);
  }
}

io.on("message", (message) => {
  try {
    const size =
      Buffer.byteLength(message.encoded) + (message.body?.byteLength ?? 0) + 8;
    Atomics.sub(queued, 0, BigInt(size));
    receive(parse(message.encoded), message.body);
  } catch {
    exit(70);
  }
});

// Import only after installing the authenticated channel and private SDK facade.
// Import/registration errors can contain application data and local paths. Let
// Rust report the fixed startup failure instead of Node printing their stacks.
try {
  const app = await import(pathToFileURL(config.entry).href);
  // CJS exports are exposed as default; ESM uses named exports, with an optional
  // default application object for compatibility with normal npm packages.
  runner = factory(
    typeof app.fetch === "function" || app.cells ? app : (app.default ?? app),
    config.descriptor,
    config.maximum,
    config.lifetimeLimit,
  );
} catch {
  exit(70);
}
send({ type: "ready" });

// Rust uses heartbeat absence to detect a blocked event loop, not slow async I/O.
setInterval(() => send({ type: "heartbeat" }), 25);
