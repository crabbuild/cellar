// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// Injected by the Rust bootstrap. No application code runs before the private
// transport and context factory have captured their dependencies.
import { readFileSync } from "node:fs";
import { Worker } from "node:worker_threads";
import { Serializer, Deserializer } from "node:v8";
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
const scope = new AsyncLocalStorage();
// Track invocation authority separately from operations waiting for Rust replies.
const active = new Map();
const pending = new Map();
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
const io = new Worker(new URL(`data:text/javascript,${encodeURIComponent(ioSource)}`), {
  workerData: { config, synchronousSlot, queueSlot: queued.buffer },
});
io.on("error", () => exit(70));
io.on("exit", () => exit(70));
function send(value, synchronous = false) {
  const encoded = stringify(value);
  const size = Buffer.byteLength(encoded) + 4;
  if (size > maximum + 4 || Atomics.add(queued, 0, BigInt(size)) + BigInt(size) > BigInt(config.bridgeLimit)) exit(70);
  io.postMessage({ type: synchronous ? "synchronous" : "send", encoded });
}
await new Promise(resolve => io.once("message", message => {
  if (message.type !== "connected") exit(70);
  resolve();
}));
delete config.secret;

function reject(code) {
  throw new Error(code);
}

// AsyncLocalStorage binds promises, timers and Node I/O continuations to their
// invocation. Rust repeats the liveness/limit checks before accepting effects.
function operation(invocation, name, payload, synchronous = false) {
  if (scope.getStore() !== invocation) {
    reject("ContextMismatch");
  }

  const state = active.get(invocation);

  if (!state) {
    reject("ContextExpired");
  }

  if (synchronous && !["clock.now", "storage.read", "storage.apply", "storage.sql", "alarm.get",
      "workers.crypto.random", "workers.crypto.uuid", "workers.identity", "workers.namespace", "workers.sql.exec", "workers.sql.next", "workers.sql.close", "workers.sql.size",
      "workers.kv.get", "workers.kv.put", "workers.kv.delete", "workers.kv.list", "workers.kv.delete_all"].includes(name)) {
    reject("UnsupportedOperation");
  }

  if (Buffer.byteLength(payload) > config.valueLimit) {
    reject("ValueLimit");
  }

  if (
    state.count >= config.operationLimit ||
    state.pending >= config.pendingLimit
  ) {
    reject("OperationLimit");
  }

  // Only known reads may overlap a mutation. SQL and nested cell fetches are
  // conservatively treated as mutations, even when a caller expects a read.
  const mutation = ![
    "clock.now",
    "workers.crypto.random",
    "workers.crypto.uuid",
    "workers.identity",
    "workers.namespace",
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
    "container.capabilities",
    "container.running",
    "container.monitor",
    "container.exec.read_stdout",
    "container.exec.read_stderr",
    "container.exec.wait",
    "container.socket.read",
    "container.websocket.receive",
  ].includes(name);

  if (mutation && state.mutation) {
    reject("ConcurrentMutation");
  }

  const id = ++next;

  if (!Number.isSafeInteger(id)) {
    reject("OperationLimit");
  }

  state.count++;
  state.pending++;
  state.mutation ||= mutation;

  if (synchronous) {
    Atomics.store(synchronousHeader, 0, 0);
    send({ type: "operation", invocation, id, name, payload, synchronous: true }, true);
    // The I/O worker may wake this slot with a reply, revocation, or failure.
    // Rust's event-loop watchdog remains authoritative; no worker heartbeats
    // pretend the application thread is responsive while it waits.
    Atomics.wait(synchronousHeader, 0, 0, config.synchronousTimeout);
    if (Atomics.load(synchronousHeader, 0) !== 1) exit(70);
    const length = Atomics.load(synchronousHeader, 1);
    if (length < 0 || length > config.valueLimit) exit(70);
    const result = Buffer.from(synchronousBytes.subarray(0, length)).toString("utf8");
    state.pending--;
    if (mutation) state.mutation = false;
    return result;
  }

  return new Promise(resolve => {
    pending.set(id, { invocation, mutation, resolve });
    send({ type: "operation", invocation, id, name, payload });
  });
}
const factory = makeRunner(operation, (invocation, name, payload) => operation(invocation, name, payload, true), (...requested) => {
  const invocation = scope.getStore();
  const state = active.get(invocation);
  if (!state) reject("ContextExpired");
  if (requested.length && (requested[0] === null || state.activation !== requested[0])) reject("ContextMismatch");
  return invocation;
}, value => {
  const serializer = new Serializer();
  serializer.writeHeader();
  serializer.writeValue(value);
  const bytes = serializer.releaseBuffer();
  if (bytes.length > config.cloneLimit) reject("ValueLimit");
  return bytes;
}, bytes => {
  if (!(bytes instanceof Uint8Array) || bytes.length > config.cloneLimit) reject("ValueLimit");
  const deserializer = new Deserializer(bytes);
  if (!deserializer.readHeader()) reject("DataCloneError");
  return deserializer.readValue();
});

// Revocation prevents new Cellar calls. It cannot undo effects already submitted
// to Rust, or stop arbitrary application work using Node's own OS capabilities.
function revoke(id) {
  active.delete(id);

  for (const [key, operation] of pending) {
    if (operation.invocation === id) {
      pending.delete(key);
      operation.resolve(
        stringify({ ok: false, code: "ContextExpired", outcome: "unknown" }),
      );
    }
  }
}

function receive(message) {
  switch (message.type) {
    case "start": {
      if (
        !runner ||
        active.has(message.id) ||
        active.size >= config.invocationLimit
      ) {
        exit(70);
      }

      const state = { count: 0, pending: 0, mutation: false, activation: parse(message.payload).object };
      active.set(message.id, state);

      scope
        .run(message.id, async () => {
          let payload = "null";
          let failed = false;

          try {
            payload = await runner.dispatch(message.id, message.payload);
          } catch {
            failed = true;
          }

          // Cancellation may already have removed this invocation while it awaited.
          if (active.get(message.id) !== state) {
            return;
          }

          // Returning with outstanding SDK operations is a handler failure.
          failed ||=
            state.pending !== 0 ||
            Buffer.byteLength(payload) > config.valueLimit;
          revoke(message.id);
          send({
            type: "complete",
            id: message.id,
            payload: failed ? "null" : payload,
            failed,
          });
        })
        .catch(() => exit(70));
      break;
    }

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

      operation.resolve(message.payload);
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

io.on("message", message => {
  try {
    const size = Buffer.byteLength(message.encoded) + 4;
    Atomics.sub(queued, 0, BigInt(size));
    receive(parse(message.encoded));
  } catch { exit(70); }
});

// Import only after installing the authenticated channel and private SDK facade.
const app = await import(pathToFileURL(config.entry).href);

// CJS exports are exposed as default; ESM uses named exports, with an optional
// default application object for compatibility with normal npm packages.
runner = factory(
  typeof app.fetch === "function" || app.cells ? app : (app.default ?? app),
  config.descriptor,
  config.maximum,
);
send({ type: "ready" });

// Rust uses heartbeat absence to detect a blocked event loop, not slow async I/O.
setInterval(() => send({ type: "heartbeat" }), 25);
