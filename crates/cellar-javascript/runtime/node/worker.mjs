// Injected by the Rust bootstrap. No application code runs before the private
// transport and context factory have captured their dependencies.
import { readFileSync } from 'node:fs';
import { connect } from 'node:net';
import { AsyncLocalStorage } from 'node:async_hooks';
import { addAbortListener } from 'node:events';
import { pathToFileURL } from 'node:url';

if (Number(process.versions.node.split('.')[0]) < 24) {
  throw new Error('Cellar requires Node 24 or later');
}

// Bootstrap configuration arrives on stdin, keeping the authentication secret
// out of argv and the environment. stdout/stderr remain available to the app.
const config = JSON.parse(readFileSync(0, 'utf8'));
const parse = JSON.parse,
  stringify = JSON.stringify;
const scope = new AsyncLocalStorage();
const active = new Map(),
  pending = new Map();
const budgets = makeBudgets(process.hrtime.bigint.bind(process.hrtime), scope.getStore.bind(scope), active,
  config.pendingLimit, (value) => send(value), () => {
    const id = ++next;
    if (!Number.isSafeInteger(id)) reject('OperationLimit');
    return id;
  });
let next = 0,
  runner;
const socket = connect({ host: '127.0.0.1', port: config.port });
const exit = process.exit.bind(process);
const maximum = Math.min(config.valueLimit * 6 + 4096, config.bridgeLimit);

// Two network-order lengths precede control JSON and an optional raw attachment.
// Presence is explicit so an encoded empty message is not a missing body.
function send(value, body) {
  const bytes = Buffer.from(stringify({ ...value, has_body: body !== undefined }));
  const bodyLength = body?.byteLength ?? 0;

  if (
    bytes.length + bodyLength > maximum ||
    socket.writableLength + bytes.length + bodyLength + 8 > config.bridgeLimit
  ) {
    exit(70);
  }

  const frame = Buffer.allocUnsafe(bytes.length + bodyLength + 8);
  frame.writeUInt32BE(bytes.length);
  frame.writeUInt32BE(bodyLength, 4);
  bytes.copy(frame, 8);
  if (body !== undefined) frame.set(body, 8 + bytes.length);
  socket.write(frame);
}

socket.on('error', () => exit(70));
socket.on('close', () => exit(70));
await new Promise(resolve => socket.once('connect', resolve));
socket.setNoDelay(true);
send({ type: 'hello', secret: config.secret });
delete config.secret;

function reject(code) {
  throw new Error(code);
}

function validBody(body) {
  return body instanceof Uint8Array && body.buffer instanceof ArrayBuffer &&
    !body.buffer.resizable && !body.buffer.detached;
}

// AsyncLocalStorage binds promises, timers and Node I/O continuations to their
// invocation. Rust repeats the liveness/limit checks before accepting effects.
const factory = makeRunner((invocation, name, payload, body, budget) => {
  if (scope.getStore() !== invocation) reject('ContextMismatch');

  const state = active.get(invocation);
  if (!state) reject('ContextExpired');
  if (name === '$check') return;
  if (name === '$rpc_budget') return budgets.start(invocation, state);
  if ((name === 'cell.call') !== (body !== undefined)) reject('ValidationError');
  if (body !== undefined && !validBody(body)) {
    reject('ValidationError');
  }
  if (Buffer.byteLength(payload) + (body?.byteLength ?? 0) > config.valueLimit) reject('ValueLimit');

  if (
    state.count >= config.operationLimit ||
    state.pending >= config.pendingLimit
  ) {
    reject('OperationLimit');
  }

  // Only known reads may overlap a mutation. SQL and nested cell fetches are
  // conservatively treated as mutations, even when a caller expects a read.
  const mutation = ![
    'clock.now',
    'storage.read',
    'alarm.get',
    'var',
    'kv.get',
    'kv.list',
  ].includes(name);

  if (mutation && state.mutation) reject('ConcurrentMutation');
  const budgetFields = budgets.parameters(invocation, budget);

  const id = ++next;
  if (!Number.isSafeInteger(id)) reject('OperationLimit');

  state.count++;
  state.pending++;
  state.mutation ||= mutation;

  const promise = new Promise(resolve => {
    pending.set(id, { invocation, mutation, resolve });
    send({ type: 'operation', invocation, id, name, payload, ...budgetFields }, body);
  });
  if (name !== 'cell.call') return promise;

  // Only admission creates this closure. It retains one operation identity,
  // never the ability to submit effects or cancel a caller-selected operation.
  let cancelled = false;
  const cancel = () => {
    if (cancelled || active.get(invocation) !== state || !pending.has(id)) return;
    cancelled = true;
    send({ type: 'cancel_operation', invocation, id });
  };
  return [promise, cancel];
}, addAbortListener);

// Revocation prevents new Cellar calls. It cannot undo effects already submitted
// to Rust, or stop arbitrary application work using Node's own OS capabilities.
function revoke(id) {
  const state = active.get(id);
  active.delete(id);
  budgets.revoke(id, state);

  for (const [key, operation] of pending) {
    if (operation.invocation === id) {
      pending.delete(key);
      operation.resolve([
        stringify({ ok: false, code: 'ContextExpired', outcome: 'unknown' }),
        undefined,
      ]);
    }
  }
}

function receive(message, body) {
  // Message consumers opt into attachments when their invocation path supports
  // them. Control messages never accept an undeclared body.
  if (body !== undefined && message.type !== 'start' && message.type !== 'reply') exit(70);
  switch (message.type) {
    case 'start': {
      if (
        !runner ||
        active.has(message.id) ||
        active.size >= config.invocationLimit
      ) {
        exit(70);
      }

      const state = { count: 0, pending: 0, mutation: false, budgets:new Map() };
      active.set(message.id, state);

      scope.run(message.id, async () => {
        let payload = 'null',
          failurePayload = 'null',
          failed = false,
          outputBody;

        try {
          const result = await runner.dispatch(message.id, message.payload, body);
          if (result instanceof Uint8Array) {
            // Missing bytes represent InvalidResponse at the native RPC
            // boundary. Do not kill unrelated calls for a bad business reply.
            if (validBody(result)) outputBody = result;
          }
          else payload = result;
        } catch (error) {
          failed = true;
          failurePayload = runner.failurePayload(error);
        }

        // Cancellation may already have removed this invocation while it awaited.
        if (active.get(message.id) !== state) return;

        // Returning with outstanding SDK operations is a handler failure.
        failed ||=
          state.pending !== 0 || state.budgets.size !== 0 || Buffer.byteLength(payload) + (outputBody?.byteLength ?? 0) > config.valueLimit;
        revoke(message.id);
        send({
          type: 'complete',
          id: message.id,
          payload: failed ? failurePayload : payload,
          failed,
        }, failed ? undefined : outputBody);
      }).catch(() => exit(70));
      break;
    }

    case 'budget_reply':
      budgets.receive(message);
      break;

    case 'reply': {
      const operation = pending.get(message.id);
      if (!operation) break;
      if (operation.invocation !== message.invocation) exit(70);

      pending.delete(message.id);
      const state = active.get(operation.invocation);
      if (!state) break;

      state.pending--;
      if (operation.mutation) state.mutation = false;
      operation.resolve([message.payload, body]);
      break;
    }

    case 'cancel':
      revoke(message.id);
      break;

    case 'retire':
      runner.retire(message.id);
      break;

    default:
      exit(70);
  }
}

// Incrementally copy into one bounded frame; coalesced frames are dispatched
// individually instead of retaining an unbounded concatenated input buffer.
const metadataDecoder = new TextDecoder('utf-8', { fatal: true });
let header = Buffer.alloc(8),
  headerBytes = 0,
  frame = null,
  frameBytes = 0,
  metadataLength = 0;

socket.on('data', chunk => {
  try {
    let offset = 0;

    while (offset < chunk.length) {
      if (!frame) {
        const n = Math.min(8 - headerBytes, chunk.length - offset);
        chunk.copy(header, headerBytes, offset, offset + n);
        headerBytes += n;
        offset += n;

        if (headerBytes < 8) continue;

        metadataLength = header.readUInt32BE();
        const bodyLength = header.readUInt32BE(4);
        const length = metadataLength + bodyLength;
        if (!metadataLength || length > maximum) exit(70);

        frame = Buffer.allocUnsafe(length);
        frameBytes = 0;
        headerBytes = 0;
      }

      const n = Math.min(frame.length - frameBytes, chunk.length - offset);
      chunk.copy(frame, frameBytes, offset, offset + n);
      frameBytes += n;
      offset += n;

      if (frameBytes === frame.length) {
        const message = parse(metadataDecoder.decode(frame.subarray(0, metadataLength)));
        if (typeof message.has_body !== 'boolean') exit(70);
        if (!message.has_body && metadataLength !== frame.length) exit(70);
        const body = message.has_body ? frame.subarray(metadataLength) : undefined;
        frame = null;
        receive(message, body);
      }
    }
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
    typeof app.fetch === 'function' || app.cells ? app : (app.default ?? app),
    config.descriptor,
    config.maximum,
  );
} catch {
  exit(70);
}
send({ type: 'ready' });

// Rust uses heartbeat absence to detect a blocked event loop, not slow async I/O.
setInterval(() => send({ type: 'heartbeat' }), 25);
