// Injected by the Rust bootstrap. No application code runs before the private
// transport and context factory have captured their dependencies.
import {readFileSync} from 'node:fs';
import {connect} from 'node:net';
import {AsyncLocalStorage} from 'node:async_hooks';
import {pathToFileURL} from 'node:url';

if (Number(process.versions.node.split('.')[0]) < 24) throw new Error('Cellar requires Node 24 or later');
const config = JSON.parse(readFileSync(0, 'utf8'));
const parse = JSON.parse, stringify = JSON.stringify;
const scope = new AsyncLocalStorage();
const active = new Map(), pending = new Map();
let next = 0, runner;
const socket = connect({host: '127.0.0.1', port: config.port});
const exit = process.exit.bind(process);
const maximum = Math.min(config.valueLimit * 6 + 4096, config.bridgeLimit);
function send(value) {
  const bytes = Buffer.from(stringify(value));
  if (bytes.length > maximum || socket.writableLength + bytes.length + 4 > config.bridgeLimit) exit(70);
  const frame = Buffer.allocUnsafe(bytes.length + 4);
  frame.writeUInt32BE(bytes.length); bytes.copy(frame, 4);
  socket.write(frame);
}
socket.on('error', () => exit(70));
socket.on('close', () => exit(70));
await new Promise(resolve => socket.once('connect', resolve));
socket.setNoDelay(true);
send({type: 'hello', secret: config.secret});
delete config.secret;
function reject(code) { throw new Error(code); }
const factory = makeRunner((invocation, name, payload) => {
  if (scope.getStore() !== invocation) reject('ContextMismatch');
  const state = active.get(invocation);
  if (!state) reject('ContextExpired');
  if (Buffer.byteLength(payload) > config.valueLimit) reject('ValueLimit');
  if (state.count >= config.operationLimit || state.pending >= config.pendingLimit) reject('OperationLimit');
  const mutation = !['clock.now','storage.read','alarm.get','var','kv.get','kv.list'].includes(name);
  if (mutation && state.mutation) reject('ConcurrentMutation');
  const id = ++next;
  if (!Number.isSafeInteger(id)) reject('OperationLimit');
  state.count++; state.pending++; state.mutation ||= mutation;
  return new Promise(resolve => {
    pending.set(id, {invocation, mutation, resolve});
    send({type: 'operation', invocation, id, name, payload});
  });
});
function revoke(id) {
  active.delete(id);
  for (const [key, operation] of pending) if (operation.invocation === id) {
    pending.delete(key);
    // An operation already submitted to Rust has an unknown outcome on loss.
    operation.resolve(stringify({ok:false, code:'ContextExpired', outcome:'unknown'}));
  }
}
function receive(message) {
  switch (message.type) {
    case 'start': {
      if (!runner || active.has(message.id) || active.size >= config.invocationLimit) exit(70);
      const state = {count: 0, pending: 0, mutation: false};
      active.set(message.id, state);
      scope.run(message.id, async () => {
        let payload = 'null', failed = false;
        try { payload = await runner.dispatch(message.id, message.payload); }
        catch { failed = true; }
        if (active.get(message.id) !== state) return;
        failed ||= state.pending !== 0 || Buffer.byteLength(payload) > config.valueLimit;
        revoke(message.id);
        send({type: 'complete', id: message.id, payload: failed ? 'null' : payload, failed});
      }).catch(() => exit(70));
      break;
    }
    case 'reply': {
      const operation = pending.get(message.id);
      if (!operation) break;
      if (operation.invocation !== message.invocation) exit(70);
      pending.delete(message.id);
      const state = active.get(operation.invocation);
      if (!state) break;
      state.pending--;
      if (operation.mutation) state.mutation = false;
      operation.resolve(message.payload);
      break;
    }
    case 'cancel': revoke(message.id); break;
    case 'retire': runner.retire(message.id); break;
    default: exit(70);
  }
}
// Incrementally copy into one bounded frame; coalesced frames are dispatched
// individually instead of retaining an unbounded concatenated input buffer.
let header = Buffer.alloc(4), headerBytes = 0, frame = null, frameBytes = 0;
socket.on('data', chunk => {
  try {
    let offset = 0;
    while (offset < chunk.length) {
      if (!frame) {
        const n = Math.min(4 - headerBytes, chunk.length - offset);
        chunk.copy(header, headerBytes, offset, offset + n); headerBytes += n; offset += n;
        if (headerBytes < 4) continue;
        const length = header.readUInt32BE();
        if (!length || length > maximum) exit(70);
        frame = Buffer.allocUnsafe(length); frameBytes = 0; headerBytes = 0;
      }
      const n = Math.min(frame.length - frameBytes, chunk.length - offset);
      chunk.copy(frame, frameBytes, offset, offset + n); frameBytes += n; offset += n;
      if (frameBytes === frame.length) {
        const message = parse(frame.toString('utf8')); frame = null;
        receive(message);
      }
    }
  } catch { exit(70); }
});
const app = await import(pathToFileURL(config.entry).href);
// CJS exports are exposed as default; ESM uses named exports, with an optional
// default application object for compatibility with normal npm packages.
runner = factory(typeof app.fetch === 'function' || app.cells ? app : (app.default ?? app), config.descriptor, config.maximum);
send({type: 'ready'});
setInterval(() => send({type:'heartbeat'}), 25);
