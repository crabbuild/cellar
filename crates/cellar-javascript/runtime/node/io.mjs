// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// Private Node transport worker. It owns no SDK authority and never executes
// application effects. Rust validates every operation on the native invocation.
import { connect } from "node:net";
import { parentPort, workerData } from "node:worker_threads";
const { config, synchronousSlot, queueSlot } = workerData;
const queued = new BigInt64Array(queueSlot);
const parse = JSON.parse;
const stringify = JSON.stringify;
const exit = process.exit.bind(process);
const socket = connect({ host: "127.0.0.1", port: config.port });
const maximum = Math.min(config.valueLimit * 6 + 4096, config.bridgeLimit);
const headerSlot = new Int32Array(synchronousSlot, 0, 2);
const bytesSlot = new Uint8Array(synchronousSlot, 8);
let waiting;
function fail() {
  Atomics.store(headerSlot, 0, 2);
  Atomics.notify(headerSlot, 0);
  exit(70);
}
function receive(message) {
  if (waiting && message.type === "reply" && message.id === waiting.id) {
    if (message.invocation !== waiting.invocation || typeof message.payload !== "string") fail();
    const bytes = Buffer.from(message.payload);
    if (bytes.length > bytesSlot.length) fail();
    bytesSlot.set(bytes);
    Atomics.store(headerSlot, 1, bytes.length);
    waiting = undefined;
    Atomics.store(headerSlot, 0, 1);
    Atomics.notify(headerSlot, 0);
    return;
  }
  if (waiting && message.type === "cancel" && message.id === waiting.invocation) {
    receive({ type: "reply", id: waiting.id, invocation: waiting.invocation,
      payload: stringify({ ok: false, code: "ContextExpired", outcome: "unknown" }) });
  }
  const encoded = stringify(message);
  const size = Buffer.byteLength(encoded) + 4;
  if (Atomics.add(queued, 0, BigInt(size)) + BigInt(size) > BigInt(config.bridgeLimit)) fail();
  parentPort.postMessage({ encoded });
}
parentPort.on("message", message => {
  if (message.type === "synchronous") {
    if (waiting) fail();
    const value = parse(message.encoded);
    waiting = { id: value.id, invocation: value.invocation };
  } else if (message.type !== "send") fail();
  sendEncoded(message.encoded);
});
// Each JSON message has a four-byte big-endian length header. Bound both the
// individual frame and queued output before allocating and writing the frame.
function sendEncoded(encoded) {
  const bytes = Buffer.from(encoded);

  if (
    bytes.length > maximum ||
    socket.writableLength + bytes.length + 4 > config.bridgeLimit
  ) {
    fail();
  }

  const frame = Buffer.allocUnsafe(bytes.length + 4);
  frame.writeUInt32BE(bytes.length);
  bytes.copy(frame, 4);
  socket.write(frame, () => Atomics.sub(queued, 0, BigInt(frame.length)));
}

socket.on("error", fail);
socket.on("close", fail);
await new Promise(resolve => socket.once("connect", resolve));
socket.setNoDelay(true);
const hello = stringify({ type: "hello", secret: config.secret });
Atomics.add(queued, 0, BigInt(Buffer.byteLength(hello) + 4));
sendEncoded(hello);
delete config.secret;
parentPort.postMessage({ type: "connected" });

// Incrementally copy into one bounded frame; coalesced frames are dispatched
// individually instead of retaining an unbounded concatenated input buffer.
let header = Buffer.alloc(4);
let headerBytes = 0;
let frame = null;
let frameBytes = 0;

socket.on("data", chunk => {
  try {
    let offset = 0;

    while (offset < chunk.length) {
      if (!frame) {
        const n = Math.min(4 - headerBytes, chunk.length - offset);
        chunk.copy(header, headerBytes, offset, offset + n);
        headerBytes += n;
        offset += n;

        if (headerBytes < 4) {
          continue;
        }

        const length = header.readUInt32BE();

        if (!length || length > maximum) {
          fail();
        }

        frame = Buffer.allocUnsafe(length);
        frameBytes = 0;
        headerBytes = 0;
      }

      const n = Math.min(frame.length - frameBytes, chunk.length - offset);
      chunk.copy(frame, frameBytes, offset, offset + n);
      frameBytes += n;
      offset += n;

      if (frameBytes === frame.length) {
        const message = parse(frame.toString("utf8"));
        frame = null;
        receive(message);
      }
    }
  } catch {
    fail();
  }
});
