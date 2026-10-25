// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// Node 24 client: no npm WebSocket or SSE client dependency is needed.
import assert from "node:assert/strict";
import { once } from "node:events";

const origin = new URL(process.argv[2] ?? "http://127.0.0.1:9876");

async function state() {
  const response = await fetch(new URL("/rooms/demo/state", origin), {
    signal: AbortSignal.timeout(10000),
  });
  assert.equal(response.status, 200);
  return response.json();
}

async function echo(path) {
  const url = new URL(path, origin);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  const socket = new WebSocket(url);
  socket.binaryType = "arraybuffer";

  try {
    await once(socket, "open", { signal: AbortSignal.timeout(5000) });

    for (const data of ["hello 🦀", new Uint8Array([0, 128, 255])]) {
      // Subscribe before sending so a fast local echo cannot be missed.
      const pending = once(socket, "message", {
        signal: AbortSignal.timeout(5000),
      });
      socket.send(data);
      const [event] = await pending;

      if (typeof data === "string") {
        assert.equal(event.data, data);
      } else {
        assert.deepEqual(new Uint8Array(event.data), data);
      }
    }
  } finally {
    if (socket.readyState !== WebSocket.CLOSED) {
      const closed = once(socket, "close", {
        signal: AbortSignal.timeout(5000),
      });
      socket.close(1000, "complete");
      await closed;
    }
  }
}

// The same native host serves the browser client and forwards API routes.
for (const [path, contentType] of [
  ["/", "text/html"],
  ["/client.js", "javascript"],
  ["/style.css", "text/css"],
]) {
  const asset = await fetch(new URL(path, origin), {
    signal: AbortSignal.timeout(10000),
  });
  assert.equal(asset.status, 200, `Missing browser asset: ${path}`);
  assert.ok(asset.headers.get("content-type").includes(contentType));
  await asset.arrayBuffer();
}

const response = await fetch(new URL("/events", origin), {
  signal: AbortSignal.timeout(10000),
});
assert.equal(response.status, 200);
assert.equal(response.headers.get("content-type"), "text/event-stream");

// HTTP chunks may split or combine SSE records. Parse using the blank-line
// event delimiter rather than assuming each network read is a complete event.
const records = (await response.text()).trim().split(/\r?\n\r?\n/);
assert.equal(records.length, 4);

for (let step = 1; step <= 3; step++) {
  assert.equal(
    records[step - 1],
    `event: progress\nid: ${step}\ndata: {"step":${step}}`,
  );
}

assert.equal(records[3], "event: done\ndata: complete");
assert.equal(
  (await fetch(new URL("/ws", origin), { signal: AbortSignal.timeout(10000) }))
    .status,
  426,
);

const before = await state();
await echo("/ws");
await echo("/rooms/demo/ws");
assert.equal((await state()).messages, before.messages + 2);

console.log(
  "PASS SSE progress, worker/cell text and binary echo, durable message count",
);
