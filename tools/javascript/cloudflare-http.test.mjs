// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import test from "node:test";
import assert from "node:assert/strict";
import {
  decodeRequest,
  decodeResponse,
  encodeResponse,
  encodeRequest,
  fetchRequest,
  headerPairs,
} from "../../packages/cellar-cloudflare/http.js";
import { workersApplication } from "../../packages/cellar-cloudflare/index.js";
import {
  FixedLengthStream,
  IdentityTransformStream,
  streamRequest,
} from "../../packages/cellar-cloudflare/streams.js";

const context = {
  _sync: () => ({ max_body_bytes: 4 }),
  _encode: (bytes) => Buffer.from(bytes).toString("base64"),
  _decode: (bytes) => Uint8Array.from(Buffer.from(bytes, "base64")),
};

test("explicit incoming HTTP streams remain pull-based and preserve binary bodies beyond a frame", async () => {
  let pulls = 0;
  const source = new ReadableStream(
    {
      type: "bytes",
      pull(controller) {
        if (pulls === 33) return controller.close();
        const bytes = new Uint8Array(65536).map(
          (_, index) => (index + pulls) % 251,
        );
        pulls++;
        controller.enqueue(bytes);
      },
    },
    { highWaterMark: 0 },
  );
  const request = decodeRequest({
    url: "http://gateway/",
    method: "POST",
    headers: [],
    body: new Uint8Array(),
    body_stream: source,
  });
  assert.equal(pulls, 0);
  const bytes = new Uint8Array(await request.arrayBuffer());
  assert.equal(bytes.length, 33 * 65536);
  for (let chunk = 0; chunk < 33; chunk++)
    for (let index = 0; index < 65536; index++)
      assert.equal(bytes[chunk * 65536 + index], (index + chunk) % 251);
});

test("incoming request stream errors and cancellation propagate to the original source", async () => {
  const failure = new Error("incoming source failed");
  let pulls = 0;
  const request = decodeRequest({
    url: "http://gateway/",
    method: "POST",
    headers: [],
    body: new Uint8Array(),
    body_stream: new ReadableStream(
      {
        pull(controller) {
          if (pulls++) controller.error(failure);
          else controller.enqueue(Uint8Array.of(0, 255));
        },
      },
      { highWaterMark: 0 },
    ),
  });
  await assert.rejects(request.arrayBuffer(), (error) => error === failure);
  let reason;
  const other = decodeRequest({
    url: "http://gateway/",
    method: "POST",
    headers: [],
    body: new Uint8Array(),
    body_stream: new ReadableStream(
      {
        cancel(value) {
          reason = value;
        },
      },
      { highWaterMark: 0 },
    ),
  });
  await other.body.cancel(failure);
  assert.equal(reason, failure);
});

test("module HTTP codecs retain binary bodies and separate Set-Cookie fields", async () => {
  const request = decodeRequest({
    url: "http://cell/",
    method: "POST",
    headers: [],
    body: Uint8Array.of(0, 255),
  });
  assert.deepEqual(
    new Uint8Array(await request.arrayBuffer()),
    Uint8Array.of(0, 255),
  );
  const original = new Response(Uint8Array.of(0, 255), {
    headers: [
      ["set-cookie", "a=1"],
      ["set-cookie", "b=2"],
    ],
  });
  const frame = await encodeResponse(original, context);
  assert.deepEqual(
    frame.headers.filter(([name]) => name === "set-cookie"),
    [
      ["set-cookie", "a=1"],
      ["set-cookie", "b=2"],
    ],
  );
  const response = decodeResponse(frame, context);
  assert.equal(response.webSocket, null);
  assert.deepEqual(response.headers.getSetCookie(), ["a=1", "b=2"]);
  assert.deepEqual(
    new Uint8Array(await response.arrayBuffer()),
    Uint8Array.of(0, 255),
  );
});

test("oversized module replies cancel a still-open producer", async () => {
  let cancelled = false;
  const response = new Response(
    new ReadableStream({
      start(controller) {
        controller.enqueue(new Uint8Array(5));
      },
      cancel() {
        cancelled = true;
      },
    }),
  );
  await assert.rejects(encodeResponse(response, context), RangeError);
  assert.equal(cancelled, true);
});

test("HTTP header bounds use UTF-8 bytes and count distinct fields", () => {
  assert.throws(
    () => headerPairs(new Headers({ large: "é".repeat(16384) })),
    RangeError,
  );
  assert.throws(
    () =>
      headerPairs(
        new Headers(
          Array.from({ length: 129 }, (_, n) => [`header-${n}`, "v"]),
        ),
      ),
    RangeError,
  );
});

test("module registration refuses missing classes and undeclared container owners", () => {
  const module = { default: { fetch: () => new Response() } };
  assert.throws(
    () => workersApplication(module, { namespaces: { CELLS: "Missing" } }),
    /Missing Durable Object export/,
  );
  assert.throws(
    () => workersApplication(module, { containers: ["Missing"] }),
    /no declared namespace/,
  );
});

test("identity byte streams apply backpressure, preserve buffers and support BYOB EOF", async () => {
  const stream = new IdentityTransformStream();
  const writer = stream.writable.getWriter();
  const reader = stream.readable.getReader({ mode: "byob" });
  const bytes = Uint8Array.of(0, 255, 42);
  let finished = false;
  const writing = writer.write(bytes).then(() => {
    finished = true;
  });
  await Promise.resolve();
  assert.equal(finished, false);
  const chunk = await reader.read(new Uint8Array(8));
  await writing;
  assert.deepEqual(chunk.value, Uint8Array.of(0, 255, 42));
  assert.equal(bytes.byteLength, 3);
  const closing = writer.close();
  const end = await reader.read(new Uint8Array(8));
  await closing;
  assert.equal(end.done, true);
  assert.equal(end.value.byteLength, 0);
});

test("cancelling identity output rejects blocked writers with the same reason", async () => {
  const { readable, writable } = new IdentityTransformStream();
  const writer = writable.getWriter();
  const reason = new Error("consumer closed");
  const writing = writer.write(Uint8Array.of(7));
  const refused = assert.rejects(writing, (error) => error === reason);
  await readable.cancel(reason);
  await refused;
});

function deferred() {
  let resolve, reject;
  const promise = new Promise((done, fail) => {
    resolve = done;
    reject = fail;
  });
  return { promise, resolve, reject };
}

function alarmCell(invoke) {
  const app = workersApplication(
    { Counter: class {}, default: { fetch: () => new Response() } },
    { namespaces: { COUNTERS: "Counter" } },
  );
  const cell = new app.cells.Counter();
  cell.owner = () => ({ invoke: (call) => call({ alarm: invoke }) });
  return cell;
}

test("cancelling a queued alarm cannot let its successor overtake the active alarm", async () => {
  const entered = deferred();
  const release = deferred();
  let calls = 0;
  const cell = alarmCell(async () => {
    calls++;
    if (calls === 1) {
      entered.resolve();
      await release.promise;
    }
  });
  const alive = { _revoked: () => new Promise(() => {}) };
  const first = cell.alarm(null, alive);
  await entered.promise;
  const cancelled = deferred();
  const second = cell.alarm(null, { _revoked: () => cancelled.promise });
  const third = cell.alarm(null, alive);
  const reason = new Error("queued event revoked");
  cancelled.reject(reason);
  await assert.rejects(second, (error) => error === reason);
  assert.equal(calls, 1);
  release.resolve();
  await Promise.all([first, third]);
  assert.equal(calls, 2);
});

test("revoking an active alarm releases its queue without reporting success", async () => {
  const entered = deferred();
  const release = deferred();
  const revoked = deferred();
  let calls = 0;
  const cell = alarmCell(async () => {
    calls++;
    if (calls === 1) {
      entered.resolve();
      await release.promise;
    }
  });
  const first = cell.alarm(null, { _revoked: () => revoked.promise });
  await entered.promise;
  const reason = Object.assign(new Error("event revoked"), {
    code: "ContextExpired",
    outcome: "unknown",
  });
  revoked.reject(reason);
  await assert.rejects(first, (error) => error === reason);
  await cell.alarm(null, { _revoked: () => new Promise(() => {}) });
  assert.equal(calls, 2);
  release.resolve();
});

async function writeChunks(writable, chunks) {
  const writer = writable.getWriter();
  try {
    for (const chunk of chunks) await writer.write(chunk);
    await writer.close();
  } finally {
    writer.releaseLock();
  }
}

test("fixed-length byte streams validate number/bigint lengths without coercion", () => {
  for (const value of [
    0,
    3,
    0n,
    3n,
    Number.MAX_SAFE_INTEGER,
    9007199254740991n,
  ])
    assert.ok(new FixedLengthStream(value).readable);
  for (const value of [-1, -1n, 0.5, NaN, Infinity, 2 ** 53, 9007199254740992n])
    assert.throws(() => new FixedLengthStream(value), RangeError);
  for (const value of [undefined, null, "3", {}, true])
    assert.throws(() => new FixedLengthStream(value), TypeError);
});

test("fixed-length streams preserve binary chunks, BYOB EOF and backpressure", async () => {
  const { readable, writable } = new FixedLengthStream(3n);
  const bytes = Uint8Array.of(0, 255, 42);
  const writing = writeChunks(writable, [
    bytes.subarray(0, 1),
    bytes.subarray(1),
  ]);
  const reader = readable.getReader({ mode: "byob" });
  const result = [];
  for (;;) {
    const { value, done } = await reader.read(new Uint8Array(2));
    result.push(...value);
    if (done) break;
  }
  await writing;
  reader.releaseLock();
  assert.deepEqual(result, [...bytes]);
  assert.equal(bytes.byteLength, 3);

  const empty = new FixedLengthStream(0);
  await Promise.all([
    writeChunks(empty.writable, []),
    new Response(empty.readable)
      .arrayBuffer()
      .then((value) => assert.equal(value.byteLength, 0)),
  ]);
});

test("fixed-length underflow, overflow and nonbyte writes reject both ends", async () => {
  for (const [chunks, message] of [
    [[Uint8Array.of(1)], /too few bytes/],
    [[Uint8Array.of(1, 2, 3)], /too many bytes/],
    [["no bytes"], /typed byte buffer/],
  ]) {
    const { readable, writable } = new FixedLengthStream(2);
    await Promise.all([
      assert.rejects(writeChunks(writable, chunks), message),
      assert.rejects(new Response(readable).arrayBuffer(), message),
    ]);
  }
});

test("fixed-length cancellation and producer abort retain their reason", async () => {
  const reason = new Error("fixed-length consumer closed");
  const stream = new FixedLengthStream(3);
  const writer = stream.writable.getWriter();
  let finished = false;
  const writing = writer.write(Uint8Array.of(1)).then(() => {
    finished = true;
  });
  const refused = assert.rejects(writing, (error) => error === reason);
  await Promise.resolve();
  assert.equal(finished, false);
  await stream.readable.cancel(reason);
  await refused;
  writer.releaseLock();

  const aborted = new FixedLengthStream(3);
  const reading = assert.rejects(
    new Response(aborted.readable).arrayBuffer(),
    (error) => error === reason,
  );
  await aborted.writable.abort(reason);
  await reading;
});

test("HTTP codecs infer only privately branded stream lengths and enforce bytes before dispatch", async () => {
  for (const request of [false, true]) {
    const { readable, writable } = new FixedLengthStream(3);
    const message = request
      ? new Request("http://cell/", {
          method: "POST",
          body: readable,
          duplex: "half",
          headers: { "content-length": "999" },
        })
      : new Response(readable, { headers: { "content-length": "999" } });
    const [frame] = await Promise.all([
      (request ? encodeRequest : encodeResponse)(message, context),
      writeChunks(writable, [Uint8Array.of(0, 255, 42)]),
    ]);
    assert.equal(new Headers(frame.headers).get("content-length"), "3");
    assert.equal(frame.body, "AP8q");
    assert.equal(message.headers.get("content-length"), "999");
  }
  const fake = new ReadableStream({
    start(controller) {
      controller.close();
    },
  });
  fake.fixedLength = 9;
  const frame = await encodeResponse(new Response(fake), context);
  assert.equal(new Headers(frame.headers).has("content-length"), false);
  const { readable, writable } = new FixedLengthStream(3);
  await Promise.all([
    assert.rejects(
      encodeRequest(
        new Request("http://cell/", {
          method: "POST",
          body: readable,
          duplex: "half",
        }),
        context,
      ),
      /too few bytes/,
    ),
    assert.rejects(writeChunks(writable, [Uint8Array.of(1)]), /too few bytes/),
  ]);
});

test("native fetch request conversion retains length through body proxies and reads init once", async () => {
  for (const fromRequest of [false, true]) {
    const stream = new FixedLengthStream(3);
    let reads = 0;
    const init = {
      method: "POST",
      duplex: "half",
      get body() {
        reads++;
        return stream.readable;
      },
    };
    const input = fromRequest
      ? new Request("http://cell/", init)
      : "http://cell/";
    const uploading = writeChunks(stream.writable, [Uint8Array.of(0, 255, 42)]);
    const dispatch = async (frame) => {
      assert.equal(new Headers(frame.headers).get("content-length"), "3");
      assert.equal(frame.body, "AP8q");
      return { status: 204, headers: [], body: "" };
    };
    const [response] = await Promise.all([
      fetchRequest(input, fromRequest ? undefined : init, context, dispatch),
      uploading,
    ]);
    assert.equal(response.status, 204);
    assert.equal(reads, 1);
  }
});

test("stream request conversion preserves native init validation and accessor receivers", async () => {
  assert.throws(() => streamRequest("http://cell/", "invalid"), TypeError);
  class Options {
    #body;
    constructor(body) {
      this.#body = body;
    }
    method = "POST";
    duplex = "half";
    get body() {
      return this.#body;
    }
  }
  const stream = new FixedLengthStream(1);
  const request = streamRequest("http://cell/", new Options(stream.readable));
  const [, frame] = await Promise.all([
    writeChunks(stream.writable, [Uint8Array.of(42)]),
    encodeRequest(request, context),
  ]);
  assert.equal(new Headers(frame.headers).get("content-length"), "1");
  assert.equal(frame.body, "Kg==");
});
