// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { fixedLength, installWebExtensions, streamRequest } from "./streams.js";

export function httpLimit(context) {
  return context._sync("workers.http.limits").max_body_bytes;
}

export function webRuntime() {
  for (const name of [
    "Request",
    "Response",
    "Headers",
    "URL",
    "ReadableStream",
    "WritableStream",
    "TransformStream",
    "AbortController",
    "AbortSignal",
    "TextEncoder",
    "TextDecoder",
  ]) {
    if (typeof globalThis[name] !== "function")
      throw new Error(`HTTP requires the web runtime profile (${name})`);
  }
  installWebExtensions();
}

export function headerPairs(headers) {
  const pairs = [];
  let bytes = 0;
  const append = (key, value) => {
    bytes += new TextEncoder().encode(key + value).byteLength;
    if (pairs.length >= 128 || bytes > 32768)
      throw new RangeError("Workers HTTP headers exceed host limit");
    pairs.push([key, value]);
  };
  for (const [key, value] of headers) {
    if (key !== "set-cookie" || typeof headers.getSetCookie !== "function")
      append(key, value);
  }
  if (typeof headers.getSetCookie === "function") {
    for (const value of headers.getSetCookie()) append("set-cookie", value);
  }
  return pairs;
}

/** Infer framing only from the private FixedLengthStream brand. */
export function messageHeaders(message) {
  const length = fixedLength(message.body);
  if (length === undefined) return headerPairs(message.headers);
  const headers = new Headers(message.headers);
  headers.set("content-length", String(length));
  return headerPairs(headers);
}

// Consume at most the declared body limit, including streaming uploads/replies.
// The caller determines whether effects have already occurred before this read.
export async function bodyBytes(message, limit, signal = message.signal) {
  if (!message.body) return new Uint8Array();
  const reader = message.body.getReader();
  const chunks = [];
  let size = 0;
  const abort = () => {
    reader.cancel(signal.reason).catch(() => {});
  };
  signal?.addEventListener("abort", abort, { once: true });
  try {
    signal?.throwIfAborted();
    for (;;) {
      const { done, value } = await reader.read();
      signal?.throwIfAborted();
      if (done) break;
      if (!(value instanceof Uint8Array) || value.byteLength > limit - size) {
        reader.cancel("Workers HTTP body exceeds host limit").catch(() => {});
        throw new RangeError("Workers HTTP body exceeds host limit");
      }
      chunks.push(value.slice());
      size += value.byteLength;
    }
    const output = new Uint8Array(size);
    let offset = 0;
    for (const chunk of chunks) {
      output.set(chunk, offset);
      offset += chunk.byteLength;
    }
    return output;
  } finally {
    if (signal?.aborted) abort();
    signal?.removeEventListener("abort", abort);
    reader.releaseLock();
  }
}

export async function encodeRequest(request, context) {
  const headers = messageHeaders(request);
  const body = await bodyBytes(request, httpLimit(context));
  return {
    url: request.url,
    method: request.method,
    headers,
    body: context._encode(body),
  };
}

export function decodeRequest(frame) {
  webRuntime();
  return new Request(frame.url, {
    method: frame.method,
    headers: frame.headers,
    ...(frame.body.byteLength ? { body: frame.body } : {}),
    redirect: "manual",
  });
}

export function decodeResponse(frame, context) {
  webRuntime();
  return new Response(
    [204, 205, 304].includes(frame.status)
      ? null
      : typeof frame.body === "string"
        ? context._decode(frame.body)
        : frame.body,
    { status: frame.status, headers: frame.headers },
  );
}

export async function encodeResponse(response, context) {
  if (!(response instanceof Response) || response.webSocket) {
    throw new TypeError(
      "Expected an HTTP Response without a WebSocket upgrade",
    );
  }
  return {
    status: response.status,
    headers: messageHeaders(response),
    body: context._encode(await bodyBytes(response, httpLimit(context))),
  };
}

export async function fetchRequest(
  input,
  init,
  context,
  dispatch,
  beforeDispatch = async () => {},
  decode = decodeResponse,
) {
  webRuntime();
  const request = streamRequest(input, init);
  request.signal.throwIfAborted();
  await beforeDispatch();
  const frame = await encodeRequest(request, context);
  try {
    return decode(
      await dispatch(frame, request.signal),
      context,
      request.signal,
      request,
    );
  } catch (error) {
    if (error.code === "Cancelled" && request.signal.aborted)
      throw request.signal.reason;
    throw error;
  }
}
