// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

/**
 * HTTP and streaming response helpers shared by Cellar's embedded V8 and Node.js backends.
 *
 * The native bridge carries bodies as base64 strings. Handlers receive decoded
 * bytes, while these builders encode outgoing bodies into the wire format.
 * Only ECMAScript builtins are used, so the helpers also work without Buffer,
 * TextEncoder, or browser Request/Response classes.
 */

/** Encode bytes in groups of three, padding the final group when necessary. */
export function bytesToBase64(bytes) {
  const alphabet =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  let result = "";

  for (let i = 0; i < bytes.length; i += 3) {
    // Combine up to three bytes into a 24-bit value, then emit four 6-bit digits.
    const value =
      (bytes[i] << 16) | ((bytes[i + 1] ?? 0) << 8) | (bytes[i + 2] ?? 0);

    result +=
      alphabet[value >>> 18] +
      alphabet[(value >>> 12) & 63] +
      (i + 1 < bytes.length ? alphabet[(value >>> 6) & 63] : "=") +
      (i + 2 < bytes.length ? alphabet[value & 63] : "=");
  }

  return result;
}

/** Encode text as UTF-8, replacing unpaired UTF-16 surrogates with U+FFFD. */
export function utf8(text) {
  // Percent encoding exposes UTF-8 bytes without requiring a host text encoder.
  // Well-formed text prevents encodeURIComponent from rejecting lone surrogates.
  const encoded = encodeURIComponent(text.toWellFormed());
  const bytes = [];

  for (let i = 0; i < encoded.length; i++) {
    if (encoded[i] === "%") {
      bytes.push(parseInt(encoded.slice(i + 1, i + 3), 16));
      i += 2;
    } else {
      // Characters left unescaped by encodeURIComponent are single-byte ASCII.
      bytes.push(encoded.charCodeAt(i));
    }
  }

  return new Uint8Array(bytes);
}

/** Build the encoded response object returned by an application or cell handler. */
export function response(body = "", { status = 200, headers = [] } = {}) {
  return {
    status,
    headers,
    body: bytesToBase64(typeof body === "string" ? utf8(body) : body),
  };
}

/** Serialize JSON and prepend its content-type header to the supplied headers. */
export function jsonResponse(value, { status = 200, headers = [] } = {}) {
  return response(JSON.stringify(value), {
    status,
    headers: [["content-type", "application/json"], ...headers],
  });
}

/** Build an encoded request for ctx.cell(binding, name).fetch(...). */
export function request(url, { method = "GET", headers = [], body = "" } = {}) {
  return {
    url,
    method,
    headers,
    body: bytesToBase64(typeof body === "string" ? utf8(body) : body),
  };
}

/** Stream bounded chunks under the original invocation, with backpressure. */
export function streamResponse(chunks, { status = 200, headers = [] } = {}) {
  return { status, headers, body: "", chunks };
}

/** Format one SSE event; use with streamResponse or sseResponse. */
export function sseEvent(data, { event, id, retry } = {}) {
  let result = "";
  for (const [key, value] of [
    ["event", event],
    ["id", id],
  ]) {
    if (value !== undefined) {
      if (typeof value !== "string" || /[\r\n\0]/.test(value))
        throw new TypeError("Invalid SSE field");
      result += `${key}: ${value}\n`;
    }
  }
  if (retry !== undefined) {
    if (!Number.isSafeInteger(retry) || retry < 0)
      throw new TypeError("Invalid SSE retry");
    result += `retry: ${retry}\n`;
  }
  return (
    result +
    String(data)
      .replace(/\r\n|\r/g, "\n")
      .split("\n")
      .map(line => `data: ${line}\n`)
      .join("") +
    "\n"
  );
}

export function sseResponse(events, options = {}) {
  return streamResponse(events, {
    ...options,
    headers: [
      ["content-type", "text/event-stream"],
      ["cache-control", "no-cache"],
      ...(options.headers ?? []),
    ],
  });
}

/** For workers, run(socket, ctx) retains the invocation until it completes. */
export function webSocketResponse(socket, { headers = [], run } = {}) {
  return { websocket: socket.id, headers, run };
}
