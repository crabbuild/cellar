// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { fixedLength } from "./streams.js";

function textSource(value) {
  let length = 0;
  for (let index = 0; index < value.length; index++) {
    const code = value.charCodeAt(index);
    if (code < 0x80) length++;
    else if (code < 0x800) length += 2;
    else if (
      code >= 0xd800 &&
      code <= 0xdbff &&
      index + 1 < value.length &&
      value.charCodeAt(index + 1) >= 0xdc00 &&
      value.charCodeAt(index + 1) <= 0xdfff
    ) {
      length += 4;
      index++;
    } else length += 3; // Includes the encoder's replacement for lone surrogates.
  }
  let offset = 0;
  const encoder = new TextEncoder();
  const body = new ReadableStream(
    {
      pull(controller) {
        if (offset === value.length) return controller.close();
        let end = Math.min(offset + 16384, value.length);
        // A surrogate pair must be encoded together, including across windows.
        if (
          end < value.length &&
          value.charCodeAt(end - 1) >= 0xd800 &&
          value.charCodeAt(end - 1) <= 0xdbff
        )
          end--;
        controller.enqueue(encoder.encode(value.slice(offset, end)));
        offset = end;
      },
    },
    { highWaterMark: 0 },
  );
  return { body, length };
}

/** Known lengths use native anonymous-file staging; unknown streams retain
 * the existing buffered-body bound. No guest header/property establishes size. */
export function partSource(value) {
  if (value instanceof ReadableStream)
    return { body: value, length: fixedLength(value) };
  if (typeof Blob === "function" && value instanceof Blob)
    return { body: value.stream(), length: value.size };
  if (typeof value === "string") return textSource(value);
  const bytes =
    value instanceof ArrayBuffer
      ? new Uint8Array(value)
      : new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
  let offset = 0;
  const body = new ReadableStream(
    {
      pull(controller) {
        if (offset === bytes.byteLength) return controller.close();
        const end = Math.min(offset + 64 * 1024, bytes.byteLength);
        controller.enqueue(bytes.subarray(offset, end));
        offset = end;
      },
    },
    { highWaterMark: 0 },
  );
  return { body, length: bytes.byteLength };
}

export async function uploadPart(context, route, body, length) {
  const reader = body.getReader();
  let id;
  let complete = false;
  let failure;
  try {
    const stage = await context._call("workers.r2.part_begin", {
      ...route,
      length,
    });
    id = stage.id;
    const maximum = stage.max_chunk_bytes;
    if (
      typeof id !== "string" ||
      !Number.isSafeInteger(maximum) ||
      maximum < 1 ||
      maximum > 64 * 1024
    )
      throw new TypeError("Invalid native R2 part stage");
    let received = 0;
    for (;;) {
      const { value, done } = await reader.read();
      if (done) break;
      if (
        !(value instanceof Uint8Array) ||
        value.byteLength > length - received
      )
        throw new RangeError("R2 part exceeds its declared length");
      received += value.byteLength;
      for (let offset = 0; offset < value.byteLength; offset += maximum)
        await context._call("workers.r2.part_write", {
          id,
          body: context._encode(value.subarray(offset, offset + maximum)),
        });
    }
    if (received !== length)
      throw new RangeError("R2 part is shorter than its declared length");
    const receipt = await context._call("workers.r2.part_finish", { id });
    complete = true;
    return receipt;
  } catch (error) {
    failure = error;
    throw error;
  } finally {
    if (!complete) {
      // Scratch retirement cannot undo a part whose completion committed.
      // Preserve the initiating failure even if source/native cleanup fails.
      const sourceClosed = reader.cancel(failure).catch(() => {});
      const retired =
        id === undefined
          ? Promise.resolve()
          : context._call("workers.r2.part_cancel", { id }).catch(() => {});
      await Promise.all([sourceClosed, retired]);
    }
    reader.releaseLock();
  }
}
