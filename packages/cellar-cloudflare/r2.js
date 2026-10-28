// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { bodyBytes, httpLimit, webRuntime } from "./http.js";
import { streamedResponse } from "./response.js";

function keyString(key) {
  if (
    typeof key !== "string" ||
    !key.length ||
    new TextEncoder().encode(key).length > 1024
  )
    throw new TypeError(
      "R2 requires a nonempty key of at most 1024 UTF-8 bytes",
    );
  return key;
}

function fields(value, allowed) {
  if (
    !value ||
    typeof value !== "object" ||
    ![Object.prototype, null].includes(Object.getPrototypeOf(value)) ||
    Object.keys(value).some((key) => !allowed.includes(key))
  )
    throw new TypeError("Unsupported R2 options");
}

function metadata(options = {}) {
  fields(options, ["httpMetadata", "customMetadata"]);
  const httpMetadata = options.httpMetadata ?? {};
  fields(httpMetadata, [
    "contentType",
    "contentLanguage",
    "contentDisposition",
    "contentEncoding",
    "cacheControl",
    "cacheExpiry",
  ]);
  const http = Object.fromEntries(
    Object.entries(httpMetadata).filter(([, value]) => value !== undefined),
  );
  if (http.cacheExpiry !== undefined) {
    if (
      !(http.cacheExpiry instanceof Date) ||
      !Number.isFinite(http.cacheExpiry.getTime())
    )
      throw new TypeError("R2 cacheExpiry requires a valid Date");
    http.cacheExpiry = http.cacheExpiry.getTime();
  }
  const custom = options.customMetadata ?? {};
  fields(custom, Object.keys(custom));
  if (Object.values(custom).some((value) => typeof value !== "string"))
    throw new TypeError("R2 custom metadata requires strings");
  return { httpMetadata: http, customMetadata: { ...custom } };
}

function object(frame, response) {
  const http = { ...frame.http_metadata };
  if (http.cacheExpiry !== undefined)
    http.cacheExpiry = new Date(http.cacheExpiry);
  const result = {
    key: frame.key,
    version: frame.version,
    size: frame.size,
    etag: frame.etag,
    httpEtag: frame.http_etag,
    uploaded: new Date(frame.uploaded_ms),
    httpMetadata: http,
    customMetadata: { ...frame.custom_metadata },
    checksums: Object.fromEntries(
      Object.entries(frame.checksums).map(([algorithm, hex]) => {
        if (!/^(?:[a-fA-F0-9]{2})+$/.test(hex))
          throw new TypeError("Invalid native checksum metadata");
        return [
          algorithm,
          Uint8Array.from(hex.match(/../g), (value) =>
            Number.parseInt(value, 16),
          ).buffer,
        ];
      }),
    ),
    storageClass: frame.storage_class,
    ...(frame.range ? { range: { ...frame.range } } : {}),
    writeHttpMetadata(headers) {
      if (!(headers instanceof Headers))
        throw new TypeError("Expected Headers");
      for (const [key, name] of Object.entries({
        contentType: "content-type",
        contentLanguage: "content-language",
        contentDisposition: "content-disposition",
        contentEncoding: "content-encoding",
        cacheControl: "cache-control",
      })) {
        if (http[key] !== undefined) headers.set(name, http[key]);
      }
      if (http.cacheExpiry !== undefined)
        headers.set("expires", http.cacheExpiry.toUTCString());
    },
  };
  if (response)
    Object.defineProperties(result, {
      body: { value: response.body, enumerable: true },
      bodyUsed: { get: () => response.bodyUsed, enumerable: true },
      arrayBuffer: { value: () => response.arrayBuffer() },
      text: { value: () => response.text() },
      json: { value: () => response.json() },
      blob: { value: () => response.blob() },
    });
  return result;
}

/** Explicit native Blob binding; this profile implements the backup gateway subset. */
export function r2Binding(current, binding) {
  const call = (kind, key, input = {}, context = current()) =>
    context._call(`workers.r2.${kind}`, {
      binding,
      key: keyString(key),
      ...input,
    });
  const resume = (key, uploadId) => {
    key = keyString(key);
    if (
      typeof uploadId !== "string" ||
      !uploadId.length ||
      uploadId.includes("\0") ||
      new TextEncoder().encode(uploadId).length > 16384
    )
      throw new TypeError("Invalid R2 upload identity");
    // Routes can be persisted as key/ID strings. Every effect still needs the
    // current event; temporary byte bodies and their readers never outlive it.
    return Object.freeze({
      key,
      uploadId,
      async uploadPart(partNumber, value, options) {
        const context = current();
        if (options !== undefined)
          throw new TypeError(
            "R2 part options are unsupported by this profile",
          );
        if (
          !Number.isInteger(partNumber) ||
          partNumber < 1 ||
          partNumber > 10000
        )
          throw new RangeError("R2 part number must be in 1..=10000");
        webRuntime();
        if (
          !(value instanceof ReadableStream) &&
          !(value instanceof ArrayBuffer) &&
          !ArrayBuffer.isView(value) &&
          typeof value !== "string" &&
          !(typeof Blob === "function" && value instanceof Blob)
        )
          throw new TypeError("R2 uploadPart requires bytes or a byte stream");
        const bytes = await bodyBytes(new Response(value), httpLimit(context));
        return call(
          "part",
          key,
          {
            upload_id: uploadId,
            number: partNumber,
            body: context._encode(bytes),
          },
          context,
        );
      },
      async complete(parts) {
        if (!Array.isArray(parts) || !parts.length || parts.length > 10000)
          throw new TypeError("R2 completion requires current part receipts");
        const receipts = parts.map((part, index) => {
          fields(part, ["partNumber", "etag"]);
          if (
            part.partNumber !== index + 1 ||
            typeof part.etag !== "string" ||
            !part.etag.length
          )
            throw new TypeError(
              "This R2 profile requires consecutive current part receipts",
            );
          return { partNumber: part.partNumber, etag: part.etag };
        });
        return object(
          await call("complete", key, { upload_id: uploadId, parts: receipts }),
        );
      },
      async abort() {
        await call("abort", key, { upload_id: uploadId });
      },
      __cellar_runtime_capability__: () => {},
    });
  };
  return Object.freeze({
    async head(key) {
      const frame = await call("head", key);
      return frame === null ? null : object(frame);
    },
    async get(key, options = {}) {
      fields(options, ["range"]);
      if (options.range !== undefined)
        fields(options.range, ["offset", "length", "suffix"]);
      const context = current();
      const frame = await call(
        "get",
        key,
        options.range === undefined
          ? {}
          : {
              range: Object.fromEntries(
                Object.entries(options.range).filter(
                  ([, value]) => value !== undefined,
                ),
              ),
            },
        context,
      );
      if (frame === null) return null;
      const response = streamedResponse(
        { ...frame.response, stream: frame.response.forward },
        context,
        new AbortController().signal,
        {
          read: "response.read",
          cancel: "response.cancel",
          releaseOnEnd: true,
        },
      );
      return object(frame.object, response);
    },
    async delete(key) {
      await call("delete", key);
    },
    async createMultipartUpload(key, options) {
      const frame = await call("create", key, { options: metadata(options) });
      return resume(frame.key, frame.uploadId);
    },
    resumeMultipartUpload: resume,
    __cellar_runtime_capability__: () => {},
  });
}
