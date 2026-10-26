// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// HTTP messages reuse the native stream lease. Metadata is validated
// by Rust alongside that lease; persisted marker data never recreates a body.
(readables, makeRequests) => {
  const ResponseClass = globalThis.Response;
  const HeadersClass = globalThis.Headers;
  const define = Object.defineProperties;
  const apply = Reflect.apply;
  const getSetCookie = HeadersClass?.prototype.getSetCookie;
  const errorResponse = ResponseClass?.error;
  const encoder = globalThis.TextEncoder && new TextEncoder();

  function headerEntries(message) {
    const headers = [];
    let bytes = 0;
    const append = (key, value) => {
      bytes += encoder.encode(key + value).byteLength;
      if (headers.length >= 128 || bytes > 32768)
        throw new RangeError("RPC HTTP headers exceed limit");
      headers.push([key, value]);
    };
    for (const [key, value] of message.headers)
      if (key !== "set-cookie" || !getSetCookie) append(key, value);
    if (getSetCookie)
      for (const value of apply(getSetCookie, message.headers, []))
        append("set-cookie", value);
    return headers;
  }
  const requests = makeRequests?.(readables, headerEntries);

  function send(response, context) {
    if (requests?.isRequest(response)) return requests.send(response, context);
    if (response.webSocket || response.status === 101)
      throw new TypeError("RPC cannot transfer a WebSocket response");
    const body = response.body;
    if (response.bodyUsed || body?.locked)
      throw new TypeError("RPC response body has already been used");
    if (!["default", "basic", "cors", "error"].includes(response.type))
      throw new TypeError("RPC cannot transfer an opaque response");
    const headers = headerEntries(response);
    const metadata = {
      status: response.status,
      statusText: response.statusText,
      headers,
      body: body !== null,
      url: response.url,
      redirected: response.redirected,
      type: response.type,
    };
    if (
      encoder.encode(metadata.statusText).byteLength > 512 ||
      encoder.encode(metadata.url).byteLength > 8192
    )
      throw new RangeError("RPC response metadata exceeds limit");
    if (metadata.type !== "error")
      new ResponseClass(null, {
        status: metadata.status,
        statusText: metadata.statusText,
        headers,
      });
    const sender = body
      ? readables.sendReturned(body, context)
      : {
          restore() {},
          async close() {},
          async invoke(operation) {
            if (operation === "cancel") return null;
            if (operation === "read") return { done: true };
            throw new TypeError("Invalid RPC response operation");
          },
        };
    return { ...sender, metadata };
  }

  function receive(metadata, call, release, reason, maximum, options) {
    if (metadata.method !== undefined)
      return requests.receive(
        metadata,
        call,
        release,
        reason,
        maximum,
        options,
      );
    if (!ResponseClass)
      throw new TypeError("RPC responses require web globals");
    const received = metadata.body
      ? readables.receive(call, release, reason, maximum)
      : { stream: null, dispose: release };
    try {
      const response =
        metadata.type === "error"
          ? apply(errorResponse, ResponseClass, [])
          : new ResponseClass(received.stream, {
              status: metadata.status,
              statusText: metadata.statusText,
              headers: metadata.headers,
            });
      define(response, {
        url: { value: metadata.url },
        redirected: { value: metadata.redirected },
        type: { value: metadata.type },
      });
      if (!metadata.body) release().catch(() => {});
      return { stream: response, dispose: received.dispose };
    } catch (error) {
      received.dispose().catch(() => {});
      throw error;
    }
  }
  return {
    isRequest: (value) => requests?.isRequest(value),
    isResponse: (value) => ResponseClass && value instanceof ResponseClass,
    send,
    receive,
  };
}
