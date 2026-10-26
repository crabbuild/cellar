// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// The native socket remains owned by its opening event. Same-activation callers
// enqueue sends after their own output proof; only the original pumps perform
// socket I/O. A later event never receives the opening event's native handle.
export function websocketRuntime() {
  for (const name of ["EventTarget", "Event", "MessageEvent", "Blob"])
    if (typeof globalThis[name] !== "function")
      throw new TypeError(`WebSocket profile requires ${name}`);
}

export function eventWebSocket(frame, owner, current) {
  websocketRuntime();
  const maximum = owner._sync("workers.http.limits").max_body_bytes;
  const id = frame.id;
  const events = new EventTarget();
  const abort = new AbortController();
  const queue = [];
  let wake,
    receiving,
    sending,
    accepted = false,
    state = 1;
  let bytes = 0,
    count = 0,
    released = false,
    finishing;
  let binaryType = "arraybuffer",
    inflight;
  const handlers = Object.create(null);
  const failure = () => new Error("WebSocket is closed");
  const emit = (type, properties = {}) => {
    const event =
      type === "message" ? new MessageEvent(type, properties) : new Event(type);
    if (type !== "message")
      for (const [name, value] of Object.entries(properties))
        Object.defineProperty(event, name, { value, enumerable: true });
    events.dispatchEvent(event);
  };
  const wakeSender = () => {
    wake?.();
    wake = undefined;
  };
  const rejectQueue = (error) => {
    for (const entry of queue.splice(0)) {
      bytes -= entry.size;
      count--;
      entry.reject(error);
    }
    wakeSender();
  };
  const finish = (code = 1006, reason = "", clean = false, error) => {
    if (finishing) return finishing;
    state = 3;
    rejectQueue(error ?? failure());
    abort.abort();
    if (error) emit("error");
    emit("close", { code, reason, wasClean: clean });
    finishing = (async () => {
      await Promise.allSettled([inflight]);
      if (!released) {
        released = true;
        await owner._call("container.release", { id });
      }
    })();
    finishing.catch(() => {});
    return finishing;
  };

  async function transmit() {
    try {
      while (state !== 3) {
        if (!queue.length) {
          await new Promise((resolve) => {
            wake = resolve;
          });
          continue;
        }
        const entry = queue.shift();
        try {
          await entry.proof;
          if (state === 3) throw failure();
          inflight = owner._call(`container.websocket.${entry.operation}`, {
            id,
            ...entry.input,
          });
          if (entry.operation === "close")
            inflight = inflight.then((value) => {
              released = true;
              return value;
            });
          await inflight;
          entry.resolve();
        } catch (error) {
          entry.reject(error);
          await finish(1006, "", false, error);
        } finally {
          bytes -= entry.size;
          count--;
        }
      }
    } finally {
      rejectQueue(failure());
    }
  }

  async function receive() {
    try {
      while (state !== 3) {
        const event = await owner._call(
          "container.websocket.receive",
          { id },
          undefined,
          abort.signal,
        );
        if (state === 3) return;
        if (event.type === "open") continue;
        if (event.type === "text") emit("message", { data: event.text });
        else if (event.type === "binary") {
          const data = owner._decode(event.data);
          emit("message", {
            data:
              binaryType === "blob" ? new Blob([data]) : data.slice().buffer,
          });
        } else if (event.type === "close") {
          await finish(event.code, event.reason, event.clean);
          return;
        } else throw new TypeError("Invalid native WebSocket frame");
      }
    } catch (error) {
      if (state !== 3) await finish(1006, "", false, error);
    }
  }

  function enqueue(context, operation, input, size) {
    if (count >= 64 || size > maximum * 2 - bytes)
      throw new RangeError("WebSocket send queue exceeds limit");
    let resolve, reject;
    const pending = new Promise((done, fail) => {
      resolve = done;
      reject = fail;
    });
    // Reserve event background capacity before admitting a frame. Its future
    // waits for the actual native acknowledgement, including a lost outcome.
    context._rpcRetain(() => pending);
    const proof =
      context.identity === null ? Promise.resolve() : context.storage.sync();
    proof.catch(() => {});
    queue.push({ operation, input, size, proof, resolve, reject });
    bytes += size;
    count++;
    wakeSender();
    pending.catch((error) => {
      finish(1006, "", false, error).catch(() => {});
    });
  }

  owner._rpcCleanup(async () => {
    await finish();
    await Promise.allSettled([receiving, sending]);
  });
  const socket = {
    get readyState() {
      return state;
    },
    get protocol() {
      return frame.protocol ?? "";
    },
    get extensions() {
      return "";
    },
    get bufferedAmount() {
      return bytes;
    },
    get binaryType() {
      return binaryType;
    },
    set binaryType(value) {
      if (!["arraybuffer", "blob"].includes(value))
        throw new TypeError("Unsupported WebSocket binaryType");
      binaryType = value;
    },
    accept() {
      current();
      if (accepted || state !== 1) return;
      // Start both pumps in the original event. The first accept occurs while
      // consuming the upgrade in that same event; later accepts are idempotent.
      accepted = true;
      try {
        receiving = owner._rpcRetain(receive);
        sending = owner._rpcRetain(transmit);
      } catch (error) {
        finish(1006, "", false, error).catch(() => {});
        throw error;
      }
    },
    send(value) {
      const context = current();
      if (!accepted)
        throw new Error("WebSocket must be accepted before sending");
      if (state !== 1) return;
      let operation, input, size;
      if (typeof value === "string") {
        operation = "send_text";
        input = { text: value };
        size = new TextEncoder().encode(value).byteLength;
      } else if (value instanceof ArrayBuffer || ArrayBuffer.isView(value)) {
        const data =
          value instanceof ArrayBuffer
            ? new Uint8Array(value)
            : new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
        size = data.byteLength;
        operation = "send_binary";
        input = { data: context._encode(data.slice()) };
      } else throw new TypeError("WebSocket send requires text or bytes");
      if (size > maximum) throw new RangeError("WebSocket frame exceeds limit");
      enqueue(context, operation, input, size);
    },
    close(code = 1000, reason = "") {
      const context = current();
      if (
        !Number.isInteger(code) ||
        !(
          (code >= 1000 &&
            code <= 1014 &&
            ![1004, 1005, 1006].includes(code)) ||
          (code >= 3000 && code <= 4999)
        )
      )
        throw new RangeError("Invalid WebSocket close code");
      if (
        typeof reason !== "string" ||
        new TextEncoder().encode(reason).byteLength > 123
      )
        throw new RangeError("Invalid WebSocket close reason");
      if (state >= 2) return;
      if (!accepted) {
        finish(code, reason, false).catch(() => {});
        return;
      }
      enqueue(context, "close", { code, reason }, 0);
      state = 2;
    },
    addEventListener: events.addEventListener.bind(events),
    removeEventListener: events.removeEventListener.bind(events),
    dispatchEvent: events.dispatchEvent.bind(events),
  };
  for (const name of ["message", "close", "error", "open"])
    Object.defineProperty(socket, `on${name}`, {
      get: () => handlers[name] ?? null,
      set(value) {
        if (handlers[name]) events.removeEventListener(name, handlers[name]);
        handlers[name] = typeof value === "function" ? value : null;
        if (handlers[name]) events.addEventListener(name, handlers[name]);
      },
    });
  for (const [key, value] of Object.entries({
    CONNECTING: 0,
    OPEN: 1,
    CLOSING: 2,
    CLOSED: 3,
  }))
    Object.defineProperty(socket, key, { value, enumerable: true });
  return Object.defineProperties(
    events,
    Object.getOwnPropertyDescriptors(socket),
  );
}

export function websocketResponse(frame, socket) {
  // Node's Response constructor refuses 101. Keep the native Response prototype
  // and HTTP header methods, then expose the actual upgrade metadata explicitly.
  const response = new Response(null, { headers: frame.headers });
  Object.defineProperties(response, {
    status: { value: 101 },
    statusText: { value: "Switching Protocols" },
    ok: { value: false },
    webSocket: { value: socket },
    clone: {
      value: () => {
        throw new TypeError("WebSocket responses cannot be cloned");
      },
    },
  });
  return response;
}
