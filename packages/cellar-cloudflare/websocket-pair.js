// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { eventWebSocket, websocketRuntime } from "./websocket.js";

const localSockets = new WeakSet();
const BaseResponse = globalThis.Response;

/** Install only for the optional Workers profile, with native current-event lookup. */
export function installSocketGlobals(current) {
  // The embedded non-web profile still uses storage/activation facades. It
  // must not acquire a dependency on Node's web globals merely by importing it.
  if (typeof current !== "function" || typeof BaseResponse !== "function")
    return;
  if (globalThis.WebSocketPair === undefined) {
    Object.defineProperty(globalThis, "WebSocketPair", {
      configurable: true,
      writable: true,
      value: class WebSocketPair {
        constructor() {
          websocketRuntime();
          const owner = current();
          const authority = owner.identity
            ? owner._activation()
            : () => {
                const active = current();
                if (active !== owner)
                  throw new Error("WebSocket belongs to another event");
                return active;
              };
          const maximum = owner._sync("workers.http.limits").max_body_bytes;
          const lanes = [lane(), lane()];
          let closed = false;
          function stop(code = 1006, reason = "", clean = false) {
            if (closed) return;
            closed = true;
            for (const lane of lanes)
              lane.close({ type: "close", code, reason, clean });
          }
          const ends = [0, 1].map((index) => {
            const adapter = {
              _sync: owner._sync,
              _encode: owner._encode,
              _decode: owner._decode,
              _rpcRetain: owner._rpcRetain,
              _rpcCleanup: owner._rpcCleanup,
              storage: owner.storage,
              async _call(operation, input, _body, signal) {
                if (operation === "container.websocket.receive")
                  return lanes[index].read(signal);
                if (operation === "container.websocket.close") {
                  stop(input.code, input.reason, true);
                  return;
                }
                if (operation === "container.release") {
                  stop();
                  return;
                }
                if (closed) throw new Error("WebSocket pair is closed");
                if (operation === "container.websocket.send_text")
                  return lanes[1 - index].send(
                    { type: "text", text: input.text },
                    new TextEncoder().encode(input.text).length,
                    maximum,
                  );
                if (operation === "container.websocket.send_binary")
                  return lanes[1 - index].send(
                    { type: "binary", data: input.data },
                    owner._decode(input.data).length,
                    maximum,
                  );
                throw new TypeError("Invalid WebSocket pair operation");
              },
            };
            const socket = eventWebSocket(
              { id: "local-pair" },
              adapter,
              authority,
              owner,
            );
            localSockets.add(socket);
            return socket;
          });
          Object.defineProperties(this, {
            0: { value: ends[0], enumerable: true },
            1: { value: ends[1], enumerable: true },
          });
        }
      },
    });
  }
  if (globalThis.Response === BaseResponse) {
    class WorkersResponse extends BaseResponse {
      constructor(body, init = {}) {
        const socket = init.webSocket;
        if (init.status !== 101 && socket == null) {
          super(body, init);
          return;
        }
        if (init.status !== 101 || body != null || !localSockets.has(socket))
          throw new TypeError(
            "Upgrade responses require a local WebSocketPair client and no body",
          );
        super(null, { headers: init.headers });
        Object.defineProperties(this, {
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
      }
      static [Symbol.hasInstance](value) {
        return value instanceof BaseResponse;
      }
    }
    globalThis.Response = WorkersResponse;
  }
}

function lane() {
  const queue = [];
  let waiter,
    closed,
    bytes = 0;
  function consume(entry) {
    bytes -= entry.size;
    entry.resolve();
    return entry.frame;
  }
  return {
    send(frame, size, maximum) {
      if (closed) return Promise.reject(new Error("WebSocket pair is closed"));
      if (queue.length >= 64 || size > maximum * 2 - bytes)
        return Promise.reject(
          new RangeError("WebSocket pair receive queue exceeds limit"),
        );
      if (waiter) {
        const ready = waiter;
        waiter = undefined;
        ready.resolve(frame);
        return Promise.resolve();
      }
      return new Promise((resolve, reject) => {
        queue.push({ frame, size, resolve, reject });
        bytes += size;
      });
    },
    read(signal) {
      if (queue.length) return Promise.resolve(consume(queue.shift()));
      if (closed) return Promise.resolve(closed);
      if (waiter)
        return Promise.reject(
          new Error("WebSocket pair already has a receiver"),
        );
      return new Promise((resolve, reject) => {
        const abort = () => {
          waiter = undefined;
          reject(new Error("WebSocket pair receive cancelled"));
        };
        signal.addEventListener("abort", abort, { once: true });
        waiter = {
          resolve(value) {
            signal.removeEventListener("abort", abort);
            resolve(value);
          },
        };
        if (signal.aborted) abort();
      });
    },
    close(frame) {
      closed = frame;
      for (const entry of queue.splice(0)) {
        bytes -= entry.size;
        entry.reject(new Error("WebSocket pair closed before delivery"));
      }
      waiter?.resolve(frame);
      waiter = undefined;
    },
  };
}
