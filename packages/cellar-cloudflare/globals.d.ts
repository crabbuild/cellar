// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// Opt-in declarations for the Node/DOM Workers compatibility profile. Projects
// using the official Workers globals should keep that environment's own types.
import type { WorkerWebSocket } from "./index.js";
export {};

declare global {
  class IdentityTransformStream {
    constructor();
    readonly readable: ReadableStream<Uint8Array>;
    readonly writable: WritableStream<BufferSource>;
  }
  class FixedLengthStream {
    constructor(length: number | bigint);
    readonly readable: ReadableStream<Uint8Array>;
    readonly writable: WritableStream<BufferSource>;
  }
  interface SubtleCrypto {
    timingSafeEqual(a: BufferSource, b: BufferSource): boolean;
  }
  interface Response {
    readonly webSocket: WorkerWebSocket | null;
  }
  interface ResponseInit {
    webSocket?: WorkerWebSocket;
  }
  var WebSocketPair: {
    new (): { readonly 0: WorkerWebSocket; readonly 1: WorkerWebSocket };
  };
}
