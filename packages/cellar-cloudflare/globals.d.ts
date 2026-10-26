// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// Opt-in declarations for the Node/DOM Workers compatibility profile. Projects
// using the official Workers globals should keep that environment's own types.
import type { WorkerWebSocket } from "./index.js";
export {};

declare global {
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
