// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// The module surface is opt-in at build time. RPC target dispatch and hosted
// services are separate capabilities; defining a base class does not enable them.
export class RpcTarget {
  constructor() {
    // An opaque callable brand keeps ordinary engine cloning from silently
    // turning a nested RPC target into an empty data object. The RPC codec must
    // materialize this capability before serializing it. JSON omits the brand.
    Object.defineProperty(this, "__cellar_runtime_capability__", {
      value: () => {},
      enumerable: true,
    });
  }
}
export class WorkerEntrypoint extends RpcTarget {
  constructor(ctx, env) {
    super();
    this.ctx = ctx;
    this.env = env;
  }
}
export class DurableObject extends RpcTarget {
  constructor(ctx, env) {
    super();
    this.ctx = ctx;
    this.env = env;
  }
}
// The pinned Sandbox helper explicitly supports unavailable tracing. This
// profile exports that disabled state, rather than manufacturing trace results.
export const tracing = undefined;
