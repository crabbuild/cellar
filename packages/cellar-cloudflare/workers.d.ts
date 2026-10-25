// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import type { DurableObjectState } from "./index.js";
export class RpcTarget {
  private readonly __rpcTarget: never;
}
export class DurableObject<
  E extends object = Record<string, unknown>,
> extends RpcTarget {
  constructor(ctx: DurableObjectState, env: E);
  ctx: DurableObjectState;
  env: E;
}
/** Only the module base is present; Worker entrypoint RPC dispatch is pending. */
export class WorkerEntrypoint<
  E extends object = Record<string, unknown>,
> extends RpcTarget {
  constructor(ctx: unknown, env: E);
  ctx: unknown;
  env: E;
}
export const tracing: undefined;
