// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import type { DurableObjectState, WorkerExecutionContext } from "./index.js";
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
export class WorkerEntrypoint<
  E extends object = Record<string, unknown>,
  P = unknown,
> extends RpcTarget {
  constructor(ctx: WorkerExecutionContext<P>, env: E);
  ctx: WorkerExecutionContext<P>;
  env: E;
}
export const tracing: undefined;
