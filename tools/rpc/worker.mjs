// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// A disposable provider owns a different Node process from every caller.
// The native Rust gate holds the nested typed call until cancellation retires
// it. No file marker or JavaScript outcome claim establishes durable effects.
import { Counter } from "./app.js";
import { CounterClient, CounterRegistration } from "./fixtures/generated/javascript/counter_cellar.js";
import { fetch } from "./node.mjs";

class WorkerCounter extends Counter {
  /** @param {Parameters<Counter["getCount"]>[0]} request
   * @param {Parameters<Counter["getCount"]>[1]} context */
  async getCount(request, context) {
    return super.getCount(request, context);
  }

  /** @param {Parameters<Counter["increment"]>[0]} request
   * @param {Parameters<Counter["increment"]>[1]} context */
  async increment(request, context) {
    if (request.operationId === "worker-before") {
      await CounterClient.bind(context, "RUST", "worker-control").getCount({});
    }
    const response = await super.increment(request, context);
    if (request.operationId === "worker-after") {
      await CounterClient.bind(context, "RUST", "worker-control").getCount({});
    }
    return response;
  }
}

export const cells = { NodeCounter: WorkerCounter };
export const rpc = { NodeCounter: CounterRegistration };
export { fetch };
