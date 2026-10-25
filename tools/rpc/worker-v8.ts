// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { jsonResponse, type Context, type Request } from "@cellar/sdk";
import { Counter } from "./app.js";
import { CounterClient, CounterRegistration } from "./fixtures/generated/typescript/counter_cellar.js";

class WorkerCounter extends Counter {
  // Registration inspects own methods, so reads are explicitly forwarded.
  override async getCount(request: Parameters<Counter["getCount"]>[0], context: Context) {
    return super.getCount(request, context);
  }

  override async increment(request: Parameters<Counter["increment"]>[0], context: Context) {
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

export const cells = { V8Counter: WorkerCounter };
export const rpc = { V8Counter: CounterRegistration };

export async function fetch(request: Request, context: Context) {
  if (!request.url.split("?")[0].endsWith("/lifecycle/terminate")) {
    return jsonResponse({ error: "Unknown lifecycle route" }, { status: 404 });
  }
  // The real native child proves this code entered before the CPU loop. The
  // production watchdog must terminate this isolate while another invocation
  // still awaits its own held native child. No test API closes the engine.
  await CounterClient.bind(context, "RUST", "worker-termination").getCount({});
  for (;;) { /* Deliberately exhaust the existing execution budget. */ }
}
