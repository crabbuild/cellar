// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// A plain JavaScript entry and generated JS client/registration. Business
// messages cross the same wire contract as the independently generated TS API.
import { Counter, handler } from "./app.js";
import { CounterClient, CounterRegistration } from "./fixtures/generated/javascript/counter_cellar.js";
import { jsonResponse } from "@cellar/sdk";

export const cells = {NodeCounter:Counter};
export const rpc = {NodeCounter:CounterRegistration};
const business = handler(CounterClient, () => new (Reflect.get(globalThis, "AbortController"))());

/** @type {typeof business} */
export const fetch = async (request, context) => {
  // Qualification-only process identity. The coordinator checks this worker's
  // separate process group after killing its native parent.
  if (request.url.split("?")[0].endsWith("/node/process")) {
    const pid = Reflect.get(globalThis, "process").pid;
    if (!Number.isSafeInteger(pid) || pid < 1) throw Error("Invalid Node PID");
    return jsonResponse({pid});
  }
  return business(request, context);
};
