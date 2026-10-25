// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import type { Context } from "@cellar/sdk";
import { CounterClient as TypeScriptClient } from "./fixtures/generated/typescript/counter_cellar.js";
import { CounterClient as JavaScriptClient } from "./fixtures/generated/javascript/counter_cellar.js";
import type { IncrementResponse } from "./fixtures/generated/typescript/counter_pb.js";
import { AbortController } from "abort-controller";

declare const context: Context;
const ts = TypeScriptClient.bind(context, "COUNTERS", "alice");
const js = JavaScriptClient.bind(context, "COUNTERS", "alice");
const response: Promise<IncrementResponse> = ts.increment({delta:9007199254740993n, operationId:"id"});
const alsoTyped: Promise<IncrementResponse> = js.increment({delta:1n}, {timeoutMs:17});
const cancellable: Promise<IncrementResponse> = ts.increment({delta:1n}, {signal:new AbortController().signal});
void cancellable;
void response;
void alsoTyped;

// Each negative assertion must produce a real error from the generated API.
// @ts-expect-error Protobuf i64 inputs require exact bigint values.
ts.increment({delta:1});
// @ts-expect-error The plain-JS package's declarations preserve i64 types too.
js.increment({delta:1});
// @ts-expect-error Only declared business methods exist on the generated client.
ts.privateHelper({});
// @ts-expect-error Timeout options retain their concrete numeric type.
js.getCount({}, {timeoutMs:"17"});
// @ts-expect-error Signal listener methods and an aborted boolean are required.
ts.getCount({}, {signal:{aborted:false}});
