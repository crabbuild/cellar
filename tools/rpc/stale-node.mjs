// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

export { cells, rpc, fetch } from "./node.mjs";
import { IncrementRequestSchema } from "./fixtures/generated/javascript/counter_pb.js";

Reflect.set(IncrementRequestSchema.fields[0], "number", 99);
