// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// Keep the copied type name and contract digest, but corrupt the actual codec.
// The native bootstrap must refuse readiness before invoking the host callback.
export { cells, rpc, fetch } from "./v8.js";
import { IncrementRequestSchema } from "./fixtures/generated/typescript/counter_pb.js";

Reflect.set(IncrementRequestSchema.fields[0], "number", 99);
