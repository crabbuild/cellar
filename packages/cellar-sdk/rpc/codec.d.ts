// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import type { InputLimits } from "./input.js";

type RuntimeFunction = (...args: never[]) => unknown;

/** The generated package supplies and types its selected codec implementation. */
export interface CodecRuntime {
  create: RuntimeFunction;
  fromBinary: RuntimeFunction;
  toBinary: RuntimeFunction;
  configureTextEncoding: RuntimeFunction;
}

/**
 * Internal byte boundary. Generated clients and dispatchers retain the concrete
 * message types and select the schema; the SDK transport remains codec-neutral.
 */
export function createCodec(runtime: CodecRuntime): {
  encodeMessage(schema: unknown, input: unknown, limits?: InputLimits): Uint8Array;
  decodeMessage(schema: unknown, bytes: Uint8Array, limits?: InputLimits): unknown;
};
