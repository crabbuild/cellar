// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { refuse } from "./scalar.js";

/** Resolve the finite budgets used by schema-driven input preflight. */
export function inputLimits(limits = {}) {
  if (limits === null || typeof limits !== "object" || Array.isArray(limits)) refuse();
  const maxDepth = limits.maxDepth ?? 64;
  const maxElements = limits.maxElements ?? 100_000;
  const maxMessageBytes = limits.maxMessageBytes ?? 1024 * 1024;
  const maxDecodedSlots = limits.maxDecodedSlots ?? 1_000_000;
  if (!Number.isSafeInteger(maxDepth) || maxDepth < 1 || maxDepth > 64 ||
      !Number.isSafeInteger(maxElements) || maxElements < 1 || maxElements > 1_000_000 ||
      !Number.isSafeInteger(maxMessageBytes) || maxMessageBytes < 1 || maxMessageBytes > 16 * 1024 * 1024 ||
      !Number.isSafeInteger(maxDecodedSlots) || maxDecodedSlots < 1 || maxDecodedSlots > 4_000_000) {
    refuse();
  }
  return {maxDepth, maxElements, maxMessageBytes, maxDecodedSlots};
}
