// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

/**
 * Activation-bound container state. Each property read consults the original
 * native provider; a previous event's promise or JS cache is not a snapshot.
 * Queued lifecycle and port methods are separate, unfinished profile work.
 */
export function durableObjectContainer(nativeContext) {
  const current = nativeContext._activation();
  const container = {
    get running() {
      return current()._sync("workers.container.running");
    },
  };
  // Keep persisted-clone/RPC codecs from turning this live native capability
  // into a plain object containing an accidentally captured running value.
  Object.defineProperty(container, "__cellar_runtime_capability__", {
    value: () => {},
    enumerable: true,
  });
  return Object.freeze(container);
}
