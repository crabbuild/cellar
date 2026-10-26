// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { idDescriptor, nativeId } from "./id.js";
import { fetchRequest } from "./http.js";
import { streamedResponse } from "./response.js";

// This initial namespace facade is event-owned, just like the native SDK
// context supplied to it. Activation-owned environment bindings are a separate
// dispatch/lifetime layer; retaining this facade never grants a later event.
export function durableObjectNamespace(nativeContext, binding) {
  return namespaceBinding(() => nativeContext, binding);
}

// Only the module dispatcher supplies an activation-bound current accessor.
// Ordinary public namespaces and their stubs keep their original event context.
export function namespaceBinding(current, binding) {
  if (typeof binding !== "string" || !binding.length)
    throw new TypeError("Expected a namespace binding name");
  const identify = (input) =>
    nativeId(current()._sync("workers.namespace", { binding, ...input }));
  const unsupportedOptions = (options) => {
    if (options !== undefined)
      throw new Error(
        "Namespace placement/jurisdiction options are not supported by this profile",
      );
  };
  const namespace = Object.freeze({
    idFromName(name) {
      if (typeof name !== "string")
        throw new TypeError("Expected a string name");
      const units = [];
      for (let i = 0; i < name.length; i++) units.push(name.charCodeAt(i));
      return identify({ kind: "name", name: units });
    },
    idFromString(id) {
      if (typeof id !== "string") throw new TypeError("Expected a string ID");
      return identify({ kind: "string", id });
    },
    newUniqueId(options) {
      unsupportedOptions(options);
      return identify({ kind: "unique" });
    },
    get(id, options) {
      unsupportedOptions(options);
      const descriptor = idDescriptor(id);
      // Validate the ID against this binding before creating the stub, even
      // when it was minted by a different namespace facade.
      const checked = namespace.idFromString(descriptor.id);
      const target = Object.create(null);
      Object.defineProperties(target, {
        id: { value: id },
        name: { value: id.name },
        __cellar_runtime_capability__: { value: () => {}, enumerable: true },
      });
      return new Proxy(Object.freeze(target), {
        get(target, method) {
          if (Object.hasOwn(target, method)) return target[method];
          if (method === "then" || typeof method !== "string") return undefined;
          if (method === "fetch")
            return (input, init) => {
              const context = current();
              return fetchRequest(
                input,
                init,
                context,
                (request, signal) =>
                  context._call(
                    "workers.fetch_stream",
                    {
                      binding,
                      id: checked.toString(),
                      name: descriptor.name,
                      request,
                    },
                    undefined,
                    signal,
                  ),
                undefined,
                (frame, _context, signal, request) => {
                  if (frame.websocket) {
                    context
                      ._call("response.cancel", { id: frame.forward })
                      .catch(() => {});
                    throw new TypeError(
                      "Namespace WebSocket upgrades require an upgrade-capable profile",
                    );
                  }
                  return streamedResponse(
                    { ...frame, stream: frame.forward },
                    context,
                    signal,
                    {
                      read: "response.read",
                      cancel: "response.cancel",
                      releaseOnEnd: true,
                      bodyless: request.method === "HEAD",
                    },
                  );
                },
              );
            };
          return async (...args) => {
            const nativeContext = current();
            return nativeContext._rpcCall(
              {
                binding,
                id: checked.toString(),
                name: descriptor.name,
                method,
              },
              args,
            );
          };
        },
      });
    },
    getByName(name, options) {
      return namespace.get(namespace.idFromName(name), options);
    },
    jurisdiction() {
      throw new Error(
        "Namespace jurisdiction is not supported by this profile",
      );
    },
  });
  return namespace;
}
