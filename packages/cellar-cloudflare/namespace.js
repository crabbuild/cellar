// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { idDescriptor, nativeId } from "./id.js";

// This initial namespace facade is event-owned, just like the native SDK
// context supplied to it. Activation-owned environment bindings are a separate
// dispatch/lifetime layer; retaining this facade never grants a later event.
export function durableObjectNamespace(nativeContext, binding) {
  if (typeof binding !== "string" || !binding.length)
    throw new TypeError("Expected a namespace binding name");
  const identify = (input) =>
    nativeId(nativeContext._sync("workers.namespace", { binding, ...input }));
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
            return () => {
              throw new Error(
                "Workers Request/Response transport is not enabled by this RPC-only namespace profile",
              );
            };
          return async (...args) => {
            const reply = await nativeContext._call("workers.rpc", {
              binding,
              id: checked.toString(),
              name: descriptor.name,
              method,
              args: nativeContext._serialize(args),
            });
            const value = nativeContext._deserialize(reply.value);
            if (reply.error) {
              if (!Array.isArray(value) || value.length !== 2)
                throw new Error("Invalid remote error envelope");
              const [thrown, properties] = value;
              if (
                thrown !== null &&
                (typeof thrown === "object" || typeof thrown === "function")
              ) {
                for (const [name, property] of Object.entries(properties)) {
                  Object.defineProperty(thrown, name, {
                    value: property,
                    writable: true,
                    configurable: true,
                    enumerable: true,
                  });
                }
              }
              throw thrown;
            }
            return value;
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
