// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { fetchRequest } from "./http.js";
import { streamedResponse } from "./response.js";
import { forwardedWebSocket } from "./websocket-forward.js";
import { RpcTarget, WorkerEntrypoint } from "./workers.js";

const reserved = new Set([
  "constructor",
  "prototype",
  "__proto__",
  "fetch",
  "connect",
  "dup",
]);
const serviceTargets = new WeakMap();

/** Copy a supplied service capability without retaining its JavaScript state. */
export function containerServiceTarget(value, context) {
  const supplied = serviceTargets.get(value);
  if (!supplied || supplied.current() !== context) {
    throw new TypeError(
      "Container HTTP handler must be a current service binding or exported entrypoint",
    );
  }
  return { ...supplied.descriptor };
}

export function isEntrypoint(target) {
  return (
    typeof target === "function" &&
    WorkerEntrypoint.prototype.isPrototypeOf(target.prototype)
  );
}

/** Reusable route metadata; every effect is minted by the current native host. */
export function serviceBinding(current, descriptor) {
  descriptor = Object.freeze({ ...descriptor });
  const target = (options) => {
    if (descriptor.kind !== "export")
      throw new TypeError("Only loopback bindings accept props");
    if (
      !options ||
      typeof options !== "object" ||
      Array.isArray(options) ||
      Object.keys(options).some((key) => key !== "props")
    )
      throw new TypeError("Loopback binding requires props options");
    const context = current();
    return serviceBinding(current, {
      ...descriptor,
      props: Object.hasOwn(options, "props")
        ? context._rpcValue(options.props)
        : undefined,
    });
  };
  const brand = () => {};
  Object.defineProperty(target, "__cellar_runtime_capability__", {
    value: brand,
    enumerable: true,
  });
  const invoke = async (kind, path, args) => {
    const context = current();
    const operation = { kind, path };
    if (kind === "call") operation.args = context._rpcValue(args);
    const frame = await context._call("workers.rpc", {
      service: descriptor,
      operation,
    });
    const value = context._rpcDecode(frame);
    if (frame.error) {
      const [thrown, properties] = value;
      if (
        thrown !== null &&
        (typeof thrown === "object" || typeof thrown === "function")
      )
        for (const [name, entry] of Object.entries(properties))
          Object.defineProperty(thrown, name, {
            value: entry,
            enumerable: true,
            writable: true,
            configurable: true,
          });
      throw thrown;
    }
    return value;
  };
  function property(path) {
    if (path.length > 64 || path.some((part) => reserved.has(part)))
      throw new TypeError("Invalid service RPC path");
    return new Proxy((...args) => invoke("call", path, args), {
      get(_target, name) {
        if (name === "then")
          return (resolve, reject) => invoke("get", path).then(resolve, reject);
        if (typeof name === "symbol") return undefined;
        return property([...path, name]);
      },
    });
  }
  const proxy = new Proxy(target, {
    get(_target, name) {
      if (name === "__cellar_runtime_capability__") return brand;
      if (name === "then" || typeof name === "symbol") return undefined;
      if (name === "fetch")
        return (input, init) => {
          const context = current();
          return fetchRequest(
            input,
            init,
            context,
            (request, signal) =>
              context._call(
                "workers.fetch_stream",
                { service: descriptor, request },
                undefined,
                signal,
              ),
            undefined,
            (frame, _context, signal, request) => {
              if (frame.websocket) {
                try {
                  return forwardedWebSocket(frame, context, current);
                } catch (error) {
                  context
                    ._call("response.cancel", { id: frame.forward })
                    .catch(() => {});
                  const failure = new Error(
                    "WebSocket upgrade response unavailable",
                    { cause: error },
                  );
                  failure.code = "Ambiguous";
                  failure.outcome = "unknown";
                  throw failure;
                }
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
      return property([name]);
    },
  });
  serviceTargets.set(proxy, { current, descriptor });
  return proxy;
}

export function workerContext(context, exported, props) {
  return Object.freeze({
    props: props == null ? undefined : context._deserialize(props),
    exports: exported,
    waitUntil: (promise) => context._waitUntil(promise),
  });
}

/** Public prototype members of RPC targets; own state and base APIs stay private. */
function member(receiver, name) {
  if (!receiver || reserved.has(name))
    throw new TypeError("Invalid Worker RPC property");
  if (receiver instanceof RpcTarget) {
    for (
      let proto = Object.getPrototypeOf(receiver);
      proto &&
      proto !== WorkerEntrypoint.prototype &&
      proto !== RpcTarget.prototype &&
      proto !== Object.prototype;
      proto = Object.getPrototypeOf(proto)
    ) {
      const descriptor = Object.getOwnPropertyDescriptor(proto, name);
      if (descriptor) return Reflect.get(proto, name, receiver);
    }
  } else {
    const descriptor = Object.getOwnPropertyDescriptor(receiver, name);
    if (descriptor) return Reflect.get(receiver, name);
  }
  throw new TypeError("No public Worker RPC property");
}

export async function entrypointRpc(target, operation) {
  const path = operation.path;
  if (
    !Array.isArray(path) ||
    !path.length ||
    path.length > 64 ||
    path.some((part) => typeof part !== "string")
  )
    throw new TypeError("Invalid Worker RPC path");
  let receiver = target;
  for (const name of path.slice(0, -1)) receiver = await member(receiver, name);
  const value = member(receiver, path.at(-1));
  if (operation.kind === "get") return value;
  if (
    operation.kind !== "call" ||
    !Array.isArray(operation.args) ||
    typeof value !== "function"
  )
    throw new TypeError("Invalid Worker RPC call");
  return Reflect.apply(value, receiver, operation.args);
}
