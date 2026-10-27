// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { activateDurableObject } from "./activation.js";
import { durableObjectContainer } from "./container.js";
import { namespaceBinding } from "./namespace.js";
import { decodeRequest, webRuntime } from "./http.js";
import { encodeStreamResponse } from "./output.js";
import { installSocketGlobals } from "./websocket-pair.js";
import {
  entrypointRpc,
  isEntrypoint,
  serviceBinding,
  workerContext,
} from "./service.js";

function environment(current, namespaces, variables, services) {
  const env = Object.create(null);
  for (const binding of Object.keys(namespaces)) {
    Object.defineProperty(env, binding, {
      value: namespaceBinding(current, binding),
      enumerable: true,
    });
  }
  for (const name of variables) {
    if (Object.hasOwn(env, name))
      throw new TypeError(`Duplicate Worker binding: ${name}`);
    Object.defineProperty(env, name, {
      get: () => current()._sync("workers.variable", { name }) ?? undefined,
      enumerable: true,
    });
  }
  for (const binding of services) {
    if (Object.hasOwn(env, binding))
      throw new TypeError(`Duplicate Worker binding: ${binding}`);
    Object.defineProperty(env, binding, {
      value: serviceBinding(current, { kind: "binding", binding }),
      enumerable: true,
    });
  }
  return Object.freeze(env);
}

/** Adapt unchanged module exports to declared native Cellar event handlers. */
export function workersApplication(
  module,
  { namespaces = {}, containers = [], variables = [], services = [] } = {},
) {
  webRuntime();
  if (
    !Array.isArray(services) ||
    services.some(
      (name) =>
        typeof name !== "string" ||
        !name.length ||
        name.length > 512 ||
        name.includes("\0"),
    )
  )
    throw new TypeError("Worker services require binding names");
  namespaces = Object.freeze({ ...namespaces });
  containers = Object.freeze([...containers]);
  variables = Object.freeze([...variables]);
  services = Object.freeze([...services]);
  const bindings = [...Object.keys(namespaces), ...variables, ...services];
  if (new Set(bindings).size !== bindings.length)
    throw new TypeError("Duplicate Worker binding");
  const worker = module.default;
  if (
    !worker ||
    typeof (isEntrypoint(worker) ? worker.prototype.fetch : worker.fetch) !==
      "function"
  ) {
    throw new TypeError(
      "This Workers module profile requires a default fetch handler",
    );
  }
  const entrypoints = new Map(
    Object.entries(module).filter(([, target]) => isEntrypoint(target)),
  );
  const fetchers = new Map(
    Object.entries(module).filter(
      ([, target]) =>
        !isEntrypoint(target) && typeof target?.fetch === "function",
    ),
  );
  const exports = (current) => {
    const result = Object.create(null);
    for (const name of [...entrypoints.keys(), ...fetchers.keys()])
      result[name] = serviceBinding(current, { kind: "export", name });
    for (const [binding, name] of Object.entries(namespaces))
      result[name] ??= namespaceBinding(current, binding);
    return Object.freeze(result);
  };
  const instance = (name, context, props) => {
    const Target = entrypoints.get(name);
    if (!Target) throw new TypeError(`Missing Worker entrypoint: ${name}`);
    return new Target(
      workerContext(
        context,
        exports(() => context),
        props,
      ),
      environment(() => context, namespaces, variables, services),
    );
  };
  const classes = new Set(Object.values(namespaces));
  for (const name of containers) {
    if (!classes.has(name))
      throw new TypeError(`Container has no declared namespace: ${name}`);
  }
  const cells = Object.create(null);
  for (const name of classes) {
    const Target = module[name];
    if (typeof Target !== "function")
      throw new TypeError(`Missing Durable Object export: ${name}`);
    cells[name] = class {
      owner(context) {
        if (!this.activation) {
          const current = context._activation();
          this.activation = activateDurableObject(Target, context, {
            env: environment(current, namespaces, variables, services),
            exports: () => exports(current),
            container: containers.includes(name)
              ? durableObjectContainer(context)
              : undefined,
          });
        }
        return this.activation;
      }
      async fetch(frame, context) {
        const owner = this.owner(context);
        const response = await owner.invoke((target) => {
          if (typeof target.fetch !== "function")
            throw new TypeError("Durable Object has no fetch handler");
          return target.fetch(decodeRequest(frame));
        });
        return encodeStreamResponse(response, context);
      }
      async cell_rpc({ method, args }, context) {
        return this.owner(context).rpc(method, args);
      }
      async alarm(_event, context) {
        const previous = this.alarmTail;
        let release;
        const completed = new Promise((resolve) => {
          release = resolve;
        });
        const tail = previous ? previous.then(() => completed) : completed;
        this.alarmTail = tail;
        tail.then(() => {
          if (this.alarmTail === tail) this.alarmTail = undefined;
        });
        try {
          // Native alarms may be rearmed while an earlier handler is pending.
          // Workers permits one alarm callback per object at a time. Each
          // waiter keeps its own native invocation and rechecks authority.
          const revoked = context._revoked();
          await Promise.race([previous, revoked]);
          return await Promise.race([
            this.owner(context).invoke((target) => {
              if (typeof target.alarm !== "function")
                throw new TypeError("Durable Object has no alarm handler");
              return target.alarm();
            }),
            revoked,
          ]);
        } finally {
          release();
        }
      }
    };
  }
  return {
    cells,
    worker_rpc(frame, context) {
      return entrypointRpc(
        instance(frame.entrypoint, context, frame.props),
        frame.operation,
      );
    },
    async fetch(frame, context) {
      installSocketGlobals(context._current);
      const name = frame.entrypoint?.name ?? "default";
      const target = entrypoints.has(name)
        ? instance(name, context, frame.entrypoint?.props)
        : fetchers.get(name);
      if (typeof target?.fetch !== "function")
        throw new TypeError(`Missing Worker fetch entrypoint: ${name}`);
      const response = entrypoints.has(name)
        ? await target.fetch(decodeRequest(frame))
        : await target.fetch(
            decodeRequest(frame),
            environment(() => context, namespaces, variables, services),
            workerContext(
              context,
              exports(() => context),
              frame.entrypoint?.props,
            ),
          );
      return encodeStreamResponse(response, context);
    },
  };
}
