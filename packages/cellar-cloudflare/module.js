// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { activateDurableObject } from "./activation.js";
import { durableObjectContainer } from "./container.js";
import { namespaceBinding } from "./namespace.js";
import { decodeRequest, webRuntime } from "./http.js";
import { encodeStreamResponse } from "./output.js";
import { installSocketGlobals } from "./websocket-pair.js";

function environment(current, namespaces, variables) {
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
  return Object.freeze(env);
}

/** Adapt unchanged module exports to declared native Cellar event handlers. */
export function workersApplication(
  module,
  { namespaces = {}, containers = [], variables = [] } = {},
) {
  webRuntime();
  namespaces = Object.freeze({ ...namespaces });
  containers = Object.freeze([...containers]);
  variables = Object.freeze([...variables]);
  const worker = module.default;
  if (!worker || typeof worker.fetch !== "function") {
    throw new TypeError(
      "This Workers module profile requires a default fetch handler",
    );
  }
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
            env: environment(current, namespaces, variables),
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
    async fetch(frame, context) {
      installSocketGlobals(context._current);
      const env = environment(() => context, namespaces, variables);
      const execution = Object.freeze({
        waitUntil: (promise) => context._waitUntil(promise),
      });
      const response = await worker.fetch(decodeRequest(frame), env, execution);
      return encodeStreamResponse(response, context);
    },
  };
}
