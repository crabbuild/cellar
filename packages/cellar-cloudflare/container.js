// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { containerPort } from "./port.js";
import { containerExec, execControls } from "./exec.js";
import { containerServiceTarget } from "./service.js";

function startOptions(options) {
  if (options === undefined) return {};
  if (!options || typeof options !== "object" || Array.isArray(options)) {
    throw new TypeError("Container start options must be an object");
  }
  for (const key of Object.keys(options)) {
    if (!["entrypoint", "env", "enableInternet", "labels"].includes(key)) {
      throw new TypeError(`Unsupported container start option: ${key}`);
    }
  }
  const pairs = (value) => {
    if (!value || typeof value !== "object" || Array.isArray(value)) {
      throw new TypeError("Container environment and labels must be records");
    }
    return Object.entries(value);
  };
  return {
    ...(options.entrypoint === undefined
      ? {}
      : { entrypoint: options.entrypoint }),
    ...(options.env === undefined ? {} : { env: pairs(options.env) }),
    ...(options.enableInternet === undefined
      ? {}
      : { enable_internet: options.enableInternet }),
    ...(options.labels === undefined ? {} : { labels: pairs(options.labels) }),
  };
}

/**
 * Activation-bound container controls. State reads always consult the native
 * provider. Accepted controls keep their original event and bounded native
 * admission ticket; only the caller's explicit destroy can recover a failed
 * control sequence. A queued start returns void before provider I/O completes.
 */
export function durableObjectContainer(nativeContext) {
  const current = nativeContext._activation();
  let run;

  function event(context) {
    return context._containerState(container);
  }

  function enqueue(context, operation, input, recover = false, barrier) {
    const record = event(context);
    if (record.failure && !recover) throw record.failure;
    const ticket = context._containerAdmit(operation, input);
    const perform = () =>
      context._call("workers.container.perform", { ticket });
    const previous =
      record.tail && barrier && record.tail !== barrier
        ? Promise.all([record.tail, barrier])
        : (record.tail ?? barrier);
    const pending = previous
      ? previous.then(perform, recover ? perform : undefined)
      : perform();
    record.tail = pending;
    // Preserve the original rejected promise for monitor/destroy. Observing it
    // here prevents an intentionally void start/signal from becoming unhandled.
    pending.then(
      () => {
        if (recover) record.failure = undefined;
      },
      (error) => {
        record.failure = error;
        // A predecessor or bridge-admission failure may leave this ticket
        // unclaimed. Discard only releases that reservation; it never proves
        // rollback of an already-submitted control or changes its outcome.
        try {
          context._sync("workers.container.discard", { ticket });
        } catch {}
      },
    );
    context._containerControl(pending);
    return pending;
  }

  function intercept(context, target, handler) {
    const service = containerServiceTarget(handler, context);
    const record = event(context);
    const previous = record.tail;
    const work = (async () => {
      if (record.failure) throw record.failure;
      if (previous) await previous;
      await context._call("container.intercept_outbound_http", {
        target,
        service,
      });
    })();
    record.tail = work;
    work.then(undefined, (error) => {
      record.failure = error;
    });
    context._containerControl(work);
    return work;
  }

  const container = {
    interceptOutboundHttp(target, handler) {
      const context = current();
      if (typeof target !== "string")
        throw new TypeError("HTTP intercept target must be a string");
      return intercept(context, target, handler);
    },
    interceptAllOutboundHttp(handler) {
      const context = current();
      return intercept(context, null, handler);
    },
    exec(cmd, options) {
      const context = current();
      const record = event(context);
      const controls = (record.execControls ??= execControls());
      const pending = record.tail;
      const work = containerExec(context, cmd, options, (make, cleanup) =>
        controls(async () => {
          if (pending) await pending;
          return make();
        }, cleanup),
      );
      context._containerControl(work);
      return work;
    },
    setInactivityTimeout(timeoutMs) {
      const context = current();
      const record = event(context);
      const controls = (record.execControls ??= execControls());
      const work = (async () => {
        const pending = record.tail;
        if (pending) await pending;
        return controls(() =>
          context.container.setInactivityTimeout(timeoutMs),
        );
      })();
      context._containerControl(work);
      return work;
    },
    getTcpPort(port) {
      return containerPort(current, port, async (context) => {
        const pending = event(context).tail;
        if (pending) await pending;
      });
    },
    get running() {
      return current()._sync("workers.container.running");
    },
    start(options) {
      const context = current();
      const previousStop = run?.stop?.work;
      const started = enqueue(
        context,
        "container.start",
        { options: startOptions(options) },
        false,
        previousStop,
      );
      run = {
        started,
        monitors: new Set(),
        stop: undefined,
        terminal: undefined,
      };
    },
    signal(signal) {
      const context = current();
      enqueue(context, "container.signal", { signal });
    },
    monitor() {
      const context = current();
      if (!run) {
        if (!container.running) {
          throw new Error(
            "there is no container instance that can be provided to this durable object",
          );
        }
        run = {
          started: Promise.resolve(),
          monitors: new Set(),
          stop: undefined,
          terminal: undefined,
        };
      }
      const observed = run;
      const before = event(context).tail ?? observed.started;
      return context._containerMonitor(() =>
        before.then(async () => {
          if (observed.terminal) {
            if (observed.terminal.ok) return;
            throw observed.terminal.error;
          }
          const native = context.container.monitor();
          observed.monitors.add(native);
          let code;
          try {
            code = await native;
          } finally {
            observed.monitors.delete(native);
          }
          if (observed.stop) {
            await observed.stop.work;
            if (observed.stop.hasError) throw observed.stop.error;
            return;
          }
          if (code !== 0) {
            const error = new Error(
              `container exited with unexpected exit code: ${code}`,
            );
            error.exitCode = code;
            throw error;
          }
        }),
      );
    },
    destroy(error) {
      const context = current();
      const observed = run;
      const destroyed = enqueue(context, "container.destroy", {}, true);
      const hasError = arguments.length !== 0;
      const work = destroyed.then(async () => {
        // Settle every native read of this run before a later start can replace
        // the engine's current-run exit observation. Wait on raw reads, never
        // wrappers which themselves await this destroy acknowledgement.
        if (observed) {
          await Promise.allSettled([...observed.monitors]);
          observed.terminal = hasError ? { ok: false, error } : { ok: true };
        }
      });
      if (observed) observed.stop = { work, hasError, error };
      context._containerControl(work);
      event(context).tail = work;
      return work;
    },
  };
  // Persisted-clone/RPC codecs must not degrade a live native capability to data.
  Object.defineProperty(container, "__cellar_runtime_capability__", {
    value: () => {},
    enumerable: true,
  });
  return Object.freeze(container);
}
