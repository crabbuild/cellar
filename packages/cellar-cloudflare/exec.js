// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { httpLimit, webRuntime } from "./http.js";
import { ownedStdin } from "./exec-stdin.js";

function terminalSize(value) {
  if (value === undefined || value === false) return;
  if (
    value !== true &&
    (!value || typeof value !== "object" || Array.isArray(value))
  )
    throw new TypeError("Exec pty requires true or terminal dimensions");
  if (
    value !== true &&
    Object.keys(value).some((key) => !["cols", "rows"].includes(key))
  )
    throw new TypeError("Unknown terminal size option");
  const cols = value === true || value.cols === undefined ? 80 : value.cols;
  const rows = value === true || value.rows === undefined ? 24 : value.rows;
  for (const dimension of [cols, rows])
    if (!Number.isInteger(dimension) || dimension < 1 || dimension > 65535)
      throw new RangeError("Terminal dimensions must be 1..65535");
  return { cols, rows };
}

// One event's process controls serialize before entering the native provider.
// Cleanup has its own bounded admission, so a full ordinary queue cannot strand
// completed native handles. Nothing holds a native lock while waiting here.
export function execControls() {
  let tail = Promise.resolve(),
    ordinary = 0,
    cleanup = 0;
  return (make, retiring = false) => {
    if ((retiring ? cleanup : ordinary) >= 64)
      return Promise.reject(new RangeError("Exec control queue exceeds limit"));
    if (retiring) cleanup++;
    else ordinary++;
    const pending = tail.then(make);
    tail = pending.catch(() => {});
    const release = () => {
      if (retiring) cleanup--;
      else ordinary--;
    };
    pending.then(release, release);
    return pending;
  };
}

function optionsFor(cmd, options) {
  if (
    !Array.isArray(cmd) ||
    cmd.length === 0 ||
    cmd[0] === "" ||
    (typeof cmd[0] === "string" && cmd[0].startsWith("-")) ||
    cmd.some((value) => typeof value !== "string" || value.includes("\0"))
  )
    throw new TypeError(
      "Exec requires a nonempty executable without a leading hyphen and NUL-free argv",
    );
  if (!options || typeof options !== "object" || Array.isArray(options))
    throw new TypeError("Exec options must be an object");
  for (const key of Object.keys(options)) {
    if (
      ![
        "stdin",
        "stdout",
        "stderr",
        "cwd",
        "env",
        "user",
        "signal",
        "pty",
      ].includes(key)
    )
      throw new TypeError(`Unsupported exec option: ${key}`);
  }
  const pty = terminalSize(options.pty);
  if (pty) {
    if (options.stderr !== undefined && options.stderr !== "combined")
      throw new TypeError("Terminal stderr must be combined");
    options.stderr = "combined";
    if (options.stdin === undefined) options.stdin = "pipe";
  }
  if (
    options.stdin !== undefined &&
    options.stdin !== "pipe" &&
    !(options.stdin instanceof ReadableStream)
  )
    throw new TypeError("Exec stdin requires pipe mode or a ReadableStream");
  const stdout = options.stdout ?? "pipe",
    stderr = options.stderr ?? "pipe";
  if (
    !["pipe", "ignore"].includes(stdout) ||
    !["pipe", "ignore", "combined"].includes(stderr) ||
    (stdout === "ignore" && stderr === "combined")
  )
    throw new TypeError("Invalid exec output modes");
  const env = options.env;
  if (
    env !== undefined &&
    (!env ||
      typeof env !== "object" ||
      Array.isArray(env) ||
      Object.entries(env).some(
        ([key, value]) =>
          !key ||
          /[=\0]/.test(key) ||
          typeof value !== "string" ||
          value.includes("\0"),
      ))
  )
    throw new TypeError("Invalid exec environment");
  for (const key of ["cwd", "user"])
    if (
      options[key] !== undefined &&
      (typeof options[key] !== "string" || options[key].includes("\0"))
    )
      throw new TypeError(`Invalid exec ${key}`);
  if (options.user !== undefined && !/^\d+:\d+$/.test(options.user))
    throw new TypeError("Exec user requires numeric UID:GID");
  if (options.signal !== undefined && !(options.signal instanceof AbortSignal))
    throw new TypeError("Exec signal must be an AbortSignal");
  return {
    cmd: cmd.slice(),
    env: Object.entries(env ?? {}),
    ...(options.cwd === undefined ? {} : { cwd: options.cwd }),
    ...(options.user === undefined ? {} : { user: options.user }),
    combined: stderr === "combined",
    ...(pty ? { pty } : {}),
  };
}

/** Node web-stream profile over one original, invocation-owned native process. */
export async function containerExec(
  context,
  cmd,
  options = {},
  admit = execControls(),
) {
  webRuntime();
  if (!options || typeof options !== "object" || Array.isArray(options))
    throw new TypeError("Exec options must be an object");
  // Application changes after allocation must not change stream or signal policy.
  options = { ...options };
  const nativeOptions = optionsFor(cmd, options);
  const limit = httpLimit(context);
  options.signal?.throwIfAborted();
  const input =
    options.stdin instanceof ReadableStream
      ? ownedStdin(options.stdin)
      : undefined;
  let id, releaseInput, info;
  try {
    if (input) {
      const retained = new Promise((resolve) => {
        releaseInput = resolve;
      });
      // Reserve both event and active callback ownership before external effects.
      context._containerMonitor(() => retained);
      context._containerControl(retained);
    }
    id = await admit(() =>
      context._call("container.exec", { options: nativeOptions }),
    );
    info = await context._call("container.exec.info", { id });
    if (
      !info ||
      typeof info.is_pty !== "boolean" ||
      info.is_pty !== Boolean(nativeOptions.pty) ||
      (info.pid !== null &&
        (!Number.isInteger(info.pid) || info.pid < 2 || info.pid > 2147483647))
    )
      throw new TypeError("Invalid native process metadata");
  } catch (error) {
    // A metadata failure follows accepted allocation. Release the original
    // handle and request termination when still authorized; never allocate again.
    if (id !== undefined) {
      await admit(
        () => context._call("container.exec.kill", { id, signal: 9 }),
        true,
      ).catch(() => {});
      await admit(() => context._call("container.release", { id }), true).catch(
        () => {},
      );
    }
    // No source read begins until allocation is acknowledged.
    input?.release();
    releaseInput?.();
    throw error;
  }
  const observations = new AbortController();
  let tail = Promise.resolve(),
    controls = 0,
    disposal;
  let stdinDone = false,
    stdoutDone = false,
    stderrDone = false,
    exited = false;
  let stdoutUsed = false,
    stderrUsed = false,
    outputUsed = false;
  const call = (name, input = {}, signal) =>
    context._call(
      `container.exec.${name}`,
      { ...input, id },
      undefined,
      signal,
    );
  // Serialize this process's writes, close and signals through native admission.
  const control = (make, cleanup = false) => {
    if (controls >= 64 && !cleanup)
      return Promise.reject(new RangeError("Exec control queue exceeds limit"));
    controls++;
    const pending = tail.then(() => admit(make, cleanup));
    tail = pending.catch(() => {});
    pending.then(
      () => controls--,
      () => controls--,
    );
    return pending;
  };
  const dispose = () => {
    if (!disposal) {
      // One cleanup reservation remains available after ordinary admission fills.
      // Publish the promise before invoking an application source's cancellation.
      disposal = Promise.resolve().then(async () => {
        observations.abort();
        const cancelled = input?.cancel(new Error("Exec observation ended"));
        const results = await Promise.allSettled([
          control(() => context._call("container.release", { id }), true),
          cancelled,
        ]);
        for (const result of results)
          if (result.status === "rejected") throw result.reason;
      });
      disposal.catch(() => {});
    }
    return disposal;
  };
  const releaseIfFinished = () =>
    exited && stdinDone && stdoutDone && stderrDone
      ? dispose()
      : Promise.resolve();
  const abort = () => {
    input?.cancel(options.signal.reason).catch(() => {});
    if (!exited) signalProcess(9).catch(() => {});
  };
  const exitCode = call("wait", {}, observations.signal).then(
    async (code) => {
      exited = true;
      stdinDone = true;
      options.signal?.removeEventListener("abort", abort);
      input?.cancel(new Error("Exec process exited")).catch(() => {});
      await releaseIfFinished();
      return code;
    },
    async (error) => {
      exited = true;
      options.signal?.removeEventListener("abort", abort);
      await dispose().catch(() => {});
      throw error;
    },
  );
  exitCode.catch(() => {});
  function signalProcess(signal) {
    return control(() => call("kill", { signal }));
  }
  function reportedControl(operation, input) {
    let finish, fail;
    const retained = new Promise((resolve, reject) => {
      finish = resolve;
      fail = reject;
    });
    // Void controls reserve event and callback ownership before queueing work.
    // Unlike output(), callers cannot observe a returned control rejection, so
    // a failed acknowledgement is reported through the owning event's jobs.
    let ticket;
    try {
      context._waitUntil(retained);
      context._containerControl(retained);
      ticket = context._containerAdmit(`container.exec.${operation}`, {
        ...input,
        id,
      });
    } catch (error) {
      finish();
      throw error;
    }
    control(() => context._call("workers.container.perform", { ticket })).then(
      finish,
      (error) => {
        // Admission can outlive a queue refusal. Discard only the unconsumed
        // ticket; an accepted provider mutation is never replayed or rolled back.
        try {
          context._sync("workers.container.discard", { ticket });
        } catch {}
        fail(error);
      },
    );
  }
  function kill(signal = 15) {
    if (!Number.isInteger(signal) || signal < 1 || signal > 64)
      throw new RangeError("Exec signal must be 1..64");
    reportedControl("kill", { signal });
  }
  function resize(cols, rows) {
    if (!info.is_pty)
      throw new TypeError("Exec process has no pseudo-terminal");
    for (const dimension of [cols, rows])
      if (!Number.isInteger(dimension) || dimension < 1 || dimension > 65535)
        throw new RangeError("Terminal dimensions must be 1..65535");
    reportedControl("resize", { cols, rows });
  }
  async function closeStdin() {
    if (stdinDone) return;
    if (!exited)
      await control(() => (exited ? undefined : call("close_stdin")));
    stdinDone = true;
    await releaseIfFinished();
  }
  async function writeStdin(bytes) {
    if (!(bytes instanceof Uint8Array))
      throw new TypeError("Exec stdin requires Uint8Array chunks");
    if (stdinDone) throw new TypeError("Exec stdin is closed");
    for (let offset = 0; offset < bytes.byteLength; offset += limit) {
      if (input && (input.cancelled || exited)) break;
      await control(() => {
        if (input && (input.cancelled || exited)) return;
        if (exited) throw new TypeError("Exec stdin is closed");
        return call("write", {
          data: context._encode(bytes.subarray(offset, offset + limit)),
        });
      });
    }
  }
  const drains = [];
  function readable(channel, ignored) {
    const done = async () => {
      if (channel === "stdout") stdoutDone = true;
      else stderrDone = true;
      await releaseIfFinished();
    };
    const read = async () =>
      context._decode(
        await call(
          `read_${channel}`,
          ignored ? { $event_owned: true } : {},
          observations.signal,
        ),
      );
    if (ignored) {
      const draining = (async () => {
        try {
          while ((await read()).byteLength) {}
          await done();
        } catch (error) {
          await dispose().catch(() => {});
          throw error;
        }
      })();
      draining.catch(() => {});
      drains.push(draining);
      return null;
    }
    return new ReadableStream(
      {
        type: "bytes",
        async pull(controller) {
          if (channel === "stdout") stdoutUsed = true;
          else stderrUsed = true;
          try {
            const bytes = await read();
            if (bytes.byteLength) controller.enqueue(bytes);
            else {
              await done();
              controller.close();
            }
          } catch (error) {
            controller.error(error);
            await dispose().catch(() => {});
          }
        },
        async cancel() {
          await done();
        },
      },
      { highWaterMark: 0 },
    );
  }
  const stdout = readable("stdout", options.stdout === "ignore");
  const stderr = readable(
    "stderr",
    ["ignore", "combined"].includes(options.stderr),
  );
  const stdin =
    options.stdin === "pipe"
      ? new WritableStream(
          {
            write: writeStdin,
            close: closeStdin,
            abort: async () => {
              await closeStdin();
            },
          },
          { highWaterMark: 1 },
        )
      : null;
  options.signal?.addEventListener("abort", abort, { once: true });
  if (options.signal?.aborted) abort();
  let inputWork, inputFailure, inputRetirement;
  if (input) {
    const retire = (error) => {
      if (!inputRetirement) {
        // Preserve the initiating failure over secondary teardown cancellation.
        if (!disposal) inputFailure = { error };
        inputRetirement = (async () => {
          if (!exited) await signalProcess(9).catch(() => {});
          await dispose().catch(() => {});
        })();
      }
      return inputRetirement;
    };
    inputWork = input
      .run(writeStdin, closeStdin, retire)
      .catch(async (error) => {
        await retire(error);
        throw error;
      });
    inputWork.catch(() => {});
    // output() reports pump failures; the reserved lifetime only joins work.
    inputWork.then(releaseInput, releaseInput);
  } else if (!stdin) {
    try {
      await closeStdin();
    } catch (error) {
      await dispose().catch(() => {});
      throw error;
    }
  }
  const process = {
    pid: info.pid ?? undefined,
    isPty: info.is_pty,
    stdin,
    stdout,
    stderr,
    exitCode,
    kill,
    resize,
    async output() {
      if (
        outputUsed ||
        stdoutUsed ||
        stderrUsed ||
        stdout?.locked ||
        stderr?.locked
      )
        throw new TypeError("Exec output has already been consumed");
      outputUsed = true;
      let retained = 0;
      async function collect(stream) {
        if (!stream) return new ArrayBuffer(0);
        const reader = stream.getReader(),
          chunks = [];
        try {
          for (;;) {
            const { done, value } = await reader.read();
            if (done) break;
            if (value.byteLength > limit - retained)
              throw new RangeError("Exec output exceeds host body limit");
            retained += value.byteLength;
            chunks.push(value);
          }
          const bytes = new Uint8Array(
            chunks.reduce((sum, chunk) => sum + chunk.byteLength, 0),
          );
          let offset = 0;
          for (const chunk of chunks) {
            bytes.set(chunk, offset);
            offset += chunk.byteLength;
          }
          return bytes.buffer;
        } finally {
          reader.releaseLock();
        }
      }
      const pending = [
        collect(stdout),
        collect(stderr),
        exitCode,
        ...drains,
        inputWork,
      ];
      try {
        const [out, err, code] = await Promise.all(pending);
        await releaseIfFinished();
        return { stdout: out, stderr: err, exitCode: code };
      } catch (error) {
        await dispose().catch(() => {});
        await Promise.allSettled(pending);
        throw inputFailure ? inputFailure.error : error;
      }
    },
  };
  Object.defineProperty(process, "__cellar_runtime_capability__", {
    value: () => {},
    enumerable: true,
  });
  return Object.freeze(process);
}
