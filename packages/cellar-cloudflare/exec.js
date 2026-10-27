// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { httpLimit, webRuntime } from "./http.js";

// One event's process controls share the bridge's conservative mutation slot.
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
      !["stdin", "stdout", "stderr", "cwd", "env", "user", "signal"].includes(
        key,
      )
    )
      throw new TypeError(`Unsupported exec option: ${key}`);
  }
  if (options.stdin !== undefined && options.stdin !== "pipe")
    throw new TypeError("Exec stdin requires pipe mode");
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
  const id = await admit(() =>
    context._call("container.exec", { options: nativeOptions }),
  );
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
      observations.abort();
      // One cleanup reservation remains available after ordinary admission fills.
      disposal = control(
        () => context._call("container.release", { id }),
        true,
      );
      disposal.catch(() => {});
    }
    return disposal;
  };
  const releaseIfFinished = () =>
    exited && stdinDone && stdoutDone && stderrDone
      ? dispose()
      : Promise.resolve();
  const abort = () => {
    if (!exited) kill(9).catch(() => {});
  };
  const exitCode = call("wait", {}, observations.signal).then(
    async (code) => {
      exited = true;
      options.signal?.removeEventListener("abort", abort);
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
  function kill(signal = 9) {
    if (!Number.isInteger(signal) || signal < 1 || signal > 64)
      throw new RangeError("Exec signal must be 1..64");
    return control(() => call("kill", { signal }));
  }
  async function closeStdin() {
    if (stdinDone) return;
    await control(() => call("close_stdin"));
    stdinDone = true;
    await releaseIfFinished();
  }
  const drains = [];
  function readable(channel, ignored) {
    const done = async () => {
      if (channel === "stdout") stdoutDone = true;
      else stderrDone = true;
      await releaseIfFinished();
    };
    const read = async () =>
      context._decode(await call(`read_${channel}`, {}, observations.signal));
    if (ignored) {
      const draining = (async () => {
        try {
          while ((await read()).byteLength) {}
          await done();
        } catch {
          await dispose().catch(() => {});
        }
      })();
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
            async write(bytes) {
              if (!(bytes instanceof Uint8Array))
                throw new TypeError("Exec stdin requires Uint8Array chunks");
              for (let offset = 0; offset < bytes.byteLength; offset += limit)
                await control(() =>
                  call("write", {
                    data: context._encode(
                      bytes.subarray(offset, offset + limit),
                    ),
                  }),
                );
            },
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
  if (!stdin) {
    try {
      await closeStdin();
    } catch (error) {
      await dispose().catch(() => {});
      throw error;
    }
  }
  const process = {
    stdin,
    stdout,
    stderr,
    exitCode,
    kill,
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
      const pending = [collect(stdout), collect(stderr), exitCode, ...drains];
      try {
        const [out, err, code] = await Promise.all(pending);
        await releaseIfFinished();
        return { stdout: out, stderr: err, exitCode: code };
      } catch (error) {
        await dispose().catch(() => {});
        await Promise.allSettled(pending);
        throw error;
      }
    },
  };
  Object.defineProperty(process, "__cellar_runtime_capability__", {
    value: () => {},
    enumerable: true,
  });
  return Object.freeze(process);
}
