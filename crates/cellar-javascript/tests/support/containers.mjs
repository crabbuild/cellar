// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

function check(value, label) {
  if (!value) throw new Error(label);
}
const ok = () => ({ status: 200, headers: [], body: "b2s=" });
const request = (url) => ({ url, method: "GET", headers: [], body: "" });
async function rejects(action, outcome) {
  try {
    await action();
  } catch (error) {
    check(error.outcome === outcome, "failure outcome: " + error.code);
    return error;
  }
  throw new Error("expected rejection");
}
async function drain(read) {
  const bytes = [];
  for (;;) {
    const chunk = await read();
    if (!chunk.length) return bytes;
    bytes.push(...chunk);
  }
}
async function receive(socket) {
  let event;
  do {
    event = await socket.receive();
  } while (event.type === "open");
  return event;
}
export class Counter {
  async fetch(req, ctx) {
    try {
      return await this.inspect(req, ctx);
    } catch (error) {
      return {
        status: 500,
        headers: [],
        body: ctx._encode(
          JSON.stringify({
            message: error.message,
            code: error.code,
            outcome: error.outcome,
            stack: error.stack,
          }),
        ),
      };
    }
  }
  async inspect(req, ctx) {
    if (req.url.endsWith("/abort-port")) {
      this.portSignal.aborted = true;
      this.portSignal.listener?.();
      return ok();
    }
    if (req.url.endsWith("/borrow")) {
      await rejects(() => this.old.running(), "not_committed");
      return ok();
    }
    const c = ctx.container;
    const capabilities = await c.capabilities();
    check(capabilities.synchronous_running, "declared synchronous observation");
    check(capabilities.queued_lifecycle, "declared queued lifecycle");
    check(
      capabilities.provider === "audited-native" &&
        capabilities.environment === "linux",
      "configured provider",
    );
    check(
      capabilities.max_resources === 64 &&
        capabilities.operations.includes("container.exec"),
      "provider limits",
    );
    check(
      Object.isFrozen(capabilities) && Object.isFrozen(capabilities.operations),
      "immutable capabilities",
    );
    if (!(await c.running()))
      await c.start({
        env: [["HELLO", "world"]],
        labels: [["app", "adapter"]],
      });
    check(ctx._sync("workers.container.running"), "native provider snapshot");
    const http = c.port(Number(await ctx.variable("HTTP_PORT")));
    if (
      req.url.endsWith("/cancel-port-head") ||
      req.url.endsWith("/cancel-port-body")
    ) {
      const pendingUrl = req.url.endsWith("/cancel-port-body")
        ? "http://container/pending-body"
        : "http://container/pending-head";
      const signal = {
        aborted: true,
        addEventListener(_name, listener) {
          this.listener = listener;
        },
        removeEventListener(_name, listener) {
          if (this.listener === listener) this.listener = undefined;
        },
      };
      const refused = await rejects(
        () => http.fetch(request(pendingUrl), { signal }),
        "not_committed",
      );
      check(refused.code === "Cancelled", "pre-abort keeps cancellation code");
      signal.aborted = false;
      this.portSignal = signal;
      const cancelled = await rejects(
        () => http.fetch(request(pendingUrl), { signal }),
        "unknown",
      );
      check(
        cancelled.code === "Cancelled",
        "native abort keeps cancellation code",
      );
      check(!signal.listener, "port abort unsubscribes");
      await ctx.storage.apply({ puts: { "port-cancelled": true } });
      check(
        (await ctx.storage.read("port-cancelled")).value === true,
        "native storage usable after abort",
      );
      const response = await http.fetch(
        request("http://container/after-abort"),
      );
      check(response.status === 302, "port mutation slot released after abort");
      check(
        await c.running(),
        "aborting a request does not destroy its container",
      );
      await c.destroy();
      return ok();
    }
    if (req.url.endsWith("/leak") || req.url.endsWith("/cancel")) {
      const result = await http.connectWebSocket("http://container/ws");
      check(result.upgraded, "leaked socket opened");
      if (req.url.endsWith("/cancel")) {
        for (;;) await result.socket.receive();
      }
      return ok();
    }
    this.old = c;
    await rejects(() => c.signal(0), "not_committed");
    await rejects(() => c.exec({ cmd: [] }), "not_committed");
    await rejects(() => c.port(0).connect(), "not_committed");
    await rejects(
      () => c.execOutput({ cmd: ["echo-stdin"] }, -1),
      "not_committed",
    );
    await c.setInactivityTimeout(60000);
    await c.signal(15);
    const process = await c.exec({
      cmd: ["echo-stdin"],
      env: [["EXEC", "yes"]],
      cwd: "/tmp",
      user: "1000",
    });
    await process.write(new Uint8Array([0, 255, 42]));
    await process.closeStdin();
    const [stdout, stderr, code] = await Promise.all([
      drain(() => process.readStdout()),
      drain(() => process.readStderr()),
      process.wait(),
    ]);
    check(
      JSON.stringify(stdout) === "[0,255,42]" &&
        stderr.length === 6 &&
        code === 7,
      "exec binary streams",
    );
    await process.release();
    await rejects(() => process.wait(), "not_committed");
    const output = await c.execOutput({ cmd: ["echo-stdin"] }, 64);
    check(
      output.exit_code === 7 &&
        output.stdout.length === 0 &&
        output.stderr.length === 6,
      "buffered exec",
    );
    await rejects(() => c.execOutput({ cmd: ["echo-stdin"] }, 2), "unknown");
    await http.waitReady(1000);
    const response = await http.fetch(
      request("https://untrusted.invalid/path?query=yes"),
    );
    check(
      response.status === 302 && response.text() === "container response",
      "redirect returned",
    );
    const socket = await c
      .port(Number(await ctx.variable("TCP_PORT")))
      .connect();
    await socket.write(new Uint8Array([0, 255, 42]));
    await socket.closeWrite();
    check(
      JSON.stringify(await drain(() => socket.read())) === "[0,255,42]",
      "TCP echo",
    );
    await socket.close();
    const declined = await http.connectWebSocket("http://container/decline");
    check(
      !declined.upgraded && declined.response.status === 302,
      "declined websocket",
    );
    const upgraded = await http.connectWebSocket("ws://container/ws");
    check(upgraded.upgraded, "websocket upgraded");
    await upgraded.socket.sendText("hello");
    const text = await receive(upgraded.socket);
    check(text.type === "text" && text.text === "hello", "websocket text");
    await upgraded.socket.sendBinary(new Uint8Array([0, 255, 42]));
    const binary = await receive(upgraded.socket);
    check(
      binary.type === "binary" &&
        JSON.stringify(Array.from(binary.data)) === "[0,255,42]",
      "websocket binary",
    );
    await upgraded.socket.close();
    const old = await c.exec({ cmd: ["echo-stdin"] });
    await c.destroy();
    const run = await c.start();
    check(typeof run === "string", "opaque run");
    await rejects(() => old.wait(), "unknown");
    await old.release();
    const monitor = c.monitor();
    await c.destroy();
    check(
      (await monitor) === 137 && !(await c.running()),
      "monitor and destroy",
    );
    return ok();
  }
}
export const cells = { Counter };
export async function fetch(req, ctx) {
  await rejects(() => ctx.container.running(), "unknown");
  const value = await ctx.cell("COUNTERS", "alice").fetch(request(req.url));
  if (value.status !== 200)
    return { status: value.status, headers: [], body: ctx._encode(value.body) };
  check(value.text() === "ok", "cell response");
  return ok();
}
