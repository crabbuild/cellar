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
    if (req.url.endsWith("/borrow-stream")) {
      await rejects(() => this.oldStream.read(), "not_committed");
      return ok();
    }
    if (req.url.endsWith("/borrow")) {
      await rejects(() => this.old.running(), "not_committed");
      return ok();
    }
    const c = ctx.container;
    if (req.url.endsWith("/borrow-terminal")) {
      const error = await rejects(
        () => this.oldTerminal.info(),
        "not_committed",
      );
      check(
        ["ContextExpired", "ContextMismatch"].includes(error.code),
        "terminal authority",
      );
      return ok();
    }
    if (req.url.endsWith("/terminal")) {
      check((await c.capabilities()).exec_pty, "declared terminal execution");
      if (!(await c.running())) await c.start();
      const process = await c.exec({
        cmd: ["echo-stdin"],
        pty: { cols: 80, rows: 24 },
      });
      const info = await process.info();
      check(
        info.is_pty && info.pid === null && Object.isFrozen(info),
        "terminal allocation metadata",
      );
      await rejects(() => process.resize(0, 24), "not_committed");
      await process.resize(132, 40);
      await process.write(Uint8Array.of(0, 255, 42));
      await process.closeStdin();
      const [stdout, stderr, code] = await Promise.all([
        drain(() => process.readStdout()),
        drain(() => process.readStderr()),
        process.wait(),
      ]);
      check(
        stdout.join(",") ===
          [0, 255, 42, 115, 116, 100, 101, 114, 114].join(","),
        "raw terminal output",
      );
      check(stderr.length === 0 && code === 7, "combined terminal channels");
      this.oldTerminal = process;
      await process.release();
      await c.destroy();
      return ok();
    }
    for (const operation of [
      "workers.container.fetch",
      "workers.container.fetch_stream",
    ]) {
      const profile = await rejects(
        () =>
          ctx._call(operation, {
            port: 8080,
            request: request("http://container/profile-refused"),
          }),
        "not_committed",
      );
      check(profile.code === "ValidationError", "Workers profile required");
    }
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
    const reconciled = await c.reconcile();
    check(
      reconciled.state === "running" && reconciled.generation === null,
      "native observation without a remote journal",
    );
    const http = c.port(Number(await ctx.variable("HTTP_PORT")));
    if (req.url.endsWith("/leak-stream")) {
      this.oldStream = await http.fetchStream(
        request("http://container/stream-body"),
      );
      return ok();
    }
    if (req.url.endsWith("/streams-limit")) {
      const streams = [];
      try {
        for (let i = 0; i < 64; i++)
          streams.push(
            await http.fetchStream(request("http://container/stream-body")),
          );
        const refused = await rejects(
          () => http.fetchStream(request("http://container/stream-body")),
          "not_committed",
        );
        check(refused.code === "ValidationError", "response handle quota");
        await rejects(() => c.exec(["echo-stdin"]), "not_committed");
      } finally {
        for (const stream of streams) await stream.cancel();
      }
      const next = await http.fetchStream(
        request("http://container/stream-body"),
      );
      await next.cancel();
      await c.destroy();
      return ok();
    }
    if (
      req.url.endsWith("/cancel-port-head") ||
      req.url.endsWith("/cancel-port-body") ||
      req.url.endsWith("/cancel-port-stream")
    ) {
      const streaming = req.url.endsWith("/cancel-port-stream");
      const pendingUrl = streaming
        ? "http://container/stream-body"
        : req.url.endsWith("/cancel-port-body")
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
      let responseStream;
      if (streaming) {
        responseStream = await http.fetchStream(request(pendingUrl), {
          signal,
        });
        check(responseStream.status === 200, "stream head before EOF");
        const first = responseStream.body.length
          ? responseStream.body
          : await responseStream.read();
        check(first.join(",") === "0,255,42", "binary first chunk before EOF");
      }
      const cancelled = await rejects(
        () =>
          streaming
            ? responseStream.read()
            : http.fetch(request(pendingUrl), { signal }),
        "unknown",
      );
      if (streaming) await responseStream.cancel();
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
    const streamed = await http.fetchStream(request("http://container/finite"));
    check(streamed.status === 302, "streamed redirect head");
    const streamedBytes = [];
    for await (const bytes of streamed.chunks()) streamedBytes.push(...bytes);
    check(
      String.fromCharCode(...streamedBytes) === "container response",
      "finite streamed body",
    );
    await streamed.cancel();
    this.old = c;
    const filePath = "/tmp/cellar-?é.bin";
    let refusedText = false;
    try {
      await c.writeFile(filePath, "text");
    } catch (error) {
      refusedText = error instanceof TypeError;
    }
    check(refusedText, "file bytes require explicit encoding");
    await rejects(() => c.readFile("../outside", 3), "not_committed");
    await rejects(
      () => c.writeFile(filePath, new Uint8Array(), { mode: 0o4755 }),
      "not_committed",
    );
    await c.writeFile(filePath, new Uint8Array([0, 255, 42]), { mode: 0o640 });
    check(
      JSON.stringify(Array.from(await c.readFile(filePath, 3))) ===
        "[0,255,42]",
      "file binary round trip",
    );
    await rejects(() => c.readFile(filePath, 2), "unknown");
    const metadata = await c.statFile(filePath);
    check(
      metadata.name === "cellar-?é.bin" &&
        metadata.kind === "file" &&
        metadata.size === 3 &&
        metadata.mode === 0o640 &&
        Object.isFrozen(metadata),
      "portable file metadata",
    );
    await c.writeFile("/tmp/empty", new Uint8Array());
    check(
      (await c.readFile("/tmp/empty", 0)).length === 0,
      "empty file with zero budget",
    );
    const files = await c.listFiles("/tmp", {
      max_bytes: 4096,
      max_entries: 2,
    });
    check(
      Object.isFrozen(files) &&
        files.length === 2 &&
        files[0].name === metadata.name &&
        files.every(Object.isFrozen),
      "immutable direct listing",
    );
    await rejects(
      () => c.listFiles("/tmp", { max_bytes: 4096, max_entries: 1 }),
      "unknown",
    );
    await rejects(() => c.signal(0), "not_committed");
    await rejects(() => c.exec({ cmd: [] }), "not_committed");
    await rejects(() => c.port(0).connect(), "not_committed");
    await rejects(
      () => c.execOutput({ cmd: ["echo-stdin"] }, -1),
      "not_committed",
    );
    await c.writeFile("/tmp/large", new Uint8Array(25000));
    await rejects(() => c.readFile("/tmp/large", 3), "unknown");
    await rejects(
      () => c.listFiles("/tmp", { max_bytes: 4096, max_entries: 128 }),
      "unknown",
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
    check(
      upgraded.headers.some(
        ([name, value]) => name === "upgrade" && value === "websocket",
      ),
      "upgrade headers preserved",
    );
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
