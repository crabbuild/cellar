// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import {
  request,
  type Context,
  type Container,
  type ContainerFileMetadata,
  type ContainerExecOutput,
  type ContainerReconciliation,
} from "../../packages/cellar-sdk/index.js";

async function control(ctx: Context) {
  const container: Container = ctx.container;
  const backup: Container = ctx.containers.get("backup");
  const bindings = await ctx.containers.list();
  const name: string = bindings[0].name;
  void name;
  void backup;
  // @ts-expect-error Declared binding metadata is readonly.
  bindings[0].name = "forged";
  const fencing:
    | "unspecified"
    | "native_cell"
    | "local_checks_and_expiry"
    | "provider_enforced" = (await container.capabilities()).fencing;
  void fencing;
  const synchronous: boolean = (await container.capabilities())
    .synchronous_running;
  void synchronous;
  const queued: boolean = (await container.capabilities()).queued_lifecycle;
  void queued;
  const pty: boolean = (await container.capabilities()).exec_pty;
  void pty;
  const terminal = await container.exec({
    cmd: ["sh"],
    pty: { cols: 80, rows: 24 },
  });
  const terminalInfo = await terminal.info();
  const pid: number | null = terminalInfo.pid;
  void pid;
  // @ts-expect-error Allocation metadata is readonly.
  terminalInfo.pid = 1;
  await terminal.resize(132, 40);
  const reconciliation: ContainerReconciliation = await container.reconcile();
  const generation: string | null = reconciliation.generation;
  void generation;
  // @ts-expect-error Lifecycle evidence cannot be changed by the application.
  reconciliation.state = "running";
  const running: boolean = await container.running();
  if (!running) {
    const run: string = await container.start({
      entrypoint: ["/app/server"],
      env: [["MODE", "worker"]],
      enable_internet: false,
      labels: [["app", "example"]],
    });
    void run;
  }
  await container.setInactivityTimeout(60000);
  await container.interceptOutboundHttp("*.example.test", "API");
  await container.interceptOutboundHttp(null, "API");
  // @ts-expect-error: portable interception selects a declared service binding
  await container.interceptOutboundHttp(null, () => new Response());
  const output: ContainerExecOutput = await container.execOutput(
    { cmd: ["/app/tool"] },
    4096,
  );
  const bytes: Uint8Array = output.stdout;
  await container.writeFile("/tmp/input.bin", bytes, { mode: 0o640 });
  const content: Uint8Array = await container.readFile("/tmp/input.bin", 4096);
  const metadata: ContainerFileMetadata =
    await container.statFile("/tmp/input.bin");
  const files: readonly ContainerFileMetadata[] = await container.listFiles(
    "/tmp",
    { max_bytes: 4096, max_entries: 128 },
  );
  void content;
  void metadata;
  void files;
  // @ts-expect-error Files preserve binary bytes; strings require explicit encoding.
  await container.writeFile("/tmp/input.bin", "text");
  // @ts-expect-error Portable metadata cannot be mutated.
  metadata.size = 0;
  const process = await container.exec({
    cmd: ["/app/tool"],
    cwd: "/app",
    user: "1000",
    combined: false,
  });
  await process.write(bytes);
  await process.closeStdin();
  const streams: [Uint8Array, Uint8Array, number] = await Promise.all([
    process.readStdout(),
    process.readStderr(),
    process.wait(),
  ]);
  void streams;
  await process.kill();
  await process.release();
  const port = container.port(8080);
  await port.waitReady(1000);
  const response = await port.fetch(request("http://container/hello"));
  await port.fetch(request("http://container/hello"), {
    signal: new AbortController().signal,
  });
  const streamed = await port.fetchStream(request("http://container/events"), {
    signal: new AbortController().signal,
  });
  const nextChunk: Uint8Array | null = await streamed.read();
  void nextChunk;
  for await (const chunk of streamed.chunks()) {
    const bytes: Uint8Array = chunk;
    void bytes;
  }
  await streamed.cancel();
  const text: string = response.text();
  void text;
  const socket = await port.connect();
  await socket.write(bytes);
  await socket.closeWrite();
  const read: Uint8Array = await socket.read();
  void read;
  await socket.close();
  const upgrade = await port.connectWebSocket("ws://container/ws");
  if (upgrade.upgraded) {
    const headers: readonly (readonly [string, string])[] = upgrade.headers;
    void headers;
    await upgrade.socket.sendText("hello");
    await upgrade.socket.sendBinary(bytes);
    const event = await upgrade.socket.receive();
    if (event.type === "binary") {
      const data: Uint8Array = event.data;
      void data;
    }
    await upgrade.socket.close(1000, "done");
  } else {
    const status: number = upgrade.response.status;
    void status;
  }
  await container.signal(15);
  const monitor: Promise<number> = container.monitor();
  await container.destroy();
  await monitor;
  // @ts-expect-error Commands are arrays of arguments, not shell strings.
  await container.exec({ cmd: "tool --flag" });
  // @ts-expect-error Container bytes are typed, not silently coerced strings.
  await socket.write("hello");
  // @ts-expect-error Use the Cellar option spelling, not Workers compatibility.
  await container.start({ enableInternet: true });
}
void control;

async function providerCapabilities(
  context: import("../../packages/cellar-sdk/index.js").Context,
) {
  const capabilities = await context.container.capabilities();
  const environment: "unspecified" | "linux" | "worker_isolate" =
    capabilities.environment;
  const version: 1 = capabilities.format;
  // @ts-expect-error capabilities are immutable
  capabilities.provider = "forged";
  // @ts-expect-error operation discovery is immutable
  capabilities.operations.push("container.snapshot");
  return { environment, version };
}
void providerCapabilities;
