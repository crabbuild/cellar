import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdir, open } from "node:fs/promises";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { setTimeout as delay } from "node:timers/promises";
import { createDemoServer } from "./server.mjs";

const host = process.env.CELLAR_AI_HOST;
if (!host)
  throw new Error(
    "Set CELLAR_AI_HOST to the development cellar-vercel-agent-example executable",
  );
const provider = process.env.CELLAR_AI_PROVIDER ?? "openai";
const model =
  provider === "fixture"
    ? "increment-three-v1"
    : (process.env.CELLAR_AI_MODEL ?? "gpt-6-luna");
if (provider === "openai" && !process.env.OPENAI_API_KEY)
  throw new Error(
    "Load OPENAI_API_KEY with node --env-file=/path/to/openai.env",
  );
const directory = resolve(
  process.env.CELLAR_AI_DEMO_DATA ?? ".cellar/vercel-chat",
);
await mkdir(directory, { recursive: true });
// Prevent two demo launchers from competing for this bucket or port.
const lock = await open(resolve(directory, "demo.lock"), "wx").catch(() => {
  throw new Error(
    "Demo directory is already in use. After an unclean launcher exit, remove demo.lock only after confirming its process stopped.",
  );
});
await lock.writeFile(String(process.pid));
let child,
  exited,
  backendUrl,
  url,
  observerToken = "",
  stopping = false,
  restarting = false;
const demo = createDemoServer({
  backend: () => backendUrl,
  token: () => observerToken,
  provider,
  model,
  directory,
  restart: async (reset) => {
    if (restarting) throw new Error("Restart already in progress");
    restarting = true;
    try {
      backendUrl = undefined;
      observerToken = randomUUID();
      reset();
      child?.kill("SIGKILL");
      await exited;
      await start();
    } finally {
      restarting = false;
    }
  },
});
async function start() {
  if (stopping) throw new Error("Demo is stopping");
  const generation = randomUUID();
  observerToken = randomUUID();
  let output = "",
    error;
  child = spawn(
    resolve(host),
    ["--listen", "127.0.0.1:0", "--internal-listen", "127.0.0.1:0"],
    {
      env: {
        ...process.env,
        CELLAR_AI_PROVIDER: provider,
        CELLAR_AI_MODEL: model,
        CELLAR_NODE_ENTRY: fileURLToPath(
          new URL("../dist/app.js", import.meta.url),
        ),
        CELLAR_NODE_EXECUTABLE: process.execPath,
        CELLAR_NODE: `ai-demo-${generation}`,
        CELLAR_BUCKET: "cellar-dev",
        CELLAR_INTERNAL_DEV_STORE: resolve(directory, "bucket.sqlite"),
        CELLAR_TEST_DATA_DIR: resolve(directory, generation),
        CELLAR_DURABILITY: "bucket",
        CELLAR_TOKIO_THREADS: "2",
        CELLAR_PLACEMENT_WEIGHT: "1",
        CELLAR_WAKER_TICK_MS: "250",
        CELLAR_REBALANCE_INTERVAL_MS: "0",
        CELLAR_HANDLER_BUDGET_S: "120",
        CELLAR_SHUTDOWN_TOTAL_MS: "8000",
        CELLAR_AI_EVENTS_URL: `${url}/internal/events`,
        CELLAR_AI_EVENTS_TOKEN: observerToken,
        CELLAR_AI_SERVICES_URL: url,
        CELLAR_AI_SERVICES_TOKEN: observerToken,
        RUST_LOG: "warn",
      },
      stdio: ["ignore", "pipe", "pipe"],
    },
  );
  exited = new Promise((resolve) => child.once("close", resolve));
  child.on("error", (e) => {
    error = e;
  });
  child.stdout.on("data", (bytes) => {
    output = (output + bytes).slice(-16000);
  });
  // Keep diagnostics local; never forward provider/host diagnostics to the browser.
  child.stderr.on("data", () => {});
  const deadline = Date.now() + 30000;
  while (Date.now() < deadline) {
    if (error || child.exitCode !== null)
      throw new Error("Worker failed to start");
    const address = /cellar listening on (\S+)/.exec(output)?.[1];
    if (address) {
      try {
        if (
          (
            await fetch(`http://${address}/.well-known/cellar/health`, {
              signal: AbortSignal.timeout(1000),
            })
          ).ok
        ) {
          backendUrl = `http://${address}`;
          return;
        }
      } catch {
        /* Wait for readiness. */
      }
    }
    await delay(100);
  }
  throw new Error("Worker readiness timed out");
}
async function stop() {
  if (stopping) return;
  stopping = true;
  await demo.close();
  child?.kill("SIGTERM");
  if (child) {
    await Promise.race([exited, delay(10000, undefined, { ref: false })]);
    if (child.pid && child.exitCode === null && child.signalCode === null) {
      child.kill("SIGKILL");
      await exited;
    }
  }
  await lock.close();
  const { unlink } = await import("node:fs/promises");
  await unlink(resolve(directory, "demo.lock"));
}
process.on("SIGINT", () => void stop());
process.on("SIGTERM", () => void stop());
try {
  await new Promise((resolve, reject) => {
    demo.server.once("error", reject);
    demo.server.listen(
      Number(process.env.CELLAR_AI_DEMO_PORT ?? 4318),
      "127.0.0.1",
      resolve,
    );
  });
  url = `http://127.0.0.1:${demo.server.address().port}`;
  await start();
  console.log(`Cellar AI chat: ${url} (${provider} / ${model})`);
} catch (error) {
  await stop();
  throw error;
}
