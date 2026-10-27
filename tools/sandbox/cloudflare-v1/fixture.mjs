// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { createHash } from "node:crypto";
import { Files, SandboxFileError } from "@cloudflare/sandbox";
import { DurableObject } from "cloudflare:workers";
import { workersApplication } from "../../../packages/cellar-cloudflare/index.js";

const length = 2 * 1024 * 1024 + 17;
let saved;
function check(condition, message) {
  if (!condition) throw new Error(message);
}

class Workspace extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.files = new Files(ctx.container);
  }
  async fetch(request) {
    try {
      return await this.qualify(request);
    } catch (error) {
      console.error(
        "Sandbox 1.x target failed",
        error.name,
        error.code,
        error.message,
      );
      throw error;
    }
  }
  async qualify(request) {
    const container = this.ctx.container;
    if (!container.running)
      container.start({
        entrypoint: ["sleep", "infinity"],
        enableInternet: false,
      });
    await container.setInactivityTimeout(60_000);
    const route = new URL(request.url).pathname;
    if (route === "/write") {
      const unavailable = await container.exec(["/cellar-missing-executable"]);
      const missingCommand = await unavailable.output();
      check(
        missingCommand.exitCode === 127 &&
          missingCommand.stdout.byteLength === 0,
        "shell-backed executable lookup failure",
      );
      const process = await container.exec([
        "node",
        "-e",
        "process.stdout.write(Buffer.from([0,255,42]));process.stderr.write('stderr');process.exitCode=7",
      ]);
      const output = await process.output();
      check(
        output.exitCode === 7 &&
          Buffer.from(output.stdout).equals(Buffer.from([0, 255, 42])) &&
          Buffer.from(output.stderr).toString() === "stderr",
        "binary exec output",
      );
      saved = process;
      await this.files.mkdir("/workspace/data", { recursive: true });
      let offset = 0;
      const content = new ReadableStream({
        pull(controller) {
          if (offset === length) {
            controller.close();
            return;
          }
          const bytes = new Uint8Array(Math.min(131_071, length - offset));
          for (let i = 0; i < bytes.length; i++) bytes[i] = (offset + i) % 251;
          offset += bytes.length;
          controller.enqueue(bytes);
        },
      });
      await this.files.writeFile("/workspace/data/binary", content);
      await this.files.rename(
        "/workspace/data/binary",
        "/workspace/data/renamed",
      );
      const stat = await this.files.stat("/workspace/data/renamed");
      check(stat.size === BigInt(length), "stat size");
      const parallel = await Promise.all([
        this.files.stat("/workspace/data/renamed"),
        this.files.stat("/workspace/data/renamed"),
      ]);
      check(
        parallel.every((value) => value.size === BigInt(length)),
        "parallel helper controls",
      );
      // More sequential helpers than the native per-event handle capacity.
      // Completed processes must release their native handles in this event.
      for (let index = 0; index < 70; index++)
        check(
          (await this.files.stat("/workspace/data/renamed")).size ===
            BigInt(length),
          "repeated stat size",
        );
      let missing = false;
      try {
        await this.files.stat("/workspace/missing");
      } catch (error) {
        missing = SandboxFileError.is(error) && error.code === "ENOENT";
      }
      check(missing, "typed missing-file error");
      return Response.json({ written: length });
    }
    if (route === "/read") {
      let refused = false;
      try {
        await saved.kill(9);
      } catch (error) {
        refused =
          error.code === "ContextMismatch" || error.code === "ContextExpired";
      }
      check(refused, "saved process borrowed later authority");
      const response = await this.files.readFile("/workspace/data/renamed");
      const hash = createHash("sha256");
      let read = 0;
      for await (const chunk of response.body) {
        hash.update(chunk);
        read += chunk.byteLength;
      }
      check(read === length, "binary read size");
      await this.files.remove("/workspace/data", { recursive: true });
      return Response.json({ read, sha256: hash.digest("hex") });
    }
    if (route === "/faults") {
      const oversized = await container.exec([
        "node",
        "-e",
        "process.stdout.write(Buffer.alloc(131072));setInterval(()=>{},1000)",
      ]);
      let bounded = false;
      try {
        await oversized.output();
      } catch (error) {
        bounded = error instanceof RangeError;
      }
      check(bounded, "oversized output became successful truncation");
      // Dropping observation/stream handles does not kill an accepted process.
      // The owning application explicitly retires this container before reuse.
      await container.destroy();
      container.start({
        entrypoint: ["sleep", "infinity"],
        enableInternet: false,
      });
      await container.setInactivityTimeout(60_000);
      const controller = new AbortController();
      const process = await container.exec(
        [
          "node",
          "-e",
          "process.stdout.write('ready');setInterval(()=>{},1000)",
        ],
        { signal: controller.signal, stderr: "ignore" },
      );
      const reader = process.stdout.getReader();
      check(
        new TextDecoder().decode((await reader.read()).value) === "ready",
        "abort probe did not start",
      );
      controller.abort();
      const code = await process.exitCode;
      check(
        Number.isInteger(code) && code !== 0,
        "live abort did not terminate its process",
      );
      await reader.cancel();
      reader.releaseLock();
      return Response.json({ bounded, aborted: true });
    }
    if (route === "/destroy") {
      await container.destroy();
      return new Response("destroyed");
    }
    return new Response("unknown route", { status: 404 });
  }
}
const app = workersApplication(
  {
    Workspace,
    default: {
      fetch(request, env) {
        return env.WORKSPACES.getByName("example").fetch(request);
      },
    },
  },
  { namespaces: { WORKSPACES: "Workspace" }, containers: ["Workspace"] },
);
export const cells = app.cells;
export async function fetch(request, context) {
  try {
    return await app.fetch(request, context);
  } catch (error) {
    console.error("Sandbox 1.x fixture failed", error.code, error.message);
    throw error;
  }
}
