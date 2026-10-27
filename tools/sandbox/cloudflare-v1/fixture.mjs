// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { createHash } from "node:crypto";
import { Files, SandboxFileError } from "@cloudflare/sandbox";
import { DurableObject } from "cloudflare:workers";
import { workersApplication } from "../../../packages/cellar-cloudflare/index.js";
import { terminalProgram } from "./terminal-program.mjs";

const length = 2 * 1024 * 1024 + 17;
let saved;
let savedTerminal;
let nativeContext;
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
    if (route === "/terminal") {
      const command = ["node", "-e", terminalProgram];
      const terminal = await container.exec(command, {
        pty: { cols: 80, rows: 24 },
      });
      check(
        terminal.isPty && terminal.pid > 1 && terminal.stderr === null,
        "terminal allocation",
      );
      const reader = terminal.stdout.getReader();
      const writer = terminal.stdin.getWriter();
      const decoder = new TextDecoder();
      let buffered = "",
        transcript = "";
      async function marker() {
        for (;;) {
          const newline = buffered.indexOf("\n");
          if (newline >= 0) {
            const line = buffered.slice(0, newline).replace(/\r$/, "");
            buffered = buffered.slice(newline + 1);
            if (line.startsWith("cellar-pty:"))
              return JSON.parse(line.slice(11));
            continue;
          }
          const { value, done } = await reader.read();
          check(!done, "terminal ended before its marker");
          const text = decoder.decode(value, { stream: true });
          check(
            transcript.length + text.length <= 4096,
            "terminal transcript quota",
          );
          transcript += text;
          buffered += text;
        }
      }
      const initial = await marker();
      check(
        initial.stage === "initial" &&
          initial.tty &&
          initial.pid === terminal.pid &&
          initial.cols === 80 &&
          initial.rows === 24,
        "initial terminal identity/geometry",
      );
      let invalid = false;
      try {
        terminal.resize(0, 24);
      } catch (error) {
        invalid = error instanceof RangeError;
      }
      check(invalid, "invalid terminal resize did not refuse synchronously");
      check(
        terminal.resize(132, 40) === undefined,
        "terminal resize must return void",
      );
      await writer.write(new TextEncoder().encode("resized\n"));
      const resized = await marker();
      check(
        resized.stage === "resized" &&
          resized.pid === terminal.pid &&
          resized.cols === 132 &&
          resized.rows === 40,
        "resized terminal geometry",
      );
      await writer.write(new TextEncoder().encode("done\n"));
      check((await marker()).stage === "done", "terminal completion marker");
      while (!(await reader.read()).done) {}
      reader.releaseLock();
      check((await terminal.exitCode) === 0, "terminal process failed");
      await writer.close();
      writer.releaseLock();
      check(
        transcript.includes("pty-stderr\r\n"),
        "terminal stderr/CRLF was not combined",
      );
      savedTerminal = terminal;
      const fast = await container.exec(
        [
          "node",
          "-e",
          "console.log(JSON.stringify({pid:process.pid,cols:process.stdout.columns,rows:process.stdout.rows}))",
        ],
        { pty: true },
      );
      const output = await fast.output();
      const defaults = JSON.parse(new TextDecoder().decode(output.stdout));
      check(
        output.exitCode === 0 &&
          defaults.pid === fast.pid &&
          defaults.cols === 80 &&
          defaults.rows === 24,
        "default terminal geometry/automatic retirement",
      );
      return Response.json({ initial, resized, defaults, combined: true });
    }
    if (route === "/stdin") {
      const native = nativeContext;
      let offset = 0;
      const input = new ReadableStream({
        type: "bytes",
        pull(controller) {
          if (offset === length) {
            controller.close();
            return;
          }
          const bytes = new Uint8Array(Math.min(131_071, length - offset));
          for (let index = 0; index < bytes.length; index++)
            bytes[index] = (offset + index) % 251;
          offset += bytes.length;
          controller.enqueue(bytes);
        },
      });
      const process = await this.ctx.blockConcurrencyWhile(() =>
        container.exec(
          [
            "node",
            "-e",
            "const hash=require('node:crypto').createHash('sha256');let read=0;process.stdin.on('data',bytes=>{read+=bytes.length;hash.update(bytes)});process.stdin.on('end',()=>setTimeout(()=>process.stdout.write(JSON.stringify({read,sha256:hash.digest('hex')})),100))",
          ],
          { stdin: input },
        ),
      );
      check(process.stdin === null, "source input exposed a second writer");
      const output = await process.output();
      check(
        output.exitCode === 0 && !input.locked,
        "source input did not join",
      );
      const received = JSON.parse(new TextDecoder().decode(output.stdout));
      check(received.read === length, "source input length");

      const silent = await this.ctx.blockConcurrencyWhile(() =>
        container.exec(
          [
            "node",
            "-e",
            "process.stdin.resume();process.stdin.on('end',()=>setTimeout(()=>{process.stdout.write('out');process.stderr.write('err');setTimeout(()=>process.exit(0),100)},100))",
          ],
          {
            stdin: new ReadableStream({
              start(controller) {
                controller.close();
              },
            }),
            stdout: "ignore",
            stderr: "ignore",
          },
        ),
      );
      const ignored = await silent.output();
      check(
        ignored.exitCode === 0 &&
          ignored.stdout.byteLength === 0 &&
          ignored.stderr.byteLength === 0,
        "admitted process observations lost event ownership",
      );

      let pulls = 0,
        refusal;
      const transactional = new ReadableStream(
        {
          pull() {
            pulls++;
          },
        },
        { highWaterMark: 0 },
      );
      await this.ctx.storage.transaction(async () => {
        try {
          await container.exec(["node", "-e", "process.exit(99)"], {
            stdin: transactional,
          });
        } catch (error) {
          refusal = error;
        }
      });
      check(
        refusal?.code === "ValidationError" &&
          refusal.outcome === "not_committed" &&
          pulls === 0 &&
          !transactional.locked,
        "transactional process allocation was not refused before effects",
      );
      let outputRefusal;
      await this.ctx.storage.transaction(async () => {
        try {
          await native.container.execOutput(
            { cmd: ["node", "-e", "process.exit(99)"] },
            32,
          );
        } catch (error) {
          outputRefusal = error;
        }
      });
      check(
        outputRefusal?.code === "ValidationError" &&
          outputRefusal.outcome === "not_committed",
        "transactional buffered process allocation was not refused",
      );

      // Reuse source-retention and native-handle capacity beyond both limits.
      for (let index = 0; index < 130; index++) {
        const source = new ReadableStream({
          start(controller) {
            controller.enqueue(Uint8Array.of(index));
            controller.close();
          },
        });
        const process = await container.exec(["cat"], { stdin: source });
        const output = await process.output();
        check(
          output.exitCode === 0 &&
            output.stdout.byteLength === 1 &&
            output.stderr.byteLength === 0 &&
            new Uint8Array(output.stdout)[0] === index &&
            !source.locked,
          "sequential source process did not retire",
        );
      }

      let cancelled = false;
      const pending = new ReadableStream(
        {
          pull() {},
          cancel() {
            cancelled = true;
          },
        },
        { highWaterMark: 0 },
      );
      const early = await container.exec(
        ["node", "-e", "process.stdout.write('early')"],
        { stdin: pending },
      );
      const stopped = await early.output();
      check(
        stopped.exitCode === 0 && cancelled && !pending.locked,
        "early process exit stranded source input",
      );

      const invalid = new ReadableStream({
        start(controller) {
          controller.enqueue("not bytes");
        },
      });
      const failed = await container.exec(
        ["node", "-e", "process.stdin.resume();setInterval(()=>{},1000)"],
        { stdin: invalid },
      );
      let refused = false;
      try {
        await failed.output();
      } catch (error) {
        refused = error instanceof TypeError;
      }
      check(
        refused && !invalid.locked,
        "invalid source became successful output",
      );
      return Response.json({
        ...received,
        early: true,
        sourceFailure: true,
        callback: true,
        transaction: true,
      });
    }
    if (route === "/late-read") {
      const gate = Promise.withResolvers();
      const native = nativeContext;
      let late, storage;
      const process = await this.ctx.blockConcurrencyWhile(async () => {
        const process = await container.exec(
          [
            "node",
            "-e",
            "process.stdin.resume();process.stdin.on('end',()=>setTimeout(()=>process.stdout.write('late'),100))",
          ],
          { stdin: "pipe" },
        );
        late = gate.promise.then(async () => {
          const reader = process.stdout.getReader();
          try {
            await reader.read();
            throw new Error("closed callback read succeeded");
          } catch (error) {
            return { code: error.code, outcome: error.outcome };
          } finally {
            reader.releaseLock();
          }
        });
        storage = gate.promise.then(async () => {
          try {
            await native._call("storage.apply", {
              $event_owned: true,
              puts: { "source-escape": true },
            });
            throw new Error("observation flag granted storage authority");
          } catch (error) {
            return { code: error.code, outcome: error.outcome };
          }
        });
        return process;
      });
      await process.stdin.close();
      gate.resolve();
      const result = await late;
      const mutation = await storage;
      const snapshot = await native._call("storage.read", {
        key: "source-escape",
      });
      check(
        mutation.code === "ValidationError" &&
          mutation.outcome === "not_committed" &&
          !snapshot.present,
        "event-owned observation escaped its read-only operation family",
      );
      await process.exitCode.catch(() => {});
      return Response.json({ ...result, storageRefused: true });
    }
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
      let staleTerminal = false;
      try {
        savedTerminal.resize(80, 24);
      } catch (error) {
        staleTerminal = /ContextExpired|ContextMismatch/.test(error.message);
      }
      check(staleTerminal, "saved terminal borrowed a later invocation");
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
export const cells = {
  Workspace: class extends app.cells.Workspace {
    fetch(frame, context) {
      nativeContext = context;
      return super.fetch(frame, context);
    }
  },
};
export async function fetch(request, context) {
  try {
    return await app.fetch(request, context);
  } catch (error) {
    console.error("Sandbox 1.x fixture failed", error.code, error.message);
    throw error;
  }
}
