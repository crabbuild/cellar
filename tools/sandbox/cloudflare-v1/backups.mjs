// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { createHash } from "node:crypto";
import {
  DirectoryBackup,
  DirectoryBackupGateway,
  Files,
} from "@cloudflare/sandbox";
import { DurableObject, WorkerEntrypoint } from "cloudflare:workers";
import { workersApplication } from "../../../packages/cellar-cloudflare/index.js";
import { directServer, interceptionProgram } from "./interception-program.mjs";
import {
  AbortAfterUploadGateway,
  cancelAllocatedBackup,
  failedBackupOperations,
  preAbortedBackupOperations,
} from "./backup-failures.mjs";
import {
  TransferAbortGateway,
  LostAcknowledgementGateway,
  cancelBackupTransfers,
  lostBackupAcknowledgements,
} from "./backup-boundaries.mjs";
import {
  LostCreateGateway,
  CrashPartGateway,
  loseBackupCreation,
  holdBackupPart,
} from "./backup-recovery.mjs";

const length = 2 * 1024 * 1024 + 17;
function check(condition, message) {
  if (!condition) throw new Error(message);
}
function bytes() {
  return Uint8Array.from({ length }, (_, index) => index % 251);
}
function archiveBytes() {
  let state = 0x6d2b79f5;
  return Uint8Array.from({ length }, () => {
    state ^= state << 13;
    state ^= state >>> 17;
    state ^= state << 5;
    return state & 255;
  });
}
function sha256(value) {
  return createHash("sha256").update(value).digest("hex");
}

class Echo extends WorkerEntrypoint {
  fetch(request) {
    check(
      ["http://route.test/message", "http://before.test/message"].includes(
        request.url,
      ),
      "intercepted request authority",
    );
    const reader = request.body.getReader();
    let received = 0;
    const body = new ReadableStream(
      {
        async pull(controller) {
          try {
            const { value, done } = await reader.read();
            if (done) {
              reader.releaseLock();
              controller.close();
            } else {
              received += value.length;
              controller.enqueue(value);
            }
          } catch (error) {
            console.error(
              "Sandbox intercepted body failed",
              error.code,
              error.message,
              received,
            );
            reader.releaseLock();
            controller.error(error);
          }
        },
        async cancel(reason) {
          try {
            await reader.cancel(reason);
          } finally {
            reader.releaseLock();
          }
        },
      },
      { highWaterMark: 0 },
    );
    return new Response(body, {
      headers: { "x-phase": this.ctx.props.phase },
    });
  }
}

class Workspace extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.files = new Files(ctx.container);
    this.backups = new DirectoryBackup(
      ctx.container,
      ctx.exports.DirectoryBackupGateway,
      {
        binding: "BACKUPS",
        prefix: "qualified/",
      },
    );
  }

  async fetch(request) {
    try {
      return await this.qualify(request);
    } catch (error) {
      console.error("Sandbox backup qualification", error.name, error.message);
      throw error;
    }
  }

  async qualify(request) {
    const route = new URL(request.url).pathname;
    const container = this.ctx.container;
    if (route === "/state") {
      const backup = await this.ctx.storage.get("backup");
      return Response.json({
        backup: backup ?? null,
        running: container.running,
      });
    }
    if (route === "/delete") {
      const record = await this.ctx.storage.get("backup");
      await this.backups.delete(record);
      check(
        (await this.env.BACKUPS.head(`qualified/${record.id}.tar.zst`)) ===
          null,
        "archive survived deletion",
      );
      return Response.json({ deleted: true });
    }
    if (route === "/destroy") {
      await container.destroy();
      return Response.json({ destroyed: true });
    }
    if (route === "/install") {
      // This handler outlives this event; start installs the copied native route
      // before allocating the application. An unrelated hostname stays direct.
      await container.interceptOutboundHttp(
        "before.test",
        this.ctx.exports.Echo({ props: { phase: "before" } }),
      );
      container.start({
        entrypoint: ["node", "-e", directServer],
        enableInternet: true,
      });
      await container.setInactivityTimeout(60_000);
      return Response.json({ installed: true });
    }
    check(container.running, "install the qualification container first");
    if (route === "/routing") return this.routing();
    if (route === "/pre-abort") return preAbortedBackupOperations(this);
    if (route === "/cancel-after-create") return cancelAllocatedBackup(this);
    if (route === "/cancel-transfer") return cancelBackupTransfers(this);
    if (route === "/lost-acks") return lostBackupAcknowledgements(this);
    if (route === "/lost-create") return loseBackupCreation(this);
    if (route === "/crash-part") return holdBackupPart(this);
    if (route === "/faults") return failedBackupOperations(this);
    if (route === "/backup") {
      await this.files.mkdir("/workspace/source", { recursive: true });
      await this.files.writeFile("/workspace/source/binary", archiveBytes());
      const record = await this.backups.backup({
        dir: "/workspace/source",
        name: "native-http-backup",
      });
      await this.ctx.storage.put("backup", record);
      return Response.json({
        format: record.format,
        size: record.size,
        sha256: record.sha256,
      });
    }
    if (route === "/restore") {
      const record = await this.ctx.storage.get("backup");
      await this.files.mkdir("/workspace/source", { recursive: true });
      await this.files.writeFile(
        "/workspace/source/binary",
        "changed after backup",
      );
      await this.backups.restore(record);
      const restored = await this.files.readFile("/workspace/source/binary");
      const body = new Uint8Array(await restored.arrayBuffer());
      check(
        body.length === length && sha256(body) === sha256(archiveBytes()),
        "published backup/restore bytes",
      );
      return Response.json({ bytes: body.length, sha256: sha256(body) });
    }
    throw new Error("unknown qualification route");
  }

  async routing() {
    const process = await this.ctx.container.exec(
      ["node", "-e", interceptionProgram],
      { stdin: "pipe", stderr: "ignore" },
    );
    const writer = process.stdin.getWriter();
    const reader = process.stdout.getReader();
    const decoder = new TextDecoder();
    let buffered = "";
    async function reply(command = "next") {
      await writer.write(new TextEncoder().encode(`${command}\n`));
      for (;;) {
        const end = buffered.indexOf("\n");
        if (end >= 0) {
          const line = buffered.slice(0, end);
          buffered = buffered.slice(end + 1);
          const reply = JSON.parse(line);
          check(!reply.failure, `HTTP probe failed: ${reply.failure}`);
          console.error(
            "Sandbox HTTP probe",
            reply.phase,
            reply.status,
            reply.bytes,
          );
          return reply;
        }
        const { value, done } = await reader.read();
        check(
          !done && buffered.length + value.length <= 4096,
          "bounded routing probe reply",
        );
        buffered += decoder.decode(value, { stream: true });
      }
    }
    const replies = [];
    try {
      replies.push(await reply("before"));
      replies.push(await reply());
      for (const phase of ["one", "two"]) {
        await this.ctx.container.interceptOutboundHttp(
          "route.test",
          this.ctx.exports.Echo({ props: { phase } }),
        );
        replies.push(await reply());
      }
      await writer.close();
      check((await reader.read()).done, "unexpected probe output");
      check((await process.exitCode) === 0, "routing probe exit");
      check(
        replies.every(
          (reply, index) =>
            reply.status === 200 &&
            reply.phase === ["before", "direct", "one", "two"][index] &&
            reply.bytes === length &&
            reply.sha256 === sha256(bytes()),
        ),
        "streamed HTTP route replacement",
      );
      return Response.json({ replies });
    } finally {
      writer.releaseLock();
      reader.releaseLock();
    }
  }
}

export default workersApplication(
  {
    Workspace,
    Echo,
    DirectoryBackupGateway,
    AbortAfterUploadGateway,
    TransferAbortGateway,
    LostAcknowledgementGateway,
    LostCreateGateway,
    CrashPartGateway,
    default: {
      fetch(request, env) {
        return env.WORKSPACES.getByName("qualified").fetch(request);
      },
    },
  },
  {
    namespaces: { WORKSPACES: "Workspace" },
    containers: ["Workspace"],
    r2: ["BACKUPS"],
  },
);
