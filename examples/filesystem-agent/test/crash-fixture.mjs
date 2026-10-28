// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// Qualification-only entry. Production app.js contains no process fault hooks.
import { appendFile, writeFile } from "node:fs/promises";
import { jsonResponse, request, response } from "@cellar/sdk";
import { createFilesystemAgentSession } from "../session.js";
import { scriptedFilesystemModel } from "../model.js";

const pauseAt = process.env.CELLAR_FS_TEST_PAUSE;
const uncertain = pauseAt?.startsWith("uncertain-");
const beforeCommit = pauseAt?.startsWith("uncertain-before-");
const model = {
  configuration: scriptedFilesystemModel.configuration,
  async decide(request, ctx) {
    if (pauseAt === "accepted") await new Promise(() => {});
    await appendFile(process.env.CELLAR_FS_TEST_CALLS, `${request.step}\n`);
    await appendFile(process.env.CELLAR_FS_TEST_REQUESTS, `${request.requestId}\n`);
    if (pauseAt === "provider") {
      await writeFile(process.env.CELLAR_FS_TEST_MARKER, JSON.stringify({ point: "provider" }));
      await new Promise(() => {});
    }
    return scriptedFilesystemModel.decide(request, ctx);
  },
};
const Base = createFilesystemAgentSession(model);

function instrument(ctx) {
  let point;
  return {
    ...ctx,
    storage: {
      ...ctx.storage,
      async apply(batch) {
        const run = batch.puts?.["filesystem-agent:turn:one"];
        const fileEffect = batch.filesystem_mutations?.length && run?.phase === "tool-receipt";
        if (beforeCommit && fileEffect) {
          // The caller sees the same unknown outcome, but this request never
          // reached the native transaction. Recovery must inspect its journal
          // rather than assume that an unknown reply implies either result.
          await writeFile(process.env.CELLAR_FS_TEST_COMMIT_MARKER, JSON.stringify({ point: "not-dispatched" }));
          throw Object.assign(new Error("Injected unknown before native dispatch"), { code: "InjectedUnknown", outcome: "unknown" });
        }
        // Keep the committed tool/receipt phase unresolved until retry
        // exhaustion. These later refusals never reach the native mutation.
        if (uncertain && batch.puts?.["filesystem-agent:turn:one:result:0"])
          throw Object.assign(new Error("Injected observation checkpoint failure"), { code: "InjectedUnavailable", outcome: "not_committed" });
        const result = await ctx.storage.apply(batch);
        if (fileEffect) await appendFile(process.env.CELLAR_FS_TEST_WRITES, `${ctx.identity.scope}/one\n`);
        if (uncertain && fileEffect) {
          // The real native transaction and its durability proof succeeded.
          // Suppress that reply at the guest caller, leaving the agent to
          // reconcile its persistent phase and stable operation receipt.
          await ctx.fs.sync();
          const version = String((await ctx.fs.stat("/notes.txt")).version);
          await writeFile(process.env.CELLAR_FS_TEST_COMMIT_MARKER, JSON.stringify({ point: "committed-reply-lost", version }));
          throw Object.assign(new Error("Injected lost tool reply"), { code: "InjectedUnknown", outcome: "unknown" });
        }
        if (uncertain && run?.status === "interrupted") {
          await ctx.storage.sync();
          const version = beforeCommit ? undefined : String((await ctx.fs.stat("/notes.txt")).version);
          await writeFile(process.env.CELLAR_FS_TEST_MARKER, JSON.stringify({ point: pauseAt, version, attempts: run.attempts }));
        }
        if (run?.status === "completed") point = "completed";
        else if (run?.status === "pending" && run?.step === 0) point = run.phase === "model" ? "accepted"
          : run.phase === "decided" ? "decision"
            : run.phase === "tool-receipt" ? "tool" : undefined;
        return result;
      },
    },
    fs: {
      ...ctx.fs,
      async sync() {
        await ctx.fs.sync();
        const saved = point;
        point = undefined;
        if (saved === "completed") await appendFile(process.env.CELLAR_FS_TEST_COMPLETED, "completed\n");
        if (saved && saved === pauseAt) {
          let version;
          if (saved === "tool" || saved === "completed") version = String((await ctx.fs.stat("/notes.txt")).version);
          await writeFile(process.env.CELLAR_FS_TEST_MARKER, JSON.stringify({ point: saved, version }));
          await new Promise(() => {});
        }
      },
    },
  };
}

class AgentSession extends Base {
  async fetch(req, ctx) {
    if (new URL(req.url).pathname.endsWith("/snapshot")) {
      try {
        const stat = await ctx.fs.stat("/notes.txt");
        const data = await ctx.fs.readFile("/notes.txt");
        const count = await ctx.storage.read("filesystem-agent:tools:one:count");
        await ctx.fs.sync();
        return jsonResponse({ present: true, text: new TextDecoder().decode(data), version: String(stat.version), receipts: count.value });
      } catch (error) {
        if (error.errno !== "ENOENT") throw error;
        const count = await ctx.storage.read("filesystem-agent:tools:one:count");
        const usage = await ctx.fs.statvfs();
        await ctx.fs.sync();
        return jsonResponse({ present: false, receipts: count.present ? count.value : 0,
          logical: String(usage.logical_bytes), allocated: String(usage.allocated_bytes), inodes: String(usage.inodes) });
      }
    }
    return super.fetch(req, instrument(ctx));
  }

  async alarm(scheduled, ctx) {
    return super.alarm(scheduled, instrument(ctx));
  }
}

export default {
  cells: { AgentSession },
  async fetch(req, ctx) {
    const match = /^\/sessions\/([A-Za-z0-9_-]{1,64})\/(?:turns\/[A-Za-z0-9_-]{1,64}|snapshot)$/.exec(new URL(req.url).pathname);
    if (!match) return jsonResponse({ error: "invalid route" }, { status: 404 });
    const result = await ctx.cell("AGENTS", match[1]).fetch(request(req.url, { method: req.method, headers: req.headers, body: req.body }));
    return response(result.body, { status: result.status, headers: result.headers });
  },
};
