// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { createHash } from "node:crypto";

const io = {
  attempts: 0,
  failed: null,
  failure: null,
  refused: 0,
  old: null,
  release: null,
  owner: null,
};
const exact = (value) =>
  JSON.stringify(value, (_, v) => (typeof v === "bigint" ? String(v) : v));
const check = (ok, message) => {
  if (!ok) throw new Error(message);
};
const hash = (bytes) => createHash("sha256").update(bytes).digest("hex");
const reply = (value) =>
  response(exact(value), { headers: [["content-type", "application/json"]] });
const seed = Uint8Array.from({ length: 32 }, (_, i) => i);
const edit = Uint8Array.from({ length: 12 }, (_, i) => 128 + i);
const progress =
  ioOperation.endsWith("_all") || ioOperation.endsWith("_exact") ? 4 : 0;

async function state(ctx) {
  const bytes = await ctx.fs.readFile("/ledger.bin");
  const stat = await ctx.fs.stat("/ledger.bin");
  const values = {};
  for (const key of ["seed", "receipt", "reconciled"]) {
    const saved = await ctx.storage.read(key);
    values[key] = saved.present ? saved.value : null;
  }
  return {
    data_base64: bytesToBase64(bytes),
    sha256: hash(bytes),
    stat,
    ...values,
  };
}
async function stale(code, observe) {
  const old = io.old;
  const before = observe ? await observe() : null;
  for (const [call, operation] of [
    [() => old.file.read(4), "read"],
    [() => old.file.pread(4, 0n), "pread"],
    [() => old.file.write(edit), "write"],
    [() => old.file.pwrite(edit, 0n), "pwrite"],
    [() => old.file.readExact(4), "read_exact"],
    [() => old.file.writeAll(edit), "write_all"],
    [() => old.file.close(), "close"],
    [() => old.duplicate.close(), "close"],
    [() => old.directory.closedir(), "closedir"],
  ]) {
    let caught = false;
    try {
      await call();
    } catch (error) {
      check(
        error.code === code &&
          error.operation === `filesystem.${operation}` &&
          error.outcome === "not_committed" &&
          error.completed_bytes === 0,
        `stale ${operation}: ${error.code}/${error.outcome}/${error.completed_bytes}`,
      );
      caught = true;
    }
    check(caught, `stale ${operation} succeeded`);
    if (observe)
      check(
        (await observe()) === before,
        `stale ${operation} changed fresh state`,
      );
    io.refused++;
  }
}
async function capacity(ctx, retired = false) {
  const file = await ctx.fs.open("/ledger.bin", { access: "read_write" });
  const duplicate = await file.dup(),
    directory = await ctx.fs.opendir("/"),
    fourth = await ctx.fs.open("/ledger.bin");
  await file.lseek(2n);
  await directory.readdir();
  const observe = async () =>
    exact([
      await ctx.fs.stat("/"),
      await file.fstat(),
      await ctx.fs.statvfs(),
      await file.lseek(0n, "current"),
      await duplicate.lseek(0n, "current"),
      await directory.telldir(),
      await file.pread(4, 0n),
    ]);
  if (retired) await stale("ContextMismatch", observe);
  let refused = false;
  try {
    await ctx.fs.open("/ledger.bin");
  } catch (error) {
    check(
      error.errno === "ENFILE" && error.outcome === "not_committed",
      "capacity refusal changed",
    );
    refused = true;
  }
  check(refused, "stale closes released fresh slots");
  await fourth.close();
  await directory.closedir();
  await duplicate.close();
  await file.close();
}
const app = {
  async fetch(req, ctx) {
    if (req.url.endsWith("/control/snapshot")) {
      const { old, release, ...value } = io;
      return reply(value);
    }
    if (req.url.endsWith("/control/release")) {
      io.release();
      return reply(true);
    }
    const result = await ctx
      .cell("COUNTERS", "byte-reply")
      .fetch(request(req.url));
    return response(result.body, {
      status: result.status,
      headers: result.headers,
    });
  },
  cells: {
    Counter: class {
      async fetch(req, ctx) {
        try {
          if (req.url.endsWith("/seed")) {
            await ctx.storage.apply({
              filesystem_mutations: [
                {
                  op: "write_file",
                  path: "/ledger.bin",
                  data_base64: bytesToBase64(seed),
                },
              ],
              puts: { seed: 7, receipt: { sha256: hash(seed), phase: "seed" } },
            });
            await ctx.fs.sync();
            return reply(await state(ctx));
          }
          if (req.url.endsWith("/prove")) {
            await ctx.fs.sync();
            return reply(await state(ctx));
          }
          if (req.url.endsWith("/reconcile")) {
            await capacity(ctx);
            const before = await state(ctx);
            await ctx.storage.apply({
              filesystem_checks: [
                {
                  path: "/ledger.bin",
                  present: true,
                  inode: String(before.stat.inode),
                  version: String(before.stat.version),
                },
              ],
              puts: {
                receipt: { sha256: before.sha256, phase: "reconciled" },
                reconciled: { operation: ioOperation, failure: io.failed },
              },
            });
            await ctx.fs.sync();
            return reply(await state(ctx));
          }
          if (req.url.endsWith("/state")) {
            await ctx.fs.sync();
            return reply(await state(ctx));
          }
          if (req.url.endsWith("/retired")) {
            await capacity(ctx, true);
            return reply({
              state: await state(ctx),
              refused: io.refused,
              owner: await ctx.variable("DOMAIN_MARKER"),
            });
          }
          if (req.url.endsWith("/post")) {
            const before = await ctx.fs.stat("/ledger.bin");
            await ctx.storage.apply({
              filesystem_checks: [
                {
                  path: "/ledger.bin",
                  present: true,
                  inode: String(before.inode),
                  version: String(before.version),
                },
              ],
              filesystem_mutations: [
                {
                  op: "write_file",
                  path: "/ledger.bin",
                  data_base64: bytesToBase64(seed),
                },
              ],
              puts: { receipt: { sha256: hash(seed), phase: "post" } },
            });
            await ctx.fs.sync();
            return reply(await state(ctx));
          }
          check(req.url.endsWith("/cancel-start"), "unexpected IO route");
          io.attempts++;
          io.owner = await ctx.variable("DOMAIN_MARKER");
          check(
            (await ctx.fs.capabilities()).limits.max_transfer_bytes === 4,
            "transfer profile changed",
          );
          const file = await ctx.fs.open("/ledger.bin", {
            access: "read_write",
          });
          io.old = {
            file,
            duplicate: await file.dup(),
            directory: await ctx.fs.opendir("/"),
          };
          await file.lseek(3n);
          const gate = new Promise((resolve) => {
            io.release = resolve;
          });
          try {
            if (ioOperation === "read") await file.read(12);
            else if (ioOperation === "pread") await file.pread(12, 3n);
            else if (ioOperation === "write") await file.write(edit);
            else if (ioOperation === "pwrite") await file.pwrite(edit, 3n);
            else if (ioOperation === "read_exact") await file.readExact(12);
            else if (ioOperation === "write_all") await file.writeAll(edit);
            else throw new Error("unknown IO operation");
            throw new Error("lost byte reply returned success");
          } catch (error) {
            check(
              error.code === "ContextExpired" &&
                error.outcome === "unknown" &&
                error.operation === `filesystem.${ioOperation}` &&
                error.completed_bytes === progress,
              `byte reply failure ${error.code}/${error.outcome}/${error.operation}/${error.completed_bytes}`,
            );
            io.failed = {
              code: error.code,
              outcome: error.outcome,
              operation: error.operation,
              completed_bytes: error.completed_bytes,
            };
            await gate;
            await stale("ContextExpired");
            return reply(false);
          }
        } catch (error) {
          io.failure = String(error.stack);
          throw error;
        }
      }
    },
  },
};
export default app;
