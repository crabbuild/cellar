// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

function budgetCheck(condition) {
  if (!condition)
    throw new Error("filesystem transport budget assertion failed");
}
async function budgetRefused(action, errno) {
  try {
    await action();
  } catch (error) {
    budgetCheck(error.errno === errno && error.outcome === "not_committed");
    return;
  }
  throw new Error("oversized filesystem request succeeded");
}
const budgetApp = {
  cells: {
    Counter: class {
      async fetch(_req, ctx) {
        const fs = ctx.fs,
          capabilities = await fs.capabilities();
        const limits = capabilities.limits;
        budgetCheck(capabilities.transport.max_value_bytes === 8192);
        budgetCheck(
          limits.max_transfer_bytes > 0 && limits.max_transfer_bytes < 8192,
        );
        await fs.writeFile("/data", new Uint8Array(4096).fill(42));
        const file = await fs.open("/data", { access: "read_write" });
        const beforeEof = await file.fstat();
        let eof;
        try {
          await file.readExact(4097);
        } catch (error) {
          eof = error;
        }
        budgetCheck(
          eof?.code === "UnexpectedEof" &&
            eof.errno === "EIO" &&
            eof.operation === "filesystem.read_exact" &&
            eof.outcome === "unknown" &&
            eof.completed_bytes === 4096,
        );
        budgetCheck(
          (await file.lseek(0n, "current")) === 4096n &&
            (await file.fstat()).version === beforeEof.version,
        );
        await file.lseek(0n);
        await file.ftruncate(16384n);
        const read = await file.read(16384);
        budgetCheck(
          read.length === limits.max_transfer_bytes && read[0] === 42,
        );
        budgetCheck((await file.lseek(0n, "current")) === BigInt(read.length));
        budgetCheck((await file.pread(16384, 0n)).length === read.length);
        budgetCheck((await file.lseek(0n, "current")) === BigInt(read.length));
        await budgetRefused(() => fs.readFile("/data"), "EFBIG");
        budgetCheck((await file.fstat()).size === 16384n);
        await file.lseek(
          limits.max_file_bytes - BigInt(limits.max_transfer_bytes),
        );
        let partial;
        try {
          await file.writeAll(
            new Uint8Array(2 * limits.max_transfer_bytes).fill(90),
          );
        } catch (error) {
          partial = error;
        }
        budgetCheck(
          partial?.errno === "EFBIG" &&
            partial.outcome === "locally_committed" &&
            partial.completed_bytes === limits.max_transfer_bytes,
        );
        budgetCheck(
          (await file.lseek(0n, "current")) === limits.max_file_bytes,
        );
        const prefix = await file.pread(
          limits.max_transfer_bytes,
          limits.max_file_bytes - BigInt(limits.max_transfer_bytes),
        );
        budgetCheck(
          prefix.length === limits.max_transfer_bytes &&
            prefix.every((byte) => byte === 90),
        );
        await file.close();

        await fs.mkdir("/names");
        for (let index = 0; index < limits.max_directory_entries + 2; index++) {
          const name = new Uint8Array(262).fill(255);
          name.set(utf8("/names/"));
          name[name.length - 1] = 65 + index;
          await fs.writeFile(name, new Uint8Array());
        }
        const page = await fs.readDir("/names", limits.max_directory_entries);
        budgetCheck(
          page.entries.length === limits.max_directory_entries &&
            page.next !== null,
        );
        budgetCheck(page.entries.every((entry) => entry.name.length === 255));
        const next = await fs.readDir(
          "/names",
          limits.max_directory_entries,
          page.next,
        );
        budgetCheck(next.entries.length === 2 && next.next === null);
        const directory = await fs.opendir("/names");
        const cookie = await directory.telldir();
        await budgetRefused(
          () => directory.readPage(limits.max_directory_entries + 1),
          "EINVAL",
        );
        budgetCheck((await directory.telldir()) === cookie);
        budgetCheck(
          (await directory.readPage(limits.max_directory_entries)).length ===
            limits.max_directory_entries,
        );
        await directory.closedir();
        const parent = `/${"a".repeat(255)}`;
        const nested = `${parent}/${"b".repeat(255)}`;
        await fs.mkdir(parent);
        await fs.mkdir(nested);
        const source = `${nested}/${"c".repeat(255)}`;
        await fs.writeFile(source, new Uint8Array(1024));
        const tools = createFilesystemTools(ctx);
        const rejected = await tools.execute("large-read-receipt", {
          operation: "read",
          path: source,
          max_bytes: 1024,
        });
        budgetCheck(rejected.result.error.code === "ValidationError");
        const replayed = await tools.execute("large-read-receipt", {
          operation: "read",
          path: source,
          max_bytes: 1024,
        });
        budgetCheck(
          replayed.replayed && replayed.result.error.code === "ValidationError",
        );
        await fs.writeFile("/receipt-destination", utf8("unchanged"));
        try {
          await tools.execute("escaped-write-receipt", {
            operation: "write",
            path: "/receipt-destination",
            content: "\0".repeat(1200),
          });
          throw new Error("unreadable receipt admitted a file mutation");
        } catch (error) {
          budgetCheck(
            error.code === "ValidationError" &&
              error.outcome === "not_committed",
          );
        }
        budgetCheck(
          Array.from(await fs.readFile("/receipt-destination")).join() ===
            Array.from(utf8("unchanged")).join(),
        );
        // A journal can fit one request and still leave no room for its next
        // compare-and-save. Admission includes that future checked update.
        const pending = { status: "pending", data: "x".repeat(3200) };
        const prior = await ctx.storage.read("bounded-journal");
        const initial = {
          checks: { "bounded-journal": prior },
          puts: { "bounded-journal": pending },
        };
        validateFilesystemCheckpoint(
          initial,
          { ...capabilities.transport, max_body_bytes: 1 },
          { journalKeys: ["bounded-journal"] },
        );
        await ctx.storage.apply(initial);
        const current = await ctx.storage.read("bounded-journal");
        const oversized = {
          checks: { "bounded-journal": current },
          puts: { "bounded-journal": { ...pending, data: "x".repeat(4000) } },
          filesystem_mutations: [
            {
              op: "write_file",
              path: "/receipt-destination",
              data_base64: bytesToBase64(utf8("changed")),
            },
          ],
        };
        let submitted = false;
        try {
          validateFilesystemCheckpoint(oversized, capabilities.transport, {
            journalKeys: ["bounded-journal"],
          });
          submitted = true;
          await ctx.storage.apply(oversized);
          throw new Error(
            "journal without recovery space admitted file effects",
          );
        } catch (error) {
          budgetCheck(
            !submitted &&
              error.code === "ValidationError" &&
              error.outcome === "not_committed",
          );
        }
        const terminal = {
          checks: { "bounded-journal": current },
          puts: {
            "bounded-journal": {
              ...pending,
              status: "failed",
              error: "checkpoint_limit",
            },
          },
        };
        validateFilesystemCheckpoint(terminal, capabilities.transport);
        await ctx.storage.apply(terminal);
        budgetCheck(
          (await ctx.storage.read("bounded-journal")).value.data.length ===
            3200,
        );
        budgetCheck(
          Array.from(await fs.readFile("/receipt-destination")).join() ===
            Array.from(utf8("unchanged")).join(),
        );
        await fs.sync();
        return response("ok");
      }
    },
  },
  async fetch(req, ctx) {
    const result = await ctx
      .cell("COUNTERS", "bounded")
      .fetch(request(req.url));
    return response(result.body, {
      status: result.status,
      headers: result.headers,
    });
  },
};
