// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

const proof = {
  seed: null,
  pending: null,
  post: null,
  owner: null,
  attempts: 0,
  committed: false,
  barrierEntered: false,
  returned: false,
  cancelled: null,
  lateEntered: false,
  lateDone: false,
  refused: 0,
  failure: null,
  old: null,
  release: null,
};
const exact = (value) =>
  JSON.stringify(value, (_, item) =>
    typeof item === "bigint" ? String(item) : item,
  );
const reply = (value) =>
  response(exact(value), { headers: [["content-type", "application/json"]] });
function check(value, message) {
  if (!value) throw new Error(message);
}
function payload(generation) {
  const data = new Uint8Array(3 * 65536 + 17).fill(generation);
  data[65535] = generation + 8;
  data[131072] = generation + 16;
  return data;
}
const receipt = (generation) => ({
  generation,
  sha256: proofHashes[generation - 1],
  size: payload(generation).length,
});
function snapshot() {
  const { old, release, ...state } = proof;
  return state;
}
function failed(error) {
  proof.failure = {
    message: String(error?.message),
    code: error?.code ?? null,
    operation: error?.operation ?? null,
  };
}
async function commit(ctx, generation) {
  const puts = {
    fs_generation: generation,
    filesystem_receipt: receipt(generation),
  };
  puts[generation === 1 ? "seed" : generation === 2 ? "pending" : "post"] =
    generation === 1 ? 7 : generation;
  await ctx.storage.apply({
    ...(generation === 2
      ? { checks: { pending: { present: false, value: null } } }
      : {}),
    filesystem_mutations: [
      {
        op: "write_file",
        path: "/ledger.bin",
        data_base64: bytesToBase64(payload(generation)),
      },
    ],
    puts,
  });
  const stat = await ctx.fs.stat("/ledger.bin");
  proof[generation === 1 ? "seed" : generation === 2 ? "pending" : "post"] = {
    inode: String(stat.inode),
    version: String(stat.version),
  };
}
async function retain(ctx) {
  const file = await ctx.fs.open("/ledger.bin", { access: "read_write" });
  proof.old = {
    ctx,
    fs: ctx.fs,
    file,
    duplicate: await file.dup(),
    directory: await ctx.fs.opendir("/"),
    version: await file.fstat(),
  };
  proof.old.cookie = await proof.old.directory.telldir();
}
async function refusals(code, observe) {
  const owner = proof.old;
  const before = observe ? await observe() : null;
  for (const [call, operation] of [
    [() => owner.file.read(1), "read"],
    [() => owner.file.write(utf8("late")), "write"],
    [() => owner.file.pread(1, 0n), "pread"],
    [() => owner.file.pwrite(utf8("late"), 0n), "pwrite"],
    [() => owner.file.lseek(1n), "lseek"],
    [() => owner.file.fstat(), "fstat"],
    [() => owner.file.ftruncate(0n), "ftruncate"],
    [() => owner.file.fchmod(0o400), "fchmod"],
    [() => owner.file.dup(), "dup"],
    [() => owner.file.close(), "close"],
    [() => owner.file.fsync(), "fsync"],
    [() => owner.file.fdatasync(), "fdatasync"],
    [() => owner.file.readExact(0), "read_exact"],
    [() => owner.file.writeAll(new Uint8Array()), "write_all"],
    [() => owner.duplicate.read(1), "read"],
    [() => owner.duplicate.close(), "close"],
    [() => owner.directory.readdir(), "readdir"],
    [() => owner.directory.readPage(1), "read_page"],
    [() => owner.directory.telldir(), "telldir"],
    [() => owner.directory.seekdir(owner.cookie), "seekdir"],
    [() => owner.directory.rewinddir(), "rewinddir"],
    [() => owner.directory.closedir(), "closedir"],
    [() => owner.directory.fstat(), "fstat"],
    [() => owner.directory.fsync(), "fsync"],
    [() => owner.fs.capabilities(), "capabilities"],
    [() => owner.fs.stat("/ledger.bin"), "stat"],
    [() => owner.fs.readFile("/ledger.bin"), "read_file"],
    [() => owner.fs.writeFile("/ledger.bin", utf8("late")), "write_file"],
    [() => owner.fs.unlink("/ledger.bin"), "unlink"],
    [() => owner.fs.sync(), "sync"],
    [() => owner.fs.collectGarbage(), "collect_garbage"],
    [
      () => owner.fs.open("/ledger.bin", { access: "write", truncate: true }),
      "open",
    ],
    [() => owner.fs.creat("/late-create"), "creat"],
    [() => owner.fs.lstat("/ledger.bin"), "lstat"],
    [() => owner.fs.truncate("/ledger.bin", 0n), "truncate"],
    [() => owner.fs.mkdir("/late-directory"), "mkdir"],
    [() => owner.fs.rmdir("/"), "rmdir"],
    [() => owner.fs.rename("/ledger.bin", "/late-renamed"), "rename"],
    [() => owner.fs.chmod("/ledger.bin", 0o400), "chmod"],
    [() => owner.fs.utimens("/ledger.bin", 0n), "utimens"],
    [() => owner.fs.opendir("/"), "opendir"],
    [() => owner.fs.statvfs(), "statvfs"],
    [
      () =>
        owner.fs.openat(owner.directory, "ledger.bin", {
          access: "write",
          truncate: true,
        }),
      "openat",
    ],
    [() => owner.fs.fstatat(owner.directory, "ledger.bin"), "fstatat"],
    [() => owner.fs.mkdirat(owner.directory, "late-directory"), "mkdirat"],
    [() => owner.fs.unlinkat(owner.directory, "ledger.bin"), "unlinkat"],
    [
      () =>
        owner.fs.renameat(
          owner.directory,
          "ledger.bin",
          owner.directory,
          "late-renamed",
        ),
      "renameat",
    ],
    [() => owner.fs.replaceFile("/ledger.bin", utf8("late")), "replace_file"],
    [
      () =>
        owner.fs.editFile("/ledger.bin", owner.version, {
          offset: 0n,
          delete_bytes: 1n,
          insert: utf8("late"),
        }),
      "edit_file",
    ],
    [() => owner.fs.readDir("/", 1), "read_dir"],
  ]) {
    let refused = false;
    try {
      await call();
    } catch (error) {
      check(
        error.code === code &&
          error.outcome === "not_committed" &&
          error.operation === `filesystem.${operation}` &&
          error.completed_bytes === 0,
        `retired ${operation}: ${error.code}/${error.outcome}/${error.operation}/${error.completed_bytes}`,
      );
      refused = true;
    }
    check(refused, `retired ${operation} succeeded`);
    if (observe)
      check(
        (await observe()) === before,
        `retired ${operation} changed fresh state`,
      );
    proof.refused++;
  }
  let refused = false;
  try {
    await owner.ctx._call("filesystem.write_file", {
      path: "/late-raw",
      data_base64: "bGF0ZQ==",
    });
  } catch (error) {
    check(
      error.code === code && error.outcome === "not_committed",
      "raw stale context admitted work",
    );
    refused = true;
  }
  check(refused, "raw stale filesystem mutation succeeded");
  if (observe)
    check(
      (await observe()) === before,
      "raw stale mutation changed fresh state",
    );
  proof.refused++;
}
async function late(gate) {
  proof.lateEntered = true;
  await gate;
  await refusals("ContextExpired");
  proof.lateDone = true;
}
async function state(ctx) {
  const values = {};
  for (const key of [
    "seed",
    "pending",
    "late",
    "late-raw",
    "fs_generation",
    "filesystem_receipt",
  ]) {
    const value = await ctx.storage.read(key);
    values[key] = value.present ? value.value : null;
  }
  const generation = values.fs_generation;
  check([1, 2, 3].includes(generation), "invalid filesystem generation");
  check(
    values.seed === 7 && values.late === null && values["late-raw"] === null,
    "stale business write reached state",
  );
  if (generation !== 3)
    check(
      values.pending === (generation === 2 ? 2 : null),
      "file/business state split",
    );
  check(
    exact(values.filesystem_receipt) === exact(receipt(generation)),
    "file receipt split",
  );
  const data = await ctx.fs.readFile("/ledger.bin"),
    expected = payload(generation);
  check(
    data.length === expected.length &&
      data.every((value, index) => value === expected[index]),
    "file bytes split",
  );
  const stat = await ctx.fs.stat("/ledger.bin");
  const metadata = { inode: String(stat.inode), version: String(stat.version) };
  check(
    exact(metadata) ===
      exact(
        proof[
          generation === 1 ? "seed" : generation === 2 ? "pending" : "post"
        ],
      ),
    "file identity/version split",
  );
  try {
    await ctx.fs.stat("/late-raw");
    throw new Error("stale raw file exists");
  } catch (error) {
    check(
      error.code === "Filesystem" && error.errno === "ENOENT",
      "stale raw file check",
    );
  }
  // Retired wrappers must not occupy slots even while their original node
  // stays alive during warm readback, before the later domain-close checks.
  const fresh = await ctx.fs.open("/ledger.bin");
  const duplicate = await fresh.dup();
  const directory = await ctx.fs.opendir("/");
  const fourth = await ctx.fs.open("/ledger.bin");
  let refused = false;
  try {
    await ctx.fs.open("/ledger.bin");
  } catch (error) {
    check(
      error.code === "Filesystem" &&
        error.errno === "ENFILE" &&
        error.outcome === "not_committed",
      "warm host ceiling changed",
    );
    refused = true;
  }
  check(refused, "warm host ceiling escaped");
  await fourth.close();
  await directory.closedir();
  await duplicate.close();
  await fresh.close();
  await ctx.fs.sync();
  return {
    ...values,
    filesystem: { ...receipt(generation), ...metadata },
    data_base64: bytesToBase64(data),
  };
}
async function checkRetired(ctx) {
  for (let i = 0; i < 64; i++) {
    if ((await ctx.fs.collectGarbage()) === 0) break;
    check(i !== 63, "GC did not settle");
  }
  const fs = ctx.fs,
    file = await fs.open("/ledger.bin", { access: "read_write" });
  const duplicate = await file.dup(),
    directory = await fs.opendir("/"),
    fourth = await fs.open("/ledger.bin");
  await directory.readdir();
  await file.lseek(2n);
  const observe = async () =>
    exact([
      await fs.stat("/"),
      await file.fstat(),
      await fs.statvfs(),
      await file.lseek(0n, "current"),
      await duplicate.lseek(0n, "current"),
      await directory.telldir(),
      await file.pread(32, 0n),
    ]);
  check(proof.refused === 51, "old continuation did not finish its refusals");
  await refusals("ContextMismatch", observe);
  try {
    await fs.open("/ledger.bin");
    throw new Error("host ceiling escaped");
  } catch (error) {
    check(
      error.code === "Filesystem" &&
        error.errno === "ENFILE" &&
        error.outcome === "not_committed",
      "fresh host ceiling changed",
    );
  }
  await fourth.close();
  await directory.closedir();
  await duplicate.close();
  await file.close();
  return {
    refused: proof.refused,
    owner: await ctx.variable("DOMAIN_MARKER"),
    state: await state(ctx),
  };
}
const app = {
  async fetch(req, ctx) {
    if (req.url.endsWith("/control/snapshot")) return reply(snapshot());
    if (req.url.endsWith("/control/release")) {
      proof.release();
      return reply(true);
    }
    const result = await ctx.cell("COUNTERS", "proof-authority").fetch(
      request(req.url, {
        method: req.method,
        headers: req.headers,
        body: req.body,
      }),
    );
    return response(result.body, {
      status: result.status,
      headers: result.headers,
    });
  },
  cells: {
    Counter: class {
      async fetch(req, ctx) {
        try {
          proof.owner = await ctx.variable("DOMAIN_MARKER");
          if (req.url.endsWith("/seed")) {
            await commit(ctx, 1);
            await ctx.fs.sync();
            return reply(true);
          }
          if (req.url.endsWith("/state")) return reply(await state(ctx));
          if (req.url.endsWith("/retired"))
            return reply(await checkRetired(ctx));
          if (req.url.endsWith("/post")) {
            await commit(ctx, 3);
            return reply(await state(ctx));
          }
          check(req.url.endsWith("/cancel-start"), "unexpected proof route");
          proof.attempts++;
          const gate = new Promise((resolve) => {
            proof.release = resolve;
          });
          await commit(ctx, 2);
          await retain(ctx);
          proof.committed = true;
          if (proofBarrier === "output") {
            late(gate).catch(failed);
            proof.returned = true;
            return reply(true);
          }
          proof.barrierEntered = true;
          try {
            if (proofBarrier === "sync") await ctx.fs.sync();
            else if (proofBarrier === "dirsync")
              await proof.old.directory.fsync();
            else await proof.old.file[proofBarrier]();
            throw new Error("withheld proof returned success");
          } catch (error) {
            check(
              error.outcome === "unknown" &&
                ((error.code === "ContextExpired" &&
                  error.errno === undefined) ||
                  (error.code === "Storage" && error.errno === "EIO")),
              `cancelled barrier ${error.code}/${error.errno}/${error.outcome}`,
            );
            proof.cancelled = {
              code: error.code,
              outcome: error.outcome,
              errno: error.errno ?? null,
              operation: error.operation,
              completed_bytes: error.completed_bytes,
            };
            await late(gate);
            return reply(false);
          }
        } catch (error) {
          failed(error);
          throw error;
        }
      }
    },
  },
};
