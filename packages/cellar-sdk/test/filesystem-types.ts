// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import type { Context, FileStat, DirectoryPage, FileVersion } from "../index.js";
import { createFilesystemTools, filesystemToolRefusal, validateFilesystemCheckpoint } from "../filesystem-tools.js";

// Compile the public SDK surface against its supported native/guest profile.
export async function filesystemContract(ctx: Context): Promise<void> {
  const fs = ctx.fs;
  const capabilities = await fs.capabilities();
  validateFilesystemCheckpoint({ puts: { run: { status: "pending" } } }, capabilities.transport, { journalKeys: ["run"] });
  const quota: bigint = capabilities.limits.max_file_bytes;
  const hostHandles: number = capabilities.limits.max_host_handles;
  const readOnly: boolean = capabilities.read_only;
  const metadata: FileStat = await fs.stat("/report");
  const file = await fs.open(new Uint8Array([47, 255]), { access: "read_write", create: true, mode: 0o600 });
  const copy = await file.dup();
  await file.pwrite(new Uint8Array([1, 2]), 1n);
  const position: bigint = await copy.lseek(-1n, "end");
  await copy.pread(2, position);
  await copy.fsync();
  await copy.close();
  await file.close();
  const directory = await fs.opendir("/");
  const cookie = await directory.telldir();
  await directory.readPage(2);
  await directory.seekdir(cookie);
  await fs.fstatat(directory, "report", true);
  await fs.mkdirat(directory, "directory", 0o700);
  await fs.renameat(directory, "report", directory, "renamed");
  await directory.closedir();
  const page: DirectoryPage = await fs.readDir("/", 2);
  await fs.readDir("/", 2, page.next);
  const edited: FileVersion = await fs.editFile("/report", metadata, { offset: 0n, delete_bytes: 1n, insert: new Uint8Array([3]) });
  await fs.replaceFile("/report", await fs.readFile("/renamed", 4096));
  await ctx.storage.apply({
    filesystem_checks: [{ path: "/report", present: true, inode: String(edited.inode), version: String(edited.version) }],
    filesystem_mutations: [{ op: "write_file", path: "/report", data_base64: "eA==" }],
    puts: { receipt: { quota: String(quota), host_handles: hostHandles, read_only: readOnly } },
  });
  await createFilesystemTools(ctx).execute("turn:1:read", { operation: "read", path: "/report", max_bytes: 1024 });
  const { result, ...effects } = await createFilesystemTools(ctx).prepare({ operation: "read", path: "/report" });
  await ctx.storage.apply({ ...effects, puts: { journal_receipt: result } });
  await ctx.storage.sync();
  const refusal = filesystemToolRefusal(new Error("unknown"));
  if (refusal !== null) await ctx.storage.apply({ puts: { refusal } });
}
