// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

async function filesystemReplyProbe(ctx, cases) {
  const fs = ctx.fs;
  const file = await fs.open("/note", { access: "read_write" });
  const directory = await fs.opendir("/");
  const bytes = new Uint8Array([1, 2, 3, 4]);
  const calls = {
    capabilities: () => fs.capabilities(),
    statvfs: () => fs.statvfs(),
    open: () => fs.open("/note"),
    creat: () => fs.creat("/note"),
    openat: () => fs.openat(directory, "note"),
    opendir: () => fs.opendir("/"),
    read: () => file.read(4),
    pread: () => file.pread(4, 0n),
    write: () => file.write(bytes),
    pwrite: () => file.pwrite(bytes, 0n),
    read_exact: () => file.readExact(4),
    write_all: () => file.writeAll(bytes),
    lseek: () => file.lseek(0n),
    fstat: () => file.fstat(),
    ftruncate: () => file.ftruncate(0n),
    fchmod: () => file.fchmod(0o600),
    dup: () => file.dup(),
    close: () => file.close(),
    fsync: () => file.fsync(),
    fdatasync: () => file.fdatasync(),
    stat: () => fs.stat("/note"),
    lstat: () => fs.lstat("/note"),
    fstatat: () => fs.fstatat(directory, "note"),
    truncate: () => fs.truncate("/note", 0n),
    mkdir: () => fs.mkdir("/new"),
    mkdirat: () => fs.mkdirat(directory, "new"),
    rmdir: () => fs.rmdir("/new"),
    unlink: () => fs.unlink("/note"),
    unlinkat: () => fs.unlinkat(directory, "note"),
    rename: () => fs.rename("/note", "/new"),
    renameat: () => fs.renameat(directory, "note", directory, "new"),
    chmod: () => fs.chmod("/note", 0o600),
    utimens: () => fs.utimens("/note"),
    readdir: () => directory.readdir(),
    read_page: () => directory.readPage(2),
    telldir: () => directory.telldir(),
    seekdir: () => directory.seekdir("AAAAAAAAAAAAAAAAAAAAAA=="),
    rewinddir: () => directory.rewinddir(),
    closedir: () => directory.closedir(),
    directory_fstat: () => directory.fstat(),
    directory_fsync: () => directory.fsync(),
    read_file: () => fs.readFile("/note", 4),
    write_file: () => fs.writeFile("/note", bytes),
    replace_file: () => fs.replaceFile("/note", bytes),
    edit_file: () =>
      fs.editFile(
        "/note",
        { inode: 1n, version: 1n },
        { offset: 0n, delete_bytes: 0n, insert: bytes },
      ),
    read_dir: () => fs.readDir("/", 2),
    sync: () => fs.sync(),
    collect_garbage: () => fs.collectGarbage(),
  };
  for (const item of cases) {
    let failure;
    try {
      await calls[item.call ?? item.operation]();
    } catch (error) {
      failure = error;
    }
    if (item.accept) {
      if (failure)
        throw new Error(`${item.id}: valid reply refused: ${failure.code}`);
      continue;
    }
    const expected = item.expected ?? {
      code: "HostError",
      errno: "EIO",
      outcome: "unknown",
      completed_bytes: item.completed_bytes ?? 0,
      cause: true,
    };
    const actual = {
      code: failure?.code,
      errno: failure?.errno,
      outcome: failure?.outcome,
      completed_bytes: failure?.completed_bytes,
      cause: failure?.cause !== undefined,
    };
    if (
      !Object.entries(expected).every(
        ([key, value]) => actual[key] === value,
      ) ||
      failure?.operation !== `filesystem.${item.operation}`
    )
      throw new Error(
        `${item.id}: ${JSON.stringify(actual)} / ${failure?.operation}`,
      );
  }
  return { checked: cases.length };
}
