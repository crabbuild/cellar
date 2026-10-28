// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

function check(value) {
  if (!value) throw new Error("filesystem conformance failed");
}
function equal(a, b) {
  check(a.length === b.length && a.every((byte, index) => byte === b[index]));
}
async function refuses(fn, code, errno) {
  try {
    await fn();
  } catch (error) {
    check(
      error.code === code && (errno === undefined || error.errno === errno),
    );
    return error;
  }
  throw new Error("filesystem operation unexpectedly succeeded");
}
async function readonlyProfile(fs, ctx) {
  check((await fs.capabilities()).read_only);
  const saved = await ctx.storage.read("readonly_expected");
  check(saved.present);
  const expected = saved.value;
  const unchanged = async () => {
    for (const [key, path] of [
      ["file", "/kept"],
      ["directory", "/directory"],
      ["root", "/"],
    ]) {
      const stat = await fs.stat(path);
      for (const [field, value] of Object.entries(expected[key]))
        check(String(stat[field]) === String(value));
    }
    const usage = await fs.statvfs();
    for (const [key, value] of Object.entries(expected.usage))
      check(String(usage[key]) === String(value));
  };
  await unchanged();
  const file = await fs.open("/kept", { create: true }),
    root = await fs.opendir("/"),
    dir = await fs.opendir("/directory");
  const copy = await file.dup();
  equal(await copy.readExact(2), utf8("or"));
  check((await file.lseek(0n, "current")) === 2n);
  await copy.close();
  equal(await file.pread(8, 0n), utf8("original"));
  await (await fs.openat(root, "kept", { create: true })).close();
  await refuses(
    () => fs.open("/kept", { create_new: true }),
    "Filesystem",
    "EEXIST",
  );
  await refuses(() => fs.open("/missing"), "Filesystem", "ENOENT");
  await refuses(
    () => fs.open("/kept", { truncate: true }),
    "Filesystem",
    "EINVAL",
  );
  const version = await file.fstat();
  for (const [call, operation] of [
    [() => fs.mkdir("/forbidden", 0o700), "mkdir"],
    [() => fs.mkdirat(dir, "forbidden", 0o700), "mkdirat"],
    [() => fs.rmdir("/directory"), "rmdir"],
    [() => fs.unlink("/kept"), "unlink"],
    [() => fs.unlinkat(root, "kept"), "unlinkat"],
    [() => fs.rename("/kept", "/renamed"), "rename"],
    [() => fs.renameat(root, "kept", dir, "renamed"), "renameat"],
    [() => fs.truncate("/kept", 0n), "truncate"],
    [() => fs.chmod("/kept", 0o400), "chmod"],
    [() => fs.utimens("/kept", 0n), "utimens"],
    [() => fs.writeFile("/kept", utf8("bad")), "write_file"],
    [() => fs.replaceFile("/kept", utf8("bad")), "replace_file"],
    [
      () =>
        fs.editFile("/kept", version, {
          offset: 0n,
          delete_bytes: 1n,
          insert: utf8("X"),
        }),
      "edit_file",
    ],
    [() => fs.open("/kept", { access: "read_write" }), "open"],
    [() => fs.open("/kept", { access: "write", truncate: true }), "open"],
    [() => fs.open("/missing", { create: true }), "open"],
    [() => fs.openat(root, "missing", { create_new: true }), "openat"],
    [() => fs.creat("/missing", 0o600), "creat"],
    [() => file.fchmod(0o400), "fchmod"],
    [() => fs.collectGarbage(), "collect_garbage"],
  ]) {
    const error = await refuses(call, "Filesystem", "EROFS");
    check(
      error.operation === `filesystem.${operation}` &&
        error.outcome === "not_committed" &&
        error.completed_bytes === 0,
    );
    await unchanged();
  }
  await refuses(() => file.write(utf8("bad")), "Filesystem", "EBADF");
  await refuses(() => file.pwrite(utf8("bad"), 0n), "Filesystem", "EBADF");
  await refuses(() => file.ftruncate(0n), "Filesystem", "EBADF");
  await fs.utimens("/kept", null);
  await file.fsync();
  await file.fdatasync();
  await dir.fsync();
  await file.close();
  await root.closedir();
  await dir.closedir();
  const failure = await refuses(
    () =>
      ctx.storage.apply({
        puts: { readonly_must_rollback: true },
        filesystem_mutations: [
          {
            op: "write_file",
            path: "/kept",
            data_base64: bytesToBase64(utf8("bad")),
          },
        ],
      }),
    "Filesystem",
    "EROFS",
  );
  check(failure.outcome === "not_committed");
  check(!(await ctx.storage.read("readonly_must_rollback")).present);
  await ctx.storage.apply({
    filesystem_checks: [
      {
        path: "/kept",
        present: true,
        inode: expected.file.inode,
        version: expected.file.version,
      },
    ],
    puts: { readonly_business_state: true },
  });
  check((await ctx.storage.read("readonly_business_state")).value === true);
  await unchanged();
  await fs.sync();
}
async function hostAdmissionProfile(fs) {
  check((await fs.capabilities()).limits.max_host_handles === 4);
  await fs.writeFile("/host-admission", utf8("original"));
  const file = await fs.open("/host-admission", { access: "read_write" });
  const duplicate = await file.dup(),
    directory = await fs.opendir("/"),
    fourth = await fs.open("/host-admission");
  const encode = (value) =>
    JSON.stringify(value, (_, value) =>
      typeof value === "bigint" ? String(value) : value,
    );
  const before = encode([await file.fstat(), await fs.statvfs()]);
  for (const [call, operation] of [
    [() => file.dup(), "dup"],
    [() => fs.opendir("/"), "opendir"],
    [
      () => fs.open("/host-refused", { access: "write", create_new: true }),
      "open",
    ],
    [
      () => fs.open("/host-admission", { access: "write", truncate: true }),
      "open",
    ],
  ]) {
    const error = await refuses(call, "Filesystem", "ENFILE");
    check(
      error.operation === `filesystem.${operation}` &&
        error.outcome === "not_committed" &&
        error.completed_bytes === 0,
    );
    check(encode([await file.fstat(), await fs.statvfs()]) === before);
  }
  equal(await fs.readFile("/host-admission"), utf8("original"));
  check((await file.lseek(0n, "current")) === 0n);
  await refuses(() => fs.stat("/host-refused"), "Filesystem", "ENOENT");
  await fourth.close();
  const freed = await fs.open("/host-refused", {
    access: "write",
    create_new: true,
  });
  await freed.close();
  await duplicate.close();
  await file.close();
  await directory.closedir();
  await fs.unlink("/host-refused");
  await fs.unlink("/host-admission");
  while ((await fs.collectGarbage()) > 0) {}
}
async function reservedNamespaceProfile(fs, group) {
  while ((await fs.collectGarbage()) > 0) {}
  await fs.mkdir("/reserved", 0o700);
  await fs.mkdir("/reserved/child", 0o700);
  await fs.writeFile("/reserved/file", utf8("kept"));
  const parent = await fs.opendir("/reserved"),
    child = await fs.opendir("/reserved/child");
  const snapshot = async () => {
    const stats = [];
    for (const path of ["/", "/reserved", "/reserved/child", "/reserved/file"])
      stats.push(await fs.stat(path));
    return JSON.stringify([stats, await fs.statvfs()], (_, value) =>
      typeof value === "bigint" ? String(value) : value,
    );
  };
  const before = await snapshot(),
    cases = filesystemReservedCases.cases.filter(
      (value) => value.group === group,
    );
  check(filesystemReservedCases.cases.length === 42 && cases.length > 0);
  for (const value of cases) {
    const base = value.base === "child" ? child : parent;
    let call;
    switch (value.operation) {
      case "mkdir":
        call = () => fs.mkdir(value.path, 0o700);
        break;
      case "rmdir":
        call = () => fs.rmdir(value.path);
        break;
      case "unlink":
        call = () => fs.unlink(value.path);
        break;
      case "mkdirat":
        call = () => fs.mkdirat(base, value.path, 0o700);
        break;
      case "unlinkat":
        call = () =>
          fs.unlinkat(base, value.path, value.remove_directory ?? false);
        break;
      default:
        throw new Error(`unknown reserved-path operation ${value.operation}`);
    }
    const error = await refuses(call, "Filesystem", value.errno);
    check(
      error.operation === `filesystem.${value.operation}` &&
        error.outcome === "not_committed" &&
        error.completed_bytes === 0,
    );
    if ((await snapshot()) !== before)
      throw new Error(`namespace changed after ${value.id}`);
  }
  equal(await fs.readFile("/reserved/file"), utf8("kept"));
  if (group === "rmdir") {
    equal((await child.readdir()).name, utf8("."));
    const cookie = await child.telldir();
    await fs.rmdir("/reserved/child");
    check((await child.fstat()).nlink === 0n);
    check(
      (await child.readdir()) === null && (await child.telldir()) === cookie,
    );
    await child.rewinddir();
    check((await child.readPage(3)).length === 0);
    await child.seekdir(cookie);
    check((await child.readdir()) === null);
    await refuses(
      () => fs.mkdirat(child, "refused", 0o700),
      "Filesystem",
      "ENOENT",
    );
  }
  await child.closedir();
  await parent.closedir();
  await fs.unlink("/reserved/file");
  if (group !== "rmdir") await fs.rmdir("/reserved/child");
  await fs.rmdir("/reserved");
  while ((await fs.collectGarbage()) > 0) {}
}
async function namespaceProfile(fs, ctx) {
  while ((await fs.collectGarbage()) > 0) {}
  await fs.mkdir("/namespace", 0o700);
  for (const name of ["source", "empty", "nonempty"])
    await fs.mkdir(`/namespace/${name}`, 0o700);
  await fs.mkdir("/namespace/nonempty/child", 0o700);
  await fs.writeFile("/namespace/file", utf8("source"));
  await fs.writeFile("/namespace/other", utf8("victim"));
  const paths = [
    "/",
    "/namespace",
    "/namespace/file",
    "/namespace/other",
    "/namespace/source",
    "/namespace/empty",
    "/namespace/nonempty",
    "/namespace/nonempty/child",
  ];
  const encode = (value) =>
    JSON.stringify(value, (_, value) =>
      typeof value === "bigint" ? String(value) : value,
    );
  const snapshot = async () => {
    const stats = [];
    for (const path of paths) stats.push(await fs.stat(path));
    return encode([stats, await fs.statvfs()]);
  };
  const before = await snapshot();
  check(
    encode(await fs.stat("/../../namespace//./file")) ===
      encode(await fs.stat("namespace/file")),
  );
  const refusal = async (call, operation, errno) => {
    const error = await refuses(call, "Filesystem", errno);
    check(
      error.operation === `filesystem.${operation}` &&
        error.outcome === "not_committed" &&
        error.completed_bytes === 0,
    );
    check((await snapshot()) === before);
  };
  for (const [path, errno] of [
    ["", "ENOENT"],
    ["/namespace/fi\0le", "EINVAL"],
    ["/namespace/" + "x".repeat(256), "ENAMETOOLONG"],
    ["/x".repeat(65), "ENAMETOOLONG"],
    ["/x".repeat(2050), "ENAMETOOLONG"],
    ["/namespace/file/child", "ENOTDIR"],
    ["/namespace/file/", "ENOTDIR"],
    ["/namespace/missing", "ENOENT"],
  ])
    await refusal(() => fs.stat(path), "stat", errno);
  await refusal(
    () => fs.open("/namespace/file", { access: "write", create_new: true }),
    "open",
    "EEXIST",
  );
  await refusal(() => fs.open("/namespace"), "open", "EISDIR");
  const version = await fs.stat("/namespace/file");
  for (const offset of [0n, 3n, 6n]) {
    const after = await fs.editFile("/namespace/file", version, {
      offset,
      delete_bytes: 0n,
      insert: new Uint8Array(),
    });
    check(after.inode === version.inode && after.version === version.version);
    check((await snapshot()) === before);
    equal(await fs.readFile("/namespace/file"), utf8("source"));
  }
  for (const path of ["/", "/namespace/source", "/namespace/nonempty"]) {
    const version = await fs.stat(path);
    for (const insert of [new Uint8Array(), utf8("x")]) {
      await refusal(
        () =>
          fs.editFile(path, version, { offset: 0n, delete_bytes: 0n, insert }),
        "edit_file",
        "EISDIR",
      );
      const error = await refuses(
        () =>
          ctx.storage.apply({
            puts: { "directory-edit-must-rollback": true },
            filesystem_mutations: [
              {
                op: "write_file",
                path: "/namespace/file",
                data_base64: bytesToBase64(utf8("changed")),
              },
              {
                op: "edit_file",
                path,
                inode: String(version.inode),
                version: String(version.version),
                offset: "0",
                delete_bytes: "0",
                data_base64: bytesToBase64(insert),
              },
            ],
          }),
        "Filesystem",
        "EISDIR",
      );
      check(
        error.operation === "storage.apply" &&
          error.outcome === "not_committed" &&
          error.completed_bytes === 0,
      );
      check(!(await ctx.storage.read("directory-edit-must-rollback")).present);
      check((await snapshot()) === before);
      equal(await fs.readFile("/namespace/file"), utf8("source"));
    }
  }
  const directory = await fs.stat("/namespace/source");
  const directoryRequest = {
    operation: "edit",
    path: "/namespace/source",
    inode: String(directory.inode),
    version: String(directory.version),
    offset: 0,
    delete_bytes: 0,
    insert: "",
  };
  await ctx.storage.apply({
    puts: { "directory-edit-request": directoryRequest },
  });
  const directoryTools = createFilesystemTools(ctx, {
    prefix: "directory-edit-tools:",
  });
  const directoryRefusal = await directoryTools.execute(
    "empty-directory-edit",
    directoryRequest,
  );
  check(
    !directoryRefusal.replayed &&
      directoryRefusal.result.error.errno === "EISDIR" &&
      directoryRefusal.result.error.outcome === "not_committed",
  );
  check((await snapshot()) === before);
  await refusal(() => fs.mkdir("/namespace/file", 0o700), "mkdir", "EEXIST");
  await refusal(() => fs.unlink("/namespace/source"), "unlink", "EPERM");
  await refusal(() => fs.rmdir("/namespace/file"), "rmdir", "ENOTDIR");
  await refusal(() => fs.rmdir("/namespace/nonempty"), "rmdir", "ENOTEMPTY");
  await refusal(() => fs.rmdir("/"), "rmdir", "EBUSY");
  for (const [old, path, errno] of [
    ["/namespace/file", "/namespace/file/", "ENOTDIR"],
    ["/namespace/file/", "/namespace/file", "ENOTDIR"],
    ["/namespace/file", "/namespace/missing/", "ENOTDIR"],
    ["/namespace/file", "/namespace/empty", "EISDIR"],
    ["/namespace/source", "/namespace/file", "ENOTDIR"],
    ["/namespace/source", "/namespace/nonempty", "ENOTEMPTY"],
    ["/namespace/nonempty", "/namespace/nonempty/child/moved", "EINVAL"],
    ["/namespace/source/.", "/namespace/empty", "EINVAL"],
  ])
    await refusal(() => fs.rename(old, path), "rename", errno);
  await fs.rename("/namespace/file", "/namespace/file");
  await fs.rename("/namespace/source", "/namespace/source/");
  check((await snapshot()) === before);
  equal(await fs.readFile("/namespace/file"), utf8("source"));
  equal(await fs.readFile("/namespace/other"), utf8("victim"));

  const source = await fs.opendir("/namespace/source"),
    displaced = await fs.opendir("/namespace/empty");
  const parentLinks = (await fs.stat("/namespace")).nlink;
  await fs.rename("/namespace/source", "/namespace/empty/");
  check(
    (await fs.stat("/namespace/empty")).inode === (await source.fstat()).inode,
  );
  check(
    (await displaced.fstat()).nlink === 0n &&
      (await fs.stat("/namespace")).nlink === parentLinks - 1n,
  );
  check((await displaced.readdir()) === null);
  await refuses(() => fs.stat("/namespace/source"), "Filesystem", "ENOENT");
  await refuses(
    () => fs.mkdirat(displaced, "refused", 0o700),
    "Filesystem",
    "ENOENT",
  );
  await source.closedir();
  await displaced.closedir();
  const file = await fs.open("/namespace/file"),
    victim = await fs.open("/namespace/other");
  await fs.rename("/namespace/file", "/namespace/other");
  check(
    (await fs.stat("/namespace/other")).inode === (await file.fstat()).inode &&
      (await victim.fstat()).nlink === 0n,
  );
  await fs.unlink("/namespace/other");
  await fs.collectGarbage();
  for (const [handle, text] of [
    [file, "source"],
    [victim, "victim"],
  ]) {
    equal(await handle.pread(6, 0n), utf8(text));
    check((await handle.fstat()).nlink === 0n);
    await handle.close();
  }
  for (const path of [
    "/namespace/nonempty/child",
    "/namespace/nonempty",
    "/namespace/empty",
    "/namespace",
  ])
    await fs.rmdir(path);
  while ((await fs.collectGarbage()) > 0) {}
  const replay = await directoryTools.execute(
    "empty-directory-edit",
    directoryRequest,
  );
  check(
    replay.replayed &&
      encode(replay.result) === encode(directoryRefusal.result),
  );
}
async function argumentProfile(fs) {
  while ((await fs.collectGarbage()) > 0) {}
  const path = "/argument-file";
  await fs.writeFile(path, utf8("original"));
  await fs.mkdir("/argument-directory", 0o700);
  const file = await fs.open(path, { access: "read_write" });
  const directory = await fs.opendir("/argument-directory");
  const version = await file.fstat();
  const edit = { offset: 0n, delete_bytes: 0n, insert: utf8("x") };
  const snapshot = async () =>
    JSON.stringify(
      [
        await fs.stat("/"),
        await file.fstat(),
        await directory.fstat(),
        await fs.statvfs(),
        await file.lseek(0n, "current"),
        await directory.telldir(),
      ],
      (_, value) => (typeof value === "bigint" ? String(value) : value),
    );
  const before = await snapshot();
  const detached = new Uint8Array([1]);
  check(detached.buffer.transfer().byteLength === 1);
  const cases = [
    [
      () => fs.open({}, { access: "write", create: true, truncate: true }),
      "open",
    ],
    [() => fs.creat({}), "creat"],
    [() => fs.openat(directory, {}), "openat"],
    [() => fs.stat({}), "stat"],
    [() => fs.lstat({}), "lstat"],
    [() => fs.fstatat(directory, {}), "fstatat"],
    [() => fs.truncate({}, 0n), "truncate"],
    [() => fs.mkdir({}), "mkdir"],
    [() => fs.mkdirat(directory, {}), "mkdirat"],
    [() => fs.rmdir({}), "rmdir"],
    [() => fs.unlink({}), "unlink"],
    [() => fs.unlinkat(directory, {}), "unlinkat"],
    [() => fs.rename({}, path), "rename"],
    [() => fs.rename(path, {}), "rename"],
    [() => fs.renameat(directory, {}, directory, "target"), "renameat"],
    [() => fs.renameat(directory, "source", directory, {}), "renameat"],
    [() => fs.chmod({}, 0o600), "chmod"],
    [() => fs.utimens({}, 0n), "utimens"],
    [() => fs.opendir({}), "opendir"],
    [() => fs.readFile({}), "read_file"],
    [() => fs.writeFile({}, utf8("bad")), "write_file"],
    [() => fs.replaceFile({}, utf8("bad")), "replace_file"],
    [() => fs.editFile({}, version, edit), "edit_file"],
    [() => fs.readDir({}), "read_dir"],
    [() => file.pread(1, 0.5), "pread"],
    [() => file.pwrite(utf8("bad"), 0.5), "pwrite"],
    [() => file.lseek(0.5), "lseek"],
    [() => file.ftruncate(0.5), "ftruncate"],
    [() => fs.truncate(path, 0.5), "truncate"],
    [() => fs.utimens(path, 0.5), "utimens"],
    [() => fs.editFile(path, version, { ...edit, offset: 0.5 }), "edit_file"],
    [
      () => fs.editFile(path, version, { ...edit, delete_bytes: 0.5 }),
      "edit_file",
    ],
    [() => fs.editFile(path, { ...version, inode: 0.5 }, edit), "edit_file"],
    [() => fs.editFile(path, { ...version, version: 0.5 }, edit), "edit_file"],
    [() => file.write({}), "write"],
    [() => file.pwrite({}, 0n), "pwrite"],
    [() => file.writeAll({}), "write_all"],
    [() => fs.writeFile(path, {}), "write_file"],
    [() => fs.replaceFile(path, {}), "replace_file"],
    [() => fs.editFile(path, version, { ...edit, insert: {} }), "edit_file"],
    [() => fs.openat(null, path), "openat"],
    [() => fs.fstatat(null, path), "fstatat"],
    [() => fs.mkdirat(null, "new"), "mkdirat"],
    [() => fs.unlinkat(null, path), "unlinkat"],
    [() => fs.renameat(null, path, directory, "target"), "renameat"],
    [() => fs.renameat(directory, "source", null, path), "renameat"],
    [() => fs.editFile(path, null, edit), "edit_file"],
    [() => fs.editFile(path, {}, edit), "edit_file"],
    [() => fs.editFile(path, version, null), "edit_file"],
    [() => fs.editFile(path, version, {}), "edit_file"],
    [() => fs.writeFile(path, detached), "write_file"],
    [() => fs.replaceFile(path, detached), "replace_file"],
    [
      () => fs.editFile(path, version, { ...edit, insert: detached }),
      "edit_file",
    ],
    [() => file.write(detached), "write"],
    [() => file.pwrite(detached, 0n), "pwrite"],
    [() => file.writeAll(detached), "write_all"],
    [() => fs.stat(detached), "stat"],
  ];
  check(cases.length === 57);
  try {
    for (const [call, operation] of cases) {
      let pending;
      try {
        pending = call();
      } catch {
        throw new Error(`argument ${operation} threw synchronously`);
      }
      check(pending !== null && typeof pending.then === "function");
      let error;
      try {
        await pending;
      } catch (caught) {
        error = caught;
      }
      if (error === undefined)
        throw new Error(
          `argument ${operation} unexpectedly succeeded; state_unchanged=${(await snapshot()) === before}`,
        );
      if (
        error?.code !== "ValidationError" ||
        error.errno !== "EINVAL" ||
        error.operation !== `filesystem.${operation}` ||
        error.outcome !== "not_committed" ||
        error.completed_bytes !== 0
      )
        throw new Error(
          `argument ${operation}: code=${error?.code}, errno=${error?.errno}, operation=${error?.operation}, outcome=${error?.outcome}, completed=${error?.completed_bytes}`,
        );
      check((await snapshot()) === before);
    }
    equal(await fs.readFile(path), utf8("original"));
    check((await file.write(new Uint8Array())) === 0);
    check((await file.pwrite(new Uint8Array(), 0n)) === 0);
    check((await snapshot()) === before);
  } finally {
    await file.close();
    await directory.closedir();
  }
  await fs.unlink(path);
  await fs.rmdir("/argument-directory");
  while ((await fs.collectGarbage()) > 0) {}
}

let liveAuthority;

async function liveAuthorityOwner(fs, ctx) {
  check(liveAuthority === undefined);
  await fs.mkdir("/at-authority", 0o700);
  await fs.mkdir("/at-authority/empty", 0o700);
  await fs.writeFile("/at-authority/source", utf8("owner-source"));
  await fs.writeFile("/at-authority/target", utf8("owner-target"));
  const directory = await fs.opendir("/at-authority");
  const file = await fs.open("/owner-orphan", {
    access: "read_write",
    create_new: true,
  });
  await file.writeAll(utf8("pinned-owner"));
  await file.lseek(0n);
  await fs.unlink("/owner-orphan");
  const snapshot = async () => {
    const stats = [];
    for (const path of [
      "/",
      "/at-authority",
      "/at-authority/empty",
      "/at-authority/source",
      "/at-authority/target",
    ])
      stats.push(await fs.stat(path));
    return JSON.stringify(
      [
        stats,
        await fs.statvfs(),
        await directory.telldir(),
        await file.fstat(),
        await file.lseek(0n, "current"),
      ],
      (_, value) => (typeof value === "bigint" ? String(value) : value),
    );
  };
  try {
    const before = await snapshot();
    check(typeof ctx.identity.scope === "string");
    liveAuthority = {
      directory,
      file,
      fs,
      cookie: await directory.telldir(),
      scope: ctx.identity.scope,
    };
    // The owner invocation remains pending while another live cell uses the
    // same path names and attempts to borrow its directory token.
    const result = await ctx
      .cell("COUNTERS", "authority-caller")
      .fetch(request("http://fixture/authority-caller/authority-live-caller"));
    if (result.status !== 200)
      throw new Error(
        `live caller ${result.status}: ${Array.from(result.body.slice(0, 512), (byte) => String.fromCharCode(byte)).join("")}`,
      );
    equal(result.body, utf8("ok"));
    check((await snapshot()) === before);
    equal(await fs.readFile("/at-authority/source"), utf8("owner-source"));
    equal(await fs.readFile("/at-authority/target"), utf8("owner-target"));
    equal(await file.pread(32, 0n), utf8("pinned-owner"));
  } finally {
    liveAuthority = undefined;
    await file.close();
    await directory.closedir();
  }
  await fs.unlink("/at-authority/source");
  await fs.unlink("/at-authority/target");
  await fs.rmdir("/at-authority/empty");
  await fs.rmdir("/at-authority");
  while ((await fs.collectGarbage()) > 0) {}
}

async function authorityProfile(fs, kind, supplied) {
  await fs.mkdir("/at-authority", 0o700);
  await fs.mkdir("/at-authority/empty", 0o700);
  await fs.writeFile("/at-authority/source", utf8("source"));
  await fs.writeFile("/at-authority/target", utf8("target"));
  const local = await fs.opendir("/at-authority");
  let bad;
  if (kind === "expired" || kind === "live") {
    check(supplied !== undefined);
    bad = supplied;
  } else if (kind === "closed") {
    bad = await fs.opendir("/");
    await bad.closedir();
  } else {
    check(kind === "file");
    bad = await fs.open("/at-authority/source");
  }
  const paths = [
    "/",
    "/at-authority",
    "/at-authority/empty",
    "/at-authority/source",
    "/at-authority/target",
  ];
  const encode = (value) =>
    JSON.stringify(value, (_, value) =>
      typeof value === "bigint" ? String(value) : value,
    );
  const snapshot = async () => {
    const stats = [];
    for (const path of paths) stats.push(await fs.stat(path));
    return encode([stats, await fs.statvfs(), await local.telldir()]);
  };
  const before = await snapshot();
  let refused = 0;
  const refusal = async (
    call,
    operation,
    code = "Filesystem",
    errno = "EBADF",
  ) => {
    const error = await refuses(call, code, errno === null ? undefined : errno);
    if (
      error.operation !== `filesystem.${operation}` ||
      error.outcome !== "not_committed" ||
      error.completed_bytes !== 0
    )
      throw new Error(
        `authority ${operation}: operation=${error.operation}, outcome=${error.outcome}, completed_bytes=${error.completed_bytes}`,
      );
    check((await snapshot()) === before);
    refused++;
  };
  if (kind === "live") {
    const owner = liveAuthority;
    for (const [call, operation] of [
      [() => owner.file.read(1), "read"],
      [() => owner.file.write(utf8("bad")), "write"],
      [() => owner.file.pread(1, 0n), "pread"],
      [() => owner.file.pwrite(utf8("bad"), 0n), "pwrite"],
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
      [() => owner.directory.readdir(), "readdir"],
      [() => owner.directory.readPage(1), "read_page"],
      [() => owner.directory.telldir(), "telldir"],
      [() => owner.directory.seekdir(owner.cookie), "seekdir"],
      [() => owner.directory.rewinddir(), "rewinddir"],
      [() => owner.directory.closedir(), "closedir"],
      [() => owner.directory.fstat(), "fstat"],
      [() => owner.directory.fsync(), "fsync"],
      [() => owner.fs.capabilities(), "capabilities"],
      [() => owner.fs.stat("/at-authority/source"), "stat"],
      [() => owner.fs.readFile("/at-authority/source"), "read_file"],
      [() => owner.fs.sync(), "sync"],
      [() => owner.fs.collectGarbage(), "collect_garbage"],
    ])
      await refusal(call, operation, "ContextMismatch", null);
  }
  for (const prefix of ["at-authority/", "/at-authority/"]) {
    const source = prefix + "source",
      target = prefix + "target",
      empty = prefix + "empty",
      created = prefix + "created";
    await refusal(
      () => fs.openat(bad, created, { access: "write", create_new: true }),
      "openat",
    );
    await refusal(
      () => fs.openat(bad, source, { access: "write", truncate: true }),
      "openat",
    );
    for (const noFollow of [false, true])
      await refusal(() => fs.fstatat(bad, source, noFollow), "fstatat");
    await refusal(() => fs.mkdirat(bad, created, 0o700), "mkdirat");
    await refusal(() => fs.unlinkat(bad, source), "unlinkat");
    await refusal(() => fs.unlinkat(bad, empty, true), "unlinkat");
    await refusal(() => fs.renameat(bad, source, local, "target"), "renameat");
    await refusal(() => fs.renameat(local, "source", bad, target), "renameat");
  }
  equal(await fs.readFile("/at-authority/source"), utf8("source"));
  equal(await fs.readFile("/at-authority/target"), utf8("target"));
  await refuses(() => fs.stat("/at-authority/created"), "Filesystem", "ENOENT");
  if (kind === "file") await bad.close();
  await local.closedir();
  await fs.unlink("/at-authority/source");
  await fs.unlink("/at-authority/target");
  await fs.rmdir("/at-authority/empty");
  await fs.rmdir("/at-authority");
  while ((await fs.collectGarbage()) > 0) {}
  return refused;
}
class Counter {
  async fetch(req, ctx) {
    let phase = "start";
    try {
      const action = req.url.split("/").at(-1);
      const fs = ctx.fs;
      if (action === "exercise") {
        phase = "capabilities";
        check(!(await fs.capabilities()).symlinks);
        await fs.mkdir("/dir", 0o700);
        phase = "write";
        await fs.writeFile("/notes", utf8("one"));
        phase = "open";
        const file = await fs.open("/notes", {
          access: "read_write",
          append: true,
        });
        phase = "positional";
        await file.pwrite(utf8("ONE"), 0n);
        phase = "append";
        await file.writeAll(utf8("!"));
        phase = "pread";
        equal(await file.pread(4, 0n), utf8("ONE!"));
        await file.lseek(1n);
        phase = "dup";
        const dup = await file.dup();
        equal(await dup.read(2), utf8("NE"));
        check((await file.lseek(0n, "current")) === 3n);
        await dup.close();
        phase = "barrier";
        await file.fsync();
        phase = "close";
        await file.close();
        await refuses(() => file.fstat(), "Filesystem", "EBADF");
        await refuses(
          () => file.writeAll(new Uint8Array()),
          "Filesystem",
          "EBADF",
        );
        await refuses(() => file.readExact(0), "Filesystem", "EBADF");
        phase = "raw-name";
        const raw = new Uint8Array([...utf8("/dir/"), 255]);
        await fs.writeFile(raw, utf8("binary"));
        phase = "directory";
        const dir = await fs.opendir("/dir");
        equal((await dir.readdir()).name, utf8("."));
        equal((await dir.readdir()).name, utf8(".."));
        const cookie = await dir.telldir();
        const entry = await dir.readdir();
        equal(entry.name, new Uint8Array([255]));
        await dir.seekdir(cookie);
        equal((await dir.readdir()).name, new Uint8Array([255]));
        phase = "rename";
        await fs.rename("/dir", "/moved");
        phase = "relative";
        const relative = await fs.openat(dir, new Uint8Array([255]));
        equal(await relative.readExact(6), utf8("binary"));
        await relative.close();
        await dir.closedir();
        phase = "helpers";
        await fs.writeFile("/published", utf8("old"));
        const old = await fs.open("/published");
        await fs.replaceFile("/published", utf8("new"));
        equal(await old.readExact(3), utf8("old"));
        const before = await fs.stat("/published");
        check((await old.fstat()).inode !== before.inode);
        const patch = { offset: 1n, delete_bytes: 1n, insert: utf8("OO") };
        const after = await fs.editFile("/published", before, patch);
        check(after.inode === before.inode && after.version > before.version);
        equal(await fs.readFile("/published"), utf8("nOOw"));
        await refuses(
          () => fs.editFile("/published", before, patch),
          "Conflict",
        );
        await refuses(
          () => fs.readFile("/published", 2),
          "Filesystem",
          "EFBIG",
        );
        await old.close();
        const first = await fs.readDir("/", 1);
        const second = await fs.readDir("/", 1, first.next);
        check(first.entries.length === 1 && second.entries.length === 1);
        check(
          bytesToBase64(first.entries[0].name) !==
            bytesToBase64(second.entries[0].name),
        );
        phase = "batch";
        const stat = await fs.stat("/notes");
        await ctx.storage.apply({
          checks: { tool: { present: false, value: null } },
          filesystem_checks: [
            {
              path: "/notes",
              present: true,
              inode: String(stat.inode),
              version: String(stat.version),
            },
          ],
          filesystem_mutations: [
            {
              op: "write_file",
              path: "/artifact",
              data_base64: bytesToBase64(utf8("saved")),
            },
          ],
          puts: {
            tool: { result: "saved", source_version: String(stat.version) },
          },
        });
        phase = "conflict";
        await refuses(
          () =>
            ctx.storage.apply({
              filesystem_checks: [
                {
                  path: "/notes",
                  present: true,
                  inode: String(stat.inode),
                  version: "0",
                },
              ],
              filesystem_mutations: [
                {
                  op: "write_file",
                  path: "/must-not-exist",
                  data_base64: "eA==",
                },
              ],
              puts: { bad: true },
            }),
          "Conflict",
        );
        await refuses(() => fs.stat("/must-not-exist"), "Filesystem", "ENOENT");
        check(!(await ctx.storage.read("bad")).present);
        phase = "capture";
        this.old = await fs.open("/notes");
        await fs.sync();
      } else if (action === "readonly") {
        phase = "readonly";
        await readonlyProfile(fs, ctx);
      } else if (action === "host-admission") {
        phase = "host-admission";
        await hostAdmissionProfile(fs);
      } else if (action === "authority-capture") {
        this.oldDirectory = await fs.opendir("/");
      } else if (action === "authority-live") {
        phase = action;
        await liveAuthorityOwner(fs, ctx);
      } else if (action === "authority-live-caller") {
        phase = action;
        check(liveAuthority !== undefined);
        check(
          typeof ctx.identity.scope === "string" &&
            ctx.identity.scope !== liveAuthority.scope,
        );
        check(
          (await authorityProfile(fs, "live", liveAuthority.directory)) === 45,
        );
      } else if (action.startsWith("authority-")) {
        phase = action;
        check(
          (await authorityProfile(
            fs,
            action.slice("authority-".length),
            this.oldDirectory,
          )) === 18,
        );
        if (action === "authority-expired") this.oldDirectory = undefined;
      } else if (action.startsWith("reserved-")) {
        phase = action;
        await reservedNamespaceProfile(fs, action.slice("reserved-".length));
      } else if (action === "namespace") {
        phase = "namespace-profile";
        await namespaceProfile(fs, ctx);
      } else if (action === "arguments") {
        phase = "arguments";
        await argumentProfile(fs);
      } else if (action === "metadata") {
        phase = "metadata-profile";
        await fs.writeFile("/metadata", utf8("original"));
        const initial = await fs.stat("/metadata");
        check(
          initial.mode === 0o644 &&
            initial.uid === 1000 &&
            initial.gid === 1000 &&
            initial.atime_ns === 0n,
        );
        await refuses(
          () => fs.open("/metadata", { truncate: true }),
          "Filesystem",
          "EINVAL",
        );
        await refuses(
          () =>
            fs.open("/metadata", {
              access: "write",
              create_new: true,
              truncate: true,
            }),
          "Filesystem",
          "EEXIST",
        );
        await refuses(
          () =>
            fs.open("/mode-refused", {
              access: "write",
              create: true,
              mode: 0o1600,
            }),
          "Filesystem",
          "EOPNOTSUPP",
        );
        await refuses(
          () =>
            fs.open("/mode-refused", {
              access: "write",
              create: true,
              sync: true,
            }),
          "ValidationError",
        );
        await refuses(() => fs.stat("/mode-refused"), "Filesystem", "ENOENT");
        equal(await fs.readFile("/metadata"), utf8("original"));
        check((await fs.stat("/metadata")).version === initial.version);
        const file = await fs.open("/metadata", { access: "read_write" });
        const helperLimit = (await fs.capabilities()).limits
          .max_file_helper_bytes;
        const helperSnapshot = async () =>
          JSON.stringify(
            [
              await file.fstat(),
              await fs.statvfs(),
              await file.lseek(0n, "current"),
            ],
            (_, value) => (typeof value === "bigint" ? String(value) : value),
          );
        const helperBefore = await helperSnapshot();
        for (const [call, operation] of [
          [() => file.readExact(-1), "read_exact"],
          [() => file.readExact(helperLimit + 1), "read_exact"],
          [() => file.writeAll(new Uint8Array(helperLimit + 1)), "write_all"],
        ]) {
          const error = await refuses(call, "ValidationError");
          check(
            error.operation === `filesystem.${operation}` &&
              error.outcome === "not_committed" &&
              error.completed_bytes === 0,
          );
          check((await helperSnapshot()) === helperBefore);
        }
        equal(await fs.readFile("/metadata"), utf8("original"));
        await file.fchmod(0o400);
        await refuses(
          () => fs.open("/metadata", { access: "write" }),
          "Filesystem",
          "EACCES",
        );
        await file.pwrite(utf8("kept"), 0n); // Already-open access survives chmod.
        await fs.chmod("/metadata", 0o600);
        const version = (await file.fstat()).version;
        for (const time of [
          -123456789n,
          -9223372036854775808n,
          9223372036854775807n,
        ]) {
          await fs.utimens("/metadata", time);
          const value = await file.fstat();
          check(
            value.mtime_ns === time &&
              value.atime_ns === 0n &&
              value.version === version,
          );
        }
        const omitted = await file.fstat();
        await fs.utimens("/metadata", null);
        await refuses(
          () => fs.utimens("/metadata", "9223372036854775808"),
          "ValidationError",
        );
        const unchanged = await file.fstat();
        check(
          unchanged.mtime_ns === omitted.mtime_ns &&
            unchanged.ctime_ns === omitted.ctime_ns &&
            unchanged.version === omitted.version,
        );
        const before = BigInt(await ctx.now()) * 1000000n;
        await fs.utimens("/metadata");
        const now = await file.fstat(),
          after = BigInt(await ctx.now()) * 1000000n;
        check(
          now.mtime_ns >= before &&
            now.mtime_ns <= after &&
            now.ctime_ns >= before &&
            now.ctime_ns <= after &&
            now.version === version,
        );
        const distant = 9007199254740993n;
        check((await file.lseek(distant)) === distant);
        equal(await file.read(1), new Uint8Array());
        await refuses(() => file.write(utf8("x")), "Filesystem", "EFBIG");
        await refuses(
          () => file.pread(1, 9223372036854775808n),
          "Filesystem",
          "EOVERFLOW",
        );
        await refuses(
          () => file.lseek(9223372036854775808n),
          "Filesystem",
          "EOVERFLOW",
        );
        check(
          (await file.lseek(0n, "current")) === distant &&
            (await file.fstat()).size === 8n,
        );
        await fs.truncate("/metadata", 3n);
        await file.ftruncate(8n);
        equal(
          await file.pread(8, 0n),
          new Uint8Array([107, 101, 112, 0, 0, 0, 0, 0]),
        );
        check((await file.lseek(0n, "current")) === distant);
        check(
          (await fs.lstat("/metadata")).inode === (await file.fstat()).inode,
        );
        await file.fdatasync();
        await file.close();
        const created = await fs.creat("/created", 0o600);
        await created.writeAll(utf8("creat"));
        check((await created.fstat()).mode === 0o600);
        await created.close();
        await fs.unlink("/created");
        const space = await fs.statvfs(),
          limits = (await fs.capabilities()).limits;
        check(
          space.logical_bytes + space.available_logical_bytes ===
            limits.max_logical_bytes,
        );
        check(
          space.allocated_bytes + space.available_allocated_bytes ===
            limits.max_allocated_bytes,
        );
        check(space.inodes + space.available_inodes === limits.max_inodes);

        phase = "directory-relative-profile";
        await fs.mkdir("/at-source", 0o700);
        await fs.mkdir("/at-destination", 0o700);
        const source = await fs.opendir("/at-source"),
          destination = await fs.opendir("/at-destination");
        await fs.mkdirat(source, "sub", 0o777);
        check((await fs.fstatat(source, "sub", true)).mode === 0o755);
        const raw = new Uint8Array([255]),
          renamed = new Uint8Array([254]);
        const held = await fs.openat(source, raw, {
          access: "read_write",
          create_new: true,
        });
        await held.writeAll(utf8("relative"));
        const identity = await held.fstat();
        check((await fs.fstatat(source, raw, true)).inode === identity.inode);
        await fs.rename("/at-source", "/at-moved");
        await fs.renameat(source, raw, destination, renamed);
        await refuses(() => fs.fstatat(source, raw), "Filesystem", "ENOENT");
        check(
          (await fs.fstatat(destination, renamed)).inode === identity.inode,
        );
        await fs.unlinkat(destination, renamed);
        check((await held.fstat()).nlink === 0n);
        equal(await held.pread(8, 0n), utf8("relative"));
        await fs.unlinkat(source, "sub", true);
        await source.rewinddir();
        const empty = await source.readPage(3);
        check(empty.length === 2);
        equal(empty[0].name, utf8("."));
        equal(empty[1].name, utf8(".."));
        await destination.fsync();
        await held.close();
        await source.closedir();
        await destination.closedir();
        check((await fs.readDir("/at-moved", 10)).entries.length === 0);
        await fs.rmdir("/at-moved");
        await fs.rmdir("/at-destination");
      } else if (action === "replay") {
        const receipt = await ctx.storage.read("tool");
        check(receipt.present && receipt.value.result === "saved");
        equal(await fs.readFile("/artifact"), utf8(receipt.value.result));
      } else if (action === "expired") {
        let original;
        try {
          await this.old.fstat();
          throw new Error("expired handle worked");
        } catch (error) {
          check(["ContextExpired", "ContextMismatch"].includes(error.code));
          original = error;
        }
        const encode = (value) =>
          JSON.stringify(value, (_, value) =>
            typeof value === "bigint" ? String(value) : value,
          );
        const before = encode(await fs.stat("/notes")),
          contents = await fs.readFile("/notes");
        for (const [call, operation] of [
          [() => this.old.readExact(0), "read_exact"],
          [() => this.old.readExact(1), "read_exact"],
          [() => this.old.writeAll(new Uint8Array()), "write_all"],
          [() => this.old.writeAll(utf8("x")), "write_all"],
        ]) {
          const error = await refuses(call, original.code);
          check(
            error.outcome === original.outcome &&
              error.errno === original.errno,
          );
          if (error.operation !== `filesystem.${operation}`)
            throw new Error(
              `expired helper operation mismatch: ${error.operation} instead of filesystem.${operation}`,
            );
          check(error.completed_bytes === 0);
          check(encode(await fs.stat("/notes")) === before);
        }
        equal(await fs.readFile("/notes"), contents);
        this.old = undefined;
      } else if (action === "isolated") {
        await refuses(() => fs.stat("/artifact"), "Filesystem", "ENOENT");
      } else if (action === "tools") {
        phase = "tools";
        const tools = createFilesystemTools(ctx);
        const invalid = await tools.execute("invalid-read-bound", {
          operation: "read",
          path: "/tool-note",
          max_bytes: 0,
        });
        check(
          invalid.result.error.code === "ValidationError" &&
            invalid.result.error.outcome === "not_committed",
        );
        check(
          (
            await tools.execute("invalid-read-bound", {
              operation: "read",
              path: "/tool-note",
              max_bytes: 0,
            })
          ).replayed,
        );
        const decision = {
          operation: "write",
          path: "/tool-note",
          content: "first",
        };
        await ctx.storage.apply({
          puts: { agent: { phase: "decided", request: decision } },
        });
        const prior = await ctx.storage.read("agent");
        const written = await tools.execute("write-one", decision, {
          checks: { agent: prior },
          puts: {
            agent: { phase: "tool-complete", operation_id: "write-one" },
          },
        });
        check(!written.replayed && written.result.written_bytes === 5);
        check(
          (await ctx.storage.read("agent")).value.phase === "tool-complete",
        );
        const read = await tools.execute("read-one", {
          operation: "read",
          path: "/tool-note",
        });
        check(read.result.text === "first" && !read.replayed);
        await fs.writeFile("/tool-note", utf8("later"));
        const replay = await tools.execute("read-one", {
          operation: "read",
          path: "/tool-note",
        });
        check(replay.replayed && replay.result.text === "first");
        check((await tools.execute("write-one", decision)).replayed);
        equal(await fs.readFile("/tool-note"), utf8("later"));
        await refuses(
          () =>
            tools.execute("write-one", { ...decision, content: "different" }),
          "ValidationError",
        );
        const raw = new Uint8Array([...utf8("/moved/"), 255]);
        check(
          (
            await tools.execute("read-binary-name", {
              operation: "read",
              path: raw,
            })
          ).result.text === "binary",
        );
        const before = await fs.stat("/tool-note");
        await tools.execute("edit-one", {
          operation: "edit",
          path: "/tool-note",
          inode: before.inode,
          version: before.version,
          offset: 0,
          delete_bytes: 1,
          insert: "L",
        });
        equal(await fs.readFile("/tool-note"), utf8("Later"));
        const stale = await tools.execute("edit-stale", {
          operation: "edit",
          path: "/tool-note",
          inode: before.inode,
          version: before.version,
          offset: 0,
          delete_bytes: 1,
          insert: "X",
        });
        check(
          stale.result.error.code === "Conflict" &&
            stale.result.error.outcome === "not_committed",
        );
        const missing = await tools.execute("missing-read", {
          operation: "read",
          path: "/not-yet-created",
        });
        check(missing.result.error.errno === "ENOENT");
        await fs.writeFile("/not-yet-created", utf8("exists now"));
        check(
          (
            await tools.execute("missing-read", {
              operation: "read",
              path: "/not-yet-created",
            })
          ).result.error.errno === "ENOENT",
        );
        const found = await tools.execute("search-one", {
          operation: "search",
          path: "/",
          pattern: "Later",
        });
        check(
          found.result.matches.some((match) => match.path === "/tool-note"),
        );
        const listing = await tools.execute("list-one", {
          operation: "list",
          path: "/",
          limit: 10,
        });
        check(
          listing.result.entries.some((entry) => entry.name === "tool-note"),
        );
        const mutable = {
          operation: "write",
          path: "/snapshot-request",
          content: "safe",
        };
        const saving = tools.execute("snapshot-one", mutable);
        mutable.content = "changed after admission";
        await saving;
        equal(await fs.readFile("/snapshot-request"), utf8("safe"));
        const limited = createFilesystemTools(ctx, {
          prefix: "limited-tools:",
          maxReceipts: 1,
        });
        await limited.execute("one", { operation: "stat", path: "/tool-note" });
        await refuses(
          () =>
            limited.execute("two", { operation: "stat", path: "/tool-note" }),
          "ValidationError",
        );
        check(
          (
            await limited.execute("one", {
              operation: "stat",
              path: "/tool-note",
            })
          ).replayed,
        );
      } else if (action === "tools-recovered") {
        phase = "tools-recovered";
        const saved = await ctx.storage.read("directory-edit-request");
        check(saved.present);
        const refused = await createFilesystemTools(ctx, {
          prefix: "directory-edit-tools:",
        }).execute("empty-directory-edit", saved.value);
        check(
          refused.replayed &&
            refused.result.error.errno === "EISDIR" &&
            refused.result.error.outcome === "not_committed",
        );
        await refuses(() => fs.stat(saved.value.path), "Filesystem", "ENOENT");
        const tools = createFilesystemTools(ctx);
        const read = await tools.execute("read-one", {
          operation: "read",
          path: "/tool-note",
        });
        check(read.replayed && read.result.text === "first");
        equal(await fs.readFile("/tool-note"), utf8("Later"));
        equal(await fs.readFile("/snapshot-request"), utf8("safe"));
      } else if (action === "gc-start") {
        // Quiesce garbage from earlier replacement tests before taking an exact
        // baseline. This case then verifies automatic reclamation of a new orphan.
        for (let batch = 0; batch < 128; batch++) {
          if ((await fs.collectGarbage()) === 0) break;
          check(batch < 127, "fixture garbage did not drain");
        }
        const baseline = await fs.statvfs();
        await ctx.storage.apply({
          puts: {
            gc_baseline: {
              allocated: String(baseline.allocated_bytes),
              inodes: String(baseline.inodes),
            },
          },
        });
        const orphan = await fs.open("/orphan", {
          access: "read_write",
          create_new: true,
        });
        await orphan.writeAll(new Uint8Array(65536).fill(7));
        await fs.unlink("/orphan");
        equal(await orphan.pread(1, 0n), new Uint8Array([7]));
        await orphan.close();
      } else if (action === "gc-check") {
        const baseline = (await ctx.storage.read("gc_baseline")).value;
        const current = await fs.statvfs();
        return response(
          String(current.allocated_bytes) === baseline.allocated &&
            String(current.inodes) === baseline.inodes
            ? "done"
            : JSON.stringify({
                baseline,
                allocated: String(current.allocated_bytes),
                inodes: String(current.inodes),
              }),
        );
      } else if (action === "recovered") {
        check((await ctx.storage.read("tool")).value.result === "saved");
        equal(await fs.readFile("/artifact"), utf8("saved"));
        await fs.writeFile("/after-recovery", utf8("new"));
        await fs.sync();
      } else throw new Error("unknown filesystem action");
      return response("ok");
    } catch (error) {
      return response(
        `phase=${phase} code=${error.code} errno=${error.errno} detail=${error.message}`,
        { status: 500 },
      );
    }
  }
}
let sessionModelReleased = false;
const sessionModelCalls = {};
const sessionBase = createFilesystemAgentSession({
  configuration: scriptedFilesystemModel.configuration,
  async decide(input, ctx) {
    sessionModelCalls[input.step] = (sessionModelCalls[input.step] ?? 0) + 1;
    if (
      input.prompt === "pause-after-write" &&
      input.step === 1 &&
      !sessionModelReleased
    )
      throw new Error("synthetic provider outage");
    return scriptedFilesystemModel.decide(input, ctx);
  },
});
class AgentSession extends sessionBase {
  async fetch(req, ctx) {
    if (req.url.endsWith("/snapshot")) {
      const stat = await ctx.fs.stat("/notes.txt");
      const data = await ctx.fs.readFile("/notes.txt");
      return jsonResponse({
        version: String(stat.version),
        text: String.fromCharCode(...data),
        model_calls: sessionModelCalls,
      });
    }
    return super.fetch(req, ctx);
  }
}
const app = {
  cells: { Counter, AgentSession },
  async fetch(req, ctx) {
    if (req.url.endsWith("/stateless-fs")) {
      let refused;
      try {
        await ctx.fs.stat("/");
      } catch (error) {
        refused = error;
      }
      check(
        refused?.outcome === "not_committed" &&
          refused.operation === "filesystem.stat",
      );
      return response("ok");
    }
    if (req.url.endsWith("/release-agent")) {
      sessionModelReleased = true;
      return response("ok");
    }
    const agent =
      /\/sessions\/([A-Za-z0-9_-]{1,64})\/(?:turns\/[A-Za-z0-9_-]{1,64}|snapshot)$/.exec(
        req.url,
      );
    if (agent) {
      const result = await ctx.cell("AGENTS", agent[1]).fetch(
        request(req.url, {
          method: req.method,
          body: req.body,
          headers: req.headers,
        }),
      );
      return response(result.body, {
        status: result.status,
        headers: result.headers,
      });
    }
    const name = req.url.split("/").at(-2);
    const result = await ctx.cell("COUNTERS", name).fetch(request(req.url));
    return response(result.body, {
      status: result.status,
      headers: result.headers,
    });
  },
};
