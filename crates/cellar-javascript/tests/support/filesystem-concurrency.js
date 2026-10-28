// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// Read waves fit the default bridge ceiling. Offset/namespace mutations stay
// sequential under the existing one-mutation rule; these controls distinguish
// read concurrency from an accidental relaxation of handle serialization.
async function filesystemConcurrency(ctx, action) {
  const fs = ctx.fs,
    count = 12,
    width = 32;
  const kind = action.split("-")[1],
    recovered = action.endsWith("-recovered");
  const path = `/concurrent-${kind}`,
    key = `concurrent-${kind}`;
  const record = (id) => new Uint8Array(width).fill(id + 1);
  const batch = async (calls) => {
    const settled = await Promise.allSettled(calls);
    for (const item of settled)
      if (item.status === "rejected") throw item.reason;
    return settled.map((item) => item.value);
  };
  const sequence = async (calls) => {
    const values = [];
    for (const call of calls) values.push(await call());
    return values;
  };
  const records = (bytes, first = 0) => {
    check(bytes.length === count * width);
    const ids = [];
    for (let offset = 0; offset < bytes.length; offset += width) {
      const id = bytes[offset];
      equal(bytes.slice(offset, offset + width), record(id - 1));
      ids.push(id);
    }
    ids.sort((a, b) => a - b);
    equal(
      ids,
      Array.from({ length: count }, (_, i) => i + first + 1),
    );
  };
  const snapshot = async () => {
    const stat = await fs.stat(path);
    const metadata = Object.fromEntries(
      Object.entries(stat).map(([k, v]) => [k, String(v)]),
    );
    if (kind === "directory") {
      const entries = (await fs.readDir(path, count + 1)).entries;
      check(entries.length === count);
      const names = entries.map((entry) => bytesToBase64(entry.name)).sort();
      equal(
        names,
        Array.from({ length: count }, (_, i) =>
          bytesToBase64(utf8(`item-${i}`)),
        ).sort(),
      );
      return { metadata, names };
    }
    const file = await fs.open(path);
    try {
      const start = kind === "append" ? 65520n : 0n;
      const bytes = await file.pread(count * width, start);
      records(bytes, kind === "append" ? 32 : 0);
      if (start) {
        equal(await file.pread(32, 0n), new Uint8Array(32));
        equal(await file.pread(32, start - 32n), new Uint8Array(32));
      }
      return { metadata, bytes_base64: bytesToBase64(bytes) };
    } finally {
      await file.close();
    }
  };
  if (recovered) {
    const saved = await ctx.storage.read(key);
    check(saved.present);
    const actual = await snapshot();
    equal(
      Object.keys(actual.metadata).sort(),
      Object.keys(saved.value.metadata).sort(),
    );
    for (const [field, value] of Object.entries(actual.metadata))
      check(value === saved.value.metadata[field]);
    if (kind === "directory") equal(actual.names, saved.value.names);
    else check(actual.bytes_base64 === saved.value.bytes_base64);
    return { kind, recovered: true, count, ...actual };
  }
  if (kind === "shared") {
    const seed = new Uint8Array(count * width);
    for (let i = 0; i < count; i++) seed.set(record(i), i * width);
    await fs.writeFile(path, seed);
    const file = await fs.open(path, { access: "read_write" });
    const duplicate = await file.dup(),
      independent = await fs.open(path);
    try {
      const before = await file.fstat();
      const reads = await sequence(
        Array.from(
          { length: count },
          (_, i) => () => (i % 2 ? file : duplicate).read(width),
        ),
      );
      records(Uint8Array.from(reads.flatMap((value) => Array.from(value))));
      check((await file.lseek(0n, "current")) === BigInt(seed.length));
      check((await duplicate.lseek(0n, "current")) === BigInt(seed.length));
      equal(await independent.read(width), record(0));
      const positional = await batch(
        Array.from({ length: count }, (_, i) =>
          file.pread(width, BigInt(i * width)),
        ),
      );
      positional.forEach((bytes, i) => equal(bytes, record(i)));
      check((await duplicate.lseek(0n, "current")) === BigInt(seed.length));
      check((await file.fstat()).version === before.version);
      const root = await fs.opendir("/");
      try {
        const cookie = await root.telldir(),
          usage = await fs.statvfs();
        const values = await batch([
          fs.capabilities(),
          fs.stat(path),
          fs.lstat(path),
          fs.statvfs(),
          file.fstat(),
          duplicate.fstat(),
          fs.readFile(path),
          fs.readDir("/", 16),
          fs.fstatat(root, path),
          root.fstat(),
          root.telldir(),
        ]);
        check(values[0].positional_io && values[0].directories);
        for (const index of [1, 2, 4, 5, 8])
          check(
            values[index].inode === before.inode &&
              values[index].version === before.version,
          );
        for (const [field, value] of Object.entries(usage))
          check(values[3][field] === value);
        equal(values[6], seed);
        check(values[7].entries.length === 1 && values[9].kind === "Directory");
        check(values[10] === cookie && (await root.telldir()) === cookie);
      } finally {
        await root.closedir();
      }
      await file.lseek(0n);
      const writes = await sequence(
        Array.from(
          { length: count },
          (_, i) => () => (i % 2 ? file : duplicate).write(record(i)),
        ),
      );
      check(writes.every((value) => value === width));
      records(await file.pread(seed.length, 0n));
      check((await file.lseek(0n, "current")) === BigInt(seed.length));
      check((await duplicate.lseek(0n, "current")) === BigInt(seed.length));
      check((await independent.lseek(0n, "current")) === BigInt(width));
      await duplicate.close();
      equal(await file.read(1), new Uint8Array());
    } finally {
      await file.close();
      await independent.close();
    }
  } else if (kind === "append") {
    await fs.writeFile(path, new Uint8Array());
    await fs.truncate(path, 65520n);
    const first = await fs.open(path, { access: "read_write", append: true });
    const second = await fs.open(path, { access: "read_write", append: true });
    const copies = [first, await first.dup(), second, await second.dup()];
    try {
      await first.lseek(7n);
      await second.lseek(11n);
      const written = await sequence(
        Array.from(
          { length: count },
          (_, i) => () => copies[i % copies.length].write(record(i)),
        ),
      );
      check(written.every((value) => value === width));
      records(await first.pread(count * width, 65520n));
      const positions = await sequence(
        copies.map((file) => () => file.lseek(0n, "current")),
      );
      check(positions[0] === positions[1] && positions[2] === positions[3]);
      check(positions[0] !== positions[2]);
      const end = 65520n + BigInt(count * width);
      check(
        positions.includes(end) &&
          positions.every((position) => position > 65520n && position <= end),
      );
      const patched = await sequence(
        Array.from(
          { length: count },
          (_, i) => () =>
            copies[i % copies.length].pwrite(
              record(i + 32),
              65520n + BigInt(i * width),
            ),
        ),
      );
      check(patched.every((value) => value === width));
      const bytes = await first.pread(count * width, 65520n);
      for (let i = 0; i < count; i++)
        equal(bytes.slice(i * width, (i + 1) * width), record(i + 32));
      check((await first.fstat()).size === end);
      const pieces = await batch(
        Array.from({ length: count }, (_, i) =>
          copies[i % copies.length].pread(width, 65520n + BigInt(i * width)),
        ),
      );
      pieces.forEach((bytes, i) => equal(bytes, record(i + 32)));
      const after = await sequence(
        copies.map((file) => () => file.lseek(0n, "current")),
      );
      check(after.every((position, i) => position === positions[i]));
    } finally {
      await sequence(copies.map((file) => () => file.close()));
    }
  } else {
    check(kind === "directory");
    await fs.mkdir(path, 0o700);
    await sequence(
      Array.from(
        { length: count },
        (_, i) => () => fs.writeFile(`${path}/item-${i}`, record(i)),
      ),
    );
    const directory = await fs.opendir(path);
    try {
      const rows = await sequence(
        Array.from({ length: count + 2 }, () => () => directory.readdir()),
      );
      check(rows.every((row) => row !== null));
      const names = rows.map((row) => bytesToBase64(row.name)).sort();
      equal(
        names,
        [".", "..", ...Array.from({ length: count }, (_, i) => `item-${i}`)]
          .map((name) => bytesToBase64(utf8(name)))
          .sort(),
      );
      const cookie = await directory.telldir();
      const pages = await batch(
        Array.from({ length: count }, () => fs.readDir(path, count + 1)),
      );
      for (const page of pages) {
        check(page.next === null);
        equal(
          page.entries.map((row) => bytesToBase64(row.name)).sort(),
          names.filter(
            (name) =>
              name !== bytesToBase64(utf8(".")) &&
              name !== bytesToBase64(utf8("..")),
          ),
        );
      }
      const cookies = await batch(
        Array.from({ length: count }, () => directory.telldir()),
      );
      check(cookies.every((value) => value === cookie));
      check(
        (await directory.readdir()) === null &&
          (await directory.telldir()) === cookie,
      );
    } finally {
      await directory.closedir();
    }
  }
  const actual = await snapshot();
  await ctx.storage.apply({
    filesystem_checks: [
      {
        path,
        present: true,
        inode: actual.metadata.inode,
        version: actual.metadata.version,
      },
    ],
    puts: { [key]: actual },
  });
  await fs.sync();
  return { kind, recovered: false, count, ...actual };
}
