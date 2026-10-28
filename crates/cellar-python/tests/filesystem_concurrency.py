# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

async def filesystem_concurrency(ctx, action):
    import asyncio
    import base64
    import dataclasses
    import sys
    import sysconfig

    fs, count, width = ctx.fs, 12, 32
    kind, recovered = action.split("-")[1], action.endswith("-recovered")
    path = key = f"concurrent-{kind}"
    path = "/" + path
    free_threaded = bool(sysconfig.get_config_var("Py_GIL_DISABLED"))
    assert sys._is_gil_enabled() == (not free_threaded)

    def record(index):
        return bytes([index + 1]) * width

    async def batch(calls):
        values = await asyncio.gather(*calls, return_exceptions=True)
        for value in values:
            if isinstance(value, BaseException):
                raise value
        return values

    def records(data, first=0):
        assert len(data) == count * width
        ids = []
        for offset in range(0, len(data), width):
            value = data[offset]
            assert data[offset:offset + width] == record(value - 1)
            ids.append(value)
        assert sorted(ids) == list(range(first + 1, first + count + 1))

    async def snapshot():
        stat = await fs.stat(path)
        metadata = {key: str(value) for key, value in dataclasses.asdict(stat).items()}
        if kind == "directory":
            entries = (await fs.read_dir(path, count + 1)).entries
            assert len(entries) == count
            names = sorted(base64.b64encode(entry.name).decode() for entry in entries)
            assert names == sorted(base64.b64encode(f"item-{i}".encode()).decode() for i in range(count))
            return dict(metadata=metadata, names=names)
        async with await fs.open(path) as file:
            start = 65520 if kind == "append" else 0
            data = await file.pread(count * width, start)
            records(data, 32 if start else 0)
            if start:
                assert await file.pread(32, 0) == bytes(32)
                assert await file.pread(32, start - 32) == bytes(32)
        return dict(metadata=metadata, bytes_base64=base64.b64encode(data).decode())

    if recovered:
        saved = await ctx.storage.read_json(key)
        assert saved.present
        actual = await snapshot()
        assert actual == saved.value
        return dict(kind=kind, recovered=True, count=count, **actual)
    if kind == "shared":
        seed = b"".join(record(i) for i in range(count))
        await fs.write_file(path, seed)
        async with await fs.open(path, access="read_write") as file, await file.dup() as duplicate, await fs.open(path) as independent:
            before = await file.fstat()
            reads = [await (file if i % 2 else duplicate).read(width) for i in range(count)]
            records(b"".join(reads))
            assert await file.lseek(0, "current") == await duplicate.lseek(0, "current") == len(seed)
            assert await independent.read(width) == record(0)
            positional = await batch(file.pread(width, i * width) for i in range(count))
            assert positional == [record(i) for i in range(count)]
            assert await duplicate.lseek(0, "current") == len(seed)
            assert (await file.fstat()).version == before.version
            async with await fs.opendir("/") as root:
                cookie, usage = await root.telldir(), await fs.statvfs()
                values = await batch([
                    fs.capabilities(), fs.stat(path), fs.lstat(path), fs.statvfs(),
                    file.fstat(), duplicate.fstat(), fs.read_file(path), fs.read_dir("/", 16),
                    fs.fstatat(root, path), root.fstat(), root.telldir(),
                ])
                assert values[0]["positional_io"] and values[0]["directories"]
                for index in (1, 2, 4, 5, 8):
                    assert (values[index].inode, values[index].version) == (before.inode, before.version)
                assert values[3] == usage and values[6] == seed
                assert len(values[7].entries) == 1 and values[9].kind == "Directory"
                assert values[10] == cookie == await root.telldir()
            await file.lseek(0)
            writes = [await (file if i % 2 else duplicate).write(record(i)) for i in range(count)]
            assert writes == [width] * count
            records(await file.pread(len(seed), 0))
            assert await file.lseek(0, "current") == await duplicate.lseek(0, "current") == len(seed)
            assert await independent.lseek(0, "current") == width
        # A separate two-descriptor scope checks that closing one duplicate
        # leaves the other open description usable without a second close.
        async with await fs.open(path) as file:
            duplicate = await file.dup()
            await duplicate.close()
            assert await file.read(width) == (await fs.read_file(path))[:width]
    elif kind == "append":
        await fs.write_file(path, b"")
        await fs.truncate(path, 65520)
        async with await fs.open(path, access="read_write", append=True) as first, await first.dup() as first_copy, await fs.open(path, access="read_write", append=True) as second, await second.dup() as second_copy:
            files = [first, first_copy, second, second_copy]
            await first.lseek(7)
            await second.lseek(11)
            written = [await files[i % len(files)].write(record(i)) for i in range(count)]
            assert written == [width] * count
            records(await first.pread(count * width, 65520))
            positions = [await file.lseek(0, "current") for file in files]
            assert positions[0] == positions[1] and positions[2] == positions[3]
            assert positions[0] != positions[2]
            end = 65520 + count * width
            assert end in positions and all(65520 < position <= end for position in positions)
            patched = [await files[i % len(files)].pwrite(record(i + 32), 65520 + i * width) for i in range(count)]
            assert patched == [width] * count
            assert await first.pread(count * width, 65520) == b"".join(record(i + 32) for i in range(count))
            assert (await first.fstat()).size == end
            pieces = await batch(files[i % len(files)].pread(width, 65520 + i * width) for i in range(count))
            assert pieces == [record(i + 32) for i in range(count)]
            assert [await file.lseek(0, "current") for file in files] == positions
    else:
        assert kind == "directory"
        await fs.mkdir(path, 0o700)
        for i in range(count):
            await fs.write_file(f"{path}/item-{i}", record(i))
        async with await fs.opendir(path) as directory:
            rows = [await directory.readdir() for _ in range(count + 2)]
            assert all(row is not None for row in rows)
            assert sorted(row.name for row in rows) == sorted([b".", b"..", *(f"item-{i}".encode() for i in range(count))])
            cookie = await directory.telldir()
            pages = await batch(fs.read_dir(path, count + 1) for _ in range(count))
            for page in pages:
                assert page.next is None
                assert sorted(entry.name for entry in page.entries) == sorted(row.name for row in rows if row.name not in (b".", b".."))
            assert await batch(directory.telldir() for _ in range(count)) == [cookie] * count
            assert await directory.readdir() is None and await directory.telldir() == cookie
    actual = await snapshot()
    await ctx.storage.apply(
        filesystem_checks=[dict(path=path, present=True, inode=actual["metadata"]["inode"], version=actual["metadata"]["version"])],
        puts={key: actual},
    )
    await fs.sync()
    assert sys._is_gil_enabled() == (not free_threaded)
    return dict(kind=kind, recovered=False, count=count, **actual)
