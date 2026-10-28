# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.
import base64
import asyncio
import sys
import sysconfig
from types import SimpleNamespace
from cellar import ContextMismatch, File, Filesystem, Overloaded, Request, Response, SDKError
from cellar_filesystem_tools import FilesystemTools

async def refuses(call, code, errno=None):
    try:
        await call()
    except SDKError as error:
        assert error.code == code, (error.code, code)
        assert errno is None or error.errno == errno, (error.errno, errno)
        return error
    raise AssertionError("filesystem operation unexpectedly succeeded")

async def helper_preflight_outcomes():
    # SDK/protocol boundary: inject a metadata failure before any byte call.
    for outcome in ("not_committed", "unknown"):
        cause = RuntimeError("injected dispatch failure")
        original = Overloaded("injected dispatch refusal", outcome=outcome)
        original.errno = "EIO"
        original.completed_bytes = 7 if outcome == "unknown" else 0

        class RefusingContext:
            async def _call(self, operation, **value):
                assert operation == "filesystem.stat" and value == {"path": "/"}
                raise original from cause

        dispatch = Filesystem(RefusingContext())
        error = await refuses(lambda: dispatch.stat("/"), "Overloaded", "EIO")
        assert error is original and error.__cause__ is cause
        assert (error.operation, error.outcome, error.completed_bytes) == ("filesystem.stat", outcome, 7 if outcome == "unknown" else 0)

        class RefusingCapabilities:
            calls = 0

            async def capabilities(self):
                self.calls += 1
                error = Overloaded("injected capability refusal", outcome=outcome)
                error.errno = "EIO"
                error.operation = "filesystem.capabilities"
                error.completed_bytes = 0
                raise error

            async def _call(self, *_args, **_kwargs):
                raise AssertionError("preflight refusal submitted byte I/O")

        fault = RefusingCapabilities()
        file = File(fault, "protocol-only")
        for call, operation in (
            (lambda: file.read_exact(0), "read_exact"),
            (lambda: file.read_exact(1), "read_exact"),
            (lambda: file.write_all(b""), "write_all"),
            (lambda: file.write_all(b"x"), "write_all"),
        ):
            error = await refuses(call, "Overloaded", "EIO")
            assert isinstance(error, Overloaded)
            assert (error.operation, error.outcome, error.completed_bytes) == (f"filesystem.{operation}", outcome, 0)
        assert fault.calls == 4

async def readonly_profile(fs, ctx):
    assert (await fs.capabilities())["read_only"]
    expected = await ctx.storage.get_json("readonly_expected")
    assert expected is not None

    async def unchanged():
        for key, path in (("file", "/kept"), ("directory", "/directory"), ("root", "/")):
            stat = await fs.stat(path)
            for field, value in expected[key].items():
                assert str(getattr(stat, field)) == str(value), (key, field)
        usage = await fs.statvfs()
        for key, value in expected["usage"].items():
            assert str(usage[key]) == str(value), key

    await unchanged()
    async with await fs.open("/kept", create=True) as file, await fs.opendir("/") as root, await fs.opendir("/directory") as directory:
        copy = await file.dup()
        assert await copy.read_exact(2) == b"or" and await file.lseek(0, "current") == 2
        await copy.close()
        assert await file.pread(8, 0) == b"original"
        async with await fs.openat(root, "kept", create=True):
            pass
        await refuses(lambda: fs.open("/kept", create_new=True), "Filesystem", "EEXIST")
        await refuses(lambda: fs.open("/missing"), "Filesystem", "ENOENT")
        await refuses(lambda: fs.open("/kept", truncate=True), "Filesystem", "EINVAL")
        version = await file.fstat()
        for call, operation in (
            (lambda: fs.mkdir("/forbidden", 0o700), "mkdir"), (lambda: fs.mkdirat(directory, "forbidden", 0o700), "mkdirat"),
            (lambda: fs.rmdir("/directory"), "rmdir"), (lambda: fs.unlink("/kept"), "unlink"),
            (lambda: fs.unlinkat(root, "kept"), "unlinkat"), (lambda: fs.rename("/kept", "/renamed"), "rename"),
            (lambda: fs.renameat(root, "kept", directory, "renamed"), "renameat"), (lambda: fs.truncate("/kept", 0), "truncate"),
            (lambda: fs.chmod("/kept", 0o400), "chmod"), (lambda: fs.utimens("/kept", 0), "utimens"),
            (lambda: fs.write_file("/kept", b"bad"), "write_file"), (lambda: fs.replace_file("/kept", b"bad"), "replace_file"),
            (lambda: fs.edit_file("/kept", version, offset=0, delete_bytes=1, insert=b"X"), "edit_file"),
            (lambda: fs.open("/kept", access="read_write"), "open"), (lambda: fs.open("/kept", access="write", truncate=True), "open"),
            (lambda: fs.open("/missing", create=True), "open"), (lambda: fs.openat(root, "missing", create_new=True), "openat"),
            (lambda: fs.creat("/missing", 0o600), "creat"), (lambda: file.fchmod(0o400), "fchmod"), (fs.collect_garbage, "collect_garbage"),
        ):
            error = await refuses(call, "Filesystem", "EROFS")
            assert (error.operation, error.outcome, error.completed_bytes) == (f"filesystem.{operation}", "not_committed", 0)
            await unchanged()
        await refuses(lambda: file.write(b"bad"), "Filesystem", "EBADF")
        await refuses(lambda: file.pwrite(b"bad", 0), "Filesystem", "EBADF")
        await refuses(lambda: file.ftruncate(0), "Filesystem", "EBADF")
        await fs.utimens("/kept", None)
        await file.fsync()
        await file.fdatasync()
        await directory.fsync()
    failure = await refuses(lambda: ctx.storage.apply(puts={"readonly_must_rollback": True}, filesystem_mutations=[dict(op="write_file", path="/kept", data_base64=base64.b64encode(b"bad").decode())]), "Filesystem", "EROFS")
    assert failure.outcome == "not_committed"
    assert not (await ctx.storage.read_json("readonly_must_rollback")).present
    await ctx.storage.apply(filesystem_checks=[dict(path="/kept", present=True, inode=expected["file"]["inode"], version=expected["file"]["version"])], puts={"readonly_business_state": True})
    assert await ctx.storage.get_json("readonly_business_state") is True
    await unchanged()
    await fs.sync()

async def host_admission_profile(fs):
    assert (await fs.capabilities())["limits"]["max_host_handles"] == 4
    await fs.write_file("/host-admission", b"original")
    async with await fs.open("/host-admission", access="read_write") as file, await file.dup() as duplicate, await fs.opendir("/") as directory:
        fourth = await fs.open("/host-admission")
        before = await file.fstat(), await fs.statvfs()
        for call, operation in (
            (file.dup, "dup"), (lambda: fs.opendir("/"), "opendir"),
            (lambda: fs.open("/host-refused", access="write", create_new=True), "open"),
            (lambda: fs.open("/host-admission", access="write", truncate=True), "open"),
        ):
            error = await refuses(call, "Filesystem", "ENFILE")
            assert (error.operation, error.outcome, error.completed_bytes) == (f"filesystem.{operation}", "not_committed", 0)
            assert (await file.fstat(), await fs.statvfs()) == before
        assert await fs.read_file("/host-admission") == b"original"
        assert await file.lseek(0, "current") == 0
        await refuses(lambda: fs.stat("/host-refused"), "Filesystem", "ENOENT")
        await fourth.close()
        async with await fs.open("/host-refused", access="write", create_new=True):
            pass
    await fs.unlink("/host-refused")
    await fs.unlink("/host-admission")
    while await fs.collect_garbage() > 0:
        pass

async def reserved_namespace_profile(fs, group):
    while await fs.collect_garbage() > 0:
        pass
    await fs.mkdir("/reserved", 0o700)
    await fs.mkdir("/reserved/child", 0o700)
    await fs.write_file("/reserved/file", b"kept")
    async with await fs.opendir("/reserved") as parent, await fs.opendir("/reserved/child") as child:
        async def snapshot():
            return [await fs.stat(path) for path in ("/", "/reserved", "/reserved/child", "/reserved/file")], await fs.statvfs()

        before = await snapshot()
        cases = [value for value in FILESYSTEM_RESERVED_CASES["cases"] if value["group"] == group]
        assert len(FILESYSTEM_RESERVED_CASES["cases"]) == 42 and cases
        for value in cases:
            base = child if value.get("base") == "child" else parent
            op, path = value["operation"], value["path"]
            if op == "mkdir":
                call = lambda: fs.mkdir(path, 0o700)
            elif op == "rmdir":
                call = lambda: fs.rmdir(path)
            elif op == "unlink":
                call = lambda: fs.unlink(path)
            elif op == "mkdirat":
                call = lambda: fs.mkdirat(base, path, 0o700)
            elif op == "unlinkat":
                call = lambda: fs.unlinkat(base, path, value.get("remove_directory", False))
            else:
                raise AssertionError(f"unknown reserved-path operation {op}")
            error = await refuses(call, "Filesystem", value["errno"])
            assert (error.operation, error.outcome, error.completed_bytes) == (f"filesystem.{op}", "not_committed", 0), value["id"]
            assert await snapshot() == before, value["id"]
        assert await fs.read_file("/reserved/file") == b"kept"
        if group == "rmdir":
            assert (await child.readdir()).name == b"."
            cookie = await child.telldir()
            await fs.rmdir("/reserved/child")
            assert (await child.fstat()).nlink == 0
            assert await child.readdir() is None and await child.telldir() == cookie
            await child.rewinddir()
            assert not await child.read_page(3)
            await child.seekdir(cookie)
            assert await child.readdir() is None
            await refuses(lambda: fs.mkdirat(child, "refused", 0o700), "Filesystem", "ENOENT")
    await fs.unlink("/reserved/file")
    if group != "rmdir":
        await fs.rmdir("/reserved/child")
    await fs.rmdir("/reserved")
    while await fs.collect_garbage() > 0:
        pass

async def namespace_profile(fs, ctx):
    while await fs.collect_garbage() > 0:
        pass
    await fs.mkdir("/namespace", 0o700)
    for name in ("source", "empty", "nonempty"):
        await fs.mkdir(f"/namespace/{name}", 0o700)
    await fs.mkdir("/namespace/nonempty/child", 0o700)
    await fs.write_file("/namespace/file", b"source")
    await fs.write_file("/namespace/other", b"victim")
    paths = ("/", "/namespace", "/namespace/file", "/namespace/other", "/namespace/source", "/namespace/empty", "/namespace/nonempty", "/namespace/nonempty/child")

    async def snapshot():
        return [await fs.stat(path) for path in paths], await fs.statvfs()

    before = await snapshot()
    assert await fs.stat("/../../namespace//./file") == await fs.stat("namespace/file")

    async def refusal(call, operation, errno):
        error = await refuses(call, "Filesystem", errno)
        assert (error.operation, error.outcome, error.completed_bytes) == (f"filesystem.{operation}", "not_committed", 0)
        assert await snapshot() == before

    for path, errno in (
        ("", "ENOENT"), ("/namespace/fi\0le", "EINVAL"),
        ("/namespace/" + "x" * 256, "ENAMETOOLONG"),
        ("/x" * 65, "ENAMETOOLONG"), ("/x" * 2050, "ENAMETOOLONG"),
        ("/namespace/file/child", "ENOTDIR"), ("/namespace/file/", "ENOTDIR"),
        ("/namespace/missing", "ENOENT"),
    ):
        await refusal(lambda: fs.stat(path), "stat", errno)
    await refusal(lambda: fs.open("/namespace/file", access="write", create_new=True), "open", "EEXIST")
    await refusal(lambda: fs.open("/namespace"), "open", "EISDIR")
    version = await fs.stat("/namespace/file")
    for offset in (0, 3, 6):
        after = await fs.edit_file("/namespace/file", version, offset=offset, delete_bytes=0, insert=b"")
        assert (after.inode, after.version) == (version.inode, version.version)
        assert await snapshot() == before
        assert await fs.read_file("/namespace/file") == b"source"
    for path in ("/", "/namespace/source", "/namespace/nonempty"):
        version = await fs.stat(path)
        for insert in (b"", b"x"):
            await refusal(lambda: fs.edit_file(path, version, offset=0, delete_bytes=0, insert=insert), "edit_file", "EISDIR")
            error = await refuses(lambda: ctx.storage.apply(
                puts={"directory-edit-must-rollback": True},
                filesystem_mutations=[
                    dict(op="write_file", path="/namespace/file", data_base64=base64.b64encode(b"changed").decode("ascii")),
                    dict(op="edit_file", path=path, inode=str(version.inode), version=str(version.version), offset="0", delete_bytes="0", data_base64=base64.b64encode(insert).decode("ascii")),
                ],
            ), "Filesystem", "EISDIR")
            assert (error.operation, error.outcome, error.completed_bytes) == ("storage.apply", "not_committed", 0)
            assert not (await ctx.storage.read_json("directory-edit-must-rollback")).present
            assert await snapshot() == before
            assert await fs.read_file("/namespace/file") == b"source"
    directory = await fs.stat("/namespace/source")
    directory_request = dict(operation="edit", path="/namespace/source", inode=str(directory.inode), version=str(directory.version), offset=0, delete_bytes=0, insert="")
    await ctx.storage.put_json("directory-edit-request", directory_request)
    directory_tools = FilesystemTools(ctx, prefix="directory-edit-tools:")
    directory_refusal = await directory_tools.execute("empty-directory-edit", directory_request)
    assert not directory_refusal["replayed"]
    assert (directory_refusal["result"]["error"]["errno"], directory_refusal["result"]["error"]["outcome"]) == ("EISDIR", "not_committed")
    assert await snapshot() == before
    await refusal(lambda: fs.mkdir("/namespace/file", 0o700), "mkdir", "EEXIST")
    await refusal(lambda: fs.unlink("/namespace/source"), "unlink", "EPERM")
    await refusal(lambda: fs.rmdir("/namespace/file"), "rmdir", "ENOTDIR")
    await refusal(lambda: fs.rmdir("/namespace/nonempty"), "rmdir", "ENOTEMPTY")
    await refusal(lambda: fs.rmdir("/"), "rmdir", "EBUSY")
    for old, path, errno in (
        ("/namespace/file", "/namespace/file/", "ENOTDIR"),
        ("/namespace/file/", "/namespace/file", "ENOTDIR"),
        ("/namespace/file", "/namespace/missing/", "ENOTDIR"),
        ("/namespace/file", "/namespace/empty", "EISDIR"),
        ("/namespace/source", "/namespace/file", "ENOTDIR"),
        ("/namespace/source", "/namespace/nonempty", "ENOTEMPTY"),
        ("/namespace/nonempty", "/namespace/nonempty/child/moved", "EINVAL"),
        ("/namespace/source/.", "/namespace/empty", "EINVAL"),
    ):
        await refusal(lambda: fs.rename(old, path), "rename", errno)
    await fs.rename("/namespace/file", "/namespace/file")
    await fs.rename("/namespace/source", "/namespace/source/")
    assert await snapshot() == before
    assert await fs.read_file("/namespace/file") == b"source"
    assert await fs.read_file("/namespace/other") == b"victim"

    async with await fs.opendir("/namespace/source") as source, await fs.opendir("/namespace/empty") as displaced:
        parent_links = (await fs.stat("/namespace")).nlink
        await fs.rename("/namespace/source", "/namespace/empty/")
        assert (await fs.stat("/namespace/empty")).inode == (await source.fstat()).inode
        assert (await displaced.fstat()).nlink == 0
        assert await displaced.readdir() is None
        assert (await fs.stat("/namespace")).nlink == parent_links - 1
        await refuses(lambda: fs.stat("/namespace/source"), "Filesystem", "ENOENT")
        await refuses(lambda: fs.mkdirat(displaced, "refused", 0o700), "Filesystem", "ENOENT")
    async with await fs.open("/namespace/file") as file, await fs.open("/namespace/other") as victim:
        await fs.rename("/namespace/file", "/namespace/other")
        assert (await fs.stat("/namespace/other")).inode == (await file.fstat()).inode
        assert (await victim.fstat()).nlink == 0
        await fs.unlink("/namespace/other")
        await fs.collect_garbage()
        for handle, contents in ((file, b"source"), (victim, b"victim")):
            assert await handle.pread(6, 0) == contents
            assert (await handle.fstat()).nlink == 0
    for path in ("/namespace/nonempty/child", "/namespace/nonempty", "/namespace/empty", "/namespace"):
        await fs.rmdir(path)
    while await fs.collect_garbage() > 0:
        pass

    replay = await directory_tools.execute("empty-directory-edit", directory_request)
    assert replay["replayed"] and replay["result"] == directory_refusal["result"]

async def wire_profile(fs, ctx, group):
    while await fs.collect_garbage() > 0:
        pass
    path = "/wire-profile/file"
    await fs.mkdir("/wire-profile", 0o700)
    await fs.write_file(path, b"original")
    file = await fs.open(path, access="read_write")
    directory = await fs.opendir("/wire-profile")
    version = await file.fstat()
    edit = dict(offset=0, delete_bytes=0, insert=b"x")

    async def snapshot():
        return await fs.stat("/"), await file.fstat(), await directory.fstat(), await fs.statvfs(), await file.lseek(0, "current"), await directory.telldir(), await file.pread(8, 0)

    before = await snapshot()
    refused = 0

    async def refusal(call, operation, errno="EINVAL"):
        nonlocal refused
        error = await refuses(call, "ValidationError", errno)
        assert (error.operation, error.outcome, error.completed_bytes) == (operation, "not_committed", 0), (group, refused, operation)
        assert await snapshot() == before
        refused += 1

    try:
        if group == "unsigned":
            for value in ("", "+1", "-1", "1.5", " 1", "1e3", "１２", "1x", "18446744073709551616"):
                for call, operation in (
                    (lambda: file.pread(1, value), "pread"), (lambda: file.pwrite(b"x", value), "pwrite"),
                    (lambda: file.lseek(value), "lseek"), (lambda: file.ftruncate(value), "ftruncate"),
                    (lambda: fs.truncate(path, value), "truncate"),
                    (lambda: fs.edit_file(path, SimpleNamespace(inode=value, version=version.version), **edit), "edit_file"),
                    (lambda: fs.edit_file(path, SimpleNamespace(inode=version.inode, version=value), **edit), "edit_file"),
                    (lambda: fs.edit_file(path, version, **(edit | dict(offset=value))), "edit_file"),
                    (lambda: fs.edit_file(path, version, **(edit | dict(delete_bytes=value))), "edit_file"),
                ):
                    await refusal(call, "filesystem." + operation, "EOVERFLOW" if value == "18446744073709551616" else "EINVAL")
            assert refused == 81
        elif group == "signed":
            for value in ("", "+1", "1.5", " 1", "1e3", "--1", "１２", "-", "9223372036854775808", "-9223372036854775809"):
                for call, operation in (
                    (lambda: file.lseek(value, "current"), "lseek"), (lambda: file.lseek(value, "end"), "lseek"),
                    (lambda: fs.utimens(path, value), "utimens"),
                ):
                    await refusal(call, "filesystem." + operation, "EOVERFLOW" if value.endswith(("5808", "5809")) else "EINVAL")
            assert refused == 30
        elif group == "options":
            for call, operation in (
                (lambda: fs.open(path, access="invalid", truncate=True), "open"),
                (lambda: fs.open("/wire-profile/missing", access="invalid", create=True), "open"),
                (lambda: fs.openat(directory, "file", access="invalid", truncate=True), "openat"),
                (lambda: fs.open(path, access="write", truncate="yes"), "open"),
                (lambda: fs.open(path, create="yes"), "open"), (lambda: fs.open(path, append="yes"), "open"),
                (lambda: fs.open(path, mode=-1), "open"), (lambda: fs.open(path, sync=True), "open"),
                (lambda: fs.open(path, mode=float("nan")), "open"), (lambda: fs.creat(path, 0.5), "creat"),
                (lambda: fs.mkdir("/wire-profile/missing", 0.5), "mkdir"),
                (lambda: fs.mkdirat(directory, "missing", 0.5), "mkdirat"),
                (lambda: fs.chmod(path, 0.5), "chmod"), (lambda: file.fchmod(0.5), "fchmod"),
                (lambda: file.read(-1), "read"), (lambda: file.pread(0.5, 0), "pread"),
                (lambda: file.lseek(0, "invalid"), "lseek"), (lambda: directory.read_page(0.5), "read_page"),
                (lambda: fs.read_file(path, -1), "read_file"), (lambda: fs.read_dir("/wire-profile", 0.5), "read_dir"),
                (lambda: fs.fstatat(directory, "file", "yes"), "fstatat"),
                (lambda: fs.unlinkat(directory, "file", "yes"), "unlinkat"),
                (lambda: directory.seekdir("%%%"), "seekdir"), (lambda: directory.seekdir(""), "seekdir"),
                (lambda: fs.read_dir("/wire-profile", 1, {}), "read_dir"),
                (lambda: file.read_exact(-1), "read_exact"),
            ):
                await refusal(call, "filesystem." + operation)
            assert refused == 26
        else:
            assert group == "batch"

            async def batch(changes, errno):
                value = dict(puts={"wire_must_not_commit": True}, filesystem_mutations=[dict(op="write_file", path=path, data_base64="eA==")]) | changes
                await refusal(lambda: ctx.storage.apply(**value), "storage.apply", errno)
                assert not (await ctx.storage.read_json("wire_must_not_commit")).present

            for value in ("", "+1", "-1", "18446744073709551616"):
                await batch(dict(filesystem_mutations=[dict(op="write_file", path=path, data_base64="eA=="), dict(op="truncate", path=path, size=value)]), "EOVERFLOW" if value == "18446744073709551616" else "EINVAL")
            for field in ("inode", "version", "offset", "delete_bytes"):
                for value in ("+1", "18446744073709551616"):
                    mutation = dict(op="edit_file", path=path, inode=str(version.inode), version=str(version.version), offset="0", delete_bytes="0", data_base64="eA==") | {field: value}
                    await batch(dict(filesystem_mutations=[dict(op="write_file", path=path, data_base64="eA=="), mutation]), "EINVAL" if value == "+1" else "EOVERFLOW")
            for field in ("inode", "version"):
                for value in ("+" + str(getattr(version, field)), "18446744073709551616"):
                    check = dict(path=path, present=True, inode=str(version.inode), version=str(version.version)) | {field: value}
                    await batch(dict(filesystem_checks=[check]), "EINVAL" if value.startswith("+") else "EOVERFLOW")
            for op in ("write_file", "replace_file", "edit_file"):
                mutation = dict(op=op, path=path, data_base64="%%%")
                if op == "edit_file":
                    mutation |= dict(inode=str(version.inode), version=str(version.version), offset="0", delete_bytes="0")
                await batch(dict(filesystem_mutations=[mutation]), "EINVAL")
            await batch(dict(filesystem_checks=[dict(path={"bytes_base64": "%%%"}, present=False)]), "EINVAL")
            await batch(dict(filesystem_checks=[dict(path=path, present=False, inode=str(version.inode), version=str(version.version))]), "EINVAL")
            assert refused == 21
    finally:
        await file.close()
        await directory.closedir()
    await fs.unlink(path)
    await fs.rmdir("/wire-profile")
    while await fs.collect_garbage() > 0:
        pass

async def argument_profile(fs):
    while await fs.collect_garbage() > 0:
        pass
    path = "/argument-file"
    await fs.write_file(path, b"original")
    await fs.mkdir("/argument-directory", 0o700)
    file = await fs.open(path, access="read_write")
    directory = await fs.opendir("/argument-directory")
    version = await file.fstat()
    edit = dict(offset=0, delete_bytes=0, insert=b"x")

    async def snapshot():
        return await fs.stat("/"), await file.fstat(), await directory.fstat(), await fs.statvfs(), await file.lseek(0, "current"), await directory.telldir()

    before = await snapshot()
    released = memoryview(b"/argument-file")
    released.release()
    noncontiguous = memoryview(b"not-contiguous")[::2]
    cases = (
        (lambda: fs.open({}, access="write", create=True, truncate=True), "open"),
        (lambda: fs.creat({}), "creat"), (lambda: fs.openat(directory, {}), "openat"),
        (lambda: fs.stat({}), "stat"), (lambda: fs.lstat({}), "lstat"),
        (lambda: fs.fstatat(directory, {}), "fstatat"),
        (lambda: fs.truncate({}, 0), "truncate"), (lambda: fs.mkdir({}), "mkdir"),
        (lambda: fs.mkdirat(directory, {}), "mkdirat"), (lambda: fs.rmdir({}), "rmdir"),
        (lambda: fs.unlink({}), "unlink"), (lambda: fs.unlinkat(directory, {}), "unlinkat"),
        (lambda: fs.rename({}, path), "rename"), (lambda: fs.rename(path, {}), "rename"),
        (lambda: fs.renameat(directory, {}, directory, "target"), "renameat"),
        (lambda: fs.renameat(directory, "source", directory, {}), "renameat"),
        (lambda: fs.chmod({}, 0o600), "chmod"), (lambda: fs.utimens({}, 0), "utimens"),
        (lambda: fs.opendir({}), "opendir"), (lambda: fs.read_file({}), "read_file"),
        (lambda: fs.write_file({}, b"bad"), "write_file"),
        (lambda: fs.replace_file({}, b"bad"), "replace_file"),
        (lambda: fs.edit_file({}, version, **edit), "edit_file"), (lambda: fs.read_dir({}), "read_dir"),
        (lambda: file.pread(1, 0.5), "pread"), (lambda: file.pwrite(b"bad", 0.5), "pwrite"),
        (lambda: file.lseek(0.5), "lseek"), (lambda: file.ftruncate(0.5), "ftruncate"),
        (lambda: fs.truncate(path, 0.5), "truncate"), (lambda: fs.utimens(path, 0.5), "utimens"),
        (lambda: fs.edit_file(path, version, **(edit | dict(offset=0.5))), "edit_file"),
        (lambda: fs.edit_file(path, version, **(edit | dict(delete_bytes=0.5))), "edit_file"),
        (lambda: fs.edit_file(path, __import__("types").SimpleNamespace(inode=0.5, version=version.version), **edit), "edit_file"),
        (lambda: fs.edit_file(path, __import__("types").SimpleNamespace(inode=version.inode, version=0.5), **edit), "edit_file"),
        (lambda: file.write({}), "write"), (lambda: file.pwrite({}, 0), "pwrite"),
        (lambda: file.write_all({}), "write_all"), (lambda: fs.write_file(path, {}), "write_file"),
        (lambda: fs.replace_file(path, {}), "replace_file"),
        (lambda: fs.edit_file(path, version, **(edit | dict(insert={}))), "edit_file"),
        (lambda: fs.openat(None, path), "openat"), (lambda: fs.fstatat(None, path), "fstatat"),
        (lambda: fs.mkdirat(None, "new"), "mkdirat"), (lambda: fs.unlinkat(None, path), "unlinkat"),
        (lambda: fs.renameat(None, path, directory, "target"), "renameat"),
        (lambda: fs.renameat(directory, "source", None, path), "renameat"),
        (lambda: fs.edit_file(path, None, **edit), "edit_file"),
        (lambda: fs.edit_file(path, {}, **edit), "edit_file"),
        (lambda: fs.stat(released), "stat"),
        (lambda: file.write(noncontiguous), "write"),
        (lambda: file.write_all(released), "write_all"),
    )
    assert len(cases) == 51
    try:
        for index, (call, operation) in enumerate(cases):
            error = await refuses(call, "ValidationError", "EINVAL")
            assert (error.operation, error.outcome, error.completed_bytes) == (f"filesystem.{operation}", "not_committed", 0)
            if index >= 48:
                assert isinstance(error.__cause__, (TypeError, ValueError, BufferError))
            assert await snapshot() == before
        assert await fs.read_file(path) == b"original"
        assert await file.pwrite(bytearray(b"o"), 0) == 1
        assert await file.pwrite(memoryview(b"riginal"), 1) == 7
        assert await file.pread(8, 0) == b"original"
    finally:
        await file.close()
        await directory.closedir()
    await fs.unlink(path)
    await fs.rmdir("/argument-directory")
    while await fs.collect_garbage() > 0:
        pass

live_authority = None

async def live_authority_owner(fs, ctx):
    global live_authority
    assert live_authority is None
    free_threaded = bool(sysconfig.get_config_var("Py_GIL_DISABLED"))
    assert sys._is_gil_enabled() == (not free_threaded)
    await fs.mkdir("/at-authority", 0o700)
    await fs.mkdir("/at-authority/empty", 0o700)
    await fs.write_file("/at-authority/source", b"owner-source")
    await fs.write_file("/at-authority/target", b"owner-target")
    directory = await fs.opendir("/at-authority")
    file = await fs.open("/owner-orphan", access="read_write", create_new=True)
    await file.write_all(b"pinned-owner")
    await file.lseek(0)
    await fs.unlink("/owner-orphan")

    async def snapshot():
        paths = ("/", "/at-authority", "/at-authority/empty", "/at-authority/source", "/at-authority/target")
        return [await fs.stat(path) for path in paths], await fs.statvfs(), await directory.telldir(), await file.fstat(), await file.lseek(0, "current")

    try:
        before = await snapshot()
        assert isinstance(ctx.identity.scope, str)
        live_authority = dict(directory=directory, file=file, fs=fs, cookie=await directory.telldir(), scope=ctx.identity.scope)
        # Keep the owner's invocation live across the nested caller request.
        result = await ctx.namespace("COUNTERS").get_by_name("authority-caller").fetch(
            Request("http://fixture/authority-caller/authority-live-caller"))
        assert result.status == 200, result.body
        assert result.body == b"ok"
        assert await snapshot() == before
        assert await fs.read_file("/at-authority/source") == b"owner-source"
        assert await fs.read_file("/at-authority/target") == b"owner-target"
        assert await file.pread(32, 0) == b"pinned-owner"
        assert sys._is_gil_enabled() == (not free_threaded)
    finally:
        live_authority = None
        await file.close()
        await directory.closedir()
    await fs.unlink("/at-authority/source")
    await fs.unlink("/at-authority/target")
    await fs.rmdir("/at-authority/empty")
    await fs.rmdir("/at-authority")
    while await fs.collect_garbage() > 0:
        pass

async def authority_profile(fs, kind, supplied):
    await fs.mkdir("/at-authority", 0o700)
    await fs.mkdir("/at-authority/empty", 0o700)
    await fs.write_file("/at-authority/source", b"source")
    await fs.write_file("/at-authority/target", b"target")
    local = await fs.opendir("/at-authority")
    if kind in ("expired", "live"):
        assert supplied is not None
        bad = supplied
    elif kind == "closed":
        bad = await fs.opendir("/")
        await bad.closedir()
    else:
        assert kind == "file"
        bad = await fs.open("/at-authority/source")
    paths = ("/", "/at-authority", "/at-authority/empty", "/at-authority/source", "/at-authority/target")

    async def snapshot():
        return [await fs.stat(path) for path in paths], await fs.statvfs(), await local.telldir()

    before = await snapshot()
    refused = 0

    async def refusal(call, operation, code="Filesystem", errno="EBADF"):
        nonlocal refused
        error = await refuses(call, code, errno)
        if code == "ContextMismatch":
            assert isinstance(error, ContextMismatch)
        actual = getattr(error, "operation", None), error.outcome, getattr(error, "completed_bytes", None)
        assert actual == (f"filesystem.{operation}", "not_committed", 0), (operation, actual)
        assert await snapshot() == before
        refused += 1

    if kind == "live":
        owner = live_authority
        for call, operation in (
            (lambda: owner["file"].read(1), "read"), (lambda: owner["file"].write(b"bad"), "write"),
            (lambda: owner["file"].pread(1, 0), "pread"), (lambda: owner["file"].pwrite(b"bad", 0), "pwrite"),
            (lambda: owner["file"].lseek(1), "lseek"), (owner["file"].fstat, "fstat"),
            (lambda: owner["file"].ftruncate(0), "ftruncate"), (lambda: owner["file"].fchmod(0o400), "fchmod"),
            (owner["file"].dup, "dup"), (owner["file"].close, "close"),
            (owner["file"].fsync, "fsync"), (owner["file"].fdatasync, "fdatasync"),
            (lambda: owner["file"].read_exact(0), "read_exact"), (lambda: owner["file"].write_all(b""), "write_all"),
            (owner["directory"].readdir, "readdir"), (lambda: owner["directory"].read_page(1), "read_page"),
            (owner["directory"].telldir, "telldir"), (lambda: owner["directory"].seekdir(owner["cookie"]), "seekdir"),
            (owner["directory"].rewinddir, "rewinddir"), (owner["directory"].closedir, "closedir"),
            (owner["directory"].fstat, "fstat"), (owner["directory"].fsync, "fsync"),
            (owner["fs"].capabilities, "capabilities"), (lambda: owner["fs"].stat("/at-authority/source"), "stat"),
            (lambda: owner["fs"].read_file("/at-authority/source"), "read_file"),
            (owner["fs"].sync, "sync"), (owner["fs"].collect_garbage, "collect_garbage"),
        ):
            await refusal(call, operation, "ContextMismatch", None)

    for prefix in ("at-authority/", "/at-authority/"):
        source, target, empty, created = (prefix + name for name in ("source", "target", "empty", "created"))
        await refusal(lambda: fs.openat(bad, created, access="write", create_new=True), "openat")
        await refusal(lambda: fs.openat(bad, source, access="write", truncate=True), "openat")
        for no_follow in (False, True):
            await refusal(lambda: fs.fstatat(bad, source, no_follow), "fstatat")
        await refusal(lambda: fs.mkdirat(bad, created, 0o700), "mkdirat")
        await refusal(lambda: fs.unlinkat(bad, source), "unlinkat")
        await refusal(lambda: fs.unlinkat(bad, empty, True), "unlinkat")
        await refusal(lambda: fs.renameat(bad, source, local, "target"), "renameat")
        await refusal(lambda: fs.renameat(local, "source", bad, target), "renameat")
    assert await fs.read_file("/at-authority/source") == b"source"
    assert await fs.read_file("/at-authority/target") == b"target"
    await refuses(lambda: fs.stat("/at-authority/created"), "Filesystem", "ENOENT")
    if kind == "file":
        await bad.close()
    await local.closedir()
    await fs.unlink("/at-authority/source")
    await fs.unlink("/at-authority/target")
    await fs.rmdir("/at-authority/empty")
    await fs.rmdir("/at-authority")
    while await fs.collect_garbage() > 0:
        pass
    return refused

class Counter:
    async def fetch(self, req, ctx):
        action = req.url.rsplit("/", 1)[-1]
        fs = ctx.fs
        if action == "exercise":
            assert not (await fs.capabilities())["symlinks"]
            await fs.mkdir("/dir", 0o700)
            await fs.write_file("/notes", b"one")
            async with await fs.open("/notes", access="read_write", append=True) as file:
                await file.pwrite(b"ONE", 0)
                await file.write_all(b"!")
                assert await file.pread(4, 0) == b"ONE!"
                await file.lseek(1)
                duplicate = await file.dup()
                assert await duplicate.read(2) == b"NE"
                assert await file.lseek(0, "current") == 3
                await duplicate.close()
                await file.fsync()
            await refuses(file.fstat, "Filesystem", "EBADF")
            await refuses(lambda: file.write_all(b""), "Filesystem", "EBADF")
            await refuses(lambda: file.read_exact(0), "Filesystem", "EBADF")
            await fs.write_file(b"/dir/\xff", b"binary")
            async with await fs.opendir("/dir") as directory:
                assert (await directory.readdir()).name == b"."
                assert (await directory.readdir()).name == b".."
                cookie = await directory.telldir()
                assert (await directory.readdir()).name == b"\xff"
                await directory.seekdir(cookie)
                assert (await directory.readdir()).name == b"\xff"
                await fs.rename("/dir", "/moved")
                async with await fs.openat(directory, b"\xff") as relative:
                    assert await relative.read_exact(6) == b"binary"
            await fs.write_file("/published", b"old")
            async with await fs.open("/published") as old:
                await fs.replace_file("/published", b"new")
                assert await old.read_exact(3) == b"old"
                before = await fs.stat("/published")
                assert (await old.fstat()).inode != before.inode
                after = await fs.edit_file("/published", before, offset=1, delete_bytes=1, insert=b"OO")
                assert after.inode == before.inode and after.version > before.version
                assert await fs.read_file("/published") == b"nOOw"
                await refuses(lambda: fs.edit_file("/published", before, offset=1, delete_bytes=1, insert=b"OO"), "Conflict")
                await refuses(lambda: fs.read_file("/published", 2), "Filesystem", "EFBIG")
            first = await fs.read_dir("/", 1)
            second = await fs.read_dir("/", 1, first.next)
            assert len(first.entries) == len(second.entries) == 1
            assert first.entries[0].name != second.entries[0].name
            stat = await fs.stat("/notes")
            await ctx.storage.apply(
                checks={"tool": __import__("cellar").MISSING},
                filesystem_checks=[dict(path="/notes", present=True, inode=str(stat.inode), version=str(stat.version))],
                filesystem_mutations=[dict(op="write_file", path="/artifact", data_base64=base64.b64encode(b"saved").decode())],
                puts={"tool": {"result": "saved", "source_version": str(stat.version)}})
            await refuses(lambda: ctx.storage.apply(filesystem_checks=[dict(path="/notes", present=True, inode=str(stat.inode), version="0")], filesystem_mutations=[dict(op="write_file", path="/must-not-exist", data_base64="eA==")], puts={"bad": True}), "Conflict")
            await refuses(lambda: fs.stat("/must-not-exist"), "Filesystem", "ENOENT")
            assert not (await ctx.storage.read_json("bad")).present
            self.old = await fs.open("/notes")
            await fs.sync()
        elif action == "readonly":
            await readonly_profile(fs, ctx)
        elif action == "host-admission":
            await host_admission_profile(fs)
        elif action == "authority-capture":
            self.old_directory = await fs.opendir("/")
        elif action == "authority-live":
            await live_authority_owner(fs, ctx)
        elif action == "authority-live-caller":
            assert live_authority is not None
            assert isinstance(ctx.identity.scope, str) and ctx.identity.scope != live_authority["scope"]
            assert await authority_profile(fs, "live", live_authority["directory"]) == 45
        elif action.startswith("authority-"):
            assert await authority_profile(fs, action[len("authority-"):], getattr(self, "old_directory", None)) == 18
            if action == "authority-expired":
                self.old_directory = None
        elif action.startswith("reserved-"):
            await reserved_namespace_profile(fs, action[len("reserved-"):])
        elif action == "namespace":
            await namespace_profile(fs, ctx)
        elif action == "arguments":
            await argument_profile(fs)
        elif action.startswith("wire-"):
            await wire_profile(fs, ctx, action[len("wire-"):])
        elif action == "metadata":
            await fs.write_file("/metadata", b"original")
            initial = await fs.stat("/metadata")
            assert (initial.mode, initial.uid, initial.gid, initial.atime_ns) == (0o644, 1000, 1000, 0)
            await refuses(lambda: fs.open("/metadata", truncate=True), "Filesystem", "EINVAL")
            await refuses(lambda: fs.open("/metadata", access="write", create_new=True, truncate=True), "Filesystem", "EEXIST")
            await refuses(lambda: fs.open("/mode-refused", access="write", create=True, mode=0o1600), "Filesystem", "EOPNOTSUPP")
            await refuses(lambda: fs.open("/mode-refused", access="write", create=True, sync=True), "ValidationError")
            await refuses(lambda: fs.stat("/mode-refused"), "Filesystem", "ENOENT")
            assert await fs.read_file("/metadata") == b"original"
            assert (await fs.stat("/metadata")).version == initial.version
            async with await fs.open("/metadata", access="read_write") as file:
                helper_limit = (await fs.capabilities())["limits"]["max_file_helper_bytes"]

                async def helper_snapshot():
                    return await file.fstat(), await fs.statvfs(), await file.lseek(0, "current")

                helper_before = await helper_snapshot()
                for call, operation in (
                    (lambda: file.read_exact(-1), "read_exact"),
                    (lambda: file.read_exact(helper_limit + 1), "read_exact"),
                    (lambda: file.write_all(bytes(helper_limit + 1)), "write_all"),
                ):
                    error = await refuses(call, "ValidationError")
                    assert (error.operation, error.outcome, error.completed_bytes) == (f"filesystem.{operation}", "not_committed", 0)
                    assert await helper_snapshot() == helper_before
                assert await fs.read_file("/metadata") == b"original"
                await helper_preflight_outcomes()
                assert await helper_snapshot() == helper_before
                await file.fchmod(0o400)
                await refuses(lambda: fs.open("/metadata", access="write"), "Filesystem", "EACCES")
                await file.pwrite(b"kept", 0)
                await fs.chmod("/metadata", 0o600)
                version = (await file.fstat()).version
                for time in (-123456789, -(2**63), 2**63 - 1):
                    await fs.utimens("/metadata", time)
                    value = await file.fstat()
                    assert (value.mtime_ns, value.atime_ns, value.version) == (time, 0, version)
                omitted = await file.fstat()
                await fs.utimens("/metadata", None)
                await refuses(lambda: fs.utimens("/metadata", str(2**63)), "ValidationError")
                assert await file.fstat() == omitted
                before = await ctx.now_ms() * 1000000
                await fs.utimens("/metadata")
                now = await file.fstat()
                after = await ctx.now_ms() * 1000000
                assert before <= now.mtime_ns <= after and before <= now.ctime_ns <= after and now.version == version
                distant = 2**53 + 1
                assert await file.lseek(distant) == distant
                assert await file.read(1) == b""
                await refuses(lambda: file.write(b"x"), "Filesystem", "EFBIG")
                await refuses(lambda: file.pread(1, 2**63), "Filesystem", "EOVERFLOW")
                await refuses(lambda: file.lseek(2**63), "Filesystem", "EOVERFLOW")
                assert await file.lseek(0, "current") == distant and (await file.fstat()).size == 8
                await fs.truncate("/metadata", 3)
                await file.ftruncate(8)
                assert await file.pread(8, 0) == b"kep\0\0\0\0\0"
                assert await file.lseek(0, "current") == distant
                assert (await fs.lstat("/metadata")).inode == (await file.fstat()).inode
                await file.fdatasync()

            async with await fs.creat("/created", 0o600) as created:
                await created.write_all(b"creat")
                assert (await created.fstat()).mode == 0o600
            await fs.unlink("/created")
            space, limits = await fs.statvfs(), (await fs.capabilities())["limits"]
            assert space["logical_bytes"] + space["available_logical_bytes"] == limits["max_logical_bytes"]
            assert space["allocated_bytes"] + space["available_allocated_bytes"] == limits["max_allocated_bytes"]
            assert space["inodes"] + space["available_inodes"] == limits["max_inodes"]

            await fs.mkdir("/at-source", 0o700)
            await fs.mkdir("/at-destination", 0o700)
            async with await fs.opendir("/at-source") as source, await fs.opendir("/at-destination") as destination:
                await fs.mkdirat(source, "sub", 0o777)
                assert (await fs.fstatat(source, "sub", True)).mode == 0o755
                async with await fs.openat(source, b"\xff", access="read_write", create_new=True) as held:
                    await held.write_all(b"relative")
                    identity = await held.fstat()
                    assert (await fs.fstatat(source, b"\xff", True)).inode == identity.inode
                    await fs.rename("/at-source", "/at-moved")
                    await fs.renameat(source, b"\xff", destination, b"\xfe")
                    await refuses(lambda: fs.fstatat(source, b"\xff"), "Filesystem", "ENOENT")
                    assert (await fs.fstatat(destination, b"\xfe")).inode == identity.inode
                    await fs.unlinkat(destination, b"\xfe")
                    assert (await held.fstat()).nlink == 0
                    assert await held.pread(8, 0) == b"relative"
                    await fs.unlinkat(source, "sub", True)
                    await source.rewinddir()
                    assert [entry.name for entry in await source.read_page(3)] == [b".", b".."]
                    await destination.fsync()
            assert not (await fs.read_dir("/at-moved", 10)).entries
            await fs.rmdir("/at-moved")
            await fs.rmdir("/at-destination")
        elif action == "replay":
            receipt = await ctx.storage.get_json("tool")
            assert receipt["result"] == "saved"
            assert await fs.read_file("/artifact") == receipt["result"].encode()
        elif action == "expired":
            try:
                await self.old.fstat()
            except SDKError as error:
                assert error.code in ("ContextExpired", "ContextMismatch")
                original = (error.code, error.outcome, getattr(error, "errno", None))
            else:
                raise AssertionError("expired handle worked")
            before, contents = await fs.stat("/notes"), await fs.read_file("/notes")
            for call, operation in (
                (lambda: self.old.read_exact(0), "read_exact"),
                (lambda: self.old.read_exact(1), "read_exact"),
                (lambda: self.old.write_all(b""), "write_all"),
                (lambda: self.old.write_all(b"x"), "write_all"),
            ):
                error = await refuses(call, original[0])
                assert (error.code, error.outcome, getattr(error, "errno", None)) == original
                if getattr(error, "operation", None) != f"filesystem.{operation}":
                    return Response(f"expired helper operation mismatch: {getattr(error, 'operation', None)} instead of filesystem.{operation}", status=500)
                assert error.completed_bytes == 0
                assert await fs.stat("/notes") == before
            assert await fs.read_file("/notes") == contents
            self.old = None
        elif action == "isolated":
            await refuses(lambda: fs.stat("/artifact"), "Filesystem", "ENOENT")
        elif action == "tools":
            tools = FilesystemTools(ctx)
            oversized = await tools.execute("oversized-offset", dict(operation="read", path="/tool-note", offset="9" * 5000))
            assert oversized["result"]["error"]["code"] == "ValidationError"
            invalid = await tools.execute("invalid-read-bound", dict(operation="read", path="/tool-note", max_bytes=0))
            assert invalid["result"]["error"]["code"] == "ValidationError"
            assert (await tools.execute("invalid-read-bound", dict(operation="read", path="/tool-note", max_bytes=0)))["replayed"]
            decision = dict(operation="write", path="/tool-note", content="first")
            await ctx.storage.put_json("agent", {"phase": "decided"})
            prior = await ctx.storage.read_json("agent")
            written = await tools.execute("write-one", decision, checks={"agent": prior}, puts={"agent": {"phase": "tool-complete"}})
            assert written == {"result": {"written_bytes": 5}, "replayed": False}
            assert (await ctx.storage.get_json("agent"))["phase"] == "tool-complete"
            read = await tools.execute("read-one", dict(operation="read", path="/tool-note"))
            assert read["result"]["text"] == "first" and not read["replayed"]
            await fs.write_file("/tool-note", b"later")
            replay = await tools.execute("read-one", dict(operation="read", path="/tool-note"))
            assert replay["replayed"] and replay["result"]["text"] == "first"
            assert (await tools.execute("write-one", decision))["replayed"]
            assert await fs.read_file("/tool-note") == b"later"
            await refuses(lambda: tools.execute("write-one", {**decision, "content": "different"}), "ValidationError")
            binary = await tools.execute("read-binary-name", dict(operation="read", path=b"/moved/\xff"))
            assert binary["result"]["text"] == "binary"
            before = await fs.stat("/tool-note")
            edit = dict(operation="edit", path="/tool-note", inode=before.inode, version=before.version, offset=0, delete_bytes=1, insert="L")
            await tools.execute("edit-one", edit)
            stale = await tools.execute("edit-stale", {**edit, "insert": "X"})
            assert stale["result"]["error"]["code"] == "Conflict"
            assert await fs.read_file("/tool-note") == b"Later"
            missing = await tools.execute("missing-read", dict(operation="read", path="/not-yet-created"))
            assert missing["result"]["error"]["errno"] == "ENOENT"
            await fs.write_file("/not-yet-created", b"exists now")
            assert (await tools.execute("missing-read", dict(operation="read", path="/not-yet-created")))["result"] == missing["result"]
            found = await tools.execute("search-one", dict(operation="search", path="/", pattern="Later"))
            assert any(match.get("path") == "/tool-note" for match in found["result"]["matches"])
            listing = await tools.execute("list-one", dict(operation="list", path="/", limit=10))
            assert any(entry.get("name") == "tool-note" for entry in listing["result"]["entries"])
            mutable = dict(operation="write", path="/snapshot-request", content="safe")
            saving = asyncio.create_task(tools.execute("snapshot-one", mutable))
            await asyncio.sleep(0)
            mutable["content"] = "changed after admission"
            await saving
            assert await fs.read_file("/snapshot-request") == b"safe"
            limited = FilesystemTools(ctx, prefix="limited-tools:", max_receipts=1)
            await limited.execute("one", dict(operation="stat", path="/tool-note"))
            await refuses(lambda: limited.execute("two", dict(operation="stat", path="/tool-note")), "ValidationError")
            assert (await limited.execute("one", dict(operation="stat", path="/tool-note")))["replayed"]
            prepared = await tools.prepare(dict(operation="write", path="/prepared", content="journal"))
            await refuses(lambda: fs.stat("/prepared"), "Filesystem", "ENOENT")
            result = prepared.pop("result")
            await ctx.storage.apply(**prepared, puts={"prepared-journal": result})
            await fs.sync()
            assert await fs.read_file("/prepared") == b"journal"
        elif action == "tools-recovered":
            saved = await ctx.storage.get_json("directory-edit-request")
            assert saved is not None
            refused = await FilesystemTools(ctx, prefix="directory-edit-tools:").execute("empty-directory-edit", saved)
            assert refused["replayed"]
            assert (refused["result"]["error"]["errno"], refused["result"]["error"]["outcome"]) == ("EISDIR", "not_committed")
            await refuses(lambda: fs.stat(saved["path"]), "Filesystem", "ENOENT")
            tools = FilesystemTools(ctx)
            read = await tools.execute("read-one", dict(operation="read", path="/tool-note"))
            assert read["replayed"] and read["result"]["text"] == "first"
            assert await fs.read_file("/tool-note") == b"Later"
            assert await fs.read_file("/snapshot-request") == b"safe"
            assert await fs.read_file("/prepared") == b"journal"
        elif action == "gc-start":
            # Drain earlier replacement fixtures before the exact baseline;
            # the new orphan below must be reclaimed by host maintenance.
            for batch in range(128):
                if await fs.collect_garbage() == 0:
                    break
            else:
                raise AssertionError("fixture garbage did not drain")
            baseline = await fs.statvfs()
            await ctx.storage.put_json("gc_baseline", dict(allocated=baseline["allocated_bytes"], inodes=baseline["inodes"]))
            async with await fs.open("/orphan", access="read_write", create_new=True) as orphan:
                await orphan.write_all(bytes([7]) * 65536)
                await fs.unlink("/orphan")
                assert await orphan.pread(1, 0) == bytes([7])
        elif action == "gc-check":
            baseline = await ctx.storage.get_json("gc_baseline")
            current = await fs.statvfs()
            return Response("done" if current["allocated_bytes"] == baseline["allocated"] and current["inodes"] == baseline["inodes"] else "pending")
        elif action == "recovered":
            assert (await ctx.storage.get_json("tool"))["result"] == "saved"
            assert await fs.read_file("/artifact") == b"saved"
            await fs.write_file("/after-recovery", b"new")
            await fs.sync()
        else:
            raise AssertionError("unknown filesystem action")
        return Response("ok")

async def fetch(req, ctx):
    if req.url.endswith("/stateless-fs"):
        try:
            await ctx.fs.stat("/")
        except SDKError as error:
            assert error.outcome == "not_committed" and error.operation == "filesystem.stat"
        else:
            raise AssertionError("stateless filesystem access succeeded")
        return Response("ok")
    name = req.url.rsplit("/", 2)[-2]
    return await ctx.namespace("COUNTERS").get_by_name(name).fetch(Request(req.url))
