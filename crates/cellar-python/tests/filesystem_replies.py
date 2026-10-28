# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Protocol faults, including real Context reply settlement, without native I/O."""
import asyncio
import json
from types import SimpleNamespace
from cellar import Context, Directory, File, Filesystem, FileVersion, Overloaded, SDKError


async def filesystem_reply_profile(fixture):
    replies = [reply for case in fixture["cases"] for reply in case["replies"]]
    submitted = 0

    class Host:
        async def _call(self, operation, **request):
            nonlocal submitted
            reply = replies[submitted]
            submitted += 1
            assert operation == "filesystem." + reply["operation"], operation
            value = fixture["templates"][reply["template"]] | reply.get("patch", {}) if "template" in reply else reply.get("value")
            payload = reply["raw"] if "raw" in reply else json.dumps(reply.get("envelope", dict(ok=True, value=value)))
            future = asyncio.get_running_loop().create_future()
            Context._reply(SimpleNamespace(_pending={1: future}), 1, payload)
            assert future.done(), "reply decoding left the SDK waiter pending"
            return await future

    fs = Filesystem(Host())
    file, directory = File(fs, "file"), Directory(fs, "directory")
    data = bytes([1, 2, 3, 4])
    calls = dict(
        capabilities=fs.capabilities, statvfs=fs.statvfs,
        open=lambda: fs.open("/note"), creat=lambda: fs.creat("/note"),
        openat=lambda: fs.openat(directory, "note"), opendir=lambda: fs.opendir("/"),
        read=lambda: file.read(4), pread=lambda: file.pread(4, 0),
        write=lambda: file.write(data), pwrite=lambda: file.pwrite(data, 0),
        read_exact=lambda: file.read_exact(4), write_all=lambda: file.write_all(data),
        lseek=lambda: file.lseek(0), fstat=file.fstat,
        ftruncate=lambda: file.ftruncate(0), fchmod=lambda: file.fchmod(0o600),
        dup=file.dup, close=file.close, fsync=file.fsync, fdatasync=file.fdatasync,
        stat=lambda: fs.stat("/note"), lstat=lambda: fs.lstat("/note"),
        fstatat=lambda: fs.fstatat(directory, "note"), truncate=lambda: fs.truncate("/note", 0),
        mkdir=lambda: fs.mkdir("/new"), mkdirat=lambda: fs.mkdirat(directory, "new"),
        rmdir=lambda: fs.rmdir("/new"), unlink=lambda: fs.unlink("/note"),
        unlinkat=lambda: fs.unlinkat(directory, "note"), rename=lambda: fs.rename("/note", "/new"),
        renameat=lambda: fs.renameat(directory, "note", directory, "new"),
        chmod=lambda: fs.chmod("/note", 0o600), utimens=lambda: fs.utimens("/note"),
        readdir=directory.readdir, read_page=lambda: directory.read_page(2),
        telldir=directory.telldir, seekdir=lambda: directory.seekdir("AAAAAAAAAAAAAAAAAAAAAA=="),
        rewinddir=directory.rewinddir, closedir=directory.closedir,
        directory_fstat=directory.fstat, directory_fsync=directory.fsync,
        read_file=lambda: fs.read_file("/note", 4), write_file=lambda: fs.write_file("/note", data),
        replace_file=lambda: fs.replace_file("/note", data),
        edit_file=lambda: fs.edit_file("/note", FileVersion(1, 1), offset=0, delete_bytes=0, insert=data),
        read_dir=lambda: fs.read_dir("/", 2), sync=fs.sync, collect_garbage=fs.collect_garbage)
    for case in fixture["cases"]:
        failure = None
        try:
            await calls[case.get("call", case["operation"])]()
        except SDKError as error:
            failure = error
        if case["accept"]:
            assert failure is None, (case["id"], failure)
            continue
        expected = case.get("expected", dict(code="HostError", errno="EIO", outcome="unknown", completed_bytes=case.get("completed_bytes", 0), cause=True))
        actual = {key: getattr(failure, key, None) for key in ("code", "errno", "outcome", "completed_bytes")}
        actual["cause"] = failure is not None and failure.__cause__ is not None
        assert actual == expected, (case["id"], actual, expected)
        assert failure.operation == "filesystem." + case["operation"], case["id"]
        if expected["code"] == "Overloaded":
            assert isinstance(failure, Overloaded)
    assert submitted == len(replies), (submitted, len(replies))
    return len(fixture["cases"])
