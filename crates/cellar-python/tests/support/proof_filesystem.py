# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""File/state/receipt coherence while actual native durability proof is held."""
import base64
import hashlib
import sys
import sysconfig
from cellar import MISSING, SDKError

_fs_seed = _fs_pending = _fs_post = _fs_handles = None
_fs_refusals = 0
_fs_barrier_entered = False


def filesystem_payload(generation):
    data = bytearray([generation]) * (3 * 65536 + 17)
    data[65535], data[131072] = generation + 8, generation + 16
    return bytes(data)


def filesystem_receipt(generation):
    data = filesystem_payload(generation)
    return dict(generation=generation, sha256=hashlib.sha256(data).hexdigest(), size=len(data))


def filesystem_snapshot():
    return dict(refusals=_fs_refusals, barrier_entered=_fs_barrier_entered,
                seed=_fs_seed, pending=_fs_pending, post=_fs_post,
                gil_enabled=sys._is_gil_enabled())


def filesystem_clear():
    global _fs_handles
    _fs_handles = None


async def filesystem_commit(ctx, generation):
    global _fs_seed, _fs_pending, _fs_post
    assert sys._is_gil_enabled() is not bool(sysconfig.get_config_var("Py_GIL_DISABLED"))
    puts = dict(fs_generation=generation, filesystem_receipt=filesystem_receipt(generation))
    if generation == 1:
        puts["seed"] = 7
    elif generation == 2:
        puts["pending"] = 1
    else:
        puts["post"] = 3
    checks = {"pending": MISSING} if generation == 2 else None
    await ctx.storage.apply(checks=checks,
        filesystem_mutations=[dict(op="write_file", path="/ledger.bin", data_base64=base64.b64encode(filesystem_payload(generation)).decode())], puts=puts)
    stat = await ctx.fs.stat("/ledger.bin")
    observed = dict(inode=str(stat.inode), version=str(stat.version))
    with _lock:
        if generation == 1:
            _fs_seed = observed
        elif generation == 2:
            _fs_pending = observed
        else:
            _fs_post = observed


async def filesystem_retain(ctx):
    global _fs_handles
    file = await ctx.fs.open("/ledger.bin", access="read_write")
    duplicate = await file.dup()
    directory = await ctx.fs.opendir("/")
    _fs_handles = dict(file=file, duplicate=duplicate, directory=directory,
                       cookie=await directory.telldir(), fs=ctx.fs)


async def filesystem_barrier(ctx):
    global _fs_barrier_entered
    with _lock:
        _fs_barrier_entered = True
    if FILESYSTEM_BARRIER == "sync":
        await ctx.fs.sync()
    elif FILESYSTEM_BARRIER == "fsync":
        await _fs_handles["file"].fsync()
    else:
        assert FILESYSTEM_BARRIER == "fdatasync"
        await _fs_handles["file"].fdatasync()


async def filesystem_late(ctx):
    global _fs_refusals
    old = _fs_handles
    for call, operation in (
        (lambda: old["file"].read(1), "read"), (lambda: old["file"].write(b"late"), "write"),
        (lambda: old["file"].pread(1, 0), "pread"), (lambda: old["file"].pwrite(b"late", 0), "pwrite"),
        (lambda: old["file"].lseek(1), "lseek"), (old["file"].fstat, "fstat"),
        (lambda: old["file"].ftruncate(0), "ftruncate"), (lambda: old["file"].fchmod(0o400), "fchmod"),
        (old["file"].dup, "dup"), (old["file"].close, "close"),
        (old["file"].fsync, "fsync"), (old["file"].fdatasync, "fdatasync"),
        (lambda: old["file"].read_exact(0), "read_exact"), (lambda: old["file"].write_all(b""), "write_all"),
        (lambda: old["duplicate"].read(1), "read"), (old["duplicate"].close, "close"),
        (old["directory"].readdir, "readdir"), (lambda: old["directory"].read_page(1), "read_page"),
        (old["directory"].telldir, "telldir"), (lambda: old["directory"].seekdir(old["cookie"]), "seekdir"),
        (old["directory"].rewinddir, "rewinddir"), (old["directory"].closedir, "closedir"),
        (old["directory"].fstat, "fstat"), (old["directory"].fsync, "fsync"),
        (old["fs"].capabilities, "capabilities"), (lambda: old["fs"].stat("/ledger.bin"), "stat"),
        (lambda: old["fs"].read_file("/ledger.bin"), "read_file"), (lambda: old["fs"].write_file("/ledger.bin", b"late"), "write_file"),
        (lambda: old["fs"].unlink("/ledger.bin"), "unlink"), (old["fs"].sync, "sync"),
        (old["fs"].collect_garbage, "collect_garbage"),
    ):
        try:
            await call()
        except SDKError as error:
            assert (error.code, error.outcome, error.operation, error.completed_bytes) == ("ContextExpired", "not_committed", "filesystem." + operation, 0)
        else:
            raise AssertionError("retired proof context admitted filesystem work")
        with _lock:
            _fs_refusals += 1
    try:
        ctx._bridge.submit(10001, "filesystem.write_file", '{"path":"/late-raw","data_base64":"bGF0ZQ=="}')
    except RuntimeError as error:
        assert str(error) == "ContextExpired"
    else:
        raise AssertionError("retired proof sender admitted raw file work")
    with _lock:
        _fs_refusals += 1


async def filesystem_state(ctx, values):
    assert sys._is_gil_enabled() is not bool(sysconfig.get_config_var("Py_GIL_DISABLED"))
    generation = await ctx.storage.get_json("fs_generation")
    assert generation in (1, 2, 3)
    if generation != 3:
        assert values["pending"] == (1 if generation == 2 else None)
    receipt = await ctx.storage.get_json("filesystem_receipt")
    assert receipt == filesystem_receipt(generation)
    assert await ctx.fs.read_file("/ledger.bin") == filesystem_payload(generation)
    stat = await ctx.fs.stat("/ledger.bin")
    observed = dict(inode=str(stat.inode), version=str(stat.version))
    with _lock:
        assert observed == {1: _fs_seed, 2: _fs_pending, 3: _fs_post}[generation]
    try:
        await ctx.fs.stat("/late-raw")
    except SDKError as error:
        assert (error.code, error.errno) == ("Filesystem", "ENOENT")
    else:
        raise AssertionError("stale raw file mutation reached recovery")
    # All four host slots must be reusable while old guest wrappers remain live.
    file = await ctx.fs.open("/ledger.bin")
    duplicate = await file.dup()
    directory = await ctx.fs.opendir("/")
    fourth = await ctx.fs.open("/ledger.bin")
    try:
        await ctx.fs.open("/ledger.bin")
    except SDKError as error:
        assert (error.code, error.errno, error.outcome) == ("Filesystem", "ENFILE", "not_committed")
    else:
        raise AssertionError("filesystem host ceiling was not captured")
    await fourth.close()
    await directory.closedir()
    await duplicate.close()
    await file.close()
    await ctx.fs.sync()
    return receipt | observed
