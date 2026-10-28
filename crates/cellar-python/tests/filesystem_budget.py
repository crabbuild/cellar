# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

from cellar import Request, Response, SDKError, ValidationError
from cellar_filesystem_tools import FilesystemTools, validate_filesystem_checkpoint


class Counter:
    async def fetch(self, request, ctx):
        tools = FilesystemTools(ctx)
        parent = "/" + "a" * 255
        nested = parent + "/" + "b" * 255
        source = nested + "/" + "c" * 255
        read = dict(operation="read", path=source, max_bytes=1024)
        if request.path.endswith("recovered"):
            saved = await ctx.storage.get_json("helper-prefix-checkpoint")
            assert saved is not None
            async with await ctx.fs.open("/helper-prefix") as file:
                stat = await file.fstat()
                assert (str(stat.inode), str(stat.version), str(stat.size)) == (saved["inode"], saved["version"], saved["size"])
                assert await file.pread(4, 0) == bytes([42]) * 4
                assert await file.pread(saved["transfer"], stat.size - saved["transfer"]) == bytes([90]) * saved["transfer"]
                assert await file.lseek(0, "current") == 0
            replay = await tools.execute("large-read-receipt", read)
            assert replay["replayed"] and replay["result"]["error"]["code"] == "ValidationError"
            assert await ctx.fs.read_file("/receipt-destination") == b"unchanged"
            final = await ctx.storage.read_json("bounded-journal")
            assert final.value == dict(status="failed", data="x" * 3200, error="checkpoint_limit")
            return Response(b"ok")
        capabilities = await ctx.fs.capabilities()
        transport, limits = capabilities["transport"], capabilities["limits"]
        assert transport["max_value_bytes"] == 8192
        assert 2 * limits["max_transfer_bytes"] <= limits["max_file_helper_bytes"]
        await ctx.fs.write_file("/helper-prefix", bytes([42]) * 4096)
        async with await ctx.fs.open("/helper-prefix", access="read_write") as file:
            before_eof = await file.fstat()
            try:
                await file.read_exact(4097)
            except SDKError as error:
                assert (error.code, error.errno, error.operation, error.outcome, error.completed_bytes) == ("UnexpectedEof", "EIO", "filesystem.read_exact", "unknown", 4096)
            else:
                raise AssertionError("read_exact succeeded past EOF")
            assert await file.lseek(0, "current") == 4096 and await file.fstat() == before_eof
            await file.lseek(limits["max_file_bytes"] - limits["max_transfer_bytes"])
            try:
                await file.write_all(bytes([90]) * (2 * limits["max_transfer_bytes"]))
            except SDKError as error:
                assert (error.errno, error.operation, error.outcome, error.completed_bytes) == ("EFBIG", "filesystem.write_all", "locally_committed", limits["max_transfer_bytes"])
            else:
                raise AssertionError("write_all succeeded past file limit")
            assert await file.lseek(0, "current") == limits["max_file_bytes"]
            assert await file.pread(limits["max_transfer_bytes"], limits["max_file_bytes"] - limits["max_transfer_bytes"]) == bytes([90]) * limits["max_transfer_bytes"]
            stat = await file.fstat()
            await ctx.storage.put_json("helper-prefix-checkpoint", dict(inode=str(stat.inode), version=str(stat.version), size=str(stat.size), transfer=limits["max_transfer_bytes"]))
        await ctx.fs.mkdir(parent)
        await ctx.fs.mkdir(nested)
        await ctx.fs.write_file(source, bytes(1024))
        refused = await tools.execute("large-read-receipt", read)
        assert refused["result"]["error"]["code"] == "ValidationError"
        replay = await tools.execute("large-read-receipt", read)
        assert replay["replayed"] and replay["result"]["error"]["code"] == "ValidationError"
        await ctx.fs.write_file("/receipt-destination", b"unchanged")
        try:
            await tools.execute("escaped-write-receipt", dict(operation="write", path="/receipt-destination", content="\0" * 1200))
        except ValidationError as error:
            assert error.outcome == "not_committed"
        else:
            raise AssertionError("unreadable receipt admitted a file mutation")
        assert await ctx.fs.read_file("/receipt-destination") == b"unchanged"
        pending = dict(status="pending", data="x" * 3200)
        prior = await ctx.storage.read_json("bounded-journal")
        initial = dict(checks={"bounded-journal": prior}, puts={"bounded-journal": pending})
        validate_filesystem_checkpoint(initial, {**transport, "max_body_bytes": 1}, journal_keys=("bounded-journal",))
        await ctx.storage.apply(**initial)
        current = await ctx.storage.read_json("bounded-journal")
        for data in ("x" * 4000, "\U0001d11e" * 600):
            oversized = dict(checks={"bounded-journal": current}, puts={"bounded-journal": {**pending, "data": data}},
                             filesystem_mutations=[dict(op="write_file", path="/receipt-destination", data_base64="Y2hhbmdlZA==")])
            submitted = False
            try:
                validate_filesystem_checkpoint(oversized, transport, journal_keys=("bounded-journal",))
                submitted = True
                await ctx.storage.apply(**oversized)
            except ValidationError as error:
                assert not submitted and error.outcome == "not_committed"
            else:
                raise AssertionError("journal without recovery space admitted file effects")
        terminal = dict(checks={"bounded-journal": current}, puts={"bounded-journal": {**pending, "status": "failed", "error": "checkpoint_limit"}})
        validate_filesystem_checkpoint(terminal, transport)
        await ctx.storage.apply(**terminal)
        assert await ctx.fs.read_file("/receipt-destination") == b"unchanged"
        await ctx.fs.sync()
        return Response(b"ok")


async def fetch(request, ctx):
    return await ctx.namespace("COUNTERS").get_by_name("one").fetch(Request(request.url))
