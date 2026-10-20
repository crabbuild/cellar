import asyncio
import hashlib
import json
import time
import weakref
from cellar import Ambiguous, Conflict, MISSING, Overloaded, Request, Response, SDKError, ValidationError

saved = None
live_context = None
scope_ready = asyncio.Event()
scope_release = asyncio.Event()
host_waiting = asyncio.Event()
host_release = asyncio.Event()
mailbox_progress = False
generator_closed = False
held_generator = None
fail_constructor = False
proof_sync_completed = False
proof_python_returned = False
proof_attempts = {}
instances = weakref.WeakSet()
native_snapshot_ready = asyncio.Event()
native_snapshot_release = asyncio.Event()
fence_waiting = asyncio.Event()
fence_release = asyncio.Event()
fence_result = None
alarm_deliveries = []

async def fetch(request, ctx):
    global saved, live_context, mailbox_progress, held_generator, fail_constructor
    global proof_sync_completed, proof_python_returned
    if request.path == "/proof/reset":
        proof_sync_completed = proof_python_returned = False
        return Response.json(True)
    if request.path == "/proof/progress":
        return Response.json({"sync": proof_sync_completed, "returned": proof_python_returned})
    if request.path == "/proof/attempts":
        return Response.json(proof_attempts)
    if request.path == "/loop-lag":
        until = time.perf_counter_ns() + 60_000_000
        while time.perf_counter_ns() < until:
            pass
        return Response.json(True)
    if request.path == "/protocol-validation":
        invalid = [("", {}), ("x" * 64, {}), ("x" * 65, {}), ("unknown", {}), ("kv.put", {"binding": 42, "key": "x", "value": True}),
                   ("sql.query", {"binding": "DB", "sql": "INSERT", "params": {}}),
                   ("queue.send", {"binding": "JOBS", "body": {}, "delay_seconds": -1}),
                   ("queue.settle", {"retry": True, "delay_seconds": -1}),
                   ("cell.fetch", {"binding": "COUNTERS", "name": "unused", "request": {}})]
        for operation, payload in invalid:
            try:
                await ctx._call(operation, **payload)
                raise AssertionError("invalid operation reached SDK execution")
            except ValidationError as error:
                assert error.code == "ValidationError" and error.outcome == "not_committed"
        try:
            await ctx.namespace("PRIVATE-BINDING-SENTINEL").get_by_name("unused").fetch(Request("http://cell/"))
            raise AssertionError("absent binding succeeded")
        except Ambiguous as error:
            # No source inspection or message guessing converts a generic
            # submitted failure into a claim of rollback.
            assert error.outcome == "unknown"
            assert "PRIVATE-BINDING-SENTINEL" not in str(error)
        return Response.json(True)
    if request.path == "/overload-after-write":
        cell = ctx.namespace("COUNTERS").get_by_name("overload")
        try:
            await cell.fetch(Request("http://cell/native-overload"))
            raise AssertionError("native failure returned success")
        except SDKError as error:
            classification = {"code": error.code, "outcome": error.outcome}
        result = await cell.fetch(Request("http://cell/overload-state"))
        classification["count"] = json.loads(result.body)
        return Response.json(classification)
    if request.path == "/unraisable":
        class Resource:
            def __del__(self):
                raise ValueError("PRIVATE-UNMANAGED-EXCEPTION-SENTINEL")
        del_resource = Resource()
        del del_resource
        return Response.json(True)
    if request.path == "/thread-error":
        import threading
        def worker():
            raise ValueError("PRIVATE-UNMANAGED-EXCEPTION-SENTINEL")
        thread = threading.Thread(target=worker)
        thread.start()
        await asyncio.to_thread(thread.join)
        return Response.json(True)
    if request.path == "/fence-waiting":
        return Response.json(fence_waiting.is_set())
    if request.path == "/fence-release":
        fence_release.set()
        return Response.json(True)
    if request.path == "/fence-result":
        return Response.json(fence_result)
    if request.path == "/native-snapshot-ready":
        return Response.json(native_snapshot_ready.is_set())
    if request.path == "/native-snapshot-release":
        native_snapshot_release.set()
        return Response.json(True)
    if request.path == "/duplicate-alarm":
        return await ctx.namespace("COUNTERS").get_by_name("duplicates").fetch(request)
    if request.path == "/constructor-failure":
        fail_constructor = True
        try:
            try:
                await ctx.namespace("COUNTERS").get_by_name("constructor").fetch(Request("http://cell/count"))
                raise AssertionError("failed constructor accepted a fetch")
            except SDKError as error:
                assert error.code == "Ambiguous" and error.outcome == "unknown"
        finally:
            fail_constructor = False
        recovered = await ctx.namespace("COUNTERS").get_by_name("constructor").fetch(Request("http://cell/count"))
        assert recovered.status == 200
        return Response.json(True)
    if request.path == "/task-budget":
        tasks = []
        try:
            for _ in range(2048):
                tasks.append(asyncio.create_task(asyncio.sleep(60)))
            raise AssertionError("task budget did not apply")
        except RuntimeError as error:
            assert str(error) == "Cellar Python task capacity exhausted"
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        return Response.json(True)
    if request.path == "/cpu-loop":
        while True:
            pass
    if request.path == "/blocked-native":
        import ctypes
        # PyDLL intentionally retains the GIL across this native call.
        ctypes.PyDLL(None).sleep(60)
        return Response.json(False)
    if request.path == "/generator":
        async def resource():
            global generator_closed
            try:
                yield 1
            finally:
                await asyncio.sleep(0)
                generator_closed = True
        held_generator = resource()
        assert await anext(held_generator) == 1
        return Response.json(True)
    if request.path == "/host-await":
        nested = asyncio.create_task(ctx.namespace("COUNTERS").get_by_name("blocking").fetch(Request("http://cell/hold")))
        await host_waiting.wait()
        # The native driver must service this same invocation while fetch awaits.
        assert await ctx.var("DOMAIN_MARKER") == "python-test"
        try:
            await ctx.namespace("COUNTERS").get_by_name("forbidden").fetch(Request("http://cell/counter"))
            raise AssertionError("concurrent mutation accepted")
        except SDKError as error:
            assert isinstance(error, Overloaded)
            assert error.code == "Overloaded"
        mailbox_progress = True
        return await nested
    if request.path == "/host-waiting":
        return Response.json(host_waiting.is_set())
    if request.path == "/host-progress":
        assert host_waiting.is_set() and not host_release.is_set()
        while not mailbox_progress:
            await asyncio.sleep(0.001)
        return Response.json(True)
    if request.path == "/host-release":
        host_release.set()
        return Response.json(True)
    if request.path == "/scope-hold":
        live_context = ctx
        scope_ready.set()
        await scope_release.wait()
        return Response.json(True)
    if request.path == "/scope-ready":
        return Response.json(scope_ready.is_set())
    if request.path == "/scope-release":
        scope_release.set()
        return Response.json(True)
    if request.path == "/scope-steal":
        assert await ctx.var("DOMAIN_MARKER") == "scope-other"
        try:
            stolen = await live_context.var("DOMAIN_MARKER")
            return Response.json({"refused": False, "stolen": stolen})
        except SDKError as error:
            return Response.json({"refused": True, "code": error.code})
    if request.path == "/scope-direct":
        try:
            live_context._bridge.submit(999999, "var", json.dumps({"name": "DOMAIN_MARKER"}))
            return Response.json({"refused": False})
        except RuntimeError as error:
            return Response.json({"refused": True, "code": str(error)})
    if request.path == "/expire":
        if saved is None:
            saved = ctx
            return Response.json(True)
        try:
            await saved.var("SECRET")
            raise AssertionError("expired context accepted")
        except SDKError as error:
            return Response.json(str(error))
    if request.path == "/fatal":
        async def fatal():
            await asyncio.sleep(0.01)
            raise SystemExit("controlled background exit")
        asyncio.create_task(fatal())
        return Response.json(True)
    if request.path == "/stubborn":
        async def stubborn():
            while True:
                try:
                    await asyncio.sleep(60)
                except asyncio.CancelledError:
                    pass
        asyncio.create_task(stubborn())
        return Response.json(True)
    if request.path == "/budget":
        for _ in range(1100):
            try:
                await ctx.var("SECRET")
            except SDKError as error:
                assert error.code == "Overloaded" and error.outcome == "not_committed"
                return Response.json(True)
        raise AssertionError("operation limit did not apply")
    if request.path == "/cancel":
        try:
            await asyncio.sleep(30)
        finally:
            # This context must have lost authority before task cancellation.
            try:
                await ctx.var("SECRET")
                raise AssertionError("cancelled context accepted")
            except SDKError:
                pass
    if request.path == "/failure-private":
        raise ValueError("request-body-secret:" + (await request.text()) + " /tmp/private-internal.py")
    if request.path == "/failure":
        raise ValueError("controlled failure")
    if request.path == "/oversize":
        return Response(b"x" * 8192)
    if request.path == "/headers":
        assert [value for name, value in request.headers if name == "x-repeated"] == ["one", "two"]
        return Response(b"headers", headers=[("x-repeated", "first"), ("x-repeated", "second")])
    if request.path == "/header-limit":
        return Response(headers=[("x-long", "x" * 32769)])
    if request.path == "/header-count":
        return Response(headers=[("x-count", "x")] * 129)
    if request.path == "/echo":
        return Response(request.body, headers=[("content-type", "application/octet-stream")])
    if request.path == "/threadpool":
        def foreign_loop():
            async def wrong_context():
                try:
                    await ctx.var("SECRET")
                    raise AssertionError("foreign event loop accepted")
                except SDKError as error:
                    assert error.code == "NotSubmitted"
            asyncio.run(wrong_context())
        await asyncio.to_thread(foreign_loop)
        return Response.json(await asyncio.to_thread(lambda: hashlib.sha256(b"cellar").hexdigest()))
    if request.path == "/library":
        # A real native extension (_hashlib) plus pure Python library imports.
        return Response.json(hashlib.sha256(b"cellar").hexdigest())
    return await ctx.namespace("COUNTERS").get_by_name("one").fetch(request)


class Counter:
    def __init__(self):
        if fail_constructor:
            raise ValueError("controlled construction failure")
        instances.add(self)

    async def fetch(self, request, ctx):
        global fence_result
        global proof_sync_completed, proof_python_returned
        storage = ctx.storage
        if request.path == "/durable-alarm-arm":
            original_alarm = await storage.get_alarm()
            original_intent = await storage.read_json("alarm-intent")
            deadline = await ctx.now_ms() + 10_000
            try:
                await storage.apply(checks={"count": MISSING},
                                    puts={"alarm-intent": deadline}, alarm_at_ms=deadline)
            except Conflict as error:
                assert error.outcome == "not_committed"
            else:
                raise AssertionError("failed check armed an alarm")
            assert await storage.get_alarm() == original_alarm
            assert await storage.read_json("alarm-intent") == original_intent
            await storage.apply(checks={"alarm-intent": original_intent},
                                puts={"alarm-intent": deadline}, alarm_at_ms=deadline)
            assert await storage.get_alarm() == deadline
            return Response.json(deadline)
        if request.path in ("/durable-alarm-state", "/durable-alarm-repeat"):
            deadline = await storage.get_json("alarm-intent")
            if request.path.endswith("repeat"):
                await self.alarm(deadline + 1_000, ctx)
            return Response.json({"intent": deadline,
                                  "receipt": await storage.get_json("alarm:" + str(deadline), False),
                                  "effects": await storage.get_json("alarms", 0),
                                  "deliveries": alarm_deliveries})
        if request.path == "/overload-state":
            return Response.json(await storage.get_json("overload-committed"))
        if request.path == "/proof/state":
            return Response.json({key: await storage.get_json("proof:" + key) for key in ("sync", "output", "failure")})
        if request.path.startswith("/proof/"):
            operation = request.path.rsplit("/", 1)[1]
            proof_attempts[operation] = proof_attempts.get(operation, 0) + 1
            key = "proof:" + operation
            snapshot = await storage.read_json(key)
            await storage.apply(checks={key: snapshot}, puts={key: (snapshot.value if snapshot.present else 0) + 1})
            if request.path in ("/proof/sync", "/proof/failure"):
                await storage.sync()
                proof_sync_completed = True
            proof_python_returned = True
            return Response.json(True)
        if request.path == "/fence-write":
            assert await storage.get_json("count") == 1
            fence_waiting.set()
            cancellations = 0
            # Deliberately noncooperative application code retains its old
            # context even when the native owner closes and cancels its task.
            while not fence_release.is_set():
                try:
                    await fence_release.wait()
                except asyncio.CancelledError:
                    cancellations += 1
            assert cancellations > 0
            try:
                await storage.put_json("fenced-write", True)
                fence_result = "accepted"
            except SDKError as error:
                fence_result = error.code
            return Response.json(True)
        if request.path == "/fence-state":
            return Response.json({"count": await storage.get_json("count"),
                                  "late": (await storage.read_json("fenced-write")).present})
        if request.path == "/batch-validation":
            original_alarm = await storage.get_alarm()
            try:
                await storage.apply(checks={"key": False}, puts={"batch-invalid": True})
                raise AssertionError("malformed snapshot accepted")
            except ValidationError as error:
                assert error.outcome == "not_committed"
            invalid = [
                {"deletes": ["same", "same"]},
                {"puts": {"same": 1}, "deletes": ["same"]},
                {"puts": {"": 1}},
                {"puts": {"x" * 2049: 1}},
                {"puts": {"__sys_private": 1}},
                {"checks": {"key": {"present": "yes"}}},
                {"checks": {"key": {"present": False, "extra": 1}}},
                {"alarm_at_ms": -1},
                {"alarm_at_ms": True},
                {"alarm_at_ms": 2**50, "delete_alarm": True},
                {"unexpected": True},
                {"puts": {str(i): i for i in range(5)}},
                {"checks": {str(i): {"present": False} for i in range(4)},
                 "alarm_at_ms": 2**50},
            ]
            for batch in invalid:
                # Exercise native validation too, bypassing facade conveniences.
                batch.setdefault("puts", {})["batch-invalid"] = True
                try:
                    await ctx._call("storage.apply", **batch)
                    raise AssertionError("invalid batch reached its transaction")
                except SDKError as error:
                    assert error.code == "ValidationError" and error.outcome == "not_committed"
                assert not (await storage.read_json("batch-invalid")).present
                assert await storage.get_alarm() == original_alarm
            # Exactly four operations: one check, one put, one delete, one alarm.
            await storage.apply(checks={"batch-exact": MISSING},
                                puts={"batch-exact": 7}, deletes=["unused"],
                                alarm_at_ms=2**50)
            assert await storage.get_json("batch-exact") == 7
            assert await storage.get_alarm() == 2**50
            await storage.delete_alarm()
            return Response.json(True)
        if request.path == "/native-conflict":
            snapshot = await storage.read_json("native-conflict")
            native_snapshot_ready.set()
            await native_snapshot_release.wait()
            try:
                await storage.apply(checks={"native-conflict": snapshot},
                                    puts={"native-conflict": 99, "python-only": True})
                raise AssertionError("Python accepted a stale native snapshot")
            except Conflict:
                return Response.json({"conflict": True,
                                      "value": await storage.get_json("native-conflict"),
                                      "native": await storage.get_json("native-only", False),
                                      "python": (await storage.read_json("python-only")).present})
        if request.path == "/codec":
            async def raw_apply(puts):
                # Bypass the facade codec to verify native depth refusal before
                # effects, rather than testing the Python encoder twice.
                ctx._sequence += 1
                sequence = ctx._sequence
                future = asyncio.get_running_loop().create_future()
                ctx._pending[sequence] = future
                try:
                    ctx._bridge.submit(sequence, "storage.apply", json.dumps({"puts": puts}))
                    await future
                finally:
                    ctx._pending.pop(sequence, None)
            exact = None
            for _ in range(62):
                exact = [exact]
            await raw_apply({"depth-exact": exact})
            assert await storage.get_json("depth-exact") == exact
            try:
                await raw_apply({"depth-over": [exact]})
                raise AssertionError("native depth limit accepted an over-limit write")
            except ValidationError as error:
                assert error.outcome == "not_committed"
            assert not (await storage.read_json("depth-over")).present
            cycle = []
            cycle.append(cycle)
            deep = None
            for _ in range(66):
                deep = [deep]
            invalid = [float("nan"), float("inf"), -float("inf"), 2**64, -(2**63)-1,
                       {1: "integer key"}, object(), cycle, deep]
            for value in invalid:
                try:
                    await storage.put_json("codec-invalid", value)
                    raise AssertionError("unsupported value reached storage")
                except ValidationError as error:
                    assert error.outcome == "not_committed"
            assert not (await storage.read_json("codec-invalid")).present
            valid = [-(2**63), 2**64-1, None, False, {"nested": [1, "é", 1.5]}]
            for value in valid:
                await storage.put_json("codec-valid", value)
                assert await storage.get_json("codec-valid") == value
            return Response.json(True)
        if request.path == "/hold":
            host_waiting.set()
            await host_release.wait()
            return Response.json(True)
        if request.path == "/slow":
            await asyncio.sleep(0.05)
            return Response.json(True)
        if request.path == "/unawaited":
            asyncio.create_task(ctx.namespace("COUNTERS").get_by_name("slow").fetch(Request("http://cell/slow")))
            await asyncio.sleep(0)
            try:
                await storage.put_json("forbidden", True)
                raise AssertionError("overlapping mutation accepted")
            except SDKError as error:
                assert error.code == "Overloaded"
            return Response.json(True)
        if request.path == "/late":
            return Response.json(await storage.get_json("forbidden"))
        if request.path == "/atomic":
            await storage.apply(puts={"nullable": None, "first": 1})
            missing = await storage.read_json("missing")
            nullable = await storage.read_json("nullable")
            assert missing == MISSING and nullable.present and nullable.value is None
            before_alarm = await storage.get_alarm()
            try:
                await storage.apply(checks={"first": MISSING}, puts={"second": 2}, deletes=["first"], alarm_at_ms=2**50)
                raise AssertionError("conflict accepted")
            except Conflict as error:
                assert error.code == "Conflict" and error.outcome == "not_committed"
                assert await storage.get_json("first") == 1
                assert not (await storage.read_json("second")).present
                assert await storage.get_alarm() == before_alarm
            await storage.apply(checks={"nullable": nullable}, puts={"second": 2}, deletes=["first"])
            assert not (await storage.read_json("first")).present
            await storage.sql("CREATE TABLE IF NOT EXISTS local_test (value INTEGER)")
            await storage.sql("INSERT INTO local_test VALUES (?)", [42])
            rows = await storage.sql("SELECT value FROM local_test")
            assert rows["rows"] == [[42]]
            await storage.sync()
            return Response.json(True)
        if request.path == "/duplicate-alarm":
            await self.alarm(12345, ctx)
            await self.alarm(12345, ctx)
            return Response.json(await storage.get_json("alarms"))
        if request.path == "/alarm":
            await storage.set_alarm(await ctx.now_ms() + 100)
        if request.method == "POST":
            before = await storage.read_json("count")
            await storage.apply(checks={"count": before}, puts={"count": (before.value or 0) + 1})
        return Response.json({"count": await storage.get_json("count", 0),
                              "alarms": await storage.get_json("alarms", 0),
                              "name": ctx.identity.name})

    async def alarm(self, scheduled_ms, ctx):
        receipt_key = "alarm:" + str(scheduled_ms)
        receipt = await ctx.storage.read_json(receipt_key)
        if receipt.present:
            return
        snapshot = await ctx.storage.read_json("alarms")
        await ctx.storage.apply(checks={receipt_key: receipt, "alarms": snapshot},
                                puts={receipt_key: True, "alarms": (snapshot.value if snapshot.present else 0) + 1})
