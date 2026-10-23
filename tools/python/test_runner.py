"""Pump/context lifetimes; native authority and I/O belong to the bridge suite."""
import asyncio
import gc
import json
from pathlib import Path
import socket
import subprocess
import sys
import time
from types import SimpleNamespace
import unittest
import weakref


def continuous_arrivals(mode):
    if mode == "storage-lifetime":
        storage_lifetime()
        return
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "crates/cellar-python/python"))
    import runner
    from cellar import Response, _current_invocation

    payload = json.dumps(dict(identity=None, object=None, kind="fetch",
                              event=dict(url="http://pump/", method="GET", headers=[])))
    blocked = False

    async def fetch(request, context):
        nonlocal blocked
        assert request.method == "GET" and request.body == b""
        try:
            asyncio.create_task(asyncio.sleep(0))
        except RuntimeError as error:
            assert str(error) == "Cellar Python task capacity exhausted"
        else:
            raise AssertionError("the running invocation escaped nested task admission")
        if mode.startswith("blocking") and not blocked:
            blocked = True
            if mode == "blocking-yield":
                await asyncio.sleep(0)
            until = time.perf_counter_ns() + 40_000_000
            while time.perf_counter_ns() < until:
                pass
        if mode == "pending-cleanup":
            class Pending:
                def cancel(self):
                    assert not context._bridge.active(), "cleanup did not revoke authority first"
                    assert not context._bridge.finished, "cleanup published completion before cancelling pending work"
                    context._bridge.inbox.cancelled_futures += 1
            context._pending[1] = Pending()
        if mode == "release-storage":
            storage = context.storage
            assert storage._context is context
        return Response(b"ok")

    class Bridge:
        def __init__(self, inbox):
            self.inbox = inbox
            self.closed = False
            self.finished = False
            inbox.bridges.add(self)

        def active(self):
            return not self.closed

        def complete(self, payload, failed, body=None):
            assert _current_invocation.get() is self
            assert not self.closed
            if failed:
                assert payload == '"Python task capacity exhausted"'
                self.inbox.rejected += 1
            else:
                assert json.loads(payload) == dict(status=200, headers=[], body="")
                assert body == b"ok"
                self.inbox.completed += 1

        def expire(self, finished=False):
            if mode == "cleanup-failure" and not self.inbox.cleanup_failed:
                self.inbox.cleanup_failed = True
                raise RuntimeError("injected context cleanup failure")
            if finished and not self.finished:
                self.inbox.finished += 1
                self.finished = True
            if not self.closed:
                self.closed = True
                self.inbox.expired += 1

    class Inbox:
        def __init__(self):
            self.reader, self.writer = socket.socketpair()
            self.reader.setblocking(False)
            self.writer.setblocking(False)
            self.stopped = False
            self.issued = self.rejected = self.completed = self.expired = self.finished = self.cancelled_futures = 0
            self.observed = None
            self.joined = 0
            self.bridges = weakref.WeakSet()
            self.cleanup_failed = False

        async def occupy_task_slot(self):
            try:
                await asyncio.Event().wait()
            finally:
                self.joined += 1

        def wakeup_fd(self):
            loop = asyncio.get_running_loop()
            # Direct Task construction bypasses the managed factory. Admission
            # must still count those live tasks, not just its own registry.
            self.occupied = ([asyncio.Task(self.occupy_task_slot(), loop=loop) for _ in range(900)]
                             if mode == "many-tasks" else [loop.create_task(self.occupy_task_slot())])
            # The stop timer must run despite a continuously replenished inbox.
            loop.call_later(.005, self.checkpoint)
            return self.reader.fileno()

        def checkpoint(self):
            self.observed = self.issued
            if mode == "rejections":
                assert not self.bridges, "yielding pump retained a rejected invocation"
            asyncio.get_running_loop().call_later(.02, self.stop)

        def drain_wakeup(self):
            while True:
                try:
                    self.reader.recv(4096)
                except BlockingIOError:
                    return

        def take(self):
            if self.stopped:
                return None
            if mode.startswith("release-"):
                assert not self.bridges, "completed eager invocation retained until the end of the batch"
            self.issued += 1
            return self.issued, payload, b"", Bridge(self)

        def replies(self):
            return []

        def take_retirements(self):
            return False

        def sample(self, payload):
            json.loads(payload)

        def done(self):
            return self.stopped

        def stop(self):
            if not self.stopped:
                self.stopped = True
                self.writer.send(b"x")

    inbox = Inbox()
    if mode.startswith("release-") or mode == "rejections":
        # Collection must not hide SDK-owned reference cycles between requests.
        gc.disable()
    try:
        capacity = 902 if mode == "many-tasks" else 2 if mode == "rejections" else 3
        report = runner.run(SimpleNamespace(fetch=fetch), inbox, 30, .2, capacity, 16, 30)
        if mode == "cleanup-failure":
            assert report["runtime_failed"] and report["runtime_failure_stage"] == "serve_failure", report
            assert inbox.cleanup_failed and inbox.issued == inbox.expired
            assert report["pending_tasks"] == report["pending_objects"] == 0, report
            # Failure can stop the pump before these coroutines first run;
            # cancellation before first poll does not enter their finally.
            assert all(task.done() and task.cancelled() for task in inbox.occupied)
            return
        assert not report["runtime_failed"], report
        assert report["pending_tasks"] == report["pending_objects"] == 0, report
        assert report["peak_tasks"] <= capacity, report
        assert inbox.issued == inbox.expired
        assert inbox.finished == inbox.issued, "managed cleanup did not publish completion"
        if mode == "pending-cleanup":
            assert inbox.cancelled_futures == inbox.completed
        if mode == "rejections":
            assert inbox.rejected == inbox.issued and inbox.completed == 0
            assert report["budget_exhaustions"] == inbox.issued
            assert report["interpreter"]["handlers_started"] == 0
        else:
            assert inbox.completed > 0
        if mode.startswith("blocking"):
            assert report["interpreter"]["loop_delay"]["maximum_ns"] >= 30_000_000, report
        assert inbox.observed is not None and inbox.issued > inbox.observed > 0, (
            "queued starts require progress between timers without another native wakeup"
        )
        assert inbox.joined == len(inbox.occupied), "stop did not cancel and join every occupied task slot"
        assert all(task.done() and task.cancelled() for task in inbox.occupied)
        gc.collect()
        assert not inbox.bridges, "the idle or stopped pump retained a rejected invocation"
        print(json.dumps({"rejected": inbox.rejected, "completed": inbox.completed,
                          "joined": inbox.joined}))
    finally:
        inbox.reader.close()
        inbox.writer.close()


def storage_lifetime():
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "crates/cellar-python/python"))
    from cellar import Context, ContextMismatch, SDKError, _current_invocation

    class Bridge:
        closed = False
        submitted = 0

        def active(self):
            return not self.closed

        def expire(self, finished=False):
            assert not finished, "ordinary context closure must not claim handler completion"
            self.closed = True

        def submit(self, sequence, operation, payload):
            assert operation == "storage.read" and json.loads(payload) == {"key": "value"}
            self.submitted += 1
            reference()._reply(sequence, '{"ok":true,"value":{"present":true,"value":42}}')

    async def exercise():
        nonlocal reference
        bridge = Bridge()
        context = Context(bridge, None)
        reference = weakref.ref(context)
        storage = context.storage
        token = _current_invocation.set(bridge)
        try:
            assert (await storage.read_json("value")).value == 42
            other = _current_invocation.set(object())
            try:
                try:
                    await storage.read_json("value")
                except ContextMismatch:
                    pass
                else:
                    raise AssertionError("storage handle escaped invocation authority")
            finally:
                _current_invocation.reset(other)
            context._close()
            del context
            assert reference() is not None, "retained storage lost its context"
            try:
                await storage.put_json("late", 1)
            except SDKError as error:
                assert error.code == "ContextExpired"
            else:
                raise AssertionError("retained storage admitted an expired mutation")
            assert bridge.submitted == 1
            del storage
            assert reference() is None, "SDK storage cycle needs cyclic collection"
        finally:
            _current_invocation.reset(token)

    reference = None
    gc.disable()
    asyncio.run(exercise())


class RunnerScheduling(unittest.TestCase):
    def run_case(self, mode):
        child = subprocess.run(
            [sys.executable, "-I", "-B", __file__, "--continuous-arrivals", mode],
            capture_output=True, text=True, timeout=10,
        )
        self.assertEqual(child.returncode, 0, child.stdout + child.stderr)

    def test_capacity_rejection_yields_to_timers_and_cancellation(self):
        self.run_case("rejections")

    def test_immediate_handlers_yield_to_timers_and_cancellation(self):
        self.run_case("handlers")

    def test_immediate_completion_releases_context_before_next_admission(self):
        self.run_case("release-eager")

    def test_storage_access_does_not_retain_completed_context(self):
        self.run_case("release-storage")

    def test_retained_storage_preserves_authority_and_expires(self):
        self.run_case("storage-lifetime")

    def test_cleanup_cancels_pending_work_before_publishing_completion(self):
        self.run_case("pending-cleanup")

    def test_failed_eager_cleanup_remains_supervised(self):
        self.run_case("cleanup-failure")

    def test_inline_blocking_handler_remains_visible(self):
        self.run_case("blocking")

    def test_blocking_after_first_yield_remains_visible(self):
        self.run_case("blocking-yield")

    def test_admission_counts_tasks_created_outside_the_factory(self):
        self.run_case("many-tasks")


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "--continuous-arrivals":
        continuous_arrivals(sys.argv[2])
    else:
        unittest.main()
