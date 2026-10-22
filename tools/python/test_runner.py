"""Pump scheduling tests; native authority and I/O belong to the bridge suite."""
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
        return Response(b"ok")

    class Bridge:
        def __init__(self, inbox):
            self.inbox = inbox
            self.closed = False
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

        def expire(self):
            if not self.closed:
                self.closed = True
                self.inbox.expired += 1

    class Inbox:
        def __init__(self):
            self.reader, self.writer = socket.socketpair()
            self.reader.setblocking(False)
            self.writer.setblocking(False)
            self.stopped = False
            self.issued = self.rejected = self.completed = self.expired = 0
            self.observed = None
            self.joined = 0
            self.bridges = weakref.WeakSet()

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
                gc.collect()
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
    try:
        capacity = 902 if mode == "many-tasks" else 2 if mode == "rejections" else 3
        report = runner.run(SimpleNamespace(fetch=fetch), inbox, 30, .2, capacity, 16, 30)
        assert not report["runtime_failed"], report
        assert report["pending_tasks"] == report["pending_objects"] == 0, report
        assert report["peak_tasks"] <= capacity, report
        assert inbox.issued == inbox.expired
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
