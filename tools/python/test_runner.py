"""Pump/context lifetimes; native authority and I/O belong to the bridge suite."""
import asyncio
import contextvars
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


def idle_housekeeping(interval, telemetry):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "crates/cellar-python/python"))
    import runner

    class Inbox:
        def __init__(self):
            self.reader, self.writer = socket.socketpair()
            self.reader.setblocking(False)
            self.writer.setblocking(False)
            self.stopped = self.watchdog_fired = False
            self.samples = self.pumps = 0

        def wakeup_fd(self):
            def deadline():
                self.watchdog_fired = True
                self.stop()
            asyncio.get_running_loop().call_later(1, deadline)
            return self.reader.fileno()

        def drain_wakeup(self):
            while True:
                try:
                    self.reader.recv(4096)
                except BlockingIOError:
                    return

        def take(self):
            self.pumps += 1
            return None

        def replies(self):
            return []

        def take_lifecycle(self):
            return 0

        def pump_needed(self):
            return False

        def sample(self, payload):
            json.loads(payload)
            if not self.stopped:
                self.samples += 1
                if self.samples == 3:
                    self.stop()

        def done(self):
            return self.stopped

        def stop(self):
            if not self.stopped:
                self.stopped = True
                self.writer.send(b"x")

    inbox = Inbox()
    try:
        report = runner.run(SimpleNamespace(), inbox, interval, .2, 16, 16, telemetry)
        assert not report["runtime_failed"] and not inbox.watchdog_fired, report
        assert inbox.samples == 3 and inbox.pumps >= 3, "idle pump stopped housekeeping or sampling"
        assert report["pending_tasks"] == report["pending_objects"] == report["pending_threads"] == 0, report
    finally:
        inbox.reader.close()
        inbox.writer.close()


def continuous_arrivals(mode):
    if mode == "resident-bridge":
        resident_bridge_lifetime()
        return
    if mode.startswith("idle-housekeeping-"):
        interval, telemetry = (.003, .012) if mode.endswith("short-poll") else (.03, .005)
        idle_housekeeping(interval, telemetry)
        return
    if mode.startswith("invalidate-"):
        invalidation_without_reply(mode.removeprefix("invalidate-"))
        return
    if mode == "late-admission":
        idle_invocation_scans(0, late=True)
        return
    if mode.startswith("idle-scans-"):
        idle_invocation_scans(int(mode.removeprefix("idle-scans-")))
        return
    if mode.startswith("reply-"):
        reply_delivery(mode.removeprefix("reply-"))
        return
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
            self.housekeeping_timers = []

        async def occupy_task_slot(self):
            try:
                await asyncio.Event().wait()
            finally:
                self.joined += 1

        def wakeup_fd(self):
            loop = asyncio.get_running_loop()
            if mode == "reused-timer":
                original = loop.call_later
                def observed_timer(delay, callback, *args, **kwargs):
                    timer = original(delay, callback, *args, **kwargs)
                    if callback.__name__ == "deadline_ready":
                        self.housekeeping_timers.append(timer)
                    return timer
                loop.call_later = observed_timer
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

        def pump_needed(self):
            return False

        def take_lifecycle(self):
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
        if mode == "reused-timer":
            assert len(inbox.housekeeping_timers) == 1, f"housekeeping timer allocations: {len(inbox.housekeeping_timers)}"
            assert inbox.housekeeping_timers[0].cancelled(), "shutdown retained the housekeeping timer"
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


def reply_delivery(mode):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "crates/cellar-python/python"))
    import runner
    from cellar import Request, Response, _current_invocation
    binary = mode.startswith("binary-")
    mode = mode.removeprefix("binary-")
    state = dict(submitted=0, resumed=0, completed=0, cleaned=0, expired=0)
    bridges = weakref.WeakSet()
    payload = json.dumps(dict(identity=None, object=None, kind='fetch',
                             event=dict(url='http://reply/', method='GET', headers=[])))

    async def fetch(request, context):
        try:
            if binary:
                value = await context.namespace('CELLS').get_by_name('one').fetch(
                    Request('http://cell/', 'POST', body=b'\x00\xff'))
                assert value.body == b'\x00\xff' and value.status == 201
                assert value.headers == [['x-result', 'accepted']]
                state['resumed'] += 1
                return value
            value = await context.var('value')
            state['resumed'] += 1
            assert value == '42'
            return Response.json(dict(value=value))
        finally:
            state['cleaned'] += 1
            if mode == 'expired':
                inbox.stop()

    class Bridge:
        closed = False

        def __init__(self):
            bridges.add(self)

        def active(self):
            return not self.closed

        def submit(self, sequence, operation, encoded, body=None):
            assert _current_invocation.get() is self
            if binary:
                assert operation == 'cell.fetch' and body == b'\x00\xff'
                assert json.loads(encoded) == dict(binding='CELLS', name='one', request=dict(
                    url='http://cell/', method='POST', headers=[], body=''))
            else:
                assert operation == 'var' and json.loads(encoded) == {'name': 'value'}
                assert body is None
            state['submitted'] += 1

            def publish():
                if mode == 'expired':
                    self.expire()
                elif mode == 'stopped':
                    inbox.stopped = True
                value = dict(status=201, headers=[['x-result', 'accepted']], body='') if binary else '42'
                reply = 'PRIVATE_SENTINEL_invalid_json' if mode == 'malformed' else (dict(ok=True, value=value) if mode == 'owned' else json.dumps(dict(ok=True, value=value)))
                output = b'\x00\xff' if binary else None
                inbox.queue.append((1, sequence, reply, output))
                if mode == 'duplicate':
                    inbox.queue.append((1, sequence, reply, output))
                inbox.writer.send(b'x')
            # Publish only after the eager handler has suspended on its SDK
            # future and the pump has registered the invocation.
            asyncio.get_running_loop().call_soon(publish)

        def complete(self, encoded, failed, body=None):
            assert _current_invocation.get() is self and self.active()
            assert not failed, encoded
            assert json.loads(encoded)['status'] == (201 if binary else 200)
            if binary:
                assert body == b'\x00\xff'
            else:
                assert json.loads(body) == {'value': '42'}
            state['completed'] += 1
            inbox.stop()

        def expire(self, finished=False):
            if not self.closed:
                state['expired'] += 1
                self.closed = True

    class Inbox:
        def __init__(self):
            self.reader, self.writer = socket.socketpair()
            self.reader.setblocking(False)
            self.writer.setblocking(False)
            self.queue = []
            self.stopped = self.issued = False

        def wakeup_fd(self):
            return self.reader.fileno()

        def drain_wakeup(self):
            while True:
                try:
                    self.reader.recv(4096)
                except BlockingIOError:
                    return

        def take(self):
            if self.issued or self.stopped:
                return None
            self.issued = True
            return 1, payload, b'', Bridge()

        def replies(self):
            replies, self.queue = self.queue, []
            return replies

        def done(self):
            return self.stopped

        def pump_needed(self):
            return False

        def take_lifecycle(self):
            return False

        def sample(self, value):
            json.loads(value)

        def stop(self):
            if not self.stopped:
                self.stopped = True
                self.writer.send(b'x')

    inbox = Inbox()
    try:
        report = runner.run(SimpleNamespace(fetch=fetch), inbox, 30, .2, 10, 16, 30)
        expected = int(mode in ('normal', 'duplicate', 'owned'))
        assert state == dict(submitted=1, resumed=expected, completed=expected, cleaned=1, expired=1), state
        assert report['runtime_failed'] == (mode == 'malformed'), report
        assert report['pending_tasks'] == report['pending_objects'] == report['pending_threads'] == 0, report
        assert report['interpreter']['handlers_completed'] == expected, report
        gc.collect()
        assert not bridges, 'runner retains finished invocation bridge'
        print(json.dumps(dict(mode=mode, state=state, stage=report['runtime_failure_stage'])))
    finally:
        inbox.reader.close()
        inbox.writer.close()


def invalidation_without_reply(mode):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "crates/cellar-python/python"))
    import runner
    from cellar import Response
    state = dict(cancelled=0, retired=0)

    class Owner:
        def active(self):
            return inbox.owner

    class Cell:
        async def fetch(self, request, context):
            asyncio.get_running_loop().call_soon(inbox.invalidate)
            try:
                await asyncio.Event().wait()
            finally:
                state["cancelled"] += 1
                asyncio.get_running_loop().call_later(.01, inbox.stop)
            return Response(b"unreachable")

        def __del__(self):
            state["retired"] += 1

    class Bridge:
        def active(self):
            return inbox.active

        def owner_active(self):
            return inbox.owner

        def owner_lifetime(self):
            return Owner()

        def expire(self, finished=False):
            inbox.active = False

        def complete(self, *args):
            raise AssertionError("revoked invocation completed")

    class Inbox:
        def __init__(self):
            self.reader, self.writer = socket.socketpair()
            self.reader.setblocking(False)
            self.active = self.owner = True
            self.stopped = self.issued = False
            self.lifecycle = 0

        def wakeup_fd(self):
            return self.reader.fileno()

        def drain_wakeup(self):
            try:
                while self.reader.recv(4096):
                    pass
            except BlockingIOError:
                pass

        def invalidate(self):
            self.active = False
            self.lifecycle = 1
            if mode == "owner":
                self.owner = False
                self.lifecycle |= 2
            self.writer.send(b"x")

        def pump_needed(self):
            return bool(self.lifecycle)

        def take_lifecycle(self):
            flags, self.lifecycle = self.lifecycle, 0
            return flags

        def take(self):
            if self.issued:
                return None
            self.issued = True
            payload = dict(identity=None, object="cell", **{"class": "Cell"}, kind="fetch",
                           event=dict(url="http://cancel/", method="GET", headers=[]))
            return 1, json.dumps(payload), b"", Bridge()

        def replies(self):
            return []

        def done(self):
            return self.stopped

        def sample(self, payload):
            json.loads(payload)

        def stop(self):
            if not self.stopped:
                assert state["cancelled"] == 1
                if mode == "owner":
                    assert state["retired"] == 1, "owner object retained until shutdown"
                self.stopped = True
                self.writer.send(b"x")

    inbox = Inbox()
    try:
        # No reply or periodic poll can rescue lost invalidation: the parent
        # kills this subprocess after ten seconds, before the 30-second poll.
        report = runner.run(SimpleNamespace(Cell=Cell), inbox, 30, .2, 10, 16, 30)
        assert not report["runtime_failed"], report
        assert report["pending_tasks"] == report["pending_objects"] == 0, report
        assert report["interpreter"]["handlers_cancelled"] == 1, report
        assert state == dict(cancelled=1, retired=1), state
    finally:
        inbox.reader.close()
        inbox.writer.close()



def resident_bridge_lifetime():
    import contextvars
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "crates/cellar-python/python"))
    import runner
    from cellar import Response
    bridges, cells = weakref.WeakSet(), weakref.WeakSet()
    observations = []

    class Owner:
        live = True

        def active(self):
            return self.live

    owner = Owner()

    class Cell:
        def __init__(self):
            cells.add(self)

        async def fetch(self, request, context):
            return Response(b"ok")

    class Bridge:
        def __init__(self):
            self.live = True
            bridges.add(self)

        def active(self):
            return self.live and owner.active()

        def owner_active(self):
            return owner.active()

        def owner_lifetime(self):
            return owner

        def expire(self, finished=False):
            self.live = False

        def complete(self, payload, failed, body=None):
            assert not failed and body == b"ok"
            # An application callback must not itself retain the invocation
            # through the copied current_invocation ContextVar.
            asyncio.get_running_loop().call_later(.01, inbox.observe, context=contextvars.Context())

    class Inbox:
        def __init__(self):
            self.reader, self.writer = socket.socketpair()
            self.reader.setblocking(False)
            self.stopped = self.issued = False
            self.lifecycle = 0

        def wakeup_fd(self):
            return self.reader.fileno()

        def drain_wakeup(self):
            try:
                while self.reader.recv(4096):
                    pass
            except BlockingIOError:
                pass

        def pump_needed(self):
            return bool(self.lifecycle)

        def take_lifecycle(self):
            flags, self.lifecycle = self.lifecycle, 0
            return flags

        def take(self):
            if self.issued:
                return None
            self.issued = True
            payload = dict(identity=None, object="cell", **{"class": "Cell"}, kind="fetch",
                           event=dict(url="http://resident/", method="GET", headers=[]))
            return 1, json.dumps(payload), b"", Bridge()

        def replies(self):
            return []

        def sample(self, payload):
            json.loads(payload)

        def done(self):
            return self.stopped

        def observe(self):
            observations.append((len(bridges), len(cells)))
            if owner.live:
                owner.live = False
                self.lifecycle = 3
                self.writer.send(b"x")
                asyncio.get_running_loop().call_later(.01, self.observe)
            else:
                self.stop()

        def stop(self):
            self.stopped = True
            self.writer.send(b"x")

    inbox = Inbox()
    gc.disable()
    try:
        report = runner.run(SimpleNamespace(Cell=Cell), inbox, 30, .2, 10, 16, 30)
        assert not report["runtime_failed"], report
        assert observations == [(0, 1), (0, 0)], f"resident cell retained completed bridge or ignored retirement: {observations}"
        assert report["pending_tasks"] == report["pending_objects"] == report["pending_threads"] == 0, report
    finally:
        inbox.reader.close()
        inbox.writer.close()


def idle_invocation_scans(cold, late=False):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'crates/cellar-python/python'))
    import runner
    from cellar import Response, _current_invocation
    rounds = 100
    state = dict(issued=0, submitted=0, resumed=0, completed=0, cold_joined=0, cold_checks=0, admission_checks=0, measuring=False, late_completed=0)
    bridges = weakref.WeakSet()

    async def fetch(request, ctx):
        if request.path == '/cold':
            try:
                await asyncio.Event().wait()
            finally:
                state['cold_joined'] += 1
        elif request.path == '/late':
            return Response(b'ok')
        else:
            for index in range(rounds):
                assert await ctx.var('value') == '42'
                if late and index >= 2:
                    assert state['late_completed'] == 1, 'new admission waited for a completed task or housekeeping timer'
                state['resumed'] += 1
            return Response(b'ok')

    class Bridge:

        def __init__(self, number):
            self.number = number
            self.closed = False
            bridges.add(self)

        def active(self):
            if self.number <= cold and state['measuring']:
                state['cold_checks'] += 1
            return not self.closed

        def submit(self, sequence, operation, encoded):
            assert self.number == cold + 1 and _current_invocation.get() is self
            assert operation == 'var' and json.loads(encoded) == dict(name='value')
            state['measuring'] = True
            state['submitted'] += 1

            def publish():
                inbox.queue.append((self.number, sequence, '{"ok":true,"value":"42"}', None))
                inbox.writer.send(b'x')
            asyncio.get_running_loop().call_soon(publish)

        def complete(self, encoded, failed, body=None):
            assert not failed and body == b'ok'
            if late and self.number == cold + 2:
                state['late_completed'] += 1
                return
            assert self.number == cold + 1
            state['completed'] += 1
            state['measuring'] = False
            # Allow the completion callback and pump to run while unrelated
            # invocations remain suspended. No cyclic GC may hide retention.
            asyncio.get_running_loop().call_later(.01, inbox.check_released, context=contextvars.Context())

        def expire(self, finished=False):
            self.closed = True

    class Inbox:

        def __init__(self):
            self.reader, self.writer = socket.socketpair()
            self.reader.setblocking(False)
            self.writer.setblocking(False)
            self.queue = []
            self.stopped = False
            self.deferred = self.deferred_issued = False
            self.lifecycle = 0

        def wakeup_fd(self):
            return self.reader.fileno()

        def drain_wakeup(self):
            while True:
                try:
                    self.reader.recv(4096)
                except BlockingIOError:
                    return

        def take(self):
            if state['measuring']:
                state['admission_checks'] += 1
            if late and self.deferred and not self.deferred_issued:
                self.deferred_issued = True
                payload = json.dumps(dict(identity=None, object=None, kind='fetch', event=dict(url='http://scan/late', method='GET', headers=[])))
                return (cold + 2, payload, b'', Bridge(cold + 2))
            if self.stopped or state['issued'] == cold + 1:
                if late and not self.deferred:
                    # Publish immediately after this queue read found no work,
                    # before the pump can consume any later hints.
                    self.deferred = True
                    self.lifecycle |= 4
                    self.writer.send(b'x')
                return None
            state['issued'] += 1
            number = state['issued']
            path = '/cold' if number <= cold else '/hot'
            payload = json.dumps(dict(identity=None, object=None, kind='fetch', event=dict(url='http://scan' + path, method='GET', headers=[])))
            return (number, payload, b'', Bridge(number))

        def replies(self):
            queue, self.queue = (self.queue, [])
            return queue

        def pump_needed(self):
            return bool(self.lifecycle)

        def take_lifecycle(self):
            flags, self.lifecycle = self.lifecycle, 0
            return flags

        def check_released(self):
            assert len(bridges) == cold, "completed invocation retained by live pump"
            assert all(bridge.number <= cold for bridge in bridges)
            self.stop()

        def sample(self, value):
            json.loads(value)

        def done(self):
            return self.stopped

        def stop(self):
            if not self.stopped:
                self.stopped = True
                self.writer.send(b'x')
    gc.disable()
    inbox = Inbox()
    try:
        report = runner.run(SimpleNamespace(fetch=fetch), inbox, 30, 0.2, 512, 16, 30)
        assert not report['runtime_failed'], report
        assert state['submitted'] == state['resumed'] == rounds and state['completed'] == 1
        assert state['cold_joined'] == cold
        assert report['pending_tasks'] == report['pending_objects'] == report['pending_threads'] == 0
        assert not bridges, 'pump retained invocation bridges'
        assert state['cold_checks'] == 0, f'idle invocation scans: {state}'
        assert state['admission_checks'] <= (3 if late else 1), f'reply-only notifications woke admission pump: {state}'
    finally:
        inbox.reader.close()
        inbox.writer.close()


class RunnerScheduling(unittest.TestCase):
    def run_case(self, mode):
        child = subprocess.run(
            [sys.executable, "-I", "-B", __file__, "--continuous-arrivals", mode],
            capture_output=True, text=True, timeout=10,
        )
        self.assertNotIn("PRIVATE_SENTINEL", child.stderr)
        self.assertEqual(child.returncode, 0, child.stdout + child.stderr)

    def test_resident_cell_releases_completed_bridge_before_retirement(self):
        self.run_case("resident-bridge")

    def test_invalidation_without_reply_cancels_and_retires(self):
        for mode in ("invocation", "owner"):
            with self.subTest(mode=mode):
                self.run_case("invalidate-" + mode)

    def test_idle_invocations_are_not_scanned_for_ordinary_replies(self):
        for count in (0, 16, 64, 127):
            with self.subTest(idle=count):
                self.run_case("idle-scans-" + str(count))

    def test_admission_published_after_empty_queue_does_not_wait_for_housekeeping(self):
        self.run_case("late-admission")

    def test_binary_reply_lifecycle(self):
        for mode in ("normal", "owned", "duplicate", "expired", "stopped", "malformed"):
            with self.subTest(mode=mode):
                self.run_case("reply-binary-" + mode)

    def test_sdk_reply_resumes_original_invocation(self):
        self.run_case("reply-normal")

    def test_duplicate_reply_completes_only_once(self):
        self.run_case("reply-duplicate")

    def test_reply_after_expiration_does_not_resume_handler(self):
        self.run_case("reply-expired")

    def test_reply_after_stop_does_not_resume_handler(self):
        self.run_case("reply-stopped")

    def test_malformed_reply_fails_safely_and_joins(self):
        self.run_case("reply-malformed")

    def test_capacity_rejection_yields_to_timers_and_cancellation(self):
        self.run_case("rejections")

    def test_idle_housekeeping_and_telemetry_rearm(self):
        for cadence in ("short-poll", "short-telemetry"):
            with self.subTest(cadence=cadence):
                self.run_case("idle-housekeeping-" + cadence)

    def test_housekeeping_timer_survives_ordinary_pump_wakeups(self):
        self.run_case("reused-timer")

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
