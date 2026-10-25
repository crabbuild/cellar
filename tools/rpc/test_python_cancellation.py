# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Task cancellation owns the native acknowledgement, even under repeated cancel."""
import asyncio
import gc
import json
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "crates/cellar-python/python"))
from cellar import Context, _current_invocation


class Bridge:
    def __init__(self):
        self.live = True
        self.submissions = []
        self.cancellations = []
        self.acknowledge = None

    def active(self):
        return self.live

    def submit(self, sequence, operation, payload, body=None):
        self.submissions.append((sequence, operation))

    def cancel_operation(self, sequence):
        self.cancellations.append(sequence)
        if self.acknowledge is not None:
            self.acknowledge()


class Cancellation(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.bridge = Bridge()
        self.context = Context(self.bridge, None)
        self.token = _current_invocation.set(self.bridge)
        self.loop_errors = []
        self.loop = asyncio.get_running_loop()
        self.previous_handler = self.loop.get_exception_handler()
        self.loop.set_exception_handler(lambda _, error: self.loop_errors.append(error))

    async def asyncTearDown(self):
        _current_invocation.reset(self.token)
        try:
            gc.collect()
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            self.assertEqual(self.context._pending, {})
            self.assertEqual(self.loop_errors, [])
        finally:
            self.loop.set_exception_handler(self.previous_handler)

    async def start_call(self, operation="cell.call"):
        task = asyncio.create_task(self.context._call(operation, _body=b""))
        await asyncio.sleep(0)
        self.assertEqual(len(self.bridge.submissions), 1)
        return task

    def reply(self, **fields):
        self.context._reply(1, json.dumps(fields))

    async def test_repeated_cancellation_joins_the_original_acknowledgement(self):
        task = await self.start_call()
        future = self.context._pending[1]
        task.cancel("stop")
        await asyncio.sleep(0)
        self.assertEqual(self.bridge.cancellations, [1])
        self.assertFalse(task.done())
        self.assertFalse(future.done())
        task.cancel("again")
        await asyncio.sleep(0)
        self.assertFalse(task.done())
        self.assertFalse(future.done())
        self.reply(ok=False, error="Cancelled", code="Cancelled", outcome="unknown")
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(self.bridge.cancellations, [1])

    async def test_already_ready_success_still_makes_no_rollback_claim(self):
        task = await self.start_call()
        self.reply(ok=True, value=None)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(self.bridge.cancellations, [1])

    async def test_already_ready_failure_is_consumed_during_cancellation(self):
        task = await self.start_call()
        self.reply(ok=False, error="Unavailable", code="Unavailable", outcome="unknown")
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        # A submitted failure must not survive as an unobserved Future error.
        await asyncio.sleep(0)

    async def test_retirement_before_cancellation_needs_no_missing_reply(self):
        task = await self.start_call()
        self.bridge.live = False
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(self.bridge.cancellations, [])

    async def test_retirement_racing_a_ready_failure_consumes_the_reply(self):
        task = await self.start_call()
        self.reply(ok=False, error="Unavailable", code="Unavailable", outcome="unknown")
        self.bridge.live = False
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

    async def test_retirement_while_joining_cancellation_releases_the_waiter(self):
        task = await self.start_call()
        task.cancel()
        await asyncio.sleep(0)
        self.assertFalse(task.done())
        self.bridge.live = False
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

    async def test_cancellation_before_the_first_poll_never_submits(self):
        task = asyncio.create_task(self.context._call("cell.call", _body=b""))
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(self.bridge.submissions, [])
        self.assertEqual(self.bridge.cancellations, [])

    async def test_existing_non_rpc_operations_keep_their_cancellation_policy(self):
        task = await self.start_call("storage.read")
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(self.bridge.cancellations, [])

    def acknowledge_cancellation(self):
        # The native acknowledgement arrives asynchronously, after cancellation
        # publication; the adapter must retain the original reply destination.
        self.loop.call_soon(lambda: self.reply(
            ok=False, error="Cancelled", code="Cancelled", outcome="unknown"))

    async def test_asyncio_timeout_waits_for_native_cleanup(self):
        self.bridge.acknowledge = self.acknowledge_cancellation
        with self.assertRaises(TimeoutError):
            async with asyncio.timeout(0):
                await self.context._call("cell.call", _body=b"")
        self.assertEqual(self.bridge.cancellations, [1])
        self.assertEqual(self.context._pending, {})

    async def test_task_group_cancellation_waits_for_native_cleanup(self):
        self.bridge.acknowledge = self.acknowledge_cancellation

        async def fail_sibling():
            await asyncio.sleep(0)
            raise ValueError("stop the task group")

        with self.assertRaises(ExceptionGroup) as caught:
            async with asyncio.TaskGroup() as group:
                group.create_task(self.context._call("cell.call", _body=b""))
                group.create_task(fail_sibling())
        self.assertEqual(len(caught.exception.exceptions), 1)
        self.assertIsInstance(caught.exception.exceptions[0], ValueError)
        self.assertEqual(self.bridge.cancellations, [1])
        self.assertEqual(self.context._pending, {})


if __name__ == "__main__":
    unittest.main()
