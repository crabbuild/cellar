# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Real PydanticAI, with a transactional storage double for failure injection."""
import asyncio
from copy import deepcopy
import json
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "crates/cellar-python/python"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import app
from cellar import Conflict, Request, Snapshot, ValidationError as CellarValidationError


class Storage:
    def __init__(self):
        self.state = None
        self.alarm = None
        self.fail = False
        self.lose_ack = False

    async def read_json(self, key):
        return Snapshot(present=self.state is not None, value=deepcopy(self.state))

    async def sync(self):
        pass

    async def apply(self, *, checks, puts, alarm_at_ms):
        if self.fail:
            self.fail = False
            raise RuntimeError("injected before commit")
        if checks["agent"].value != self.state:
            raise Conflict("changed")
        self.state = deepcopy(puts["agent"])
        self.alarm = alarm_at_ms
        if self.lose_ack:
            self.lose_ack = False
            raise RuntimeError("injected after commit; outcome unknown")


class Context:
    def __init__(self):
        self.storage = Storage()
        self.identity = SimpleNamespace(name="demo")
        self.now = 100000
        self.fs = SimpleNamespace(capabilities=self.capabilities)

    async def capabilities(self):
        return {"transport": {"max_value_bytes": 2 * 1024 * 1024}}

    async def now_ms(self):
        return self.now


class DurableAgentTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.ctx = Context()
        self.agent = app.DurableAgent()
        self.start = Request("http://test/agents/demo", "POST", b'{"prompt":"remember coffee"}')
        with patch.dict(app.os.environ, {"CELLAR_AGENT_MODEL": "demo"}):
            self.assertEqual((await self.agent.fetch(self.start, self.ctx)).status, 202)

    async def tick(self, delivery=42):
        self.ctx.now = self.ctx.storage.alarm
        with patch.dict(app.os.environ, {"CELLAR_AGENT_MODEL": "demo"}):
            # Each delivery recreates the Python cell object: no in-memory state.
            await app.DurableAgent().alarm(delivery, self.ctx)

    async def test_checkpointed_tool_resume_and_replayed_creation(self):
        await self.tick()
        self.assertEqual(self.ctx.storage.state["phase"], "tools")
        self.assertEqual(self.ctx.storage.state["notes"], {})
        self.assertTrue(self.ctx.storage.state["history"])
        await self.tick(delivery=99999)
        self.assertEqual(self.ctx.storage.state["notes"], {"demo-note": {"text": "remember coffee"}})
        await self.tick()
        final = deepcopy(self.ctx.storage.state)
        self.assertEqual(final["phase"], "complete")
        self.assertEqual(final["answer"], "Saved note: remember coffee")
        self.assertEqual(final["steps"], 3)
        self.assertIsNone(self.ctx.storage.alarm)
        await self.agent.alarm(123456, self.ctx)
        self.assertEqual((await self.agent.fetch(self.start, self.ctx)).status, 202)
        self.assertEqual(self.ctx.storage.state, final)
        other = Request(self.start.url, "POST", b'{"prompt":"different"}')
        self.assertEqual((await self.agent.fetch(other, self.ctx)).status, 409)

    async def test_atomic_tool_failure_then_redelivery(self):
        await self.tick()
        saved = deepcopy(self.ctx.storage.state)
        self.ctx.storage.fail = True
        with self.assertRaisesRegex(RuntimeError, "injected"):
            await self.tick()
        self.assertEqual(self.ctx.storage.state, saved)
        await self.tick()
        # Re-delivery before the next business phase is due does no extra work.
        after = deepcopy(self.ctx.storage.state)
        await self.agent.alarm(987, self.ctx)
        self.assertEqual(self.ctx.storage.state, after)
        self.assertEqual(len(after["notes"]), 1)

    async def test_lost_creation_ack_reconciles_same_request(self):
        self.ctx.storage = Storage()
        self.ctx.storage.lose_ack = True
        with patch.dict(app.os.environ, {"CELLAR_AGENT_MODEL": "demo"}):
            with self.assertRaisesRegex(RuntimeError, "outcome unknown"):
                await self.agent.fetch(self.start, self.ctx)
        accepted = deepcopy(self.ctx.storage.state)
        wakeup = self.ctx.storage.alarm
        response = await app.DurableAgent().fetch(self.start, self.ctx)
        self.assertEqual(response.status, 202)
        self.assertEqual(json.loads(response.body), {"accepted": True, "agent": "demo"})
        self.assertEqual(self.ctx.storage.state, accepted)
        self.assertEqual(self.ctx.storage.alarm, wakeup)
        for _ in range(3):
            await self.tick()
        self.assertEqual(self.ctx.storage.state["phase"], "complete")

    async def test_lost_model_checkpoint_ack_does_not_repeat_model(self):
        original = app.model_step

        async def lose_ack_after_model(state):
            result = await original(state)
            self.ctx.storage.lose_ack = True
            return result

        with patch.object(app, "model_step", side_effect=lose_ack_after_model) as model:
            with self.assertRaisesRegex(RuntimeError, "outcome unknown"):
                await self.tick()
            self.assertEqual(self.ctx.storage.state["phase"], "tools")
            self.assertEqual(self.ctx.storage.state["attempts"], 1)
            # A recreated cell reloads the committed tool intent, rather than
            # repeating the model request whose checkpoint lost its response.
            await self.tick()
            self.assertEqual(model.await_count, 1)
        await self.tick()
        self.assertEqual(self.ctx.storage.state["answer"], "Saved note: remember coffee")
        self.assertEqual(self.ctx.storage.state["attempts"], 2)

    async def test_lost_tool_checkpoint_ack_preserves_effect_and_receipt(self):
        await self.tick()
        self.ctx.storage.lose_ack = True
        with self.assertRaisesRegex(RuntimeError, "outcome unknown"):
            await self.tick()
        committed = deepcopy(self.ctx.storage.state)
        self.assertEqual(committed["phase"], "model")
        self.assertEqual(committed["notes"], {"demo-note": {"text": "remember coffee"}})
        self.assertEqual(committed["results"], committed["notes"])
        await app.DurableAgent().alarm(123, self.ctx)
        self.assertEqual(self.ctx.storage.state, committed)
        await self.tick()
        self.assertEqual(self.ctx.storage.state["phase"], "complete")
        self.assertEqual(self.ctx.storage.state["notes"], committed["notes"])
        self.assertEqual(self.ctx.storage.state["steps"], 3)
        self.assertIsNone(self.ctx.storage.alarm)

    async def test_cancellation_fences_inflight_model(self):
        entered, release = asyncio.Event(), asyncio.Event()
        original = app.model_step

        async def blocked(state):
            entered.set()
            await release.wait()
            return await original(state)

        with patch.object(app, "model_step", blocked):
            task = asyncio.create_task(self.tick())
            await entered.wait()
            result = await self.agent.fetch(Request(self.start.url, "DELETE"), self.ctx)
            self.assertEqual(json.loads(result.body)["phase"], "cancelled")
            release.set()
            with self.assertRaises(Conflict):
                await task
        self.assertEqual(self.ctx.storage.state["phase"], "cancelled")
        self.assertEqual(self.ctx.storage.state["notes"], {})
        self.assertIsNone(self.ctx.storage.alarm)

    async def test_interrupted_attempt_is_charged_and_recovers(self):
        async def interrupted(state):
            raise asyncio.CancelledError

        with patch.object(app, "model_step", interrupted):
            with self.assertRaises(asyncio.CancelledError):
                await self.tick()
        self.assertEqual(self.ctx.storage.state["attempts"], 1)
        self.assertIsNotNone(self.ctx.storage.alarm)
        await self.tick()
        self.assertEqual(self.ctx.storage.state["attempts"], 2)
        self.assertEqual(self.ctx.storage.state["phase"], "tools")

    async def test_model_waits_for_durable_intent_proof(self):
        async def unavailable():
            raise RuntimeError("proof unavailable")

        with patch.object(self.ctx.storage, "sync", unavailable), patch.object(app, "model_step") as model:
            with self.assertRaisesRegex(RuntimeError, "proof unavailable"):
                await self.tick()
            model.assert_not_called()
        self.assertEqual(self.ctx.storage.state["attempts"], 1)
        await self.tick()
        self.assertEqual(self.ctx.storage.state["phase"], "tools")

    async def test_changed_tool_configuration_refuses_pending_work(self):
        self.ctx.storage.state["configuration"] = "different release"
        saved = deepcopy(self.ctx.storage.state)
        with self.assertRaisesRegex(RuntimeError, "configuration changed"):
            await self.tick()
        self.assertEqual(self.ctx.storage.state, saved)

    async def test_small_transport_reserves_a_terminal_checkpoint(self):
        async def capabilities():
            return {"transport": {"max_value_bytes": 8192, "max_body_bytes": 1}}

        self.ctx.fs.capabilities = capabilities
        snapshot = await self.ctx.storage.read_json("agent")
        state = deepcopy(snapshot.value)
        state["padding"] = "x" * 2500
        await app.checkpoint(self.ctx, snapshot, state)
        saved = deepcopy(self.ctx.storage.state)
        current = await self.ctx.storage.read_json("agent")
        for padding in ("x" * 4000, "\U0001d11e" * 600):
            candidate = {**saved, "padding": padding}
            with self.assertRaises(CellarValidationError):
                await app.checkpoint(self.ctx, current, candidate)
            self.assertEqual(self.ctx.storage.state, saved)
        await app.checkpoint(self.ctx, current, app.finish(deepcopy(saved), "failed", "checkpoint_limit"))
        self.assertEqual(self.ctx.storage.state["phase"], "failed")
        self.assertEqual(self.ctx.storage.state["padding"], "x" * 2500)
        self.assertIsNone(self.ctx.storage.alarm)

    async def test_full_tool_journal_records_terminal_failure_without_losing_decisions(self):
        await self.tick()

        async def capabilities():
            return {"transport": {"max_value_bytes": 8192}}

        self.ctx.fs.capabilities = capabilities
        snapshot = await self.ctx.storage.read_json("agent")
        state = deepcopy(snapshot.value)
        state["calls"] = [{"id": "large-read", "name": "read_file", "args": {"path": "/large"}}]
        state["padding"] = ""
        state["padding"] = "x" * (3480 - len(app.fingerprint(state).encode()))
        await app.checkpoint(self.ctx, snapshot, state)
        saved = deepcopy(self.ctx.storage.state)

        async def prepare(_request):
            return dict(result={"text": "x" * 8000}, filesystem_checks=[], filesystem_mutations=[])

        with patch.object(app, "FilesystemTools", return_value=SimpleNamespace(prepare=prepare)):
            await self.tick()
        final = self.ctx.storage.state
        self.assertEqual(final["phase"], "failed")
        self.assertEqual(final["error"], "checkpoint_limit")
        self.assertEqual(final["history"], saved["history"])
        self.assertEqual(final["calls"], saved["calls"])
        self.assertEqual(final["filesystem_receipts"], saved["filesystem_receipts"])
        self.assertEqual(final["notes"], {})
        self.assertIsNone(self.ctx.storage.alarm)

    async def test_provider_failure_is_sanitized_and_bounded(self):
        async def failed(state):
            raise ValueError("SECRET-provider-key")

        with patch.object(app, "model_step", failed):
            for _ in range(app.MAX_ATTEMPTS):
                await self.tick()
        self.assertEqual(self.ctx.storage.state["phase"], "failed")
        self.assertEqual(self.ctx.storage.state["error"], "model_budget_exhausted")
        self.assertNotIn("SECRET", json.dumps(self.ctx.storage.state))
        self.assertIsNone(self.ctx.storage.alarm)

    async def test_invalid_multi_tool_batch_has_no_partial_effect(self):
        await self.tick()
        self.ctx.storage.state["calls"].append({"id": "bad", "name": "shell", "args": {}})
        await self.tick()
        self.assertEqual(self.ctx.storage.state["phase"], "failed")
        self.assertEqual(self.ctx.storage.state["notes"], {})

    async def test_openai_provider_uses_saved_tools_and_closes_clients(self):
        import httpx2
        import openai
        original = openai.AsyncOpenAI
        clients, requests = [], []

        async def respond(request):
            body = json.loads(request.content)
            requests.append(body)
            self.assertEqual(body["model"], "test-model")
            if len(requests) == 1:
                message = {"role": "assistant", "content": None, "tool_calls": [{
                    "id": "provider-note", "type": "function", "function": {
                        "name": "save_note", "arguments": '{"text":"provider note"}'}}]}
                reason = "tool_calls"
            else:
                self.assertTrue(any(m["role"] == "tool" for m in body["messages"]))
                message = {"role": "assistant", "content": "Saved your note."}
                reason = "stop"
            return httpx2.Response(200, json={
                "id": "chat-test", "object": "chat.completion", "created": 1,
                "model": "test-model", "choices": [{"index": 0, "message": message,
                                                       "finish_reason": reason}],
                "usage": {"prompt_tokens": 10, "completion_tokens": 10, "total_tokens": 20}})

        def client(**kwargs):
            value = original(**kwargs, http_client=httpx2.AsyncClient(transport=httpx2.MockTransport(respond)))
            clients.append(value)
            return value

        self.ctx.storage.state["model"] = "test-model"
        self.ctx.storage.state["configuration"] = app.configuration("test-model")
        with patch.dict(app.os.environ, {"OPENAI_API_KEY": "local-test"}), patch.object(openai, "AsyncOpenAI", client):
            for _ in range(3):
                await self.tick()
        self.assertEqual(self.ctx.storage.state["phase"], "complete")
        self.assertEqual(self.ctx.storage.state["notes"], {"provider-note": {"text": "provider note"}})
        self.assertEqual(len(requests), 2)
        self.assertTrue(all(value.is_closed() for value in clients))
        if "t" in sys.abiflags:
            self.assertFalse(sys._is_gil_enabled(), "provider import enabled the GIL")

    async def test_invalid_body_and_early_alarm(self):
        saved = deepcopy(self.ctx.storage.state)
        for body in (b'[]', b'{}', b'{"prompt":"x","step_delay_ms":true}', b'not-json'):
            result = await self.agent.fetch(Request(self.start.url, "POST", body), self.ctx)
            self.assertEqual(result.status, 422)
        await self.agent.alarm(0, self.ctx)
        self.assertEqual(self.ctx.storage.state, saved)


if __name__ == "__main__":
    unittest.main()
