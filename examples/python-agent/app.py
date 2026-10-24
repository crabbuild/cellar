# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""PydanticAI tool calls checkpointed by Cellar storage and durable alarms."""
import asyncio
from copy import deepcopy
import json
import os
import re
import sys
from typing import Annotated

from cellar import Conflict, Response
from pydantic import BaseModel, ConfigDict, Field, ValidationError
from pydantic_ai import Agent, CallDeferred, DeferredToolRequests, DeferredToolResults
from pydantic_ai.messages import (
    ModelMessagesTypeAdapter, ModelResponse, TextPart, ToolCallPart,
    ToolReturnPart, UserPromptPart,
)
from pydantic_ai.models.function import FunctionModel
from pydantic_ai.usage import UsageLimits

MAX_ATTEMPTS = 8
MAX_STEPS = 16
MAX_HISTORY_BYTES = 128 * 1024
TERMINAL = {"complete", "failed", "cancelled"}
NAME = re.compile(r"[A-Za-z0-9_.-]{1,64}\Z")


class Start(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    prompt: str = Field(min_length=1, max_length=2000)
    step_delay_ms: int = Field(default=500, ge=100, le=30000)


class Note(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    text: str = Field(min_length=1, max_length=1000)


async def demo_model(messages, info):
    """Real PydanticAI FunctionModel, deterministic and offline (not an LLM)."""
    returns = [part for message in messages for part in message.parts
               if isinstance(part, ToolReturnPart)]
    if returns:
        return ModelResponse(parts=[TextPart("Saved note: " + returns[-1].content["text"])])
    prompt = next(part.content for message in messages for part in message.parts
                  if isinstance(part, UserPromptPart))
    return ModelResponse(parts=[ToolCallPart("save_note", {"text": prompt[:1000]},
                                            tool_call_id="demo-note")])


def make_agent(model):
    agent = Agent(model, output_type=[str, DeferredToolRequests], retries=0,
                  instructions="Save useful notes with save_note, then give a concise answer. "
                  "You have no external tools. Do not claim to have performed external actions.")

    @agent.tool_plain
    async def save_note(text: Annotated[str, Field(min_length=1, max_length=1000)]) -> str:
        """Save a note in this agent's durable notebook."""
        # No side effect here. Cellar commits the intent before executing it.
        raise CallDeferred

    return agent


async def model_step(state):
    history = ModelMessagesTypeAdapter.validate_python(state["history"])
    arguments = dict(
        message_history=history,
        deferred_tool_results=DeferredToolResults(calls=state["results"]) if state["results"] else None,
        usage_limits=UsageLimits(request_limit=2, output_tokens_limit=2048),
    )
    prompt = state["request"]["prompt"] if not history else None
    model = state["model"]
    async with asyncio.timeout(10):
        if model == "demo":
            return await make_agent(FunctionModel(demo_model)).run(prompt, **arguments)
        # Own and close the provider client inside this invocation. No global
        # client, hidden event loop, background worker or retained Cellar context.
        from openai import AsyncOpenAI
        from pydantic_ai.models.openai import OpenAIChatModel
        from pydantic_ai.providers.openai import OpenAIProvider
        async with AsyncOpenAI(api_key=os.environ["OPENAI_API_KEY"], max_retries=0, timeout=8) as client:
            provider = OpenAIProvider(openai_client=client)
            return await make_agent(OpenAIChatModel(model, provider=provider)).run(
                prompt, model_settings={"max_tokens": 2048}, **arguments)


def finish(state, phase, error=None):
    state.update(phase=phase, next_at=None, error=error)
    return state


async def checkpoint(ctx, snapshot, state):
    # The notebook, tool receipts, conversation, phase and next wake are one
    # checked transaction. A rejected or ambiguous write is never replayed here.
    await ctx.storage.apply(checks={"agent": snapshot}, puts={"agent": state},
                            alarm_at_ms=state["next_at"])


async def fetch(request, ctx):
    if request.path == "/health" and request.method == "GET":
        return Response.json({"framework": "pydantic-ai", "model": os.environ.get("CELLAR_AGENT_MODEL", "demo"),
                              "free_threaded": "t" in sys.abiflags, "gil_enabled": sys._is_gil_enabled()})
    parts = request.path.strip("/").split("/")
    if len(parts) != 2 or parts[0] != "agents" or not NAME.fullmatch(parts[1]):
        return Response.json({"error": "use /agents/NAME"}, 404)
    return await ctx.namespace("AGENTS").get_by_name(parts[1]).fetch(request)


class DurableAgent:
    async def fetch(self, request, ctx):
        snapshot = await ctx.storage.read_json("agent")
        if request.method == "GET":
            if not snapshot.present:
                return Response.json({"error": "not found"}, 404)
            state = snapshot.value
            return Response.json({key: state[key] for key in (
                "phase", "steps", "attempts", "notes", "answer", "error", "next_at")})
        if request.method == "POST":
            try:
                start = Start.model_validate_json(request.body).model_dump()
            except ValidationError:
                return Response.json({"error": "invalid prompt or step_delay_ms"}, 422)
            if snapshot.present:
                if snapshot.value["request"] != start:
                    return Response.json({"error": "name already used for a different request"}, 409)
            else:
                state = {"request": start, "model": os.environ.get("CELLAR_AGENT_MODEL", "demo"),
                         "phase": "model", "steps": 0, "attempts": 0,
                         "history": [], "calls": [], "results": {}, "notes": {},
                         "answer": None, "error": None,
                         "next_at": await ctx.now_ms() + start["step_delay_ms"]}
                try:
                    await checkpoint(ctx, snapshot, state)
                except Conflict:
                    return Response.json({"error": "concurrent creation; reconcile with GET"}, 409)
            # Stable acceptance receipt, even after completion or cancellation.
            return Response.json({"accepted": True, "agent": ctx.identity.name}, 202)
        if request.method == "DELETE":
            if not snapshot.present:
                return Response.json({"error": "not found"}, 404)
            state = deepcopy(snapshot.value)
            if state["phase"] not in TERMINAL:
                try:
                    await checkpoint(ctx, snapshot, finish(state, "cancelled"))
                except Conflict:
                    return Response.json({"error": "state changed; reconcile with GET"}, 409)
            return Response.json({"phase": state["phase"]})
        return Response.json({"error": "method not allowed"}, 405)

    async def alarm(self, scheduled_ms, ctx):
        # Native retries may change scheduled_ms. The durable phase is the
        # business occurrence; delivery timestamps never identify a tool effect.
        snapshot = await ctx.storage.read_json("agent")
        if not snapshot.present or snapshot.value["phase"] in TERMINAL:
            return
        state = deepcopy(snapshot.value)
        now = await ctx.now_ms()
        if now < state["next_at"]:
            await checkpoint(ctx, snapshot, state)
            return
        if state["steps"] >= MAX_STEPS:
            await checkpoint(ctx, snapshot, finish(state, "failed", "step_budget_exhausted"))
            return
        if state["phase"] == "tools":
            # Only this local tool is allowed. Validation precedes all writes.
            results = {}
            try:
                for call in state["calls"]:
                    if call["name"] != "save_note" or call["id"] in results:
                        raise ValueError("unsupported or duplicate tool")
                    note = Note.model_validate(call["args"]).model_dump()
                    if call["id"] in state["notes"] and state["notes"][call["id"]] != note:
                        raise ValueError("tool identity reused with different arguments")
                    state["notes"][call["id"]] = note
                    results[call["id"]] = note
            except (ValueError, ValidationError):
                await checkpoint(ctx, snapshot, finish(deepcopy(snapshot.value), "failed", "invalid_tool_call"))
                return
            state.update(results=results, calls=[], phase="model", steps=state["steps"] + 1,
                         next_at=now + state["request"]["step_delay_ms"])
            await checkpoint(ctx, snapshot, state)
            return
        if state["attempts"] >= MAX_ATTEMPTS:
            await checkpoint(ctx, snapshot, finish(state, "failed", "model_budget_exhausted"))
            return
        # Charge the attempt and arm recovery before external I/O. A crash can
        # repeat a paid model call, but cannot consume an unbounded retry budget.
        state.update(attempts=state["attempts"] + 1, next_at=now + 15000)
        await checkpoint(ctx, snapshot, state)
        snapshot = await ctx.storage.read_json("agent")
        # A concurrent cancellation or newer turn must not start this call.
        if snapshot.value != state:
            return
        try:
            result = await model_step(state)
        except Exception:
            # Never store provider exception text, credentials or response bodies.
            # CancelledError is a BaseException and propagates to the native turn.
            state["error"] = "model_failed"
            if state["attempts"] >= MAX_ATTEMPTS:
                finish(state, "failed", "model_budget_exhausted")
            await checkpoint(ctx, snapshot, state)
            return
        history = result.all_messages_json()
        if len(history) > MAX_HISTORY_BYTES:
            await checkpoint(ctx, snapshot, finish(state, "failed", "history_limit"))
            return
        state.update(history=json.loads(history), results={}, error=None, steps=state["steps"] + 1)
        if isinstance(result.output, DeferredToolRequests):
            calls = result.output.calls
            if result.output.approvals or not 1 <= len(calls) <= 4:
                await checkpoint(ctx, snapshot, finish(state, "failed", "tool_limit"))
                return
            state.update(phase="tools", calls=[{"id": call.tool_call_id, "name": call.tool_name,
                                               "args": call.args_as_dict()} for call in calls],
                         next_at=await ctx.now_ms() + state["request"]["step_delay_ms"])
        elif len(result.output) <= 8000:
            state["answer"] = result.output
            finish(state, "complete")
        else:
            finish(state, "failed", "answer_limit")
        await checkpoint(ctx, snapshot, state)
