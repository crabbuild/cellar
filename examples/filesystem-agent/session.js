// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { jsonResponse, utf8 } from "@cellar/sdk";
import { createFilesystemTools, validateFilesystemCheckpoint } from "@cellar/sdk/filesystem-tools";

const sessionHeadKey = "filesystem-agent:head";
const sessionIdPattern = /^[A-Za-z0-9_-]{1,64}$/;
const sessionTurnKey = id => `filesystem-agent:turn:${id}`;
const sessionDecisionKey = (id, step) => `${sessionTurnKey(id)}:decision:${step}`;
const sessionResultKey = (id, step) => `${sessionTurnKey(id)}:result:${step}`;
function sessionBounded(value, maximum) {
  const encoded = JSON.stringify(value);
  if (utf8(encoded).length > maximum) throw new Error("Agent journal exceeds its bound");
  return JSON.parse(encoded);
}
async function sessionApply(ctx, batch) {
  validateFilesystemCheckpoint(batch, (await ctx.fs.capabilities()).transport);
  return ctx.storage.apply(batch);
}

/** One cell owns the workspace, indexed turn decisions and tool receipts.
 * model.decide receives {prompt, observations, step, requestId} plus the original
 * context. Use SDK I/O for provider calls. A decision must be saved and proven
 * before its tool executes; a repeated model call before that save is possible.
 */
export function createFilesystemAgentSession(model, options = {}) {
  const policy = Object.freeze({ maxSteps: 16, maxTurns: 32, maxAttempts: 5, maxModelBytes: 262144, maxDecisionBytes: 65536, ...options });
  if (typeof model.configuration !== "string" || !model.configuration.length) throw new Error("Model configuration identity is required");
  for (const value of Object.values(policy)) if (!Number.isSafeInteger(value) || value < 1) throw new Error("Invalid agent policy");
  const configuration = JSON.stringify({ model: model.configuration, policy, tools: "filesystem-tools-v1", checkpoint: "transport-bounded-v1" });

  return class AgentSession {
    running = false;

    async fetch(req, ctx) {
      try { return await this.handleRequest(req, ctx); }
      catch (error) {
        if (error.code === "Conflict" && error.outcome === "not_committed")
          return jsonResponse({ error: "session changed; read the turn before retrying" }, { status: 409 });
        throw error;
      }
    }

    async handleRequest(req, ctx) {
      const id = req.url.split("?")[0].split("/").at(-1);
      if (!sessionIdPattern.test(id)) return jsonResponse({ error: "invalid turn ID" }, { status: 400 });
      const key = sessionTurnKey(id);
      if (req.method === "GET") {
        const run = await ctx.storage.read(key);
        await ctx.storage.sync();
        return run.present ? jsonResponse(run.value) : jsonResponse({ error: "turn not found" }, { status: 404 });
      }
      if (req.method !== "POST") return jsonResponse({ error: "method not allowed" }, { status: 405 });
      let input;
      try { input = req.json(); }
      catch { return jsonResponse({ error: "invalid JSON body" }, { status: 400 }); }
      if (!input || typeof input !== "object" || Array.isArray(input) || typeof input.prompt !== "string" || !input.prompt.length || utf8(input.prompt).length > 8192)
        return jsonResponse({ error: "prompt must contain 1–8192 bytes" }, { status: 400 });
      await ctx.fs.capabilities(); // Refuse before recording a run on an unsupported cell.
      const head = await ctx.storage.read(sessionHeadKey);
      const run = await ctx.storage.read(key);
      if (run.present) {
        if (run.value.prompt !== input.prompt) return jsonResponse({ error: "turn ID has different input" }, { status: 409 });
        if (input.abandon === true && head.present && head.value.active === id) {
          if (this.running) return jsonResponse({ error: "workspace step is still running; inspect the turn before abandoning" }, { status: 409 });
          await sessionApply(ctx, { checks: { [sessionHeadKey]: head, [key]: run }, puts: { [sessionHeadKey]: { ...head.value, active: null }, [key]: { ...run.value, status: "abandoned", outcome: "unknown" } }, delete_alarm: true });
        } else if (input.resume === true && run.value.status === "interrupted" && head.present && head.value.active === id) {
          await sessionApply(ctx, { checks: { [sessionHeadKey]: head, [key]: run }, puts: { [key]: { ...run.value, status: "pending", attempts: 0 } }, alarm_at_ms: await ctx.now() });
        }
        await ctx.storage.sync();
        return jsonResponse((await ctx.storage.read(key)).value);
      }
      const state = head.present ? head.value : { active: null, turns: 0 };
      if (state.active !== null) return jsonResponse({ error: "another turn owns this workspace", active: state.active }, { status: 409 });
      if (this.running) return jsonResponse({ error: "previous workspace step is finishing" }, { status: 409 });
      if (state.turns >= policy.maxTurns) return jsonResponse({ error: "session turn capacity reached" }, { status: 409 });
      const initial = { format: 1, configuration, prompt: input.prompt, status: "pending", step: 0, phase: "model", attempts: 0 };
      await sessionApply(ctx, {
        checks: { [sessionHeadKey]: head, [key]: run },
        puts: { [sessionHeadKey]: { active: id, turns: state.turns + 1 }, [key]: initial },
        alarm_at_ms: await ctx.now(),
      });
      await ctx.fs.sync();
      return jsonResponse(initial, { status: 202 });
    }

    async alarm(_scheduled, ctx) {
      if (this.running) {
        await sessionApply(ctx, { alarm_at_ms: (await ctx.now()) + 100 });
        await ctx.storage.sync();
        return;
      }
      this.running = true;
      let attempted;
      try {
        const head = await ctx.storage.read(sessionHeadKey);
        attempted = head.present ? head.value.active : null;
        await this.advance(ctx);
      }
      catch (error) {
        if (attempted === undefined) throw error;
        const head = await ctx.storage.read(sessionHeadKey);
        if (!head.present || head.value.active === null || head.value.active !== attempted) return;
        const id = head.value.active, key = sessionTurnKey(id);
        const run = await ctx.storage.read(key);
        if (!run.present || run.value.status !== "pending") return;
        // A lost reply may follow a committed tool. Preserve its phase and
        // receipts; the next attempt reconciles the same stable operation ID.
        const attempts = run.value.attempts + 1;
        const interrupted = attempts >= policy.maxAttempts;
        const next = { ...run.value, attempts, status: interrupted ? "interrupted" : "pending", last_error: { code: typeof error.code === "string" && /^[A-Za-z][A-Za-z0-9_]{0,63}$/.test(error.code) ? error.code : "HostError", outcome: "unknown" } };
        await sessionApply(ctx, { checks: { [sessionHeadKey]: head, [key]: run }, puts: { [key]: next }, ...(interrupted ? { delete_alarm: true } : { alarm_at_ms: (await ctx.now()) + 100 }) });
        await ctx.storage.sync();
      } finally { this.running = false; }
    }

    async advance(ctx) {
      const head = await ctx.storage.read(sessionHeadKey);
      if (!head.present || head.value.active === null) return;
      const id = head.value.active, key = sessionTurnKey(id);
      const run = await ctx.storage.read(key);
      if (!run.present || run.value.status !== "pending") return;
      if (run.value.configuration !== configuration) throw new Error("Agent configuration changed during a pending turn");
      const step = run.value.step;
      if (step >= policy.maxSteps) {
        await sessionApply(ctx, { checks: { [sessionHeadKey]: head, [key]: run }, puts: { [key]: { ...run.value, status: "interrupted", last_error: { code: "StepLimit", outcome: "unknown" } } }, delete_alarm: true });
        await ctx.storage.sync();
        return;
      }
      const decisionKey = sessionDecisionKey(id, step);
      const saved = await ctx.storage.read(decisionKey);
      if (!saved.present) {
        const observations = [];
        for (let index = 0; index < step; index++) {
          const result = await ctx.storage.read(sessionResultKey(id, index));
          if (!result.present) throw new Error("Agent observation journal is incomplete");
          observations.push(result.value);
        }
        const request = sessionBounded({ prompt: run.value.prompt, observations, step, requestId: `${ctx.identity.scope}/${id}/${step}` }, policy.maxModelBytes);
        await ctx.fs.sync(); // Prove the intent before provider I/O.
        const decision = sessionBounded(await model.decide(request, ctx), policy.maxDecisionBytes);
        if (decision.type !== "tool" && decision.type !== "complete") throw new Error("Invalid model decision");
        if (decision.type === "tool" && (typeof decision.id !== "string" || !sessionIdPattern.test(decision.id) || !decision.request)) throw new Error("Invalid model tool identity");
        if (decision.type === "complete" && (typeof decision.text !== "string" || utf8(decision.text).length > 8192)) throw new Error("Invalid model completion");
        await sessionApply(ctx, {
          checks: { [sessionHeadKey]: head, [key]: run, [decisionKey]: saved },
          puts: { [decisionKey]: { format: 1, request, decision }, [key]: { ...run.value, phase: "decided" } },
          alarm_at_ms: await ctx.now(),
        });
        await ctx.fs.sync();
        return;
      }
      const decision = saved.value.decision;
      if (decision.type === "complete") {
        await sessionApply(ctx, { checks: { [sessionHeadKey]: head, [key]: run }, puts: { [sessionHeadKey]: { ...head.value, active: null }, [key]: { ...run.value, status: "completed", text: decision.text } }, delete_alarm: true });
        await ctx.fs.sync();
        return;
      }
      const operationId = `${step}:${decision.id}`;
      const tools = createFilesystemTools(ctx, { prefix: `filesystem-agent:tools:${id}:`, maxReceipts: policy.maxSteps });
      let result;
      if (run.value.phase === "decided") {
        const completed = { ...run.value, phase: "tool-receipt" };
        result = (await tools.execute(operationId, decision.request, { checks: { [sessionHeadKey]: head, [key]: run }, puts: { [key]: completed } })).result;
      } else if (run.value.phase === "tool-receipt") {
        // A crash after the effect/receipt batch resumes here. Replaying a
        // saved read returns its original bytes even if a file has since changed.
        result = (await tools.execute(operationId, decision.request)).result;
      } else throw new Error("Invalid agent replay phase");
      const current = await ctx.storage.read(key);
      if (!current.present || current.value.step !== step || current.value.phase !== "tool-receipt") throw new Error("Agent checkpoint changed");
      await sessionApply(ctx, {
        checks: { [sessionHeadKey]: head, [key]: current },
        puts: { [sessionResultKey(id, step)]: sessionBounded({ operation_id: operationId, request: decision.request, result }, policy.maxModelBytes), [key]: { ...current.value, step: step + 1, phase: "model", attempts: 0 } },
        alarm_at_ms: await ctx.now(),
      });
      await ctx.fs.sync();
    }
  };
}
