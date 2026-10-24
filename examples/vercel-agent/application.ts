import {
  jsonResponse,
  request,
  response,
  type Application,
  type Context,
  type Request,
} from "@cellar/sdk";
import {
  accept,
  execute,
  readRun,
  type Hook,
  type Progress,
} from "./durable.js";
import type { Model } from "./provider.js";
import type { ToolServices } from "./tools.js";
import type { ModelMessage } from "ai";
import { fingerprint } from "./tools.js";

function publicMessages(messages: ModelMessage[]) {
  // Keep replay metadata in the journal, while exposing only public content.
  return JSON.parse(
    JSON.stringify(
      messages.map((message) => ({
        ...message,
        ...(Array.isArray(message.content)
          ? {
              content: message.content.filter(
                (part) => part.type !== "reasoning",
              ),
            }
          : {}),
      })),
      (key, value) =>
        ["providerMetadata", "providerOptions"].includes(key)
          ? undefined
          : value,
    ),
  );
}

export function application(
  model: Model,
  hook?: Hook,
  observe?: (task: string, event: Progress) => void,
  services?: ToolServices,
): Application {
  class DurableAgent {
    async fetch(req: Request, ctx: Context) {
      if (req.method === "POST") {
        let value;
        try {
          value = req.json();
        } catch {
          return jsonResponse({ error: "invalid JSON" }, { status: 400 });
        }
        if (
          !value ||
          typeof value !== "object" ||
          Array.isArray(value) ||
          typeof value.prompt !== "string" ||
          !value.prompt.trim() ||
          value.prompt.length > 8000
        ) {
          return jsonResponse(
            { error: "prompt must be 1–8000 characters" },
            { status: 400 },
          );
        }

        const previous = value.previous ?? null;
        if (
          previous !== null &&
          (typeof previous !== "string" ||
            !/^[a-zA-Z0-9_-]{1,64}$/.test(previous) ||
            previous === ctx.identity?.name)
        ) {
          return jsonResponse(
            { error: "invalid previous turn" },
            { status: 400 },
          );
        }

        let history: ModelMessage[] = [];
        if (previous) {
          // Follow-ups trust the completed parent cell, not client transcripts.
          const parent = await ctx
            .cell("AGENTS", previous)
            .fetch(request("http://agent/"));
          if (parent.status !== 200)
            return jsonResponse(
              { error: "previous turn not found" },
              { status: 409 },
            );
          const state = parent.json();
          if (
            !state ||
            typeof state !== "object" ||
            Array.isArray(state) ||
            state.status !== "completed" ||
            !Array.isArray(state.messages)
          ) {
            return jsonResponse(
              { error: "wait for the previous turn to complete" },
              { status: 409 },
            );
          }
          history = state.messages as unknown as ModelMessage[];
          if (Buffer.byteLength(JSON.stringify(history)) > 192 * 1024)
            return jsonResponse(
              { error: "conversation limit reached; start a new chat" },
              { status: 413 },
            );
        }

        const run = await accept(ctx, value.prompt, model, previous, history);
        if (!run)
          return jsonResponse(
            { error: "task ID already has a different prompt" },
            { status: 409 },
          );
        await hook?.("accepted");
        return jsonResponse(
          { status: run.status },
          { status: run.status === "pending" ? 202 : 200 },
        );
      }

      if (req.method !== "GET")
        return jsonResponse({ error: "method not allowed" }, { status: 405 });
      const run = await readRun(ctx);
      if (!run)
        return jsonResponse({ error: "task not found" }, { status: 404 });

      // Read count from the same journal snapshot so status and receipt agree.
      const order = run.models.flatMap((entry) =>
        entry.response.content
          .filter((part) => part.type === "tool-call")
          .map((part) => fingerprint({ id: part.toolCallId })),
      );
      const receipts = Object.entries(run.tools)
        .sort(([a], [b]) => order.indexOf(a) - order.indexOf(b))
        .map(([, receipt]) => receipt);
      const result = receipts.find((item) => item.name === "increment")?.result;
      return jsonResponse({
        status: run.status,
        prompt: run.prompt,
        attempts: run.attempts,
        previous: run.previous,
        modelSteps: run.models.length,
        toolReceipts: Object.keys(run.tools).length,
        count:
          result && typeof result === "object" && !Array.isArray(result)
            ? (result.count ?? 0)
            : 0,
        receipts: JSON.parse(JSON.stringify(receipts)),
        messages: publicMessages(
          run.status === "completed"
            ? run.messages
            : [...run.history, { role: "user", content: run.prompt }],
        ),
        usage: JSON.parse(
          JSON.stringify(run.models.map((entry) => entry.response.usage)),
        ),
      });
    }

    // Acceptance schedules durable work; alarms drive execution and retries.
    async alarm(_alarm: unknown, ctx: Context) {
      await execute(
        ctx,
        model,
        hook,
        observe
          ? (event) => {
              if (ctx.identity?.name) observe(ctx.identity.name, event);
            }
          : undefined,
        services,
      );
    }
  }

  return {
    cells: { DurableAgent },
    async fetch(req, ctx) {
      const match = /^\/agents\/([a-zA-Z0-9_-]{1,64})$/.exec(
        new URL(req.url).pathname,
      );
      if (!match)
        return jsonResponse(
          { error: "use /agents/<task-id>" },
          { status: 404 },
        );
      const result = await ctx.cell("AGENTS", match[1]).fetch(
        request("http://agent/", {
          method: req.method,
          headers: req.headers,
          body: req.body,
        }),
      );
      return response(result.body, {
        status: result.status,
        headers: result.headers,
      });
    },
  };
}
