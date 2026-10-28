// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { createHash } from "node:crypto";
import { tool } from "ai";
import { z } from "zod";
import type { Batch, Context, Json } from "@cellar/sdk";
import {
  createFilesystemTools,
  filesystemToolRefusal,
} from "@cellar/sdk/filesystem-tools";

const filePath = z.string().min(1).max(4096);
const fileInteger = z
  .string()
  .max(19)
  .regex(/^(0|[1-9][0-9]*)$/);

export const schemas = {
  increment: z.object({ amount: z.number().int().min(1).max(100) }).strict(),

  calculate: z
    .object({
      operation: z.enum(["add", "subtract", "multiply", "divide"]),
      a: z.number().min(-1e9).max(1e9),
      b: z.number().min(-1e9).max(1e9),
    })
    .strict(),

  lookup: z
    .object({ topic: z.enum(["durability", "streaming", "tools"]) })
    .strict(),

  saveNote: z
    .object({
      title: z.string().trim().min(1).max(120),
      content: z.string().trim().min(1).max(4000),
    })
    .strict(),

  readState: z.object({}).strict(),

  createTicket: z
    .object({
      title: z.string().trim().min(1).max(120),
      priority: z.enum(["low", "normal", "high"]),
    })
    .strict(),

  readFile: z
    .object({
      path: filePath,
      offset: fileInteger.optional(),
      max_bytes: z.number().int().min(1).max(32768).optional(),
    })
    .strict(),
  listFiles: z
    .object({
      path: filePath,
      limit: z.number().int().min(1).max(128).optional(),
      cursor: z
        .object({
          scope: z.string().max(4096),
          inode: fileInteger,
          position: fileInteger,
        })
        .strict()
        .nullable()
        .optional(),
    })
    .strict(),
  writeFile: z
    .object({ path: filePath, content: z.string().max(8192) })
    .strict(),
  editFile: z
    .object({
      path: filePath,
      inode: fileInteger,
      version: fileInteger,
      offset: fileInteger,
      delete_bytes: fileInteger,
      insert: z.string().max(8192),
    })
    .strict(),
  renameFile: z.object({ path: filePath, destination: filePath }).strict(),
  deleteFile: z.object({ path: filePath }).strict(),
  searchFiles: z
    .object({ path: filePath, pattern: z.string().min(1).max(128) })
    .strict(),
};

export interface Receipt {
  name: string;
  request: string;
  input: Json;
  result: Json;
}

export interface ToolState {
  tools: Record<string, Receipt>;
  intents: Record<string, { request: string; operation: string }>;
}

export interface ToolServices {
  url: string;
  token: string;
}

export type Effects = Pick<
  Batch,
  "checks" | "puts" | "filesystem_checks" | "filesystem_mutations"
>;
export type Save = (extra?: Effects) => Promise<void>;

export function fingerprint(value: unknown): string {
  // Object property order must not change the identity of a replayed request.
  const ordered = (item: unknown): unknown =>
    Array.isArray(item)
      ? item.map(ordered)
      : item && typeof item === "object"
        ? Object.fromEntries(
            Object.keys(item)
              .sort()
              .map((key) => [
                key,
                ordered((item as Record<string, unknown>)[key]),
              ]),
          )
        : item;
  return createHash("sha256")
    .update(JSON.stringify(ordered(value)))
    .digest("hex");
}

export function agentTools(
  ctx: Context,
  state: ToolState,
  save: Save,
  options: {
    services?: ToolServices;
    emit: (name: string, result: Json, replayed: boolean) => void;
    external: () => Promise<void>;
    committed: () => Promise<void>;
    check: () => void;
    failed: (error: unknown) => void;
  },
) {
  const files = createFilesystemTools(ctx);
  async function receipt(
    name: string,
    input: unknown,
    id: string,
    effect: (
      key: string,
      request: string,
    ) => Promise<{ result: Json } & Effects>,
  ) {
    try {
      options.check(); // A previous tool in this same SDK step may have failed.
      const key = fingerprint({ id });
      const request = fingerprint({ name, input });
      const cached = state.tools[key];

      if (cached) {
        if (cached.request !== request) throw new Error("tool replay diverged");
        options.emit(name, cached.result, true);
        return structuredClone(cached.result);
      }

      if (Object.keys(state.tools).length >= 16)
        throw new Error("tool receipt limit reached");

      // Effects return local writes rather than committing them independently,
      // so save() can commit the mutation and its receipt together.
      let { result, ...batch } = await effect(key, request);
      state.tools[key] = {
        name,
        request,
        input: JSON.parse(JSON.stringify(input)),
        result,
      };
      try {
        await save(batch);
      } catch (error) {
        const refusal =
          batch.filesystem_checks || batch.filesystem_mutations
            ? filesystemToolRefusal(error)
            : null;
        if (refusal === null) throw error;
        // The original checked file batch proved no commit. Persist its refusal
        // with the journal only; keep the same run/state guards in save().
        result = refusal;
        state.tools[key].result = result;
        await save({ checks: batch.checks, puts: batch.puts });
      }
      options.emit(name, result, false);
      await options.committed();
      return structuredClone(result);
    } catch (error) {
      options.failed(error);
      throw error;
    }
  }

  return {
    readFile: tool({
      description:
        "Read a bounded range from this turn's cell filesystem. Returns inode/version and saved bytes; replay returns the original read.",
      inputSchema: schemas.readFile,
      execute: (args, { toolCallId }) =>
        receipt("readFile", args, toolCallId, async () =>
          files.prepare({ operation: "read", ...schemas.readFile.parse(args) }),
        ),
    }),
    listFiles: tool({
      description:
        "List a bounded page in this turn's cell filesystem. Use the opaque continuation cursor to request another page.",
      inputSchema: schemas.listFiles,
      execute: (args, { toolCallId }) =>
        receipt("listFiles", args, toolCallId, async () =>
          files.prepare({
            operation: "list",
            ...schemas.listFiles.parse(args),
          }),
        ),
    }),
    writeFile: tool({
      description:
        "Create or overwrite a bounded text file in this turn's cell filesystem. File contents and the tool receipt commit atomically.",
      inputSchema: schemas.writeFile,
      execute: (args, { toolCallId }) =>
        receipt("writeFile", args, toolCallId, async () =>
          files.prepare({
            operation: "write",
            ...schemas.writeFile.parse(args),
          }),
        ),
    }),
    editFile: tool({
      description:
        "Edit a byte range using the inode/version from a prior read. A conflict is returned as a saved refusal; inspect the current file before a new decision.",
      inputSchema: schemas.editFile,
      execute: (args, { toolCallId }) =>
        receipt("editFile", args, toolCallId, async () =>
          files.prepare({ operation: "edit", ...schemas.editFile.parse(args) }),
        ),
    }),
    renameFile: tool({
      description:
        "Rename a file within this turn's cell filesystem and save its receipt atomically.",
      inputSchema: schemas.renameFile,
      execute: (args, { toolCallId }) =>
        receipt("renameFile", args, toolCallId, async () =>
          files.prepare({
            operation: "rename",
            ...schemas.renameFile.parse(args),
          }),
        ),
    }),
    deleteFile: tool({
      description:
        "Unlink a file in this turn's cell filesystem and save its receipt atomically.",
      inputSchema: schemas.deleteFile,
      execute: (args, { toolCallId }) =>
        receipt("deleteFile", args, toolCallId, async () =>
          files.prepare({
            operation: "unlink",
            ...schemas.deleteFile.parse(args),
          }),
        ),
    }),
    searchFiles: tool({
      description:
        "Search bounded file prefixes for literal text in this turn's cell filesystem. Reports traversal and truncation limits.",
      inputSchema: schemas.searchFiles,
      execute: (args, { toolCallId }) =>
        receipt("searchFiles", args, toolCallId, async () =>
          files.prepare({
            operation: "search",
            ...schemas.searchFiles.parse(args),
          }),
        ),
    }),
    increment: tool({
      description:
        "Add an integer to this turn’s durable counter. At most one distinct increment per turn.",
      inputSchema: schemas.increment,
      execute: (args, { toolCallId }) =>
        receipt("increment", args, toolCallId, async () => {
          schemas.increment.parse(args);
          if (
            Object.values(state.tools).some((item) => item.name === "increment")
          )
            throw new Error("task already has an increment");
          const before = await ctx.storage.read("count");
          const previous = before.present ? before.value : 0;
          if (
            typeof previous !== "number" ||
            !Number.isSafeInteger(previous) ||
            previous < 0
          )
            throw new Error("invalid stored counter");
          const count = previous + args.amount;
          if (!Number.isSafeInteger(count))
            throw new Error("counter limit reached");
          return {
            result: { count },
            checks: { count: before },
            puts: { count },
          };
        }),
    }),

    calculate: tool({
      description:
        "Calculate with two numbers without executing code. Division by zero returns a recoverable domain error.",
      inputSchema: schemas.calculate,
      execute: (args, { toolCallId }) =>
        receipt(
          "calculate",
          args,
          toolCallId,
          async (): Promise<{ result: Json }> => {
            const { operation, a, b } = args;
            if (operation === "divide" && b === 0)
              return { result: { error: "division_by_zero" } };
            const value =
              operation === "add"
                ? a + b
                : operation === "subtract"
                  ? a - b
                  : operation === "multiply"
                    ? a * b
                    : a / b;
            if (
              !Number.isFinite(value) ||
              (Number.isInteger(value) && !Number.isSafeInteger(value))
            )
              return { result: { error: "result_out_of_range" } };
            return { result: { value } };
          },
        ),
    }),

    lookup: tool({
      description:
        "Read a short reference about Cellar durability, streaming, or tool execution.",
      inputSchema: schemas.lookup,
      execute: (args, { toolCallId }) =>
        receipt("lookup", args, toolCallId, async () => ({
          result: {
            topic: args.topic,
            text: {
              durability:
                "Cellar checkpoints model decisions before tools. Local effects and receipts commit atomically. Alarms resume unfinished work.",
              streaming:
                "Live text is provisional until checkpointed. Reloads read durable state. Reconnection does not start another agent.",
              tools:
                "Read, compute and write tools have durable receipts. External writes require downstream idempotency and reconciliation.",
            }[args.topic],
          },
        })),
    }),

    saveNote: tool({
      description:
        "Save a note in cell-owned SQLite. The note and tool receipt commit atomically.",
      inputSchema: schemas.saveNote,
      execute: (args, { toolCallId }) =>
        receipt("saveNote", args, toolCallId, async (key) => ({
          result: { saved: true, title: args.title },
          puts: { [`note:${key}`]: args },
        })),
    }),

    readState: tool({
      description:
        "Read this turn’s durable counter and recorded tool receipts.",
      inputSchema: schemas.readState,
      execute: (args, { toolCallId }) =>
        receipt("readState", args, toolCallId, async () => {
          const count = await ctx.storage.read("count");
          return {
            result: {
              count: count.present ? count.value : 0,
              receipts: Object.values(state.tools).map((item) => ({
                name: item.name,
                result: item.result,
              })),
            },
          };
        }),
    }),

    createTicket: tool({
      description:
        "Create a ticket via a configured external HTTP service with a durable idempotency key. Only call when requested.",
      inputSchema: schemas.createTicket,
      execute: (args, { toolCallId }) =>
        receipt("createTicket", args, toolCallId, async (key, request) => {
          if (!options.services)
            throw new Error("ticket service is not configured");
          const prior = state.intents[key];
          if (prior && prior.request !== request)
            throw new Error("external intent replay diverged");
          if (!ctx.identity?.name)
            throw new Error("external tool requires a named cell");

          // Ownership epochs change during recovery. Keep the downstream key
          // stable so a retry can reconcile a write whose response was lost.
          const operation =
            prior?.operation ??
            fingerprint({
              scope: ctx.identity.scope,
              class: ctx.identity.class,
              name: ctx.identity.name,
              key,
            });
          if (!prior) {
            state.intents[key] = { request, operation };
            await save(); // Intent is durable before dispatching an external write.
          }

          const reply = await fetch(
            `${options.services.url}/internal/tickets`,
            {
              method: "POST",
              headers: {
                "content-type": "application/json",
                authorization: `Bearer ${options.services.token}`,
                "idempotency-key": operation,
              },
              body: JSON.stringify(args),
              signal: AbortSignal.timeout(10000),
            },
          );
          if (!reply.ok) {
            await reply.body?.cancel();
            throw new Error("ticket service unavailable");
          }

          const body = await reply.text();
          if (body.length > 8000)
            throw new Error("ticket response exceeds limit");
          const value = z
            .object({
              id: z.string().max(100),
              title: z.string().max(120),
              priority: z.enum(["low", "normal", "high"]),
            })
            .strict()
            .parse(JSON.parse(body));
          await options.external(); // Qualification can interrupt after remote commit.
          return { result: value };
        }),
    }),
  };
}
