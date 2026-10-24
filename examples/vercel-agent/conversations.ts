import { jsonResponse, request, type Context, type Request } from "@cellar/sdk";
import { z } from "zod";
import type { Run } from "./durable.js";

const id = z.string().regex(/^[a-zA-Z0-9_-]{1,64}$/);
const entrySchema = z
  .object({
    task: id,
    title: z.string().min(1).max(48),
    prompt: z.string().min(1).max(8000),
    previous: id.nullable(),
  })
  .strict();

// One bounded directory belongs to this loopback workspace. It is a Cellar
// cell, so discovery survives loss of both browser storage and worker files.
export class ConversationDirectory {
  async fetch(req: Request, ctx: Context) {
    if (!["GET", "POST"].includes(req.method))
      return jsonResponse({ error: "method not allowed" }, { status: 405 });
    await ctx.storage.sql(`CREATE TABLE IF NOT EXISTS conversation_turns (
      sequence INTEGER PRIMARY KEY,
      task TEXT NOT NULL UNIQUE,
      title TEXT NOT NULL,
      prompt TEXT NOT NULL,
      previous TEXT
    )`);
    if (req.method === "GET") {
      const result = await ctx.storage.sql(`SELECT task, title, prompt, previous
        FROM conversation_turns AS turn
        WHERE NOT EXISTS (SELECT 1 FROM conversation_turns AS child WHERE child.previous = turn.task)
        ORDER BY sequence DESC LIMIT 20`);
      await ctx.storage.sync();
      return jsonResponse({
        chats: result.rows.map(([task, title, prompt, previous]) => ({
          task,
          title,
          prompt,
          previous,
        })),
      });
    }

    let input;
    try {
      input = entrySchema.safeParse(req.json());
    } catch {
      return jsonResponse(
        { error: "invalid directory entry" },
        { status: 400 },
      );
    }
    if (!input.success)
      return jsonResponse(
        { error: "invalid directory entry" },
        { status: 400 },
      );
    const { task, title, prompt, previous } = input.data;
    // SQLite arbitrates concurrent registrations. A read/modify/write JSON
    // snapshot would race between acceptance and the turn's first alarm.
    await ctx.storage.sql(
      `INSERT INTO conversation_turns (task, title, prompt, previous)
      VALUES (?, ?, ?, ?) ON CONFLICT(task) DO NOTHING`,
      [task, title, prompt, previous],
    );
    const existing = await ctx.storage.sql(
      "SELECT title, prompt, previous FROM conversation_turns WHERE task = ?",
      [task],
    );
    const prior = existing.rows[0];
    if (
      !prior ||
      prior[0] !== title ||
      prior[1] !== prompt ||
      prior[2] !== previous
    ) {
      return jsonResponse(
        { error: "conversation identity conflict" },
        { status: 409 },
      );
    }
    // Keep parent references so replaying an older turn does not resurrect
    // it beside its newer child. Entry count and prompt length are bounded.
    await ctx.storage.sql(`DELETE FROM conversation_turns WHERE sequence IN
      (SELECT sequence FROM conversation_turns ORDER BY sequence DESC LIMIT -1 OFFSET 128)`);
    await ctx.storage.sync();
    return jsonResponse({ saved: true });
  }
}

export async function indexConversation(ctx: Context, run: Run) {
  const first = run.history.find((message) => message.role === "user");
  const title = (
    first && typeof first.content === "string" ? first.content : run.prompt
  ).slice(0, 48);
  const reply = await ctx.cell("CONVERSATIONS", "workspace").fetch(
    request("http://directory/", {
      method: "POST",
      body: JSON.stringify({
        task: ctx.identity?.name,
        title,
        prompt: run.prompt,
        previous: run.previous,
      }),
    }),
  );
  if (reply.status !== 200)
    throw new Error("conversation history could not be saved");
}
