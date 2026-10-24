// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import {jsonResponse, request, response, type Context, type Request, type Alarm} from "@cellar/sdk";
import {z} from "zod";

const Amount = z.number().int().min(1).max(100);
export class Counter {
  async fetch(req: Request, ctx: Context) {
    const snapshot = await ctx.storage.read("count");
    const count = snapshot.present ? z.number().parse(snapshot.value) : 0;
    if (req.method === "POST") {
      const amount = Amount.parse(req.json());
      // Checked atomic Rust transaction. A conflict never partially applies.
      await ctx.storage.apply({checks: {count: snapshot}, puts: {count: count + amount}});
      await ctx.storage.sync();
      return jsonResponse({count: count + amount});
    }
    return jsonResponse({count});
  }
  async alarm(_event: Alarm, ctx: Context) {
    await ctx.storage.apply({delete_alarm: true});
  }
}
export const cells = {Counter};
export async function fetch(req: Request, ctx: Context) {
  const name = req.url.split("/").pop() || "default";
  const result = await ctx.cell("COUNTERS", name).fetch(request("http://counter/", {
    method: req.method, headers: req.headers, body: req.body,
  }));
  return response(result.body, {status: result.status, headers: result.headers});
}
