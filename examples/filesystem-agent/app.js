// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { jsonResponse, request, response } from "@cellar/sdk";
import { createFilesystemAgentSession } from "./session.js";
import { scriptedFilesystemModel } from "./model.js";

const AgentSession = createFilesystemAgentSession(scriptedFilesystemModel);
export default {
  cells: { AgentSession },
  async fetch(req, ctx) {
    const match = /^\/sessions\/([A-Za-z0-9_-]{1,64})\/turns\/([A-Za-z0-9_-]{1,64})$/.exec(new URL(req.url).pathname);
    if (!match) return jsonResponse({ error: "use /sessions/<session>/turns/<turn>" }, { status: 404 });
    const result = await ctx.cell("AGENTS", match[1]).fetch(request(req.url, { method: req.method, body: req.body, headers: req.headers }));
    return response(result.body, { status: result.status, headers: result.headers });
  },
};
