import {jsonResponse, request, response, type Application, type Context, type Request} from '@cellar/sdk';
import {accept, execute, readRun, type Hook} from './durable.js';
import type {Provider} from './provider.js';

export function application(provider: Provider, hook?: Hook): Application {
  class DurableAgent {
    async fetch(req: Request, ctx: Context) {
      if (req.method === 'POST') {
        let value;
        try { value = req.json(); } catch { return jsonResponse({error: 'invalid JSON'}, {status: 400}); }
        if (!value || typeof value !== 'object' || Array.isArray(value)
          || typeof value.prompt !== 'string' || !value.prompt.trim() || value.prompt.length > 8000) {
          return jsonResponse({error: 'prompt must be 1–8000 characters'}, {status: 400});
        }
        const run = await accept(ctx, value.prompt, provider);
        if (!run) return jsonResponse({error: 'task ID already has a different prompt'}, {status: 409});
        await hook?.('accepted');
        return jsonResponse({status: run.status}, {status: run.status === 'pending' ? 202 : 200});
      }
      if (req.method !== 'GET') return jsonResponse({error: 'method not allowed'}, {status: 405});
      const run = await readRun(ctx);
      if (!run) return jsonResponse({error: 'task not found'}, {status: 404});
      const count = await ctx.storage.read('count');
      return jsonResponse({status: run.status, attempts: run.attempts,
        modelSteps: run.models.length, toolReceipts: Object.keys(run.tools).length,
        count: count.present ? count.value : 0,
        messages: JSON.parse(JSON.stringify(run.messages))});
    }
    async alarm(_alarm: unknown, ctx: Context) { await execute(ctx, provider, hook); }
  }
  return {cells: {DurableAgent}, async fetch(req, ctx) {
    const match = /^\/agents\/([a-zA-Z0-9_-]{1,64})$/.exec(new URL(req.url).pathname);
    if (!match) return jsonResponse({error: 'use /agents/<task-id>'}, {status: 404});
    const result = await ctx.cell('AGENTS', match[1]).fetch(request('http://agent/',
      {method: req.method, headers: req.headers, body: req.body}));
    return response(result.body, {status: result.status, headers: result.headers});
  }};
}
