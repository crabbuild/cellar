// One typed workload, compiled to both embedded V8 and Node.js JavaScript.
import {jsonResponse, request, response, type Context, type Request, type CellarError} from '../../../packages/cellar-sdk/index.js';

export class Counter {
  async fetch(req: Request, ctx: Context) {
    const snapshot = await ctx.storage.read('count');
    const count = snapshot.present ? Number(snapshot.value) : 0;
    if (req.method === 'POST') {
      try {
        await ctx.storage.apply({checks: {count: snapshot}, puts: {count: count + 1}});
      } catch (error) {
        const failure = error as CellarError;
        // Report only proven transaction refusals as conflicts. Never retry an
        // unknown submitted mutation or disguise it as a safe refusal.
        if (failure.code === 'Conflict' && failure.outcome === 'not_committed') {
          return jsonResponse({code: failure.code, outcome: failure.outcome}, {status: 409});
        }
        throw error;
      }
      await ctx.storage.sync();
      return jsonResponse({count: count + 1});
    }
    return jsonResponse({count});
  }
}
export const cells = {Counter};
export async function fetch(req: Request, ctx: Context) {
  if (req.url.includes('/echo')) return response(req.body);
  const name = req.url.split('/').pop() || 'default';
  const result = await ctx.cell('COUNTERS', name).fetch(request('http://counter/', {
    method: req.method, headers: req.headers, body: req.body,
  }));
  return response(result.body, {status: result.status, headers: result.headers});
}
