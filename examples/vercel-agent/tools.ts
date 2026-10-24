import {createHash} from 'node:crypto';
import {tool} from 'ai';
import {z} from 'zod';
import type {Context, Json, Snapshot} from '@cellar/sdk';

export const schemas = {
  increment: z.object({amount: z.number().int().min(1).max(100)}).strict(),
  calculate: z.object({operation: z.enum(['add', 'subtract', 'multiply', 'divide']), a: z.number().min(-1e9).max(1e9), b: z.number().min(-1e9).max(1e9)}).strict(),
  lookup: z.object({topic: z.enum(['durability', 'streaming', 'tools'])}).strict(),
  saveNote: z.object({title: z.string().trim().min(1).max(120), content: z.string().trim().min(1).max(4000)}).strict(),
  readState: z.object({}).strict(),
  createTicket: z.object({title: z.string().trim().min(1).max(120), priority: z.enum(['low', 'normal', 'high'])}).strict(),
};
export interface Receipt {name: string; request: string; input: Json; result: Json}
export interface ToolState {tools: Record<string, Receipt>; intents: Record<string, {request: string; operation: string}>}
export interface ToolServices {url: string; token: string}
export type Save = (extra?: {checks?: Record<string, Snapshot>; puts?: Record<string, Json>}) => Promise<void>;
export function fingerprint(value: unknown): string {
  const ordered = (item: unknown): unknown => Array.isArray(item) ? item.map(ordered)
    : item && typeof item === 'object'
      ? Object.fromEntries(Object.keys(item).sort().map(key => [key, ordered((item as Record<string, unknown>)[key])])) : item;
  return createHash('sha256').update(JSON.stringify(ordered(value))).digest('hex');
}

export function agentTools(ctx: Context, state: ToolState, save: Save, options: {
  services?: ToolServices;
  emit: (name: string, result: Json, replayed: boolean) => void;
  external: () => Promise<void>;
  committed: () => Promise<void>;
  check: () => void;
  failed: (error: unknown) => void;
}) {
  async function receipt(name: string, input: unknown, id: string,
    effect: (key: string, request: string) => Promise<{result: Json; checks?: Record<string, Snapshot>; puts?: Record<string, Json>}>) {
    try {
      options.check(); // A previous tool in this same SDK step may have failed.
      const key = fingerprint({id});
      const request = fingerprint({name, input});
      const cached = state.tools[key];
      if (cached) {
        if (cached.request !== request) throw new Error('tool replay diverged');
        options.emit(name, cached.result, true);
        return structuredClone(cached.result);
      }
      if (Object.keys(state.tools).length >= 16) throw new Error('tool receipt limit reached');
      const {result, ...batch} = await effect(key, request);
      state.tools[key] = {name, request, input: JSON.parse(JSON.stringify(input)), result};
      await save(batch);
      options.emit(name, result, false);
      await options.committed();
      return structuredClone(result);
    } catch (error) { options.failed(error); throw error; }
  }
  return {
    increment: tool({description: 'Add an integer to this turn’s durable counter. At most one distinct increment per turn.', inputSchema: schemas.increment,
      execute: (args, {toolCallId}) => receipt('increment', args, toolCallId, async () => {
        schemas.increment.parse(args);
        if (Object.values(state.tools).some(item => item.name === 'increment')) throw new Error('task already has an increment');
        const before = await ctx.storage.read('count');
        const previous = before.present ? before.value : 0;
        if (typeof previous !== 'number' || !Number.isSafeInteger(previous) || previous < 0) throw new Error('invalid stored counter');
        const count = previous + args.amount;
        if (!Number.isSafeInteger(count)) throw new Error('counter limit reached');
        return {result: {count}, checks: {count: before}, puts: {count}};
      })}),
    calculate: tool({description: 'Calculate with two numbers without executing code. Division by zero returns a recoverable domain error.', inputSchema: schemas.calculate,
      execute: (args, {toolCallId}) => receipt('calculate', args, toolCallId, async (): Promise<{result: Json}> => {
        const {operation, a, b} = args;
        if (operation === 'divide' && b === 0) return {result: {error: 'division_by_zero'}};
        const value = operation === 'add' ? a + b : operation === 'subtract' ? a - b : operation === 'multiply' ? a * b : a / b;
        if (!Number.isFinite(value) || (Number.isInteger(value) && !Number.isSafeInteger(value))) return {result: {error: 'result_out_of_range'}};
        return {result: {value}};
      })}),
    lookup: tool({description: 'Read a short reference about Cellar durability, streaming, or tool execution.', inputSchema: schemas.lookup,
      execute: (args, {toolCallId}) => receipt('lookup', args, toolCallId, async () => ({result: {
        topic: args.topic, text: {
          durability: 'Cellar checkpoints model decisions before tools. Local effects and receipts commit atomically. Alarms resume unfinished work.',
          streaming: 'Live text is provisional until checkpointed. Reloads read durable state. Reconnection does not start another agent.',
          tools: 'Read, compute and write tools have durable receipts. External writes require downstream idempotency and reconciliation.',
        }[args.topic],
      }}))}),
    saveNote: tool({description: 'Save a note in cell-owned SQLite. The note and tool receipt commit atomically.', inputSchema: schemas.saveNote,
      execute: (args, {toolCallId}) => receipt('saveNote', args, toolCallId, async key => ({
        result: {saved: true, title: args.title}, puts: {[`note:${key}`]: args},
      }))}),
    readState: tool({description: 'Read this turn’s durable counter and recorded tool receipts.', inputSchema: schemas.readState,
      execute: (args, {toolCallId}) => receipt('readState', args, toolCallId, async () => {
        const count = await ctx.storage.read('count');
        return {result: {count: count.present ? count.value : 0, receipts: Object.values(state.tools).map(item => ({name: item.name, result: item.result}))}};
      })}),
    createTicket: tool({description: 'Create a ticket via a configured external HTTP service with a durable idempotency key. Only call when requested.', inputSchema: schemas.createTicket,
      execute: (args, {toolCallId}) => receipt('createTicket', args, toolCallId, async (key, request) => {
        if (!options.services) throw new Error('ticket service is not configured');
        const prior = state.intents[key];
        if (prior && prior.request !== request) throw new Error('external intent replay diverged');
        if (!ctx.identity?.name) throw new Error('external tool requires a named cell');
        const operation = prior?.operation ?? fingerprint({scope: ctx.identity.scope, class: ctx.identity.class, name: ctx.identity.name, key});
        if (!prior) {
          state.intents[key] = {request, operation};
          await save(); // Intent is durable before dispatching an external write.
        }
        const reply = await fetch(`${options.services.url}/internal/tickets`, {method: 'POST',
          headers: {'content-type': 'application/json', authorization: `Bearer ${options.services.token}`, 'idempotency-key': operation},
          body: JSON.stringify(args), signal: AbortSignal.timeout(10000)});
        if (!reply.ok) { await reply.body?.cancel(); throw new Error('ticket service unavailable'); }
        const body = await reply.text();
        if (body.length > 8000) throw new Error('ticket response exceeds limit');
        const value = z.object({id: z.string().max(100), title: z.string().max(120), priority: z.enum(['low', 'normal', 'high'])}).strict().parse(JSON.parse(body));
        await options.external(); // Qualification can interrupt after remote commit.
        return {result: value};
      })}),
  };
}
