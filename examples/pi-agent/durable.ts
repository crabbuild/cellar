// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import {createHash} from 'node:crypto';
import {Agent, type AgentMessage, type StreamFn} from '@earendil-works/pi-agent-core';
import type {AssistantMessage} from '@earendil-works/pi-ai';
import {Type} from 'typebox';
import type {Context, Json, Snapshot} from '@cellar/sdk';
import {completed, message, type Provider} from './provider.js';

const systemPrompt = 'Use increment once to add the requested integer (1 through 100) to this task counter, then report the result.';
const maxSteps = 16;
const maxAttempts = 5;
const retryMs = 2000;
const maxBytes = 512 * 1024;

interface ModelEntry {request: string; response: AssistantMessage}
interface ToolEntry {request: string; result: {content: {type: 'text'; text: string}[]; details: {count: number}}}
export interface Run {
  format: 1;
  configuration: string;
  prompt: string;
  timestamp: number;
  status: 'pending' | 'completed' | 'failed';
  attempts: number;
  models: ModelEntry[];
  tools: Record<string, ToolEntry>;
  messages: AgentMessage[];
}
export type Checkpoint = 'accepted' | 'model' | 'tool' | 'completed';
export type Hook = (point: Checkpoint) => Promise<void>;
export interface Progress {
  type: 'attempt' | 'model' | 'text' | 'checkpoint' | 'tool' | 'completed' | 'failed';
  attempt: number;
  step?: number;
  text?: string;
  count?: number;
  replayed?: boolean;
}
export type Observer = (event: Progress) => void;

function json(value: unknown): Json {
  const text = JSON.stringify(value);
  if (Buffer.byteLength(text) > maxBytes) throw new Error('agent checkpoint exceeds 512 KiB');
  return JSON.parse(text) as Json;
}

function hash(value: unknown): string {
  // Stable object order, including tool argument maps. Pi-generated timestamps
  // are removed at the call site; all semantic request fields remain checked.
  const ordered = (item: unknown): unknown => Array.isArray(item) ? item.map(ordered)
    : item && typeof item === 'object'
      ? Object.fromEntries(Object.keys(item).sort().map(key => [key, ordered((item as Record<string, unknown>)[key])])) : item;
  return createHash('sha256').update(JSON.stringify(ordered(value))).digest('hex');
}

function configuration(provider: Provider) {
  return hash({format: 1, sdk: 'pi-1.4.0', systemPrompt, model: provider.model,
    tool: 'increment-v1'});
}

export async function readRun(ctx: Context): Promise<Run | null> {
  const snapshot = await ctx.storage.read('run');
  await ctx.storage.sync();
  return snapshot.present ? snapshot.value as unknown as Run : null;
}

// A cell name is the permanent idempotency key for one bounded agent task.
export async function accept(ctx: Context, prompt: string, provider: Provider): Promise<Run | null> {
  const snapshot = await ctx.storage.read('run');
  if (snapshot.present) {
    const run = snapshot.value as unknown as Run;
    // Another invocation may have committed locally but lost its sync reply.
    // A duplicate acceptance must establish durability before acknowledging it.
    await ctx.storage.sync();
    return run.prompt === prompt ? run : null;
  }
  const now = await ctx.now();
  const run: Run = {format: 1, configuration: configuration(provider), prompt,
    timestamp: now, status: 'pending', attempts: 0, models: [], tools: {}, messages: []};
  await ctx.storage.apply({checks: {run: snapshot}, puts: {run: json(run)}, alarm_at_ms: now});
  await ctx.storage.sync();
  return run;
}

export async function execute(ctx: Context, provider: Provider, hook?: Hook, observer?: Observer): Promise<void> {
  let snapshot = await ctx.storage.read('run');
  if (!snapshot.present) return;
  const run = structuredClone(snapshot.value) as unknown as Run;
  // Preview delivery is best effort and never participates in durability.
  const emit = (event: Omit<Progress, 'attempt'>) => {
    try { observer?.({...event, attempt: run.attempts}); } catch { /* An observer cannot fail a task. */ }
  };
  const textOf = (response: AssistantMessage) => response.content
    .filter(part => part.type === 'text').map(part => part.text).join('');
  if (run.status !== 'pending') return;
  if (run.format !== 1 || run.configuration !== configuration(provider)) {
    throw new Error('agent configuration changed; recover using the original release');
  }
  const save = async (extra: {checks?: Record<string, Snapshot>; puts?: Record<string, Json>} = {}) => {
    const value = json(run);
    await ctx.storage.apply({checks: {run: snapshot, ...extra.checks},
      puts: {run: value, ...extra.puts},
      ...(run.status === 'pending' ? {alarm_at_ms: (await ctx.now()) + retryMs} : {delete_alarm: true})});
    await ctx.storage.sync();
    snapshot = {present: true, value};
  };
  if (run.attempts >= maxAttempts) {
    run.status = 'failed';
    await save();
    emit({type: 'failed'});
    return;
  }
  run.attempts++;
  await save(); // Recovery wake exists before any provider or tool execution.
  emit({type: 'attempt'});
  let step = 0;
  let failure: unknown;
  const stream: StreamFn = async (model, context, options) => {
    try {
      if (failure) throw failure;
      if (step >= maxSteps) throw new Error('agent exceeded model step limit');
      const index = step++;
      const request = hash({model, messages: context.messages.map(item => {
        const semantic = {...item} as Record<string, unknown>;
        delete semantic.timestamp;
        delete semantic.durationMs; // Replayed tools take a different amount of time.
        return semantic;
      })});
      const cached = run.models[index];
      if (cached) {
        if (cached.request !== request) throw new Error('model replay diverged');
        emit({type: 'checkpoint', step: index, text: textOf(cached.response), replayed: true});
        return completed(structuredClone(cached.response));
      }
      emit({type: 'model', step: index});
      const signal = AbortSignal.any([AbortSignal.timeout(30_000), ...(options?.signal ? [options.signal] : [])]);
      const source = await provider.stream(model, context, {...options, signal});
      for await (const event of source) {
        if (event.type === 'text_delta') {
          emit({type: 'text', step: index, text: textOf(event.partial)});
        }
      }
      const result = await source.result();
      if (result.stopReason !== 'stop' && result.stopReason !== 'toolUse') {
        throw new Error('provider did not complete a usable response');
      }
      // Persist the decision before Pi can execute any requested tool.
      run.models.push({request, response: structuredClone(result)});
      await save();
      emit({type: 'checkpoint', step: index, text: textOf(result), replayed: false});
      await hook?.('model');
      return completed(structuredClone(result));
    } catch (error) {
      failure = error;
      return completed({...message(model, [], 'error'), errorMessage: 'Durable attempt interrupted'});
    }
  };
  const parameters = Type.Object({amount: Type.Integer({minimum: 1, maximum: 100})}, {additionalProperties: false});
  const agent = new Agent({initialState: {systemPrompt, model: provider.model, tools: [{
    name: 'increment', label: 'Increment task counter', description: 'Add an integer to the durable task counter', parameters,
    execute: async (id, args) => {
      try {
        if (failure) throw failure;
        if (!args || typeof args !== 'object' || !('amount' in args)
          || typeof args.amount !== 'number' || !Number.isInteger(args.amount)
          || args.amount < 1 || args.amount > 100) throw new Error('invalid increment arguments');
        const key = hash({id}); // Never use model-supplied IDs as object properties.
        const request = hash({name: 'increment', args});
        const cached = run.tools[key];
        if (cached) {
          if (cached.request !== request) throw new Error('tool replay diverged');
          emit({type: 'tool', count: cached.result.details.count, replayed: true});
          return structuredClone(cached.result);
        }
        if (Object.keys(run.tools).length >= maxSteps) throw new Error('tool limit exceeded');
        const before = await ctx.storage.read('count');
        const count = (before.present ? Number(before.value) : 0) + args.amount;
        if (!Number.isSafeInteger(count)) throw new Error('invalid counter');
        const result = {content: [{type: 'text' as const, text: String(count)}], details: {count}};
        run.tools[key] = {request, result};
        // The effect and receipt commit together. A lost response is resolved by
        // reading this receipt; it never causes a second increment.
        await save({checks: {count: before}, puts: {count}});
        emit({type: 'tool', count, replayed: false});
        await hook?.('tool');
        return structuredClone(result);
      } catch (error) {
        failure = error;
        throw error;
      }
    },
  }]}, streamFn: stream, toolExecution: 'sequential'});
  await agent.prompt({role: 'user', content: run.prompt, timestamp: run.timestamp});
  if (failure) throw failure;
  if (agent.state.errorMessage) throw new Error('Pi attempt failed');
  const last = agent.state.messages.at(-1);
  if (last?.role !== 'assistant' || last.stopReason !== 'stop') throw new Error('agent did not finish');
  run.messages = agent.state.messages;
  run.status = 'completed';
  await save();
  emit({type: 'completed'});
  await hook?.('completed');
}
