import {createHash} from 'node:crypto';
import {ToolLoopAgent, isStepCount, tool, wrapLanguageModel, type ModelMessage} from 'ai';
import {z} from 'zod';
import type {Context, Json, Snapshot} from '@cellar/sdk';
import type {Decision, Model} from './provider.js';

const instructions = 'Use increment exactly once to add the requested integer (1 through 100) to this task counter, then report the result.';
const maxSteps = 16;
const maxAttempts = 5;
const retryMs = 2000;
const maxBytes = 512 * 1024;
const parameters = z.object({amount: z.number().int().min(1).max(100)}).strict();

interface ModelEntry {request: string; response: Decision}
interface ToolEntry {request: string; result: {count: number}}
export interface Run {
  format: 1;
  configuration: string;
  prompt: string;
  status: 'pending' | 'completed' | 'failed';
  attempts: number;
  models: ModelEntry[];
  tools: Record<string, ToolEntry>;
  messages: ModelMessage[];
}
export type Checkpoint = 'accepted' | 'model' | 'tool' | 'completed';
export type Hook = (point: Checkpoint) => Promise<void>;

function json(value: unknown): Json {
  const text = JSON.stringify(value);
  if (Buffer.byteLength(text) > maxBytes) throw new Error('agent checkpoint exceeds 512 KiB');
  return JSON.parse(text) as Json;
}

function hash(value: unknown): string {
  const ordered = (item: unknown): unknown => Array.isArray(item) ? item.map(ordered)
    : item && typeof item === 'object'
      ? Object.fromEntries(Object.keys(item).sort().map(key => [key, ordered((item as Record<string, unknown>)[key])])) : item;
  return createHash('sha256').update(JSON.stringify(ordered(value))).digest('hex');
}

function configuration(model: Model) {
  return hash({format: 1, sdk: 'ai-7.0.152', providerSdk: 'openai-4.0.101',
    provider: model.provider, model: model.modelId, instructions,
    tool: 'increment-once-v1', schema: z.toJSONSchema(parameters), maxSteps});
}

export async function readRun(ctx: Context): Promise<Run | null> {
  const snapshot = await ctx.storage.read('run');
  await ctx.storage.sync();
  return snapshot.present ? snapshot.value as unknown as Run : null;
}

// The cell name permanently identifies one bounded task, including its retries.
export async function accept(ctx: Context, prompt: string, model: Model): Promise<Run | null> {
  const snapshot = await ctx.storage.read('run');
  if (snapshot.present) {
    const run = snapshot.value as unknown as Run;
    // An earlier apply may have committed but lost its sync acknowledgement.
    await ctx.storage.sync();
    return run.prompt === prompt ? run : null;
  }
  const run: Run = {format: 1, configuration: configuration(model), prompt,
    status: 'pending', attempts: 0, models: [], tools: {}, messages: []};
  await ctx.storage.apply({checks: {run: snapshot}, puts: {run: json(run)}, alarm_at_ms: await ctx.now()});
  await ctx.storage.sync();
  return run;
}

export async function execute(ctx: Context, provider: Model, hook?: Hook): Promise<void> {
  let snapshot = await ctx.storage.read('run');
  if (!snapshot.present) return;
  const run = structuredClone(snapshot.value) as unknown as Run;
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
    return;
  }
  run.attempts++;
  await save(); // Commit a recovery wake before starting provider or tool work.
  let step = 0;
  // The SDK converts tool exceptions into model-visible tool errors. A failed
  // durability barrier must instead abort the whole attempt, without generating
  // or checkpointing a different conversation on the next loop iteration.
  let failure: unknown;
  const model = wrapLanguageModel({model: provider, middleware: {
    specificationVersion: 'v4',
    wrapGenerate: async ({doGenerate, params}) => {
      if (failure) throw failure;
      if (step >= maxSteps) throw new Error('agent exceeded model step limit');
      const index = step++;
      // Exclude transport cancellation and headers (which can contain secrets).
      const {abortSignal: _signal, headers: _headers, ...semantic} = params;
      const request = hash(semantic);
      const cached = run.models[index];
      if (cached) {
        if (cached.request !== request) throw new Error('model replay diverged');
        return structuredClone(cached.response);
      }
      const generated = await doGenerate();
      if (!['stop', 'tool-calls'].includes(generated.finishReason.unified)) {
        throw new Error('provider did not complete a usable response');
      }
      // Keep semantic output and usage; omit raw HTTP request/response metadata.
      const response: Decision = {content: generated.content,
        finishReason: generated.finishReason, usage: generated.usage, warnings: []};
      run.models.push({request, response});
      await save();
      await hook?.('model');
      // No requested tool can execute until this decision has a durability proof.
      return structuredClone(response);
    },
    wrapStream: async () => { throw new Error('durable runner uses buffered generate only'); },
  }});
  const agent = new ToolLoopAgent({
    model, instructions, maxRetries: 0, maxOutputTokens: 1024,
    stopWhen: isStepCount(maxSteps), toolCallConcurrency: 1,
    tools: {increment: tool({
      description: 'Add an integer to the durable task counter. Only one distinct increment is allowed per task.',
      inputSchema: parameters,
      execute: async (args, {toolCallId}) => {
        try {
          if (failure) throw failure;
          parameters.parse(args);
          const key = hash({toolCallId}); // Model IDs never become object keys.
          const request = hash({name: 'increment', args});
          const cached = run.tools[key];
          if (cached) {
            if (cached.request !== request) throw new Error('tool replay diverged');
            return structuredClone(cached.result);
          }
          if (Object.keys(run.tools).length) throw new Error('task already has an increment');
          const before = await ctx.storage.read('count');
          const previous = before.present ? before.value : 0;
          if (typeof previous !== 'number' || !Number.isSafeInteger(previous) || previous < 0) {
            throw new Error('invalid stored counter');
          }
          const count = previous + args.amount;
          if (!Number.isSafeInteger(count)) throw new Error('counter limit reached');
          const result = {count};
          run.tools[key] = {request, result};
          // Effect + receipt share a checked Rust transaction. Unknown outcomes
          // are reconciled from storage on the next alarm, never blindly retried.
          await save({checks: {count: before}, puts: {count}});
          await hook?.('tool');
          return structuredClone(result);
        } catch (error) {
          failure = error;
          throw error;
        }
      },
    })},
  });
  const result = await agent.generate({prompt: run.prompt, abortSignal: AbortSignal.timeout(30_000)});
  if (failure) throw failure;
  if (result.finishReason !== 'stop' || !result.text.trim() || Object.keys(run.tools).length !== 1) {
    throw new Error('agent did not finish with one successful increment');
  }
  run.messages = [{role: 'user', content: run.prompt}, ...result.responseMessages];
  run.status = 'completed';
  await save();
  await hook?.('completed');
}
