// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import {createModels, type Api, type Model, type AssistantMessage} from '@earendil-works/pi-ai';
import {openaiProvider} from '@earendil-works/pi-ai/providers/openai';
import {AssistantMessageEventStream} from '@earendil-works/pi-ai/utils/event-stream';
import type {StreamFn} from '@earendil-works/pi-agent-core';

export interface Provider {model: Model<Api>; stream: StreamFn}

export function completed(message: AssistantMessage) {
  if (message.stopReason === 'pending') throw new Error('cannot replay a pending response');
  const stream = new AssistantMessageEventStream();
  if (message.stopReason === 'error' || message.stopReason === 'aborted') {
    stream.push({type: 'error', reason: message.stopReason, error: message});
  } else {
    stream.push({type: 'done', reason: message.stopReason, message});
  }
  stream.end(message);
  return stream;
}

export function message(model: Model<Api>, content: AssistantMessage['content'],
  stopReason: AssistantMessage['stopReason']): AssistantMessage {
  return {role: 'assistant', content, stopReason, api: model.api,
    provider: model.provider, model: model.id, timestamp: 0, durationMs: 0,
    usage: {input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0,
      cost: {input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0}}};
}

// This replaces only the provider. Pi's real Agent executes the tool loop.
export function fixtureProvider(): Provider {
  const model: Model<Api> = {id: 'cellar-fixture-v1', name: 'Deterministic fixture',
    api: 'openai-responses', provider: 'openai', baseUrl: 'https://fixture.invalid',
    reasoning: false, input: ['text'], contextWindow: 8192, maxTokens: 1024,
    cost: {input: 0, output: 0, cacheRead: 0, cacheWrite: 0}};
  return {model, stream: (_model, context) => {
    const result = context.messages.findLast(item => item.role === 'toolResult');
    return completed(message(model, result
      ? [{type: 'text', text: `Recorded result: ${JSON.stringify(result.content)}`}]
      : [{type: 'toolCall', id: 'increment-1', name: 'increment', arguments: {amount: 3}}],
    result ? 'stop' : 'toolUse'));
  }};
}

export function configuredProvider(): Provider {
  const name = process.env.CELLAR_PI_PROVIDER ?? 'fixture';
  if (name === 'fixture') return fixtureProvider();
  if (name !== 'openai') throw new Error('CELLAR_PI_PROVIDER must be fixture or openai');
  const id = process.env.CELLAR_PI_MODEL;
  if (!id || !process.env.OPENAI_API_KEY) throw new Error('set CELLAR_PI_MODEL and OPENAI_API_KEY');
  const models = createModels();
  models.setProvider(openaiProvider());
  const model = models.getModel('openai', id);
  if (!model) throw new Error('CELLAR_PI_MODEL is absent from the pinned Pi catalog');
  return {model, stream: models.streamSimple.bind(models)};
}
