import {createOpenAI} from '@ai-sdk/openai';
import type {wrapLanguageModel} from 'ai';

export type Model = ReturnType<typeof wrapLanguageModel>;
export type Decision = Awaited<ReturnType<Model['doGenerate']>>;

// Replace only the provider: the real AI SDK still validates and executes tools
// and constructs the conversation. This fixture always increments by three.
export function fixtureModel(): Model {
  const model: Model = {
    specificationVersion: 'v4', supportedUrls: {},
    provider: 'cellar-fixture', modelId: 'increment-three-v1',
    doGenerate: async ({prompt}) => {
      const result = prompt.findLast(message => message.role === 'tool');
      return {
        content: result
          ? [{type: 'text', text: `Recorded result: ${JSON.stringify(result.content)}`}]
          : [{type: 'tool-call', toolCallId: 'increment-1', toolName: 'increment', input: '{"amount":3}'}],
        finishReason: {unified: result ? 'stop' : 'tool-calls', raw: undefined},
        usage: {
          inputTokens: {total: 0, noCache: 0, cacheRead: undefined, cacheWrite: undefined},
          outputTokens: {total: 0, text: 0, reasoning: undefined},
        },
        warnings: [],
      };
    },
    doStream: async params => {
      const result = await model.doGenerate(params);
      return {stream: new ReadableStream({start(controller) {
        controller.enqueue({type: 'stream-start', warnings: []});
        for (const part of result.content) {
          if (part.type === 'text') {
            controller.enqueue({type: 'text-start', id: 'answer'});
            controller.enqueue({type: 'text-delta', id: 'answer', delta: part.text});
            controller.enqueue({type: 'text-end', id: 'answer'});
          } else if (part.type === 'tool-call') controller.enqueue(part);
        }
        controller.enqueue({type: 'finish', finishReason: result.finishReason, usage: result.usage});
        controller.close();
      }})};
    },
  };
  return model;
}

export function configuredModel(): Model {
  const provider = process.env.CELLAR_AI_PROVIDER ?? 'fixture';
  if (provider === 'fixture') return fixtureModel();
  if (provider !== 'openai') throw new Error('CELLAR_AI_PROVIDER must be fixture or openai');
  const id = process.env.CELLAR_AI_MODEL;
  if (!id || !process.env.OPENAI_API_KEY) throw new Error('set CELLAR_AI_MODEL and OPENAI_API_KEY');
  return createOpenAI({baseURL: process.env.OPENAI_BASE_URL})(id);
}
