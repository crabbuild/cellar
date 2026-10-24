import test from 'node:test';
import assert from 'node:assert/strict';
import {streamDecision} from '../dist/streaming.js';
import {fixtureModel} from '../dist/provider.js';

test('real provider stream text is visible while the complete decision is still withheld', async () => {
  const model = fixtureModel();
  let release, controller;
  const gate = new Promise(resolve => { release = resolve; });
  const usage = (await model.doGenerate({prompt: [], tools: []})).usage;
  model.doStream = async () => ({stream: new ReadableStream({start(c) {
    controller = c;
    c.enqueue({type: 'text-start', id: 'text'});
    c.enqueue({type: 'text-delta', id: 'text', delta: 'First live text'});
  }})});
  const previews = [];
  let done = false;
  const running = streamDecision(model, {prompt: []}, text => { previews.push(text); release(); }).then(result => { done = true; return result; });
  await gate;
  assert.equal(done, false);
  assert.deepEqual(previews, ['First live text']);
  controller.enqueue({type: 'text-delta', id: 'text', delta: ' and final text'});
  controller.enqueue({type: 'text-end', id: 'text', providerMetadata: {openai: {itemId: 'saved-id'}}});
  controller.enqueue({type: 'finish', usage, finishReason: {unified: 'stop'}});
  controller.close();
  const result = await running;
  assert.equal(result.content[0].text, 'First live text and final text');
  assert.equal(result.content[0].providerMetadata.openai.itemId, 'saved-id');
});

for (const mode of ['missing-finish', 'error', 'provider-tool']) test(`incomplete or unsupported provider output is refused: ${mode}`, async () => {
  const model = fixtureModel();
  model.doStream = async () => ({stream: new ReadableStream({start(c) {
    if (mode === 'error') c.enqueue({type: 'error', error: new Error('private diagnostic')});
    if (mode === 'provider-tool') c.enqueue({type: 'tool-call', providerExecuted: true, toolCallId: 'id', toolName: 'external', input: '{}'});
    c.close();
  }})});
  await assert.rejects(streamDecision(model, {prompt: []}, () => {}));
});
