import assert from 'node:assert/strict';
import test from 'node:test';
import {accept, execute, readRun} from '../dist/durable.js';
import {fixtureProvider, completed, message} from '../dist/provider.js';

function storage() {
  const data = new Map();
  let alarm;
  const ctx = {now: async () => 1000, storage: {
    read: async key => data.has(key) ? {present: true, value: structuredClone(data.get(key))} : {present: false, value: null},
    apply: async batch => {
      for (const [key, expected] of Object.entries(batch.checks ?? {})) {
        assert.deepEqual(await ctx.storage.read(key), expected, 'storage check failed');
      }
      for (const [key, value] of Object.entries(batch.puts ?? {})) data.set(key, structuredClone(value));
      if (batch.alarm_at_ms !== undefined) alarm = batch.alarm_at_ms;
      if (batch.delete_alarm) alarm = undefined;
    }, sync: async () => {},
  }};
  return {ctx, data, alarm: () => alarm};
}

for (const boundary of ['model', 'tool', 'final-model', 'completed']) {
  test(`replay after interruption at ${boundary} keeps one effect and reuses decisions`, async () => {
    const {ctx, data, alarm} = storage();
    const provider = fixtureProvider();
    const stream = provider.stream;
    let calls = 0;
    provider.stream = (...args) => { calls++; return stream(...args); };
    await accept(ctx, 'Add three', provider);
    let models = 0;
    await assert.rejects(execute(ctx, provider, async point => {
      if (point === 'tool') await new Promise(resolve => setTimeout(resolve, 15));
      if (point === 'model' && ++models === 2 && boundary === 'final-model') throw new Error('injected interruption');
      if (point === boundary) throw new Error('injected interruption');
    }), /injected interruption/);
    await execute(ctx, provider);
    const run = await readRun(ctx);
    assert.equal(run.status, 'completed');
    assert.equal(data.get('count'), 3);
    assert.equal(Object.keys(run.tools).length, 1);
    assert.equal(calls, 2);
    assert.equal(alarm(), undefined);
    assert.ok(run.messages.some(item => item.role === 'toolResult'));
    await execute(ctx, provider);
    assert.equal(calls, 2);
  });
}

test('a lost acknowledgement after atomic tool commit is reconciled on replay', async () => {
  const {ctx, data} = storage();
  const provider = fixtureProvider();
  await accept(ctx, 'Add three', provider);
  let lost = false;
  ctx.storage.sync = async () => {
    if (data.has('count') && !lost) { lost = true; throw new Error('unknown outcome'); }
  };
  await assert.rejects(execute(ctx, provider), /unknown outcome/);
  await execute(ctx, provider);
  assert.equal(data.get('count'), 3);
  assert.equal((await readRun(ctx)).status, 'completed');
});

test('task acceptance is idempotent and rejects prompt reuse', async () => {
  const {ctx} = storage();
  const provider = fixtureProvider();
  await accept(ctx, 'Add three', provider);
  assert.equal(await accept(ctx, 'Add four', provider), null);
  await execute(ctx, provider);
  assert.equal((await accept(ctx, 'Add three', provider)).status, 'completed');
});

test('duplicate acceptance and status reads require a durability proof', async () => {
  const {ctx} = storage();
  const provider = fixtureProvider();
  await accept(ctx, 'Add three', provider);
  ctx.storage.sync = async () => { throw new Error('durability unavailable'); };
  await assert.rejects(accept(ctx, 'Add three', provider), /durability unavailable/);
  await assert.rejects(readRun(ctx), /durability unavailable/);
});

test('retry exhaustion preserves journal and configuration changes refuse execution', async () => {
  const {ctx, alarm} = storage();
  const provider = fixtureProvider();
  await accept(ctx, 'Add three', provider);
  const different = fixtureProvider();
  different.model.id = 'different-release';
  await assert.rejects(execute(ctx, different), /configuration changed/);
  provider.stream = model => completed({...message(model, [], 'error'), errorMessage: 'unavailable'});
  for (let n = 0; n < 5; n++) await assert.rejects(execute(ctx, provider));
  await execute(ctx, provider);
  assert.equal((await readRun(ctx)).status, 'failed');
  assert.equal(alarm(), undefined);
});

test('replay rejects a changed recorded request before further effects', async () => {
  const {ctx, data} = storage();
  const provider = fixtureProvider();
  await accept(ctx, 'Add three', provider);
  await assert.rejects(execute(ctx, provider, async () => { throw new Error('stop'); }));
  data.get('run').models[0].request = 'wrong';
  await assert.rejects(execute(ctx, provider), /replay diverged/);
  assert.equal(data.has('count'), false);
});
