import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {mkdtemp, readFile, rm, access} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join, resolve} from 'node:path';
import {fileURLToPath} from 'node:url';
import {setTimeout as delay} from 'node:timers/promises';

const program = process.env.CELLAR_PI_HOST;
assert.ok(program, 'CELLAR_PI_HOST must name the built cellar-pi-agent-example executable');
const entry = fileURLToPath(new URL('./crash-fixture.mjs', import.meta.url));
const root = await mkdtemp(join(tmpdir(), 'cellar-pi-'));
const children = new Set();

async function until(check, description, timeout = 60_000) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    const value = await check();
    if (value) return value;
    await delay(50);
  }
  throw new Error(`Timed out: ${description}`);
}

async function start(boundary, generation) {
  let stdout = '', stderr = '';
  const child = spawn(resolve(program), ['--listen', '127.0.0.1:0', '--internal-listen', '127.0.0.1:0'], {
    env: {...process.env, CELLAR_NODE_ENTRY: entry, CELLAR_NODE_EXECUTABLE: process.execPath,
      CELLAR_NODE: `pi-${boundary}-${generation}`,
      CELLAR_TEST_DATA_DIR: join(root, `${boundary}-${generation}-data`),
      CELLAR_BUCKET: 'cellar-dev', CELLAR_INTERNAL_DEV_STORE: join(root, `${boundary}-bucket.sqlite`),
      CELLAR_DURABILITY: 'bucket', CELLAR_TOKIO_THREADS: '2', CELLAR_PLACEMENT_WEIGHT: '1',
      CELLAR_WAKER_TICK_MS: '250',
      CELLAR_REBALANCE_INTERVAL_MS: '0', CELLAR_HANDLER_BUDGET_S: '60',
      CELLAR_SHUTDOWN_TOTAL_MS: '8000', CELLAR_PI_PAUSE_AT: generation === 1 ? boundary : '',
      CELLAR_PI_MARKER: join(root, `${boundary}.marker`), CELLAR_PI_CALLS: join(root, `${boundary}.calls`),
      CELLAR_PI_COMPLETIONS: join(root, `${boundary}.completed`),
      RUST_LOG: 'warn'}, stdio: ['ignore', 'pipe', 'pipe'],
  });
  children.add(child);
  const exited = new Promise(resolve => child.once('exit', (code, signal) => resolve({code, signal})));
  let spawnError;
  child.on('error', error => { spawnError = error; });
  child.stdout.on('data', chunk => { stdout += chunk; });
  child.stderr.on('data', chunk => { stderr += chunk; });
  const addresses = await until(async () => {
    if (spawnError) throw spawnError;
    if (child.exitCode !== null) throw new Error(`Host exited: ${stdout}\n${stderr}`);
    const ingress = /cellar listening on (\S+)/.exec(stdout)?.[1];
    const internal = /cellar internal listening on (\S+)/.exec(stdout)?.[1];
    return ingress && internal ? {ingress: `http://${ingress}`, internal: `http://${internal}`} : null;
  }, 'host startup');
  await until(async () => {
    try { return (await fetch(`${addresses.ingress}/.well-known/cellar/health`)).ok; }
    catch { return false; }
  }, 'host readiness');
  return {...addresses, child, exited, logs: () => `${stdout}\n${stderr}`};
}

try {
  for (const boundary of ['accepted', 'provider', 'model', 'tool', 'final-model', 'completed']) {
    const first = await start(boundary, 1);
    const url = `${first.ingress}/agents/task`;
    // Acceptance can itself lose its response. Recovery uses the same task ID.
    const submission = fetch(url, {method: 'POST', body: JSON.stringify({prompt: 'Add three'})})
      .then(reply => { assert.ok(reply.ok); }, () => {});
    await until(async () => {
      if (first.child.exitCode !== null) throw new Error(first.logs());
      try { await access(join(root, `${boundary}.marker`)); return true; } catch { return false; }
    }, `checkpoint ${boundary}`);
    first.child.kill('SIGKILL');
    await first.exited;
    await submission;
    children.delete(first.child);

    // A different node and empty local directory must recover from the bucket.
    const second = await start(boundary, 2);
    const recoveredUrl = `${second.ingress}/agents/task`;
    const expectedCalls = boundary === 'provider' ? 3 : 2;
    // Do not route a task request to wake the cell. The durable alarm must
    // resume it independently after the former owner's lease expires.
    await until(async () => {
      try {
        return (await readFile(join(root, `${boundary}.completed`), 'utf8')).trim() === 'completed';
      } catch { return false; }
    }, 'recovery without a task HTTP request');
    const result = await until(async () => {
      const reply = await fetch(recoveredUrl);
      if (!reply.ok) return null; // Prior owner lease may still need to expire.
      const value = await reply.json();
      if (value.status === 'failed') throw new Error(`Agent failed: ${JSON.stringify(value)}\n${second.logs()}`);
      return value.status === 'completed' ? value : null;
    }, 'alarm-driven recovery');
    assert.equal(result.count, 3);
    assert.equal(result.toolReceipts, 1);
    assert.equal(result.modelSteps, 2);
    assert.ok(result.messages.some(message => message.role === 'toolResult'));
    const calls = (await readFile(join(root, `${boundary}.calls`), 'utf8')).trim().split('\n');
    assert.equal(calls.length, expectedCalls, 'only an uncheckpointed provider request may repeat');
    const repeat = await fetch(recoveredUrl, {method: 'POST', body: JSON.stringify({prompt: 'Add three'})});
    assert.equal(repeat.status, 200);
    const conflict = await fetch(recoveredUrl, {method: 'POST', body: JSON.stringify({prompt: 'Add four'})});
    assert.equal(conflict.status, 409);
    assert.equal((await (await fetch(recoveredUrl)).json()).count, 3);
    assert.equal((await fetch(`${second.ingress}/agents/other`)).status, 404);
    const shutdown = await fetch(`${second.internal}/shutdown`, {method: 'POST'});
    assert.ok(shutdown.ok);
    const outcome = await Promise.race([second.exited, delay(15_000, undefined, {ref: false}).then(() => { throw new Error('shutdown timeout'); })]);
    assert.equal(outcome.code, 0, second.logs());
    children.delete(second.child);
    console.log(`PASS ${boundary}: SIGKILL, cold bucket recovery, one tool effect, ${expectedCalls} provider calls, idempotent retry`);
  }
} finally {
  await Promise.all([...children].map(async child => {
    if (child.exitCode === null && child.signalCode === null) {
      const exited = new Promise(resolve => child.once('exit', resolve));
      child.kill('SIGKILL');
      await exited;
    }
  }));
  await rm(root, {recursive: true, force: true});
}
