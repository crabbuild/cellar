// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {mkdtemp, readFile, rm, access, writeFile, mkdir, readdir} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join, resolve} from 'node:path';
import {fileURLToPath} from 'node:url';
import {setTimeout as delay} from 'node:timers/promises';
import {DatabaseSync} from 'node:sqlite';

const program = process.env.CELLAR_PI_HOST;
assert.ok(program, 'CELLAR_PI_HOST must name the built cellar-pi-agent-example executable');
const live = process.env.CELLAR_PI_TEST_LIVE === '1';
if (live) {
  assert.equal(process.env.CELLAR_PI_PROVIDER, 'openai', 'live test requires CELLAR_PI_PROVIDER=openai');
  assert.ok(process.env.OPENAI_API_KEY && process.env.CELLAR_PI_MODEL, 'live test needs a key and model');
}
const evidence = [];
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
  const filesystem = boundary.startsWith('filesystem');
  const data = join(root, `${boundary}-${generation}-data`);
  await mkdir(data);
  assert.deepEqual(await readdir(data), [], 'every node must start with empty local storage');
  let stdout = '', stderr = '';
  const child = spawn(resolve(program), ['--listen', '127.0.0.1:0', '--internal-listen', '127.0.0.1:0'], {
    env: {...process.env, CELLAR_NODE_ENTRY: entry, CELLAR_NODE_EXECUTABLE: process.execPath,
      CELLAR_NODE: `pi-${boundary}-${generation}`,
      CELLAR_TEST_DATA_DIR: data,
      CELLAR_PI_FILESYSTEM: filesystem ? '1' : '0',
      CELLAR_PI_TEST_LIVE: filesystem ? '0' : (live ? '1' : '0'),
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
  return {...addresses, child, exited, data, logs: () => `${stdout}\n${stderr}`};
}

async function fixtureFiles(data) {
  const paths = (await readdir(data, {recursive: true})).filter(path => path.startsWith('DurableAgent:') && path.endsWith('/db.sqlite'));
  assert.equal(paths.length, 1);
  const db = new DatabaseSync(join(data, paths[0]), {readOnly: true});
  try {
    return db.prepare(`SELECT CAST(i.id AS TEXT) AS inode, CAST(i.version AS TEXT) AS version,
      (SELECT substr(data,1,min(i.size,4096)) FROM __sys_fs_chunks WHERE inode=i.id AND chunk_index=0) AS prefix
      FROM __sys_fs_entries e JOIN __sys_fs_inodes i ON i.id=e.inode
      WHERE e.parent=1 AND i.kind=0 ORDER BY e.name LIMIT 32`).all()
      .map(row => ({...row, prefix: row.prefix === null ? null : Buffer.from(row.prefix).toString('base64')}));
  } finally { db.close(); }
}

try {
  const boundaries = [...(live ? ['tool', 'completed'] : ['accepted', 'provider', 'model', 'tool', 'final-model', 'completed']),
    'filesystem', 'filesystem-read'];
  for (const boundary of boundaries) {
    const filesystem = boundary.startsWith('filesystem');
    const prompt = filesystem ? 'Exercise the bounded cell filesystem tools, including a stale-version refusal.' : 'Add three';
    const first = await start(boundary, 1);
    const url = `${first.ingress}/agents/task`;
    // Acceptance can itself lose its response. Recovery uses the same task ID.
    const submission = fetch(url, {method: 'POST', body: JSON.stringify({prompt})})
      .then(reply => { assert.ok(reply.ok); }, () => {});
    await until(async () => {
      if (first.child.exitCode !== null) throw new Error(first.logs());
      try { await access(join(root, `${boundary}.marker`)); return true; } catch { return false; }
    }, `checkpoint ${boundary}`);
    first.child.kill('SIGKILL');
    await first.exited;
    await submission;
    children.delete(first.child);
    const capturedFiles = filesystem ? await fixtureFiles(first.data) : undefined;
    await rm(first.data, {recursive: true});
    await assert.rejects(access(first.data), {code: 'ENOENT'});

    // A different node and empty local directory must recover from the bucket.
    const second = await start(boundary, 2);
    const recoveredUrl = `${second.ingress}/agents/task`;
    const expectedCalls = filesystem ? 5 : (boundary === 'provider' ? 3 : 2);
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
    assert.equal(result.count, filesystem ? 0 : 3);
    assert.equal(result.toolReceipts, filesystem ? 9 : 1);
    assert.equal(result.modelSteps, filesystem ? 5 : 2);
    assert.ok(result.messages.some(message => message.role === 'toolResult'));
    if (filesystem) {
      const replies = result.messages.filter(item => item.role === 'toolResult');
      const original = replies.find(item => item.toolCallId === 'call-readFile').details;
      assert.equal(original.text, 'first contents');
      assert.equal(replies.find(item => item.toolCallId === 'call-read-renamed').details.text, 'other contents');
      const refusal = replies.find(item => item.toolCallId === 'call-stale-edit');
      assert.equal(refusal.isError, true);
      assert.deepEqual(refusal.details.error, {code: 'Conflict', errno: 'EBUSY', outcome: 'not_committed'});
      assert.equal(replies.find(item => item.toolName === 'searchFiles').details.matches.length, 1);
      assert.equal(replies.find(item => item.toolName === 'listFiles').details.entries.length, 1);
      assert.equal(capturedFiles[0].inode, original.inode);
      if (boundary === 'filesystem') {
        assert.equal(capturedFiles[0].version, original.version, 'recovery must replay the receipt without rewriting the file');
        assert.equal(Buffer.from(capturedFiles[0].prefix, 'base64').toString(), 'first contents');
      } else {
        assert.ok(BigInt(capturedFiles[0].version) > BigInt(original.version));
        assert.equal(Buffer.from(capturedFiles[0].prefix, 'base64').toString(), 'other contents');
      }
      assert.deepEqual(await fixtureFiles(second.data), []);
    }
    if (live && !filesystem) {
      const assistants = result.messages.filter(message => message.role === 'assistant');
      assert.equal(assistants.length, 2);
      assert.ok(assistants.every(message => message.provider === 'openai'
        && message.model === process.env.CELLAR_PI_MODEL && message.usage.totalTokens > 0));
      assert.ok(assistants.at(-1).content.some(part => part.type === 'text' && /3|three/i.test(part.text)));
    }
    const calls = (await readFile(join(root, `${boundary}.calls`), 'utf8')).trim().split('\n');
    assert.equal(calls.length, expectedCalls, 'only an uncheckpointed provider request may repeat');
    const repeat = await fetch(recoveredUrl, {method: 'POST', body: JSON.stringify({prompt})});
    assert.equal(repeat.status, 200);
    const conflict = await fetch(recoveredUrl, {method: 'POST', body: JSON.stringify({prompt: 'Add four'})});
    assert.equal(conflict.status, 409);
    assert.deepEqual(await (await fetch(recoveredUrl)).json(), result);
    assert.equal((await fetch(`${second.ingress}/agents/other`)).status, 404);
    const shutdown = await fetch(`${second.internal}/shutdown`, {method: 'POST'});
    assert.ok(shutdown.ok);
    const outcome = await Promise.race([second.exited, delay(15_000, undefined, {ref: false}).then(() => { throw new Error('shutdown timeout'); })]);
    assert.equal(outcome.code, 0, second.logs());
    children.delete(second.child);
    evidence.push({boundary, providerCalls: calls.length, capturedFiles, emptyLocalRecovery: true, result});
    console.log(`PASS ${boundary}: SIGKILL, cold bucket recovery, ${result.toolReceipts} tool receipts, ${expectedCalls} provider calls, idempotent retry`);
  }
  if (process.env.CELLAR_PI_TEST_REPORT) {
    await writeFile(process.env.CELLAR_PI_TEST_REPORT, JSON.stringify({live, evidence}, null, 2));
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
