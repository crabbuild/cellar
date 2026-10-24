// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import assert from 'node:assert/strict';
import net from 'node:net';
import {once} from 'node:events';
import {setImmediate} from 'node:timers/promises';
import test from 'node:test';
import {WebSocketServer} from 'ws';
import {stats, tunnel} from './peer.mjs';

const WINDOW = 256 * 1024;

async function fixture(t, onConnection) {
  const remote = new WebSocketServer({port: 0, host: '127.0.0.1'});
  await once(remote, 'listening');
  remote.on('connection', (ws, request) => {
    assert.equal(request.headers.authorization, 'Bearer fixture');
    onConnection(ws);
  });
  const relay = net.createServer(tcp => tunnel(tcp, 2, {
    index: 1,
    origin: `ws://127.0.0.1:${remote.address().port}`,
    token: 'fixture',
  }));
  relay.listen(0, '127.0.0.1');
  await once(relay, 'listening');
  const client = net.connect(relay.address().port, '127.0.0.1');
  client.setNoDelay(true);
  await once(client, 'connect');
  t.after(async () => {
    client.destroy();
    for (const ws of remote.clients) ws.terminate();
    await Promise.all([
      new Promise(resolve => relay.close(resolve)),
      new Promise(resolve => remote.close(resolve)),
    ]);
  });
  return client;
}

function readBytes(client, length) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let received = 0;
    const data = chunk => {
      chunks.push(chunk);
      received += chunk.length;
      if (received >= length) {
        cleanup();
        resolve(Buffer.concat(chunks));
      }
    };
    const closed = () => {cleanup(); reject(new Error('relay closed before complete response'));};
    const failed = error => {cleanup(); reject(error);};
    const cleanup = () => {
      client.off('data', data);
      client.off('close', closed);
      client.off('error', failed);
    };
    client.on('data', data);
    client.once('close', closed);
    client.once('error', failed);
  });
}

test('split replies preserve ordering and reuse one tunnel', {timeout: 10000}, async t => {
  let calls = 0;
  const opened = stats.opened;
  const client = await fixture(t, ws => {
    ws.on('message', async (bytes, binary) => {
      if (!binary) return;
      assert.equal(bytes.toString(), 'request');
      ws.send(`a:${bytes.length}`);
      const value = ++calls;
      ws.send(Buffer.alloc(17, value));
      await setImmediate();
      ws.send(Buffer.alloc(31, value));
    });
  });
  for (let value = 1; value <= 12; value++) {
    const reply = readBytes(client, 48);
    client.write('request');
    assert.deepEqual(await reply, Buffer.alloc(48, value));
  }
  assert.equal(stats.opened - opened, 1);
});

test('both directions transfer beyond the credit window without losing bytes', {timeout: 10000}, async t => {
  const request = Buffer.alloc(WINDOW * 3 + 71, 67);
  const response = Buffer.alloc(WINDOW * 2 + 37, 83);
  const client = await fixture(t, ws => {
    const parts = [];
    let received = 0;
    let sent = 0;
    let credit = WINDOW;
    const pump = () => {
      if (received !== request.length) return;
      while (credit && sent < response.length) {
        const length = Math.min(32 * 1024, credit, response.length - sent);
        ws.send(response.subarray(sent, sent + length));
        credit -= length;
        sent += length;
      }
    };
    ws.on('message', (bytes, binary) => {
      if (binary) {
        parts.push(bytes);
        received += bytes.length;
        assert.ok(received <= request.length);
        ws.send(`a:${bytes.length}`);
        if (received === request.length) assert.deepEqual(Buffer.concat(parts), request);
      } else {
        assert.match(bytes.toString(), /^a:[0-9]+$/);
        credit += Number(bytes.toString().slice(2));
        assert.ok(credit <= WINDOW);
      }
      pump();
    });
  });
  const reply = readBytes(client, response.length);
  client.write(request);
  assert.deepEqual(await reply, response);
});

test('invalid or excessive credit closes the tunnel', {timeout: 10000}, async t => {
  for (const credit of ['a:0', `a:${WINDOW + 1}`, 'not-credit']) {
    const client = await fixture(t, ws => ws.send(credit));
    client.on('error', () => {});
    await once(client, 'close');
  }
});
