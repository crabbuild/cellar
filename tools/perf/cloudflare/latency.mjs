// A transport diagnostic, not a Cellar request benchmark or a CI latency gate.
import assert from 'node:assert/strict';
import net from 'node:net';
import {once} from 'node:events';
import {performance} from 'node:perf_hooks';
import {WebSocketServer} from 'ws';
import {tunnel} from './peer.mjs';

const remote = new WebSocketServer({port: 0, host: '127.0.0.1'});
await once(remote, 'listening');
remote.on('connection', ws => ws.on('message', (bytes, binary) => {
  if (!binary) return;
  ws.send(`a:${bytes.length}`);
  ws.send(Buffer.alloc(100, 1));
  setTimeout(() => ws.send(Buffer.alloc(100, 2)), 1);
}));
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
client.setTimeout(10000, () => client.destroy());
const samples = [];
try {
  for (let request = 0; request < 100; request++) {
    const started = performance.now();
    const reply = new Promise((resolve, reject) => {
      let body = Buffer.alloc(0);
      const failed = error => {cleanup(); reject(error);};
      const closed = () => failed(new Error('relay closed before complete reply'));
      const cleanup = () => {
        client.off('data', received);
        client.off('error', failed);
        client.off('close', closed);
      };
      const received = bytes => {
        body = Buffer.concat([body, bytes]);
        if (body.length >= 200) {
          cleanup();
          resolve(body);
        }
      };
      client.on('data', received);
      client.once('error', failed);
      client.once('close', closed);
    });
    client.write('request');
    assert.deepEqual(await reply, Buffer.concat([Buffer.alloc(100, 1), Buffer.alloc(100, 2)]));
    samples.push(performance.now() - started);
  }
  const measured = samples.slice(10).sort((a, b) => a - b);
  console.log(JSON.stringify({
    diagnostic: 'split-response relay', samples: measured.length, warmup: 10,
    p50_ms: measured[Math.floor(measured.length * .5)],
    p99_ms: measured[Math.floor((measured.length - 1) * .99)],
  }));
} finally {
  client.destroy();
  for (const ws of remote.clients) ws.terminate();
  await Promise.all([
    new Promise(resolve => relay.close(resolve)),
    new Promise(resolve => remote.close(resolve)),
  ]);
}
