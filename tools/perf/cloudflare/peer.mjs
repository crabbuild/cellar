// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import net from 'node:net';
import WebSocket from 'ws';

const WINDOW = 256 * 1024;
const CHUNK = 32 * 1024;

export const stats = {opened: 0, closed: 0, active: 0, tx: 0, rx: 0, failures: 0};

// Carry native Cellar peer bytes; ownership, framing and durability stay in Rust.
export function tunnel(tcp, target, {index, origin, token}) {
  // Small response fragments must leave immediately. Nagle on this accepted
  // socket otherwise waits for delayed ACKs, even though Cellar's sockets and
  // the WebSocket connection already disable it.
  tcp.setNoDelay(true);
  if (target === index) {
    const remote = net.connect(8081, '127.0.0.1');
    remote.setNoDelay(true);
    remote.on('error', () => tcp.destroy());
    tcp.on('error', () => remote.destroy());
    tcp.pipe(remote).pipe(tcp);
    tcp.on('close', () => remote.destroy());
    return;
  }

  tcp.pause();
  const ws = new WebSocket(`${origin.replace('https:', 'wss:')}/peer/node${target}`, {
    headers: {'User-Agent': 'cellar-counter-benchmark/1.0', Authorization: `Bearer ${token}`},
    perMessageDeflate: false,
    maxPayload: WINDOW,
    handshakeTimeout: 15000,
  });
  let credit = WINDOW;
  let waiter = null;
  let pending = 0;
  let closed = false;
  let chain = Promise.resolve();

  const stop = (failed = false) => {
    if (closed) return;
    closed = true;
    stats.closed++;
    stats.active--;
    if (failed) stats.failures++;
    waiter?.();
    tcp.destroy();
    ws.terminate();
  };
  stats.opened++;
  stats.active++;
  ws.on('error', () => stop(true));
  tcp.on('error', () => stop(true));
  ws.on('close', () => stop());
  tcp.on('close', () => stop());
  tcp.on('end', () => stop());
  ws.on('open', () => tcp.resume());

  ws.on('message', (data, binary) => {
    if (!binary) {
      const text = data.toString();
      if (!/^a:[0-9]+$/.test(text)) return stop(true);
      const count = Number(text.slice(2));
      if (count <= 0 || credit + count > WINDOW) return stop(true);
      credit += count;
      waiter?.();
      waiter = null;
      return;
    }
    pending += data.length;
    if (pending > WINDOW) return stop(true);
    // Credit follows completed TCP writes, so a slow receiver bounds buffering.
    chain = chain
      .then(() => new Promise((resolve, reject) => tcp.write(data, error => error ? reject(error) : resolve())))
      .then(() => {
        pending -= data.length;
        stats.rx += data.length;
        if (!closed) ws.send(`a:${data.length}`);
      })
      .catch(() => stop(true));
  });

  tcp.on('data', async data => {
    tcp.pause();
    try {
      for (let offset = 0; offset < data.length;) {
        while (credit === 0 && !closed) await new Promise(resolve => {waiter = resolve;});
        if (closed) return;
        const size = Math.min(CHUNK, credit, data.length - offset);
        credit -= size;
        const part = data.subarray(offset, offset + size);
        await new Promise((resolve, reject) => ws.send(part, {binary: true}, error => error ? reject(error) : resolve()));
        stats.tx += size;
        offset += size;
      }
      if (!closed) tcp.resume();
    } catch {
      stop(true);
    }
  });
}
