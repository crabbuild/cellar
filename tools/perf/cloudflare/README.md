<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Cloudflare peer relay

This helper carries native Cellar peer TCP connections over authenticated
WebSockets in the Cloudflare counter benchmark. The counter and its durability
protocol run in Rust. Import `tunnel` in the container's Node supervisor and
pass its accepted TCP socket, destination node index and fleet endpoint:

```js
import net from 'node:net';
import {tunnel} from './peer.mjs';

net.createServer(tcp => tunnel(tcp, destinationIndex, {
  index: localIndex,
  origin: process.env.PEER_ORIGIN,
  token: process.env.PEER_TOKEN,
})).listen(localPeerPort, '127.0.0.1');
```

The Worker counterpart forwards binary WebSocket messages to its container's
internal TCP port. Text messages `a:<bytes>` return credit only after the
receiver completes its TCP write. Each direction has a 256 KiB credit window
and sends at most 32 KiB per message. The relay preserves native protocol bytes;
transport credit never acknowledges an application write.

The accepted TCP socket and the local direct connection use `TCP_NODELAY`.
Without it, small response fragments can wait for delayed TCP acknowledgements
even when Cellar and the WebSocket connection already write eagerly.
See Node's [`socket.setNoDelay` documentation](https://nodejs.org/api/net.html#socketsetnodelaynodelay).

```sh
npm ci --ignore-scripts --prefix tools/perf/cloudflare
npm test --prefix tools/perf/cloudflare
npm run bench --prefix tools/perf/cloudflare
```

Tests exercise split replies over a reused connection, both directions beyond
the credit window, and refusal of invalid credit. They assert byte and resource
behavior rather than machine-dependent latency thresholds. Benchmark deployment
configuration, credentials and cell inventories stay outside this directory.
The optional benchmark measures split replies through this relay, with ten
warmup exchanges and 90 measured exchanges. Compare revisions on the same host;
its timings are not counter throughput or Cloudflare latency measurements.
