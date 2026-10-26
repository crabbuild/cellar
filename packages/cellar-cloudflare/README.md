<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Optional Cloudflare compatibility runtime

Run selected Workers and Durable Object APIs above Cellar's native ownership,
SQLite and durability boundaries. This package is **experimental**; complete
Workers profiles, published Sandbox/image execution and remote providers remain
under development in the [delivery plan](../../plans/sandbox-provider-compatibility.md).

[Module example](#adapt-a-workers-module) · [API coverage](#api-coverage) ·
[Lifetime](#which-context-can-be-retained) · [Tests](#execution-evidence)

## How it works

```mermaid
flowchart TD
    App[Worker module and published packages] --> Facade[Optional JS compatibility facade]
    Facade --> Bridge[Bounded native invocation bridge]
    Bridge --> Storage[Cell-owned SQLite and alarms]
    Bridge --> Routing[Native object routing and RPC]
    Bridge --> Provider[Selected container provider]
    Provider --> Engine[Docker / compatible Podman]
```

The JS runtime serializes values and runs application callbacks. Native Cellar
executes SDK effects in their original invocation, including ownership/epoch
checks and durability/output gates. Native-only hosts remain engine-free.

## Adapt a Workers module

Keep application exports in their own module. A small Cellar entry declares the
bindings and classes that the native host makes available:

```js
// cellar-entry.js
import * as worker from "./worker.js";
import { workersApplication } from "@cellar/cloudflare";

const app = workersApplication(worker, {
  namespaces: { COUNTERS: "Counter" },
  variables: ["GREETING"],
});
export const { cells, fetch } = app;
```

```js
// worker.js
import { DurableObject } from "cloudflare:workers";

export class Counter extends DurableObject {
  controls() {
    return { read: () => this.ctx.storage.get("count") };
  }
  async fetch() {
    const count = (await this.ctx.storage.get("count") ?? 0) + 1;
    await this.ctx.storage.put("count", count);
    return Response.json({ greeting: this.env.GREETING, count });
  }
}

export default {
  fetch(request, env) {
    return env.COUNTERS.getByName("alice").fetch(request);
  },
};
```

The current fixture bundler maps `cloudflare:workers` to this package's `workers`
entry. The wrapper requires standard web globals at startup: Node supplies them;
embedded V8 refuses this HTTP module profile before the host starts.

| Native host declaration | Required value for this example |
| --- | --- |
| Source mode | `Source::module().workers_lifetimes()` |
| Root events | `EventKind::Fetch` |
| Class registration | `Counter` |
| Class events | `Fetch`, `Alarm`, `CellRpc`, as used by the application |
| Namespace binding | `COUNTERS` → `Counter` |
| Variable | `GREETING` configured in native host variables |

The wrapper declares adapters, not host permissions. Native configuration must
independently register matching classes, namespaces, variables and container
images. See the [native module tests](../../crates/cellar-javascript/tests/workers_module.rs)
for complete host wiring.

For a published Container class, add its exported class name to `containers`:

```js
const app = workersApplication(worker, {
  namespaces: { MY_CONTAINER: "MyContainer" },
  containers: ["MyContainer"],
});
```

## API coverage

| Surface | Implemented slice | Still required |
| --- | --- | --- |
| Module dispatch | Default `fetch`; exported DO `fetch`, prototype RPC and `alarm`; named namespace and variable bindings | Complete loader, compatibility dates/flags and other binding kinds |
| HTTP | Node Request/Response conversion, bounded binary bodies, separate Set-Cookie headers, manual redirects, abortable namespace/port calls | Streaming bridge, WebSocket upgrades and embedded web bootstrap |
| Streams | Byte-only `IdentityTransformStream`, BYOB reads, backpressure and cancellation; ordinary responses expose `webSocket: null` | Full Workers stream/upgrade conformance |
| SQL | Real lazy SQLite cursors, iteration/raw iteration, `toArray`, `one`, column names, counters and database size | Broader SQLite/runtime profile qualification |
| KV | Sync `storage.kv`, async KV, batches, lists, alarm access and `sync` | PITR and full profile limits |
| Values | Native structured clone: cycles, binary, maps/sets, dates, bigint, undefined and errors; returned functions preserve graph identity through native leases | Callback arguments, transfers and returned `RpcTarget` |
| Constructor admission | `blockConcurrencyWhile`, real native input gate, activation reset on failure | Full abort/reconstruction qualification |
| Transactions | Native sync/async savepoints, nesting, rollback and callback return/throw identity | Full lifetime/deadline profile qualification |
| Object RPC | Native IDs/routing; inherited public prototype methods; clone values/errors; root-event returned function calls, nested functions, `dup()` and disposal | Property RPC, pipelining, returned `RpcTarget`, callback arguments, scoped callback imports and cross-event failure state |
| Event lifetime | Retained native drivers for `waitUntil`, transaction callbacks, outstanding SDK work and complete container monitors | Complete compatibility eviction/deadline policy |

Alarm callbacks serialize per object, including when a running handler rearms
its alarm. Other events can still make progress while that handler awaits work.
This follows the [single running alarm contract](https://developers.cloudflare.com/durable-objects/api/alarms/).

`WorkerEntrypoint` and `RpcTarget` base classes are importable. Defining these
classes does not implement remote target transport. The tracing export used by
the pinned Sandbox fallback is explicitly disabled.

### Returned functions

With `workers_lifetimes()` enabled, a cell method can return ordinary objects
containing functions. Calls execute in the exporting event's native driver.
They keep its epoch, deadline and SDK authority, and await its storage output
proof before returning a delegated result.

```js
// In the caller's event:
async function readCounter(env) {
  const controls = await env.COUNTERS.getByName("alice").controls();
  try {
    return await controls.read();
  } finally {
    controls[Symbol.dispose]();
  }
}
```

| Operation | Lifetime |
| --- | --- |
| `await fn(...)` | Routed native call; only value arguments are supported |
| `fn.dup()` | A separate native alias within the caller's event |
| `fn[Symbol.dispose]()` | Releases that alias; repeated disposal is harmless |
| `result[Symbol.dispose]()` | Releases the function stubs in that returned object |
| Caller completion/cancellation | Drops native imports, even if JS still retains them |
| Producer retirement | Revokes the exported group and its retained aliases |

This follows the [RPC ownership model](https://developers.cloudflare.com/workers/runtime-apis/rpc/lifecycle/)
for these returned-function operations. Export/import uses root events; existing
value RPC remains available in native transaction/block callbacks. Function
arguments, `RpcTarget`, property access, pipelining and complete disconnect/fault
semantics remain unqualified. Capability markers cannot be persisted as callable
values: only a live native session grants authority.

The stream extension follows the documented byte-forwarding and BYOB contract
for [IdentityTransformStream](https://developers.cloudflare.com/workers/runtime-apis/streams/transformstream/).
This API slice does not establish full Workers stream conformance.

### Container API

| API | Behavior |
| --- | --- |
| `running` | Synchronous authoritative provider observation under the current event |
| `start(options)` | Validates and admits a bounded native ticket; returns `void` before provider I/O |
| `signal(signal)` | Admits an ordered control; returns `void` |
| `monitor()` | `Promise<void>`; nonzero exits reject with `exitCode` |
| `destroy(reason?)` | Acknowledges cleanup; monitors resolve or receive the explicit reason |
| `getTcpPort(port).fetch(input, init?)` | Bounded upload, incremental byte response/BYOB reads after admitted startup; native cancellation |

Providers must opt into `synchronous_running` and `queued_lifecycle`. Native
admission permits at most 64 tickets, with aggregate JSON inputs bounded by the
host body limit. Tickets are invocation-owned and consumed once. Queued lifecycle
inside transactions and `blockConcurrencyWhile` currently refuses before effects.

## Which context can be retained?

| Value | Valid lifetime |
| --- | --- |
| `activateDurableObject` owner, its storage and module-provided `env` | Same live native activation; each call resolves that continuation's current event |
| `durableObjectNamespace(context, binding)` and its stubs | Supplied event only |
| Namespace/stub saved from module-provided DO `env` | Same activation; cannot be borrowed by another object |
| SQL cursor | Opening event, with bounded handles and retained payload bytes |
| Ordinary Cellar context | Original invocation only |

```mermaid
sequenceDiagram
    participant App as Application
    participant Driver as Native event driver
    participant Work as Background work
    App->>Driver: publish HTTP / value RPC result once
    Driver-->>App: response may finish
    Driver->>Work: retain original event for owned jobs
    Work-->>Driver: settle / fail / cancel
    Driver->>Driver: microtask checkpoint and recheck
    Driver->>Driver: revoke when drained or native deadline ends
```

`workers_lifetimes()` keeps authority attached to original native work after a
response or client disconnect. It never extends native deadlines or permits a
retired activation to submit effects. Explicit jobs and callback boundaries
share bounded admission; joined shutdown waits for owned work.

## Bounds and failure behavior

| Boundary | Contract |
| --- | --- |
| HTTP bodies / clone values | Host body limit; oversize streamed input is cancelled before dispatch |
| HTTP headers | At most 128 fields and 32 KiB of UTF-8 names/values |
| Storage batches / callbacks / background registration | Host batch limit |
| KV keys | Nonempty, well-formed Unicode; at most 2048 UTF-8 bytes |
| Private keys | Direct `__sys_` access refused; lists omit the reserved range |
| Lost or unrepresentable response after execution | Outcome may be unknown; earlier effects may have committed |
| Cancel after native submission | Stops waiting through native acknowledgement; does not prove remote rollback |

SQL retained-payload quotas exclude SQLite VM/page-cache allocations; they are
not process RSS limits. Unsupported capabilities throw explicitly. Native
transaction completion waits for commit/rollback acknowledgement; a lost response
cannot be reported as successful rollback.

Routed object RPC/HTTP and Workers container-port requests have independent
native operation admission. They can overlap without holding the local storage
mutation slot. Their native authority checks, output gates, byte/operation
quotas and unknown-outcome reporting still apply.

## Execution evidence

| Test boundary | What it proves |
| --- | --- |
| Node and V8 native Workers fixtures | Real SQLite/clone storage, native input gates, sync/async transactions, constructor refusal, value/function RPC, disposal, retained events and restart recovery |
| Node module fixture | HTTP object routing by named/opaque ID, activation-owned env, binary/cookie preservation, cross-object refusal and effects surviving an oversized response |
| Unchanged celld Container module + Containers 0.3.7 | Named, singleton and random routing through native cells and an engine protocol peer; separate container ownership and explicit joined cleanup |
| Published constructor fixtures | Containers 0.3.7 construction/scheduling in both engines; Sandbox 0.12.9 configuration/metadata recovery in Node |
| Pinned Sandbox image through Cellar | Unchanged SDK/application exec, file, background startup and later process-list assertions pass, followed by explicit destroy and joined shutdown |

The engine peer is a test server, not a running container image. The complete
published SDK/image baseline retains `conformance: not_run` in the input manifest.
Sleep/wake, complete hooks/timeouts, streamed RPC/Worker forwarding and upgrades, Sandbox previews and
the remaining SDK/image fault qualification
remain required, along with hosted providers.

```sh
node tools/sandbox/compat/build.mjs
npm test --prefix tools/javascript
cargo test --locked -p cellar-javascript --features embed,node \
  --test workers --test workers_module
```

See the [API inventory](../../tools/sandbox/compat/api-inventory.md),
[provider guide](../../crates/cellar-container/README.md) and
[implementation ledger](../../plans/sandbox-provider-progress.md) for precise
requirements and remaining work.
