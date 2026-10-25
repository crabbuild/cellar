<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Optional Cloudflare compatibility runtime

This package is the Workers/DO runtime implementation under development for the
[sandbox delivery plan](../../plans/sandbox-provider-compatibility.md). It is
not yet a complete Workers profile or a runnable replacement for Wrangler.
The [implementation ledger](../../plans/sandbox-provider-progress.md) tracks
required runtime, SDK/image and provider acceptance separately.

The current slice implements an activation-owned DO storage facade and
constructor admission above the original native Cellar invocation:

- Real lazy SQLite cursors: `exec`, iteration/raw iteration, `toArray`, `one`,
  column names, read/write counters and database size. Statements and handles
  stay in native SQLite. Cursors are bounded and owned by their opening event;
  retained Rust column/row/error payloads share a byte quota. This excludes
  SQLite VM/page-cache allocations and is not a process memory limit.
- Synchronous KV and async KV wrappers, multi-key operations, lists, alarm
  reads/writes and native storage synchronization. Stored values use Cellar's
  engine-free structured-clone codec; Node/V8 serialize only inside the optional
  language runtime. Cycles, binary buffers, maps, dates and bigint retain value
  semantics. Transfer/delegate objects and stored RPC targets require their own
  capabilities and fail explicitly.
- A state facade that can be retained across events of one native activation.
  Another cell, cancelled event or retired activation cannot borrow its storage
  authority. Ordinary saved Cellar contexts remain invocation-owned.
- Native object identity and optional random-values/UUID primitives for V8
  scheduling paths. Crypto resolves the current event, with integer-array brand
  checks and the 64 KiB random-values quota; full Web Crypto and bootstrap
  globals remain separate work.
- Constructor `blockConcurrencyWhile` admission: await initialization before
  invoking application methods, acquire the real native input gate and reset
  the actual activation on callback failure. Callbacks remain attached to their
  original native event.
- Async `storage.transaction`: native savepoints, nested transactions, explicit
  rollback and callback return/throw values. SQL and KV share the same native
  transaction view across Node/V8 awaits. Unrelated promises cannot borrow it;
  escaped transaction views and closed callback continuations refuse effects.
  Completion waits for the native commit/rollback, preserving output gates.
- `storage.transactionSync`: native synchronous SQL/KV transactions, nested
  savepoints and inline return/throw values. Its bounded callback loop accepts
  only synchronous calls from the current callback, refusing async SDK effects
  and native Promise results. The SQLite transaction stays on the original
  native host thread until commit/rollback; no writes are replayed from JS.
- `Source::workers_lifetimes()` opts the native host into retained event drivers.
  `ctx.waitUntil(promise)`, async transaction callbacks and outstanding SDK calls
  retain their original invocation after a response or client disconnect. HTTP
  and value-only RPC results publish before background drainage; native deadlines,
  retirement and joined shutdown still govern that work. Explicit `waitUntil`
  registrations are bounded by the host's batch limit. This switch
  selects lifetime behavior, not a complete versioned Workers API profile.
  A once-only response publication has a reserved slot and does not consume the
  ordinary SDK operation/pending budgets; ordinary effects remain bounded.
- Native namespace ID derivation/validation and event-owned stubs for value-only
  cell RPC. Actual native `Invocation::CellRpc` / `Output::Rpc` carry clone graphs;
  calls retain native routing, ownership, durability and cancellation behavior.
  Prototype methods (including inherited methods) are callable; instance fields,
  arrow properties, getters, system handlers and base-object methods are refused.
  Remote thrown values and error properties/causes survive; error stacks are
  removed. Encoding/size failures after execution remain unknown outcomes.
- TypeScript declarations for this implemented slice, including typed method
  arguments/results and storage. Synchronous callbacks returning Promises are
  refused by the declarations as well as at runtime.

`activateDurableObject` owns activation storage and exposes `rpc(method, args)`
for prototype-method dispatch. The `workers` entry supplies the imported base classes and
an explicitly disabled tracing export used by the pinned Sandbox fallback.
Defining `RpcTarget` does not implement RPC transport or remote object lifetime.

The complete Worker module loader, HTTP/web globals, automatic activation-owned
namespace environment bindings, property RPC, RPC targets,
PITR, full lifetime/profile deadline qualification, abort/reconstruction,
container lifecycle and Sandbox control-server integration remain required.
Unavailable APIs throw; they must not silently succeed or lose transaction,
network policy or cancellation guarantees. Current storage list/batch operations
are bounded by the host batch limit, and cloned values/results by the host body
limit. Keys must be nonempty, well-formed Unicode strings up to 2048 UTF-8 bytes.
Direct access to `__sys_` keys is refused and lists omit that range without
charging it against the visible result limit. Native `deleteAll` semantics apply
to application SQL/KV state; host/runtime metadata belongs in protected system
tables. This is an explicit partial runtime
slice, not a full Cloudflare SDK conformance claim.

## Current execution evidence

`node tools/sandbox/compat/build.mjs` verifies the pinned upstream input inventory
and builds genuine SDK constructor/storage fixtures. `cargo test --locked -p
cellar-javascript --features embed,node --test workers` executes them through real
native nodes, temporary cell databases and configured provider observation. The
container view uses `durableObjectContainer(ctx)`: its synchronous `running`
getter reads the native provider on every access under the current event's
activation authority. Providers must opt in with `synchronous_running`; an
async-only provider refuses instead of returning a guessed or cached state.
The facade also supplies queued lifecycle controls for root events with
`Source::workers_lifetimes()` enabled:

| API | Current behavior |
| --- | --- |
| `start(options)` | Synchronous validation and bounded native admission; returns `void` before provider I/O |
| `signal(signal)` | Admits an ordered control and returns `void` |
| `monitor()` | Returns `Promise<void>`; nonzero exits reject with `exitCode` |
| `destroy(reason?)` | Acknowledges native cleanup; monitors resolve normally or reject with the explicit reason |

Providers must declare `queued_lifecycle` and implement effect-free preflight.
Native tickets are invocation-owned, one-shot and bounded by both command count
and aggregate input bytes. Startup does not occupy the ordinary JS storage
mutation slot; provider effects still await native durability/output gates.
Uncertain failures are retained without automatic replay. Explicit destroy can
clean up after failed startup. Queued calls inside native transactions and
`blockConcurrencyWhile` callbacks currently refuse before effects.

The fixtures exercise startup/teardown against the engine protocol peer,
including a blocked startup, storage progress, monitor outcomes, restart and a
submitted startup failure. They do not execute an actual container image or
certify a full SDK profile. Port methods, callback-owned lifecycle drainage,
published class hooks and full stopped-state/lifetime behavior remain required.

The storage fixture exercises structured graphs, SQL blobs/numeric conversion,
private data boundaries, cursor/byte quotas, failed admission, stale contexts,
idled/replaced activations and fresh-runtime recovery. Native gate fixtures
observe the Rust cell callback boundary while another request is queued, then
verify real activation reconstruction and failed-constructor refusal. Native
SDK tests also verify escaped scope tokens release the gate and refuse reuse.
Synchronous tests cover SQL/KV rollback together, nested sync/async savepoints,
return identity, thrown `undefined`, masked Promise rejection, refused async
mutations, escaped continuations and the nesting bound. Runaway callbacks also
prove refused output, joined shutdown and rollback after a fresh-runtime restart.

The bridge uses an opaque native callback scope, installed only while each SDK
future is polled in the original invocation/facet. Node AsyncLocalStorage and
V8 continuation metadata carry its token across awaits. At most the configured
batch limit of callback boundaries can be retained by one invocation. Expired
native tokens hold weak block references, so retaining one cannot retain the
input gate. Callback completion control does not occupy a JS mutation slot;
native transaction queues/guards serialize effects and result loss stays unknown.

Ordinary Cellar handlers still close the bridge on completion/cancellation and
require awaited SDK work. Compatibility hosts opt into `Source::workers_lifetimes()`
to retain a native-owned driver. It tracks explicit background promises, whole
transaction callbacks and outstanding SDK calls, including repeated registrations
by completing jobs. Before final revocation, each backend observes a full
microtask checkpoint and rechecks owned jobs/native operations, so derived I/O
continuations can register further work. Background rejection is reported through native work without
replacing an already published response. Cancelled/retired drivers remove private
JS job registries and revoke effects; they cannot extend native authority.

Full Cloudflare compatibility dates/flags, eviction/deadline profiles, timers,
RPC targets, container monitoring and the complete published package/image paths
still require qualification. Native-only hosts remain engine-free.

## Native value-only RPC integration

The Rust source must declare `EventKind::CellRpc` for each participating cell.
The Cellar handler receives `{ method, args }` with decoded arguments, and its
return value is encoded by the optional runtime. A wrapper can reuse the DO
owner across native events:

```js
import { activateDurableObject, durableObjectNamespace } from "@cellar/cloudflare";
import { DurableObject } from "@cellar/cloudflare/workers";

class Counter extends DurableObject {
  async increment(by) {
    const next = (await this.ctx.storage.get("count") ?? 0) + by;
    await this.ctx.storage.put("count", next);
    return next;
  }
}
class CounterCell {
  cell_rpc({ method, args }, context) {
    this.owner ??= activateDurableObject(Counter, context);
    return this.owner.rpc(method, args);
  }
}
export const cells = { Counter: CounterCell };

// Inside a Cellar handler, with this invocation's native context:
export async function increment(context) {
  return durableObjectNamespace(context, "COUNTERS").getByName("alice").increment(1);
}
```

The namespace facade and its stubs belong to the supplied event; retaining one
does not grant a later event authority. IDs preserve UTF-16 naming inputs and
are validated against the configured native namespace. Placement/jurisdiction
options, HTTP `fetch`, property access, promise pipelining, cross-event stub
failure state and returned `RpcTarget` capabilities need further implementation.
The bridge's operation/value quotas apply in addition to clone graph/body
limits. The native `Stub::call_with_limits` bounds successful and error replies
before decoding; a reply that cannot be represented may follow committed effects.
The RPC fixtures cover native JSON interoperability, restart recovery, namespace
isolation, cycles/binary/bigint/undefined and visibility/error boundaries in
both Node and embedded V8. This is not full Workers RPC conformance.
