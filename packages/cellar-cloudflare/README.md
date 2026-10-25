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
  invoking application methods and refuse a failed initialization. Callbacks
  remain attached to their original native event.

`activateDurableObject` is the shared owner for the upcoming native event/module
and RPC dispatcher. The `workers` entry supplies the imported base classes and
an explicitly disabled tracing export used by the pinned Sandbox fallback.
Defining `RpcTarget` does not implement RPC transport or remote object lifetime.

The complete Worker module loader, HTTP/web globals, namespace binding APIs,
RPC targets, transactions/rollback, PITR, native input-gate/reset/30-second block
semantics, `waitUntil`, abort/reconstruction,
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
container view in these fixtures exposes the actual provider's stopped state;
it deliberately supplies no start/monitor/port implementation. No test here
executes a container image or certifies a full SDK profile.

The storage fixture exercises structured graphs, SQL blobs/numeric conversion,
private data boundaries, cursor/byte quotas, failed admission, stale contexts,
idled/replaced activations and fresh-runtime recovery. Both async and sync
transactions currently refuse before invoking application callbacks; full
transaction/rollback support is a remaining requirement, not a no-op fallback.
