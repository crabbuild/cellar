# 008: JavaScript and TypeScript above the native Cellar SDK

Status: V8 and Node backends implemented, with native Node fleet packaging; broader production gates remain open.
Current implementation contract: [adapter guide](../crates/cellar-javascript/README.md)
and [ADR 005](../docs/adr/005-embedded-javascript.md). The research below records
the wider target architecture; it must not be read as a completed qualification report.
Prepared: 2026-10-22. Owner: Cellar runtime and SDK contributors.
Reviewed Cellar baseline: `fb660b164b3cb0c8f88afcb659473445928a92a2`
(`origin/main`, pulled with `git pull --ff-only origin main`).

## Target architecture and original research

The implemented optional `cellar-javascript` crate implements `sdk::Application`
and `sdk::Cell` with direct `rusty_v8`. It uses one dedicated engine thread per
application bootstrap; the bounded multi-lane pool described below remains a
production scaling target. `rquickjs` is a fallback candidate, not a qualified
backend. The initial preference for `deno_core` was withdrawn following the
maintenance announcement below. TypeScript is checked and bundled during the
application build. The closed bundle, SDK facade and Rust registrations are
embedded into the native executable.

Developers can write handlers, reusable business modules and durable cell
classes in JS or TS. Rust continues to own ingress, cell identity, activation,
placement, SQLite, transactions, durability proof, routing, recovery and node
lifecycle. Ordinary Rust applications retain an engine-free dependency graph.

This extends the optional-language-adapter approach established by
[Python](007-embedded-python-pyo3.md). It explicitly amends the
workspace-wide engine exclusion in [ADR 001](../docs/adr/001-native-applications.md)
and [AGENTS.md](../AGENTS.md), together with a scoped dependency guard. Those
native-only rules remain in effect. ADR 005 records the scoped optional-engine
amendment. Implementation does not establish production qualification.

For Node built-ins and native npm addons, the implemented `node` backend uses
an actual supervised Node process beside the Rust host. This supersedes the
initial Node-owned-host/Rust-addon recommendation below and preserves Cellar's
Rust process entry point. npm module compatibility does not automatically adapt
an existing Express/Nest server to Cellar's request and durable-cell contracts;
framework ingress adapters remain separate integrations. Bare V8 alone does not
provide Node compatibility.

### Maintenance correction, 2026-10-22

The [October 9 Deno announcement](https://deno.com/blog/cloudflare) says the
team is joining Cloudflare. It commits to one more year of runtime bug/security
updates, then ends its runtime development; Deploy closes after six months.
`rusty_v8` support continues. No separate long-term `deno_core` commitment is
stated. The initial investigation missed this announcement.

For Cellar, recent crate releases establish current activity but do not resolve
long-term maintenance risk. Keep `deno_core` as a technical reference and possible
candidate only with an explicit maintenance/exit strategy. Direct `rusty_v8`
would require Cellar to implement more module, Promise and event-loop machinery;
`rquickjs` requires its own compatibility, performance and maintainer assessment.
The maintenance correction alone did not select or qualify an engine;
the subsequent implementation selects direct V8 in ADR 005. The invocation
mailbox, native authority and durability contracts below remain the design basis.

## What the existing Python bridge establishes

The reviewed baseline has substantially more than FFI function calls:

| Existing boundary | Source | Implication for JS |
| --- | --- | --- |
| `Application`, `Cell`, `Invocation`, `Output`, `HandlerFuture` | [application/mod.rs](../crates/cellar/application/mod.rs) | Implement existing contracts; do not create another cell runtime |
| Immutable script/version factories and process inventory validation | [node/native.rs](../crates/cellar/node/native.rs) | JS registrations must remain part of verified compiled applications |
| Proxy factories, activation objects and native invocation driver | [Python application](../crates/cellar-python/application.rs) | Factories allocate native proxies; engine work stays off native workers |
| Revocable, bounded operation sender and mutation admission | [Python bridge](../crates/cellar-python/runtime/bridge.rs) | Owned messages carry requests, never transferable SDK authority |
| Operations executed through the original `Context` | [Python operations](../crates/cellar-python/operations.rs) | The engine cannot move SDK futures onto its own executor |
| Checked JSON mutations and alarm updates in one transaction | [Python storage](../crates/cellar-guest/storage.rs) | Read/compute/write needs explicit conflict detection |
| Typed refusal versus ambiguous submitted mutations | [Python failures](../crates/cellar-python/failure.rs) | A rejected/lost result must not imply rollback |
| Loop scope, lazy objects and cancellation cleanup | [Python runner](../crates/cellar-python/python/runner.py) | Promise scope and object retirement are runtime contracts |
| Build-bound environment and returning host lifecycle | [Python guide](../crates/cellar-python/README.md) | Bundle identity and joined shutdown are part of the bridge |

At this baseline Python uses one managed main-thread asyncio loop and a 1 ms
polling bridge. This is an observation of the pulled source, not a claim about
unmerged Python work. JS should use channel wakeups and engine event-loop
readiness from its first implementation; it need not inherit CPython's
main-thread constraint or polling schedule.

Keep the Python implementation intact initially. Extract shared, language-free
operation validation, checked batches or error classification only after both
adapters demonstrate the same contract. Do not make JS depend on a crate whose
private implementation and build policy are CPython-specific.

## Hosting and engine choices

The table combines upstream capabilities with Cellar-specific engineering
judgment. No engine performance measurements were made in this investigation.

| Option | Fit | Main cost | Recommendation |
| --- | --- | --- | --- |
| Rust host + direct `rusty_v8` | V8 embedding with an explicit upstream support commitment | Cellar must own module loading, Promise integration, event-loop machinery and host APIs | Evaluate in JS-0 |
| Rust host + `deno_core`/V8 | ESM, promises, Rust operations and an event-loop substrate inside the existing executable | Deno runtime development wind-down, unresolved crate maintenance, engine builds and API churn | Technical reference; no longer the default recommendation |
| Rust host + `rquickjs` | Embedding API with modules, pending jobs and runtime resource controls | Cellar must build its host APIs and qualify library compatibility, performance and maintenance | Evaluate alongside direct V8 in JS-0 |
| Rust host + Boa | Rust ECMAScript interpreter | Upstream describes it as experimental; compatibility and operational fit need independent qualification | Research option, not first delivery |
| Node process + `napi-rs` addon | Existing Node/npm ecosystem; JS owns process entry | New host/package lifecycle, Node event-loop integration, process-wide failure domain | Alternative; the implemented backend keeps Rust in charge with a supervised Node child |
| Rust host embeds libnode | Node APIs inside a Rust-owned process | C++ glue, libuv/V8 lifecycle and Node-major embedder changes | Defer unless both embedded hosting and Node compatibility are required |
| Separate JS worker process | Independent kill/restart and dependency isolation | IPC, authentication, call cancellation, flow control and deployment coordination | Implemented for Node compatibility; buffered Cellar operations cross authenticated IPC |
| JS-in-Wasm | Potential portable execution boundary | Additional runtime/tooling and compatibility work | No demonstrated benefit for this trusted embedded use case |

`deno_core::JsRuntime` is `!Send` and `!Sync`; upstream also requires process-wide
V8 platform initialization with a common parent thread. This supports keeping
each runtime on a dedicated thread with owned messages across the boundary.
The runtime provides module loading and event-loop polling, rather than a
complete automatically configured Deno CLI environment.
[JsRuntime documentation](https://docs.rs/deno_core/latest/deno_core/struct.JsRuntime.html)
and [runtime options](https://docs.rs/deno_core/latest/deno_core/struct.RuntimeOptions.html).

Upstream verification found an important version-specific detail:
`deno_core` **0.413.0**, published 2026-10-21, defaults to V8 and also exposes an
alternative QuickJS feature. Its `deno_v8` 0.5.0 facade selects mutually
exclusive backends; the published facade resolves V8 through package `v8`
150.4.0, or QuickJS through `v8x` 149.4.0-rc.4. This does **not** establish equal
behavior or production readiness for the alternative backend. Start with one
explicit supported backend, and do not enable both through an indiscriminate
upstream all-features build. These are inspected versions, not yet selected
Cellar dependency pins. See the immutable
[core manifest](https://github.com/denoland/deno/blob/fe7ed012d92c642fd8e5607ac5a45d517538b5c5/libs/core/Cargo.toml)
and [engine facade manifest](https://github.com/denoland/deno/blob/fe7ed012d92c642fd8e5607ac5a45d517538b5c5/libs/deno_v8/Cargo.toml).

The old `denoland/deno_core` repository is archived because development merged
into `denoland/deno`. That archive event is distinct from the subsequent runtime
development wind-down; neither current releases nor the repository move provide
a long-term maintenance commitment for the crate.
[Upstream repository notice](https://github.com/denoland/deno_core).

QuickJS resource controls are attractive, but check allocator configuration:
`rquickjs::Runtime::set_memory_limit` is documented as ineffective with a custom
allocator, including `rust-alloc`/`allocator`. Interrupts, stack limits and memory
statistics also need qualification under the exact chosen configuration.
[rquickjs runtime documentation](https://docs.rs/rquickjs/latest/rquickjs/struct.Runtime.html).

NAPI-RS supplies thread-safe callbacks and async-to-Promise conversion. Neither
automatically confers Cellar invocation authority: exported async functions run
on its runtime unless configured otherwise, and even a custom executor does
not recreate the original Cellar event. A Node adapter still needs the scoped
mailbox contract described below.
[Thread-safe functions](https://napi.rs/docs/concepts/threadsafe-function),
[async functions](https://napi.rs/docs/concepts/async-fn).
Embedded Node additionally has an upstream warning that its C++ embedding API
can change on each major release without the ordinary deprecation process.
[Node embedding documentation](https://nodejs.org/api/embedding.html).

## Execution and authority

### Native proxy and engine lanes

Use one `JsApplication` per compiled application generation. It owns a bounded
pool of engine lanes. Each lane owns a thread, its local event loop, an engine
instance (`JsRuntime` for the deno_core candidate), module registrations,
activation-object table and promise table.
Never store V8 values or scopes in a `Send` SDK handler future.

Assign a cell activation to a lane for its entire lifetime. Allocate its JS
object lazily on that lane, keyed by a unique activation token, application
generation and runtime incarnation. A business cell name or epoch by itself is
not an object identity. Factories and configuration validators remain synchronous
native operations and never enter the engine. Stateless events use bounded lane
admission. No mutable globals are shared across lanes; module initialization
runs per runtime and may therefore run more than once per process.

A lane must multiplex invocations while native SDK work is pending. Pinning a
cell to a lane does not grant the lane a new right to serialize or reorder that
cell's events; retain native turn admission and concurrency semantics. JS object
fields are transient caches and can be observed across interleaved handlers.

### One SDK call

1. Rust admits an invocation under its native execution domain, captures its
   `Context`, reserves capacity and sends an owned event plus private capability
   to the assigned engine lane.
2. The engine enters the handler with a fresh invocation scope. An SDK method
   validates the calling scope and encodes a bounded operation into that
   invocation's mailbox, then returns a Promise.
3. The **original Rust handler future** receives the operation and polls its SDK
   future under the original generation, event and activation. The engine's
   async operation may await a mailbox reply; it does not execute storage itself.
4. Rust delivers a bounded owned reply, waking the lane. The lane resolves the
   corresponding Promise in its original runtime and scope.
5. Handler completion revokes SDK admission, resolves native output conversion
   and returns through Cellar's existing final output proof gate.

Start SDK operations in receive order. Permit overlap only for operations known
to be reads. Admit at most one outstanding mutation per invocation initially;
SQL, sync and nested calls conservatively count as mutations. A second mutation
is refused before submission. Keep its permit until the native future finishes
or is dropped, even if JS abandons its Promise. This does not serialize all
invocations or grant a cross-operation transaction.

The event pump must continue accepting starts, replies and cancellation while
handlers await native calls. A design that awaits handler A to completion before
starting B can deadlock when A calls B on the same lane. Exercise A-to-B and
A-to-B-to-A calls against the native reentrancy/cycle rules. Also test recursion
when normal invocation permits are exhausted: use bounded refusal or a proven
native-compatible reserve, never an unbounded queue or a silent dependency wait.

### Scope across await is a delivery gate

A global `currentContext` variable is incorrect once promises interleave.
An application-supplied invocation ID is not authority, and possession of a live
`ctx` must not allow invocation B to use invocation A's context.

Use private continuation-local scope backed by the selected engine's async
context mechanism. The inspected `deno_core` 0.413.0 source exposes
`getAsyncContext`, `setAsyncContext`, `scopeAsyncContext` and `AsyncVariable`
over continuation-preserved embedder data. This is a useful implementation
substrate for that candidate, not a tested Cellar security contract. A direct
V8 or QuickJS implementation must separately establish equivalent propagation
and revocation; the deno_core helpers do not come with those engines automatically.
[Inspected core source](https://github.com/denoland/deno/blob/fe7ed012d92c642fd8e5607ac5a45d517538b5c5/libs/core/01_core.js).

Each SDK submission must match the current continuation scope to the immutable
capability's invocation, runtime incarnation and activation, followed by native
admission checks. Restore scope on every entry/exit path. Validate promise chains,
`await`, `Promise.all`, exceptions, queued microtasks, module evaluation and
host-created callbacks. Root module code and constructors receive no SDK context.
Do not expose low-level context setters, operation tables or arbitrary capability
construction through the application facade. Trusted application execution is
still not an isolation boundary against deliberate engine/runtime exploitation.

Background promises inherit only the originating invocation's scope. Once that
invocation ends, their SDK calls fail even if another invocation is active.
There is no implicit `waitUntil`. Unsupported callback sources such as arbitrary
timers/workers stay unavailable until their scope and lifetime are implemented.
Failure to prove scope propagation blocks concurrent-lane delivery; a single
process-global variable is not an acceptable fallback.

### Cancellation, retirement and CPU loops

Returning, cancellation, native timeout, ownership loss, activation retirement,
generation retirement or runtime failure revokes the relevant capabilities.
Use explicit native-owned liveness state; JS finalizers and garbage collection
are never responsible for timely revocation or releasing native permits.
Discard late replies after validating incarnation and invocation IDs, while
releasing their byte reservations. Queued, unstarted operations are refused;
already submitted mutations may have committed.

Dropping a Rust future does not stop a JS CPU loop. Use an independent supervisor
with an engine termination handle. V8 documents cross-thread termination;
termination applies to the **isolate**, so all concurrent work in that lane may
be affected. Fail those invocations conservatively, retire their transient
objects and recreate the lane only after teardown joins. Do not resume the
same half-executed business handler.
[V8 termination API](https://docs.rs/v8/latest/v8/struct.IsolateHandle.html).
Verify this behavior against the version actually selected by `deno_core`.

Heap limits and near-limit callbacks are operational controls, not guaranteed
recovery from every allocation failure. A fatal engine OOM or native crash can
still terminate the node. Rust host callbacks must remain bounded and cannot be
assumed preemptible by JS termination. Use the existing external supervisor for
process recovery. A per-cell isolate would narrow failure impact at a likely
memory cost; measure that alternative before promising per-cell CPU/heap limits.

## Developer API and components

### Proposed initial surface

Expose a small typed module, provisionally `@cellar/sdk`, backed by the embedded
facade. Its name is illustrative and does not refer to a published package.
JavaScript uses the same API with editor declarations/JSDoc; TypeScript adds
compile-time checks, not additional runtime authority.

| Surface | Initial contract |
| --- | --- |
| Module `fetch(request, ctx)` | Buffered HTTP handler returning a bounded Cellar response |
| Cell class `fetch(request, ctx)` | One transient object per native activation |
| Cell `alarm(scheduledMs, ctx)` | Native durable alarm delivery |
| Module `queue(batch, ctx)` / `scheduled(event, ctx)` | Explicit registration and native retry/settlement behavior |
| `ctx.identity` | Immutable native-assigned identity; wide integers represented without rounding |
| `ctx.storage.readJson`, `apply`, `sql`, `sync` | Presence-aware reads, checked batches, bounded SQL and proof barrier |
| Alarm read/set/delete | Native alarm persistence, including atomic state-plus-alarm batch |
| `ctx.namespace(binding).getByName(name).fetch(...)` | Host-routed bounded call into JS, Rust or Python cell registrations |
| `ctx.kv`, `ctx.sql`, `ctx.queue`, `ctx.var`, `ctx.nowMs` | Explicit declared bindings and host clock, following Python parity |
| `ctx.signal` | Cancellation observation; abort does not imply mutation rollback |

Start with explicitly named buffered Request/Response types in the SDK facade.
Standard web-compatible globals can be added with a documented compatibility
surface; do not advertise WHATWG Fetch conformance from a small wrapper. Native
SQL, HTTP, queue and cell semantics remain authoritative. Settlement acknowledgements
are delivery decisions, not a new exactly-once execution guarantee.

Validate registered exports and callable handlers before admission; do not infer
handler correctness from `Function.length`, constructor names or TS types.
Accept ordinary functions returning values/Promises and normalize their result.
Zero-argument constructors are synchronous and cannot perform SDK calls. Reject
thenable constructor objects and malformed outputs before using them as cell
objects. Module initialization has a deadline and resource budget too.

### Example of the intended programming model

The following is a **proposed API sketch**, not runnable against today's SDK.
It shows a reusable TS component inside a durable cell and a checked mutation.
The component receives the current invocation context explicitly; it never
stores it on the long-lived cell object.

```ts
import { Response, type CellContext } from "@cellar/sdk";

async function increment(ctx: CellContext): Promise<number> {
  const before = await ctx.storage.readJson<number>("count");
  const current = before.present ? before.value : 0;
  if (!Number.isSafeInteger(current) || current >= Number.MAX_SAFE_INTEGER) {
    throw new RangeError("counter is outside the supported range");
  }
  const next = current + 1;
  await ctx.storage.apply({
    checks: { count: before },
    puts: { count: next },
  });
  await ctx.storage.sync();
  return next;
}

export class Counter {
  async fetch(_request: unknown, ctx: CellContext): Promise<Response> {
    return Response.json({ count: await increment(ctx) });
  }
}
```

`Conflict` has no writes from this batch. The caller chooses whether to reread
and recompute a side-effect-free calculation. A sync failure or lost response
after `apply` can leave a committed increment: this example is deliberately
not an idempotent increment endpoint. A production mutation should require a
validated idempotency key and atomically check/write its receipt with the state.
Bind the key to normalized request content, reject different content under the
same key, and return the stored result for a duplicate. An uncertain result
requires receipt reconciliation; never automatically replay the mutation.

### What a component means here

| Component | Boundary and persistence |
| --- | --- |
| Pure TS module, validator or business function | Ordinary in-runtime composition; no new cell identity |
| Stateful business component in a cell | Same invocation and SQLite boundary; durable state goes through storage |
| Separate durable cell | Native identity, placement and call boundary; no cross-cell transaction |
| Rust service/cell | Call through existing declared bindings with bounded owned values |
| Python service/cell | Same routing principle; both optional runtimes must be present and qualified |
| React/Vue/browser component | Browser build and static assets; backend bridge is unnecessary for client rendering |
| Server-rendered UI component | Possible later with a qualified renderer and host API subset; DOM/hydration/streaming are separate work |

`CompiledApplications` already registers multiple script/version factories, so
a host can compose a JS application beside Rust applications using that registry.
Adding JS-to-service fetch support uses the existing native service binding path.
Mixing languages within one application's class registry needs an explicit
delegating `Application` implementation with disjoint registrations and forwarded
validation. It must not bypass event dispatch by calling raw JS from a Rust
transaction closure. Such same-application delegation is optional follow-up work.

Do not expose arbitrary Rust objects, closures or SQLite connections through FFI.
Future custom bindings should declare typed operations, input/output codecs,
read/mutation classification, limits, cancellation and ambiguity semantics.
Generate TS declarations from those schemas and validate again in Rust.

## Storage, values and failure semantics

All SQLite transactions execute synchronously in Rust. There is no initial
`transaction(async tx => ...)` API and no JS callback while a Rust transaction
is held. `apply` checks presence/value snapshots and applies state and alarm
changes together. Snapshot equality must include native writers. Sequential
SDK calls and nested cell calls do not become one transaction merely because
they share a JS function.

Keep native replication/output gates on `sync` and final handler output. Promise
resolution for a local mutation is not a new durability level. Arbitrary external
effects are outside the cell transaction; a supported durable outbox binding
needs a separate bridge contract before it is advertised.

Define a bounded codec before exposing a broad structured-clone surface:

- JSON null differs from a missing key. Use tagged presence snapshots.
- JSON numbers must be finite, and integer-valued JS numbers must be safe
  integers. Rust integers outside that range require an explicit typed/tagged
  representation; never silently round epochs, offsets or SQL values.
- Treat `bigint` as an explicit future codec extension or reject it initially.
  Encode opaque/wide identities as strings. Do not accidentally accept bigint
  through lossy JSON conversion.
- Binary bodies use copied `Uint8Array`/owned Rust bytes with byte limits.
  Reject SharedArrayBuffer and detached buffers initially. No zero-copy claim
  without pinned backing-store lifetime and accounting evidence.
- Reject unsupported functions, symbols, undefined values in persisted JSON,
  cycles and excessive depth/collection counts. Define string/Unicode behavior
  explicitly, including lone surrogates and invalid inbound UTF-8.
- Bound conversion work and intermediate allocations. Getters, proxies,
  `toJSON` and thenables can execute application code: perform such conversion
  on the engine lane under deadlines, never inside native transaction locks.
- Preserve repeated headers as ordered pairs; validate HTTP status, methods,
  URLs, header bytes and body restrictions through native conversion rules.

Retain fixed SDK error codes and an operation-specific outcome:

| Situation | Result contract |
| --- | --- |
| Invalid input, expired/mismatched scope or capacity refusal before dispatch | Proven not submitted/not committed for that operation |
| Failed checked batch | `Conflict`, no changes from that batch |
| Ownership lost during submitted work | `OwnershipLost`, outcome may be unknown |
| Submitted mutation loses reply or result exceeds conversion budget | `Ambiguous`, unknown; reconcile |
| Downstream overload propagated after outer writes | Preserve overload without claiming outer rollback |
| Aborted wait after submission | Cancellation observed; mutation may have committed |

Failures in a later operation do not undo earlier writes in the invocation.
Backend diagnostics and uncaught application values must not leak request data,
SQL, secrets or local paths through ingress errors. Provide bounded, redacted
operational diagnostics and optional private source-mapped traces separately.

## TypeScript build, npm compatibility and deployment

TypeScript is a build-time input. Run a pinned TypeScript checker with strict
settings, then bundle/transpile with a pinned tool such as esbuild. esbuild
removes TS syntax but does not type-check; both steps are necessary.
[TypeScript handling](https://esbuild.github.io/content-types/#typescript).

Bundle for an explicit Cellar runtime target, using an allowlisted resolver and
build metadata. `platform: neutral` can be useful but is not a compatibility
checker. Include JS, facade, registrations, source-map policy, package lock,
dependency content hashes, compiler versions/options and runtime/backend identity
in the inventory. Resolve every import to an embedded module or declared runtime
module before publication. Any supported dynamic import must resolve only in
that verified graph; reject unresolved dynamic require/import patterns.

Pure JS libraries using supported ECMAScript and facade APIs are candidates for
bundling. Validate specific pinned libraries in CI. Node `fs`, sockets, process
inspection, native `.node` addons, install-time generated runtime files, dynamic
package discovery and arbitrary filesystem/network imports are not provided by
direct `rusty_v8`; the earlier deno_core candidate also requires additional
runtime integrations for these facilities. Some CommonJS can be converted by a bundler, but successful bundling
alone does not establish runtime compatibility. Fail unsupported imports during
build or cold validation, rather than during a production request.

Start with buffered HTTP and small business/validation packages. Add an HTTP
framework only after its exact required APIs pass compatibility tests. A full
Node/Deno compatibility layer would materially expand the product and should
have its own requirements and maintenance budget.

### Sharing the npm ecosystem with direct V8

Keep standard `package.json`, package-manager lockfiles, registries, workspaces,
ESM exports and TypeScript declarations. Use npm or pnpm on the development/build
machine to install dependencies, including private organizational packages. V8
executes the built JavaScript; the build tool resolves npm packages and creates
the verified module graph. There is no need to implement an npm client or install
packages inside a serving node. `rusty_v8` supplies engine bindings; host APIs
and application runtime policy remain Cellar responsibilities.
[V8 embedding guide](https://v8.dev/docs/embed).

Separate compatibility by the dependency's actual runtime requirements:

| Dependency kind | Cellar treatment |
| --- | --- |
| Pure ECMAScript business/validation/utility module | Bundle and test under the selected V8 version |
| Module using web APIs | Support after its required API subset is implemented and qualified |
| Statically resolvable CommonJS | Bundle-convert where supported, then test the emitted code |
| Module using small Node utility APIs | Consider individually tested shims; do not infer general Node support |
| Runtime Node filesystem, TCP, process, worker or native-addon dependency | Refuse unless an explicit host integration exists; use a separate Node service when appropriate |
| Browser UI package | Bundle for the browser and serve assets; DOM access belongs to that environment |
| Build tool requiring Node or a native binary | May run on the build machine; it need not be compatible with Cellar's serving runtime |

For ecosystem reach, implement a versioned web API profile incrementally:
encoding, URL, headers and buffered request/response first, then qualified
outbound fetch, AbortSignal, crypto and streams. These are proposed capabilities,
not current guarantees. Fetch must use the original invocation's native HTTP
policy and accounting. Host I/O, timers and callbacks must retain invocation
scope and cancellation. Explicitly qualify module-top-level work that lacks a
cell invocation. Expose only the implemented types in Cellar's runtime typings;
including every DOM or Node declaration would create false compatibility.

Do not replace unsupported filesystem/process/network APIs with empty objects
or successful no-ops. Native `.node` modules require their host API environment;
even Node-API's stable ABI does not make those modules loadable by bare V8.
[Node-API documentation](https://nodejs.org/api/n-api.html).

### Proposed build and package flow

1. Install the exact dependency graph using the project's pinned package manager
   and lockfile. For npm projects, `npm ci` refuses manifest/lock disagreement
   and leaves the lockfile unchanged. Keep the corresponding install configuration
   with the project. Build-time scripts and generated files belong to the recorded
   build environment, never to request processing.
   [npm ci contract](https://docs.npmjs.com/cli/v11/commands/npm-ci/).
2. Run TypeScript checking and the project's build. Produce an ESM application
   bundle plus any explicitly packaged chunks/assets. A generated Rust host can
   register exported handlers/classes, so users need not author Rust themselves;
   the first deployment model still requires a Rust toolchain on the build runner.
3. Resolve package exports and dependencies using a Cellar-targeted bundler.
   With esbuild, start from `platform: neutral`, `format: esm`, an explicit
   engine target and `mainFields: ["module", "main"]`. Set a proposed `cellar`
   condition and exactly one of production/development. The bundler retains its
   import/require/default semantics; export-object key order remains significant.
   Neutral mode alone supplies neither npm main-field defaults nor platform
   conditions. These settings are a starting policy, not a compatibility proof.
   [esbuild platform and conditions](https://esbuild.github.io/api/#platform).
4. Use a resolver plugin to map the exact supported SDK import to the embedded
   facade, reject unsupported built-ins including both bare and `node:` forms,
   and package declared assets. Preserve resolved file identity for duplicate
   package versions. Resolve workspace links only within declared build inputs.
   Inspect emitted imports and dynamic loading constructs as well as the input
   dependency graph. Unknown dynamic paths fail the build; statically declared
   imports can become verified chunks. Never add `node` or `browser` conditions
   globally just to make resolution succeed. A browser branch may require a DOM.
5. Generate an inventory covering all emitted modules/assets, SDK ABI and
   runtime profile, registrations, package lock/content identities, build tools,
   licenses/notices and source-map policy. Bind it to the host artifact. npm
   semver/peer dependency resolution is useful at build time but cannot replace
   host ABI/capability checks before admission.
6. Run cold import and package-behavior tests in the actual embedded V8 host,
   including unsupported-global failures and async context tests. Build success
   and tests under Node alone cannot establish runtime compatibility.
7. Compile/link the Rust host with the verified bundle and stage an ordinary
   target-specific Cellar native package, extended with JS runtime feature
   admission. The deployed artifact carries the engine and application code;
   serving machines need its OS dependencies but no Node/npm installation.

Native JS built-ins, import resolution callbacks, module evaluation, dynamic
import callbacks, Promise completion and microtask pumping must be connected by
the direct V8 adapter. A single ESM bundle reduces module-loader work but does
not eliminate these runtime responsibilities. Dynamic import is restricted to
the immutable packaged graph and is never a network/package-manager operation.

For a development UI, npm scripts can invoke a future Cellar JS build/dev
command. Watch mode rebuilds changed JS quickly, then relinks/replaces the native
host under the first packaging model; cached dependencies reduce work but the
native link cost remains. A prebuilt runtime plus a separately verified bundle
could remove the per-app Rust build later. That requires a new executable/source
loading contract, registration/inventory validation and rollout design, and must
not be presented as supported by the current compiled-factory model.

### Publishing reusable components

Publish business components as ordinary npm packages with ESM output, `.d.ts`
files, explicit exports, a `files` allowlist and licenses. A simple package can
use the following illustrative manifest; it describes a pure shared module and
does not depend on a special Cellar package format:

```json
{
  "name": "@example/order-rules",
  "version": "1.0.0",
  "type": "module",
  "files": ["dist", "LICENSE"],
  "exports": {
    ".": {
      "types": "./dist/index.d.ts",
      "default": "./dist/index.js"
    }
  }
}
```

Cellar-specific components can export cell classes and a declaration manifest;
the consuming application explicitly registers them and supplies bindings. Such
packages should declare a supported SDK peer range and required runtime profile,
and never initialize a private Cellar host or capture invocation contexts at
module scope. Pure business modules can stay independent of the SDK and be shared
between Cellar, Node and browser builds.

If a package needs distinct integrations, use explicit subpath exports or a
documented custom `cellar` export condition with a portable default where one
exists. Keep `types` first and default last; order controls conditional selection.
The custom condition is a proposed Cellar build convention, not an existing
ecosystem-wide standard or automatic Node behavior.
[Package export conditions](https://nodejs.org/api/packages.html#conditional-exports).

Validate the actual published tarball in a clean consumer project with
`npm pack` before publication. This catches omitted generated output, leaked
workspace assumptions and missing declaration files.
[npm pack](https://docs.npmjs.com/cli/v11/commands/npm-pack/).
Publishing a library distributes source/build output through npm; deploying a
Cellar application distributes the verified native host through Cellar's package
flow. Installing a component must not itself deploy cells or create durable state.

Provisionally provide an SDK/types package and a separate build-tool package
through npm; names and release versions remain to be decided. The build tool can
wrap the existing Rust CLI. If prebuilt CLI binaries are distributed through
platform-specific npm packages, select OS/architecture/libc explicitly and verify
their identities. The published SDK facade and the embedded host must agree on
ABI and capability profile; reject mismatches instead of trusting package version
labels alone. Keep client/browser exports separate from runtime-only bindings.

Measure ecosystem support using a pinned package matrix: selected utility,
validation, routing and fetch-based client packages; ESM and CommonJS inputs;
workspace/private packages; and explicit negative cases for DOM, filesystem,
sockets and native addons. Record required APIs and runtime tests per package
version. A claim such as “all npm packages work” would require a substantially
broader Node-compatible runtime than this design proposes.

Embed artifacts in the Rust executable initially, as with Python source.
Production nodes need neither Node nor a TS compiler, runtime npm installs or
package downloads. Changing application source requires a new executable and
process replacement. Development watch mode can rebuild/restart using ordinary
preserving recovery. JS heaps, globals, object fields, Promise stacks and code
caches are transient, not durable checkpoints.

Use native artifact verification and explicit runtime inventory feature admission.
The current `CompiledApplications::runtime_inventory` accepts only
`embedded-python-v1`; supporting a proposed `embedded-js-v1` requires native
protocol, package/install and generation-validation work. Do not reuse the
Python inventory slot, treat an unknown feature as supported, or broaden a string
allowlist without verifying the matching artifact. Keep Cellar format markers
at 1 under the repository's hard-cutover policy. Engine snapshots/code caches
are optional derived artifacts tied to exact engine/target/source identities;
they are not portable business state or an independently trusted executable.

There are two current engine checks to update deliberately: the workspace
dependency guard and
[`verify_native_dependencies` in the application builder](../crates/cellar/cli/native_deploy/build.rs).
The latter rejects `v8` in the selected application's normal/build graph before
compilation. JS-enabled projects need an explicit validated build mode allowing
the selected adapter/engine graph; ordinary native projects must still reject
it. Add refusal tests for missing/unknown JS mode and mismatched runtime identity.
Changing only the workspace guard would leave the application unpackageable.

JS and Python can coexist in a process in principle: Python keeps its bootstrap
requirements and main loop, Rust supervision owns JS lanes and node services.
Qualify startup ordering, signals, process-global libraries, combined memory and
joined shutdown before advertising that combination. Do not infer mixed-runtime
support solely from independent adapters passing their tests.

## Limits, observability and shutdown

Capture explicit runtime policy before host admission. Bound lane count, active
and queued invocations, resident activation objects, unresolved host Promises,
per-invocation operation counts, queued operations, aggregate bridge bytes,
body/value sizes, conversion depth and engine heap/stack budgets. Reserve bytes
before copying, including both sides of a copy while both are live.

Provision a separately bounded control path for completion, cancellation,
retirement and shutdown, with reserved capacity tied to admitted invocations.
An operation queue full of application work must not prevent context revocation
or completion. Replies and pending host futures also need bounded reservations.
Do not use unbounded channels to avoid backpressure deadlocks.

Select defaults from measurements rather than copying Python's 128-invocation,
4096-object and 32 MiB bridge settings. Shared-lane engine heap is a lane budget;
it is not accurate per-cell memory accounting. JS heap metrics exclude native
buffers, SQLite, engine code and other host allocations; process RSS is global.

Observe bridge queue/start/reply latency, operation duration, handler wall time,
event-loop delay, engine thread CPU, heap/external bytes, active objects,
pending Promises, reservations, refusals, cancellations, lane resets and proof
failures. Publish snapshot age from Rust so a blocked engine cannot look healthy
by retaining its last successful observation.

Shutdown stops native and engine admission, revokes capabilities, drains accepted
work under native policy, cancels managed JS work, retires objects, releases
buffers and joins engine lanes and owned native tasks. Keep cancellation/drain
ordering consistent with existing native shutdown; never replay accepted work
to make the report appear complete. Preserve native fenced/incomplete outcomes
in a combined report. Zero JS Promises is not proof that native shutdown succeeded.
If a callback/thread cannot join, report incomplete and let the external
supervisor enforce its process policy.

## Fleet cells and Raft cells are different bridge projects

The ordinary `Application`/`Cell` bridge covers the Fleet cell model used by
the Python adapter. Cellar now also has a restricted
[Raft application contract](multi-raft/application-contract.md).
`Application::resolve_raft_class` resolves a separate registered definition;
implementing async `Cell::invoke` does not automatically implement it.

Raft command callbacks are synchronous inside a transaction and cannot await,
perform outbound networking, spawn work or retain writable storage. Queries
have pinned read-only images and their own authority. JS execution in those
callbacks needs a distinct design for engine affinity, bounded synchronous
execution, captured inputs, schema/operation digests, read-only enforcement,
receipts and failure handling. Do not hold an open native transaction across a
round trip to an asynchronously scheduled engine lane.

Initial delivery should refuse JS Raft registrations explicitly. A later bridge
could expose typed Rust Raft command/query clients to JS without putting JS
inside a command transaction. Writing command implementations themselves in JS
is separate work, with separate qualification; it is not implied by this plan.

## Implementation slices and acceptance gates

These are ordered work packages, not completed milestones. A working fetch demo
alone does not satisfy the bridge contract.

| Slice | Concrete changes | Required exit evidence |
| --- | --- | --- |
| JS-0: Contract and backend spike | Compare direct rusty_v8 and rquickjs, review maintenance, then pin an engine/toolchain; establish lane, module and async-scope behavior | Written backend/maintenance decision plus real engine tests for interleaved await, wrong live context, leaked context, CPU loop termination, teardown and same-lane nested calls; record build size/startup/RSS |
| JS-1: Optional adapter skeleton | Add `crates/cellar-js`, proxy Application/Cell, bounded mailboxes, event-driven pump, registration validation and feature-specific CI | Native-only builds retain zero engines; JS startup refuses invalid registrations; constructors/imports cannot use SDK; no timer polling required |
| JS-2: Storage and buffered HTTP | Implement codec, requests/responses, checked batch, alarms, SQL, sync and typed errors | Native-writer conflicts, absent/null, unsafe integers, binary limits, malformed outputs, failed proof and lost-reply receipt reconciliation |
| JS-3: Routing and events | Namespace/service fetch, KV/shared SQL/queue/cron and mixed Rust/JS host example | Same/remote-owner calls, saturation without deadlock, cycles, settlement, retries, cancellation and fencing through real nodes |
| JS-4: Verified TS delivery | Type declarations/checking, locked bundling, embedded inventory, JS feature admission and CLI deployment example | Offline install/start, altered-module/runtime refusal, unknown-feature refusal and source-upgrade preserving recovery |
| JS-5: Operational qualification | Supervision, limits, telemetry, failure injection and comparative benchmark harness | Joined TERM/INT, lane/host failure, ownership takeover, both proof paths failing, cold owner-loss recovery and repeated bounded-residency runs |

The first slice should be a real engine spike in a separate opt-in build, not a
Node-only mock of embedding. Pinning must verify Cellar's declared minimum
Rust version and SDK CI toolchain as well as target/linker availability. Newest
upstream release is not automatically the appropriate first dependency pin.

Expected owning files:

| Location | Intended responsibility |
| --- | --- |
| `crates/cellar-js/application.rs` | Native application/cell proxies and original-invocation driver |
| `crates/cellar-js/runtime/mod.rs` and focused children | Lane ownership, event pump, scope, budgets, cancellation, object registry and reports |
| `crates/cellar-js/js/` | Embedded facade/bootstrap; no native storage execution |
| `crates/cellar-js/operations.rs`, `storage.rs`, `values.rs`, `failure.rs` | SDK dispatch, checked mutation, bounded codecs and typed failures |
| `examples/javascript/` | Rust host plus TS business classes and a reproducible build |
| `tools/javascript/` | Build/inventory, qualification and performance harnesses |
| Native deployment/protocol components | JS inventory, artifact/feature validation and install/startup support |
| `tools/check_native_dependencies.py`, CI and ADRs | Native graph isolation and explicit optional-runtime policy |

Do not remove the current engine guard. It presently traverses every workspace
member with all features and therefore intentionally rejects an embedded engine
anywhere in that graph. Replace that single workspace assertion with isolated
native-root dependency checks plus an explicit adapter feature matrix. Include
new engine/facade package names in the deny set as appropriate; inspect actual
resolved package identities rather than Rust import aliases. Prove a JS-only
feature cannot enter `cellar-logic`, native `cellar` or `cellar-native`. Python-only
builds should also remain engine-free. Check the JS adapter graph separately
against the selected allowed backend and pins.

## Qualification matrix

| Boundary | Cases that must be exercised |
| --- | --- |
| Authority | Context retained after return; another live invocation uses it; same cell reactivation; old generation/runtime replies; promise/microtask exception scope restoration |
| Scheduling | Interleaved reads; second mutation refusal; native future pending while pump receives nested starts; same-lane calls; saturated invocation/control/reply capacity |
| Durability | Local commit with no output proof; withheld follower and bucket proof independently/together; lost response; restart receipts; native writer between read and apply |
| Fleet recovery | Remote owner, handoff, resumed stale task after fencing, killed owner with deleted local disk, recovered alarm occurrence with stable receipt identity |
| Codec | Missing/null, safe-integer boundary and larger native integers, non-finite values, cycles, accessors, malicious thenables, repeated headers, invalid body/status, exact byte/depth limits |
| Lifecycle | Import/constructor failure, unhandled rejection, pending Promise on return, abort before/after submission, CPU loop, microtask starvation, heap pressure and joined lane reset |
| Events | Queue ack/retry conflicts and timeout redelivery; cron occurrence identity; alarm state committed with effect receipt; unsupported event refusal |
| Packaging | Offline execution, changed bundle/lock/facade/engine identity, wrong target, unsupported imports, unknown runtime feature and preserving process replacement |
| Resource ownership | Retirement while idle, capacity restored after cancellations, reservations released on every error path, bounded repeated activation/reset cycles |
| Native isolation | Core/native/Python builds activate no JS engine; native handlers progress while a JS lane is blocked |

Compare Rust, Python and JS using the same real HTTP workloads, durability
configuration and proof gates. Include no-op, read, checked write plus sync,
nested call, queues/alarms, concurrent cold/warm cells and owner-loss recovery.
Report p50/p95/p99, throughput, CPU, RSS, heap/external bytes, bridge copies,
queue latency, startup and executable size. Sweep lane count and object count;
include saturation/refusal rate and event-loop delay. Keep VM warmup and GC
visible. Do not select an engine from a trivial arithmetic microbenchmark.

## Evidence and current limits

This investigation read the native SDK/factory contracts, Python proxy/bridge,
operation/storage/error paths, runner, packaging guidance, Raft application
contract, dependency guard and CI/documentation conventions at the baseline
above. It verified current upstream documentation and inspected published
`deno_core` 0.413.0 and `deno_v8` 0.5.0 source archives in memory. Their embedded
VCS metadata identifies Deno commit
`fe7ed012d92c642fd8e5607ac5a45d517538b5c5` and paths `libs/core` and
`libs/deno_v8`; the immutable links above preserve that provenance.

The proposed architecture and engine recommendation are engineering judgments
derived from those sources. No JS engine was compiled, no embedding spike was
run, no npm compatibility claim was qualified, and no performance or fleet result
is asserted. Existing Python evidence does not qualify JS. The next concrete
deliverable is JS-0, followed by the first fetch/storage implementation only
after its authority and lifecycle tests pass.

Validation for this documentation change:

- `python3 tools/docs/check.py`: passed; 37 documents, 7 crate READMEs,
  10 visual guides, 6 flows and 656 local references.
- Additional local-link/anchor and whitespace checks for this plan and its
  index: passed. Plans are outside the documentation catalog's coverage.
- `python3 tools/check_native_dependencies.py`: passed; 452 reachable
  workspace packages with all features, no JavaScript engine.
- `git diff --check`: passed. No runtime source, manifest, lockfile, protocol,
  dependency guard or architectural policy was changed.

## Executable delivery (2026-10-22)

Implemented `cellar-javascript`, the shared `cellar-guest` native boundary, an
ESM npm SDK with declarations, strict TS/JS build tooling, lockfiles, an npm/Zod
example and explicit JavaScript opt-in to native packaging. See the adapter guide
for exact APIs and executable commands. The implementation uses one application
isolate and fails it on a runaway turn; multi-lane scheduling and automatic
engine recreation remain future work. Native executable hashes cover bundled
source, so no separately installed JS runtime inventory is required.

Remaining production acceptance work includes the broader fault/performance
matrix above, remote fleet rollout, cross-target packaging, runtime metrics,
source-map diagnostics and additional npm/Web API compatibility profiles. Do
not mark JS-0 through JS-5 complete from the initial adapter tests alone.

### Local validation evidence

Validated on the working tree on `codex/js-ts-bridge-design`, 2026-10-22,
using Rust 1.98.1 and macOS ARM64. This is local evidence; the added Linux CI
workflow has not been run remotely in this task.

- 9 V8 runtime tests passed: interleaved continuation scope, cancellation,
  concurrent mutation refusal, activation retention/retirement/capacity,
  ambiguous result conversion, unsafe queue integers, registration refusal,
  runaway handlers and runaway module initialization.
- 3 shared-operation tests and the real-node TS/Zod routing, persistence,
  isolation, shutdown and restart test passed.
- The native packaging opt-in policy test passed.
- Python's existing `--runtime-bindings` and `--runtime-durability` regression
  modes passed after the common operation boundary moved into `cellar-guest`.
- Clippy passed with `-D warnings` for JavaScript/shared-guest all targets and
  the Python embedded library. Existing vendored dependency warnings remain.
- Both npm lockfiles installed with `npm ci --ignore-scripts`; 4 build/SDK
  tests and the example typecheck passed. The SDK was packed into an npm archive.
- Rust formatting and module-layout checks passed for 803 Rust files.
- Documentation and native-only dependency checks passed. The native-only
  graph has 454 reachable packages and no JavaScript engine.
- The native dev package built and its embedded executable passed
  `--check-native-package`. Target: `aarch64-apple-darwin`; executable SHA-256:
  `a4ebdfca952ec9ff4a0907ff607ca17740f9c8dbdc6e24511e6dde2816a5f680`;
  executable size: 225,867,576 bytes. This is a development build, not a
  size-optimized release artifact. Package output is `.cellar/javascript-package`;
  the npm archive is `.cellar/javascript-artifacts/cellar-sdk-0.6.2.tgz`.

The final parallel test run initially exposed an overly short startup deadline.
Startup compilation now has a separate bounded budget from ordinary engine
turns. Regression tests independently check both startup-loop and handler-loop
termination; the handler test also requires that the host was actually admitted.


## Node compatibility delivery

The optional `node` Cargo feature now supervises an actual Node.js process and
shares the Rust application proxy with the V8 backend. The implementation uses
normal Node module loading, AsyncLocalStorage invocation scope, bounded framed
IPC with a bootstrap secret, native admission checks, process-group shutdown,
and a heartbeat for event-loop failure. Node-only builds have no V8 dependency.

The executable example exercises TypeScript, Zod, CommonJS Lodash, React SSR,
Argon2 native addons, dynamic imports, Node built-ins and durable cell routing.
The directory packager includes Node, the Rust host, emitted code and installed
packages with a checked file inventory. This external-runtime delivery format
is distinct from native fleet packages. Native fleet provisioning/inventory binding
is implemented in the follow-up below. Windows process supervision and expanded
Cellar streaming/event APIs remain open. The [Node guide](../examples/node/README.md) is the operational contract.


Node delivery validation on macOS arm64 / Node 24.13.1 / Rust 1.98.1:

- Combined V8/Node/guest suite: 24 Rust tests passed, including both real native
  node integrations. Node-only mode: 11 tests passed; its dependency graph has
  neither V8 nor Deno. Clippy passed in combined and Node-only feature modes.
- Six npm tooling tests passed, including extended JSONC TypeScript builds,
  failed-build stale-output removal, package integrity checks and V8 import limits.
- Node integration passed ESM, CommonJS, dynamic imports, Zod, Lodash, React SSR,
  the native Argon2 addon, crypto/Buffer, filesystem, timers, streams and fetch.
  Routed cell writes survived a fresh Node process and native-node restart.
- Node lifecycle tests passed async invocation scoping, context revocation,
  overlapping mutation refusal, startup failure, blocked bootstrap pipe timeout,
  event-loop runaway, process crash after mutation submission without replay,
  bounded fragmented IPC, stdout separation and child reaping with live timers.
- The Node-only executable and full runtime directory were built locally. After
  relocating the directory, its integrity verifier and executable `--help`
  succeeded, and the real-node integration passed using its copied Node runtime,
  emitted application and npm dependencies.
- Formatter, module layout, native-only dependency guard, documentation links and
  whitespace checks passed. The npm installation audit reported zero known
  vulnerabilities at validation time. The initial PR subsequently passed Linux
  JavaScript/Node and Python example CI; Windows and production multi-node fault
  qualification remain open.

The generated local delivery is `.cellar/node-delivery/`; no release was published
and no fleet was modified. The package is specific to macOS arm64. Use the same
build and checks on the deployment target to produce a Linux artifact.


## Follow-up: native fleet delivery for Node

PR #99 adds a `node_environment` project input and `node-runtime-v1` deployment
resource. Build captures the immutable Node/application/npm tree before Cargo;
the adapter embeds that inventory, verifies it before spawning Node, and binds
its digest to host admission. Native publish uploads verified content-addressed
archives before the selector. Install requires `--node-root` matching the compiled
prefix, restores checked regular files atomically without overwrite, and publishes
the executable only after runtime restoration succeeds. Python installation keeps
its existing API and now shares the atomic directory publication helper.

This supersedes the earlier external-Node fleet packaging limitation. Relocatable
local-graph directories remain supported. Windows supervision, system shared
library closure capture, Cellar streaming/WebSocket/workflow/RPC APIs, and broader
production fleet qualification remain open. Linux JavaScript/Node CI passed for
the initial implementation on PR #99 before this follow-up.


The initial full SDK CI run exposed an error-message regression after extracting
shared guest request conversion: Python's over-limit request diagnostic had
changed. The shared boundary now returns a typed `RequestBodyLimit`, and the
Python wrapper preserves its existing diagnostic. The operating-defaults
qualification passes without changing its assertions: mixed requests at
concurrency 1/4/16/32, exact body boundary, saturation/refusal, killed-owner
recovery after disk deletion, and joined shutdown. Its six report guards pass.


Follow-up validation on macOS arm64 / Node 24.13.1:

- The native package was built, published to a private SQLite development bucket,
  and installed with the captured runtime directory moved aside. The installer
  restored the real Node binary, application and npm files from the archive.
- The installed executable passed its native-package check and served CommonJS,
  React SSR and the native Argon2 addon. A second process with a distinct node
  identity and fresh data directory recovered the acknowledged counter from
  bucket storage and committed another update. Both processes drained and joined.
- Four Node archive tests cover integrity, malformed entries, path escape,
  symlinks, tampering, and missing-runtime publication/installation refusal.
  Admission and protocol tests require an exact executable/inventory pairing.
  Five Python archive regression tests pass with the shared publication helper;
  a separate test checks directory permissions after private staging.
- The 24-test combined adapter suite, 11-test Node-only suite, seven npm tooling
  tests and Python operating-defaults qualification pass. Clippy passes for the
  combined adapters, core library and Python adapter. The Linux workflow now
  explicitly runs native Node publication, archive restoration and process
  recovery in addition to the existing Node/npm compatibility tests.

These are local development-bucket checks, not live fleet rollout or production
fault/performance qualification. System prerequisites, Windows, broader Cellar
transport APIs and the production gates above remain outstanding.
