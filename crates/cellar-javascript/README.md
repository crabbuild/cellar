<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# JavaScript and TypeScript applications on Cellar

`cellar-javascript` lets you write business logic and cell handlers in JavaScript
or TypeScript while Rust owns the host, routing, SQLite storage, and durability.
It implements Cellar's native `Application` and `Cell` contracts through two
optional backends: embedded V8 and a supervised Node.js process.

TypeScript is checked and compiled before execution. Applications use
[`@cellar/sdk`](../../packages/cellar-sdk/README.md) for typed contexts and HTTP
helpers; Rust registers their handlers and supplies deployment bindings.

```text
                         Your JS / TS application
                        fetch(), cells, queue(), ...
                                    |
                              @cellar/sdk
                                    |
                   +----------------+----------------+
                   |                                 |
             Embedded V8                         Node.js
          inside the Rust process          supervised child process
                   |                                 |
                   +---------- cellar-javascript ----+
                                    |
                         Original Rust invocation
                                    |
                             cellar-guest
                      validation + native SDK calls
                                    |
                           Cellar Rust core
                  ownership, routing, SQLite, durability
```

Start with the [TypeScript counter](../../examples/javascript/README.md) for an
embedded executable, the [Node example](../../examples/node/README.md) for npm
runtime compatibility, or the [durable Pi agent](../../examples/pi-agent/README.md)
for checkpointed model and tool steps.
The [Vercel AI SDK agent](../../examples/vercel-agent/README.md) demonstrates
`ToolLoopAgent`, durable model middleware, atomic tools, six process interruption
boundaries, and concurrent task recovery through the same Node bridge.
It also includes a React/shadcn streaming chat workspace, six function tools,
downstream HTTP idempotency, and live GPT-6 Luna interruption qualification.

## Choose an execution backend

| Question | Embedded V8 (`embed`) | Node.js (`node`) |
| --- | --- | --- |
| How does code execute? | One dedicated V8 thread inside the Rust host. | An actual Node process owned by the Rust bootstrap. |
| What ships? | A Rust executable containing the compiled application bundle. | The Rust host, Node executable, emitted modules, and dependencies. |
| How does npm work? | Compatible ESM/CommonJS code is bundled at build time. | Normal installed npm module resolution, including ESM/CommonJS and dynamic imports. |
| Which runtime APIs exist? | ECMAScript builtins, bounded text encoding and Cellar SDK bindings. | Node builtins, timers, global fetch, streams, and compatible native addons. |
| How is a blocked runtime detected? | An interruptible V8 pump-turn budget. | An event-loop heartbeat timeout. |
| When is it useful? | Business modules whose effects use Cellar bindings. | Agent SDKs or packages needing the Node runtime and OS APIs. |

The crate's default feature set enables neither backend. A Node-only Cargo build
does not link the embedded V8 dependency; the external Node binary supplies its
own engine. Native-only Cellar applications remain free of JavaScript engines.
The current Node supervisor requires **Node 24+ on Unix** (macOS or Linux).

Embedded V8 provides no Node builtins, native `.node` addons, DOM, global fetch,
timers, or runtime module loader. A package must fit the APIs used by its bundled
code paths. The counter qualifies Zod number validation, not every Zod API. Its
configuration skips third-party declaration checking while strictly checking
application code against ES2023 without DOM or Node globals.

Both backends expose buffered HTTP, streaming HTTP/SSE, and host-owned WebSocket
upgrades/events. See [streaming and WebSockets](../../docs/27-streaming-and-websockets.md)
for producers, forwarding, hibernation and cancellation. Cellar workflows and JavaScript-authored
Raft commands are outside this adapter's current contract. The Node example
qualifies React server rendering; browser DOM execution and arbitrary framework
integrations are separate concerns.

Named, typed unary RPC is implemented and under cross-language qualification.
Protobuf generates `CounterClient` methods, handler interfaces and explicit
registrations; RPC-only cells do not need a dummy `fetch`. The
[runnable RPC fixtures](../../tools/rpc/README.md) exercise Rust/V8/Node pairs and
receipt recovery. See the [implementation ledger](../../plans/named-typed-rpc/implementation-status.md)
for remaining fleet, schema validation, packaging and performance gates.

Embedded V8 installs `TextEncoder` and `TextDecoder` before bundle evaluation,
using Cellar's native encoding primitives. Decoding supports labels, fatal mode,
BOM handling and incremental state. Inputs are bounded by `max_value_bytes`;
native decoder handles have a separate `max_text_decoders` cap and follow their
JS owners. Fixed private byte storage is required at this native boundary.

## Write an application

Export application-level handlers and a `cells` map of named classes. The Rust
`Source` must register the matching handlers before the host admits work.

| Export or method | Signature | Rust registration |
| --- | --- | --- |
| Application `fetch` | `fetch(request, ctx)` | `.events([EventKind::Fetch])` |
| Application `queue` | `queue(batch, ctx)` | `.events([EventKind::Queue])` |
| Application `scheduled` | `scheduled(event, ctx)` | `.events([EventKind::Scheduled])` |
| Cell `fetch` | `fetch(request, ctx)` | `.cell("Counter")` |
| Cell `alarm` | `alarm(event, ctx)` | `.cell_events("Counter", [EventKind::Fetch, EventKind::Alarm])` when both handlers exist |

Pass all application event kinds together to `Source::events`; it sets the event
list rather than appending. Constructors take no arguments and receive no SDK
context. Each handler receives its own invocation-scoped `Context`.

### A small durable counter

This complete `app.ts` routes every request to the cell named `alice`. `GET`
reads its count; `POST` increments it by one. It uses only APIs shared by both
backends. Configure the `COUNTERS` namespace binding to point to `Counter`.

```ts
import {
  jsonResponse, request, response,
  type Context, type Request,
} from "@cellar/sdk";

export class Counter {
  async fetch(req: Request, ctx: Context) {
    if (req.method !== "GET" && req.method !== "POST") {
      return jsonResponse({error: "Use GET or POST"}, {status: 405});
    }

    const snapshot = await ctx.storage.read("count");
    let count = snapshot.present ? snapshot.value : 0;
    if (typeof count !== "number" || !Number.isSafeInteger(count) || count < 0) {
      throw new Error("Invalid stored counter");
    }

    if (req.method === "POST") {
      if (!Number.isSafeInteger(count + 1)) {
        return jsonResponse({error: "Counter limit reached"}, {status: 409});
      }

      count += 1;
      // Compare and write in one synchronous Rust transaction.
      await ctx.storage.apply({
        checks: {count: snapshot},
        puts: {count},
      });
      await ctx.storage.sync();
    }

    return jsonResponse({count});
  }
}

export const cells = {Counter};

export async function fetch(req: Request, ctx: Context) {
  const result = await ctx.cell("COUNTERS", "alice").fetch(
    request("http://counter/", {method: req.method}),
  );
  return response(result.body, {
    status: result.status,
    headers: result.headers,
  });
}
```

A concurrent update can invalidate the snapshot. The checked batch then raises
`Conflict / not_committed` without applying that batch; this example leaves the
error to the host and does not retry. See the [SDK counter example](../../packages/cellar-sdk/README.md#a-complete-durable-counter)
for validation and an explicit conflict response.

```text
POST to the application
        |
        v
application fetch() -- COUNTERS / alice --> Counter.fetch()
                                                |
                           read snapshot: count = 7
                                                |
                           apply: check 7, then put 8
                                                |
                           sync: await durability proof
                                                |
                           return JSON: {"count": 8}
```

`Request` is Cellar's buffered value, not the browser `Request` class.
`req.body` is a `Uint8Array`; `req.text()` and `req.json()` decode synchronously.
Use `request()`, `response()`, and `jsonResponse()` to build wire values rather
than manually base64-encoding bodies.

### A Node-only handler

Node mode can import normal Node modules. For example, this alternative `app.ts`
hashes the incoming bytes and returns JSON:

```ts
import {createHash} from "node:crypto";
import {jsonResponse, type Request} from "@cellar/sdk";

export async function fetch(req: Request) {
  const sha256 = createHash("sha256").update(req.body).digest("hex");
  return jsonResponse({sha256});
}
```

Register only the application `Fetch` event for this module. Embedded V8 refuses
its `node:crypto` import. The [Node compatibility example](../../examples/node/app.ts)
also exercises npm, native addons, dynamic imports, and React server rendering.
Direct Node filesystem and network effects are outside Cellar's transactions
and cancellation guarantees; use the SDK when an effect must follow Cellar's
storage and durability rules.

## Connect the Rust host

Both bootstraps validate application exports before calling the host closure.
Use `Host::run_until` with `application.stopped()` so an engine failure drains
the native host and allows bootstrap cleanup to finish. `Host::run` may exit the
process before adapter teardown.

These entry points use a fleet deployment named `counter-app`, version `v1`,
with a `Counter` class and a `COUNTERS` namespace binding. Registration does not
create that deployment or binding. Follow the reference examples for complete
project configuration; the [Node example's Rust entry point](../../examples/node/main.rs)
also shows a local deployment graph for development.

### Embedded V8 entry point

With `embed` enabled, place this `main.rs` beside the generated `app.bundle.js`:

```rust
use anyhow::Result;
use cellar::sdk::Host;
use cellar_javascript::{EventKind, JavaScriptRuntime, Options, Source};

fn main() -> Result<()> {
    let source = Source::new(include_str!("app.bundle.js"), "app.ts")
        .events([EventKind::Fetch])
        .cell("Counter");

    JavaScriptRuntime::run(Options::default(), source, |application| {
        let stop = application.clone();
        Host::fleet()
            .register("counter-app", "v1", move || Ok(application.clone()))?
            .run_until(async move { stop.stopped().await })
    })
}
```

The build tool emits a bundle defining `CellarApp`; pass that bundle to
`Source::new`, not uncompiled TypeScript or an arbitrary ESM source file.

### Node entry point

With `node` enabled, select the built module at the CLI boundary. Set
`CELLAR_NODE_ENTRY` to an absolute path such as `/srv/counter/dist/app.js`:

```rust
use anyhow::{Context as _, Result};
use cellar::sdk::Host;
use cellar_javascript::{EventKind, NodeOptions, NodeRuntime, Options, Source};

fn main() -> Result<()> {
    let entry = std::env::var_os("CELLAR_NODE_ENTRY")
        .context("set CELLAR_NODE_ENTRY to the absolute module path")?;
    let node = NodeOptions::new(std::path::PathBuf::from(entry));
    let source = Source::module()
        .events([EventKind::Fetch])
        .cell("Counter");

    NodeRuntime::run(Options::default(), node, source, |application| {
        let stop = application.clone();
        Host::fleet()
            .register("counter-app", "v1", move || Ok(application.clone()))?
            .run_until(async move { stop.stopped().await })
    })
}
```

This explicit-entry bootstrap resolves `node` through `PATH`. Set
`NodeOptions::executable` for a specific binary and `working_directory` to an
absolute directory when packages use relative OS paths. The default working
directory is inherited. Explicit environment additions are supported;
`NODE_OPTIONS` and `NODE_PATH` are always removed.

For an inventory-bound fleet package, use `NodeOptions::packaged()` and pass its
hash to `Host::runtime_inventory(FEATURE_NODE_V1, hash)`, as the reference Node
entry point does. That verifies the build-bound runtime and dependencies before
import and connects the process inventory to deployment admission.

## Build and deliver

```text
Embedded V8
  app.ts + npm packages
          |
      typecheck + bundle
          |
      app.bundle.js (CellarApp)
          |
      include_str! + Cargo
          |
      Rust executable with V8 and application
      Node/npm are build-time inputs only.

Node.js
  app.ts -- typecheck + emit --> dist/*.js
                                    |
                  Node + node_modules + Rust host
                                    |
                      inventory + package archive
                                    |
                  verified runtime files + executable
                  Node remains a serving dependency.
```

From the repository root, use the repository Rust toolchain and a writable,
checkout-specific `CARGO_TARGET_DIR` as required by [AGENTS.md](../../AGENTS.md).
Build the existing examples before running their tests:

```sh
npm ci --prefix tools/javascript --ignore-scripts

# Embedded V8 reference application
npm ci --prefix examples/javascript --ignore-scripts
npm run --prefix examples/javascript build
cargo build --locked -p cellar-javascript --features embed --bin cellar-javascript-example

# Streaming application (also compiled by crate-wide tests)
npm ci --prefix examples/streaming --ignore-scripts
npm run --prefix examples/streaming build

# Node reference application
npm ci --prefix examples/node --ignore-scripts
npm run --prefix examples/node build
cargo build --locked -p cellar-javascript --features node --bin cellar-node-example
```

These commands build the checked-in examples. To adapt the counter above, use
its matching fetch-only registrations and deployment names. Generated bundles
are ignored by Git and must exist before Cargo's `include_str!` runs.

The [embedded delivery guide](../../examples/javascript/README.md) uses the
existing native executable packaging and digest verification. The
[Node delivery guide](../../examples/node/README.md#run-and-deliver) distinguishes
a relocatable local delivery directory from a
[fleet package](../../examples/node/README.md#native-fleet-packaging-and-installation),
whose runtime is restored at the exact compiled prefix. Build Node packages and
native addons for the target OS/CPU; operating-system libraries may still need
separate provisioning. Changing code, dependencies, or runtime inputs requires
rebuilding and replacing the deployment.

## Understand invocation authority and object lifetime

```text
JavaScript runtime             Original Rust invocation          Native SDK
       |                                |                             |
       |<-------- dispatch(event) ------|                             |
       |                                |                             |
 await ctx.storage.read("count")        |                             |
       |----- operation + call ID ----->|--- validate and execute --->|
       |       Promise pending          |                             |
       |                                |<------- native result ------|
       |<------ bounded reply ----------|                             |
       |       Promise resumes          |                             |
       |-------- handler result ------->|                             |
       |                                |-- revoke call authority     |
```

One V8 thread multiplexes promises. V8 continuation-preserved embedder data
identifies the current invocation; Node uses `AsyncLocalStorage` across promises,
timers, and I/O. The bridge checks invocation identity and liveness before
admitting calls, and Rust independently checks invocation and activation
liveness. SDK futures remain in the original native handler. JavaScript receives
owned values and a private call facade, never a Rust context or storage handle.

Await every SDK operation. Known reads may overlap; only one mutation may be
outstanding per invocation. SQL, storage sync, and nested cell fetches count as
mutations. Returning with outstanding operations fails the handler. Do not save
contexts or binding handles for later calls or issue concurrent mutations with
`Promise.all()`.

```text
Activation A                    Retire / restart               Activation B
  new Counter()                       |                        new Counter()
  object fields / caches -------------X                        fresh fields
  ctx.storage ------------------------+----------------------->recovered state
                                      |
                         prior contexts lose authority
```

A JavaScript cell object lasts for one native activation. Persist business state
through `ctx.storage`; object fields are caches. Native cancellation revokes SDK
authority but does not undo committed writes. V8 and Node settle pending SDK
promises with `ContextExpired/unknown`, allowing `catch` and `finally` cleanup to
run. New SDK operations from that cleanup fail with
`ContextExpired/not_committed`, including after awaiting another JavaScript
promise. In Node, cancelled application code may continue its own OS work.

### Typed RPC deadlines and cancellation

Generated clients accept `{timeoutMs, signal}`. Use a native Node `AbortSignal`,
or bundle a compatible implementation for embedded V8. The
[SDK example](../../packages/cellar-sdk/README.md#cancel-a-named-call) shows both.

Every generated call starts a private budget before codecs run. Verification,
encoding, native admission/transport/proof and decoding share the original
deadline, capped by host policy and the incoming parent call. V8 uses the native
monotonic clock; Node uses a bounded private IPC control, conservatively charging
its round trip and one clock tick. Options are captured before that wait.

```text
original Context -> native budget -> verify -> encode -> native call -> decode
                         |                                  |
                  parent/host ceiling                no fresh timeout
```

Budget controls have their own per-invocation bound and consume no extra SDK
mutation slot. Release and retirement reclaim them. An expired pre-submission
call is `DeadlineExceeded/not_committed`; expiry after a successful native call
is `DeadlineExceeded/unknown`, including during decoding. Codec checks occur
before and after synchronous work; they do not preempt it.

```text
checked admission -> [private reply promise, cancellation control]
                            |                    |
                  await native reply      AbortSignal callback
                            |                    |
                            |            original native call
                            |             local / signed peer
                            v
                release mutation and byte ownership
                            |
                    settle the JS promise
```

The control can only cancel its admitted operation. It grants no ability to
submit SDK effects and carries no caller-selected cancellation ID. Ordinary
SDK admission still checks the original V8 continuation or Node async scope.
Cancellation consumes no extra SDK operation slot; cleanup keeps the original
native future and reservations until it settles. Signal subscriptions are also
removed on invocation retirement. Node captures its
[protected abort-listener API](https://nodejs.org/docs/latest-v24.x/api/events.html#eventsaddabortlistenersignal-listener)
before importing the application. Cancellation never promises rollback, and
synced writes remain committed. The signed two-node fixture now tests guest
remote/nested deadlines, receiver caps, slow codecs and Node control queueing.
Broader proof/lifecycle waits, dependency cycles and cancellation qualification
remain in the [RPC ledger](../../plans/named-typed-rpc/implementation-status.md).

## Limits, errors, and shutdown

Defaults come from [Options](runtime/mod.rs) and [NodeOptions](runtime/node/mod.rs):

| Setting | Default | Scope |
| --- | --- | --- |
| `max_invocations` / `max_objects` | 64 / 4,096 | Admitted calls / retained cell objects per runtime. |
| `max_text_decoders` | 4,096 | Native text-decoder handles in embedded V8; separate from cell objects. |
| `max_pending_operations` / `max_operations` | 16 / 1,024 | Outstanding / total SDK operations per invocation. |
| `max_body_bytes` / `max_value_bytes` | 1 MiB / 2 MiB | Buffered HTTP body / encoded bridge value. |
| `max_batch_operations` | 128 | Checks, mutations, and optional alarm change per storage batch. |
| `max_bridge_bytes` | 32 MiB | Shared payload budget; also bounds Node transport queues. |
| `heap_bytes` | 128 MiB | Embedded V8 heap or Node old-space limit; not process RSS. |
| `startup_timeout` | 5 seconds | Application initialization. |
| `turn_timeout` | 100 ms | Entire V8 pump pass, including its calls and microtasks. |
| `heartbeat_timeout` | 2 seconds | Node event-loop responsiveness; configured on `NodeOptions`. |

The V8 turn budget is not a per-request wall-clock deadline. Node uses the
heartbeat instead; slow asynchronous I/O can continue while its event loop
remains responsive. A runaway V8 turn terminates the isolate, while a blocked
or crashed Node worker stops the application. V8 bootstrap joins its engine and
watchdog; Node bootstrap kills the owned Unix process group and waits for its
child. Process restart belongs to the external supervisor. Do not detach Node
children from the owned group without separate supervision.

`application.bridge_usage()` returns a small `BridgeUsage` snapshot for host
diagnostics. It reads native reservation counters and requires no guest call:

| Field | Observation |
| --- | --- |
| `stopped` | Admission has been revoked and native waiters notified. |
| `active_invocations` | Native invocation permits still held. |
| `retained_bytes` | Owned invocation, operation and reply payload reservations. |

The fields are sampled independently. They do not measure heap, RSS or cached
cell objects, and `stopped` alone does not prove cleanup. Join callers and the
host before checking that native reservations returned to zero. The
[owned Node qualification](../../tools/rpc/README.md#owned-node-worker-loss-and-active-call-shutdown)
and [V8 qualification](../../tools/rpc/README.md#owned-v8-termination-and-active-call-shutdown)
use these counters during real typed calls, process loss, watchdog termination
and shutdown.

JSON values must use finite numbers, safe integers, and nesting depth at most
64. Encode larger integers as strings. A missing storage key and a stored JSON
`null` have different snapshots.

| SDK error | Meaning for this operation |
| --- | --- |
| `Conflict / not_committed` | The checked batch applied no changes. |
| `ValidationError / not_committed` | Validation refused the operation before effects. |
| Synchronous `ContextExpired`, `ContextMismatch`, or another admission refusal with `not_committed` | The call was refused before submission. |
| `Ambiguous / unknown` | A submitted mutation or its result failed; effects may have committed. |
| `Overloaded / unknown`, `OwnershipLost / unknown` | A native failure does not establish rollback, including nested-call failures. |
| `HostError / unknown` | A known-read operation failed to produce a usable result. |

A pending Node call revoked after submission can return `ContextExpired` with
`outcome: "unknown"`. Inspect the outcome as well as the code; expiration alone
does not prove that an earlier operation was refused.

There is no automatic mutation replay. Reconcile unknown outcomes before
retrying. SDK errors expose `code` and `outcome`; `CellarError` is a TypeScript
interface, not a runtime class. See the [shared guest boundary](../cellar-guest/README.md)
for failure provenance and conversion limits, and the
[SDK guide](../../packages/cellar-sdk/README.md) for application error handling.

For named RPC, an entered handler's fixed failure category survives with
`unknown` effects. A child `DeadlineExceeded/not_committed` can therefore escape
its parent as `DeadlineExceeded/unknown`. Earlier synced writes remain visible.
Exception text, arbitrary codes and outcome getters cannot claim rollback;
the native completion channel accepts only fixed categories. The
[provenance matrix](../../tools/rpc/README.md#failures-after-a-parent-commits)
checks this for both V8/TS and Node/JS.

These are trusted application runtimes. V8 heap exhaustion can terminate the
host process, and Node has normal OS capabilities. Deployment still needs native
process supervision and memory limits.

## Validate a change and find the implementation

After building both example applications, these focused commands exercise the
bridge and real HTTP/restart paths without requiring a fleet account:

```sh
cargo test --locked -p cellar-javascript --features embed,node \
  --lib --test node --test node_runtime -- --test-threads=1
cargo test --locked -p cellar-guest
cargo clippy --locked -p cellar-javascript --features embed,node \
  --lib --tests -- -D warnings
npm test --prefix tools/javascript
python3 tools/docs/check.py
```

Tests cover continuation isolation, cancellation, overlapping mutation refusal,
startup failures, runtime termination, IPC limits, npm/native-addon compatibility,
named-cell isolation, and storage recovery in a fresh runtime. Installed-package
checks have additional setup in the Node delivery guide. These checks are not
production multi-node fault or performance qualification.

| Source | Responsibility |
| --- | --- |
| [application.rs](application.rs) | `Source` registration, native `Application`/`Cell` proxies, and SDK execution within each invocation. |
| [runtime/mod.rs](runtime/mod.rs) | Options, bounded channels, byte reservations, and joined bootstrap. |
| [runtime/runner.js](runtime/runner.js) | Shared SDK facade, export validation, and activation-local objects. |
| [runtime/engine.rs](runtime/engine.rs) | V8 isolate, continuation identity, promises, and watchdog. |
| [runtime/node/mod.rs](runtime/node/mod.rs) | Node startup, authentication, liveness checks, and process ownership. |
| [runtime/node/worker.mjs](runtime/node/worker.mjs) | Node module loading, async context, and JavaScript IPC peer. |
| [runtime/node/transport.rs](runtime/node/transport.rs) | Bounded, length-prefixed JSON transport separate from stdout/stderr. |
| [runtime/node/package.rs](runtime/node/package.rs) | Build-bound Node inventory verification. |
| [cellar-guest](../cellar-guest/README.md) | Shared native operation validation, storage batches, and failure envelopes. |

For design context, see [ADR 005](../../docs/adr/005-embedded-javascript.md) and
[delivery gates](../../plans/008-embedded-javascript-typescript.md). For repeatable
measurements, use the [bridge benchmark](../../tools/perf/javascript/README.md)
and read its [recorded results and limits](../../plans/javascript-bridge-performance-2026-10-22.md).
