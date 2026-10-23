# JavaScript and TypeScript runtimes

`cellar-javascript` implements the native `Application` and `Cell` contracts
using optional V8 and Node.js backends. Embedded V8 applications use the pinned
`rusty_v8` engine and build-time bundles; Node applications retain normal npm
module loading. Rust owns every SDK operation and durability gate.
The default crate features do not activate an engine. Enable `embed` for V8,
or `node` for an actual supervised Node.js process. The Node-only dependency
graph does not link V8.

The [Node.js example and delivery guide](../../examples/node/README.md) covers
normal ESM/CommonJS/npm loading, dynamic imports, Node built-ins, native addons,
and React server rendering. Native fleet builds capture its runtime and npm graph
into an inventory-bound executable and archive; install restores the exact prefix.
It uses the same Rust operation boundary and typed
SDK with a separate authenticated IPC channel. The sections below describe the
embedded V8 profile unless stated otherwise.

The [TypeScript counter](../../examples/javascript/README.md) is the executable
reference. It imports Zod from npm, routes requests into named cells, and updates
SQLite using checked atomic batches. [Design and delivery gates](../../plans/008-embedded-javascript-typescript.md)
track the broader runtime work; [ADR 005](../../docs/adr/005-embedded-javascript.md)
records this implementation's decisions.

## Application contract

A bundled application defines `CellarApp` with named `fetch`, `queue`, and/or
`scheduled` exports and a `cells` map of named constructors. Constructors receive
no SDK context. Cell methods receive `(event, context)` on every invocation;
register `fetch` and/or `alarm` explicitly in the Rust `Source`. The npm
[`@cellar/sdk`](../../packages/cellar-sdk/README.md) package supplies types and
buffered request/response builders. A cell instance lasts for one native
activation, so its fields are a cache, never durable state.

`JavaScriptRuntime::run(options, source, host)` initializes the application
before calling `host`. Pass `application.stopped()` to `Host::run_until` to drain
native work when the engine fails. The bootstrap joins its engine and watchdog
on all returning paths and preserves both host and engine failure. Avoid
`Host::run`, which may exit the process before adapter teardown.

| Capability | Implemented profile |
| --- | --- |
| Languages | Modern ECMAScript; JS/TS checked before bundling; ESM and statically bundled CommonJS dependencies |
| HTTP | Buffered requests/responses, byte bodies, ordered header pairs; UTF-8 text/JSON helpers |
| Cells | Named classes, activation-local objects, fetch, alarms, nested namespace fetch |
| Storage | Presence-aware JSON reads; checked atomic batches; local SQL; explicit sync; alarms |
| Infrastructure | JSON KV get/put/delete/list; shared SQL; queue send and settlement; scheduled no-retry |
| Deployment | Closed bundle embedded in native executable; existing target/digest/package verification |
| Limits | Invocation/object admission, operation counts, one mutation at a time per invocation, byte budgets, heap limit, interruptible engine turns |

The embedded V8 profile does **not** provide the Node.js runtime, native `.node` addons,
DOM, browser `Request`/`Response`, global fetch, timers, streams, WebSockets,
workflow execution, JS-authored Raft commands, RPC, dynamic module loading, or
arbitrary network/filesystem access. Unsupported invocations fail explicitly.
Use Cellar bindings for effects. npm compatibility is assessed per package and
used code path: the example qualifies Zod number validation, not all Zod APIs.
The example skips third-party declaration checking because Zod's declarations
also refer to a browser URL API; application code is still strictly checked
against ES2023 without DOM/Node globals.

## Authority and failure behavior

One dedicated V8 thread multiplexes promises while Rust SDK futures stay in
the original native handler. V8 continuation-preserved embedder data identifies
each continuation. A native callback checks that identity against the private
context token, invocation liveness, and activation liveness before admission.
A saved context cannot acquire another invocation's authority. Native cancellation
revokes authority immediately; the engine collects cancelled work on its pump.
No SDK context, storage handle, or transaction callback enters JavaScript.

A native operation starts in mailbox order. Known reads may overlap; only one
mutation is admitted per invocation. A handler returning with outstanding
operations fails. Applications must await all effects. Checked storage batches
validate and execute entirely in a synchronous Rust transaction. Conflict means
`not_committed`; submitted mutation errors retain `unknown` outcome. There is
no automatic mutation replay. Missing storage values and stored JSON null differ.
JSON values reject unsupported types, nonfinite numbers, unsafe integers and
depth above 64. Encode larger integers explicitly as application strings.

Startup compilation/initialization has a separate five-second default budget;
ordinary engine turns default to 100 ms. Both limits are configurable and bounded.
An infinite loop interrupts the entire isolate and fails its application.
Existing writes may already have committed. The bootstrap reports the failure;
restart belongs to the process supervisor. The heap limit is not an RSS limit,
and V8 allocation failure can terminate the process. This is a trusted application
runtime, not a sandbox for mutually untrusted tenants. Deployment needs the
usual native process supervision and memory limits.

## Validation

After building the example bundle:

```sh
cargo test --locked -p cellar-javascript --features embed
cargo test --locked -p cellar-guest
cargo clippy --locked -p cellar-javascript --features embed --all-targets -- -D warnings
npm test --prefix tools/javascript
```

Use the checkout-specific target directory required by the root AGENTS guide.
Engine tests exercise continuation scope, cancellation, concurrent mutations,
startup refusal and CPU termination. The real-node test exercises the TS/npm
bundle, nested routing, named-cell isolation, writes, validation refusal, native
shutdown and SQLite recovery in a fresh engine. These checks do not constitute
multi-node fault-injection or production performance qualification.
