# ADR 005: Embed compiled JavaScript above the native SDK

Status: implemented adapter; production qualification remains open.
Recorded: 2026-10-22.

## Decision

Use optional `cellar-javascript` with pinned `rusty_v8` 152.2.0. Keep V8 out of
`cellar`, `cellar-logic`, and native-only/Python-only dependency paths. Compile
TypeScript and closed npm dependency graphs at build time. Embed the bundle
into the existing native executable; executable digests cover its engine, SDK
facade and source. Deployment does not load JavaScript from remote manifests.
A project must explicitly set `javascript: true` to package an engine dependency.
In this embedded V8 profile, the native protocol format remains unchanged because
the executable remains the entire serving runtime artifact. The build inventory is informational provenance,
not a separately loaded artifact or an attestation of arbitrary external inputs.

Direct V8 was selected after the Deno team's announced runtime maintenance
transition; `deno_core` is not a dependency. See the linked design for official
sources and the alternatives considered. This choice means Cellar owns its
promise pump, SDK facade, quotas and module build contract. It does not inherit
Node's standard library, npm addon loader or browser Web APIs.

## Execution boundary

A dedicated engine thread multiplexes invocation promises. Native futures run
inside their original Rust invocation. Continuation-preserved embedder data and
private captured context IDs enforce scope across async continuations. Native
liveness flags revoke invocation/activation authority without entering V8.
Bounded admission and separate reply capacity prevent new work from consuming
all completion capacity. One native mutation per invocation is admitted at a
time. Returning without awaiting effects fails the handler.

Python's native storage/operations/value/failure boundary is moved into the
engine-free `cellar-guest` crate. Both adapters call this shared implementation;
Rust still owns synchronous transactions, routing and durability acknowledgement.
JS constructors have no context and object identity follows native activation.

The initial runtime uses one isolate per compiled application bootstrap, with
persistent activation objects. A watchdog interrupts an excessive engine turn;
this fails the whole application and requires supervised restart. It never
replays interrupted mutations. Heap exhaustion can be process-fatal. These
bounds are for trusted application operation, not hostile tenant isolation.

## Consequences and delivery gates

The implemented buffered V8 profile covers the same event and operation categories
as the existing Python bridge. It is deliberately explicit about unsupported
Node APIs, streams, workflows, RPC and JavaScript Raft handlers. The delivery
contract, runnable commands and verification matrix live in the
[adapter guide](../../crates/cellar-javascript/README.md). The broader
[design](../../plans/008-embedded-javascript-typescript.md) retains production
gates for multi-node fencing, ambiguous outcomes, saturation, memory behavior,
packaged cross-target qualification and performance before fleet rollout.

## Node compatibility backend

An optional `node` feature now runs an actual Node.js process, reusing the native
application proxy and engine-free guest operations. Direct V8 remains available
for closed bundles. Node support uses ordinary module loading for ESM, CommonJS,
dynamic imports and native addons, with an explicit absolute entry module and
installed dependencies. The Rust host uses a bounded, authenticated loopback
channel; AsyncLocalStorage and native liveness checks preserve invocation scope.
Application stdout and stderr are independent of the protocol.

This is a trusted OS process, not an implementation of Node APIs inside V8. Direct
Node effects are outside Cellar durability. Unix process-group teardown and a
heartbeat bound worker shutdown and event-loop stalls. No effects are replayed
when a worker fails. External Node resources can use a relocatable directory or
a native fleet package. Fleet builds embed the captured inventory, declare `node-runtime-v1`,
publish content-addressed runtime archives, and restore the exact compiled prefix
with `--node-root`. Admission refuses a mismatched process inventory.
See the [Node guide](../../examples/node/README.md) for delivery and limits.
