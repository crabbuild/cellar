<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Sandbox provider implementation ledger

Objective: deliver the full
[sandbox provider and compatibility plan](sandbox-provider-compatibility.md),
including local published Cloudflare package compatibility, portable four-language
controls, the named remote providers, lifetime/fencing/reconciliation behavior,
and the plan's live and fault qualification. This ledger records progress; it
does not narrow that objective or turn fixture passes into full compatibility.

## Requirement audit

| Slice | Current evidence | Remaining work / completion evidence |
| --- | --- | --- |
| SP-01 | Exact celld reference commit/files/license, npm packages/lock/integrities, imported runtime bytes and immutable Sandbox image manifest are in `tools/sandbox/compat`; drift/refusal tests pass; API/fixture inventory lists native mappings and explicit gaps | Input inventory implemented; runtime conformance remains in SP-02–SP-04 and later profiles |
| SP-02 | Native-backed DO SQL/KV/clone facade, activation-bound authority, object identity, value-only namespace/cell RPC, constructor admission, native input gates/reset, sync/async transactions and retained event/background drivers execute in Node/V8; actual published constructors are exercised | Complete module/bootstrap/web globals, automatic activation-owned environment bindings, property/target RPC and lifetimes, full versioned lifetime/deadline qualification and profile/version admission; retain native-only dependency boundary |
| SP-03 | Actual published `@cloudflare/containers@0.3.7` constructor, state and SQL scheduling run in both engines with native storage and a stopped-provider view; unchanged upstream example is pinned | Execute the unchanged app, class hooks, routing, sleep/wake, process/port behavior and cleanup against the native engine |
| SP-04 | Actual `@cloudflare/sandbox@0.12.9` constructor/configuration and metadata recovery run in Node; matching upstream app/Dockerfile and `linux/amd64` image digest are pinned | Run the unchanged app and SDK/image end to end over HTTP and RPC; files, streamed exec, background records, previews, teardown and failure cases |
| SP-05 | Provider environment/operation/limit discovery is implemented in Rust, Python, JS and TS; native and restricted provider dispatch tests cover it | Portable files and richer operations, named configuration, durable remote bindings, fencing, expiry/reconnect, loss/unknown outcomes and orphan reconciliation |
| SP-06 | No Vercel/E2B implementation yet | Real shared provider implementations, fixtures and real-account conformance, explicit Cloudflare profile compatibility/gaps |
| SP-07 | Separate 1.x contract identified in the design; it is not the 0.12.9 baseline | Pin and run exact current package/helper/image, extended container API, errors, streams, cancellation and supported persistence |
| SP-08 | No Daytona/Modal implementation yet | Provider implementations and individual process/files/storage/PTY/GPU/snapshot qualification |
| SP-09 | No Dynamic Worker or hosted Cloudflare provider yet | Actual isolate loader/limits/capability isolation; optional authenticated hosted gateway, outbound policy and cleanup qualification |
| SP-10 | Initial input manifest explicitly reports `conformance: not_run` | Publish operation/version compatibility results only after actual four-language, lifecycle, fault, resource, fencing and shutdown qualification |

## Verified implementation details

### Provider capability discovery

`cellar-container::Adapter::capabilities` describes host configuration without
creating a session or contacting an engine. Invocation-owned
`container.capabilities` is provided by the shared boundary, including for a
restricted provider that does not implement that operation itself. It requires
root-cell event authority and never reaches the provider's effect dispatcher.

The version-1 descriptor contains provider name, explicit execution environment,
actually supported operations, host body limit and the provider's declared
handle limit. Existing custom providers default to `unspecified` environment
and an unknown handle limit, rather than claiming Linux or unlimited resources.
Native declares Linux and its enforced 64-handle limit. Future snapshot, PTY or
isolate features are not advertised before their operations exist.

Python exposes an immutable dataclass/tuple; JS exposes a frozen descriptor and
operation list; TS declarations preserve readonly fields. This is a partial
SP-05 delivery, not a remote provider or compatibility implementation.

### Published input audit

The pinned Containers package executes synchronous Durable Object SQL from its
constructor and alarm scheduling and uses synchronous `storage.kv`. The Sandbox
package additionally imports Workers RPC/tracing and uses storage transactions
and the real Cap'n Web transport. Replacing SQL cursors with empty arrays or
rewriting package APIs is not acceptable conformance evidence.

The native operation boundary now has a bounded, invocation-owned synchronous
path for the optional compatibility runtime. SDK operations still execute in
the original native turn with the same operation/value budgets, effect
classification, epoch checks and cancellation. No language runtime thread
operates cell SQLite. A finite allowlist excludes external/network/container
I/O from synchronous calls. V8 waits on a private reply channel; Node's private
I/O worker wakes a bounded shared buffer and accounts queued messages against
one shared byte limit. The application event-loop watchdog still applies.

Both backends pass real-node SQL/KV mutation and fresh-runtime restart tests,
including cell isolation, saved-context refusal, asynchronous/synchronous
mutation conflicts, alarm reads and pre-effect refusal of synchronous external
calls. Runtime tests cover operation exhaustion, unsafe post-mutation replies,
cancellation with subsequent runtime reuse, and joined shutdown without a reply.
The follow-up adds the actual SQL/KV facade, constructor admission and native
value-only DO RPC slices described below. Complete module/DO dispatch, target
RPC and native critical-section/transaction/lifetime semantics remain required.

### Native-backed Workers storage and activation slice

`packages/cellar-cloudflare` owns the optional module/storage/activation facade.
SQL uses real lazy native cursors with typed blobs, explicit JS numeric
conversion, shared raw/object iteration, counters and size. Handle count and
retained Rust payload bytes are bounded; post-step/result/quota failures remain
unknown outcomes. Cursor release frees quota and cancellation/drop schedules
native cleanup. SQLite VM/page-cache/RSS limits are a separate qualification.

KV uses the existing engine-free persisted-clone codec, with serialization only
inside Node/V8. Cycles, shared binary backing stores, bigint, dates, maps/sets,
errors, sparse arrays and lone UTF-16 value strings survive round trips and
runtime restart. Unsupported RPC/delegate capabilities fail instead of becoming
empty objects. Batches validate/decode before writes, limit aggregate clone
bytes, and protect host-private key access. List iteration jumps the reserved
range in either direction without consuming the visible limit.

A private activation getter reuses only the current continuation's exact native
object token and checks invocation/owner liveness. The Rust host independently
binds resources to `InvocationAuthority`; the container provider authority now
reuses that exact invocation/facet check. Tests cover foreign cells, saved event
contexts/cursors, idle eviction with a new epoch, and instance replacement at the
same epoch. Native identity and V8 random/UUID scheduling paths are implemented.

Published Containers 0.3.7 construction/state/scheduling and Sandbox 0.12.9
construction/configuration/metadata recovery execute through real native nodes.
The fixtures observe a stopped configured provider and intentionally supply no
start/monitor/port methods. They do not run the unchanged upstream apps or the
Sandbox image. Shared initialization is memoized in the fixture adapter so an
alarm and fetch cannot construct duplicate objects for one activation.

Constructor callbacks remain attached to their original event and admission
refuses failed initialization, including rejection with `undefined`. The
follow-up below now uses the actual native gate/reset and async transaction
paths. The retained-driver follow-up below adds response/background/disconnect
behavior; full profile qualification remains required. Do not replace native contracts with detached JS work, guessed rollback or an HTTP-only
RPC substitute. Module/bootstrap globals,
automatic environment bindings, RPC objects and full container lifecycle remain
SP-02/SP-03 work.

### Native callback scopes, input gates and asynchronous transactions

The engine-free SDK exposes an opaque `CallbackScope` captured only inside a
currently polled native block/transaction callback. Its original invocation and
facet authority, block liveness and transaction view are rechecked for each
adapter operation. Native block/transaction guards are installed only for that
poll, then restored. A weak block reference prevents an escaped scope token from
retaining the host input gate after its boundary ends.

The optional JS adapter owns a bounded callback registry per event and uses
`Context::block_concurrency_while`, `Storage::transaction` and native completion
work. Node AsyncLocalStorage and V8 continuation metadata carry the callback
token across awaits. Closing the boundary waits for native commit/rollback.
Callback completion may overlap the next callback's admission without occupying
the JS mutation slot; native queues/guards remain responsible for serialization.
The completion remains a mutation for failure classification, including lost
acknowledgements.

Async transactions share SQL/KV state across global and transaction facades,
support nested commit/rollback and explicit rollback, preserve callback results
and arbitrary thrown values (including `undefined`), and refuse escaped views.
Unrelated promises cannot borrow an active transaction; promises from a closed
callback cannot fall back to root authority. Constructor/method blocks now use
the real input gate. Fixtures observe the Rust callback boundary while a second
request is queued, then verify actual activation reconstruction after failure
and zero application method invocations after failed constructor admission.

This is still partial SP-02 delivery. Default handlers close their bridge on
event completion/cancellation. The retained-driver follow-up below connects
Workers response/background work and dropped transactions to native ownership.
Full profile/deadline qualification and abort/reconstruction remain unfinished. Native SDK lifetime tests do not prove those JS
profile guarantees. The complete delivery objective is unchanged.

### Native synchronous transactions

`storage.transactionSync` now enters the real `Storage::transaction_sync` on the
original native execution domain. A bounded inline callback loop replies to
Node/V8 synchronous requests while keeping that SQLite stack frame installed.
It uses a finite synchronous allowlist, exact callback tokens, current invocation/
activation checks, one deadline across nested callbacks and the shared callback
quota. An optional native `CallbackScope::run_sync` reinstalls an outer async
callback's guards when a synchronous child starts.

Both engines carry a synchronous-continuation bit and refuse async SDK submission
before effects. Native Promise brand checks reject returned Promises even when
their prototypes are masked, without reading a plain object's `then` getter.
Completion is acknowledged only after native commit/rollback and a renewed
invocation check; rollback poisoning or result loss cannot masquerade as a
successful rollback. Ordinary return values and thrown values remain in JS,
retaining identity and `undefined`. Continuations captured inside a closed sync
boundary cannot fall back to root storage authority.

Tests execute actual SQL/KV commit and rollback, nested sync/async savepoints,
escaped continuations, refused async mutations and nesting refusal/release in
Node and V8. Runaway callback tests require refused HTTP output, joined native
shutdown and a new runtime observing the write absent. These do not complete
background/response/disconnect lifetime support or the full SDK/image/provider
qualification. The entire delivery objective remains active.

### Native namespace and value-only cell RPC

The optional JS runtime now maps native `Invocation::CellRpc` and `Output::Rpc`
directly, with an explicit `EventKind::CellRpc` registration and typed handler
declaration. Namespace ID operations use the original native SDK, including
UTF-16 name hashing, unique IDs and namespace-key validation. Event-owned
namespace facades expose method stubs without an HTTP envelope or hosted service.

`activateDurableObject.rpc` dispatches only public prototype methods above the
DO base, including inheritance. It refuses own fields/arrows, getters, reserved
system handlers and base-object properties. Clone graphs retain cycles, shared
binary data, bigint, undefined and thrown values. Remote error own properties
and causes survive while error stacks are stripped. Result serialization failures
are transport/result failures with unknown mutation outcomes, not invented
business exceptions or rollback. Native `Stub::call_with_limits` and the bounded
RPC response decoder apply clone byte/object/value/depth limits to arguments
and both normal/error replies. Rust/native-only callers retain default limits.

Real Node/V8 fixtures exercise native JSON interoperability, fresh-runtime
SQLite recovery, distinct cells/namespaces, forged/name-mismatched IDs, stale
stubs, inherited methods, visibility, rich value/error graphs, and writes before
both business errors and unavailable results. TypeScript declarations and
compile checks cover the implemented storage/RPC slice. This does not implement
automatic activation-owned env bindings, HTTP namespace fetch, placement,
property RPC, pipelining, cross-event stub failure state or remote `RpcTarget`
lifetime/capability transport. Those remain required for the published Sandbox
methods and full SP-02/SP-04 acceptance.

## Reproduction commands

```sh
npm ci --ignore-scripts --prefix tools/sandbox/compat
npm run inventory --prefix tools/sandbox/compat
npm test --prefix tools/sandbox/compat
node tools/sandbox/compat/build.mjs
npm test --prefix tools/javascript
python3 tools/python/test_encoding.py
```

The full affected Rust command is:

```sh
cargo test --locked -p cellar-container -p cellar-guest -p cellar-javascript -p cellar-python \
  --features cellar-python/embed,cellar-javascript/embed,cellar-javascript/node
cargo test --locked -p cellar-javascript --no-default-features --features node --lib
```

Native builds use the checkout's existing target directory on the workspace
volume and the repository's documented Rust/Python prerequisites. The current
main branch also needs `npm ci --ignore-scripts --prefix examples/streaming` and
`npm run --prefix examples/streaming build` before Cargo builds the embedded
streaming example.

Checkpoint `37d53cf6` passed 81 Rust harness/doc cases plus the
standalone CPython bridge, startup and live streaming programs. Four unrelated
fleet-package/documentation cases remain explicitly ignored by their harnesses.
The bridge program includes container controls, owner/crash recovery, alarm
recovery, fault/output gates and joined shutdown. The isolated CPython container
scenario also passes. Node-only library tests pass 13 cases. Published input
checks pass four tests; JS/build/type checks pass eight; Python encoding passes
12. A final repeat of the JS library and synchronous restart tests passes 24
cases after tightening the post-reply authority/allocation guards.

Strict affected-crate and Node-only Clippy, Rust formatting/module layout (918
files), documentation references, new fixture/plan links and whitespace checks
pass. The all-feature native-only dependency guard reports 455 packages with no
JavaScript engine. An initial wider run hit the existing 250 ms V8 fixture
deadline; it passed again with that deadline unchanged. The Node npm example
also required rebuilding its ignored output before the wider run. Hosted CI,
real provider accounts and actual Cloudflare package/image conformance remain
separate qualification; none is claimed here.

The new slice passes 615 Rust harness cases after merging main's Raft image
limit update (`7b38f670`) without conflicts. The checks run across the native core and affected
container/guest/JS/Python libraries and selected integrations, plus standalone
CPython streaming and isolated container programs. This includes five real-node
Worker/constructor tests (Sandbox constructor in Node), 23 JS runtime tests,
SQL buffer/handle quotas and same-epoch instance replacement. A final rerun of
the five runtime fixtures after clone-brand changes also passes. Node-only
qualification passes 14 library cases and three runtime fixtures.

Strict native/affected-crate all-target Clippy and Node-only Clippy pass. Rust
formatting/layout checks cover 927 files. Published input tests (four), JS/build/
type tests (eight), documentation/fixture links and whitespace checks pass. The
native-only all-feature dependency guard still reports 455 packages with no JS
engine. Hosted CI and all remaining SDK/image/cloud qualification remain separate.
The full SP-01–SP-10 objective stays active.

The native value-only RPC checkpoint passes **623 Rust harness cases** across
the core/native/container/guest/JS/Python libraries and selected integrations,
plus the standalone CPython streaming program. Seven real-node Workers fixtures
include the two new Node/V8 native RPC recovery scenarios. Node-only checks pass
14 library cases and four Workers fixtures. Strict affected-crate all-target
Clippy passes; formatting/layout cover 930 Rust files. Documentation and
whitespace checks pass. The all-feature native dependency guard still reports
455 packages without a JS engine. JavaScript/build/type checks pass nine cases,
pinned-input checks pass four, and the package dry run includes all declarations
and namespace implementation files. Full SDK/image/cloud conformance remains
unqualified and the complete delivery objective remains active.

The native callback checkpoint passes 627 distinct Rust harness cases across
the owning suites and isolated reruns, plus the standalone CPython streaming
program. This includes the native 30-second gate/transaction suites, closed and
foreign callback tokens, nine real-node Workers fixtures, nested child drainage,
128-boundary admission/refusal/release, and both backend restart paths. Node-only
qualification passes 14 library cases and five Workers fixtures. Strict affected
all-target and Node-only Clippy pass; formatting/layout cover 931 Rust files.
Documentation/fixture links, whitespace, nine JS/build/type checks, four pinned
input checks and the 455-package native-only engine boundary pass.

Combined runs under concurrent host stress hit existing 100 ms V8 pump, 5-second startup
and 250 ms fixture handler deadlines. The same runtime unit, V8 example, Node
addon and transport checks pass in serial/isolated reruns with every deadline
and assertion unchanged. These are local results, not hosted CI or a complete
Workers/SDK/image/provider conformance claim. Full response/background/disconnect
lifetimes, synchronous transactions and the remaining SP-01–SP-10 work stay open.


The synchronous transaction checkpoint passes 23 JavaScript runtime library
cases and 11 real-node Workers fixtures, including two runaway callback rollback/
restart faults. The owning native concurrency/transaction suites pass both cases
with live/closed synchronous scope checks. Node-only qualification passes 14
library cases and six Workers fixtures. Five additional Node runtime, live SSE/
WebSocket and synchronous restart integration cases pass. The shared callback
quota fixture also refuses synchronous admission while 128 async boundaries
remain open, then permits reuse after they close.

Strict native and affected-crate all-target Clippy and Node-only Clippy pass.
Formatting/layout cover 932 Rust files; documentation links, whitespace, nine
JS/build/type checks, four pinned-input checks and the native-only all-feature
dependency guard (455 packages, no engine) pass. This is focused qualification
of the new boundary and transports, not a repeat of all 627 earlier harness
cases or completion of the remaining SP-01–SP-10 acceptance scope.


### Retained Workers event drivers

`Source::workers_lifetimes()` selects native-owned background drivers rather than
transferring effects to a language task. HTTP and native value-only RPC results
can publish before the original driver finishes draining JS jobs. Explicit
`waitUntil` promises, whole async transaction callbacks and outstanding native SDK
operations retain that invocation's exact authority. Repeated registration by
completing jobs is drained; rejected explicit background work reports a native
background failure without changing an already published response. Live explicit
registrations are bounded by the host's batch limit. A once-only response
publication uses reserved completion capacity even when ordinary SDK pending/total
budgets are exhausted; ordinary operations still refuse at their original limits.
The original default Cellar
handler/stream cancellation behavior remains available.

The new fixture exercises response ordering, foreign saved-context refusal,
dropped transaction commit/rollback, nested and rejected background work, quota
refusal, native RPC result ordering and disconnects before and after response
headers. It also rejects oversized responses while finishing accepted background
work, saturates total/pending SDK budgets without blocking response publication,
and proves native shutdown waits for a held RPC, resumes the JS callback and
persists its write across a fresh runtime. Both engines pass these cases. A follow-up regression exposed premature V8
completion when an I/O-derived continuation crossed eight promise awaits before
starting another native call. Both backends now retain the private job registry
until their full microtask checkpoint reports no owned jobs or native operations;
backend cancellation performs final cleanup. The regression now passes in both
engines, including a subsequent real SQLite write from the derived native call.
Full compatibility-date/flag deadline/eviction behavior,
container monitors, timers and the remaining delivery scope stay open.


The retained-driver checkpoint passes 23 JavaScript runtime library cases, 15
real-node Workers fixtures and five Node/V8 transport/restart integrations. All
compatibility fixtures now enable retained lifetimes, including the actual
published Container/Sandbox constructors. Shared guest tests pass 13 cases;
CPython passes 25 library cases plus its standalone live-streaming program.
Node-only qualification passes 14 library cases and eight Workers fixtures.
These focused checks do not repeat or replace the earlier broad harness audit.

Strict affected-crate all-target and Node-only Clippy, Rust formatting/layout
(934 files), documentation/fixture links, whitespace, nine JS/build/type checks,
four pinned-input checks and the native-only dependency guard (455 packages, no
engine) pass. An initial combined V8 streaming check exceeded its existing 100 ms
budget under host build load. It passed an isolated rerun and the later complete
transport rerun with all deadlines and assertions unchanged. These are local
results. Full Workers/SDK/image/provider and hosted CI qualification remain open;
the complete SP-01–SP-10 objective stays active.
