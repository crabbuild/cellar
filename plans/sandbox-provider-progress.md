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
| SP-02 | Native-backed DO SQL/KV/clone facade, activation-bound authority, object identity, value/returned-function namespace RPC and Node returned readable/writable byte streams and byte-stream input sessions, Request/Response results and HTTP-message arguments, and bounded default-source byte streams, event-style port WebSockets/local pairs, constructor admission, native input gates/reset, sync/async transactions and retained event/background drivers execute in Node/V8; actual published constructors are exercised; Node module HTTP dispatch, namespace bindings and activation-owned env execute against native cells | Complete loader/bootstrap, embedded web globals, other binding kinds, property/target RPC and lifetimes, full versioned lifetime/deadline qualification and profile/version admission; retain native-only dependency boundary |
| SP-03 | Actual published `@cloudflare/containers@0.3.7` constructor, state and SQL scheduling run in both engines; root-event/concurrency-block queued lifecycle and run-bound monitors; native callback admission remains held through void-control completion, with nested transaction/late-continuation refusal; Node runs published readiness with native HTTP ports, `onStart` controls, readiness `onError`, nonzero monitor/`onStop` parameters, idle SIGTERM sleep/wake cycles and persisted hook state; unchanged pinned module named/singleton/random routes execute against the engine peer; async alarm controls yield across overlapping native callback gates; Worker/DO/container WebSocket forwarding passes selected native paths | Embedded web globals, complete continuation/hook/timeout behavior, callback failure/cancellation and image lifecycle/upgrade/fault qualification |
| SP-04 | Actual `@cloudflare/sandbox@0.12.9` constructor/configuration and metadata recovery run in Node; matching upstream app/Dockerfile and `linux/amd64` image digest are pinned; unchanged application exec/file/background-process assertions pass against the real image, including later process discovery, in-object/DO HTTP/direct-RPC execStream, SDK destroy, a gated 2 MiB binary SDK RPC upload and exact binary download through Cap'n Web/local WS proxy, HTTP/WebSocket preview token/runtime routing and drained shutdown | Complete SDK HTTP/RPC/helper coverage and full RPC transport fault/lifetime semantics, complete process-helper/preview coverage, persistence/restart/fencing and failure cases |
| SP-05 | Four-language provider environment/operation/limit discovery; portable binary file read/write/stat/list; Native engine archive implementation, shared input/result bounds and unknown-outcome tests; direct file scenario passes against the real image across events; host-only, revisioned binding journals preserve resource intent across business clear and recovery; E2B/Vercel adapters integrate durable lifecycle reconciliation | Richer operations, named guest configuration/discovery, remote fencing, expiry/reconnect, provider loss/takeover qualification and orphan reconciliation |
| SP-06 | Experimental native E2B/Vercel providers implement bounded exec/files with durable binding reconciliation; protocol/restart/failure peers and CPython/Node/V8 fixtures pass; operation and fencing gaps are declared | Live account/image, expiry, takeover and orphan qualification deferred until user configurations; other framework/provider work continues |
| SP-07 | Separate 1.x contract identified in the design; it is not the 0.12.9 baseline | Pin and run exact current package/helper/image, extended container API, errors, streams, cancellation and supported persistence |
| SP-08 | No Daytona/Modal implementation yet | Provider implementations and individual process/files/storage/PTY/GPU/snapshot qualification |
| SP-09 | No Dynamic Worker or hosted Cloudflare provider yet | Actual isolate loader/limits/capability isolation; optional authenticated hosted gateway, outbound policy and cleanup qualification |
| SP-10 | Initial input manifest explicitly reports `conformance: not_run` | Publish operation/version compatibility results only after actual four-language, lifecycle, fault, resource, fencing and shutdown qualification |

## Delivery order

Keep the next milestones tied to usable published SDK operations. Extend the
shared native provider boundary when a feature needs it; provider selection and
credentials remain host configuration. Native Rust/Python/JS/TS controls stay
independent of the optional Workers runtime.

| Priority | Feature milestone | Acceptance boundary |
| --- | --- | --- |
| Delivered | Container-port WebSockets, local pairs, SDK binary upload/download and returned RPC responses | Pinned SDK/image round-trip; metadata, source-prefix/error, cancellation, ownership, output proof and joined cleanup tests |
| Delivered fix; broader qualification open | Requested V8 shutdown and initial-error diagnostics | Deterministic regression plus 97 selected native cases at original budgets; earlier cold-call delay remains unproven |
| Delivered Node slice | RPC `Request` transfers and HTTP-message arguments | Native metadata/body ownership, round-trip forwarding, pre-effect refusal, streaming in both directions, live abort after EOF and resource release |
| Delivered fix; profile partial | Overlapping callback/alarm resumptions | The original extra-barrier image probe passes after async alarm APIs use the asynchronous native bridge; deadlines and native gates remain intact |
| Delivered Node slice | Worker/DO/container upgrade forwarding and published Sandbox WebSocket previews | Native network tests, held-receiver disconnect and exact-image text/binary round trips with joined cleanup |
| Delivered selected slice | Concurrency-block controls and published error/exit hooks | Void controls join before native admission releases; nested blocks execute; transactions, inherited transaction views and late continuation admission refuse. Node exercises published readiness errors and nonzero exit/stop parameters |
| 1 — qualification | Cold-first-call latency, callback failure/cancellation and remaining hooks/timeouts | The 250 ms module and alarm-start failures reproduced in this campaign; a fresh full module run passes unchanged. Native routing/activation before DO JS entry is the next investigation boundary; no fix is established |
| 2 — feature delivery | Named portable bindings and reconnect/expiry/orphan handling | Same native contract in four languages; explicit fencing guarantees, durable reconciliation and no blind mutation replay |
| Following milestones | Sandbox 1.x, Daytona/Modal, Dynamic Workers and optional hosted Cloudflare | Independent pinned profiles and the SP-07–SP-10 acceptance requirements |

Failure, lifetime and resource checks belong to each delivered feature. E2B and
Vercel live qualification remains deferred until configuration is supplied; it
does not block the local Cloudflare or common-framework milestones. This order
does not mark any partial SP profile complete.

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
The fixtures now use the activation-bound `durableObjectContainer` running
property rather than retaining an asynchronously observed stopped value. They
now supply the root-event queued lifecycle slice described below; they do not
run port methods, the unchanged upstream apps or the Sandbox image. Shared
initialization is memoized in the fixture adapter so an
alarm and fetch cannot construct duplicate objects for one activation.

The provider contract separately declares `synchronous_running` and implements
`ProviderSession::running_snapshot`. Native reads the actual cell-owned engine
state without I/O; the shared session checks invocation/facet authority before
and after the observation. Async-only providers refuse before dispatch. Rust,
Python and JS/TS capability descriptors report the opt-in. Node/V8 fixtures
observe actual native start/destroy against the engine protocol peer and reject
another cell's saved running facade. The observation also executes inside native
sync/async transactions, and both codecs refuse to persist the branded live
container capability as plain data. This is running-property evidence, not
evidence that a full published application or a real image works.

### Queued root-event lifecycle

The optional Workers container facade separates synchronous admission from
native execution. Start/signal return void after shared input validation and
effect-free provider preflight. Native, invocation-owned tickets retain immutable
inputs, consume once and hold quota until execution or discard: at most 64
commands, with aggregate serialized input bytes bounded by the host body limit.
The provider must opt in to both queued lifecycle and synchronous running.

Execution retains the original event, authority checks, provider serialization
and output gate. It does not occupy the ordinary JS storage mutation slot.
Submitted failures keep their unknown outcome and are not automatically replayed;
explicit destroy may clean up. Transaction and block callbacks currently refuse
queued lifecycle before effects; callback-owned queue drainage remains required.

Node/V8 peer fixtures exercise blocked startup with native storage progress,
duplicate/invalid admission refusal, normal destroy, explicit monitor error
identity, restart and a submitted startup failure. Native integration also
reproduces and fixes delayed polling of an old monitor after destroy/restart:
each monitor captures its run's exit record before returning its future. Old
records survive only while producers/observers retain them. This is protocol-peer
qualification, not an unchanged package/image or full lifetime-profile pass.
The blocked-start probe observes engine entry before issuing its same-cell
progress request. An earlier probe issued that synchronous request while the
fixture was still exercising callback gates and timed out in Node. Arbitrary
interleaving around callback admission remains unqualified by this test.

### Abortable container HTTP and buffered Workers ports

The shared provider session accepts an optional native abort signal. An
already-aborted signal refuses before dispatch; cancellation after admission
drops the invocation-owned provider future and reports an unknown outcome.
An available provider acknowledgement wins a simultaneous abort. Guest failure
mapping preserves these outcomes without treating cancellation as rollback.

Node and V8 issue private per-operation cancellation capabilities for portable
`container.port.fetch`, using the same bounded native acknowledgement path as
typed RPC. The operation slot and byte quota remain held until Rust responds.
Other container operations do not acquire that guest cancellation capability.

The Workers port facade waits for admitted startup, bounds request-body
consumption before dispatch, forwards through the selected native port and
constructs a standard buffered response. Redirects stay manual. Aborted body
reads cancel their JS stream source; native cancellation maps to the caller's
abort reason. The facade requires standard Request/Response globals, available
in Node; embedded V8 explicitly refuses until its web runtime is implemented.

Thirteen JavaScript build/type/package/port tests pass, including binary data,
redirect preservation, oversize streamed input refusal, aborted pending body
reads and retired authority. Native provider tests cover pre-dispatch refusal,
dropping pending work and completion winning a simultaneous abort. Node executes
the real published `startAndWaitForPorts`, persists healthy state, invokes
`onStart`, forwards HTTP, and recovers stopped state through the package's own
monitor/stop synchronizer and `onStop`. Both fresh-runtime rounds pass; V8 checks
the explicit missing-web-profile refusal. Native Node/V8 integration tests also
cancel pending-head and pending-body endpoints, assert the `Cancelled` code and
unknown submitted outcome, observe peer cleanup, and perform storage/fetch work
afterward while the container remains running. The pre-aborted calls keep
`not_committed`. All 34 JS runtime unit tests and 15 Workers integrations pass.
Streaming responses, TCP streams,
WebSocket upgrades, complete timeout semantics and unchanged applications/images
remain required. This does not change the pinned inventory's `not_run` status.

### Real-image startup preparation

On 2026-10-25 a separate task-owned Colima VM (Docker 29.5.2, aarch64 with
Rosetta amd64 emulation) pulled the exact pinned Sandbox 0.12.9 manifest digest.
Its amd64 image started `/container-server/sandbox`, reported version 0.12.9,
and returned `success: true` / `status: healthy` from `/api/ping` over an explicit
loopback port. This proves image startup only: published SDK calls through
Cellar, image ownership/network policy and teardown qualification remain pending.

The VM's first automatically published port reached an unrelated pre-existing
host service. Qualification switched to an explicitly selected free loopback
port and verified the expected Sandbox protocol response. A successful HTTP
status alone is insufficient evidence that a VM port reaches the intended image.

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

### Node module HTTP and activation-owned environment

`workersApplication` now adapts an application's default fetch handler and
exported DO classes without rewriting their source. The native host separately
declares each class, namespace, event kind, variable and container image. Node
uses standard Request/Response with bounded buffered conversion, separate
Set-Cookie headers, native namespace routing and per-operation cancellation.
Embedded V8 explicitly refuses this module HTTP profile before host startup
because its web bootstrap remains absent.

Module-provided DO environment namespaces and saved stubs resolve the current
event of the same activation; another object's continuation is refused. Native
tests cover named/opaque IDs, binary HTTP, variables, restart recovery and a
write whose oversized reply fails while the earlier effect remains observable.
The public event-owned namespace helper retains its original lifetime.

The actual pinned celld Container module with Containers 0.3.7 now executes its
named, singleton and random routes against native cells and the engine protocol
peer. Four distinct objects own four native container records. Cleanup uses
the package's inherited destroy method, waits for delayed engine acknowledgement
and requires zero remaining containers plus joined native shutdown. Full monitor
wrapper promises are retained under the original event's bounded job quota.
This is peer evidence, not real-image conformance or complete package lifecycle.

The web extension slice supplies ordinary `Response.webSocket: null` and a
byte-only IdentityTransformStream with BYOB reads, backpressure, cancellation
and preserved caller buffers. JavaScript tests exercise those behaviors and
HTTP size/header bounds; this does not implement upgrade transport or streamed
HTTP across the native bridge. The complete pinned baseline remains `not_run`.

Module follow-up validation on Rust 1.98.1: 34 JS runtime unit tests, 16 Workers
integrations, 2 portable container integrations, 2 typed RPC integrations and
2 module integrations pass. The module target passes again after its test-helper
readability refactor. Provider/guest checks pass (8 + 16 unit tests and 1 native
provider contract), as do the isolated CPython container scenario and 2 provider
documentation tests. All 19 JavaScript build/type/package/HTTP/stream tests and
4 pinned-input tests pass; package dry-run includes all 18 declared files.
Strict affected-crate all-target Clippy, Rust format/layout (1047 sources), JS
formatting (48 owned sources), documentation references and the 466-package
native-only engine guard pass. No live cloud-provider result is claimed.

### Real SDK/image qualification and independent Workers I/O

| Boundary | Delivered / observed evidence |
| --- | --- |
| Routed object calls | Workers RPC/HTTP can overlap under native routing/output gates and existing operation/byte quotas; they remain mutations for failure provenance |
| Workers port requests | A profile-only `workers.container.fetch` operation delegates to the selected provider; native lifetime admission is required before effects and ordinary portable storage mutation rules remain intact |
| Local progress | Port I/O no longer occupies the JS local-storage mutation slot; the actual Sandbox onStart transaction can progress while version/port work is pending |
| Alarm ownership | Module dispatch serializes alarm callbacks per object, retaining each original native invocation; revocation releases a cancelled wait without allowing a later callback to bypass an active predecessor |
| Real image | Unchanged pinned Sandbox app/SDK executes commands with stdout/stderr/nonzero child exit and a file round trip across requests |
| Remaining live failure | `/serve` starts the process, then loses its function-bearing result at the value-only RPC codec; `/processes` is not reached and requires the same delegate transport |

The full opt-in `sandbox_image` acceptance target remains failing at `/serve`;
its assertions and complete route requirements are retained. Explicit SDK destroy
and zero-owned-container checks run after failures too. The first live runs also
exposed overlapping alarm callbacks: one SDK timer promise remained unresolved,
and native shutdown correctly reported undrained work. Serial alarm admission
fixes that real-image drain failure without increasing shutdown deadlines.

A task-owned Colima daemon uses an observed-free ephemeral port range to avoid
a verified host/VM forwarding collision. The initial test command terminated
the SDK's persistent shell; the corrected probe runs a child shell and preserves
the exact exit-code assertion. Later file comparison uses JSON-encoded contents
to preserve the trailing newline independently of session-output normalization.
This is evidence for those assertions, not a full SP-04 pass. RPC function and
RpcTarget transport must retain their native event/activation authority, quotas,
caller disposal and cancellation; removing process methods is not conformance.

Follow-up validation: 21 JS build/type/package/HTTP/alarm tests pass, including
queued and active alarm revocation. Native qualification passes 34 runtime unit,
2 portable container, 16 Workers and 3 module integrations. The new alarm probe
uses a 250 ms actor-operation deadline to force redelivery while its handler
remains live; its two-second observation window is inside a separate five-second
handler budget. Existing handler deadlines and assertions remain unchanged.
The provider/guest unit and contract checks pass (8 + 16 + 1), as do strict
all-target Clippy, Rust format/layout (1048 sources), JS formatting (49 sources),
input identity checks, documentation checks and the native-only dependency guard.

One combined typed RPC run hit the existing 250 ms V8 codec-budget deadline;
the exact current binary passed that test in isolation without source changes.
The stale-codec test passed in the combined run. The full live image target was
rebuilt against the final runtime: command/file assertions and explicit teardown
pass, `cells_drained` and `application_work_drained` are true with no timeout,
and the overall target remains failing at `/serve`'s function-bearing result.
This is not a complete published SDK/image qualification result.

### Native RPC result transport lifetime

Native `RpcData::Leased` carries a finite value with a shared, revocable result
lifetime. `sdk::rpc::Lease::for_context` retains the original native event under
its existing cancellation, retirement and deadline limits. Weak closure
observers and revocation guards do not keep an ownership alias alive.

| Boundary | Implemented behavior |
| --- | --- |
| Local calls | Aliases share one lifetime; the last drop notifies the original producer |
| Peer calls | The existing authenticated RPC route sends a versioned finite-value prefix followed by a live response body; body closure releases the result |
| Revocation | Explicit close, producer retirement or peer-reader domain shutdown closes retained aliases and releases owned transport work |
| Bounds | Check the finite peer value's 64 MiB ceiling before allocation; reject invalid/truncated framing, nested leases and unknown versions |
| Existing value services | Refuse leased values or arguments explicitly; preserve payload accounting and the existing output proofs |

This does not yet export/import JS functions or `RpcTarget`, dispatch callbacks,
provide reverse callback arguments, or implement language-level `dup`/disposal.
Those operations must retain the original event and apply their own quotas and
output proof before returning delegated results. The live Sandbox `/serve`
failure therefore remains unresolved, and SP-02/SP-04 stay partial. Full routed
peer/fault qualification and profile-specific result budgets also remain open.

Validation for this transport slice passes six lease/framing unit tests and the
extended real native-cell integration. The latter checks cleanup through the
producer's original SDK context and revocation on retirement while a caller
retains an alias. HTTP/1 loopback tests check caller-drop and server-revocation
behavior; they do not substitute for a routed multi-node fault test. All 16
shared guest unit tests, strict affected-crate all-target Clippy, Rust
format/layout (1,051 sources), JS formatting (49 sources), all 21 JS tooling
tests, three container documentation examples, documentation references and
the 466-package native-only engine guard pass.

Final Node/V8 regressions pass all 16 Workers integrations and both typed RPC
integrations with existing deadlines and assertions unchanged. The new native
lease API documentation example also compiles. No new live SDK/image or
cloud-account acceptance result is claimed for this transport-only change.

### Returned functions and live Sandbox processes

The optional Workers adapter now materializes returned functions over native
leased results. The native registry binds each export group to its original
cell object, scope, epoch and event; a fresh routed CellRpc only brokers a call
to that event's existing driver. It never supplies SDK authority to the function.

| Boundary | Behavior |
| --- | --- |
| Value graph | Preserve cycles, aliases, sparse arrays, maps and shared buffers; fresh marker identities carry no authority without a native lease |
| Callable result | Support plain-object functions, nested returned functions and native value arguments; errors retain existing clone/error behavior |
| Ownership | Native handles for each imported function, separate `dup` aliases, explicit/object disposal, automatic caller-end cleanup and producer revocation |
| Resource admission | Enforce native handle/group quotas, pending-call semaphores, bounded mailboxes and shared bridge-byte reservations |
| Output proof | Flush the original producer's storage before sending a delegated reply; the broker event's output gate cannot establish that proof |
| Failed publication | Close unpublished groups so an unrepresentable result does not strand its producer |
| Scope | Export/import uses root events; ordinary value RPC retains its existing transaction/block callback path |

The first actual SDK/image run passes all unchanged route assertions: stdout,
stderr and exit 7; exact file contents across requests; `/serve` process startup;
and `/processes` discovering the same live process in a later request. Explicit
SDK destroy removes its containers, and both cell/application work drain without
timeout. A rebuilt run after adding shared-budget reservations also passes.
Methods stay on process objects; no DTO stripping or SDK/application rewriting
was used. The test remains the existing basic-route acceptance, not full SP-04.

Initial Node/V8 native RPC checks pass invocation-owned calls, cyclic callable
graphs, nested functions, duplication, explicit disposal and refusal of saved
functions in a later event/restart. Node initially stalled because its local
mutation classifier counted a mailbox poll as a mutation; matching the Rust
independent-operation classifier fixes it without extending any deadline.

Still required: returned `RpcTarget` and property RPC, function properties,
callback arguments/reverse transport, scoped callback imports, pipelining,
multi-node fault/output-proof qualification, complete disconnect/deadline
semantics, streamed SDK output and previews. Hosted providers, portable files
and durable remote bindings, newer Sandbox profiles and Dynamic Workers remain
open. The complete published baseline continues to report `conformance: not_run`.

The publication-failure regression now checks an oversized function-bearing
reply after a storage write: the caller receives an unknown outcome, the write
remains visible, and the unpublished export closes without stranding its event.
Codec tests also check object prototypes, no-op value disposal, stale disposal
and job admission before native export registration. TypeScript maps returned
functions to asynchronous, disposable stubs with separately owned `dup` aliases.

Repeated live runs exposed an isolated-daemon resource limit: 29 empty test
bridges exhausted Docker's default address pools, confirmed by a direct engine
probe. Only inactive, host-labelled bridges in this task's daemon were removed.
The image harness now removes its own verified empty bridges after shutdown;
two consecutive runs pass and leave only the three default networks. Container
and network ownership assertions remain strict, and all deadlines are unchanged.

Final-source checks pass 36 runtime unit tests, 16 Workers integrations, both
typed RPC integrations, 16 shared guest unit tests, 25 JS/tool/type checks and
four pinned-input checks. Strict affected-crate all-target Clippy passes with
Node, embedded V8 and CPython enabled. The final image run after the declared-RPC
admission guard passes with joined shutdown and removal of owned containers and
networks. Rust layout/format covers 1,056 files; JS format covers 51 sources.
Documentation and whitespace checks and the native-only 466-package engine guard
pass. This remains local evidence, not hosted CI or live-cloud qualification.


### Portable provider files

`container.file.read`, `.write`, `.stat` and `.list` extend the shared provider
vocabulary and Rust/Python/JS/TS facades. Native uses Docker/Podman archive
endpoints; it does not shell out or extract archives on the host. Every file
operation retains native invocation/owner/run checks and the mutation output
gate, including reads which issue external I/O.

| Boundary | Delivered behavior |
| --- | --- |
| Input | Canonical absolute UTF-8 container paths; host byte limits, permission-only modes and entry budgets; no implicit directory creation |
| Data | Binary bytes, zero-byte files, UTF-8/query punctuation in names and portable metadata |
| Native archives | Bound downloads including overhead, check raw entry offsets/sizes before GNU/PAX processing, reject sparse/truncated/multi-file reads and never extract on the host |
| Listings | Direct-child metadata, duplicate-name refusal and complete-result byte/entry limits; native archive scans include descendant contents |
| Provider failures | Pre-dispatch validation refuses before provider calls; lost write replies preserve the observable effect and are not replayed; oversized/invalid provider replies remain unknown outcomes |
| Live image | Independent portable-file fixture preserves exact bytes and permissions across requests, validates them through a real container process, rejects oversized archive/list results, destroys the instance and joins shutdown |

The real-image target also reruns the unchanged published Sandbox application;
both tests pass. This is local Docker/amd64-image evidence, not Podman or live
cloud qualification. Native-only dependency inspection remains engine-free
(466 reachable packages). Durable remote bindings, fencing/reconciliation and
the remaining SP-05 work are still required; no remote provider is claimed.

Validation for this slice passes four archive unit tests, both native container
integrations, 11 provider unit tests, the custom-provider integration and three
adapter doctests. Node and embedded V8 container integrations and the CPython
container harness pass; all 25 JS/tool/type tests pass. Final regressions pass
36 runtime unit tests, 16 Workers integrations and both typed RPC integrations.
Strict all-target Clippy passes for the host, provider and both language crates
with Node/V8/CPython enabled. Format/layout covers 1,061 Rust files and JS format
covers 52 owned sources; documentation and whitespace checks pass. Both real
image tests pass again after the strict JS byte-input guard, with owned container
and network cleanup. No existing assertion or deadline was weakened.


### Protected durable binding journal

Native host providers can retain a credential-free resource ID, provider and
configuration/parameter digests, generation, stable request ID, desired lifecycle intent
and pending/confirmed observation in `BindingJournal`. Records live in reserved
SQLite tables rather than application KV. Application SQL cannot read or alter
them, and business `delete_all` preserves records and format markers.

| Boundary | Behavior |
| --- | --- |
| Invocation | Journal handles capture exact root-event authority; stateless, foreign-cell, later-event and storage-transaction use refuses |
| Reservation | Version-checked writes await required storage durability and recheck the current revision before returning |
| Unknown create | Keep the pending reservation and stable identity across restart; a second initial reservation conflicts instead of authorizing another create |
| Generation | Keep provider/config/request identity stable, never replace or forget a known resource, and require confirmed retirement plus a fresh request ID before generation + 1 |
| Bounds | 64 host-selected binding records per cell, 16 KiB per encoded record; retained tombstones count toward the limit |
| Recovery | Validate format-1 marker/layout and retained records before schema mutation; refuse unmarked, incompatible or corrupt data |
| SQL failure | Native autocommit and implicit batches use fixed savepoints; failed updates roll back and preserve original SQLite failures |

The private journal is a persistence primitive, not a shipped remote backend or
provider-side fence. Its activation epoch records provenance and is explicitly
not a distributed ownership token. Providers must verify account/configuration
identity, correlate remote request metadata, enforce their service/broker lease
and fence, and qualify expiry/orphan cleanup. Named guest bindings and their
four-language discovery still require integration. The full SP-05–SP-10 scope
remains open; no live-cloud result is claimed for this change.

Final-source validation passes all 44 storage unit tests, 11 shared-provider unit
tests and both provider integrations. The native journal fixture covers three
host lifecycles, pending-create recovery, superseded reservations, revision
conflicts, transaction refusal, protected SQL, business clear and replacement.
Strict all-target Clippy passes for `cellar` and `cellar-container`. Rust
format/layout checks cover 1,065 files; docs and native-only dependency checks pass. Native-only dependencies remain 466 packages with no JS engine.

The journal fixture also exhibited first-request timeouts at its existing 250 ms
handler budget during intermediate runs. The final uninstrumented run passes
in 0.63 seconds overall with the same per-handler budget; the timeout cause is
not established. No assertion or deadline was relaxed. This local result does
not complete the broader deadline/fault or live-provider qualification.


### Experimental direct E2B execution profile

The Rust `e2b::E2b` provider uses pinned upstream control OpenAPI and envd
HTTP/Connect protocols. The existing Rust, Python, JavaScript and TypeScript
container APIs select it in the native host; no vendor SDK or JavaScript engine
is introduced into a native-only application.

| Boundary | Delivered behavior |
| --- | --- |
| Explicit ownership choice | Direct E2B requires `LocalChecksAndExpiry`; strict provider-fence admission refuses before allocation. Shared capabilities expose the configured `fencing` guarantee in all four languages. |
| Durable lifecycle | Persist a stable creation identity before POST, retain unknown outcomes across restart, reconcile by exact protected metadata and confirm one resource; never blindly replay a create after an empty listing. |
| Delete / replacement | Persist deletion intent, inspect the original ID after a lost reply, confirm retirement before a new generation; reject foreign configuration or metadata before commands/deletion. |
| Protocol / credentials | Host-pinned HTTPS endpoints, manual redirects, separate API/envd authentication, bounded control bodies and original-context HTTP; no credentials in guest results or the protected journal. |
| Commands / files | Direct argv, cwd/env/Unix username, bounded binary stdout/stderr and child exit codes; file read/write/stat/list, UTF-8 names and permission modes. Writes use tee/chmod without shell construction or implicit mkdir. |
| Cancellation / bounds | Overall operation deadline, incremental Connect decoding, combined output budgets, strict framing/terminal checks and dropped native streams on failure. Stdin submission and stdout draining run concurrently. |

The native protocol peer covers lost create/delete replies, host recovery,
metadata mismatches, binary/empty/512 KiB files, combined output, nonzero child
exits, malformed/compressed/truncated streams, output overflow, timeout cleanup
and redirect refusal. Actual CPython, Node and embedded V8 fixtures exercise the
same backend and reject a saved facade from a later event. TypeScript checks
cover the portable API and fencing descriptor. These tests execute no E2B VM;
source pins and local fixtures do not establish live provider or SDK conformance.

Direct E2B cannot fence an already accepted remote request. Fixed lifetime is
not inactivity or automatic owner-loss cleanup. Interactive handles, monitor,
signals, ports/previews, PTY/snapshots and Workers queued/synchronous lifecycle
remain unsupported. A fenced broker, live-account expiry/takeover/orphan/fault
qualification, named guest bindings and the remaining SP-05/SP-06 requirements
are still open. The E2B manifest retains `qualification: not_run`; Vercel,
Daytona, Modal, current Sandbox, Dynamic Workers and the rest of SP-01–SP-10
remain in the active full goal.


Final-source checks pass 14 provider unit tests, 16 shared guest unit tests, all
three provider integrations and four adapter doctests. E2B acceptance passes in
CPython, Node and embedded V8; the existing native-provider container regressions
also pass in those runtimes. Strict all-target Clippy passes for the provider,
shared guest and both language crates with all affected runtime features enabled.
All 25 JS/tool/type tests, both E2B pinned-input tests, 55 owned JS/TS formatting
checks, documentation references and the 466-package native-only dependency guard
pass. Rust format/layout covers 1,080 sources. Owned-source whitespace checks
pass; the pinned process protobuf retains its upstream whitespace and is checked
byte-for-byte instead. No existing deadline or assertion was weakened, and no
live-cloud qualification is claimed.


### Experimental Vercel named-sandbox/session profile

The native `vercel::Vercel` provider implements portable lifecycle, bounded
commands and file operations over the pinned named-sandbox/session REST API.
Reviewed upstream source is `vercel/sandbox` at
`8910e6205b1c72e1ad6ebaa2ceae236a3aa4d87f`, SDK reference `3.9.0`.
The shared native HTTP adapter retains original invocation authority, bounded
response collection, manual redirects and future-drop cleanup for E2B and Vercel.
Neither provider embeds a vendor SDK or adds a JavaScript engine to native hosts.

| Boundary | Behavior / evidence |
| --- | --- |
| Ownership | Shared host `Ownership` choice retains E2B's existing public path; direct Vercel refuses strict fencing and requires explicit `LocalChecksAndExpiry`. |
| Identity | Durably reserve a random name and creation intent, correlate one protected ownership tag, pin the exact returned session ID, and refuse changed sessions before command/deletion effects. |
| Lifecycle | Disable persistence and use `resume=false` inspection; lost creates reconcile the same name without another POST. Confirm retirement before replacing a generation. |
| Deletion | Persist deletion intent, delete the verified named resource with owned orphan-snapshot cleanup requested, observe absence, and reconcile a lost reply without blindly replaying deletion. |
| Binary execution | Image-local Python helper receives JSON over stdin and emits ASCII/base64 channel frames over Vercel's text logs; argv/cwd/env never enter a shell. Native code validates command/session identity and both framing layers. |
| Files | Raw binary read endpoint; helper write/stat/list with existing-parent semantics, binary data, modes and bounded direct-child metadata. Lost stdin/write replies remain uncertain and observable, without replay. |
| Local acceptance | Native protocol/restart fixture, actual CPython/Node/V8 using the same portable application fixture as E2B, and separate tests of the actual helper with owned temporary files and controlled subprocesses. |

The helper is a remote image prerequisite, not an embedded Python interpreter.
Its local acceptance checks invalid UTF-8 on both channels, child exit/cwd/env,
concurrent 512 KiB stdout/stderr pipes, large/empty files, permissions, symlinks,
entry/byte limits, no implicit mkdir, output overflow and joined child deadline
cleanup. The native protocol peer covers lost create/delete/write replies,
foreign metadata/session refusal, native recovery, exact binary files, command
output, truncated/error/oversized streams, deadline/stream cleanup and credential
redirect refusal. The peer executes no VM or real command.

Still required: live Vercel image/account acceptance, expiry/takeover/orphan and
remote-fencing qualification; interactive processes, signals/monitor, inactivity,
ports/previews, PTY and snapshots; named guest binding discovery and the full
SP-05/SP-06 profile. Fixed session timeout does not establish owner-loss cleanup.
The manifest keeps `qualification: not_run`; Workers synchronous lifecycle remains
unsupported. Other SP-01–SP-10 targets stay active and incomplete.


Final-source validation passes 16 provider unit tests, 16 shared guest tests,
all four native provider integrations and five adapter doctests. E2B and Vercel
fixtures pass in CPython, Node and embedded V8; the existing Node/V8 native
container regressions also pass. Strict all-target Clippy passes for the provider,
shared guest and both language crates with all affected runtime features enabled.
Four actual-helper tests, four E2B/Vercel pinned-input tests, all 25 JS/tool/type
tests, format/layout (1,092 Rust sources; 57 owned JS/TS sources), documentation
references and the 466-package engine-free native dependency guard pass. CI now
runs the Vercel input and helper checks. No live-account acceptance is claimed.

The existing journal regression again exceeded its unchanged 250 ms initial
handler budget in an intermediate full-suite run. Three unchanged standalone
runs and the final full-suite run pass; the latency cause remains unproven.
No deadline or assertion was relaxed. This repeats the earlier timing caveat and
is not evidence that the broader deadline/fault qualification is resolved.

### Incremental port responses and published Sandbox streaming

The common provider vocabulary now includes `FetchStream`, `ReadResponse` and
`CancelResponse`. Native keeps the HTTP head and bounded prefix separate from
its body stream. Each pull has a byte limit, concurrent reads refuse, EOF frees
the handle, and cancellation drops the response consumer. Response handles share
the existing 64-resource invocation limit. Original event authority is checked
before and after reads; a retained response cannot borrow a later event's handle
table. Dropping the event releases abandoned consumers.

| Application surface | Implemented path |
| --- | --- |
| Rust | The shared `Session` operations, including native abort signals |
| Python | `port.fetch_stream()`, `read()`, `chunks()` and `cancel()` |
| JavaScript / TypeScript | `port.fetchStream()`, typed pull/iterator/cancel methods and read abort signals |
| Optional Cloudflare facade | `getTcpPort().fetch()` returns a demand-driven web byte stream, including BYOB and abort after headers |

The [stream guide](../crates/cellar-container/streams.md) documents the wire
frames, lifecycle, limits and four-language usage. Upload bodies remain bounded
and buffered. Cancelling output cannot establish command termination or HTTP
rollback. E2B/Vercel do not advertise these new operations.

The real-image fixture extends the published `Sandbox` class with a probe that
calls its unchanged `execStream()` and published SSE parser. A command writes its
first stdout event and waits for a file; only the reader's observation of that
event allows a second SDK command to create the file. The reader then observes
final stdout, stderr and exit code 7. This passes against the pinned 0.12.9 SDK
and linux/amd64 image digest already recorded in the manifest. The original
upstream exec/file/process routes also pass, followed by explicit destroy,
complete host drain and no remaining owned containers.

This is incremental port consumption inside the object. Streamed RPC return
values, Worker response forwarding, previews, upgrades, fault/ownership
qualification and the complete SP-02/SP-04 profiles remain pending. Live E2B and
Vercel account qualification is deferred at the user's request; Cloudflare and
the common framework remain the current priority. The full SP-01–SP-10 goal is
active and incomplete.


During the stricter real-image rerun, native startup refused before the stream
probe: the fence helper exited 137. Test-only provider diagnostics exposed the
native chain, and the task-owned VM's kernel log confirmed a memory-cgroup OOM
kill of `nft` under its 16 MiB cap. The VM retained 22 Cellar fence tables from
prior namespaces. The one-shot helper now has a bounded 64 MiB memory/swap
allowance; CPU, capabilities, rules, validation and refusal on fence failure stay
intact. The stricter image test passes against the same ruleset without deleting
fences or relaxing deadlines. Much larger rulesets remain unqualified.

The image probe specifically requires a stdout event before releasing the file
gate, and separate stdout/stderr data events for final output. SDK start records
repeat the command text and cannot satisfy these checks. Test-only native
provider diagnostics preserve the production guest error envelope's privacy.

Validation passes 20 provider unit tests (including strict stream-frame checks),
16 shared guest tests, all four provider integrations and five adapter doctests.
Node and embedded V8 pass finite/pending HTTP reads, the shared handle cap,
pre-dispatch refusal, pending-read abort, stale-reader refusal and abandoned
connection cleanup. CPython passes its new finite/pending stream facade checks.
All three Worker-module regressions, all three native engine/package tests, the
stricter published SDK/image probe, 29 JS/tool/type checks and four compatibility
inventory checks pass. Strict adapter all-target Clippy and native-host library
Clippy pass. Format/layout covers 1,095 Rust files and 58 owned JS/TS sources;
documentation references, guide links and native-only dependency checks pass.

An intermediate Worker-module count request returned HTTP 500 without a backend
cause in the original diagnostic. The test now retains status/body/restart
context on failure; the final run passes at its unchanged budget. That original
cause remains unproven. No deadline or assertion was weakened; the image output
assertions were strengthened to distinguish data events from SDK start records.

### Worker and Durable Object HTTP response forwarding

Standard web `Response` bodies now flow incrementally through the optional
module adapter and namespace `fetch()` facade. The native transport retains
headers/body capabilities under the original invocation and facet, with bounded
prefixes and reads. A joined disconnect waiter wakes pending web reads and
releases upstream consumers; it uses the existing operation and pending budgets.

| Boundary | Implementation / evidence |
| --- | --- |
| Native authority | Response reads, forwarding and cancellation check exact native invocation/facet authority; an actual Rust host test refuses a foreign live event and a later event of the same cell. |
| Body versus event lifetime | Explicit `response.finish` closes the HTTP body independently of background monitors/jobs. Native network tests verify successful EOF, failure and cancellation while independent `waitUntil` work remains held and can later finish. |
| Durability at EOF | `Context::finish_stream` gates clean EOF behind writes performed after the final chunk. The native proof fixture withholds that gate, then verifies successful release and typed `DurabilityUnproven` refusal without clean EOF or a success acknowledgement. |
| Web conversion | Headers precede the first read; per-chunk limits permit larger cumulative output. HEAD/no-body responses release unused readers; disturbed bodies refuse; producer/overflow errors remain body failures. |
| Publication | A post-header failure errors the body and preserves earlier effects; it cannot replace the status already sent. The existing module regression now requires that body failure and still verifies the committed effect. |
| Packaging | The actual packed npm artifact imports and dispatches in an isolated directory. This caught and fixed the previously omitted `response.js`; the new `output.js` is also included. |

The pinned Sandbox 0.12.9 SDK/image now passes an additional external HTTP test:
a probe calls the unchanged `execStream()` inside its published Sandbox class,
returns the SSE body through a DO fetch and Worker fetch, and the real network
client must observe a stdout event before a second request releases the command.
The client then verifies final stdout/stderr data events and exit code 7. This is
HTTP forwarding, not returned-stream RPC transport. Original upstream basic
routes, the in-object stream probe, explicit destroy and joined shutdown also
pass; no owned containers remain.

Validation passes the Worker/DO HTTP network test, three module regressions,
native live/stale handle authority, native EOF proof/cleanup/retirement, CPython
streaming, and the published SDK/image test. Node's existing SSE/WS test passes;
embedded V8's existing SSE/WS test passes in its diagnostic run. Frontend/tool
checks pass 37 tests, including packed-package import and cleanup/error cases.
All 16 shared guest tests, affected-adapter all-target Clippy and native-host
library Clippy pass. Rust format/layout checks cover 1,104 sources after merging main, JS formatting
covers 62 owned sources, and documentation plus the native-only dependency guard
pass.
The full SP-01–SP-10 goal remains active: RPC stream transfers, upgrades, previews,
full loader/bindings/lifetimes/fault qualification and the other planned provider
profiles remain incomplete. Live E2B/Vercel qualification stays deferred.

Intermediate existing SSE runs exceeded Node's unchanged 250 ms handler budget
and failed embedded V8 dispatch/lifetime cleanup. Native EOF completion fixed a
separately reproduced container-monitor body hang. The original SSE/V8 failures
remain unproven; V8 now distinguishes execution termination at those call sites,
and its diagnostic rerun passed at the unchanged 100 ms pump budget. No deadline
or assertion was relaxed and no timing fix is claimed.

The branch was merged with main `0c23ae0b` after its SQLite and Node transport
hardening landed. The sole conflict combined test-host configuration helpers;
default test policies remain unchanged. Post-merge validation passes all seven
selected Node/V8 HTTP/WS, Worker-module and authority tests, native EOF proof,
and both pinned-image scenarios (SDK HTTP streams plus portable files). Both
image scenarios drain their native/application work; no owned containers remain.
Format/layout, documentation and the native-only dependency guard pass again.

### Returned RPC readable byte streams

The optional Node Workers profile now transfers returned readable byte streams,
including nested/cyclic result graphs with shared aliases. It uses the existing
native lease, bounded producer mailbox and original event/facet authority; each
read and EOF reply still waits for that producer's storage output proof. Stream
arguments, writable streams and Request/Response RPC transfer remain required.
The internal publication cuts over together with its reader, retaining marker 1.

| Boundary | Implementation / evidence |
| --- | --- |
| Transfer ownership | Acquire a BYOB reader without pulling; the sender's source remains locked after transfer. Object/locked streams refuse, and failed graph/export admission restores untouched readers. |
| Native authority | Distinct empty markers and exact known kind counts are validated in Rust; persisted value bytes cannot mint a live stream. Imported handles retain the native lease and refuse later-event reads. |
| Negotiated limits | Native publication advertises the producer budget and import clamps it to the consumer budget. Pulls grant at most the smaller budget minus 128 bytes of clone/control reserve, capped at 64 KiB. Arguments account for the routed base64 envelope; native clone and bridge checks remain independent. |
| Backpressure / EOF | One bounded BYOB read per pull, with larger cumulative bodies. EOF and native release failures remain failures; a pending BYOB request receives its final zero-byte completion. |
| Cancellation / disposal | Explicit cancel joins upstream cancellation and releases the import; synchronous disposal starts the same owned cleanup. Lease loss wakes the producer poll and joins source cancellation, including a pending read. Invalid cancellation reasons are refused after cleanup without that reason. |
| Scope and lifetime | Native network tests exercise source errors, client disconnect, clean EOF with independent background jobs held, unconsumed imports, uncloneable reasons and stale readers; joined shutdown reports zero bridge bytes/invocations. |

The real pinned Sandbox 0.12.9 image also passes a direct RPC stream probe. The
Worker calls the unchanged published `stub.execStream()` method, wraps its
returned byte stream in a Response, and the network client must observe a stdout
event before a second request releases the command. Final stdout/stderr data
events and exit code 7 follow. The earlier in-object/DO-HTTP probes and original
application routes still pass, followed by SDK destroy and drained shutdown.
This is returned readable-stream evidence, not a complete RPC transfer profile.

A broader native regression repeatedly refused an independent post-disconnect
`waitUntil` storage write. Targeted tracing proved `ConcurrentMutation` with
`not_committed`: the pending response write occupied the local storage mutation
slot. Both JS and Rust admission now separate response chunk/EOF output waits
from that slot, preserving native output gates and uncertain-effect failure
classification. The original test passes, followed by all 16 Workers tests and
the full selected suite. Diagnostic logging was removed; no deadline or
assertion was relaxed.

Validation passes 56 JS-adapter library tests, 16 Workers tests, three module
tests, two Node/V8 SSE/WS tests, native response authority and Worker/DO HTTP/RPC
streaming: 79 selected native tests total. Both pinned-image scenarios pass,
as do native EOF proof, 46 JS/tool/type checks, four inventory checks and strict
JS/guest all-target Clippy. Format/layout covers 1,104 Rust sources and formatting
covers 64 owned JS/TS sources; documentation and the 466-package native-only
dependency guard pass. An earlier parallel library run hit an embedded V8
cancellation-budget failure; the serial library runs pass with unchanged budgets.
That separate timing cause remains unproven.

The complete SP-01–SP-10 goal remains active. Readable results do not complete
stream inputs, writable/message transfer, upgrades, previews, full lifetime/fault
qualification, the remaining provider profiles or release qualification. Live
E2B/Vercel acceptance stays deferred per the user's configuration instruction.

### Returned RPC writable byte streams

The optional Node profile now transfers writable results over the existing
native lease/mailbox path. A dedicated private factory owns web writers; Rust
validates the additional `writable` kind beside the existing function/readable
markers. Native authority, producer storage proof, live-handle quotas and the
negotiated producer/consumer budget remain shared with returned functions.

| Boundary | Implementation / evidence |
| --- | --- |
| Ownership / graph | The original sender stays locked. Nested aliases resolve to one imported stream. Failed graph/encoding/export admission restores an untouched writer; value-only persisted bytes cannot recreate it. |
| Bounded frames | Caller writes accept Uint8Array, including empty chunks. Larger values split into `min(64 KiB, floor((budget - 128) * 3 / 4) - 64)` bytes, reserving routed base64 and clone/control fields. Each source also validates its own chunk limit. |
| Backpressure / proof | Each frame waits for the actual sink and then its original native producer's storage proof. Close waits for the sink close; neither is acknowledged optimistically. |
| Partial outcomes | A later frame's proven pre-effect refusal cannot establish rollback of a larger write with accepted frames. The composite failure becomes `Ambiguous/unknown`, preserving the original failure as its cause. No data frame is replayed. |
| Abort / dispose | Explicit abort and native lease loss join the standard source-writer abort. Accepted in-flight writes can finish before abort; cancellation cannot prove rollback. Synchronous disposal starts the same owned cleanup, including for a locked imported writer. |
| Failure / retirement | Sink write/close/abort failures remain failures, without a successful close. Imports drop on caller completion; unconsumed native writers abort. Retained writers refuse SDK work under a later event. |

The new native `workers_writable` fixture sends 4 KiB through a 1 KiB message
budget, holding write and close acknowledgements behind independent requests.
It verifies exact persisted binary bytes, retained background work after close,
a failed write's accepted prefix, abort reasons, uncloneable reasons, abandoned
imports and stale-writer refusal. An actual identity-transform pair crosses one
mixed RPC result; readable/writable traffic progresses concurrently, writable
aliases stay identical, and both sides drain. Native shutdown reports zero
bridge reservations and active invocations.

Validation passes all 80 selected native JS-adapter tests (56 library, 16 Workers,
three module, two SSE/WS, authority, readable streaming and writable streaming),
56 JS/tool/type checks, four pinned-input inventory tests and strict JS/guest
all-target Clippy. Both pinned Sandbox image scenarios still pass as regressions;
these remain SDK basic/readable-stream acceptance, not SDK upload acceptance.
Rust format/layout covers 1,105 sources, formatting covers 67 owned JS/TS sources,
and documentation plus the 466-package native-only dependency guard pass.

The complete goal remains active. Stream arguments still need a bidirectional
native request session; the peer request tunnel explicitly refuses leased
arguments. The pinned SDK's HTTP file client also explicitly refuses
`writeFileStream`, which needs its separate RPC/Cap'n Web upload transport.
Idle remote-loss notification and complete disconnect/cross-event semantics,
message transfers, upgrades, previews and full fault qualification remain open,
along with the remaining provider and release requirements. Live E2B/Vercel
qualification stays deferred; writable results do not narrow the SP-01–SP-10 goal.

### RPC byte-stream arguments and caller-owned sessions

Node namespace methods and returned RPC functions now accept readable byte
sources and writable byte sinks. A private session uses four returned native
controls instead of requiring the caller to expose a routable object. The
original caller owns source/sink work; the original callee owns its input
proxies and method execution. This extends the existing native lease/mailbox
framework without introducing a JavaScript engine into native-only hosts.

| Boundary | Implementation / evidence |
| --- | --- |
| Admission | Rust validates bounded clone graphs, argument arrays, distinct stream-only markers and exact counts. Invocation reservations count inputs plus four controls against handle/pending quotas. Local graph/encoding/admission failures restore untouched source locks. Callback arguments remain refused. |
| Identity / authority | Native handles retain original object, epoch and invocation checks. Stream inputs work from a stateless Worker, through another DO, and through returned functions. Serialized markers alone grant no authority. The raw native request tunnel still refuses leased arguments; this higher-level session uses actual returned native leases. |
| Flow / bounds | One source read or sink write progresses under consumer demand. Payload grants account for both base64 layers in completion submissions and the smaller host budget. A larger transfer does not become one buffered request. |
| Output proof | Native delegated invocation proves the original cell caller's storage writes before forwarding. Stateless Workers have no local SQLite capability. Callee replies retain the existing producer proof. |
| Lifetime | A returned stream/function may capture an input while the original caller event lives. Private control waits consume native budgets but do not hold the caller alive forever. After public/background jobs drain, cleanup cancels the waiter, closes proxies, joins the pump and all answers admitted by it, then releases native reservations. |
| Failure | Bounded source/sink errors pass native validation and stack stripping; accepted prefixes remain observable. Failed data acknowledgement is an unknown composite outcome; no frame is replayed. Cleanup racing an observed source failure cannot replace it with a later expired-command refusal. Cancellation can overtake and join a pending read. |

The native `workers_inputs` fixture observes a first upload chunk before an
independent HTTP request releases the source, and separately holds an actual
sink write. It transfers 4 KiB through a 2 KiB message budget, checks exact bytes
and alias identity, forwards through another DO, invokes a returned function
with a stream, reads an input captured by a returned function/stream, verifies
cancel reasons, preserved source-error prefixes and stripped producer stacks,
unused-input cleanup, and
pre-effect quota/encoding refusal. Shutdown must drain application work and all
native bridge reservations. Six bounded protocol-peer tests additionally cover
nested framing, backpressure, cancellation/cleanup races and failed
acknowledgement without replay; that peer is not native authority evidence.

The first native run exposed a root Worker storage-proof request; proof now
belongs to the native cell-aware operation. A subsequent bounded run exposed the
second base64 layer, and another exposed a source-error/cleanup race. Those
failures were corrected without increasing the test's message budget or
weakening assertions. New session tests use their own explicit handshake budget;
existing tests keep their original deadlines.

Validation passes 82 selected native adapter tests (57 library, 16 Workers,
three module, two Node/V8 SSE/WS, authority, readable responses/results, writable
results and stream arguments), 62 JS/tool/type checks, four pinned-input tests,
strict affected-adapter all-target Clippy, documentation and the 466-package
native-only dependency guard. Formatting/layout checks cover 1,107 Rust sources;
formatting covers 70 owned JS/TS sources. One broader run hit the previously
observed V8 shutdown lifetime-cancellation budget failure while Clippy compiled.
The unchanged isolated test and final serial suite pass at the same budgets;
the cause remains unproven, without a timing-fix claim.

Both pinned-image scenarios pass on the final source and leave no owned
containers. One intervening SDK image run failed at `/__stream` with an
unavailable HTTP endpoint after earlier commands succeeded; portable files and
joined cleanup still passed. VM kernel logs showed no new OOM. The unchanged
image assertions pass on rerun, but the connectivity cause remains unproven.
The harness now captures bounded state/recent logs of verified owning-host
containers before explicit SDK destroy on failure; it never dumps environment
configuration or inspects other hosts. No image assertion, deadline or native
mutation retry rule was changed.

This does not qualify the SDK's separate Cap'n Web upload transport. Published
Sandbox SDK upload acceptance, Request/Response RPC transfers, upgraded routing,
full abrupt-disconnect/remote-loss and cross-event fault semantics, previews,
remaining provider profiles and release qualification are still required by the
full SP-01–SP-10 goal. Live E2B/Vercel qualification remains deferred until the
user supplies configurations; the complete goal stays active.

### Container WebSockets and published SDK RPC uploads

The optional Node profile supports native container-port upgrades and local
`WebSocketPair` endpoints used by the published Containers proxy. The pinned
Sandbox SDK now uploads readable byte sources with its actual Cap'n Web
transport when the declared host variable `SANDBOX_TRANSPORT` is `rpc`. This
uses the same selected provider and native cell authority as portable APIs;
neither the SDK nor its control server is rewritten.

| Contract | Implementation / acceptance |
| --- | --- |
| Upgrade | Bodyless GET; actual status/protocol/network handshake headers; declined upgrades remain HTTP responses. Rust/Python/JS/TS portable facades expose the headers. |
| Ownership | Original-event pumps own native socket I/O. Later events of the same activation enqueue after their own storage output proof. Foreign or retired activations refuse. |
| Flow / bounds | Text and copied binary frames; real EventTarget/MessageEvent delivery; at most 64 queued entries and two frame budgets of bytes per queue, plus native handle, operation and retained-work quotas. |
| Failure / cleanup | Peer close, receive cancellation and event drainage wake and join pumps. Unaccepted upgrades release. Post-upgrade representation/admission failure remains an unknown outcome and schedules release. Accepted frames are never replayed. |
| Real SDK upload | 2 MiB + 17 binary bytes through the published SDK and pinned image; observe a real temporary-file prefix before releasing the source, then require SDK success/byte count, exact final bytes, explicit destroy and drained native bridge resources. |

Actual Cap'n Web initialization exposed overlapping storage proofs being refused
as `ConcurrentMutation/not_committed`. `storage.sync` is an output barrier,
not a SQLite mutation; Node and V8 admission now let proofs overlap while still
refusing a second real mutation. A regression holds an actual admitted write,
admits two proofs and refuses another write before completing the first write.
Native durability/output gates and uncertain-effect classification remain intact.

The first image attempts also exposed missing local-pair support and an incorrect
test assumption about when the final file appears. Inspection of the pinned
server showed atomic `.tmp.<uuid>` writing and rename at EOF. The test observes
that real temporary file through a separate SDK execution session: commands in
the writer's own session serialize behind the upload. It still requires bytes
before source release and an unpublished destination, without relaxing existing
assertions/deadlines or adding mutation retries. Temporary tracing was removed.

Worker/DO upgrade forwarding, `Request`/`Response` RPC transfer, hibernation,
complete idle/reconnect/disconnect semantics, sleep/wake/hooks, previews and
remaining SP-01–SP-10 requirements stay open. The facade does not supply the
global network WebSocket constructor. Full SDK conformance remains `not_run`;
this evidence qualifies the selected operation and pinned image only.

Validation after merging `main` at `d3206c92`: 91 selected Node/V8 native
library/integration tests, all three pinned-image scenarios, 21 provider unit
tests, the CPython container fixture and native WebSocket opening test pass.
The 69 JS/tool/type checks, four pinned-input checks, strict affected-adapter
all-target Clippy and native-host library Clippy pass. Formatting/layout covers
1,150 Rust sources; JS formatting covers 76 owned sources. Documentation and
the 466-package native-only dependency guard pass. The merge also passed all
six peer-tunnel scope/ownership tests. Earlier intermittent budget/connectivity
causes remain recorded above; this delivery does not claim to fix them.

### Returned HTTP responses and published SDK binary downloads

DO RPC results can contain real `Response` objects. Native marker validation
checks bounded status, headers, URL and body metadata alongside a live stream
lease; serialized markers cannot recreate capabilities. Body chunks retain the
original producer event and storage output proof. Bodyless/error responses
release their lease immediately, aliases share one response, and cancellation or
unused-result disposal joins upstream cleanup. Used bodies, opaque responses
and WebSocket upgrades refuse this transfer.

| Delivery | Evidence / limit |
| --- | --- |
| HTTP response result | Native fixture preserves status/text, separate cookies, null/error bodies and aliases; a 3 KiB source crosses a 2 KiB bridge budget before source release |
| Failure and cancellation | Preserve a prefix before source error; client disconnect and unconsumed-body disposal cancel the producer; shutdown leaves no retained bridge work |
| Default byte sources | Returned streams may emit bounded `Uint8Array` chunks through a standard reader; split into negotiated native frames, skip empty chunks, reject objects/oversized chunks and cancel upstream |
| Input admission | Generic stream arguments still require a byte source before calling the target; no broad object-stream input support was added |
| Published SDK download | Unchanged `readFile(path, { encoding: "none" })` returns its file-result object/content stream; every downloaded byte matches the real 2 MiB + 17-byte SDK upload |
| Still open | Request transfer and HTTP-message arguments, full cross-event/remote-loss qualification, upgraded routing, previews and the remaining SP profiles |

The published SDK returns a file-result object, not a `Response`. Its Cap'n Web
pipe uses a standard `TransformStream`, whose Node readable does not supply a
BYOB reader. Returned streams therefore validate each default-source chunk at
the existing native boundary. No SDK source or package resolution was replaced.
The first native response fixture also exposed a source continuing after
cancellation; the fixture now stops that continuation without weakening its
cancellation-state assertion. An empty-chunk regression exposed a stalled byte
read; the receiver now continues demand under the same native operation budget.

The broader regression run exposed a distinct WebSocket admission race: an
echo can invoke a storage-writing message handler while the original send
acknowledgement is pending. Socket output was incorrectly occupying the local
storage mutation slot. Text/binary sends and close now share the independent
output-operation admission rule; native effect classification, output proofs
and provider ownership remain unchanged. A Node/V8 regression holds all three
output operations, admits a storage write and still refuses another overlapping
storage mutation. The native socket fixture retains its storage/order assertions.

Validation on the feature revision based on `d3206c92`: all three pinned-image
scenarios, 74 JS/tool/type tests, four input-pin tests, strict adapter all-target
Clippy, format/layout (1,152 Rust sources; 79 JS/TS sources), docs and the
466-package native dependency guard pass. The new response fixture and the
socket fixture both pass again in isolation without a compiler running.

Broader native qualification is **not green**. The final serial runs passed 64
library tests, two container tests and 19 other integration tests, with nine
failures across Node compatibility, SSE/WebSockets, stream inputs, responses,
module startup/alarms and HTTP streaming. Failures included execution/handler
budgets, startup/input deadlines, an alarm HTTP 500 and drainage. An earlier V8
shutdown-budget failure passed unchanged in isolation. Concurrent qualification
work and substantial disk traffic were observed, but causality is unproven;
passing isolated reruns do not close these failures. No deadline or assertion was
relaxed. Reproduction and resolution remain a qualification gate before full
profile or release claims.

The follow-up merge of `main` at `ce687427` preserves both sandbox bundle ignores
and binding-example build ignores, plus both the feature-gated Python Vercel test
and binding-example binary. The new JS/TS binding examples build, Cargo metadata
validates both Python targets, adapter all-target Clippy passes, and format/layout
(1,156 Rust sources), docs (808 local references) and the native dependency guard
pass. This merge adds examples/manifests; it does not resolve the runtime failures
recorded above.

### Qualification follow-up after PR #117

The follow-up starts from merged `main` at `80026d16`. A deterministic V8
regression now closes the host during an unacknowledged synchronous native
mutation, catches cancellation in the guest and continues running. Before the
fix, requested interruption returned an execution-budget error. The runtime now
retains the first stop cause and treats only a typed V8 interruption caused by
an explicit host stop as joined shutdown. A timeout or protocol failure cannot
be relabelled by a later host return. Real runaway/startup budget tests still
fail as intended, and the new regression passes in Node and V8 with zero bridge
reservations. No mutation is replayed and no budget is increased.

Stream-input, response, writable and WebSocket fixtures now preserve their
original exercise error together with bridge usage and shutdown evidence;
a drainage error can no longer replace the initial request failure.

All 95 selected native library/integration tests pass serially at the original
budgets after this change. The preserved diagnostic runs also reproduced a cold
call timing out before its cell handler entered, while lease renewal succeeded
promptly. That delay's cause remains unproven. Disk-waiting build processes and
other qualification work were observed; a passing campaign does not establish
the cause of those earlier delays or complete SP-10 release/fault qualification.


### RPC Request results and HTTP-message inputs

The optional Node profile now transfers real Requests in results and accepts
Request/Response arguments through caller-owned native input sessions. Shared
HTTP metadata validation runs before target dispatch; unsupported heads or
already used/locked/aborted sources are refused without a target mutation.
Body streams retain their producer event, native limits and output proof.

| Delivered behavior | Evidence |
| --- | --- |
| Request head/body and shared aliases; Response arguments | Native gated 32 KiB + 17 binary upload through an 8 KiB bridge limit |
| Live signal after EOF | Both Request arguments and returned Requests wait for later aborts |
| Later-event proof | Protocol test holds the aborting caller's proof; source cancellation remains in the original context. Native returned Request observes the later caller's committed write |
| Abort exception | Custom DOMException name/message survive native cloning |
| Unused Request | Owned cleanup closes an idle signal waiter and body without retaining the event |
| Native refusal | Request metadata tests reject unsupported methods, URLs, mode/cache combinations, headers, body combinations and extension fields before routing |

The complete selected campaign passes 97 native library/integration cases, 84
JS/tool/type checks and all three pinned Sandbox image scenarios. Strict adapter
all-target Clippy also passes. This includes the shutdown-cause fix above at the
original existing budgets. Full remote-loss/disconnect profiles, embedded web
globals, Workers-specific HTTP extensions and SP-10 release qualification remain
open. Default-reader chunks must fit the producer body limit; aggregate streams
may be larger. EOF does not release a Request's signal lease; disposal and event
cleanup do.


### Published idle lifecycle and local HTTP previews

The published Containers 0.3.7 alarm loop now has a native sleep/wake fixture.
It observes readiness, persisted start/stop/expiry hook counts, exactly one
SIGTERM per idle cycle, alarm removal while stopped and joined resource cleanup.
Four cycles pass across recreation of both the language runtime and native host
on the same SQLite state.
Its deterministic engine peer models a process which exits cleanly on SIGTERM;
this does not substitute for image-specific shutdown qualification.

The separate Sandbox 0.12.9 image scenario calls unchanged `startProcess`, the
returned process's `waitForPort`, `exposePort`, `proxyToSandbox`, `getExposedPorts`,
`unexposePort` and `destroy`.

| Boundary | Image evidence |
| --- | --- |
| HTTP forwarding | Method, path/query and exact binary body reach the application port |
| Routing headers | Forged internal preview headers are replaced and stripped before reaching the service |
| Invalid authorization | A different same-length token returns 404 |
| Stopped runtime | Retained authorization returns 410; preview traffic does not create a container |
| Replaced runtime | Waking the sandbox does not silently reactivate an old exposure |
| Explicit re-exposure | Retains the durable URL/token and activates the new runtime |
| Revocation | `unexposePort` makes the previously valid URL return 404 |
| Teardown | SDK destroy and native shutdown leave zero owned containers and bridge reservations |

This delivery uncovered and fixes two missing Node Workers surfaces. Cancellation
of the SDK's process-readiness stream leaves a floating `pipeTo().finally()`
rejection; the admitted profile now emits Workers rejection events and reports
uncancelled notifications. Late handling runs in the original promise scope.
Ordinary Node applications and bootstrap failures retain fatal behavior. The
SDK's token comparison now uses `crypto.subtle.timingSafeEqual`, backed by Node's
native constant-time implementation.

The pinned image exposes only control port 3000. Deployment `exposed_ports`
adds at most 64 distinct TCP declarations without rebuilding that image. On
non-Linux hosts those additional ports publish on loopback using daemon-assigned
host ports. Package manifests require `container-ports-v1`, and invalid ports
refuse before container admission. Image EXPOSE metadata remains available.

#### Resolved callback/alarm qualification failure

An extra harness barrier after monitor completion exposed an overlapping native
callback/alarm gap. The following stop probe reached the empty block, then a
previous alarm attempted synchronous `storage.apply` while another callback's
input gate remained held. Blocking the language thread prevented that callback
from completing, and Node's original two-second event-loop watchdog fired:

```js
const monitor = this.monitor;
await this.ctx.container.destroy();
await monitor;
await this.ctx.blockConcurrencyWhile(async () => {});
await this.syncPendingStoppedEvents();
```

`getAlarm`, `setAlarm` and `deleteAlarm` are asynchronous public APIs. They now
await the asynchronous native bridge instead of blocking on its synchronous
reply channel. The pending alarm yields while native admission is held, allowing
the callback to finish. The bridge still executes the original native operation
with its context, output proof, ownership checks and budgets.

The owned alarm regression fails before this change and passes after it. The
original extra-barrier probe is restored in the pinned-image fixture and passes
at the same deadlines, including zero retained bridge resources. This resolves
the reproduced alarm deadlock; it does not qualify every continuation or permit
queued container controls inside transaction/block callbacks. Those controls,
upgraded routing and complete Workers conformance remain open.


### Merged-main delivery verification

The delivery branch incorporates `main` at `73dab42b`. Its V8 startup/admission
changes and typed shutdown interruption remain intact. A new regression proves
that an earlier runtime failure cannot be relabelled by later host shutdown.
The ordinary Node floating-rejection fatality regression also passes.

| Check | Result |
| --- | --- |
| Selected Node/V8 adapter library and native integrations | 104 passed |
| Rust core library, native containers and native image packaging | 643 + 2 + 1 passed |
| Exact Sandbox 0.12.9 image scenarios | All 4 passed: portable files, published routes/streams, RPC binary files, HTTP previews |
| JS/tool/type checks | 88 passed, including held native admission for asynchronous alarm controls |
| SDK/reference/image input pin checks | 4 passed |
| Strict Clippy | Affected adapter all-target and core/container/package targets pass |
| Native-only dependency graph | 466 reachable packages; no JavaScript engine |

This table records the complete campaign before the async-alarm follow-up. The
follow-up passes all 88 JS checks, all four exact-image scenarios, the 16-test
Workers target and three of four module tests. Independent Node-only and
V8-only strict Clippy, Rust format/layout, JS format and docs checks also pass.

The repeat campaign does **not** pass the complete module target: its first
`/alice/count` request intermittently exceeds the existing 250 ms handler budget.
It also fails with the prior alarm implementation restored. Temporary boundary
timing observes a much slower first native object admission than the restarted
case, but does not establish its cause. No deadline or assertion was changed,
and the passing instrumented run is not a resolution. Initial V8 authority-start
and first-request timeouts pass in isolation and on the 16-test repeat; their
intermittency likewise remains part of broader qualification. Temporary probes
are removed. These selected checks do not establish complete SP-10 qualification.

E2B/Vercel real-account qualification still awaits configuration; durable provider
reconnect/expiry/orphan delivery and separately qualified 1.x/additional-provider
profiles remain outstanding.

### Worker/DO upgrades and published WebSocket previews

The Node module dispatcher can return an unaccepted container socket or local
pair endpoint, and forward an unchanged namespace upgrade through later Worker/DO
hops. Private transfer capabilities bind to the canonical opening event, so
same-event facade aliases work without granting a later event authority. Accepted,
synthetic and already-transferred endpoints refuse; invalid heads close only
their own untransferred endpoint.

| Boundary | Delivery and evidence |
| --- | --- |
| Native server | Explicit `accept_polled` retains frame delivery and native I/O cleanup in the opening event even after publishing the upgrade; later events cannot borrow it or turn it into a hibernating socket. Ordinary invocation-delivered and hibernating sockets keep their existing modes |
| Namespace routing | Forward the existing native upgrade without another language proxy; edited response headers pass native bounds and survive the final handshake |
| Frame flow | Two original-event pumps await each send, preserve binary/text kinds and keep existing frame, queue, operation and retained-work bounds |
| Disconnect | A cancellable native close observer does not take the frame-reader lock or consume queued frames. It wakes a bridge while local delivery is held and preserves terminal metadata across retirement |
| Native network fixture | Worker and DO pairs, container forwarding, namespace acceptance, storage callbacks, headers, clean/abrupt disconnect, an unaccepted local receiver and zero-resource joined shutdown |
| Published preview image | The unchanged 0.12.9 `proxyToSandbox` helper forwards text/binary WebSockets, strips forged routing headers and closes cleanly or abruptly before/after explicit re-exposure |

The image keeps its exact digest and dependencies. Only the probe service changes
to use HTTP/WebSockets from Bun 1.3.12 already included in that image; no rewritten
Sandbox SDK or replacement control server supplies the result.

Two regressions fail before their fixes: returning a valid upgrade through the
module adapter, and disconnecting while application acceptance holds delivery.
The latter requires observing native closure independently of the busy frame
pump. Native queue tests also prove the observer does not consume the buffered
prefix, wakes despite a held reader/full queue and retains the close code/reason.

The fresh `main` baseline and feature campaign both pass the module target at
the original 250 ms budget. Three separate cold-first-call probes also pass.
This does not establish a cause or a fix for the earlier intermittent latency;
no assertion or deadline was weakened.

The branch incorporates `main` at `185c397a`, including V8 completion reservations
and isolated Linux engine-peer ports. The new native ownership probe additionally
keeps an upgrade live while a later event attempts send/read/close/accept, requires
every attempt to refuse, then ends the opening event without a JS close and
observes native transport cleanup. Polled servers also refuse hibernation.

| Combined-source verification | Result |
| --- | --- |
| Adapter library / Workers target | 76 + 16 passed |
| Streaming, HTTP messages, module, port and upgrade targets | 11 passed, including all 5 module tests at existing budgets |
| Native close-queue tests / existing native WebSocket integration targets | 3 + 4 passed; the integration targets each execute their test without a name filter |
| Exact Sandbox 0.12.9 image scenarios | All 4 passed, including WebSocket previews and RPC binary files |
| JS/tool/type checks / reference input pins | 95 + 4 passed |
| Strict Clippy | Adapter all-target and affected core/library/WebSocket targets pass |
| Repository checks | Rust/JS formatting, 1,184-file layout, docs and native-only dependency guard pass; 466 native packages, no JS engine |

Complete slow-peer/overflow, cross-node, ownership-loss, hibernation and versioned
Workers/SDK conformance remain open. Callback-owned queued controls and remaining
hooks are the next local compatibility work; named durable provider bindings,
reconnect/expiry/orphan cleanup and separate Sandbox 1.x/additional providers
remain subsequent delivery milestones.

### Concurrency-block controls and published error/exit hooks

Queued start/signal/destroy operations now run in native concurrency callbacks.
The JS bridge retains their complete promises before submitting callback finish,
including destroy's monitor-drain wrapper. Native callback checks remain active
for each poll. A private continuation-token query selects the original callback;
it supplies no native authority of its own.

| Boundary | Selected evidence |
| --- | --- |
| Void start | Hold the real engine startup acknowledgement; another native cell invocation remains outside the input gate until the accepted control completes |
| Nested blocks | Signal and awaited destroy execute under their original callbacks; destroy acknowledges cleanup before the next state write |
| Transaction inheritance | Both direct transactions and blocks nested in them refuse admission before effects; exact engine signal counts exclude refused calls |
| Returned continuation | Resume an original-token continuation while startup is held and the callback body has returned; new control admission refuses with `ContextExpired` / `not_committed` |
| Published `onStart` | The unchanged 0.3.7 package invokes the subclass hook in a native block; its signal reaches the engine once per successful start |
| Published `onError` | Readiness failure invokes the real hook; a hook throw does not replace the original failure |
| Published `onStop` | Explicit destroy reports the package's normal stop parameters; a later nonzero run reports exit code 137 / reason `exit`, with hook state persisted in native DO storage |
| Constructor/idle alarm | Preserve the numeric-alarm assertion at constructor admission, then run the published idle/no-schedule handler, require the alarm to clear, and admit another read |

The Node/V8 constructor/queued-control target exercises fresh and recreated
runtimes against the engine peer. Hook HTTP/readiness assertions require Node's
web runtime. These selected paths do not complete SP-03: callback failure and
cancellation, complete timeout/hook behavior, image faults and full versioned
conformance remain open. The 250 ms first-request timeout and an alarm-start
failure reproduced in the combined native campaign; a fresh full module run
then passes unchanged. A passing timing probe measured about 135 ms between
the Worker's native routed call and entry into the first DO's JS handler, versus
about 10 ms for later cell admissions. This narrows the investigation boundary
without establishing a cause or a fix; the temporary timing probes were removed.
Named provider bindings and supervised expiry/orphan cleanup remain feature
milestones; the explicit reconciliation slice below advances event-driven
reconnect and cleanup observation. Live E2B/Vercel tests still await configuration.

| Callback/hook campaign | Result |
| --- | --- |
| Adapter library / Workers target | 76 + 16 passed; Node/V8 callback controls run before and after runtime recreation |
| HTTP messages / fresh module and streaming/upgrade/socket targets | 2 + 8 passed in completed targeted suites; all five module cases pass on the fresh full run |
| Exact Sandbox 0.12.9 image | All four scenarios pass, including previews and RPC binary files |
| JS/tool/type checks / input pins | 95 + 4 passed |
| Static checks | Strict adapter all-target and core-library Clippy, Rust/JS formatting, layout, docs and native-only dependency guard pass |
| Cold dispatch repetitions | One of three further default probes times out. An alternating eight-run-per-configuration comparison reproduces the timeout with both default and eager Node compilation; no flag change is justified or applied |

Timing budgets remain unchanged. The initial combined module campaign has two
failures; the later passing full run does not erase
that unresolved intermittent qualification failure.

A later repetition also exposed a fixture assertion that incorrectly required
the constructor alarm on every subsequent request. The published SDK can clear
it when stopped without schedules, and alarm delivery may temporarily consume
the timestamp. The constructor assertion now runs at admission; a separate
published idle-handler scenario requires a cleared alarm and a successful later
request. This corrects the fixture's lifecycle invariant without changing the
SDK, native alarm behavior or deadlines.

### Explicit provider reconciliation and observed retirement

The portable `container.reconcile` operation gives Rust, Python, JavaScript and
TypeScript an event-owned recovery boundary. It returns only a bounded lifecycle
state and nullable decimal generation; provider IDs and credentials stay native.
It can checkpoint verified evidence but sends no create, resume or delete request.
Because journal writes are possible, the shared session serializes it as a
control under the native mutation/output gate.

| Boundary | Delivered behavior / local evidence |
| --- | --- |
| No reservation | Return `unbound` without calling the provider |
| Lost create reply | Recover the original E2B resource or exact Vercel session after host restart; retain generation 1 and issue exactly one create |
| Unresolved/foreign evidence | Hidden results, metadata mismatch and changed Vercel sessions refuse; no replacement, deletion or command replay |
| Fixed expiry | An exact-resource absent observation retires a confirmed generation; the next explicit start advances its generation |
| Pending deletion | A successful acknowledgement while the resource remains visible retains pending intent; reconciliation returns `deleting`, including after restart |
| Lost delete reply | Observed absence confirms retirement without another DELETE |
| Native engine | Report running/stopped with no remote-journal generation; preserve native engine authority |
| Language facades | Frozen Python/JS values and TS readonly types; saved facades cannot borrow a later event's authority |

E2B destruction now checks absence before confirming retirement, matching the
existing Vercel cleanup boundary. Protocol fixtures exercise provider expiry and
delayed removal; they do not execute cloud VMs or prove live provider behavior.

The serial module campaign also exposed an invalid zero-byte assertion while
the Node engine/transport was still live: zero active invocations coexisted with
90 retained bridge bytes. An isolated unchanged rerun passed. The lifecycle
fixture now retains its native-invocation drain assertion, then requires stopped
admission, zero invocations and zero retained bytes **after NodeRuntime joins**.
Live transport reservations are part of `BridgeUsage`; a native host drain alone
does not end that transport. No delay, relaxed deadline or byte allowance was
added. All five module cases pass with the joined assertion.

Named guest binding selection, supervised orphan sweeps, ownership-transfer and
late-completion faults remain open. The cold 250 ms dispatch failure is still
unresolved; serial module passes do not erase the earlier cold-process failures.
SP-02–SP-06 remain partial, Sandbox 1.x/additional providers remain separate
profiles, and E2B/Vercel live-account testing still awaits configuration.

| Reconciliation campaign (macOS host) | Result |
| --- | --- |
| Shared provider library / native integrations | 22 + 4 passed, including paused/stopped state, unknown-state refusal and three native-host rounds |
| JS runtime library / selected integrations | 76 + 11 passed across Node 24.13.1 and embedded V8; all five module cases pass with the joined cleanup check |
| CPython 3.14.6 | Both provider fixtures and the native-container fixture pass |
| JS/tool/type checks | 95 passed with Node 24.13.1 |
| Strict Clippy | All targets in cellar-container, cellar-guest, cellar-javascript (Node/V8) and cellar-python (embed) pass |
| Format / layout / docs / native graph | 1,188 Rust files, 94 JS sources, documentation references and the 466-package native-only guard pass |

The initial machine-default Homebrew Node 26.5.0 run failed two package-copy
launch tests. Both pass under Node 24.13.1; this campaign does not qualify the
Homebrew Node 26 packaging path. Exact Sandbox image tests were not rerun for
this portable API slice. Earlier native/image evidence retains its own limits.

### Named provider selection and discovery

Hosts can declare additional provider slots with `Source::container_binding`
or the shared Rust `Bindings` registry. Python/JavaScript/TypeScript select them
with `ctx.containers.get(name)` and discover names/capabilities using
`ctx.containers.list()`. The existing default facade and Workers controls keep
the `container` slot. No vendor SDK, ambient credentials or native-engine alias
is introduced by selection.

| Boundary | Delivered behavior |
| --- | --- |
| Admission | Exact provider binding identity, unique names and no default/named journal collision; startup rechecks later default overrides |
| Bounds | 63 additional slots, 128-byte ASCII names, per-slot resource limits and bounded discovery output |
| Discovery | Default first, then names in sorted order; credentials and remote resource IDs remain native; no provider I/O |
| Selection | Unknown/invalid slots refuse before effects; no fallback to a default or another provider |
| Authority | Original root-event checks remain in every session; a saved named facade/session cannot borrow a later event |
| Handles | Processes, sockets, WebSockets and HTTP response readers retain their selected slot on follow-up and release calls |
| Durable resources | Separate E2B/Vercel journal names and generations; destroying the default resource preserves the named backup |

The native Rust protocol campaigns exercise a named resource and stale-session
refusal. The language fixtures use two real protocol-peer resources with the
same file path and different bytes, then read them in a later event and retire
them independently. A shared-runner unit exercises every process/socket/response
follow-up against its selected slot. These are local protocol/runtime tests;
they do not qualify live VMs, remote fencing or background orphan cleanup.

The cold 250 ms admission timeout and remaining callback faults remain open.
Host-supervised cleanup, ownership-transfer failure evidence and separately
versioned Sandbox 1.x/additional-provider profiles remain subsequent deliveries.

| Named-binding campaign | Result |
| --- | --- |
| Shared provider library / native integrations | 24 + 4 passed; includes configuration bounds, named native-Rust execution and later-event refusal |
| JS runtime library / selected integrations | 77 + 11 passed, including Node/V8 two-resource peers, all child-handle selectors and unchanged default Cloudflare module routes |
| CPython 3.14.6 | E2B/Vercel two-resource fixtures and the native-container fixture pass |
| JS/tool/type checks | 95 passed with Node 24.13.1 |
| Strict Clippy / static checks | All targets in the four affected crates, 1,190-file Rust format/layout, 94 JS sources, documentation references and the 466-package native-only guard pass |

The extra-slot language fixtures use local protocol peers, not configured cloud
accounts. Exact Sandbox image scenarios were not rerun for this shared-framework
slice. The earlier cold-process and Homebrew Node 26 packaging failures remain
open; this campaign does not broaden those qualification claims.
