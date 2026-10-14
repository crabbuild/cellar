# Outbox implementation status

Updated 2026-10-13. The goal remains the entire
[outbox, stream, and subscription design](002-outbox-streams-subscriptions.md).
The feature is under implementation and is not ready for normal deployment or
release. The native fixture now exercises source, catalog, stream, and consumer
turns together; that local evidence does not satisfy the fleet/release gates.

## Current code

- `Storage::tx` delegates to the existing synchronous savepoint machinery.
- `Storage::outbox(&OutboxOptions)` exposes bounded source diagnostics through
  the owning activation: total backlog, per-lane rows/bytes, destination identity,
  retry deadline/attempts, retained error and seal state. Pages contain at most
  100 lanes and never copy event bodies or full bindings. Continuations bind the
  source scope and incarnation; pages observe current state rather than a frozen
  snapshot. Empty inspection creates no source/schema/wake state.
- `Storage::retry_outbox_lane(&OutboxRevision)` requires the observed source
  incarnation, dispatch generation, next sequence and settled watermark. The
  pure core rejects a changed revision or exhausted generation. A successful
  retry advances the generation to fence an older in-flight reply and commits
  wake work atomically, without changing event bytes or skipping sequences.
  The existing bulk SDK retry now advances pending-lane generations too and
  refuses exhaustion before changing any lane.
  The authenticated operator route now exposes this foundation.
- `cellar outbox inspect/retry/status/audit` reaches the source owner through
  fleet-HMAC ingress and distinct source RPC provenance. Source control methods
  run before application constructors or handlers. Event authority retains its
  prior wire shape. The original input, ownership, output and discovery gates
  cover both local and tunneled source commands.
- Source retry receipts are committed with wake work and retained under an
  independent version-1 control marker. Their operation/digest/operator/reason/
  expected-revision/result identities resolve replay and reject conflicting
  requests. The pure core owns admission/replay/capacity decisions. Status and
  bounded audit survive business reset and owner handoff; unknown operations
  return `OperationUnknown`. No receipt expiry or sequence abandonment is added.
  New operations cap at the captured receipt limit or 10,000 per source;
  exact replay remains available at capacity. Request/RPC/receipt/client-reply
  budgets are 16/64/4/1024 KiB, and revision files are read through a 4 KiB cap.
- `Context::stream`, typed `Event`/`Stream` handles, resolved resource identities,
  binding publication permissions, domain ownership, and fallible envelope
  decoding are present. `WorkerConfigOptions::stream_bindings` is propagated
  into native worker configuration; existing constructors explicitly supply
  empty bindings.
- Native `ApplicationConfig` now models typed `streams` and `stream_bindings`.
  Packages install both reserved SQLite event classes and require
  `event-delivery-v1`. Publication validation, managed ingestion and fleet
  loading reject unmarked/oversized/malformed event metadata, mismatched
  principals/scopes/grants, conflicting environment names and incompatible
  subscription limits. Parsing bounds each declaration before cloning it.
- Loaded deployments retain desired definitions separately from resolved
  application bindings. Generation construction repeats validation under its
  captured host limits, checks graph-wide resource/name uniqueness, installs
  the built-in event classes for programmatic hosts, and retains definitions
  with their declaring script through `Generation::event_streams`. This data
  is desired state, not an activation receipt.
- Node startup and adoption now reconcile captured definitions through the
  authenticated owner route. Stable deployment operations replay safely;
  readiness also verifies the catalog's current lifecycle, accepted revision
  and definition digest, rather than trusting historic configuration receipts.
  Every explicit binding independently resolves the partition's full resource
  identity, current grant subset and live subscription names, after an authorized
  catalog lifecycle check rejects creating/updating/retiring/deleted resources.
  This also applies to bindings with no local resource declaration. Generation-local
  readiness gates emission, reads and consumption; public startup ingress waits
  for activation, and failed adoption preserves the serving generation.
  Already committed configuration is not rolled back across resources.
- Deployed event bindings require replication and durable wake discovery,
  including on one bucket-backed node. SQLite-only startup is rejected before
  provisioning. A real Node test exposed unqualified local restart inventory
  and uncovered internal alarms during handoff; no deadline extension or drain
  assertion relaxation was used to conceal that limitation. Local-only event
  support requires a separate discovery/recovery implementation and proof.
- `Storage::emit` and the source SQLite outbox record immutable envelopes,
  persistent source incarnation, per-destination lane sequences, counters,
  and wake intent in the same transaction. Internal savepoints prevent caught
  emission errors from leaving partial state. Pending delivery prevents
  `delete_all`; an empty reset preserves protocol identity/history.
- Outbox, inbox, stream and catalog schemas all have version-1 markers.
  Activation rejects unmarked, unsupported or incomplete event layouts before
  schema writes. Current-state reopening retains obligations and identities;
  lower host limits refuse incompatible retained payloads.
- Fleet, peer and native host markers stay at 1. Fresh marker creation is
  concurrent and reopenable; unknown layouts fail closed. Development state is
  recreated on hard cutover, with all participants rebuilt. No fleet upgrade
  command or old-build compatibility qualification remains in scope.
- Event encoding enforces a byte budget during serialization, before building
  the decoded payload, and bounds final envelope encoding inside the savepoint.
  Key limits apply before copying. Admission failures expose typed transaction,
  family, permission, payload, producer-capacity and outbox-capacity outcomes;
  zero event versions are rejected before they can strand a durable obligation.
  Integer payloads outside i64/u64 are rejected instead of rounded through f64.
- Internal work has independent deadlines and persistent generations. Wake
  triggers project the minimum of application alarms and internal work,
  advance installation identity even when the minimum stays unchanged, and
  participate in commit/rollback and restore hints.
- Native alarm dispatch selects due internal work independently of application
  alarm handling. The source dispatcher sends one bounded lane batch through
  RPC after explicit discovery/data proof, retains a retry deadline, and
  fences response cleanup with a dispatch generation and echoed identity.
- Dispatch authority uses the principal/grant stored in the immutable source
  lane, independently of the current application script name or environment.
  New application emission still checks the current script and binding. The
  native fixture adopts a renamed script with no bindings, proves a previously
  committed event still appends, rejects new emission, and then restores the
  original application configuration. Destination grants remain authoritative.
- Explicit storage sync independently waits for discovery without consuming
  the response's original gates. Removing an application alarm refreshes
  discovery if another deadline remains.
- Dependency-free decisions cover contiguous append deduplication, cursor
  expiry, exact-batch acknowledgment, idempotent lease extension, retention
  protection, retry timing, and captured event API limits.
- Protocol metadata integers use decimal strings. Event bodies remain encoded
  bytes carried as base64, preserving integer payloads through the binary64
  structured codec. Hard transport batch limits apply even under unrestricted
  host API policy.

- Host-minted event RPC provenance travels through native runtime routing and
  authenticated peer tunnels. Ordinary application RPC cannot target either
  reserved event class. Receivers authorize persisted resource grants rather
  than trusting publication flags in packet arguments. Emission also checks
  the source activation's current application bindings.
- `__EventCatalog` persists pending creation/update intent, operation identity,
  canonical definition digest and retry work before contacting `__EventStream`.
  Activation becomes durable catalog state only after identity-checked stream
  acceptance. Retries preserve the operation and original authorized caller.
- The stream cell atomically stores ordered event bytes and producer high-water
  marks. Watermarks survive payload retention. Reads and fetches select bounded
  metadata before copying bytes, avoiding count-times-event-size allocations.
- Required and bounded retention restrict oldest-prefix reclamation by live
  leases and required checkpoints. Stream counters and retention floors move
  atomically. Optional subscriptions explicitly expire when they lose history.
  Dispatch persists receiver batch-capacity hints and adapts oversized batches
  without dropping, bypassing, or partly accepting an event prefix.
- Typed subscription handles resolve deployed names. Fetch request identity,
  batch receipt, checkpoint, lease generation, token, retry deadline, and last
  acknowledgment are persisted. Same-request fetch replay returns the same live
  batch; competing fetches return `LeaseBusy`. Ack/retry/absolute extension use
  exact receipts and the dependency-free policies. Logical time is persisted
  even when a deadline operation returns a typed error.
- Long polls subscribe before observing durable state, release transactions
  before awaiting, and use the execution-domain clock and cancellation. Waiter
  budgets apply both per partition and per host. Host dispatch reservations cap
  concurrency and prevent two in-flight batches for the same source lane;
  SQLite deadlines remain the recovery authority.
- `Storage::inbox(scope).accept(&record)` composes a digest receipt with business
  state and further emission in the same transaction. Matching retained
  receipts skip effects, conflicts fail, capacity rejects new identities, and
  bounded explicit pruning ends the selected deduplication guarantee. Receipts
  and accounting survive storage reset; there is no automatic expiry.

- Subscription administration now has owner-routed status, guarded pause/resume,
  explicit reset, deletion, and paginated durable audit receipts. UUID/generation
  guards and operation digests prevent delayed commands from affecting a
  successor. Reset reports skipped offsets and clears old lease/ack authority;
  deleting the last required subscription is refused until retention changes.
- Deletion retains subscription tombstones. Reconciliation can explicitly
  recreate a live name with a new ID; stale receipts and fetches cannot attach
  to it. Consumer handles retain their first resolved identity and include it
  in every fetch. Current consumer grants remain usable even when an unrelated
  administrative permission in the local binding is stale.
- Stream schema version two transactionally migrates the earlier global unique
  subscription-name constraint to live-name uniqueness, preserving identity,
  progress, and leases. The native fixture seeds an old database and verifies
  that migration, subsequent deletion/recreation, and durable audit capacity.
- `tx.seal(&stream, operation)` requires an existing fully settled source lane.
  It disables emission and records a terminal seal obligation with wake intent
  atomically. The dispatcher proves and retries that obligation through the
  same host/lane reservation and generation-fence path as append. Destination
  sealing checks immutable source/grant identity and exact terminal high water.
  Source reopen/reset retains pending seal work; old append and seal retries
  remain idempotent while new emission fails with `ProducerSealed`.
- Storage reset now preserves stream schemas, producer watermarks, subscription
  and audit metadata, and catalog tombstones as well as source/inbox state.
- Stream retirement records catalog intent before delegated closure. Coordinated
  mode checks sealed registered producers, caught-up subscriptions and no live
  leases; the named external procedure is an explicit attestation about unknown
  sources. Force records a loss acknowledgment and closes ordinary append/read/
  fetch/ack/configuration authority. Pending source rows remain blocked and
  inspectable; neither mode silently retargets or discards them.
- Terminal partition closure survives owner eviction and bounded event/metadata
  cleanup. Row and byte budgets apply to metadata too. The catalog waits for
  completed partition cleanup before retaining its deleted UUID tombstone and
  allowing the live name to bind a new UUID. Lost closure replies reconcile
  through the same immutable operation; historical configuration replay cannot
  revive retired authority.
- Stream schema version three adds retirement state. Catalog schema version
  two migrates earlier pending configuration state and backfills immutable
  operation receipts. Current catalog grant checks precede configuration and
  retirement receipt disclosure, including for the original operation caller.
  Duplicate coordinated retirement rechecks receipts after its async preflight.
  Current stream/catalog versions are 4/3: nullable operator provenance preserves
  legacy SDK receipt ownership, and missing current-version columns fail before
  base schema mutation. Terminal catalog receipts retain the captured payload
  range and counters after bounded cleanup. Authenticated retirement/status CLI
  commands enforce revision, operation, caller and explicit destructive intent.

The dispatcher targets `__streamAppend` and `__streamSeal` on `__EventStream`; both event system
cell factories and their internal maintenance handlers now exist. Configuration
and consumption are available through programmatic native bindings. Native
project/manifest metadata now survives publication and generation loading, and
Node startup/adoption now performs owner-routed activation and binding readiness.
The development contracts are implemented but release qualification is incomplete, so
this does not constitute a released deployed primitive yet.

## Requirement audit

| Requirement | Evidence now | Work remaining |
| --- | --- | --- |
| Short producer APIs and atomic emission | Source tables, `tx`/`emit`, bounded serializer, typed admission errors, native source test, storage rollback/capacity/reopen tests | Complete public error/overflow classification and adoption budgets |
| Resource identity and authorization | Host RPC provenance, reserved classes, durable catalog reconciliation, independent receiver grants, receipt revocation, deletion/recreation and terminal UUID tombstones; validated native declarations/bindings; owner-routed startup/adoption with current-state proof and independent binding resolution | Friendly name-only resolution and complete administrative authority qualification |
| Stream append/read and deduplication | Persisted destination schema, built-in factory, atomic producer watermark, bounded read API; duplicate acceptance after payload expiry; version-1 stream/catalog and source/inbox schemas; feature/format capability | Wider fault and adversarial qualification |
| Durable outbox delivery | Real destination integration, generation-fenced cleanup, blocked permanent errors, adaptive batch limits, host/lane reservations | Cancellation/routing/fairness qualification and operational controls |
| Subscriptions and settlement | Durable leases/checkpoints/ack receipts; same-fetch replay, busy competitors, retry, absolute extension, bounded long polling | Complete deadline/error/race matrix and production lifecycle/crash qualification |
| Recovery and ownership | Source reopen; live subscription lease survives broker eviction/reopen; bucket-backed owner handoff and removed-class recovery preserve delivery, receipts and business state; existing output/wake protocols reused | Wider follower/owner-loss, source/destination/consumer crash and response-loss qualification |
| Retention and backpressure | Both modes, lease exclusion, bounded sweeps, optional cursor expiry; required-consumer pressure resumes after ack; coordinated/forced retirement and catalog tombstones | Complete lifecycle limits, audited whole-source destruction, scalable blocked-retention wake policy and capacity evidence |
| Consumer inbox | Atomic storage helper, digest conflict, capacity, rollback, reopen and bounded pruning tests; actual bucket-backed Node processing/retry/ack suppresses duplicate effects; runnable producer/consumer example verifies retained receipts across preserving restart | Complete proof/crash qualification |
| Operational controls | Source retry/seal and pending-reset protection; signed owner-routed source/stream/subscription CLI/API, guarded pause/resume/reset/delete and catalog retirement/status, immutable operation receipts, bounded subscription and destination-observed source inventory, metadata reads and payload opt-in | Catalog/fleet inventories, exported metrics, source destruction and broader control qualification |
| Compatibility and deployment | Native metadata/feature validation, binding validation under host limits, generalized wake projection; activation/readiness and failed-adoption preservation; peer/native/fleet/schema markers at 1; hard-cutover development policy and incompatible source-policy refusal | Fresh-state/reopen fault qualification; destination/consumer policy-adoption matrix |
| Examples and documentation | Full joint design, implemented transaction/inbox API guide, cataloged standalone stream-backed application and actual executable smoke with preserving restart | Extend examples as remaining lifecycle controls land; release qualification remains separate |
| Release criteria | Local SDK/storage/policy and integrated native fixture | Full workspace gates, supported single-node/fleet proofs, capacity, lifecycle and fault matrix |

## Next implementation step

Broaden activation qualification to missing/revoked external bindings at startup,
cancellation/deadline failures, configuration retries during revocation and
actual fleet routing. The current Node regression covers proven startup,
delivery and inbox redelivery, failed candidate binding resolution, serving
generation preservation, valid adoption, retirement and historic-receipt refusal.
It retains the full existing shutdown assertion and explicitly verifies that a
failed unsupported startup releases its directory for immediate reuse. Friendly
name-only resource configuration remains separate from resolved UUID bindings.

Add catalog stream listings, exported bounded metrics and remaining source
inventory/lifecycle controls. Subscription listing is implemented in the current
work below; catalog-wide authority remains a separate contract. Source
inspection, guarded retry, operation status and audit now have authenticated owner-routed HTTP/CLI
entry points. Subscription
administration, producer sealing and stream retirement are implemented, but
operator integration and production proof/failure coverage remain part of the
full goal. Audited whole-source destruction is still absent. It must verify
terminal retirement of every pending destination and fence the captured source
incarnation before discarding obligations; never add a sequence-skipping API as
a substitute for that protocol.

Retain the source operator's separate host-minted authority and owner RPC/tunnel
path when extending controls. Source retirement/destruction still needs its
own audited lifecycle protocol, rather than a raw delete or generation skip.
Qualify source operator cancellation, real response truncation and stale-owner
routing beyond the current two-node handoff test. Cold activation now has a
separate framework recovery pool for restored sources whose class is absent.
The core rejects unknown fresh scopes; admission then checks retained source
identity under the restored owner epoch and closes non-source connections.
This executor has no application bindings or callbacks and can dispatch stored
obligations and source controls. Business calls remain unavailable. Internal
work has priority over unavailable application alarms; those alarms back off
without using the abandonment budget. Re-registering the class returns business
execution to the application through ordinary generation movement. Wider crash,
follower and cancellation qualification remains required.

Exercise fresh initialization, same-build reopening and current-layout admission
under real fleet faults, including dropped marker CAS responses and the complete
destination/consumer lower-host-policy matrix. Historical migrations and prior
executables are outside the internal development policy.

Remaining correctness audits include configuration retries during revocation,
unknown/expired receipt classification after maintenance, long-poll cancellation
and waiter limits, permanent versus transient error mapping, incompatible lower
host policies, destructive source operations, control-message allocation bounds, and
maintenance scheduling for protected old history. Keep adapter effects separate
from pure protocol decisions. Source and destination tables currently use hex
text identities; the final schema still requires review. Layout changes use
development hard cutovers without migration machinery.
Normalized native projects currently supply explicit resolved UUIDs in
`Definition` and `Binding`; a friendly name-only configuration still needs
catalog resolution. Resource definition capture alone must not be mistaken for
successful activation.

Continue real failure qualification through discovery, replication, follower
proof, ownership changes, output gates and shutdown. The runnable example uses
actual bucket-backed Node services and requires complete preserving shutdown;
it verifies explicit retry and clean restart, not crash or actual response loss.
The lower-level fixture deliberately supplies a successful proof adapter and
no replication; it cannot establish those production guarantees. Do not mark
the goal complete from a local passing delivery test.

## Verification and environment

The checkout is `codex/outbox-design`, based on
`c35c1b257a5804d367a2aeffe65b8a3ba5c1aa3a`, with main through `6a8e252`
merged to preserve the Axum/external-ingress and workflow additions. The
implementation is submitted as draft PR #27 at the user's request. Operator tooling, examples and
production event failure qualification remain part of the active goal. Do not
discard the original design or documentation edits.

Use the installed toolchain via
`PATH=/Users/haipingfu/.cargo/bin:$PATH` and the checkout-specific mounted target:
`CARGO_TARGET_DIR=/Users/haipingfu/Workspace/crabbuild-target/cellar-2a16-outbox`.
Local rustc is 1.97.0; SDK CI specifies 1.98.1. `hex` was added as a direct
workspace dependency; the lock change only adds it to Cellar's dependency list,
with no package/version changes.

The latest completed focused run passed 67 Cellar library tests, 11 event policy
tests, the integrated native event-delivery fixture, the native source-outbox
test, and existing native transaction and Queue tests: 82 total. The integrated
fixture covers catalog idempotence, durable destination append, exact i64 payload
preservation, duplicate append after retention, unauthorized public RPC, current
grant revocation/restoration, stable live fetch replay, competing fetch exclusion,
retry, idempotent absolute extension, repeated ack, forged-token rejection,
live lease eviction/reopen, lease expiry and redelivery, bounded-history cursor
expiry, long-poll notification, adaptive atomic batching and required-consumer
backpressure. New evidence covers pause with required retention, resume,
explicit skipped ranges, reset invalidation, operation replay/conflict, old
command and fetch rejection, delete/recreate identity, paginated audit history,
operation capacity, resource grant checks despite stale local permissions,
legacy schema migration, terminal producer sealing, source reopen with pending
seal work, rejected new emission, duplicate terminal acceptance, and protocol
metadata preservation through reset. Latest evidence also covers coordinated
retirement's local preconditions, forced closure with a live lease and pending
source row, a dropped closure reply, restart during two-row cleanup batches,
catalog deletion/recreation, old-resource request rejection, and a retained
blocked source obligation. The fixture seeds an earlier catalog's pending
creation and verifies migration without losing its operation or authority.
After grant revocation, original callers cannot bypass current authorization
through configuration/status/retirement receipt replay. Its scope is local
storage/native turns and injected successful proofs, not production fleet
delivery qualification.

The source fixture verifies that an event serializer generating ten million
elements stops within the configured byte budget, caught admission failures
leave other business writes intact and consume no sequence, invalid event
versions are rejected, and integer payloads outside the supported range fail
without rounding. Storage tests additionally reject a first oversized envelope
without retaining source identity/schema initialization or outbox wake work.
One preexisting loader test initially failed because its drop probe used ambient
host registries during a foreign-domain drop. The probe now captures weak owner
registry references; the regression deliberately holds a different domain's
registry lock while dropping both retained worker slots. The final parallel
library run passes all 67 tests, including that regression.

Workspace Clippy across all targets and features with warnings denied passed
on the latest source state.
Documentation integrity, whitespace, formatter and the all-feature dependency
inspector pass; the latter reports 387 reachable packages and no JavaScript
engine. The formatter covers 358 Rust files. Clippy also type-checks all
workspace targets and features. The completed focused command is:

```sh
cargo test -p cellar --lib --test native_outbox --test native_event_stream \
  --test event_stream_policy --test native_transactions --test native_queue --locked
```

The latest deployment-focused run passed four native artifact tests plus the
native source and integrated event fixtures. It exercises event package
publication/loading, retained desired definitions, current binding projection,
reserved class installation, missing feature/schema refusal, forged principals,
scope/grant/subscription mismatch, conflicting environment names, lower host
limits and generation validation after tampering. The native CLI gate also
passes. That fixture proves the metadata contract, not runtime provisioning:
no automatic resource activation or real fleet proof is exercised by it.
Workspace/all-target/all-feature Clippy with warnings denied passes after these
deployment and stored-principal dispatch changes; formatting covers 359 Rust
files. The updated integrated fixture additionally verifies script rename and
binding removal across real generation adoption: old pending events reach the
destination under their original grant, while the new application cannot emit.

The complete workspace/all-feature library and integration test command now
passes, including CLI/artifact checks, the external Cargo dev/rebuild/restart
workflow, 16 public-node lifecycle/recovery tests, and all new event fixtures:

```sh
cargo test --workspace --all-features --lib --tests --locked
cargo test --workspace --all-features --doc --locked
```

All nine doctests pass in the separate stable target directory
`/Users/haipingfu/Workspace/crabbuild-target/cellar-2a16-outbox-docs`. Two attempts
in the main shared target failed to link runnable documentation examples because
transitive library artifacts were missing in rlib form. Building ordinary
libraries changed which artifacts were missing but did not resolve that cache.
An isolated complete build passed without changing source or weakening the tests.
The Cargo dev regression concurrently built an external project against the
main target; keep the doctest cache separate from that workflow. No other
checkout's build cache was cleaned.

The activation changes passed 90 focused regressions: 67 Cellar library tests,
four artifact tests, the integrated stream fixture, all 17 public Node tests,
and the native source fixture:

```sh
cargo test -p cellar --lib --test native_node --test native_artifacts \
  --test native_outbox --test native_event_stream --locked
```

The actual public Node event test uses a single-node development bucket without
a synthetic successful-gate adapter. It checks unsupported local-only startup
and joined cleanup, owner-routed declaration activation, delivery, durable inbox
redelivery, failed binding adoption, valid adoption, retired-resource replay
refusal and complete shutdown. A subsequent lifecycle review added catalog
resolution before partition binding resolution, closing adoption while delegated
retirement remains pending. The Node test now also refuses external bindings to
that retiring resource, without a local declaration that could mask the missing
catalog check. Both this Node test and the integrated stream fixture passed again
after that change. Workspace/all-target/all-feature Clippy with warnings denied
also passed on the latest source. Formatting covers 362 Rust files; documentation
integrity, whitespace and the all-feature dependency inspector pass. The complete
workspace and doctest results above predate activation changes; do not imply the
complete release matrix was repeated or achieved by these focused checks.

Event example, capacity, deployment cutover and event-specific production fleet
fault gates remain outstanding. Existing node/log/container suites are regression
coverage; they do not qualify the event primitive's complete activation path or
its full failure matrix. This file records work in progress, not production
readiness evidence.

The source schema and compatibility milestone passed 98 focused tests (72
library, five artifact, one CLI, one integrated stream, 18 public Node, and one
source SDK test) plus workspace/all-target/all-feature Clippy with warnings
denied. The public Node test refuses the old fleet marker before replacing its
predecessor, restarts after explicit upgrade, and refuses peer protocol 5.
Source activation under a smaller host policy fails without admitting storage;
a compatible domain reopens the original bytes and next producer sequence.
Legacy migrations preserve pending seal intent, consumer receipts and durable
deadlines. Unknown/missing markers across all four event schema families fail
before base schema mutation. A failed legacy accounting check rolls migration
back without repairing or discarding retained records.

Fleet upgrade tests scan more than one lease page, refuse an unconfirmed or live
fleet, and exercise eight concurrent identical CAS operations with immutable
receipt replay/conflict checks and retained cell/wake bytes. The actual shipped
CLI help and missing-confirmation errors pass for both binary names. The final
wire review also aligned the tunnel version with peer version 6 and added a
separate authenticated version-5 tunnel refusal. The isolated doctest gate passes
all nine tests on that final source, and workspace/all-target/all-feature Clippy
with warnings denied also passes after the tunnel change. Formatting covers
364 Rust files; documentation, whitespace and the dependency inspector pass.
The full workspace/all-feature library and integration command now passes on
the final source, including the external Cargo rebuild/restart regression and
both public Node event tests with the tunnel refusal. The separate doctest and
final Clippy commands also completed successfully. These tests still do not run the actual previous binary,
inject every crash boundary, or qualify the complete release matrix.

The draft-PR follow-up merges main through `6a8e252`, preserving optional
listeners and external ingress behind event resource activation. The Axum
counter explicitly supplies empty event metadata; the fleet upgrade CLI test
now follows main's single `cellar` executable after removal of its CLI alias.
The workspace/all-feature library and integration command passes 341 tests,
including the development build/restart regression, both public Node event
regressions, external ingress and the new Axum suites. The final bulk retry
fencing adjustment also passes all 14 focused event policy/source/stream tests.
All 11 workspace doctests pass in the separate cache. Final-source Clippy with
warnings denied, formatting across 400 Rust files, documentation (35 documents,
6 flows, 362 references), whitespace and native dependency inspection (401
packages; no JavaScript engine) pass. GitHub CI did not start: its check
annotation reports failed account payments or a spending-limit restriction.
This does not change the outstanding event release qualification gates.

The source-operator milestone passes the complete workspace/all-feature library
and integration command: 344 tests, including the external development
build/restart workflow and all Axum suites. The focused source/event/CLI/library/
public-Node command passes 96 tests. All 11 workspace doctests pass in the
separate target cache; final-source all-target/all-feature Clippy with warnings
denied and formatting across 405 Rust files pass. Documentation validates 35
documents, 6 flows and 364 references; whitespace and the 401-package native
dependency graph pass, with no JavaScript engine.

The new actual two-node test sends signed commands through a non-owner, proves
that unsigned/oversized requests are refused and inspection does not construct
the application cell, repairs a grant-blocked event, and verifies immutable
retry replay/conflict/caller guards, stale producer refusal, audit pagination,
settled no-op retry, delivery and receipt recovery after owner handoff. Business
reset retains the receipt and an unknown operation has a typed result. The SDK
fixture uses an isolated domain with a two-receipt budget: capacity rejection
leaves metadata/wake deadlines unchanged and exact replay remains available.
Schema tests retain receipts across activation and refuse corrupt accounting;
wire tests preserve the original event authority JSON shape. Treating a received
reply as lost exercises idempotent replay, not actual network truncation. The
complete production event failure matrix and wider orphan-source failure qualification
remain outstanding.

The removed-class recovery milestone passes 347 workspace/all-feature library
and integration tests, 110 focused tests and all 11 workspace doctests.
All-target/all-feature Clippy with warnings denied passes. A bucket-backed
public Node regression removes the source class, recovers its pending delivery
and operator receipt without application code, and reintroduces the class with
business state intact. A native worker regression retains an unavailable
application alarm through eight attempts without consuming its abandonment
budget and closes rejected non-source activations. These checks qualify those
paths, not the complete crash, follower-proof or cancellation matrix.

Public `Cursor::decode` and `Receipt::decode` now retain `InvalidCursor` and
`StaleReceipt` across malformed base64, JSON, protocol integers and identities.
They do not expose supplied token bytes in diagnostics. The token regression
corpus also preserves valid positions above binary64 precision through i64::MAX.
This closes that SDK boundary; deadline, RPC-reply and overflow classification
still require their own qualification.
All three token regressions and the existing integrated native stream test pass
after this change; final-source workspace/all-target/all-feature Clippy with
warnings denied also passes. The 347-test workspace result above applies to the
preceding recovery milestone.

Stream and subscription operator controls now delegate a supplied resource
binding through signed `/streams/` ingress and repeat current receiver grant
checks. RPC provenance limits operators to the stream control methods and cannot
authorize configuration or append. `cellar stream` exposes describe/read/status/
audit; `cellar subscription` exposes describe/pause/resume/reset/delete. Both
families use the same authenticated operator label for immutable receipt status
and replay. SDK event packets omit that optional field and SDK receipts without
it remain readable. Subscription operation receipts persist operator identity
inside their existing JSON field; no SQLite layout change is required.

Requests/binding files, expected identity files, read bytes and replies are
bounded to 16/4/256/1024 KiB; pages stop at 100 records. Metadata reads remove both
payload and its encoded envelope; payload display requires opt-in. Destructive
CLI changes require confirmation flags and the observed subscription identity.
An actual two-node regression passes signed forwarding, grant-claim refusal,
metadata/payload/continuation reads, pause/reset/resume/delete, stale identity,
immutable replay/conflict/operator guards, audit pagination, typed unknown status,
owner handoff recovery and revocation even for an existing receipt. The test
replays received replies; it does not inject actual network response loss.
Resource inventories, stream retirement CLI, exported metrics and the full
control-message fault/capacity matrix remain outstanding.

Read projection also rejects malformed page shapes and a response naming another
resource before issuing a resume cursor. Its regression reproduced a mutable
JSON-indexing panic before the fix. The first broader workspace run failed an
Axum service response assertion; the enhanced diagnostic assertion preserves
the same response/handler requirements. An isolated rerun and twelve concurrent
repeatability runs pass, but the original cause remains unproven. The final
workspace gate is rerun with Clippy scheduled afterward to avoid adding compiler
load during short-lease host tests.

That final runtime source passes all 354 workspace/all-feature library and
integration tests, including the Axum service assertion and the full 21-test
public Node suite, plus all 11 doctests. The two shipped CLI tests pass again
after replacing ambient fixture writes with owned temporary files. Final-source
all-target/all-feature Clippy with warnings denied passes. The earlier service
failure remains recorded above; passing reruns do not establish its cause.

Stream retirement now has owner-routed `retire` and `retirement-status` commands.
The CLI requires a revision, immutable operation, bounded reason and explicit
confirmation, plus either an all-producer coordination attestation or a force
data-loss acknowledgment. Operator authority permits catalog retirement/status
and its partition preflight/closure; configuration and append remain excluded.
Both catalog and partition receipts retain the authenticated operator label and
reject replay by another operator. Current grant checks remain independent.

Catalog schema 3 and stream schema 4 add nullable operator columns. Migrations
preserve earlier receipts with NULL provenance; current markers missing those
columns, or declaring another type, fail before base schema mutation. Terminal
receipts retain captured records/bytes and the exact affected retained payload
range after bounded cleanup. The range derives from the closure's record count
and immutable end offset; retirement closes append and retention only removes
prefixes. Range/counter/provenance acceptance is checked before the catalog
records a terminal result. Older SDK status JSON remains readable.

The actual two-node regression passes both coordinated and forced retirement:
unsealed producers and undrained subscriptions refuse coordination; a source
seal and consumer acknowledgment permit it. Force captures the unconsumed range.
Receipts survive owner handoff and preserve replay/conflict/operator guards,
typed unknown status, deleted-stream read refusal and historic deployment
refusal. The logical migration test preserves old receipt fields and enforces
new provenance bounds; it is not previous-binary fleet cutover qualification.
The coordination procedure is an attestation, not proof of unseen producers.
Resource inventories, exported metrics, whole-source destruction, runnable
examples and the complete lifecycle/fault/capacity matrix remain outstanding.

The first full retirement gate run reported `NodeFenced` in two native actor
checks while doctests were compiling. The exact same actor binary passes all
six tests after the competing compile finishes, with lease/deadline policies
unchanged. The cause is unproven; the full workspace gate is rerun sequentially
with Clippy scheduled after tests. No fencing or drain assertion was weakened.

The final sequential retirement gate passes 358 workspace/all-feature library
and integration tests, including all 22 public Node tests and the development
rebuild/restart regression. All 11 doctests pass in the separate target cache;
all-target/all-feature Clippy with warnings denied passes on final source.
The earlier actor failures remain recorded above; passing reruns do not establish
their cause or qualify the complete production fault/capacity matrix.


The cataloged `cellar-example-streams` executable now demonstrates a business
transaction and `tx.emit`, bounded resumable reads, a required subscription and
a local projection composed with `tx.inbox`. Its explicit `db.sync` precedes
remote retry/ack. The smoke test verifies duplicate/conflicting commands, two
source cells, stream delivery, cursor resumption, committed effects before retry,
a preserving process restart, retained-inbox suppression before acknowledgment,
an empty fetch and a fresh effect after restart. Both shutdowns require complete
ingress, application, cell and pending-dispatch drain reports. The example owns
its executors and a private SQLite-backed object-store fleet, and persists a
randomly generated fleet secret across restart. It does not qualify a bucketless
SQLite node, abrupt process failure, network response loss or a production fleet.

`examples/durable-streams/README.md` provides the runnable walkthrough and smoke
command. The storage and asynchronous-work guides distinguish implemented APIs
from remaining release criteria. The example is included in the 35-entry catalog.
The all-feature executable build and actual smoke pass; all-target/all-feature
workspace check and Clippy with warnings denied pass. Formatting, native
dependency inspection (401 reachable packages, no JavaScript engine), catalog
source/guide existence, documentation integrity (35 documents, 6 flows, 370 local
references) and whitespace checks pass. The default-feature executable check,
all eight required CLI/artifact regressions and the cookbook catalog regression
also pass. Resource inventories and bounded exported metrics are the next
operational priorities, alongside the still-open source destruction and
full lifecycle/fault/capacity matrix.


Subscription inventory is wired through the SDK's `Stream::subscriptions`,
`cellar subscription list` and signed `subscriptions` commands on the existing
owner route. The current durable administration grant is checked on each page.
A primary-key range reads at most 101 metadata rows for a 100-record page, even
with unrestricted host limits. Deleted incarnations remain visible while
retained; no lease token, payload or envelope is returned. The `subs1` cursor
is capped at 256 bytes, bound to the stream UUID and distinct from event-read
cursors. Receiver and client/operator acceptance validate identity, ordering,
page bounds and exact continuation position. Pages are live observations, not a
fleet census or snapshot. No persisted layout or wire authority fields change.

Focused validation passes: both cursor/error and malformed-response corpora,
all eight CLI/artifact regressions and an actual two-node pagination/lease/
tombstone/handoff/revocation test. All-target/all-feature workspace check passes.
The final broader workspace run passes all 361 library/integration tests,
including all 23 public Node tests and the Axum forwarding assertion.
All-target/all-feature Clippy with warnings denied, all 11 doctests and formatting
(413 Rust files) pass. The rebuilt default-feature stream example passes its
actual delivery/inbox/ack/preserving-restart smoke with complete shutdown
reports. Documentation integrity (35 documents, 6 flows, 370 local references),
native dependency inspection (401 packages, no JavaScript engine) and whitespace
checks pass. These checks are not the full production release qualification. Catalog
stream/source inventories, exported metrics, whole-source destruction and the
complete lifecycle/fault/capacity matrix remain open.


The first broader subscription-inventory gate passed all 23 public Node tests
but failed the Axum forwarding test during startup: node authority was not
established before its startup deadline. Two isolated runs of the exact same
`errors-f316316f0fc88d26` binary and four repeatability runs (two at a time) pass
without code, lease, startup-deadline or drain-policy changes. The cause remains
unproven. The local reload/replication diagnostic appears in successful runs as
well and is not evidence of the cause. The full workspace gate is rerun before
compiler/doc/example gates; the original failure remains part of the evidence.


Catalog inventory work exposed an unmet creation requirement: proposed resource
grants could authorize their own initial creation. The guard now requires a
private provisioning claim from an exact trusted deployment declaration for both
catalog and partition creation. SDK and resource-operator handles cannot mint it.
`Generation::event_provisioning_request` prepares this request for a library host;
it still needs ordinary owner/recovery/proof routing and is not readiness proof.
The native fixture uses fresh state and explicitly checks SDK creation refusal.

## Internal development policy (2026-10-13)

The user superseded earlier compatibility milestones: every Cellar-owned protocol,
format and schema marker stays at 1. Hard cutovers recreate development state and
rebuild participants. No old-build compatibility qualification, schema migration,
fleet upgrade command or upgrade receipt is required. Prior verification entries
below/above describe historical work, not current compatibility promises. Current
code removes those paths, retains creation authority and durability/ownership
checks, and keeps upstream SQLite WAL/LTX codec identifiers unchanged.

Catalog/source inventories, bounded exported metrics and the remaining actual
crash, response-loss, recovery and capacity qualification remain open.
The first full gate passed 78 library tests but refused three legacy fixtures:
the source-recovery fixture seeded outbox tables without a marker, the inbox
fixture deleted its marker before reopening, and the reset/replication fixture
seeded stream/catalog tables without markers. These fixtures now initialize
current version-1 state and retain their original recovery, rollback, deduplication,
reset and replication assertions. No production validation was relaxed.
An independently executed catalog regression also refused the fixture's stale
serving bindings during declaration validation. Its valid trusted provisioning
graph is now separate from the intentionally stale SDK serving graph; receiver
revocation checks and declaration validation remain unchanged. Those regressions
passed on the corrected fixtures. The native development rebuild/restart case
passed in 804.77 seconds across its separate unchanged readiness deadlines.
The full Cargo run then stopped at two outbound HTTP deadlines. The exact
`native_egress-bc2323df6faa7849` executable passed all five tests in isolation,
but a repeat overlapping the remaining runtime executables reproduced those
timeouts. After runtime checks finished, four single-test and four whole-executable
runs passed without code, timeout, lease or drain changes. The cause remains
unproven; these results do not establish that compiler or system load caused it.
The 65 remaining compiled workspace executables ran directly, including the
corrected catalog regression and all 23 public Node tests. Only the tooling
executable lacked `rustfmt` because that direct harness omitted Cargo from PATH;
all six tooling cases passed after restoring the normal PATH. No uninterrupted
full Cargo pass is claimed for this cutover. All final executables have passing
results; the HTTP timing failures remain part of the evidence.
All-target/all-feature Clippy passed with warnings denied. Final formatting
checked 412 Rust files; documentation validated 35 documents, 6 flows and 370
local references, and whitespace checks passed. All Cellar-owned protocol,
format and schema markers are 1; upstream LTX/SQLite encoding and runtime
revision/sequence counters retain their meanings. No development state outside
private test fixtures was reset.


## Destination source inventory (implemented; qualification incomplete)

Adds SDK `Stream::sources`, signed `sources` operator commands and `cellar stream
sources`. Pages require current resource administration and expose only observed
producer lane, source/incarnation, captured principal/grant, accepted sequence
watermark and seal state. A primary-key range bounds SQL to 101 rows, pages to
100 records, and namespace-bound `sources1` continuations to 256 bytes. Receivers
and clients validate lane derivation, ordering, continuation tail, metadata bounds
and canonical decimal watermarks. Payloads, bearer tokens and source retry state
are absent. This is not an undispatched-source census or all-producer drain proof.

All-target/all-feature workspace check and documentation validation passed.
Eight focused source tests passed, including namespace-bound cursors, page-tail
validation, large decimal watermarks, forged lane identity and sensitive field
rejection. The focused public Node/CLI gate passed for retained seals after payload
expiry, real two-node owner handoff and administration revocation during pagination.
Its test uses actual bucket-backed Node services and unchanged proof/shutdown
checks; a zero-event source seal is not fabricated through the SDK. No
protocol/schema version bump.
Catalog-wide/fleet-wide inventory, exported metrics, audited whole-source
destruction and the remaining actual failure/capacity matrix stay in scope.


The initial Node/CLI gate passed both CLI tests and 23 existing Node tests, but
the new inventory test reproduced an incomplete entrance-node handoff drain.
Diagnostics showed its source seal was locally accepted, zero owned/occupied
cells, and two terminal adoption operations still waiting for pacing permits.
The core records terminal failed acknowledgements only after ownership release,
but the driver extended its stall window only for successful acknowledgements.
With more cells than release slots, completion of a failed first batch was not
recognized as progress and the next batch was cut off.

The core now exposes settled handoff count (successes plus terminal failures);
the driver uses that count for progress while retaining the original absolute
shutdown deadline, admission gates, durability proofs, and successful-adoption
count. Seven core routing tests passed, including terminal-only counting, paced
continuation and duplicate/stale reply exclusion. The unchanged full drain
assertion now passes: all 24 public Node and both CLI tests passed, including
source metadata after payload expiry, owner handoff, grant revocation, and the
absolute-deadline regression. Temporary warning diagnostics are removed.
The build script now declares its sole file input, build.rs, so unrelated test
file edits do not trigger Cargo's default package-wide build-script rerun.
Broader workspace qualification was attempted before this milestone was pushed;
its failures and independent replays are recorded below.

The follow-up marker audit found telemetry's `v0-unstable` metadata and the KV
large-value `v2` reference/object prefix. They now use `1` and `v1` respectively,
with matching producer, reader, collector and test callers. Epoch fencing and
collection rules are unchanged. Existing development KV blobs require recreation;
there is no old-reference reader, migration or dual-prefix collector. That
workspace suite was compiled before these two marker edits; its results do not
qualify the final codec changes. The existing KV integration was rerun afterward.

The broader all-feature Cargo run failed dev's unchanged 600-second first-build
readiness stage, two outbound HTTP cases, three external ingress cases and the
facet case. The all-feature Node binary reported five failures and then stopped
producing terminal results for its remaining event cases for over ten minutes.
It was terminated so `--no-fail-fast` could continue other executables; this is a
failed, incomplete gate. A one-second OS stack sample collected data but stalled
while resolving symbols and supplied no usable diagnosis. Memory compression and
other compilation were observed, but neither establishes the cause. No test
deadline, fencing rule or shutdown assertion was weakened. Exact-binary replay
and final marker qualification followed, as recorded below.
The later replication case failed with `Resource temporarily unavailable` and
Axum errors, host-parity, lifecycle and SDK-events suites also failed startup or
ingress checks. The full command exited 101 with ten failing targets. Passing
individual replication checks earlier in that run do not establish a passing
replication suite. An exact-binary serial Node replay followed independently
with unchanged per-operation deadlines and drain proofs.
The exact all-feature Node executable then passed all 24 tests in isolation with
serial test scheduling (194.28 seconds), including the five prior failures, the
source-inventory regression and all drain assertions. This scheduling comparison
does not prove the cause or turn the failed concurrent run into a pass.
Serial exact-binary replays also passed external ingress (5), facets (1), Axum
errors (1), host parity (4), lifecycle (2) and SDK events (1). Outbound HTTP
retained both deadline failures (3/5 passed); the supplied-credential replication
case again returned OS error 35 (3/4 passed). These two failures remain
reproducible in isolation. The dev build case was not repeated in this replay.
Final KV, CLI and artifact tests rebuilt the snapshot containing the
telemetry/KV version-1 edits with bounded compiler concurrency and passed (one
KV case, two CLI cases and six artifact cases). The KV integration retains its
large-value restore, orphan cleanup, failed deletion, concurrent sweep/put and
later-epoch protection assertions under the `v1` reference/object layout.

Final all-target/all-feature Clippy passed with warnings denied, and all 11
workspace doctests passed. Formatting checked 413 Rust files; documentation
validated 35 documents, 6 flows and 370 references. The all-feature dependency
graph retains 401 reachable packages with no JavaScript engine; whitespace checks
passed. These successes do not supersede the failed/incomplete broad runtime gate
or establish full release qualification.
