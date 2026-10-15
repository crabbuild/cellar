# Outbox implementation status

Updated 2026-10-14. The goal remains the entire
[outbox, stream, and subscription design](002-outbox-streams-subscriptions.md).
The feature is under implementation and is not ready for normal deployment or
release. The native fixture now exercises source, catalog, stream, and consumer
turns together; that local evidence does not satisfy the fleet/release gates.

All Cellar-owned protocol, format and schema markers stay at 1 during internal
development. Layout changes use hard cutovers with recreated state and rebuilt
participants. Later sections retain historical verification notes; their old
version numbers, migrations, legacy readers and fleet-upgrade tests describe
removed implementations and are not current supported behavior.

## Current code

- `Storage::tx` delegates to the existing synchronous savepoint machinery.
- Stream age maintenance quiesces when the first retained record is protected
  by a required checkpoint or live lease. Lease-expiry work remains scheduled;
  checkpoint/lifecycle/configuration transitions transactionally recompute the
  deadline. Indexed minimum checkpoint, bounded leases and one oldest record
  bound eligibility observation without scanning retained payloads. Eligible sweeps retain their existing
  bounded continuation interval and other alarms/work reasons remain intact.
- `cellar outbox census [CLASS]` walks one bounded bucket-directory page and
  probes source owners through signed inspection, including root producers
  unseen by destination inventories. It reports source/empty/unavailable without
  lanes/payloads/tokens; partial failure prints the page/cursor and fails the exit.
  `census1` continuations bind fleet/prefix/class and filtering never expands the
  listing budget. This is live root census, not facet/historical inventory or
  aggregate gauges. The real RustFS CLI roundtrip verifies one-prefix pagination,
  a capacity-blocked undispatched source, empty cells, foreign cursor rejection
  and unavailable owners. Broader backend/ownership/cold/facet gates remain open.
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
- The version-1 stream schema uses live-name subscription uniqueness, preserving
  retained deleted identities. Earlier development layouts require recreated
  state; the native fixture exercises deletion/recreation and durable audit
  capacity without a schema migration.
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
- Version-1 stream and catalog schemas contain retirement state and immutable
  operation receipts. Current catalog grant checks precede configuration and
  retirement receipt disclosure, including for the original operation caller.
  Duplicate coordinated retirement rechecks receipts after its async preflight.
  Nullable operator provenance distinguishes SDK and operator receipt ownership,
  and missing current-layout columns fail before
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
| Resource identity and authorization | Host RPC provenance, reserved classes, durable catalog reconciliation, independent receiver grants, receipt revocation, deletion/recreation and terminal UUID tombstones; validated native declarations/bindings; owner-routed startup/adoption with current-state proof and independent binding resolution; optional name selectors resolve and pin UUIDs once per generation | Complete name/adoption race and administrative authority qualification |
| Stream append/read and deduplication | Persisted destination schema, built-in factory, atomic producer watermark, bounded read API; duplicate acceptance after payload expiry; exact-byte append retry after a proven peer reply is lost; version-1 stream/catalog and source/inbox schemas; feature/format capability | Wider fault, separate-host/storage and adversarial qualification |
| Durable outbox delivery | Real destination integration, generation-fenced cleanup, blocked permanent errors, adaptive batch limits, host/lane reservations | Cancellation/routing/fairness qualification and operational controls |
| Subscriptions and settlement | Durable leases/checkpoints/ack receipts; same-fetch replay, busy competitors, retry, absolute extension, bounded long polling; proven ack reply loss and unchanged receipt replay return `AlreadyAcknowledged` with one effect/checkpoint | Complete deadline/error/race matrix and production lifecycle/crash qualification |
| Recovery and ownership | Source reopen; live subscription lease survives broker eviction/reopen; bucket-backed owner handoff and removed-class recovery preserve delivery, receipts and business state; existing output/wake protocols reused | Wider follower/owner-loss, source/destination/consumer crash and response-loss qualification |
| Retention and backpressure | Both modes, lease exclusion, bounded sweeps, optional cursor expiry; required-consumer pressure resumes after ack; coordinated/forced retirement and catalog tombstones | Complete lifecycle limits, source destruction crash/lease/policy and SQL-reset capacity qualification, retention continuation/capacity evidence |
| Consumer inbox | Atomic storage helper, digest conflict, capacity, rollback, reopen and bounded pruning tests; actual bucket-backed Node processing/retry/ack suppresses duplicate effects; runnable producer/consumer example verifies retained receipts across preserving restart | Complete proof/crash qualification |
| Operational controls | Source retry/seal and pending-reset protection; signed owner-routed source/stream/subscription CLI/API, guarded pause/resume/reset/delete and catalog retirement/status, immutable operation receipts, bounded subscription and destination-observed source inventory, metadata reads and payload opt-in; guarded source destruction/recreation, exact terminal evidence, retained loss receipts, bounded remote/local facet cleanup and incarnation fencing; namespace catalog inventory and fixed-cardinality event metrics | Catalog namespace/facet/historical census, aggregate owner-state/physical metrics and broader control qualification |
| Compatibility and deployment | Native metadata/feature validation, binding validation under host limits, generalized wake projection; activation/readiness and failed-adoption preservation; peer/native/fleet/schema markers at 1; hard-cutover development policy and incompatible source-policy refusal | Fresh-state/reopen fault qualification; destination/consumer policy-adoption matrix |
| Examples and documentation | Full joint design, implemented transaction/inbox API guide, cataloged standalone stream-backed application and actual executable smoke with preserving restart | Extend examples as remaining lifecycle controls land; release qualification remains separate |
| Release criteria | Local SDK/storage/policy and integrated native fixture | Full workspace gates, supported single-node/fleet proofs, capacity, lifecycle and fault matrix |

## Next implementation step

The host/routing failure classification is implemented below. Continue the full
SDK preflight/codec/timeout/cancellation mapping audit, aggregate metrics and wider
inventories, alongside the complete release matrix.
The bounded/canonical Stream::read acceptance gap found by that audit is repaired
below; it does not close the complete client/codec/subscription response audit.

Investigate the observed cancelled-bootstrap deadline/DomainClosed failure; isolated
repeats and a complete node replay pass, but its cause remains unestablished.

Complete the remaining release fault matrix across independent processes/physical
hosts and supported storage backends, including followers, expired/isolated owners,
source cleanup cut points, grant/policy races and whole-fleet recovery. Keep saved
independent measuring ledgers and audit before replay at every ambiguous boundary.
The process harness and before-first-send SIGKILL case supply scoped single-node
SQLite-bucket evidence. The real peer proxy adds append/ack response loss across
three embedded nodes in one process, not the entire fleet fault matrix.
The independent-process append-loss driver also runs on a fresh private RustFS
S3 bucket; its passed case proves network-store recovery at that boundary, without
qualifying store/follower loss or separate physical hosts.
The whole-fleet driver also proves one bucket-mode recovery boundary on both
backends after killing all three processes and discarding their local node data,
with a pending source and proven unacked consumer effect. Wider cut points and
follower-only/store/power faults remain required.
The independent ack-loss driver also audits checkpoint/effect/receipt before
destination SIGKILL and lost reply, then replays only the persisted exact receipt
after recovery audit. Its SQLite/RustFS cases cover that selected ack boundary;
consumer loss and the remaining lease/ack cuts are still required.

Source destruction/recreation now has a durable closed-admission lifecycle,
exact destination evidence, immutable loss receipts, source/work/lane fences,
registered facet cleanup and fresh producer/facet identities. Explicit replay
resumes outage-delayed cleanup; remote and local removal counts are bounded.
Qualify abrupt loss, lease/grant/policy changes and large business SQL reset,
and measure physical SQLite/WAL/LTX/bucket growth and maintenance fairness.
Cold wakes, hot streams, compaction/restore, lag/backpressure, memory and offline
soaks retain their full capacity scope.

Finish catalog namespace/facet/historical source discovery, aggregate owner-state/physical metrics, the complete
name-resolution/policy/error/adoption matrix and
qualified SQLite-only recovery inventory. Current subscription, accepted-source
and namespace catalog listings are bounded live views; accepted-source inventory
is not a census of undispatched producers or an all-producer drain proof.
Fixed-cardinality host event observations are not cold-cell authoritative gauges.
Run the full workspace/release gates and audit every design requirement before
claiming completion. Historical verification notes below do not replace current
requirement-by-requirement evidence.

Retain the source operator's separate host-minted authority and owner RPC/tunnel
path when extending controls. Never substitute raw deletion or a sequence skip
for the audited lifecycle.
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
Native definitions retain explicit creation UUIDs. `BindingConfig` now supports
omitted IDs for catalog name resolution; runtime `Binding` identities stay fully
resolved and pin to the activated generation. Resource definition capture alone
must not be mistaken for successful activation.

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
implementation is submitted as PR #27 at the user's request. Operator tooling, examples and
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

## Runtime qualification diagnostics (2026-10-13)

The first instrumented native HTTP replay reproduced its 30-second deadline
while the first `network` call was pending. Service fetch/RPC/stream, loopback,
restricted environment and broker routing had completed. A later stage-marked
run passed, and all three replays with client-builder boundary markers passed.
The diagnostic native S3 case passed; generic store construction took about
2.4 seconds of the existing peer's 3-second accept window. An isolated client
construction probe measured native-certificate builds at about 0.74 and 0.16
seconds, while disabling that one input completed below the log's millisecond
resolution. These observations do not establish the cause of the earlier
HTTP deadline or S3 error. No production trust configuration, client factory,
credential provider, timeout or proof policy changed.

Temporary client/run logs and the construction probe were removed. The HTTP
fixture now reports a fixed phase label on its original deadline error, and
S3 accept/header failures identify the reader kind and request attempt. Final
serial HTTP and replication suites passed all 5 and 4 cases respectively;
focused all-feature Clippy passed with warnings denied. The failed broad gate
and earlier isolated failures remain part of the evidence; these diagnostic
passes do not prove a fix or establish full release qualification.

Catalog inventory is the next implementation priority. Namespace inspection
needs its own host-minted capability bound to tenant/application, catalog scope
and the read-only listing method. Existing stream administration remains
resource-scoped and must not mint that capability. Inventory must use bounded
primary-key pages, retain deleted identities, expose only lifecycle/identity
metadata, and use namespace-bound continuations. It cannot substitute for
whole-fleet producer discovery or a coordinated retirement proof.

## Catalog namespace inventory (implemented; release qualification incomplete)

Adds `cellar stream list --tenant/--application`, signed `/catalogs/` ingress,
namespace-bound host RPC authority and bounded scalar metadata pages. Resource
administration cannot mint the namespace capability or invoke the listing method.
Current identity/lifecycle, operation and accepted/desired revision columns commit
with definition transitions; listings copy no configuration, grants, tokens or
payloads. Cursor and response acceptance bind namespace, order and page tail.
The catalog schema remains 1; a prior layout lacking metadata columns is refused
before base schema mutation and must be recreated. No migrations are introduced.

Compilation and targeted qualification passed. New checks cover bounds, cursor namespace,
decimal revisions, sensitive-field rejection and method/scope capability limits.
The real bucket-backed Node fixture creates three resources, retires one, resumes
one-record pages after owner handoff and requires full original shutdown proofs.
The reset/replication fixture initializes the current scalar catalog columns while
preserving its existing reset and restore assertions. Whole-fleet inventory,
metrics, source destruction and the full fault/capacity matrix remain open.

The first routed integration run failed with a remote RPC error. A direct-owner
probe reproduced `InvalidRequest: event RPC arguments must be a single object`:
the new request producer had omitted the event protocol's one-element argument
array. After correcting that, the direct-owner result exposed a missing operator
`ok` to `result` normalization. Both contracts are now exercised through the real
event decoder and response projection, including malformed reply envelopes.
The original two-node test then passed (7.49 seconds), with retained tombstones,
one-record continuation after handoff, unsigned-request refusal, namespace/path
mismatch refusal and complete original shutdown reports. No deadline or proof
assertion was changed.

The wider targeted gate passed 88 library, 6 artifact, 2 CLI, 1 catalog delivery/
retention/revocation and all 25 Node tests (229.46 seconds for the Node suite).
Workspace all-target/all-feature Clippy passed with warnings denied. Formatting
checked 417 Rust files; documentation validated 35 documents, 6 flows and 370
references; the dependency graph remains 401 reachable packages without a
JavaScript engine. All 11 final workspace doctests passed. This is targeted qualification,
not a new uninterrupted full workspace runtime pass or release completion.

## Event host observations (in progress)

Adds an execution-domain-owned fixed-size metric registry, public
`HostServices::event_metrics()` snapshots and fleet-authenticated internal
`GET /event-metrics` (Prometheus text) / `?format=json` exports. No cell, event,
stream, subscription, payload or caller labels enter the registry. Counters and
histogram buckets saturate rather than overflowing; JSON counters/sums preserve
full integer precision with decimal strings. Timers retain their original host
clock and registry across cancellation in another domain. Missing clocks after
closure are counted explicitly rather than replaced by invented elapsed time.

Current observations cover dispatch RPC outcomes/duration, local append and
deduplicated record counts, per-new-record source timestamp to local append age,
ack processing/replay/duration, cursor-expiration observations and due-discovery
listing failures. Active waiter/inflight dispatch gauges use existing bounded
reservations. These are host-lifetime observations, not durable acknowledgment
receipts or a census of dormant/cold cells. Backlog, bounded blocked-reason,
stream allocated/logical byte and offset, active-lease, subscription-lag and
retention-blocked byte gauges still require authoritative owner-state snapshots.
The full metrics requirement therefore remains unfinished.

Two registry tests passed (saturation, exact export, bucket accumulation, host
isolation and cancellation/closed-clock attribution). The real two-node test
passed in 10.65 seconds: actual emission/append/ack appears in SDK and signed
JSON/text exports, unsigned reads are refused, sibling-host counters remain
untouched, and both nodes retain original complete shutdown assertions. The wider
targeted gate passed 90 library, 6 artifact, 2 CLI, 1 catalog delivery/retention/
revocation and all 26 Node tests (239.44 seconds for the Node suite). Workspace
all-target/all-feature Clippy passed with warnings denied; formatting checked
419 Rust files, documentation validated 35 documents, 6 flows and 370 references,
and the dependency graph remains 401 packages without a JavaScript engine.
All 11 workspace doctests passed.
No persistent marker or compatibility path is added. This is targeted
qualification; the complete runtime, fault and capacity matrix remains open.

## Subscription owner observations

Owner-routed subscription status and bounded administration inventory now include
sample time, logical unread offset distance, retained unread record count, oldest
retained unread append age, lost-history state and lease activity. Core arithmetic
handles the full signed offset range. Owner time clamps to the durable logical
clock without advancing it or expiring leases; each returned row adds at most
one primary-key timestamp lookup without payload copies or unread-range scans.
All integer fields use canonical decimal strings. SDK/operator acceptance checks
lag/age/lease consistency and target identity; retained deleted incarnations
remain inventory entries. The internal development markers remain 1, with no
legacy response fallback.

The core lag regression passed for empty streams, lost history, maximum offsets
and invalid checkpoints. The persisted stream fixture passed new assertions for
lease activity, acknowledgment clearing lag, retention leaving positive unread
lag with null retained age, and an expired stored lease remaining visible as
inactive. All 26 serial Node cases passed (233.93 seconds), including lag/age assertions
across real owner handoff and the original revocation, tombstone, payload-exclusion
and complete shutdown gates. Final source qualification passed 91 library cases,
the core lag test, workspace all-target/all-feature Clippy with warnings denied,
and 11 workspace doctests (one existing ignored case). Formatting checked 419
Rust files; documentation validated 35 documents, 6 flows and 370 references;
dependency checks retained 401 packages without a JavaScript engine, and
whitespace checks passed. All 2 CLI and 6 artifact smoke checks passed.
The latest published-head CI run (`51be79a`) could not start because its annotation
reports failed account payments or a spending-limit restriction. No CI success
or new uninterrupted full workspace runtime pass is claimed.

This adds authoritative per-subscription diagnostics, not aggregate gauges in
`/event-metrics` or a fleet/cold-cell census. Outbox oldest age and bounded blocked
reasons, stream physical/logical byte and offset observations, retention-blocked
bytes, aggregate lease/lag gauges, whole-fleet discovery, source destruction and
the full release fault/capacity matrix remain unfinished.

## Source backlog observations

Outbox inspection now includes sampled wall time, whole-source oldest pending
age, and seven fixed permanent blocked-reason counts. Lane metadata includes
its own oldest pending age and typed blocked reason. Blocked terminal seals
count even with no pending event payload; null age means no pending event rows.
Wall-clock skew clamps negative age to zero. These are source observations,
not cold-cell/fleet totals or aggregate series in the host metric export.

Per-lane row/byte and source reason counters update with emission, deletion,
block/retry and acceptance in the same SQLite transaction. Reason classification
uses typed permanent failure codes, never parsed human messages. Explicit
covering age indexes read at most one timestamp; lane inspection no longer
counts/sums its pending range. Restore audits counters and blocked states and
requires accounting triggers/age indexes. SDK and signed operator page acceptance
checks source/lane identity, totals, fixed reasons and age/cursor consistency.
The schema remains 1. Earlier development layouts require recreation; there is
no migration or legacy response fallback.

The first compile exposed a missing semicolon in the cursor helper extraction;
it was fixed before qualification. All 94 library cases, 6 artifact and 2 CLI
checks passed, including new accounting rollback/restore, corrupted counter /
missing index/trigger refusal, indexed query plans, decimal precision and forged
response cases. The persisted stream delivery/retention/revocation test passed
with the new layout. All 26 serial Node cases passed (237.01 seconds), including source retry,
handoff and removed-class recovery with current age/reason observations and the
original full shutdown gates. The native outbox SDK fixture initially failed its
whole-response equality check because new sampled ages advanced by one
millisecond. Its comparison now derives creation timestamps from those ages,
retaining every original metadata/counter/reason/wake-deadline equality check.
The exact SDK replay passed (0.20 seconds), including operation capacity and
immutable receipts. Workspace all-target/all-feature Clippy passed with warnings
denied; 11 workspace doctests passed with one existing ignored case. Formatting
checked 419 Rust files; docs validated 35 documents, 6 flows and 370 references;
dependency checks retained 401 packages without a JavaScript engine, and
whitespace checks passed. This is targeted qualification; the earlier failed
full workspace runtime gate and remaining release matrix are not superseded.

Complete aggregate export, stream physical/logical byte/offset and retention
observations, whole-fleet discovery, source destruction and actual process/
response-loss/follower/ownership/capacity release qualification remain open.

## Stream retention observations

Adds typed `Stream::stats` and owner-routed stream describe observations under
current administration grants: retained rows/bytes, earliest/end offsets, sample
time, active leases, expired subscriptions, oldest retained append age, deletion
frontier, protected bytes and age-expired bytes blocked against deletion. Typed
SDK/operator acceptance checks identity and metric consistency. Metadata is
bounded by the definition's 128-subscription cap; frontier/timestamp indexes and
exact offset reads avoid event-range aggregation.

Private modulo-2^63 byte-prefix positions measure exact retained windows below
the modulus without imposing a lifetime byte-admission ceiling. They are not
delivery identity counters; offsets and sequences remain checked/non-wrapping.
Byte accounting commits with append and survives prefix deletion/retirement.
Restore validates contiguous offsets, encoded sizes, timestamp order and byte
positions before base-schema mutation. The schema remains 1 and old layouts
require recreated development state without migration.

SQLite allocated/used page observations use a fixed framework helper after
activation checks. They are explicitly not WAL, local LTX, bucket, or filesystem
allocated-block measurements; complete physical usage and aggregate export
remain unfinished. Logical retention observations cannot substitute for those
remaining storage/capacity requirements.

The first gate passed existing library/CLI/artifact/outbox and core checks but
failed the new stream/Node description paths because SQLite denied the allocation
pragma query. It was moved to the fixed storage boundary helper rather than
broadening public pragma permissions. The persisted stream replay passed,
including age-blocked lease retention, required-checkpoint protection, empty
streams and post-reclamation observations. New metadata guard cases passed for
wrap, prefix reclamation, inconsistent counts/positions/times and missing layouts/
indexes, preserving business state before schema mutation. The corrected gate
passed 97 library cases, 2 core cases, 6 artifact, 2 CLI, 1 persisted stream,
1 native outbox and all 26 serial Node cases (230.76 seconds). Workspace
all-target/all-feature Clippy passed with warnings denied. All 11 workspace
doctests passed with one existing ignored case. Formatting checked 420 Rust
files; docs validated 35 documents, 6 flows and 370 references; dependency checks
retained 401 packages without a JavaScript engine; whitespace checks passed.
This is targeted qualification, not completion of the earlier failed full
workspace runtime gate or the remaining process-fault/capacity matrix.

## Actual process loss and independent receipt ledger

The durable-stream executable has an explicit `--fault-harness` mode with a
one-record required stream, source command/producer receipts, atomic consumer
receipt/effect archive and an owning-request hold after `db.sync` and before ack.
The ordinary walkthrough has no enabled hold/probe controls. Normal framework
ownership, discovery, replication, output, lease and shutdown gates are retained.

The independent client fsyncs its plan/attempts before issuing commands, records
source acknowledgments and payload digests, archives encoded-envelope digests,
and saves each original snapshot. A real proxy consumes the source's successful
HTTP response and shuts the client socket. Actual SIGKILL exits are checked for
an acknowledged source with pending capacity pressure and for proven consumer
local effects before ack; forced loss cannot supply a graceful stop report.
Every restart is audited before ambiguous source retry or consumer redelivery.
Retained lease expiry is observed before fresh fetch; redelivery adds no duplicate
local effect. Archived effect receipts account for intentionally reclaimed events.
The final projection equals its two distinct receipts and checkpoint 2.
The final host requires all original complete preserving-shutdown assertions.

The first process run passed both post-kill audits but its premature fresh fetch
failed while the restored lease was active. The harness now waits for the original
lease expiry and checks that expiry did not advance the checkpoint; no lease or
handler deadline was weakened. Three subsequent real runs passed four audits, two
SIGKILLs and one dropped source response. The final rebuilt executable digest is
`996b91316f82d53cb1aff53d285d7445ce383f07b58440775908cf72b4e11928`.
Evidence remains in `/private/tmp/cellar-stream-process-faults-20261014-d`, with
logs, the independent JSONL ledger, private state and `summary.json`. The final
normal executable smoke also passed. Nine oracle guards reject missing client/
upstream acknowledgments, unissued operations, changed payload/envelope/producer
identity, duplicate acceptance/effects, early checkpoints, missing archives and
premature/misordered replay. Audit-only rechecks of both passing journals passed.
Final workspace all-target/all-feature Clippy passed with warnings denied;
11 workspace doctests passed with one existing ignored case. Formatting checked
420 Rust files; docs validated 35 documents, 6 flows and 370 references; the
all-feature dependency graph retained 401 packages without a JavaScript engine;
whitespace checks passed. No new complete workspace runtime gate is claimed.

This qualifies actual single-node process loss and source HTTP response loss
under persistent SQLite-bucket assumptions. It does not qualify source loss
before the first dispatch attempt, destination append/ack response loss,
separate producer/destination/consumer hosts, followers, network/store/power
faults, every commit/proof cut point, capacity soaks or the full release matrix.
Those requirements, complete physical storage/aggregate metrics, whole-fleet
inventory and guarded source destruction remain unfinished.


## Terminal destination evidence for source destruction

The catalog owner exposes exact-resource terminal evidence through a distinct
read-only platform RPC capability, also used by `cellar stream retirement-proof
--resource RESOURCE.json`. It checks the immutable UUID, namespace, family and
name against the retained deleted catalog row and its matching committed
retirement receipt. The response contains only scalar resource identity,
operation, digest, terminal state and accepted revision. It does not read or
return grant/configuration/payload bodies and cannot configure, publish, retire
or inspect another namespace through the same capability. Platform verification
continues after application and original administration grants are revoked.
All Cellar-owned markers remain 1; no migration or mixed-build reader is added.

This is a prerequisite, not an implementation of whole-source destruction.
Closing emission and serving authority, capturing/rechecking lane generation,
persisting verified evidence with an audited discard receipt, bounded cleanup,
retained source tombstones and fresh-incarnation recreation remain unfinished.
Destination watermarks are unchanged by this read-only operation.


The targeted runtime gate passed 99 library cases, 6 artifact cases, the persisted
stream fixture and 25 of 26 serial Node cases, including the extended catalog
proof test. That test rejects live-resource evidence, revokes the original
application/admin grants, verifies terminal evidence through the platform
capability, rejects the revoked resource status grant and compares the exact
proof after owner handoff. An unchanged stream-control case failed shutdown with
`NodeLeaseExpired`; its exact isolated replay passed. The cause remains unproven,
so this is not an uninterrupted full runtime pass. Initial CLI validation used
an earlier binary while sources changed; the rebuilt CLI then exposed a new
test's incorrect expectation of an error code for plain UUID validation. The
assertion now checks the actual specific resource-incarnation error.

Workspace all-target/all-feature Clippy passed with warnings denied. All 11
workspace doctests passed with one existing ignored case. Formatting covers
421 Rust files; docs check 35 documents, 6 flows and 370 references; the all-feature
dependency check retains 401 packages without a JavaScript engine. This evidence
does not qualify source destruction or the remaining complete release matrix.

The final rebuilt CLI passes both existing test cases, including missing/invalid/
oversized resource input and rejection of inventory/binding options before fleet
access. No runtime deadline or shutdown assertion was weakened.


## Source loss before the first dispatch attempt

`examples/durable-streams/before_send.py` now qualifies this previously missing
single-node cut point with the actual compiled executable. The opt-in source
request acquires the ordinary `block_concurrency_while` input gate before its
business/outbox transaction. It calls `db.sync`, then exposes the proven command
receipt and a lane with zero attempts and dispatch generation. The observer
fsyncs its independent command plan and boundary evidence, verifies no destination
record and kills the still-pending process. Neither the source HTTP reply nor a
client acknowledgment is fabricated; the command is proven but ambiguous to the
original HTTP caller.

Before any framework restart, the client copies the killed source's complete
SQLite family and reads only that forensic copy. The original business state,
source incarnation, next sequence, pending obligation and zero attempts are
verified after SIGKILL. The envelope digest is recorded before retry. SQLite/WAL
hashes authenticate the archived durable bytes; SHM read marks are mutable
bookkeeping and excluded. After restart, the routed source/destination/effect/
checkpoint snapshot is audited before source replay or consumer processing.
Dispatch may recover without client retry. The command replay must return the
original receipt as a duplicate; destination offset/event/producer/payload and
encoded-envelope identity match the killed image. The consumer creates exactly
one receipt/effect and checkpoint 1, and final preserving shutdown uses all
original drain assertions.

The initial real run passed the boundary, post-kill forensic audit, recovery and
final effect checks. The final real run also authenticates the saved database/WAL
in audit-only mode. Evidence is retained in
`/private/tmp/cellar-stream-before-send-20261014-b`; the executable digest is
`034c7cd9d1e124141ff9791d2ab3cef65604d59b9ab81d90e91894a55752a90a`.
Its final summary reports two runtime audits, one actual SIGKILL and one effect;
the forensic pre-restart audit is additional. Sixteen oracle tests pass, including
nonzero pre-send/post-kill attempts, lost source state, changed incarnation,
missing pending/accepted work, premature effects/checkpoints and replay-before-
audit refusals. The ordinary executable smoke passes. The prior process fault
scenario also passes on this exact binary, with four audits, two SIGKILLs and one
dropped source HTTP response; evidence remains in
`/private/tmp/cellar-stream-process-faults-20261014-e`.

This is actual source loss before first send under persistent single-node
SQLite-bucket assumptions. It does not qualify separate-host destination/ack
response loss, followers, network/store/power faults, every cut point, capacity
soaks or complete release readiness. Whole-source destruction remains unfinished;
its serving/admission lifetimes must fence suspended contexts and retained handles,
in addition to durable closure, bounded cleanup and tombstone recreation.


Final workspace all-target/all-feature Clippy passes with warnings denied; all
11 workspace doctests pass with one existing ignored case. Formatting checks
421 Rust files. Docs validate 35 documents, 6 flows and 370 references; the
all-feature dependency check retains 401 packages without a JavaScript engine;
whitespace checks pass. Both final measuring journals pass audit-only rechecks.
This is targeted qualification, not a new uninterrupted full runtime gate.


## Source closure, destruction and recreation

The source control path now accepts guarded destruction and recreation operations.
Destruction commits its exact request/digest/operator, captured source incarnation,
source admission generation, lane snapshot, current work generation and initial
receipt in one transaction. Dispatch generations advance to reject older replies.
Cached admission closes only after that transaction commits, before any await;
ordinary application constructors/handlers, SDK operations and suspended turns
cannot continue using the closed source. Retained storage/cursor/inbox/facet
handles keep their original source generation. Activation and instance reset
reconstruct the fence from durable state; recreation never revalidates old handles.

Framework controls and original outbox recovery remain admitted under private
host provenance. Each verification event makes at most one owner-routed retirement
proof call outside SQLite. Required evidence covers pending obligations and lanes
without an accepted terminal seal. Fully settled, accepted sealed lanes preserve
their live destination. Source incarnation, lane hash and captured work generation
are rechecked before recording proof. Every needed proof must commit before the
source can enter cleanup; verification evidence itself passes `db.sync` before
reclamation. Live destinations return `PendingDelivery` without discarded rows;
the accepted closure remains durable and can be inspected/retried independently
of its closed application. Drain/seal before destruction or retire every affected
destination through its independent administration authority.

Cleanup batches at most 1,000 event rows under a bounded encoded-byte budget,
then inbox receipts and lane/seal/batch-limit metadata in bounded batches. Business
storage uses the existing transactional reset, while the source administration
schema, immutable request identity and terminal receipt remain retained. The
receipt reports exact captured/discarded event counts and bytes and the original
reason/loss acknowledgment. Destination watermarks are not erased by source
cleanup. Recreation requires the exact deleted incarnation, retains the old
receipt, mints a fresh random producer incarnation and advances admission generation.
An old destruction replay returns its original terminal receipt without closing
that successor. CLI `outbox destroy/recreate` exposes this host path with explicit
confirmation flags; destruction additionally requires textual loss acknowledgment.
All Cellar-owned markers stay 1; prior control layouts require recreated state.

The two new Node cases cover a live destination refusal, closed admission across
owner handoff, independent destination retirement, exact discard counts, durable
receipt replay/conflict/operator checks and fresh identity/business-state reset.
The second case closes a sealed source with a live destination, drops an actual
suspended handler, rejects a retained storage handle after recreation in the same
activation and compares the unchanged live destination watermark. Its first run
exposed fixture ordering (seal before proven settlement), and the next exposed
missing required inventory parameters. The corrected fixture preserves every
original runtime deadline and assertion. A restore guard also refuses mismatched
work and missing retirement evidence before any base-schema mutation.

The first compiler pass found missing imports and an incremental Rust compiler
panic; compilation with incremental state disabled exposes normal diagnostics.
A later native reset branch needed explicit failure propagation because its
caller returns a turn tuple rather than `Result`. No cache cleanup, protocol
migration, proof bypass or deadline extension was used to hide those failures.

This implements and tests the base source database lifecycle. Independent
facet-family cleanup and recreation isolation, large SQL table-reset cost,
exhaustive source cleanup crash/lease/grant/policy races and aggregate/physical
capacity qualification remain unfinished. Full design completion is not claimed.


The larger targeted run passed all 28 serial Node cases, 6 artifact cases,
2 CLI cases, the persisted stream fixture, the native facet fixture and the
native outbox SDK fixture. The new restore fixture initially initialized an
event schema outside a transaction; the corrected library replay passes all
100 cases, including both pre-mutation refusal checks. The dependency-free
source admission test passes; all 11 workspace doctests pass with one existing
ignored case. Workspace all-target/all-feature Clippy passes with warnings
denied. Formatting covers 422 Rust files; docs validate 35 documents, 6 flows
and 370 references; dependencies retain 401 packages without a JavaScript engine;
whitespace checks pass.

The first attempt at these final checks failed with no space on the private
build volume. Only this checkout's disposable incremental caches were removed;
source, binaries, snapshots and other checkouts were preserved. Repeated checks
ran with incremental compilation disabled. These are targeted gates and corrected
replays, not a new uninterrupted full workspace runtime pass or completion of
the remaining source/fleet/capacity release matrix.


Final Clippy and formatting checks also pass after the restore-fixture correction.
The rebuilt ordinary durable-stream smoke passes. The real before-first-send
SIGKILL case passes again on this binary, with two recovery/final audits, one
kill and one effect. Its digest is
`1b8426e420b3d383d928522fbe3d056ff35dd56e5e92cd2236d8f77bfb15c7a4`;
evidence remains in `/private/tmp/cellar-source-lifecycle-before-send-20261014`.
This requalifies ordinary generation-zero SDK behavior and that cut point; it is
not source-destruction crash injection or independent facet-family qualification.


## Independent source facet-family reclamation and isolation

Top-level facet storage names now register in `__sys_cell_facet_names` before an
SDK open. The registry follows the source database's proof/recovery, survives
ordinary business reset and records children even when they are absent from the
current process. Source cleanup reads one registry key, proves the closed source,
then requests subtree deletion outside SQLite. The original source/operation/work
snapshot is rechecked before removing that key; interrupted/partial cleanup keeps
it available for recovery. The terminal source receipt is recorded only after
all registered families have completed.

The host serializes cleanup with begun opens through the root's opening lock and
checks resident owner epoch before deletion. Each batch lists at most 101 replica
objects and deletes at most 100 from one exact facet subtree. Local replicas and
dirty tails are closed/credited through the original backend deletion path; the
root database, its proof/receipt and sibling streams are preserved. Existing
ordinary facet deletion retains its API. Local directory removal remains a
separate filesystem cost requiring capacity qualification.

Recreated sources derive physical facet names from producer incarnation and the
logical user name. Public logical names, startup IDs and properties stay intact.
A delayed cleanup cannot target a successor's differently qualified namespace.
Native startup carries a captured source admission check into the host, including
after waits and restore; a stale parent cannot materialize a child. The permit
uses an opaque validator to avoid a recursive concrete channel/activation type.
Ordinary activation retirement retains its existing diagnostic wording, separately
from source-incarnation refusal. All owned markers stay 1; earlier outbox layouts
missing the registry are refused before base-schema mutation and require recreated
state, without migration.

The real Node source tests now write both child and nested-child state before
closure. The warm test verifies old local family removal and zero child values
after same-activation source recreation. The owner-handoff case verifies that a
new owner reclaims a registered child it never opened and that neither child nor
nested state reappears after recreation. Original handle, pending-turn, source
identity, loss-count, receipt and live-destination-watermark assertions remain.
A backend test seeds 205 replica objects and verifies remaining counts 105, 5 and
0 across three batches, while root/sibling/other-root objects retain their exact
bytes. A delayed native startup test advances source admission and verifies its
child file remains unopened.

Early checks caught a shared-table initialization guard error, a borrowed request
in the fixture and incorrect durable-namespace declaration of a facet class.
The bootstrap regression caught changed activation-retirement wording; the
original assertion/message is preserved rather than weakened. No deadlines,
ownership/proof checks or shutdown assertions were loosened.

The targeted runtime gate passes all 28 serial Node cases, 6 artifact cases,
2 CLI cases and the stream, native facet and native outbox fixtures. The corrected
library replay passes 101 cases, including bounded replica cleanup. Workspace
all-target/all-feature Clippy passes with warnings denied, and all 11 workspace
doctests pass with one existing ignored case. This is targeted source/facet
qualification, not completion of cleanup crash/lease/policy races, large local
filesystem/SQL-reset capacity, the full workspace runtime gate or the complete
outbox/stream/subscription release matrix.

Final library qualification includes the delayed source-startup refusal and passes
all 101 cases. Final Clippy passes with warnings denied; formatting checks 422
Rust files. Docs validate 35 documents, 6 flows and 370 references; dependencies
retain 401 packages without a JavaScript engine; whitespace checks pass.


## Interrupted source facet cleanup and explicit resumption

An authenticated replay of an unfinished destruction now re-arms the original
source's durable Outbox work before passing proof. Background cleanup failures
retain their five-minute outage backoff, but a deliberate operator replay can
resume after service returns. The original operation/digest/operator and source
incarnation checks still precede this mutation. A terminal replay returns its
retained receipt without scheduling work against a successor.

The serial Node lifecycle case injects a replica client that actually deletes
facet objects in the persistent SQLite bucket, records their exact paths in an
independent test ledger, then loses the deletion replies. Further facet listings
fail until the interrupted owner fully shuts down. Before replacement startup,
a separate caller-owned execution domain verifies every recorded object is absent.
The source remains `cleaning` with exact captured/discarded rows and bytes;
business calls and premature recreation are refused. The replacement has fresh
local state and no bindings to the retired destination. It reads the recovered
closure/counts before operator replay, completes cleanup, then proves root/child/
nested-child business state is empty after recreation. Original loss, ownership,
operator conflict, receipt replay and complete final shutdown assertions remain.

The case exposed that replay previously returned a nonterminal receipt without
advancing its durable outage deadline. The fixture also caught attempted graph
cloning, a direct bucket audit outside its execution domain and unsupported
adoption of a retired destination binding. These were corrected without changing
runtime deadlines, retirement adoption checks or shutdown expectations. This is
injected replica response-loss/outage plus graceful owner replacement evidence;
it is not SIGKILL, power-loss, expired-lease or isolated-owner qualification.
The focused case and all 28 serial Node cases pass (208.51 seconds for the
complete Node run). Workspace all-target/all-feature Clippy passes with warnings
denied; formatting checks 423 Rust files. Docs validate 35 documents, 6 flows
and 370 references; dependencies retain 401 packages without a JavaScript engine,
and whitespace checks pass.


## Bounded local facet reclamation

Source facet reclamation no longer invokes recursive `remove_dir_all` on an
async host turn. The injected `FileSystem::remove_dir_batch(path, limit)` contract
removes at most the requested number of files/directories and reports completion
only when the root is absent. The direct provider streams one directory entry
at a time, keeps one traversal path, closes each iterator before descending and
unlinks symlinks without following them. It can resume from the same root after
process-local state is lost. A zero budget is refused; an absent root completes
idempotently; other filesystem errors propagate. Custom providers default to an
explicit unsupported error and must implement the contract to reclaim sources;
there is no unbounded fallback.

The replication backend completes its at-most-100-object remote sweep before
admitting at-most-100-path local batches on the owning blocking executor. The
nonreplicated host deletion path uses the same injected method. Executor refusal
propagates without panic. Incomplete local work returns false, so the original
source registry key and durable cleanup obligation remain retained. Existing
ordinary facet deletion keeps its separate API and behavior.

Tests seed 205 remote objects and 205 local files under the same closed subtree.
Remote counts 105, 5, 0 are followed by local counts 105, 5 and complete removal;
terminal completion is withheld through incomplete local batches. Exact remote
root/sibling/other-root and local root bytes remain unchanged. A nested-tree test
reconstructs the direct provider each turn, independently inventories removals
and asserts no batch exceeds 17 entries, including directories. It verifies
symlink targets outside the subtree survive and root symlinks are unlinked.
Final qualification passes 102 library cases, both serial source lifecycle cases,
workspace/all-target/all-feature Clippy with warnings denied, 423-file formatting,
11 doctests with one existing ignored case, docs (35/6/370), dependencies (401,
no JavaScript engine) and whitespace checks. Unix symlink behavior is exercised;
the Windows-specific directory-symlink unlink branch follows the installed Rust
standard library contract but has not been executed on Windows. These
removal-count/memory bounds do not prove a
wall-time or byte-I/O bound for arbitrary directory depth, filesystem latency,
large individual unlink or SQLite business table reset. Capacity and abrupt-loss
qualification remain required.


## Proven destination append reply loss across embedded nodes

The Node suite now runs destination, producer and consumer as independently
started nodes with distinct data directories/execution domains over the private
persistent SQLite bucket. Source/consumer factory probes confirm business state
materializes on their intended hosts; the destination materializes neither. It
owns the warmed stream before the producer starts. Its advertised private peer
address points to a loopback TCP proxy forwarding unmodified HTTP/1 establishment
and tunneled request/response framing, including the original authenticated
handshake. No production receiver, output proof, routing or ownership gate is
replaced.

The proxy captures the complete successful append reply after the real destination
output gate. Before closing sockets, an independent measuring path verifies the
acknowledged command state, known `{value:1}` payload, exact source incarnation,
sequence 1/next sequence 2, one pending source row with settled prefix 0, and one
destination offset whose full envelope matches the captured request. Only then
is the loss released. Framework retry sends identical encoded request bytes and
receives identical acceptance bytes for the same resource/lane/accepted prefix.
Destination records remain exactly unchanged. A separate consumer commits/proves
an inbox effect, explicitly retries its batch, then redelivers/acknowledges with
one effect and checkpoint 1. All three nodes must complete original preserving
shutdown, and the proxy joins every connection. Runtime deadlines remain intact.

The focused case and all 29 serial Node cases pass (212.63 seconds for the full
run). Workspace/all-target/all-feature Clippy passes with warnings denied;
formatting checks 424 Rust files, docs validate 35 documents/6 flows/370 references,
dependencies retain 401 packages without a JavaScript engine and whitespace
checks pass.
This is actual peer append response loss across embedded nodes in one test
process, with in-process measurement snapshots and a persistent SQLite bucket.
It does not qualify independent physical hosts/process failures, saved fsynced
measuring ledgers, destination ack response loss, follower loss, power loss or
all other commit/proof cut points. Those release requirements remain open.


## Proven subscription acknowledgment reply loss

The real peer reply proxy now selects an armed RPC method and records method,
exact scope and opaque request/response bytes. Its helper was renamed to
`peer_reply_fault.rs`; it continues forwarding the original authenticated tunnel
and framing. Capturing an append does not consume an armed acknowledgment fault,
and vice versa. The original append-loss assertions are retained in the combined
three-node case.

After one proven inbox effect and explicit batch retry, the consumer redelivers
without repeating the effect. In that same inbox transaction it saves the exact
new batch receipt, then `db.sync` proves the receipt/effect before acknowledgment.
The proxy captures a complete successful `acknowledged` response from the real
destination. Independent status reads verify checkpoint 1 and no live lease
while that reply is held. Only then does the proxy close sockets. The consumer
reports reply loss or the transport's safe `AlreadyAcknowledged` replay; a
premature successful acknowledgment is refused by the test boundary.

Before explicitly replaying the receipt, the measuring client inspects the
retained processed total. `/ack-stored` uses the SDK's public subscription ack
method with the persisted original receipt, adding no local effect or new batch
fetch. Captured replay request bytes match exactly, including subscription UUID,
incarnation, generation, token and range; the reply is `AlreadyAcknowledged`.
A subsequent empty processing call retains one effect and checkpoint 1. Original
independent-host factory probes, exact append acceptance, runtime deadlines,
complete node shutdown and joined proxy connection assertions remain intact.
A compile check caught the held HTTP future retaining the consumer through its
shutdown move; explicit future completion/drop fixes that test ownership.

The focused combined case and all 29 serial Node cases pass (216.22 seconds
for the full run). Workspace/all-target/all-feature Clippy passes with warnings
denied; formatting checks 424 Rust files, docs validate 35 documents/6 flows/370
references, dependencies retain 401 packages without a JavaScript engine and
whitespace checks pass. This is actual append/ack network response-loss evidence between embedded
nodes over a persistent SQLite bucket, with in-process measurement snapshots.
It does not complete independent process/physical-host failures, fsynced saved
measuring ledgers, follower/isolated-owner/expired-lease/power loss, all commit/proof
cut points, capacity or the full workspace release gate.


## Refreshed executable fault evidence on f46b878

A locked all-feature build of `cellar-example-streams` at source revision
`f46b8788c0baa9632d641f90b682b8da026e66da` produces executable SHA-256
`c384539175bdeca45bb3b74eafe3b6d502dc0e623f431a0bf21594e9b245dfa1`.
The unchanged ordinary smoke and both process-fault drivers pass on that exact
binary. The process driver observes source HTTP response loss and two SIGKILLs
(pending acknowledged source obligation; proven consumer effect before ack),
with four independent saved audits, preserved original lease expiry, two distinct
final effects/checkpoint 2 and complete final preserving shutdown. The first-send
driver observes a proven but HTTP-unacknowledged source SIGKILL with zero attempts,
audits the killed SQLite/WAL image before restart and the recovered source before
replay, and finishes with one effect/checkpoint 1. Both audit-only commands recheck
the saved ledgers/image; all 16 oracle guards pass.

Private evidence is retained at
`/private/var/folders/rn/k2b7mvbs2dgb__6ys6_h94pm0000gn/T/cellar-stream-current-evidence-aylyo6uy`.
`qualification.json` records the source revision, build command, binary digest,
ledger hashes and exact scope; each driver retains its own summary, logs, private
state and ledger. This refresh follows the bounded local cleanup and current
append/ack embedded-node test changes. It is single-node persistent SQLite-bucket
process evidence, not independent physical hosts, followers, power loss, source
cleanup loss, arbitrary commit/proof cut points or capacity qualification.

The workspace feature-unified build subsequently produced SHA-256
`8d94fc339e1d29d8c9755ce1225a810fb7c4061c5993fc12a70b9e79b9acdaf0`.
That image is archived as `workspace-image/cellar-example-streams` inside the
same private evidence directory. Ordinary smoke, both actual process drivers
and both saved-ledger audits were rerun successfully against the archived image;
`workspace-process-faults` and `workspace-before-send` retain their fresh evidence.
The manifest keeps both build variants distinct rather than assigning earlier
fault results to an overwritten Cargo target file.

The first serial full-workspace library/integration gate passed the library and
native integration cases through the Cargo dev workflow, then stopped on two
loader-policy cases with `no output-gate channel`. Their runtime-only host never
installed a gate adapter because their parent previously made no storage writes;
durable facet-name registration now writes parent state. The fixture now serves
its per-domain gate contract like the neighboring native loader/cell fixtures,
tracks invocation and verifies facet registration actually reaches it. Its
runtime-only responder is not fleet durability evidence; production gate code
and all budget/domain/shutdown assertions are unchanged. The focused native
loader and all six loader-policy cases pass, including the invalid-environment
subprocess case. The corrected serial full-workspace library/integration gate passes: 99 test
reports, 391 passed cases, no failures or ignored cases. This includes all 29
Node cases and the actual Cargo dev workflow, plus the remaining SDK/workspace
targets. `workspace-tests.log` and its digest are retained in the private evidence
manifest. Workspace/all-target/all-feature Clippy passes with warnings denied,
11 workspace doctests pass with one existing ignored case, formatting checks
424 Rust files and documentation validates 35 documents/6 flows/370 references.
All 16 fault oracle tests and saved-ledger re-audits pass; whitespace is clean.
Full-workspace test success does not close the independent-host/follower/power/capacity or full
commit/proof qualification matrix.


## Name-selected deployment bindings with pinned delivery identities

`BindingConfig`/`ResourceConfig` now separate deployment selectors from runtime
`Binding`/`Resource`. Config JSON may omit `resource.id`; explicit IDs preserve
their serialized shape and require exact matching. Counts, fields, grant subsets
and a 64 KiB declaration encoding cap validate before worker construction.
Programmatic configuration call sites convert resolved bindings with `.into()`.
Stored lane and event formats retain mandatory UUIDs; all markers stay 1.

A private `event-name-resolution-v1` authority is bound to one catalog namespace,
selector, environment, principal and grant and permits only `__catalogResolveName`.
It reads one indexed nondeleted name row, rejects nonactive state, and checks the
actual durable grant. No provisioning/mutation capability is added. Requested
permissions live in a bounded RPC body (128 KiB including framing, 64 KiB decoded
JSON), not in the 16 KiB authority header; body identity must match the header.
The existing exact catalog/partition identity and grant checks repeat afterward,
closing lookup/recreation/revocation races through UUID checks. Only all-successful
resolution installs a once-pinned generation map before admission. A failed
candidate preserves the serving generation; reactivation cannot retarget its map.

Context stream handles clone resolved bindings. Emit/seal checks compare current
resolved identity, never a name wildcard, so retained old handles cannot publish
to a successor. Background obligations keep immutable stored binding identities.
The two-node case resolves external names, proves delivery, rejects missing names,
missing/revoked grants and wrong families, preserves serving code after failures,
then retires/recreates the name under another UUID. A new generation resolves the
successor while the retained old handle stays on its original UUID and emission
is refused; new delivery reaches the successor with exact value 2. Original node
shutdown and runtime deadlines remain. The fixture retirement request was fixed
to use the catalog's observed revision 3 rather than an invented successor 4.
Unit guards cover namespace/method scope, no authority promotion, maximum-size
subscription declarations without oversized headers, and once-pinned UUIDs.
Artifact metadata checks cover omitted versus malformed IDs.

The focused real-node case and complete serial workspace gate pass: 394 cases
across 99 reports, including 30 Node cases. After Clippy caught implementations
following test modules, item order was corrected without behavior/assertion changes;
final library replay passes 104 cases and Clippy-D passes for workspace/all-targets/
all-features. Formatting checks 426 Rust files; 11 doctests pass (one existing
ignored), docs validate 35/6/370, dependencies retain 401 packages without a JS
engine, 16 oracle guards and whitespace checks pass. Ordinary smoke and both real
process-fault drivers pass on archived workspace-image SHA-256
`83fb2875e434f1e47ceadf37a645230a65bb4ccd41d3c6f77d5dec3720fbb1f9`,
with saved-ledger audits passing. Private evidence is retained at
`/private/var/folders/rn/k2b7mvbs2dgb__6ys6_h94pm0000gn/T/cellar-named-binding-evidence-1baheykw`
with a manifest identifying the item-order-only correction after image qualification. Wider name lookup/revocation/adoption fault races and the existing full
process/follower/ownership/capacity release matrix remain unqualified.


## Independent-process destination append loss and recovery

The standalone stream executable has an opt-in bounded `--fleet-fixture FILE`
mode with unique node identity, shared private bucket path, loopback peer/advertise
addresses and explicit resource-declaration role. Producer, destination and
consumer run in separate processes/directories. Their factory-owned diagnostics
prove where source and effect state are actually hosted. Ordinary configuration,
all owned markers (1), original operation/handler/lease policies and complete
shutdown assertions remain intact.

The new `fleet_faults.py` forwards the real authenticated HTTP/1 tunnel and framing,
then holds a complete successful append reply. Its fsynced measuring ledger records
the original command, acknowledgment, payload/envelope identity and raw peer-byte
digests. Before observed destination SIGKILL, it audits one pending source row and
exactly one proven destination acceptance. Automatic append attempts cannot reach
the restarted destination until a separate recovery audit verifies that acceptance
survived. Retry sends identical request bytes and returns identical acceptance;
source settlement and consumer inbox redelivery finish with one effect/checkpoint.
Original/replacement process IDs, actual kill exit, stage order and each complete
final shutdown are required by saved-evidence audit.

The first run reached successful destination recovery/settlement but its first
consumer request hit a stale pooled RPC connection. The harness now audits effects
and checkpoint before repeating an ambiguous consumer operation; if a lease was
installed, it waits for that original expiry. Expected connection refusal is
recorded only during the deliberate outage/teardown. No protocol deadlines or
proof gates were relaxed. A subsequent run passes with one interruption audit.
Ten new oracle guards reject mutated original commands, lost proven acceptance,
retry-before-audit, wrong/foreign process identity, changed captured bytes,
missing reply loss, premature recovery/shutdown, duplicate effects and incomplete
final shutdown. All 26 existing/new Python oracle cases pass. Final diagnostic
pattern matching replaces an unnecessary unwrap; ordinary routes retain their
original response fields outside fault mode. Final Clippy-D passes for workspace/
all-target/all-feature, 11 doctests pass with one existing ignored case, formatting
checks 426 Rust files, docs validate 35/6/370, dependencies retain 401 packages
without a JavaScript engine and whitespace checks pass. The SDK production core
is unchanged from the preceding 394-case workspace gate; this batch qualifies the
fixture and measuring client directly rather than claiming another workspace run.

On archived executable SHA-256
`9fe1d1bb1dff2691f507e366440123bed4b2dc8b3fba1da6bdc51113ed279a76`,
the fresh independent-process case passes with five saved audits, three initial
processes, one replacement destination, one observed SIGKILL/reply drop and one
identical append retry. Ordinary smoke, both earlier process-fault drivers and
all three audit-only commands pass on the same image. Private evidence, binary,
ledger hashes, logs, source patch and scope manifest are retained at
`/private/var/folders/rn/k2b7mvbs2dgb__6ys6_h94pm0000gn/T/cellar-independent-stream-parent-r5j0ub8t`.
The final surviving producer/consumer and replacement destination have distinct
complete preserving-shutdown reports.

This is independent-process evidence on one machine using a persistent SQLite
bucket. It is not separate physical hosts, network object storage, follower loss,
destination acknowledgment SIGKILL, power loss, source cleanup loss or complete
capacity/release qualification.

## Network-store destination recovery (2026-10-14)

The opt-in standalone fleet fixture accepts either its existing SQLite bucket
path or a bounded loopback S3 endpoint/name. The application explicitly builds
ordinary, paginated and no-retry conditional ownership clients. Test credentials
are captured only at this process boundary and never saved in fixture configs.
No production API, deadline, proof gate, schema or version marker changes.

`fleet_faults.py --rustfs-image IMAGE` owns a fresh Docker RustFS server, random
loopback port, random credentials and new bucket. Its ledger records backend,
actual image ID and server/bucket identity without credentials. Cleanup removes
only that container after the original producer/consumer and replacement
destination have returned complete preserving shutdown. The saved oracle rejects
missing, foreign or premature store cleanup. Its 27 guards pass.

The first attempt stopped during setup because RustFS health preceded S3 bucket
readiness (HTTP 503). Its partial evidence is retained; it is not a passed run.
The driver now uses the established fleet harness's bounded startup-only 503
retry. This does not change node ownership/replication timeouts or conditional
write retries. A fresh run passes against RustFS image
`sha256:040304b66e029a5cde4bed140b41513e925909839a9b912a40a98340610d1f66`.
Four saved independent audits prove one accepted event before destination
SIGKILL and after recovery, before allowing automatic append retry. Original
request/reply bytes match; source settlement, one inbox effect and checkpoint 1
survive. Actual process IDs, reply drop, replacement startup and all final
shutdowns are checked again by audit-only mode.

Archived executable SHA-256
`51c2e4e1c25a60a13553b80b9730c1113b3efdc2737796cea9a57d3e4f2b9889`
and private evidence are retained at
`/private/var/folders/rn/k2b7mvbs2dgb__6ys6_h94pm0000gn/T/cellar-stream-network-evidence-cfq3dp2e`.
Ordinary smoke and the default SQLite fleet case pass on the same executable;
the latter has five saved audits, including one consumer interruption audit.
Both backend ledgers pass independent audit-only rechecks. Focused example all-feature
Clippy-D passes; documentation validates 35 documents/6 flows/370 references,
dependency checks retain 401 packages without a JavaScript engine and whitespace
checks pass. The previous 394-case workspace gate applies to the unchanged core;
this batch exercises the changed host and measuring harness directly.

This qualifies one separate-process destination append/reply-loss cut point
using a network object store on the same machine. It does not establish
follower-only proof, store loss, whole-fleet recovery without local data,
physical-host/power loss, acknowledgment SIGKILL, source cleanup loss, every
commit/proof cut point or capacity/soak qualification.


## Whole-fleet bucket recovery after local data loss (2026-10-14)

The opt-in standalone fleet configuration now captures `fleet_durability`
explicitly. Ordinary walkthrough and the existing append-loss driver retain
fleet mode; `whole_fleet.py` selects bucket mode on every initial/replacement
node. No SDK production core, deadline, proof gate or version marker changes.
The fixture JSON is internal and is regenerated on cutover.

The new case starts independent destination/producer/consumer processes. It
proves one accepted command and a retained consumer inbox effect, releases its
batch without acknowledgment and commits a second source command blocked by
required retention. A fsynced independent audit verifies both original source
receipts, pending accounting, immutable accepted event/effect and checkpoint 0.
Only then does the driver observe all three SIGKILL exits, inventory the stopped
local files and remove the three private node data directories. Store data,
source commands, measuring ledger and logs remain outside the discarded paths.

Replacement nodes recover the catalog/stream, business source/outbox state and
consumer inbox from the bucket. Before any source POST or consumer processing,
a recovery audit requires the same receipts/event/effect, checkpoint 0 and one
pending obligation. Source commands are never replayed in this case. Consumer
redelivery must add zero effects; ack releases capacity for automatic pending
source dispatch. Subsequent processing adds one new effect, final total 1100
across two receipts and checkpoint 2. Every replacement returns complete
preserving shutdown.

Both RustFS S3 and SQLite bucket modes pass four saved independent audits, three
actual SIGKILLs and three local node-directory discards. Audit-only rechecks pass.
The eight new oracle guards reject incomplete/foreign process loss, premature or
missing local discard/replacement, replay before audit, repeated effects, lost
accepted event/effect/pending source, mutated commands, incomplete shutdown,
wrong proof mode and premature/missing private store cleanup. All 35 original/new
Python guards pass. The existing fleet-mode append/reply-loss case passes five
audits on this image, and ordinary smoke passes. Focused all-feature example
Clippy-D passes, formatting checks 426 Rust files, docs validate 35/6/370,
dependencies remain 401 without a JavaScript engine, and whitespace checks pass.
Production core is unchanged from the prior 394-case full workspace gate; no
new complete-workspace run is claimed for this fixture/harness batch.

Archived binary SHA-256
`b813cd9d2e9451a2811f44ee801e49706cacb080da73fbaa9ab62a20dc276068`,
ledgers, file inventories, logs and source/configuration manifest remain at
`/private/var/folders/rn/k2b7mvbs2dgb__6ys6_h94pm0000gn/T/cellar-whole-fleet-evidence-oapnsvzs`.

This proves bucket-only whole-fleet recovery with lost local node data at one
selected boundary on one machine. The object store remains alive and both
caller executors in killed nodes are lost abruptly. This does not prove fleet
follower-only acknowledgments/recovery, object-store/power loss, separate
physical hosts, every commit/proof cut point, independent ack SIGKILL, source
cleanup loss or full lifecycle/capacity/soak qualification.


## Independent-process proven acknowledgment loss (2026-10-14)

The standalone opt-in consumer now stores an encoded exact batch receipt in
its effect/inbox transaction and syncs before remote ack. Its test diagnostics
expose only the receipt digest; a separate replay route decodes the stored
receipt and calls the SDK acknowledgment API without fetching another batch.
Ordinary routes, SDK production core, all proof/deadline/lease policies and
owned markers (1) remain unchanged. The existing real peer proxy can now target
append or ack while retaining authenticated tunnel/framing and complete reply
capture.

`ack_faults.py` starts independent destination/producer/consumer processes.
After source acceptance settles and is audited, the client holds the complete
ack reply and audits checkpoint 1, no live lease, the proven consumer effect and
receipt digest before destination SIGKILL. It drops the original socket reply;
the consumer records one new effect and an actual lost-ack outcome. Replacement
destination startup precedes a recovery audit of the same checkpoint/effect/
receipt. Only then does it release the proxy and replay the stored receipt.
Captured request bytes match the original ack; the SDK returns
`AlreadyAcknowledged`, with no new fetch, effect or checkpoint advance.

The first RustFS attempt recovered all audited state but then hit a stale pooled
RPC connection during exact replay. Its partial evidence is retained and excluded
from passed qualification. The measuring client now records transport ambiguity
and audits the unchanged checkpoint/effect/receipt again before retrying the same
stored receipt. Original deadlines and lease policies remain intact. A synthetic
corruption test also exposed shared objects between its fabricated stage rows;
the fixture now copies each snapshot independently so changing a recovery digest
cannot change its earlier evidence.

Fresh SQLite and RustFS S3 cases pass four and five saved audits respectively
(the RustFS case includes one replay-interruption audit), each with observed
destination SIGKILL/reply loss, one identical receipt replay and complete final
shutdown of all surviving/replacement processes. Audit-only rechecks pass.
Nine new oracle guards reject receipt/RPC substitution, checkpoint/lease/effect
loss, missing/wrong process or original reply loss, premature replay/restart,
mutated stored receipt/captured bytes, wrong idempotent status, duplicate effects,
incomplete shutdown, missing/premature private store cleanup and unaudited replay
ambiguity. All 44 original/new Python guards pass.

On archived executable SHA-256
`f521ac787753620af084f9957f73af174700f1889f80ff897777c54b061ecda2`,
ordinary smoke plus the previous fleet-mode append-loss (five audits) and whole-
fleet SQLite bucket-loss (four audits/three SIGKILLs/three local discards) cases
pass with saved re-audits. Focused example all-feature Clippy-D passes; formatting
checks 426 Rust files, docs validate 35/6/370, dependencies remain 401 without a
JavaScript engine and whitespace checks pass. Core is unchanged from the prior
394-case workspace gate; no new full-workspace run is claimed for this batch.
Private image, ledgers, logs, source patch and qualification manifest are retained
at `/private/var/folders/rn/k2b7mvbs2dgb__6ys6_h94pm0000gn/T/cellar-independent-ack-evidence-_aueu9u7`.

This qualifies one independent-process ack-proof/destination-loss boundary on
one machine with SQLite/RustFS storage. It does not qualify consumer process
loss while replaying a receipt, follower-only proof, object-store/power loss,
physical hosts, all lease/ack or source-cleanup cuts, full policy/lifecycle races
or capacity/soak release requirements.


## Bounded root-source fleet census (2026-10-14)

`cellar outbox census [CLASS]` walks the existing durable bucket cell-directory
index, then performs authenticated owner-routed source inspection. It needs no
new registration publication or state schema. Each call asks for one provider
page of at most 100 prefixes and runs at most four concurrent source probes in
key order. Each inspection requests one lane, validates its complete owner page
and projects only source identity, total pending event rows/bytes and fixed
observations. No lane/principal/grant/payload/receipt metadata is returned.

The `census1` continuation retains an opaque provider token, bound by a digest
to backend, bucket/prefix, endpoint and optional class. Input/decoded provider
continuations are bounded and canonical; malformed/foreign tokens fail before
listing/probing. Filtering cannot trigger more provider pages, so an empty page
may still have a continuation. Only stream/catalog classes are excluded; other
framework roots remain candidates. A current source, no current outbox identity
and an unavailable observation have distinct row states. Failed probes remain
explicit rows; the CLI prints the usable page/cursor, sets `complete_probes: false`
and returns a failed exit. It cannot substitute zero counts for unobserved data.

The actual two-process RustFS CLI smoke qualifies three one-prefix pages over
a settled source, a capacity-blocked pending source absent from destination
`stream sources`, and an empty cell. It requires matching original incarnation/
receipt scope and pending counts, no duplicate source or lane metadata, and
foreign-class cursor rejection. After complete node shutdown with the bucket
retained, a new CLI call must return all three unavailable rows without pending
counts, false complete-probe flag and a failed exit. The normal output/proof,
owner route and authorization are reused; no business SQLite is read or mutated
directly from the bucket. Cold inspection may restore/progress durable work.

All 106 library cases and both native CLI integration cases pass, including
bounds/class/cursor guards and help/input rejection. The first focused command
used the physical file name as its Rust test filter and matched zero tests; the
correct logical `outbox_cli::census` filter then ran/passed both guards, and the
full library replay passed. Workspace/all-target/all-feature Clippy-D, 427-file
formatting, docs (35/6/370), dependencies (401/no JavaScript engine), 44 Python
fault oracle guards and whitespace checks pass. This does not claim another
complete workspace integration/fault run. The build briefly filled the mounted
target volume; archived tested images were preserved, and only obsolete Oct-13
Cellar library/rmeta cache files in this checkout's target were removed. No
source/evidence/current binaries or other checkout caches were removed.

Private paired CLI/example images, fixture/ledger/logs, source patch and manifest
remain at `/private/var/folders/rn/k2b7mvbs2dgb__6ys6_h94pm0000gn/T/cellar-census-evidence-dr58nnxt`.
CLI SHA-256: `e2b880f98faabdbedce23f066a641e398188d45778ed4de9b19bb6ea1584b067`.
Example SHA-256: `feb746e3e580b472e79bc5b2d82392456fd1671843513cf5ec07cb0f53e372fb`.
All owned markers remain 1.

This supplies live root-source census for undispatched producers. It is not a
snapshot, all-producer drain proof, facet/historical source or catalog namespace
inventory, nor aggregate authoritative gauges. Other cloud backends, removed-
class/cold/facet coverage, ownership/listing races, scale/latency and physical
accounting remain qualification/implementation work.


## Quiescent protected retention maintenance (2026-10-14)

Previously, the oldest retained append time always selected an age deadline,
including when a required checkpoint prevented deletion. Once that age elapsed,
maintenance rearmed at the existing one-second minimum forever without possible
progress. Scheduling now reads one oldest primary-key record and compares its
offset against the same required-checkpoint/live-lease deletion frontier used by
sweeping. Protected prefixes have no age wake; lease cleanup keeps its expiry
wake. Checkpoint/retry/lease/lifecycle transitions already recompute work, and
accepted configuration now recomputes its deadline in that configuration's
transaction so changed retention or age limits cannot leave eligible work dormant.

The frontier reads the indexed minimum required checkpoint and at most 129 live
leases, rejecting more than the current 128-subscription bound. Scheduling clamps
wall time to durable logical time. Checked age-deadline overflow refusal remains
active even with a protected prefix, avoiding acceptance that could become
unacknowledgeable when protection lifts. Eligible expired records continue in
bounded row/byte sweeps with the existing one-second minimum; clearing this
reason leaves application alarms and other work intact. No schema, protocol,
version marker, source/destination identity or durability gate changes.

Four new production-SQL tests use a synthetic activation, not a fleet proof
adapter. They require no periodic work for a protected required prefix, progress-
triggered rearming, one-record bounded continuation, retained application alarm,
live/expired lease handling, configuration rollback/release, durable-clock clamp
and protected deadline-overflow refusal. All 110 library cases pass. The native
stream fixture now checks that a live lease's expiry is its only deadline and a
paused required checkpoint has no age deadline; it retains protection/age, receipt,
reclamation, authorization and lifecycle assertions. That integration case passes.

The first serial Node run passed 29/30 but cancelled bootstrap hit its original
five-second deadline and a worker panicked with `DomainClosed`. Its selected
original diagnostic is retained separately (not misrepresented as a complete raw
log). One isolated replay and five repeats pass without changes. A full replay
with backtraces passes all 30 cases in 256.57s. No cancellation fix, relaxed budget
or established root cause is claimed; investigate this observed failure before
closing release qualification. Event delivery/ownership/lifecycle cases passed
in both runs. Workspace/all-target/all-feature Clippy-D, 11 doctests (one existing ignored),
428-file formatting, docs (35/6/370), dependencies (401/no JavaScript engine),
44 fault oracle guards and whitespace checks pass. This batch does not claim another complete-workspace integration gate.

On archived CLI/example images, the real RustFS whole-fleet case passes four
audits, three SIGKILLs and three discarded local node directories. Independent
SQLite ack loss passes four audits, destination SIGKILL/reply drop and identical
stored receipt replay. Both saved audit-only checks pass. Real RustFS census
passes three one-prefix pages, undispatched/empty/failed-probe cases and scoped
cursor refusal. Ordinary smoke passes. Private evidence, images, initial failure,
isolated/full replay logs, source patch and manifest are retained at
`/private/var/folders/rn/k2b7mvbs2dgb__6ys6_h94pm0000gn/T/cellar-retention-scheduling-evidence-4mi5bznc`.
Example SHA-256: `1c312594d0016938fd06fbdaff78cbf2ccd352bf556dea6573596f1fdb815530`.
CLI SHA-256: `d86d84fa4d574742fd2e58c8a6dc4be815935bf0a77f681ea7c60415e5bac6cd`.

This removes periodic age work that cannot reclaim a protected prefix. It does
not qualify hot-stream/large-metadata/wake fairness, complete clock/lease/config
races, follower/power/store/physical-host loss or the remaining capacity/soak
matrix. Aggregate/physical metrics and wider census remain implementation work.


## Fallible bootstrap/adoption work admission (2026-10-14)

Twenty more isolated original cancellation cases passed. The intermittent
five-second timeout remains unreproduced and its cause unestablished. The
public-node test now records an independent directory-lock probe before each
replacement attempt and labels failure as an old held directory or pending
replacement bootstrap. A diagnostic lock is released immediately; Node::start
still performs its own exclusivity check. Factory join, caller executors,
original five-second limit and every lifetime assertion remain intact.

A separate deterministic regression exposed a concrete panic hazard: an
ExecutionDomain::run poll can be admitted, then close before a later blocking
effect in that same poll. Both embedded generation build and metrics sampling
used infallible blocking admission and panicked with DomainClosed. The new test
captures that exact boundary, requiring a typed refusal and zero application-
factory/metrics-sampler invocations. Its first compile needed a test-only
Generation error extraction correction because Generation lacks Debug. Once
corrected, both old calls panic and the behavioral regression fails; that is
the baseline signal, not the earlier compiler error.

Private helpers now use try_blocking for bootstrap/adoption generation builds
and initial/periodic metrics sampling. Closed admission returns an error before
callbacks run; metrics maintenance ends on direct DomainClosed. Already-started
blocking work remains owned and joined before releasing the node directory.
No new task is allowed after closure, no deadline is increased and no durability,
lease, readiness, binding or protocol/schema marker changes. All owned markers
remain 1.

The deterministic test passes after the fix, as do all 111 library cases and
all 30 serial Node cases (275.51s), including the enhanced cancellation fixture,
real event append/ack loss, catalog/source/subscription handoff, removed-class
recovery, source destruction, generation adoption and complete shutdown proofs.
Workspace/all-target/all-feature Clippy-D, 429-file formatting, documentation
(35/6/370), dependencies (401/no JavaScript engine), all 44 saved fault-oracle
guards and whitespace checks pass. This is not another full-workspace integration or complete release
matrix pass. The earlier bootstrap timeout audit remains open; proving this
admission panic does not prove it caused that timeout.

Private twenty-run logs, selected baseline/fixed regression diagnostics, source
patch and qualification manifest are retained at
`/private/var/folders/rn/k2b7mvbs2dgb__6ys6_h94pm0000gn/T/cellar-bootstrap-diagnosis-lvy5ulth`.
Other missing implementation and follower/power/store/physical-host, lifecycle,
policy, cleanup and capacity/soak qualifications retain their full scope.

## Selected offline source capacity qualification (2026-10-14)

An opt-in Backlog cell commits many distinct commands into one source/lane. The
fixture captures a bounded row quota, pauses actual append traffic and requires
OutboxFull for the extra command. Its business write and next sequence roll back.
Four audits verify every original command/receipt, pending accounting and producer
identity before the offline soak, before SIGKILL, after recovery before retry and
after complete delivery. No source POST replay repairs acknowledged state.

The one-record required stream forces batch-limit adaptation and consumer-paced
retention. Resumable cursors avoid rereading acknowledged history still retained
until the next capacity sweep. Each accepted envelope is independently recorded;
inbox effects are proved before redelivery, which adds zero effects, and exact
checkpoint progression is checked. All replacement/surviving processes report
complete preserving shutdown before the private RustFS server is removed.

The initial eight-command probe exposed a harness cursor error and was retained
as failed evidence. Its corrected SQLite case passed eight acceptances/effects
and checkpoint 8. The first 64-command RustFS probe exposed another harness error:
the proxy rejected unarmed StreamFull responses, hiding advertised batch limits
from the producer. That probe was interrupted and is not qualification. The
proxy now forwards unarmed errors unchanged, with a real socket regression;
armed proven-response-loss cases still require success.

The fresh RustFS case passed 64 commands, four audits, one producer SIGKILL, a
30-second offline soak, 64 acceptances/effects, checkpoint 64 and complete final
shutdown. Saved audit-only checks pass for both successful cases. Observed p95
producer response was 96.3 ms; consumer redelivery-plus-ack was 598.5 ms. These
are local case measurements, not performance guarantees. RSS, residency/cache
and bounded live local SQLite/WAL/LTX file observations are saved; allocation
does not measure unique physical extents or bucket bytes.

Example build and focused all-feature Clippy-D passed; ordinary smoke, all 52
Python oracle/proxy guards, 430-file formatting, docs (35/6/370), dependencies
(401/no JavaScript engine) and whitespace checks pass. No framework Rust changes
were made in this batch; this is not a new complete-workspace integration gate.
All Cellar-owned markers remain 1, with internal hard cutover and no legacy readers.

Private images, successful and failed evidence are retained at
`/private/var/folders/rn/k2b7mvbs2dgb__6ys6_h94pm0000gn/T/cellar-outbox-capacity-evidence-xjhtzzjh`.
Example SHA-256: `b360d5a607778cd1e66c80497980626453f25d6df3ba96c5270e393e7f424a3b`.
This qualifies one source/lane at its configured row quota on one machine. Many
cold sources, long soaks, memory SLOs, compaction/restore capacity and the complete
follower/power/store/physical-host, lifecycle and policy matrices remain open.

## Many-source cold recovery reproducer: qualification fails (2026-10-14)

`many_sources.py` adds one independently acknowledged command/event per Order
cell and a saved oracle for distinct source/lane identities, protected acceptance,
remaining pending obligations, no source replay, exact effects/checkpoints,
resource limits and every final shutdown. A bounded opt-in `stream_rows` fixture
setting leaves ordinary examples and protocol markers unchanged. Counts cap at
192, stream rows at 20, residency at 128 and route cache at 256.

The first probes exposed harness assumptions. Redelivery can include additional
new records unless the protected window fills first. Source owners can move onto
other fleet nodes; a proxy cannot pause their local appends. The current case
therefore uses a full required-retention window for backpressure, accepts current
fleet owners with original identities, and restarts all three processes. Every
replacement is started before awaiting any readiness because fleet-mode recovery
can need a peer's retained log. Cleanup continues after a failed drain and closes
all owned processes/proxy/store/ledger; its regression verifies that behavior.

The corrected 144-command RustFS audit proved eight protected acceptances and
136 pending obligations before all three observed SIGKILLs. Source owners were
distributed across producer/consumer/destination under residency pressure. All
commands and receipts were independently audited before loss. The 30-second
protected-backlog observation retains RSS/residency/cache/local-file samples.
It does not prove successful recovery or consume any original event after loss.

Both fresh concurrent-restart cases (eight commands/SQLite and 144/RustFS) fail
resource activation with a peer SendRequest connection closed before completion.
The SQLite failure reaches producer/consumer activation; RustFS has two ready
replacements before the third fails. RustFS cleanup additionally reports one
pending dispatch even though ingress/work/cells are drained and no drain deadline
elapsed. The saved source/image/config/log evidence is a production failure
reproducer. Do not increase readiness/operation/drain deadlines, replay commands,
relax shutdown or switch durability modes to turn these cases green. Diagnosis
must distinguish recovering follower-only listeners, stale pooled connections
and ownership transitions. The earlier isolated bootstrap timeout remains a
separate audit; this evidence does not establish its cause.

Example build, focused all-feature Clippy-D, ordinary smoke, all 62 Python oracle/
proxy/cleanup guards, 430-file formatting, docs (35/6/370), dependencies (401/no
JavaScript engine) and whitespace checks pass. No full-workspace gate was rerun.
Current many-source process qualification is explicitly failed, not complete.
Earlier small producer-only probes and harness failures are retained separately
and do not satisfy the current all-process case or release matrix.

Evidence root:
`/private/var/folders/rn/k2b7mvbs2dgb__6ys6_h94pm0000gn/T/cellar-many-source-evidence-jlt5pat1`.
Example SHA-256: `e09dfaa99b5e7a8aa8a7c8e02789ea2b8bf08b0c937104aae764ea5a2f20748c`.
The objective remains active with this recovery/admission failure at the next
priority; the full implementation, qualification and requirement audit retain
their existing scope.

## Activation-specific transport replay (2026-10-14)

The general RPC router correctly stops an ambiguous failed/truncated exchange:
it cannot safely repeat an arbitrary application handler. It previously flattened
that error's type before returning it, and event activation also stopped after
one exchange even though its configuration operation is durably idempotent and
its other methods only resolve/read state. Startup peer connection closure could
therefore end otherwise recoverable resource activation.

Remote RPC now preserves a private network-failure context for Reqwest/Hyper/I/O
errors, including interrupted response bodies. Header/protocol/authority failures
retain ordinary errors. General dispatch's ambiguity classification and refusal
remain unchanged. The activation helper alone repeats tagged transport errors,
using the same authority and encoded body; a fresh request ID avoids a previous
abort tombstone. Backoff caps at 200 ms under the original enclosing activation
deadline, with no detached retry owner. Successful responses still pass the same
resource/revision/digest/grant checks before binding pin/readiness. Permanent
delivery/protocol/codec failures and response-channel closure are not replayed.

The one-attempt regression compiled and failed with the tagged peer-closed error;
earlier test compilation corrections are not the behavioral baseline. Four final
regressions pass: network replay, unchanged general-RPC ambiguity, permanent
permission refusal, untagged protocol refusal and caller-deadline cancellation
with no surviving retry task. All 115 library and 30 serial Node cases pass
(281.98s for Node), including actual append/ack loss, ownership, removed-class
recovery, binding/retirement and unchanged shutdown deadlines.

The archived example passes fresh SQLite eight-source recovery: three SIGKILLs,
original protected history, autonomous delivery without source requests, eight
acceptances/effects, checkpoint 8 and all final shutdown reports. A RustFS
144-source replay also passes three SIGKILLs, 144 original acceptances/effects,
checkpoint 144, complete replacement shutdown and owned server removal. Its
drain was 105.78s on this machine; saved audit-only checks pass for both cases.
The ordinary example smoke and all 62 Python oracle/proxy/cleanup guards pass.
Workspace/all-target/all-feature Clippy-D passes (2m19s), as do all 10 Cellar
doctests, 430-file formatting, docs (35/6/370), dependencies (401/no JavaScript
engine) and whitespace checks. This is not another complete-workspace integration
or complete release-matrix pass.

The first RustFS 144-source run on this same image still failed the original
activation deadline, followed by a spawn panic at execution/domain.rs:668 after
domain closure. A diagnostic replay with RUST_BACKTRACE=1 passed instead, so it
did not identify the panic's caller. Neither replay establishes that deadline/
panic cause or closes the prior pending-dispatch shutdown failure. All failed
cases remain preserved and excluded from passing qualification. Large recovery,
late task admission, maintenance fairness and release qualification stay open.

Evidence root:
`/private/var/folders/rn/k2b7mvbs2dgb__6ys6_h94pm0000gn/T/cellar-activation-replay-evidence-aj1xd665`.
Example SHA-256: `855eebf8a2c0fa80c8071bf309fb65171d58a0db514d60676506cf6feaf8be7f`.
All owned markers remain 1. This scoped fix does not complete the design's full
implementation, fault/capacity/lifecycle matrix or requirement audit.

## Startup wake pressure and closed-domain gated replies (2026-10-14)

The opt-in diagnostic 144-source RustFS run reproduced the activation deadline
and panic. RPC stage logs stopped at catalog configure/resolve. Host snapshots
showed all 16 activation slots in owner-log recovery with 113–119 additional
activations queued, while only one or two cells were resident. The backtrace
identified `host_turn::send_answer_after_arm_gates` called from native alarm
failure settlement after the execution domain had closed. This replaces the
earlier unknown-caller/unknown-pressure account with concrete evidence; it does
not establish the cause of the separate original five-second bootstrap timeout.

Reply delivery now uses fallible task admission. When a begun turn settles after
closure, refusal drops the reply value and discovery-gate receivers in their
originating domain. The returned closed completion remains visible to the driver;
no success or handler error bypasses an unproven gate. Both closed-poll tests
failed with the original infallible spawn; they pass after the fix, as does normal
admitted delivery waiting for discovery. Successful values drop exactly once.

Shared `node/wake_discovery.rs` orders boot/periodic discovery after the captured
generation's event-resource activation. Its first store read and first cold hint
cannot consume admission ahead of catalog/partition readiness. Embedded and CLI
bootstraps retain predecessor recovery, node authority and internal serving.
Readiness failure/cancellation cannot launch discovery; no-event hosts proceed
immediately. Durable wake publications are neither consumed nor discarded while
waiting, and the regular scan resumes after activation. Catalog configuration
can directly reconcile a retained pending operation before the general scan.
Tests cover pending activation, success, failure, cancellation and no-event hosts.

All 121 library and 30 serial Node cases pass (286.15s for Node), including
removed-class source recovery, native outbox append/ack loss, source retirement,
ownership and original lifetime/deadline/shutdown assertions. The diagnostic
helpers preserve the original readiness deadline and record host metadata only;
activation debug logs report method/stage/timing/success without reply payloads.
Temporary debug tags are removed. Diagnostic observations add traffic and are
kept separate from the final qualification runs.

The final archived image passes untraced RustFS runs with 144 and 192 independent
sources, each exceeding one host's 128-resident limit. Each audits original
accepted/pending state, observes all three SIGKILLs, recovers unchanged protected
history and delivers/consumes every original event without source requests before
completion. Exact inbox effects, checkpoints 144/192, all three complete final
shutdown reports and owned server removal pass; both saved audit-only checks
pass. Drains took 95.03s and 140.39s on this machine, not SLO guarantees. Ordinary
smoke, all 62 Python guards, docs (35/6/370), dependencies (401/no JavaScript
engine) and whitespace checks pass. The failed diagnostic/backtraces, baseline
and fixed regressions and all images/configuration/evidence are preserved.
Workspace/all-target/all-feature Clippy-D passes (1m56s), along with the final
121-library replay, all 10 Cellar doctests and 432-file formatting. These checks
do not substitute for another complete-workspace integration or release-matrix
run.

Evidence root:
`/private/var/folders/rn/k2b7mvbs2dgb__6ys6_h94pm0000gn/T/cellar-large-recovery-diagnosis-dkejbs8w`.
Final example SHA-256: `ffd5dbd8f516552166c08b7014c92f4133added3024206325d84580181837515`.
These fixes address the observed startup-pressure and gated-reply panic paths.
They do not close general wake fairness, every failed-startup/shutdown/network
cut, the prior pending-dispatch failure's full audit, follower/power/store/physical-
host loss, broader capacity/SLO accounting or the full design requirement audit.
All Cellar-owned markers remain 1 and the full goal remains active.

## Current contract audit: implementation and evidence kept separate (2026-10-14)

This is a working audit of the complete design, not a completion claim. Historical
test notes above prove only their stated source/image/configuration and boundary.
The CI-equivalent workspace library/integration run passed 411 tests across 99
executables against `d9ab3d8`, before the subsequent read-decoder fix. The
compatibility instruction in the storage-model paragraph is corrected: all owned
markers remain 1 and development layouts use hard cutovers without migrations.

| Contract item | Current authoritative implementation/evidence | Completion limit or missing evidence |
| --- | --- | --- |
| One cell transaction; root/savepoint rollback; caught emit failure consumes no identity | `application/storage/outbox.rs`, `storage/sqlite/outbox.rs`, native_outbox and storage rollback/capacity tests | Full transaction-view escape, abandoned async transaction and unfinished write-cursor cut matrix still needs explicit qualification |
| Encode once under a byte budget; immutable bytes and source/lane identity | event_stream encoding, SQLite outbox, opaque base64 transport, exact peer-retry ledgers | Independent identity test vectors and every overflow/adversarial decoder case require audit; no global producer-order promise |
| Source accepted only after data and discovery; explicit sync covers both | outbox dispatch sync, wake projection/arm gates, source-before-send forensic harness | Every commit/publication/proof cut, follower-only proof and power/store loss remain open |
| No task alone owns delivery; removed-class recovery | persistent work generations, owner restore and recovery worker, native removed-class test; many-source all-process ledgers | Every owner isolation/expiry/source/facet recovery cut is not proven by these selected cases |
| Receiver authentication plus durable publish/read/consume/admin grants | host-minted Authority, runtime RPC provenance, catalog/store authorization and native revocation tests | Complete cross-tenant, revocation/reconciliation/adoption race matrix is not yet qualified |
| Creation cannot authorize itself; configuration intent, stable operation, proof and current-state readiness | catalog reconciliation, deployment resource validation, event_resources, creation-authority regression | Complete interrupted create/update and multi-resource failed-adoption cuts remain open |
| Name selection resolves a UUID once; old handles cannot attach to recreation | name_resolution, BindingConfig/readiness pinning, named-binding Node test | Wider concurrent name/adoption/revocation scenarios remain unqualified |
| Atomic contiguous append and durable dedup beyond retention | store append, cellar-logic decisions, retained producer watermarks, native stream and actual append reply-loss cases | Complete duplicate suffix/digest/offset exhaustion and follower-only/physical-host loss matrix remains open |
| Per-lane reservations, generation-fenced settlement and adaptive batches | bounded dispatcher/service and stored receiver hints; capacity harness | General cancellation/routing/fairness and all stale-completion interleavings remain open |
| Raw read cursor is independent of subscriptions and validates every returned batch | bounded/canonical server read plus repaired client acceptance, nine decoder regressions and native stream integration | Scope/identity/budget/continuity checks are qualified locally; complete codec and hostile-peer response matrix remain open |
| Fetch identity, lease authority, checkpoint and retention share one transaction | subscription_store and cellar-logic lease policies, native stream integration | Lost fetch reply, expiry/ack/extend/retry races and lost-wake/long-poll cancellation matrix not fully qualified |
| Exact ack and absolute extension; lost reply does not duplicate effects or progress | subscription receipts and actual embedded/independent ack-loss ledgers | Other consumer/follower/owner-loss cuts and full deadline/logical-clock matrix remain open |
| Unknown event schemas remain inspectable without automatic settlement | raw Envelope and fallible typed decode API | Audit all read/fetch envelope validation and hostile replies independently of decoder implementation |
| Local inbox effect/receipt atomicity, digest conflict, explicit bounded pruning | SQLite inbox and native/process projection cases | Two-consumer receipt race and every proof/pruning/crash boundary remain open; external effects require application idempotency |
| Bounded/required retention, lease protection, explicit cursor expiry, poison-event handling | stream retention/store and lifecycle controls; required one/eight-record capacity cases | Complete policy/clock/lease/maintenance interleavings and large protected metadata capacity remain open |
| No automatic correctness receipt/watermark expiry; explicit seal and tombstones | producer seal, subscription operations, catalog/stream retirement schemas | Metadata exhaustion and long-outage/recreation/late-packet full matrix remain open |
| Source destruction closes authority, proves terminal destinations and retains loss evidence | source_destruction, retirement proofs, native lifecycle and bounded facet cleanup tests | Every abrupt cleanup/lease/grant/policy cut and large SQL/directory reset capacity remain open |
| Ordinary reset preserves source, inbox and destination protocol state | SQLite reset preservation and PendingDelivery refusal tests | Full reset-vs-in-flight destruction/adoption interleavings remain unqualified |
| Startup/adoption readiness and finite owned work; no unproven output on closed admission | event_resources, wake_discovery and gated reply tests; untraced 144/192-source recoveries | General wake fairness, every failed-startup/shutdown/network wait and the prior pending-dispatch audit remain open |
| Supported host and hard-cutover compatibility contract | bucket-backed activation gate and version-1 markers; unsupported-layout and host-policy refusal tests | Qualified SQLite-only recovery/discovery is absent; external codecs and sequence/revision counters are separate |
| Typed caller, pressure, consumer and authority outcomes | DeliveryError/ErrorCode plus local policy failures | Host RPC now retains OwnershipLost/Ambiguous plus local/peer phase classification; complete SDK preflight, codec, timeout and cancellation mapping audit is still required |
| All envelope/metadata transport allocations stay within captured/hard budgets | bounded encoding, receiver budgets, subscription batch checks, repaired read acceptance, structured RPC ceiling | Complete codec/response-validation audit is not yet complete; global transport ceiling and caller decode budgets are separate |
| Logical bytes plus physical storage headroom/accounting | per-source/stream counters, SQLite page observations and live local file samples | Full WAL/LTX/bucket/filesystem accounting and restore/compaction headroom are not implemented/qualified as a complete profile |
| Export authoritative backlog, blocked lanes, offsets, leases, lag and retention gauges with bounded labels | fixed host operation metrics and owner-sampled source/stream/subscription diagnostics | Fleet root collector now exports checked owner-sampled durable gauges with coverage refusal; resident-host gauges, full physical profile and historical/facet inventories remain gaps |
| Bounded operators and complete resource/source inventories | signed source/stream/subscription controls, namespace catalog inventory, root census and destination watermarks | **Implementation gap:** fleet catalog namespace/facet/historical-source census is incomplete; pages are live observations, not drain proof/snapshot |
| Native declarations, feature validation, same local development protocol and examples | deployment adapters/Node activation, durable-streams native application and process harnesses | Complete packaging/dev-policy matrix and deployment/operator examples still need audit against the current SDK |
| Enumerate commit/proof/wake/append/lease/ack/retention/ownership interleavings | decision-core tests plus dedicated native and process cases | Full short-interleaving model enumeration and independent invariant coverage are not demonstrated |
| Exact release crash matrix across supported proof modes/backends | saved single/independent/all-process cases with original receipts and pre-replay audits | Follower-only, isolated/expired owner, storage/power/physical-host faults and all specified cut points remain unqualified |
| Capacity, lifecycle and operational release gates | selected row-quota, slow required consumer and many-source short-soak cases | Transaction/append profiles, long soaks, compaction/restore, general fairness, physical memory/storage SLOs and assigned operational owners remain incomplete |

Priority follows correctness and missing scope: complete typed failure
classification and aggregate/inventory implementation. Continue the original
failure/capacity/lifecycle qualification matrix and final requirement audit; no
single table row or passing scoped case closes the entire goal.

## Refreshed workspace gate and bounded read acceptance (2026-10-14)

The CI-equivalent command `cargo test --workspace --all-features --lib --tests
--locked -- --test-threads=1` passes 411 tests across 99 test executables on
`d9ab3d8`, with zero failed/ignored cases. It includes the real Cargo development
workflow, compiled daemon lifecycle, Node/event integrations, Axum durability/SSE/
WebSocket ownership, decision core, storage and formatter tests. This is evidence
for that exact pre-decoder-fix source state, not the entire release fault matrix.

The contract audit found that Stream::read trusted server-side budgets and used
direct integer parsing. Extracting the existing acceptance path and adding six
tests produced five behavioral failures: noncanonical positions, excessive batch
count, skipped offsets/next cursor, foreign envelope identity and ignored requested
position. A test-only borrow error was corrected before this behavioral baseline;
it is not counted as reproduction evidence.

The client now clamps outgoing budgets to captured host and hard protocol limits,
validates the requested cursor, and independently checks canonical decimal
metadata, resource scope, retained interval and contiguous offset advancement.
Returned count is checked before per-record processing; encoded size is checked
against event and remaining batch budgets before base64 allocation. Decoded size
and checked total accounting apply afterward. Source incarnation/scope, positive
sequence/version, timestamp, family/key limits, rederived lane and logical event ID
are validated before returning records. Wrong identity or malformed input cannot
create a valid next cursor. Read-only replay still changes no checkpoint and raw
unknown application event versions remain inspectable with original encoded bytes.

Nine acceptance regressions cover these failures, i64-end/above-binary64 positions,
unrestricted policy's hard record ceiling, invalid encoding and zero event version.
All 130 library cases and the native stream delivery/retention/revocation integration
pass, as do the rebuilt ordinary stream example and all 62 Python guards. The
post-change gate is scoped accordingly; the earlier 411-test workspace run must
not be relabeled as testing this later source. Full codec/subscription response
validation, typed OwnershipLost/Ambiguous mapping, aggregate metrics and broader
inventories remain work, along with the existing crash/lifecycle/capacity matrix.

Evidence root:
`/private/var/folders/rn/k2b7mvbs2dgb__6ys6_h94pm0000gn/T/cellar-outbox-workspace-audit-4eom26wk`.
Example SHA-256: `5c46eae8349018dfa73a307bc8bd466b10e9a0bd9f001081eaf631b38a5594e5`.
The design's remaining migration sentence is corrected to the authorized internal
hard-cutover policy. All Cellar-owned markers stay 1; full completion remains
unproven and the goal is active.


## Typed event host failures preserve execution certainty (2026-10-14)

The named `OwnershipLost` and `Ambiguous` outcomes now exist in `ErrorCode`.
`node/event_failure.rs` separates pre-execution route refusal from a failed output
gate after execution. A fenced authority or exhausted stale route yields ownership
loss; a request refused before submission or a never-connected peer yields
unavailability. Lost submitted results, truncated reply bodies, unproven output
and otherwise unclassified dispatched failures remain ambiguous. Typed ownership
loss does not prove that an earlier attempt failed to commit.

The invocation event path uses the existing egress gate verdict before handing
its request to the host channel. Refusal cannot send an event packet; a lost
submitted result retains cancellation for its exact request identity. Ordinary
RPC uses its existing path. Authenticated peer host replies carry a bounded fixed
verdict header; missing/unknown verdicts cannot turn an unknown result into safe
non-execution. Existing DeliveryError codes and the private activation transport
tag remain in the error chain, so activation-only retries keep their original
policy/deadline and ordinary ambiguous RPC never gains automatic replay. Framework
protocol/schema markers stay 1.

Six new cases exercise unsent pooled request preservation, local/peer gate agreement, transport certainty and tag
preservation, unknown peer verdicts, zero submission after source-gate refusal,
and exact cancellation after a lost submitted result. The existing real peer
append/ack reply-loss integration now requires a typed ambiguous lost-ack outcome
before replaying the original persisted receipt. It retains independent acceptance,
checkpoint/effect audits, exact retry bytes and complete node shutdown checks.

The stricter real-network assertion initially failed: the caller received
AlreadyAcknowledged after the proxy dropped a proven ack. Source inspection found
that send_inner retried every pooled failure when its body was buffered, bypassing
the dispatcher ambiguity policy. It now uses Hyper 1.11.0 try_send_request and
retries only when take_message returns the unsent original request. Serialized
requests are never replayed by this pool, including ordinary RPC/fetch. Returned
requests retain their body and headers, including streamed bodies, without clone
or re-encoding. This corrects an observed implementation gap, not the assertion.
The failed gate is retained separately from the pending post-fix gates. The
post-pool integration then exposed an unframed-response parser gap in the proxy:
the required cancellation sends a valid bodyless 204 response, which the fixture
rejected and used to terminate its listener. Subsequent source retries became
Unavailable. The parser now accepts bodyless 101/204/304 responses while retaining
framing checks for ordinary responses; a real socket regression verifies that
the following framed reply remains intact. The strict lost-ack assertion and all
original pre-replay/receipt/shutdown audits remain required.

Final qualification passes 136 library tests (including the six new host/pool
cases) and all 31 serial Node cases (including the bodyless parser and strict
proven append/ack loss tests). The Node suite takes 276.10 seconds and retains its
original deadlines, ownership, receipt and shutdown assertions. Workspace/all-
target/all-feature Clippy with warnings denied, 436-file formatting, 62 unchanged
Python guards, docs (35/6/370), dependencies (401/no JavaScript engine) and whitespace
checks pass. Earlier passing results before the pool fix and both failed gates
remain separate; none is relabeled as qualifying the later source. The earlier
411-test workspace integration and 11-workspace-doctest gates predate these host/
transport changes and do not establish a refreshed complete release gate.

Evidence root: `/private/var/folders/rn/k2b7mvbs2dgb__6ys6_h94pm0000gn/T/cellar-event-outcomes-evidence-lnz62meb`.
All owned format/schema/protocol markers remain 1. A test-only
Debug-bound assertion error was corrected before this gate; it is not a behavioral
baseline. These host outcomes do not complete the full SDK preflight/codec/timeout/
cancellation mapping audit, aggregate gauges, complete inventories, physical
storage accounting or the remaining fault/capacity/lifecycle release matrix.


## Owner-routed fleet state collection (2026-10-14)

`cellar event metrics` now collects current durable outbox rows/bytes/age and
fixed blocked reasons, stream rows/bytes/offset sums, protection/retention bytes,
live leases, expired/live subscriptions, logical/retained lag and oldest retained
unread age. Root SQLite page allocation is counted once per observed root. Stream
partition counts include retained retired instances, with a separate retired count.
The platform StateAuthority is exact-scope/method bound, authenticated through
fleet ingress, and cannot authorize an application invocation or mutation. Native
sampling runs under the original owner's activation/output/discovery gates and
works through the framework handler, including removed-class recovery. It shares
the existing indexed stream observation calculations; metadata work remains bounded
by 128 live subscriptions with exact timestamp reads. Sampling never advances a
checkpoint, expires a lease or changes durable logical time.

The collector uses bounded provider pages (100 prefixes) and four concurrent
probes. Its root/page cap defaults to 10,000 and cannot exceed 100,000. Duplicate
or invalid directory entries, failed probes, failed/repeated listing tokens and
caps remain explicit incomplete coverage. No unreadable state becomes a successful
zero total: incomplete Prometheus emits coverage only; JSON uses gauges=null and
separate observed_gauges, and the CLI exits unsuccessfully. Complete means a
successful live root traversal, not a consistent fleet snapshot or drain proof.
Global labels contain no resource/cell/subscription identities. Canonical decimal
samples, complete fixed metric sets and accounting are checked before any total
changes; sums use checked u128, and age metrics take maxima.

Qualification is recorded after the final source/binary gates below. Earlier
140-library/31-Node results predate the last retired-partition, incomplete-output
and collection-acceptance refinements; they are not relabeled as final evidence.
The new path does not complete resident-host aggregate gauges, facet/WAL/LTX/
bucket/filesystem physical profiles, historical producer/fleet namespace inventory,
or the original fault/capacity/lifecycle/release matrix. All owned markers stay 1.


Final library checks pass 140 cases; the first broad Node gate passes 30 cases but
fails the existing cross-fleet admitted-write drain case with NodeLeaseExpired.
The same test passes isolated with unchanged one-second lease/eight-second shutdown
limits. Its cause is not established; concurrent compilation is an observation,
not proof of scheduling causality. Preserve the failed gate alongside the pending
complete no-build replay and targeted latest cold-first assertion. Do not relax
lease, readiness, durability or shutdown checks to conceal this failure.


The complete no-build replay passes all 31 Node cases in 292.42 seconds, including
the unchanged fleet-drain case. The latest targeted removed-class test also passes
in 48.37 seconds: the platform sample is the first direct source inspection after
restart and observes one source, one pending row and one Forbidden-blocked lane
without an Account implementation. Metrics authentication and lag-after-ack plus
ordinary/forced retired-partition sampling are covered by the Node gate. The
original lease-expiry failure remains unestablished, not erased by these passes.
Workspace/all-target/all-feature Clippy-D, 440-file formatting, 62 Python guards,
docs (35/6/370), dependency inspection (401/no JavaScript engine) and whitespace
checks pass. No fresh full-workspace release-matrix claim is made.

Archived CLI/example binaries pass the expanded RustFS census/metrics exercise.
The independent commands leave one capacity-blocked source obligation and one
protected stream record. Complete collection sees five roots, two producers, one
pending outbox row, one stream row, one record of logical/retained lag and exact
protected-byte accounting. A one-root cap yields incomplete coverage and gauges=null;
after both processes stop, all five state probes are unavailable, authoritative
gauges remain null and Prometheus omits backlog series rather than emitting zero.
The original source census, signed source inventory, shutdown/store cleanup and
independent measuring ledger checks remain. These are selected same-machine RustFS
process observations, not every backend, long-soak/fairness or power/store-loss
qualification. Complete live traversal is not a snapshot or drain proof.

Evidence root: `/private/var/folders/rn/k2b7mvbs2dgb__6ys6_h94pm0000gn/T/cellar-state-qualification-1omyqbhf`.
CLI SHA-256: `09970a182fa3f7f96ce58797b5e03c62328284ba8fcb4629c794df53676d94f0`.
Example SHA-256: `c270b64106215dc050ec96122ba90f6cecf094ed5b2e83aa1501cf21c1b0e76a`.
All Cellar-owned markers remain 1. Remaining resident-host metrics, physical
profiles, broad historical/facet/namespace inventories, SDK outcome/codec audits
and the complete failure/capacity/lifecycle/release requirements keep the goal active.


## Bounded fleet descriptor and retained history inventory (2026-10-14)

Whole-fleet root descriptor discovery now identifies catalog namespaces, retained
stream UUIDs and current source/history availability, without out-of-band tenant/
application selection. It reads one bounded provider page and four owner probes;
its inventory1 token binds bucket/prefix/endpoint/region. Root namespace identity
is rederived from scalar catalog metadata and checked against the hashed owner
scope. Empty catalogs report missing retained namespace identity explicitly.
Partial probes print unavailable rows/continuation and return a failed exit.

Scoped resource pages reuse the bounded validated catalog reader and include
tombstones. Producer pages reuse the bounded lane/watermark reader and preserve
source/lane identity checks, while exposing retirement/cleanup-complete flags.
Inspection found that retirement intentionally purges these rows after retaining
terminal identity/receipt; the implementation does not invent historical lanes
or call terminal absence an active empty producer set. Root source history reads
retained operation sequence ranges, including retries rather than scanning past
them, and projects old/new producer incarnations, lifecycle and loss counts.
The full receipt, operator reason/acknowledgment, token and payload are excluded.
Supported recreation preserves this ledger and its scope/sequence continuation.

A distinct exact-scope/method platform InventoryAuthority, signed ingress and
native framework handler retain original activation/output/discovery gates and
cannot authorize mutations or be replaced with metrics/resource authority. All
owned markers stay 1. Source/module namespace and borrow/build issues were corrected
before qualification; they are not behavioral reproduction evidence. Final gates
and archived process evidence are recorded below when complete. This does not
close facet inventories, every historical/backend/ownership/capacity cut, physical
profiles, host metrics, codec/outcome audits or the full release matrix.


The first behavioral gate passes 144 library cases and the native event integration,
but the new retired-partition assertion fails. An isolated run reproduces it;
inspection identifies a query for a nonexistent cleaned column. Retirement actually
records completion with finished_ms. The query is corrected to that durable field,
and unavailable ingress responses are structured JSON. Preserve both failed gates
separately; do not weaken the terminal-cleanup assertion or count compile fixes as
behavioral reproduction. Corrected full gates are recorded below.


The corrected final gate passes **144 library tests**, the native event-stream
integration and **all 31 serial Node tests** (279.05 seconds), including terminal
retirement cleanup and paged old/new incarnation history across source recreation.
Workspace/all-target/all-feature Clippy with warnings denied, 444-file formatting,
62 Python guards, documentation (35 documents/6 flows/370 references), native
dependencies (401 packages/no JavaScript engine) and whitespace checks pass.
A dependency-check invocation without Cargo on PATH failed before inspection;
the corrected PATH invocation passes. These are scoped gates, not a refreshed
complete workspace integration/release matrix.

The archived corrected CLI and example pass the actual RustFS census/metrics/
inventory exercise. Five bounded descriptor pages discover all five roots, recover
the catalog namespace and stream UUID, and agree with independently observed
producer watermarks. Scoped resource/producer pages, fresh empty source history,
foreign-fleet cursor refusal and all-five-owner unavailability after complete
shutdown pass. The ledger records owned store removal. This supplies same-machine
RustFS process evidence; facet/historical restore and the complete backend/owner/
failure/capacity qualification remain open.

Evidence root: `/private/var/folders/rn/k2b7mvbs2dgb__6ys6_h94pm0000gn/T/cellar-inventory-qualification-13ibixvc`.
CLI SHA-256: `deb9c040a2514985e9bcfbcb71ecd4ae29c86d21b8dd0ffd6f6f830c549c59b2`.
Example SHA-256: `484aaac7787e6707316a717409625f824fa36a5fd42162dac597cb1bf0c8db9b`.
Initial failed behavior logs remain archived separately from corrected passes.
Cellar-owned peer protocol, native-host protocol, wake format and schema markers
remain **1**, with hard development cutovers rather than backward compatibility.
The implementation goal remains active; the remaining requirement audit and
qualification must continue before release.


## Subscription reply acceptance audit (2026-10-14)

The client previously checked resource/name and receipt offsets but did not compare
subscription UUID/incarnation with the identity resolved for its fetch. It also
accepted unvalidated envelope identity and arbitrary issued/expiry intervals.
Fetch acceptance now checks all resolved receipt identity fields, exact nonempty
contiguous range, canonical timestamps and positive lease duration within captured
host lifetime. Raw and leased reads share one bounded envelope decoder, including
remaining-byte allocation checks and family/source/incarnation/lane/event identity.
Malformed base64/JSON becomes a typed InvalidRequest; unknown application event
versions and exact integer payloads remain inspectable. Retry policy/negative
extension deadline preflight and extension response lower-bound checks are included.
All owned markers stay 1. Current qualification results will be recorded below;
this does not close the full transport/codec/cancellation or lease/fault matrix.

The first library gate passes 150 cases and fails one new oversized-envelope
fixture: its size*8 string is still below the default host allocation bound, so
InvalidRequest for malformed JSON is correct. The fixture now exceeds the actual
captured host base64 bound. No acceptance check or error expectation was relaxed;
this failed fixture result is retained separately from final qualification.

A second library run passes 152 cases and reproduces the same oversized fixture
failure. Inspection finds the source still uses size*8: a concurrently running
formatter rewrote its earlier snapshot after the correction. The fixture is now
corrected only after that formatter is terminal; production acceptance code is
unchanged. Rebuilt native binaries from the final compile run separately while
the corrected library gate is prepared. Preserve both failed fixture gates.

Next audited priority: storage/sqlite/wake_record.rs still drops previous
alarm-only __sys_cell_wake_* triggers and installs the current work projection;
work.rs explicitly tests upgrading that layout. This is a remaining internal
compatibility path contrary to the hard-cutover policy. Replace it with explicit
old-layout refusal before base-schema mutation and retain fresh/current-layout
wake identity, reopen and rollback coverage. This finding is not yet fixed or
qualified by the subscription gates.

The corrected serial library gate passes **153 tests** (363.01 seconds), including
all nine new subscription acceptance regressions and the nine existing raw-read
acceptance regressions. Rebuilt native stream integration passes (12.43 seconds);
all **31 rebuilt serial Node cases** pass (293.26 seconds), including exact lost
append/ack reply handling and complete shutdown. Final formatting checks 445 Rust
files; 62 Python guards, docs (35 documents/6 flows/370 references), native
dependencies (401/no JavaScript engine) and whitespace checks pass. Final source workspace/all-target/all-feature Clippy with warnings denied
passes (11m10s). These are scoped library/stream/Node
checks, not a refreshed entire workspace release or process-fault matrix.

Evidence root: `/private/var/folders/rn/k2b7mvbs2dgb__6ys6_h94pm0000gn/T/cellar-subscription-acceptance-119lzq9p`.
It retains both failed fixture gates, lint-style failure, corrected source/logs
and archived rebuilt native binaries. No receipt, deadline, shutdown or caller
budget assertion is relaxed. Full transport allocation, cancellation, lease-race,
follower/power/store-loss, physical metrics and release qualification remain open.


## Alarm projection hard-cutover admission (2026-10-14)

The existing built legacy-upgrade test passes, reproducing acceptance of the
previous alarm-only wake projection. The new refusal regression fails against the before-change code with
"previous projection was silently upgraded", independently confirming the policy
violation. Its failed log is retained. The implementation
now checks for each previous __sys_cell_wake_* trigger at schema entry before DDL,
journal pragmas or activation writer-epoch updates. It returns typed
UnsupportedProtocol with development-state recreation guidance. The DROP TRIGGER
upgrade path is removed; all Cellar-owned markers remain 1.

The refusal regression checks untouched schema/journal mode, retained alarm time
and wake epoch/sequence, plus no installed storage after failed open. A positive
current-layout regression checks same-epoch reopen, independent alarm/work
removal, consumed tombstones, new-epoch sequence retention, and obsolete-work
refusal. Final scoped qualification is recorded below; this does not close remaining schema,
backend/follower/power/store-loss, maintenance/capacity or release-matrix work.


### Next SDK acceptance priorities from current-source inspection

These are concrete remaining checks, not completed qualification or new protocol
semantics. Audit all returned metadata before reporting successful mutation or
exposing receipt authority; preserve operation/fetch/receipt identity on retry.

| Surface | Current evidence | Remaining implementation/qualification |
| --- | --- | --- |
| Configuration and status (`event_stream/client.rs`) | Request definition/operation preflight; replies only deserialize Configuration | Bind returned resource/operation to request, recognize catalog states, bound error metadata, and test historical operation replay versus current status |
| Subscription lifecycle change (`event_stream/lifecycle.rs`) | Operation/previous identity equality and current resource/name validation | Validate returned action/reason/state/current UUID/incarnation transitions, checkpoint/skipped range/time, including reset/delete and replay |
| Stream audit (`event_stream/lifecycle.rs`) | Caller page bounds; returned AuditPage only deserializes | Enforce count/order/continuation, identity and receipt validity without exposing tokens; test malformed, repeated, out-of-page and foreign records |
| Retirement (`event_stream/retirement.rs`) | Resource/operation equality | Validate state, affected range, error/provenance bounds and transitions; preserve terminal loss receipt after cleanup |
| Retry (`event_stream/subscription.rs`) | Receipt preflight and captured delay cap; RPC reply discarded | Require canonical nonnegative retry_at_ms consistent with delay; malformed successful replies cannot count as accepted retry |
| Shared event RPC (`event_stream/mod.rs`, `error.rs`) | Host execution certainty is typed; bounded global codec elsewhere | Audit request/response allocation limits and codec/timeout/cancellation classification; do not confuse malformed submitted results with proof of non-execution |

The fetch/extension and raw-read gates above address their specific acceptance
rules; they do not establish correctness of these other operations. Prioritize
configuration/lifecycle/audit acceptance together so their invariants share one
reviewable set, then complete typed transport and fault/cancellation qualification.

The freshly rebuilt library test binary passes all **7 work/wake tests**
(0.15 seconds), including the new refusal and current-layout/tombstone regressions
plus existing rollback, alarm independence, restore and generation-exhaustion
checks. The binary and SHA-256 are archived separately. Full library/native and
Clippy gates are still live; these seven cases are not a substitute for them.

The broad final run passes **154 library tests** (11.84 seconds) and the native
stream integration (11.00 seconds), then finishes Node **30 passed/1 failed**
(344.43 seconds). The final predecessor-recovery case reports execution domain
is closed. It passes isolated on the same rebuilt binary with unchanged policies
and deadlines (10.74 seconds). The cause remains unestablished; do not label the
isolated pass a fix or attribute it to load without evidence. Preserve both logs.
Native outbox was not reached because Cargo stops at the Node failure; it runs
separately. A full unchanged Node replay is pending. Workspace/all-target/all-
feature Clippy-D passes (21m54s including its build-lock wait); 445-file formatting,
62 Python guards, docs (35/6/370), dependencies (401/no JavaScript engine) and
whitespace checks pass. The broader failure audit remains open.

The separate rebuilt native outbox target passes its transaction/authority/
publication-grant integration (0.46 seconds). Exact rebuilt stream/Node/outbox
binaries and hashes are archived. The unchanged full Node replay is still live;
its result cannot be inferred from the isolated predecessor pass.

The unchanged complete Node replay passes **all 31 cases** (305.32 seconds),
including predecessor recovery and complete shutdown. This passes the selected
current-source gate; it does not establish why the earlier 30/1 run failed or
complete its separate failure audit. No code, deadline, lease, receipt or shutdown
assertion changed between the failed full run, isolated pass and full passing
replay. The exact archived binaries, both full logs, isolation log and source
hashes retain that distinction.

Evidence root: `/private/var/folders/rn/k2b7mvbs2dgb__6ys6_h94pm0000gn/T/cellar-wake-cutover-f3ljgfrj`.
All owned markers remain 1. Old-projection refusal and current-projection identity
recovery are qualified by the before/after and full scoped gates. Next work is the
SDK acceptance audit above, followed by remaining transport/cancellation, physical
metrics, capacity/ownership/backend and full release qualification. The full
implementation goal remains active.


## SDK administrative response acceptance (2026-10-14)

Current SDK configuration replies now bind resource/operation and active accepted
revision/digest to the exact requested definition. Pending creation requires
revision 1; pending update retains an earlier accepted revision. Current status
can legitimately report a later operation, while historical configure replay must
match its original operation and definition. Known scalar metadata is bounded
before copying; error text allows the server's 1024 Unicode characters/4096 bytes.
No whole untrusted configuration object is cloned for acceptance.

Lifecycle receipt validation checks identities, operation/action/reason/operator,
state, incarnation preservation/invalidation, checkpoint/time and loss-range
consistency. Reset validates its exact target; the receipt does not carry the
previous checkpoint, so acceptance cannot independently reconstruct a skipped
range's old start. Audit pages validate receipt scope, principal/grant bounds,
unique operation IDs, ordered sequence/count/byte bounds and exact continuation,
while allowing legitimate sequence gaps and other authorized principals.

Retirement validates state, identity, operator/error/range bounds and terminal
range/count/byte accounting; initial pending intent need not yet carry partition
loss counters. Existing shared closure acceptance now rejects impossible byte/
row accounting and offset exhaustion. Retry can no longer report success after
discarding a malformed reply; its canonical nonnegative deadline must cover the
requested delay. All owned markers remain 1. Qualification is in progress; these
changes do not close full codec/allocation, execution-certainty/cancellation,
physical metrics, historical/facet/backend/capacity or the release matrix.

The initial library check passes **168 cases** (19.24 seconds), and the initial
workspace/all-target/all-feature Clippy-D passes (9m06s including build-lock
waits). These precede the final raw audit-count and partial-retirement-field
checks; they are not final-source qualification. Finished source adds two further
regressions (170 expected library cases). Final serial library/outbox/stream/Node,
source Clippy-D and formatter-check handles remain live. Docs/dependency/whitespace
and 62 Python guards pass. No initial result completes the outstanding gates.


The final pre-merge SDK run passes **170 library cases**, native stream, all
**31 Node cases**, and native outbox; final workspace/all-target/all-feature
Clippy-D and 447-file formatting pass. Exact source hashes and logs are retained
under `/private/var/folders/rn/k2b7mvbs2dgb__6ys6_h94pm0000gn/T/cellar-sdk-administration-9q78hy5_`.
Those results qualify the saved pre-merge source only.

PR #27 now incorporates main at `88886a8` through merge commit `6ccc964`.
The merge retains both dashboard/cron management and event operator routing,
reply budgets, RPC authority, outbox modules and version-1 cutover assertions.
The published merge alone passes **206 Cellar tests** (162 library, 3 CLI,
39 Node, stream and outbox), **24 dashboard/FleetCare tests**, workspace
Clippy-D, 489-file formatting, 64 Python guards, docs (35/6/373), and the
403-package/no-JavaScript-engine dependency audit. Merge evidence is retained
under `/private/var/folders/rn/k2b7mvbs2dgb__6ys6_h94pm0000gn/T/cellar-pr27-merge-evidence-2ebna8hu`.
The SDK batch was restored byte-for-byte except for AGENTS' merged main guidance.
Combined SDK/merge qualification is running separately; neither earlier run
proves that combined source.

The next shared reply boundary audit finds two concrete gaps in
`event_stream/error.rs` and `event_stream/mod.rs`: `unwrap_reply` clones entire
success/error values and accepts a reply carrying both branches, while structured
response decoding and invalid error envelopes can escape without a typed
execution-certainty verdict. These are post-submission results; malformed bytes
cannot establish that a mutation did not commit. Preserve valid protocol errors
and their bounded pressure/cursor metadata, reject contradictory envelopes, and
classify undecodable results as `Ambiguous` without copying payload text into
messages. Full transport allocation, cancellation and cut-point qualification
remain distinct requirements.


## Enumerated core interleavings (2026-10-14)

`crates/cellar-logic/tests/event_interleavings.rs` adds two exhaustive six-event
permutation scenarios (720 orders each). The wake case varies three delayed PUT
confirmations across predecessor retirement, tombstone retirement and cache loss.
Its independent persisted-key set forbids either old certificate from deleting
the successor's epoch and requires republishing to retain its exact identity.
The subscription case varies acknowledgment/replay, expiry, extension, required
retention and committed incarnation invalidation. An independent decision table
checks advance versus duplicate/reset/expired outcomes; retention cannot delete
unacknowledged required history. These exercise actual pure-core methods.
Qualification is pending. They do not model the SQLite transaction, proof or
output gate that authorizes those facts; combined commit/proof/append/ownership
model enumeration and the full crash matrix remain required.


The combined SDK/merge library passes **178 cases** (54.42 seconds).
The first native stream execution fails at the analytics receipt unwrap
(`native_event_stream.rs:886`, 4.24 seconds); the fixture did not retain the
actual error response. The exact unchanged binary then passes isolated
(12.10 seconds) and in a full serial replay (36.04 seconds). No validator,
retention window, lease/deadline or assertion changed. The one-second history
window is a hypothesis, not an established cause. Preserve all three logs;
this remains an open release failure audit. Rebuilt native outbox passes
(12.68 seconds). Workspace/all-target/all-feature Clippy-D passes (17m11s,
including the lock wait), and 492-file formatting and docs (35/6/373) pass.
The complete Node run is still live and has reported a bootstrap-cancellation
failure; its terminal diagnostics and unchanged verification remain pending.
These results do not establish a complete passing combined SDK gate.

Both core enumeration cases pass **1,440 orders** in 0.02 seconds; the targeted
Clippy-D gate passes too. The 10m17s Cargo elapsed time includes its shared build
lock wait. Keep enumeration scope separate from SQLite/proof/network/crash tests.


The rebuilt Node binary finishes **38 passed/1 failed** (490.80 seconds):
`cancelled_bootstrap_retains_directory_until_started_application_factory_has_joined`
expires its unchanged five-second restart deadline after sampling the old directory
as unavailable, with replacement startup pending. That sample does not prove the
lock stayed held throughout replacement startup. The same binary fails isolated
(46.26 seconds) with the same message. All event-specific Node cases pass, but
that does not qualify the failed lifecycle gate or establish its cause.

A diagnostic-only fixture change distinguishes replacement inputs from
`Node::start`; it performs the same calls and preserves deadlines/assertions.
The rebuilt diagnostic case passes once (33.53 seconds), then the unchanged
repeat fails earlier (10.70 seconds) on the initial factory-entry channel timeout.
No production fix is claimed. Both failure points require tracing; setup/executor
or filesystem delays and unfinished cleanup are hypotheses, not proven causes.
The SDK batch remains uncommitted pending that audit. The reply-boundary follow-up
is specified in `/tmp/cellar-event-reply-boundary-plan.txt`; it is not implemented.

Core enumeration is published in **86b7fe8** on PR #27, which remains mergeable.
Current combined evidence is retained under
`/private/var/folders/rn/k2b7mvbs2dgb__6ys6_h94pm0000gn/T/cellar-sdk-merged-evidence-7im_h1n4`.
The full implementation goal remains active, including unresolved native failures,
shared reply/codec/cancellation checks and the complete release matrix.


## Shared submitted-reply boundary (2026-10-14)

Two new regressions fail on the prior decoder: success clones its payload, and a
contradictory ok/error object retains a permanent Forbidden verdict. The new
shared decoder consumes its sole branch, bounds wire bytes at 64 MiB before
JSON/structured decoding, bounds encoded error metadata at 16 KiB and message
text at 4096 bytes, and maps malformed or unprojectable submitted results to a
generic Ambiguous without reflecting body text. Valid protocol codes and exact
metadata remain intact. Both bound and authorized event calls use this boundary;
pre-submission argument errors and host execution-certainty verdicts are unchanged.
Seven regressions cover moved success allocations, null results, contradictory
and invalid envelopes, error budgets, structured/JSON success and protocol errors,
invalid bytes/capability/remote-error/cyclic results, and an oversized valid JSON
success. Final-source qualification is pending. All owned markers remain 1.
This does not qualify every operation-specific acceptance error, transport body
allocation, timeout/cancellation or crash boundary.

Startup diagnosis retains the repeated initial-factory and restart deadline
failures. Bounded macOS sample/debugger probes returned no stacks; their owned
sampler/debugger processes were stopped at 20/30 seconds, and the debugger child
was resumed. Debugger-timed runs are diagnostic only. Temporary tagged
[DEBUG-outbox-startup] traces now distinguish directory acquisition, reload
certificate, blocking generation admission/entry/completion, root completion,
domain join and directory release. A fixture subscriber captures only that target.
The native stream fixture now reports absent-receipt error metadata instead of
panicking without its response. Original calls, deadlines and assertions remain;
these are instrumentation, not production fixes, and must be cleaned up after
root-cause evidence is obtained.


The peer success path at `node/mod.rs` still uses unbounded
`response.into_body().collect()` before the SDK decoder. Bound event/control
collection itself at the shared wire ceiling. A length-limit refusal is a
submitted-result Ambiguous and must not carry RemoteRpcTransportFailure:
activation retries only that network tag, never malformed/oversized protocol
results. Genuine body transport errors must retain the tag and execution
certainty. Preserve caller cancellation precedence and post-head abort-disarm
semantics for already-proven handlers and their waitUntil work. Add chunked
unknown-length/declared-size/error tests and verify no suffix collection after
budget refusal. This is the next concrete transport implementation gap; the new
SDK decoder alone does not close it.


The instrumented reply-boundary run passes **185 library tests**, native stream,
**39 Node tests** and native outbox (226 cases total); workspace Clippy-D passes.
All five unchanged traced bootstrap repeats pass. Traces show the expected passing
order: factory completion, old-domain join and directory release, then replacement
build. They do not capture or establish the earlier failures' causes. Evidence is
retained under `/private/var/folders/rn/k2b7mvbs2dgb__6ys6_h94pm0000gn/T/cellar-event-reply-traced-evidence-996bp16y`.
The owned sample/debugger attempts returned no stacks within their bounded waits;
no production fix follows from those attempts. A transient build-volume observation
of 13 GiB free, later 40 GiB, is not evidence of failure causation. No unrelated
process or cache was stopped or cleaned.

Temporary startup tracing and its fixture subscriber are removed. The final
untraced source passes **all 226 selected Rust cases**: 185 library (28.65s),
native stream (12.02s), all 39 serial Node cases (370.95s) and native outbox
(0.32s). Workspace/all-target/all-feature Clippy-D passes (12m15s including build
lock wait); 493-file formatting, 64 Python guards, docs (35/6/373) and whitespace
checks pass. The native fixtures retain only clearer failure diagnostics, with
unchanged calls, deadlines and assertions. The seven reply regressions have
before/after evidence: both initial regressions fail on the old implementation;
all seven pass after the shared decoder change. This qualifies the administrative
acceptance and outer decoder batch, not the whole process-fault or release matrix.
Earlier native failures remain recorded and unexplained; no passing replay is
called their fix. Next implementation priority is bounded peer response collection
without false network retry tags, followed by cancellation and remaining release
cuts, physical metrics and inventory/recovery/capacity gaps. The full goal remains
active, and all owned markers remain 1.

Final untraced evidence root: `/private/var/folders/rn/k2b7mvbs2dgb__6ys6_h94pm0000gn/T/cellar-sdk-reply-final-evidence-u9etj6yq`. Exact rebuilt library/stream/Node/outbox binaries, hashes, source diff and gate logs are archived.


## Bounded peer event result collection (2026-10-14)

`node/event_reply.rs` now collects event/control responses using the SDK's shared
64 MiB wire ceiling. Declared oversize is refused before body polling; unknown-
length overflow stops without collecting the suffix. The original route deadline
continues through the body phase and cannot be reset by response-head arrival.
Protocol overflow and deadline exhaustion return Ambiguous without the activation
network retry tag. Genuine body transport errors retain that tag and their typed
execution certainty. Caller cancellation retains precedence, including ties with
ready bodies/expired deadlines, and releases stalled body buffers. The existing
post-proven-head abort disarm remains in the caller; no collection failure may
kill a proven handler's later waitUntil work. All owned markers remain 1.

Twelve new tests cover exact/unknown-length budgets, declared-size admission,
suffix refusal, I/O tag preservation, the actual shared ceiling, cancellation/
dropped-sender ties, stalled-body/future release, actual Hyper declared-overflow
versus truncated responses over an in-memory duplex, original-deadline exhaustion
and deadline cleanup/ties. Combined library/Node/stream/outbox and workspace
Clippy-D qualification is running. Hyper duplex exercises its Incoming body/error
contract; it is not peer authentication, physical-host or store-fault evidence.
Whole request/head deadline behavior, upstream transport allocation, every
cancellation/ownership cut and the full release matrix remain separate gates.


The initial limited-collector stage finishes **195 passed/2 failed** library
cases. Both failures show genuine body I/O errors losing RemoteRpcTransportFailure
after Limited boxes them: a concrete io::Error fixture and actual truncated Hyper
Incoming both reproduce it. Clippy also refuses one redundant test async wrapper.
Initial evidence is retained under
`/private/var/folders/rn/k2b7mvbs2dgb__6ys6_h94pm0000gn/T/cellar-event-collector-initial-ob4u1czt`.
These are before-fix results, not final qualification.

The final collector reads frames into one buffer whose allocation growth is
capped by the wire byte limit, rather than retaining a chunk list plus unused
trailer maps. It ignores trailers and yields after bounded frame/byte work, even
for immediately ready empty frames. This keeps cancellation/deadlines observable
without per-frame accumulation. Wrapping each concrete body error directly
preserves genuine I/O tags; overflow/allocation/deadline refusals remain untagged
Ambiguous. The redundant test wrapper is removed. Four more regressions cover
coalescing tiny frames, unused trailers, cancellation under always-ready empty
frames, and a ready result under the largest captured budget. The locked Tokio
sleep implementation uses checked instant addition/far-future fallback; no new
budget rejection or timer policy is introduced. Sixteen collector tests (201
expected total library cases) and fresh combined/Clippy gates are running.


Final-source workspace/all-target/all-feature Clippy-D passes (5m06s), along with
495-file formatting, 64 Python guards, documentation (35/6/373), dependency
(403 packages/no JavaScript engine) and whitespace checks. The refined combined
Rust run is compiling; the earlier 195/2 run remains before-fix evidence and
cannot qualify this source.


The refined library passes **all 201 cases** (23.75 seconds), including all
**16 collector regressions**. The two I/O-tag failures now pass with concrete
error wrapping, while declared/observed overflow and deadline/cancellation
refusals remain untagged. The always-ready empty-frame cancellation test passes,
as does the maximum captured-budget case. Native stream passes (12.54 seconds).
The complete 39-case Node suite is running; native outbox follows it. These
results do not refresh the full process-fault/backend/cancellation release matrix.


The final collector batch passes **all 242 selected Rust cases**: 201 library
(23.75s), native stream (12.54s), all 39 serial Node cases (394.21s), and native
outbox (1.52s). Final workspace/all-target/all-feature Clippy-D, 495-file formatting,
64 Python guards, docs (35/6/373), the 403-package/no-JavaScript-engine dependency
audit and whitespace checks pass. The failed initial 195/2 stage remains archived
separately; both concrete-I/O tag regressions pass after frame-level wrapping.
No original lease, deadline, acknowledgment or shutdown assertion was weakened.
Evidence root: `/private/var/folders/rn/k2b7mvbs2dgb__6ys6_h94pm0000gn/T/cellar-event-collector-final-evidence-cwgk96g8`. Exact rebuilt binaries/hashes, source and logs
are retained. This closes the peer event/control body collector gap, not every
request/head/local-invocation deadline, upstream allocation or cancellation/
ownership/process-fault cut. Earlier native startup/drain/analytics failures keep
their separate unestablished-cause audits. All markers remain 1 and the full goal
remains active; next prioritize whole-operation request/head/local cancellation
and the remaining physical metrics/inventory/backend/capacity release work.


## One deadline across entered event RPC dispatch (2026-10-14)

Current source gives node event/control RPC dispatch one immutable operation
budget across routing, retry/invalidation, local execution/output-gate waiting,
request submission, response head and body. Cancellation is selected once around
the operation and wins ready-result/deadline ties. Explicit never-connected/stale
verdicts update submission certainty; pre-invocation timeout/cancellation is
Unavailable, exhausted known-stale ownership is OwnershipLost, and a possibly
submitted mutation is Ambiguous. Deadline/cancellation verdicts do not carry the
activation network retry tag and do not undo prior commits. Existing typed
handler/host outcomes remain intact. A local request owns its activity before
its wait future is polled, so refusal/drop cannot leave a routed activity behind.
The peer abort guard is still disarmed after a proven response head.

Eight new tests cover unpolled expired waits, refusal/route certainty, modeled
post-submission effects/drop, cancellation/deadline/result ties, existing outcomes
and maximum budgets, actual LocalRequest activity release, and actual dispatcher
routing timeout with its CancelRoute message. The modeled atomic effect is not
SQLite durability evidence. Initial Clippy catches fixture type setup errors
(RpcData versus gated RpcOutcome, and an unwrap_err Debug bound); tests are
corrected to the actual API without changing production contracts. Qualification
is pending. SDK source egress-gate/channel-queue waits and every physical/process/
ownership/cancellation cut remain separate work; this entered-dispatch budget
does not establish complete end-to-end SDK qualification. All markers remain 1.


The SDK source RPC now captures its host operation budget before egress proof and
retains it through channel submission and result waiting. A pending proof timeout
returns Unavailable without submitting; queued-result timeout returns Ambiguous
and cancels that exact request. Two source-domain regressions exercise a held
real GateReq and an unconsumed real RpcCallReq. These are added after the initial
node compilation attempt; qualification of the combined source remains pending.
The node captures a later host budget, while the earlier source cancellation
bounds source-originated calls; no monotonic timestamp is serialized or compared
across hosts. Ordinary RPC does not gain an event deadline. All markers remain 1.


Event RPC deadline budgets include the canonical, nonnegative `__streamNext`
wait when it is within the captured host polling policy, plus the host transport
budget. That combined absolute deadline is captured once per source/entered-node
operation and is never reset at response headers or a retry. This preserves
empty-poll results when the default 30-second wait exceeds the 15-second transport
budget. Budget inspection accepts at most 16 KiB and bounded structured objects/
values/depth, reads only the array/body/wait field without alias expansion, and
never grants extra time to malformed or over-policy waits. Two parser regressions
cover JSON and structured arguments, canonicality, caps and saturating addition.
The source changed during the earlier build; fresh qualification is required.
All Cellar-owned markers remain 1.


## Product contract and developer entry points (2026-10-15)

PR #27 is merged. Follow-up work uses codex/event-delivery-product on the merged
main baseline, preserving the pending node/source deadline changes above. The
new docs/26-event-delivery.md is the developer entry point: source atomicity,
stream reads versus leased consumption, inbox/proof-before-ack, replay identities,
retention pressure, typed outcomes, and owner-routed operations. It is registered
in the offline catalog and linked from SDK/storage/work/example documentation.
The stale application-owned-outbox flow now describes the implemented framework
path and explicitly retains its under-development status. Search, flow selection,
step navigation, failure details, guide target and the regenerated PNG were
checked in an isolated agent-browser session. Documentation checks pass for
36 documents/6 flows/386 local references; dependency checks cover 403 packages
with no JavaScript engine, and 62 Python fault-harness guards pass.

ErrorCode::as_str and Display use canonical wire names; DeliveryError documents
is_permanent as automatic outbox lane blocking, not universal retry authority.
Missing bindings return typed InvalidRequest and closed domains Unavailable. A
new regression checks canonical names and typed errors under application context.
Rust selected tests, workspace Clippy and rustdoc remain live or queued; source
changed after the intermediate build began, so it cannot qualify the final patch.
The branch remains a draft until final-source validation passes. Full backend,
ownership, fault, capacity, physical metrics and release gates remain open.
All Cellar-owned markers remain 1.
