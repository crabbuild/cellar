<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Qualification and evidence contract

Purpose: Failure oracles, workload profiles and evidence required for release.

Status: proposed workload contracts and release gates, not measured results.
`profiles.json` freezes candidate requirements. Change a missed target only through
an explicit reviewed profile revision, retaining the old run and its failure.
Do not silently raise budgets, remove failures, retry mutations with new IDs, or
compare different logical cell populations to manufacture an improvement.

## Tests and release levels

Every task has exact named native cases. Its executor brief first lists tests,
then runs each required case with `--exact --test-threads=1`. The evidence validator
requires the name in the inventory and a successful one-test result; Cargo exiting
zero after selecting no tests is a failure. These test targets are planned outputs,
not available commands until their owning task creates them.

Use real SQLite/LTX and Raft Engine for state assertions. Scheduler-only fixtures
with stub image markers can test fairness but cannot qualify durability. Both
materialized and extent state, empty enrollment, managed and unmanaged groups,
all read modes and mixed Quorum/S3 commands must appear in the matrix. Native
test execution is serial between fixtures, concurrent within each fixture.

| Level | Evidence | Claim allowed |
| --- | --- | --- |
| Package | Python validator/tests, source preflight and local links | Executable plan only |
| Native | All task cases on real engines plus injected crash/process cuts | Exact tested functional boundaries |
| Local deployment | Five owned Cellar containers/processes, actual RustFS, enforced resource limits and complete ledgers | Local measured workload only |
| Independent domains | Actual separate machines/failure domains and observed physical isolation under load | Only the measured topology/domain-loss envelope |
| Release | All tasks, regressions and required profiles at one immutable source | Complete cell-group capability within that envelope |

The final release requires all profiles marked required in `profiles.json`.
Container counts alone cannot satisfy the independent-domain profile. Million-cell
and Q2–Q4 scale claims remain separate larger experiments from the parent roadmap.

## Managed S3 component verification

The managed native target now implements these explicit backend cases:

- `bootstrap::lifecycle::rustfs_managed_s3_group_replacement_and_joined_restart`
- `bootstrap::lifecycle::maintenance::rustfs_managed_archive_maintenance_stop_return_and_resume`
- `bootstrap::lifecycle::maintenance::loss::rustfs_managed_maintenance_recovers_one_lost_membership_observation`
- `bootstrap::lifecycle::s3::rustfs_managed_s3_seed_refusal_and_source_failover`
- `bootstrap::lifecycle::isolation::rustfs_managed_isolated_voter_s3_replacement_retains_disk_and_fence`
- `bootstrap::lifecycle::recovery::rustfs_managed_failure_recovery_restarts_and_verifies_s3_before_removal`
- `bootstrap::requests::rustfs_managed_native_axum_read_write_and_retained_restart`

Run each by its exact name with `--ignored --exact --test-threads=1` in
`native_cell_groups_managed`. Configure an owned local backend with
`CELLAR_CELL_GROUPS_RUSTFS_ENDPOINT`, `CELLAR_CELL_GROUPS_RUSTFS_BUCKET`,
`CELLAR_CELL_GROUPS_RUSTFS_PREFIX`, `AWS_ACCESS_KEY_ID` and
`AWS_SECRET_ACCESS_KEY`. The helper accepts only a loopback HTTP endpoint,
requires an isolated prefix and supplies explicit credentials; absence fails.
Use a fresh prefix for every run. Never copy the private credential file into
evidence or a PR. Build first, record and copy the executables, then run cases
serially after writeback. Record and verify the pinned backend image, actual
resource limits and signed conditional-write/read-after-write probe separately.

Both image forms run. Replacement checks real S3 payload transfer, independent
SQLite histories/receipts, joint finish, lost settlement replies, owned drain and
retained joined-member restart. Maintenance covers initial and joined voters,
owned stop, offline confirmation, paused return and fresh resume. The fault case
denies target payload GETs before attachment, requires retained nonserving owner
and debit, refuses a peer-mode change, stops the source service and requires a
fresh election and completed repair. Replacement-local queries verify actual
responder, values and versions; new writes require S3 proof. A surviving original
voter's missing physical inventory must block catalog settlement and retain its
debit even after membership repair completes.

The isolated-voter case first joins the complete obsolete Node, then supplies
fixture isolation evidence. Reject evidence before rooted exclusion and reject
the wrong process binding without advancing the root. A saved pre-isolation
startup capability must return the typed catalog conflict, with both fleet root
and retained engine files unchanged. Replace that voter from S3, require fresh
stable RF3 and the exact `isolated_sources` receipt, lose the actual settlement
confirmation and reconcile the same request. Check both restored cell
images/receipts through replacement-local queries, explicit S3 writes, the
replacement's physical capacity floor and byte-identical retained obsolete log.
This fixture does not supply permanent Docker launch fencing or terminal node
removal. Keep the original eight reconciliation turns and five-second calls.

The managed HTTP case uses the counter's existing native and Axum adapters with
independently bound applications and catalog clients on three actual managed
Nodes. Cold startup must qualify capacity, reserve/publish/confirm group birth,
activate initial voters and replicate enrollment before HTTP admission. Check
independent cell state, immutable retry receipts, conflicting-input refusal and
cross-cell token refusal. Require a follower command refusal before explicit
write-origin admission, reject foreign namespaces and stale processes, revoke
one current process and require no mutation, then restore permission with a
newer revision and retry the same command. Require explicit S3 acknowledgement
through the follower as well as ordinary quorum responses. Force remote
minimum-version and linearizable reads
through every adapter/node, requiring the actual responder identity. Join the
leader, require successor service, then restart retained disks with new process
generations and require old receipts/version floors plus new writes. After two
joined voter stops, observe actual quorum loss and require both adapters to
refuse writes and linearizable reads. Shutdown joins every server and Node and
accounts for unresolved work; failures retain all voter directories. Both image
forms run. The same fixture supplies `ingress_cannot_cross_cell_authority` in
`native_cell_groups_public`, with the managed exact test names retained. These
local embedded Nodes and fixture capacity attestations are separate from the
complete lifecycle, independent voter-process, permanent fencing,
crash/partition and load gates.

The automatic recovery case uses the Node-owned failure service with the original
failure grace, probe interval and five-second turn limits. Hold an exact-boot
isolation callback, verify repair has not begun, refuse a duplicate worker, then
shut down and require
joined callback cancellation and released control admission. Restart the service
through S3 replacement while the independent verifier refuses payload GETs.
Require fresh replacement inventory, no terminal receipt and unchanged obsolete
disk. Stop again, restore reads and restart: require additional physical S3 GETs,
exact-operation terminal removal, no repeated committed isolation, empty movement
and reservation books, original receipts/new S3 writes and unchanged retained
obsolete disk. Both image forms start with cold private verifier inventories;
no business mutation drives the repair. These fixture attestations do not qualify
Docker launch revocation or the complete public lifecycle gate.

The lost-observation regression uses independently owned catalog clients over the
same actual backend. After handoff, hold one foreground local-member catalog read
without resuming it; background trust refresh and the controller's client remain
unaffected. The walk must return a transient refusal, retry that pending history,
and verify all three members within the original five-second renewal envelope.
Both image forms require one handoff and exactly one verified history. This case
ends at renewal; the unchanged full lifecycle case separately qualifies owned
stop, return, resume and capacity settlement. On failure, bounded read-only local
status and admission snapshots are recorded before cleanup; the original error
still fails the test.

Initial/final test handoffs reconcile actual leadership under one original
five-second envelope. Re-observe the current leader, capture its term and
membership revision, and use guarded transfer admission. NotReady or a lost
admission response requires observing actual state again; acceptance alone is
not election completion. Successful handoff still requires the target's actual
ready leader state. The original eight movement turns remain unchanged.

The fault case also blocks actual registry mutations after target payload
refusal. Require a witnessed denied write, retained pending journal and observable
cleanup error. Restore writes without issuing another learner request: the node's
idle owner must confirm retirement and sync the empty record. A second failed
retirement remains pending through source-service close; restore writes only after
that source joins so the surviving quorum can repair. Preserve the shared-backend
effect when the first fixture owns the underlying store.

After every old host joins shutdown, reopen only that source on its retained disk.
With registry writes denied, background discovery must find the original engine-
and archive-bound intent without a live quorum or another learner request. Restore
writes and require durable retirement without changing fleet authority. Gate an
actual archive read, start close, verify sealed admission and retained S3/sweep
ownership, cancel its waiter, then release the read and require joined shutdown.
Sample retained cell inventory before close seals engine I/O admission. Both image
forms run. Separate journal tests reject a foreign engine, ordinary archive-journal
purpose, missing state and missing identity; existing-only open cannot reconstruct
those records.

These are embedded native Nodes with separate domains, disks and TLS. Real RustFS
verifies the S3 adapter; it does not supply independent or resource-constrained
voter hosts. A source-service stop is distinct from process SIGKILL and permanent
platform fencing. Retain every missed five-second readiness window and failed
run; later passes cannot establish a fixed failover SLA or waive that failure.
Five-second calls and eight reconciliation turns remain unchanged. See
[progress](progress.md) for source-bound results and unresolved qualification.

## Shared-group workload harness

CG-14 provides `tools/perf/cell_groups.py` with source-pinned SQL/RustFS campaigns,
independent raw-evidence verification and owned process stop. The
[campaign guide](../../../../tools/perf/cell_groups.md) defines the implemented
private configuration and acceptance boundaries. CG-14's five named cases are
accepted at `23414b47`, with constrained five-process RF3/RustFS coverage and
bounded disks, archive-lag and logical-closure evidence. This closes the harness
feature task. Full release profiles remain CG-15/16 gates. The private S3 matrix
retains three ambiguous five-second writes; its passing minimized reproduction
is not a replacement for that failed matrix. See the latest [progress](progress.md).

The available CLI is:

```text
python3 tools/perf/cell_groups.py preflight --profile PROFILE --config PRIVATE_CONFIG
python3 tools/perf/cell_groups.py run --profile PROFILE --config PRIVATE_CONFIG --output FRESH_RUN_DIRECTORY
python3 tools/perf/cell_groups.py verify --run RUN_DIRECTORY
python3 tools/perf/cell_groups.py stop --run RUN_DIRECTORY
```

The complete harness configuration contract below also covers the remaining
deployment and recovery adapters. The private configuration must bind a run UUID, explicit Docker context or process
host inventory, immutable Cellar/RustFS image digests or executable manifest,
source revision, dedicated data roots, credentials-file paths, port allocation,
node CPU/memory/swap/disk limits, failure domains and fencing-authority endpoint.
Validate all required fields; missing values never fall back to the current Docker
context or a default data directory. Credentials and unrestricted Docker inspect
output stay out of reports. Unsupported resources fail before provisioning.

Use `tools/perf/raft_fleet.py` for managed-node provisioning patterns and the
shared-group daemon fixture under `crates/cellar/tests/native_raft_daemon.rs`
for native process kill and disk-erasure proof. Extend their existing owners. Own resources by immutable IDs, run labels and exact paths. The destructive
profile must confirm ownership and join stopped processes before removing only
the recorded voter roots. Keep RustFS data and retained evidence outside voter
roots, build cleanup and global prune paths. `stop` retains data for inspection.

## Workload accounting

Build once before timing. Record source, compiler/features, binary digest, actual
CPU/memory limits, disk/filesystem, host activity, RustFS digest and TLS topology.
Run cohorts serially with fresh identical initial data and a frozen arrival trace.
Compare the old one-cell baseline against the replacement, and compare group
occupancies 1/8/32/64/128 within the replacement. Capture measured actual groups,
not only configured target occupancy. Each logical cell has a separate SQLite DB.

The offered-load ledger records stable cell/client/sequence, input digest, request
start, deadline, response/timeout/cancel/refusal, returned result, cell version,
group witness and requested/achieved durability. Keep every offered request in
throughput/error denominators. Reconciliation is a separate phase and cannot
retroactively turn a timed-out request into a successful timed sample.

Independently verify all acknowledged effects and receipt results on every serving
replica and the selected recovery cut. Check per-cell real-time strong-read order,
at-most-once mutation by command identity, allowed ambiguous outcomes and no
overlapping writable owners. Deliberately corrupt oracle inputs to prove it detects
lost effects, borrowed versions, double execution and stale strong reads.

Measure CPU cores, process RSS, descriptors, group/cell counts, outgoing control
bytes below/above TLS with the exact boundary stated, log/image/manifest write
bytes, fsyncs, S3 PUT/GET bytes/requests and retained/archive lag. Report p50/p95/p99
and sample counts by durability, hot/cold cell and fault phase. Warmup, setup,
election, cache restore, archive drain and joined shutdown are separate intervals.

For ratios, require a positive meaningful matched baseline; a zero denominator
cannot pass a reduction gate. Resource peaks use actual OS/engine observations,
not configured limits or synthetic host metrics. A recovered follower is not proof
that every replica or every cell recovered. Selected-cut Quorum suffix loss is
reported separately from missing S3-protected history.

## Required fault schedule

Kill/restart at image sync, manifest sync, marker commit, enrollment, snapshot
install, archive publication, reclaim, seal authorization, parent seal, child seed,
root publication, activation, retirement, park and wake generation closure.
Inject corrupt/missing objects, duplicate/delayed messages, stale routes, unknown
CAS responses, leader/controller loss, a slow/minority follower, disk-full/sync
failure, network partitions and bounded S3 outage. Run foreground writes and
strong reads while splitting/merging and moving replicas.

For zero-quorum tests, verify the archive/ledger boundary first, fence all possible
old voting lineages and demonstrate launch revocation. Delete all voter disks,
restore complete groups from real RustFS with no surviving peer images, verify
every cell and receipt, reject old tokens/old-host restart, acknowledge new writes,
and restart the fresh quorum on its new disks. Repeat before/after topology
publication and after reclamation. Managed inventory/transition journal failures
must fail closed. Unreconciled old-origin outbox work must remain refused.

## Full primitive compatibility (CG-17)

CG-17 is an implementation and qualification task. CG17-01 is in progress with
no accepted cases; the existing SQL and effect tests do not establish this broader
support. Its 32 exact cases are defined in `tasks.json` and displayed by
`plan.py show CG-17`. Implementations of those planned cases are required
outputs, not tests that this plan change has passed. CG-15 and CG-16 depend on
CG-17; the validator rejects removal of the task, required cases or cutover
dependencies.

The completion contract is that every existing public Cellar primitive works
through normal SDK declarations and host adapters on the shared runtime. Preserve
public behavior, transaction boundaries, error types, authorization, ordering and
retry semantics. Any necessary public API change needs an explicit contract
review; a blanket UnsupportedCapability, smaller supported product scope or
legacy per-cell fallback cannot satisfy compatibility. Unsupported configurations
must still refuse activation while their implementation is incomplete.

Build the inventory from all exports in `cellar::sdk`, their public application
modules, built-in cell classes, bindings and supported feature modes. Each entry
records public symbols/examples, the owning adapter, durable state and provider
dependencies, minimum acknowledgement proof, ordering/deduplication contract,
recovery behavior, tests and retained evidence. New exports must extend this
inventory. Every entry must map to a stateful family below or an exercised
stateless/ephemeral regression; unclassified entries fail the coverage oracle.

| Family | Required public behavior and state |
| --- | --- |
| Storage, SQL and KV | Transactions, conditional updates, list/query semantics, rollback, consistency modes and command receipts; preserve separate boundaries for bindings to other cells or providers. |
| Queue | Producers and consumer callbacks, delayed delivery, batch/lease generations, acknowledgement, retry, purge, retention and dead-letter transitions. |
| EventStream and outbox/inbox | Transactional emission, lane ordering/sealing, replay, subscription checkpoints, delivery identity and atomic receiver receipts. |
| Workflow | Creation, step results/journals, waits, timers, retries, streams, cancellation, retention and rollback. |
| Alarms and cron | Durable due times, generations, cancellation, occurrence identities and history; execution resumes without a client request. |
| Filesystem | SQLite-backed file data and metadata, file-plus-business transactions, descriptor/inode/cookie behavior, sync/fsync, quotas and revision-aware reclamation. |
| Blob and external payloads | Public object conditions, ranges, checksums and multipart behavior; ordinary KV external values and provider volumes retain explicit publication, recovery, fencing and retention contracts. In-SQL Raft Blob values are a separate storage mechanism. |
| HTTP, services, cell calls and RPC | Normal request/response, authorization, streaming, original deadlines, cancellation and explicit uncertain outcomes. |
| WebSockets | Upgrades, callbacks, attachments/tags, auto-responses, hibernation and defined reconnect behavior under fenced ownership. |
| Container, supervision and sandbox | Desired state, provider-enforced execution fencing, stable operation identity, restart and persistent storage recovery. |
| Other SDK exports | Assets, cache, facets, loader, streams, sockets, HTML rewriting, codecs and utilities retain their public behavior and applicable feature regressions. Record which resources are ephemeral. |

Use the existing replicated storage and effect services as the foundation.
Ordinary async callbacks need an owned leader-side orchestration adapter that
submits durable mutations through shared consensus. Keep synchronous SQL callbacks
restricted and keep locks out of await points. Persist intent atomically with
business state; schedule external work under current execution authority and
settle results through replicated commands. Failover must rediscover pending work
from committed state. Shared bounded schedulers, worker pools and due indexes
must not become one worker, timer or open SQLite connection per idle cell.

Compile declared primitive requirements into deployment validation before opening
ingress. Validate request/envelope sizes, result/receipt capacity, client identity
floors, scheduler metadata, images, archive dependencies and provider capabilities.
For example, the signed inbox envelope has a 60 KiB bound: a SQL-only 1 KiB input
profile must refuse that declaration, while an explicit sufficient bounded profile
must run it. Never silently increase deployment quotas or weaken capacity checks.

For every stateful family, the evidence matrix must contain:

1. Normal public requests and retries after a response is lost following commit.
   Verify business state independently, exact result identity, ordering and the
   primitive's actual deduplication/lease rules.
2. Actual leader process SIGKILL, partition/quorum loss, follower catch-up,
   joined-member replacement and retained restart. Reject minority writes and
   strong reads; exercise stale executor, callback and lease settlement.
3. Whole-cell movement, split, merge, placement and parked wake with pending work,
   including interrupted transition-controller operations. No work can disappear
   or acquire a new identity merely because its physical group changes.
4. Real RustFS outage, bounded archive debt, backend return, actual local log
   reclamation and independent restoration of complete data/metadata/receipt and
   payload closure. Newly live or elected leaders must attach their existing
   archive binding without requiring an extra restart to unstrand S3 effects.
5. Permanently fenced zero-quorum recovery after deleting all owned voter disks.
   Establish fresh incarnations and quorum, reject old writers/tokens/grants,
   verify the selected recovery cut and resume safe pending work.
6. Cancellation, capacity saturation, ownership revocation and joined shutdown
   during outstanding callbacks/provider operations, followed by recovery of
   unfinished durable obligations.

Run both materialized and extent images, managed and unmanaged namespaces,
Quorum and explicit S3 durability, occupancy one and multiple cells per group,
and at least two groups on the same nodes. Applicable SDK/native/Axum paths and
CLI/admin lifecycle must use normal registration and adapters. Use the CG-14
constrained RF3 process/RustFS harness and the real supported container provider
for execution tests. Native component checks supplement this campaign. Missing
providers or features cannot convert required cases into ignored or passing
tests. Feature-off behavior remains covered by the workspace regression matrix.

Quorum acknowledgements protect committed cell state; asynchronous archives may
lag. Measure the Quorum-only suffix/RPO separately from the S3-protected recovery
cut. Metadata replication alone cannot establish external payload durability.
External delivery is at least once unless the receiver's own protocol supplies
deduplication; arbitrary network effects are not universally exactly once.
Cross-cell calls remain separate transactions even within one group. Sockets,
streams, caches and process memory retain explicit transport/disposable semantics;
test the promised reconnect/restart behavior without inventing continuity.

After disaster recovery, old-origin effects with unknown outcomes require
authenticated reconciliation or an explicit unresolved refusal that retains the
work obligation. Never mint a new delivery identity to bypass ambiguity. A
provider without the required fencing/reconciliation contract keeps that adapter
unqualified and CG-17 open; stored metadata is not an execution proof.

The complete-coverage case must check the SDK inventory against actual family ×
scenario × configuration results, reject omissions/ignored/zero-selected tests,
and bind public application binaries, source hashes, configuration, process/fault
observations and state/effect ledgers to one source. Preserve failures and provider
limitations. CG-15 repeats the matrix with its combined fault campaign; CG-16
reruns all exact cases and the workspace/features release gates at the final
source. No full compatibility or cutover claim precedes those gates.

### Audit-driven delivery packages

The 2026-11-07 audit identified integration and contract gaps beyond the original
family-level cases. The eight packages below refine CG-17, not its completion
scope. Their exact actions, test names and oracles are in `tasks.json`; the
executor brief prints each package with its commands. All outputs and cases are
planned work until implemented and evidenced. No acceptance status changes merely
because this plan is more detailed.

| Package | Delivery | Ordering |
| --- | --- | --- |
| CG17-01 | Public API/capability inventory, explicit coverage requirements, report schema and independent acceptance validator with negative fixtures | Begin now on the CG-07/08 foundations; establish proof before family ports |
| CG17-02 | One shared activation/Storage backend, typed framework-operation authority, private transactions, receipts and output proof integration | After inventory contracts; reuse for every family |
| CG17-03 | Async invocation/transaction/attempt state machine, nested work and external executor fencing/reconciliation | After the storage bridge; preserve synchronous SQL callback restrictions |
| CG17-04 | Committed discovery and generation-specific settlement for every due-work reason, with clock and park/wake behavior | After storage and execution ownership |
| CG17-05 | Normal Storage/SQL/KV/filesystem APIs and actual external payload publication/recovery/GC | After applicable storage/execution contracts; reuse existing conformance suites |
| CG17-06 | Queue, EventStream and Workflow state machines, receipt horizons and schema/deployment changes | After storage, execution and work discovery; use payload support where required |
| CG17-07 | Remaining public/provider adapters, native/Axum/daemon/CLI/admin wiring and Fleet/bucket/mixed-mode regression | After each adapter's foundations; assert the selected backend |
| CG17-08 | Complete constrained RF3/RustFS/provider fault matrix, cancellation/resource proofs and release evidence | After packages 01–07 and every task-level lifecycle dependency |

Package checkpoints permit parallel feature delivery; they do not mark CG-17 DONE.
Its existing CG-07/08/10/12/13/14 acceptance dependencies remain. CG-15/16 still
require the complete task. Do not wait for the final crash campaign to begin the
shared Storage bridge or provider contracts.

### Shared execution and proof contract

The owning integration includes `cellar/application`, `cellar/runtime`,
`cellar/storage`, `cellar/node`, `cellar/execution`, the pure core, SQLite/LTX/value
adapters and supported language adapters. Changes belong beside those existing
owners, not in one catch-all primitive dispatcher. Python/JavaScript feature
regressions must preserve their supported contracts without pulling an optional
language engine into the native core.

Specify the invocation, transaction, attempt, command and external-operation
identities and their Prepared/Submitted/Applied/Ambiguous transitions before
implementing asynchronous adapters. Those names are semantic states to define,
not APIs already delivered. Distinguish:

- A synchronous local Storage/file operation returning to application code.
- Private preparation and local transaction completion.
- A quorum-committed and applied exact receipt.
- Protected output or external dispatch released by its required proof.
- Explicit sync/fsync and stronger S3 coverage.
- An uncertain submitted operation retaining its original recovery obligation.

Preserve ordinary nested/async transaction behavior: read-your-writes, child
drainage, rollback/panic, reentrancy, escaped-view refusal and cancellation. Awaiting
external work cannot publish half an atomic transaction, run I/O inside a
replicated SQL callback or let an obsolete continuation settle under a new leader.
Replaying one transaction is not permission to rerun an entire callback that
already committed other transactions. A later pre-admission rejection describes
only that attempt; it cannot clear earlier uncertainty.

The ordinary RPC adapter uses these identities and transitions:

- The invocation is the exact cell history, client/sequence, operation contract
  and request digest. Each root transaction has a monotonically increasing
  checkpoint number; nested transactions remain savepoints within that root.
- A Prepared checkpoint owns one private SQL image and a fresh proposal/preview
  identity. Submitted checkpoints retain their original producer, cell admission
  and byte reservation through response expiry until exact apply or newer-term
  reconciliation resolves them. A timeout supplies no non-commit proof.
- An Applied checkpoint updates the invocation's existing receipt row with its
  checkpoint number. It uses the original client floor, receipt count, retry
  horizon and physical receipt quota. A completed invocation replaces that marker
  with its final result. Partial commits survive later handler failure.
- An unfinished marker is an ambiguous invocation. Public receipt lookup refuses
  to describe it as completed or absent, and retry admission refuses to rerun its
  factory or callback. A new leader restores the marker, fences the obsolete
  in-memory continuation and preserves the already committed state.
- The root callback retains its ordered slot and Rust return value until the
  exact checkpoint is applied. Explicit S3 durability additionally verifies that
  checkpoint's pending receipt from recovered archive bytes before continuation.
  Background archival for Quorum requests retains its documented RPO.

Ordinary RPC `Storage::sync()` reserves an owned boundary in the same call-order
queue as root transactions. It checkpoints preceding private SQL/KV writes and
waits for the invocation's exact quorum/S3 proof before completing. Dropping its
result future does not remove the admitted boundary. Sync inside an open
transaction refuses before scheduling: it cannot publish a savepoint or split an
atomic callback. Synchronous registered commands and immutable queries retain
their separate completion contracts and cannot schedule RPC checkpoints.

Canonical SDK commands, queries, receipt inspection and enrollment from ordinary
RPCs capture target bounds, namespace admission and an absolute deadline at the
call boundary. Their owned task proves the ordered source checkpoint before
actual dispatch. Target durability and consistency remain independent, and the
caller persists its original target identity/input. A dropped response retains
admitted child work and gates parent output; this is not a cross-cell transaction
or an automatic handler-recovery protocol. Synchronous callbacks, immutable
queries and open atomic transactions cannot acquire this dispatch permit.

A proven SDK wait can release the source writer, including for a recursive call
to the same cell. If the child reaches admission before its parent parks, only
a received writer-lease refusal before enqueue grants a wait under the original
request identity, inputs, route ticket and deadline. A worker/callback error or
lost reply never grants callback replay. Leader, route, history, activation and
original deadline checks fence the continuation and preserve uncertain outcomes.

Ordinary cell instances are activation-owned caches, not replayable application
state. Constructor Storage handles may enter later invocations of that same live
activation; invocation handles retain a distinct non-reusable token. A successful
quorum-applied final receipt admits the instance back to the idle cache.
Normal public receipt reconciliation may retain the instance only at its exact
prepared final image with the original leader fence still current. Preparation
failure, rejected proposals and changed-term reconciliation retire it.
Receipt/input refusals before native execution preserve an untouched instance
only while its original cache fence remains current.
Cache reuse requires the same class
factory, exact cell version, physical engine, route and leader term. Its weak
fence retains no image, log reservation or engine owner. Node-wide instance
capacity uses the configured SQL pending-cell ceiling; idle instances expire
after sixty seconds, with bounded pruning on existing SQL workers. Eviction,
leadership/routing changes and joined shutdown retire constructor handles too.

These transitions are delivery contracts, not acceptance claims. Complete their
public recovery, fault and feature matrix before accepting CG-17. The receipt
schema adds a protected pending-checkpoint field under the existing format-1
internal hard cutover; matching nodes and applications require a rebuild, and
older receipt images are not migrated by this change.

The common kernel reuses existing command/receipt, LTX, alarm and outbox/inbox
owners. Built-in schemas and internal operations retain separate host-minted
authority. Bind publish/read/consume/admin and provider grants to the correct
origin, target and operation; caller arguments cannot mint internal provenance.
Record transitive binding dependencies before activation. A caller's Quorum
setting neither joins another binding's transaction nor proves its provider data.

For external execution, define intent persistence, operation identity, execution
fence, result settlement, provider lookup/reconciliation and idempotency horizon.
Exercise callbacks that continue after timeout. A Workflow generation is not by
itself a current leadership or infrastructure fence. An already transmitted
arbitrary request cannot be unsent; preserve its documented at-least-once/unknown
outcome. Require real provider enforcement only where that adapter promises it;
otherwise report the weaker external contract explicitly without weakening the
cell's replicated state proof.

### Storage SQL cursor ownership

`Storage::sql_cursor` uses the existing lazy typed SQL engine in native turns,
private Raft commands and committed Raft queries. The worker owns statements and
cache entries in the exact command/query frame. Public tokens contain owned
column/row buffers and cancellation signals, never raw SQLite handles or image
pins. Query exit, command completion/abort and frame destruction finalize every
statement before the connection or committed-image owner is released.

A cursor created through constructor-cached Storage binds the current invocation;
the cached Storage itself may be reused, but its SQL/KV cursor may not enter a
later invocation. Cursors retain ordinary savepoint behavior within the live
invocation. A live SQL VM retains its connection and prevents connection-free
parking; other cells can continue using the shared SQL worker. Idle caches are
cleared before a frame parks and may resume against a different working image.
Dropped tokens outside a turn only signal cancellation; the owning worker prunes
them before its next poll/checkpoint. They cannot append unbounded cleanup work
to an idle activation or finalize another invocation's VM.

Schema rollback can invalidate an existing SQLite read VM. Native and Raft
cursors preserve the already-prefetched row and deliver the following step's
error on the next call. A rollback-free checkpoint success test must remain
separate from the paired native/Raft schema-rollback oracle.

Unfinished write/`RETURNING` statements must be consumed or dropped before exact
checkpoint or final-output publication. A statement failure retains the existing
SQLite transaction-poisoning rules. Immutable queries preserve their read-only
and framework-table authorizer on every prepare/step, including automatic schema
recompilation. No output or child dispatch acquires weaker durability.

`SqlLimits` retains finite live-cursor count and sampled statement/bind-byte
budgets in the unrestricted profile. Native and Raft paths share the same lazy
parser, prefix execution, typed values, deferred errors and row/write counters;
this does not eagerly materialize arbitrary result sets or relax SQLite limits.
The byte budget is not a page-cache, sorter, application-buffer or RSS ceiling.

Qualification must exercise typed BLOB/non-finite values, aliases, multi-statement
prefixes, concurrent cursors, `RETURNING`, failed/missing bindings, protected SQL,
limits before mutating steps, transaction boundaries, independent roots,
constructor-cached handles, escaped command/query tokens, cancellation and joined
teardown. Keep native Storage regressions and both image/Quorum/S3 RustFS cases.
This ownership delivery does not waive complete named bindings, committed due
work, family adapters or any full CG-17 scenario/configuration gate.

### Storage keyset cursors

`Storage::list_cursor` uses the same keyset range implementation in ordinary
Fleet storage and shared Raft command/query storage. Each step reads one owned
row; it retains no SQL statement, connection, worker or image pin. Prefix,
start/end/start-after, reverse traversal and limits retain their existing
semantics. Inserts/deletes around the last key cannot shift an offset.

Raft cursors validate their original invocation or committed query on every
step. Starting another iterator on the same Storage owner invalidates the older
one with a non-wrapping generation. The Storage owner retains only the current
range; invalidated handles retain no old range buffers. Dropping an old handle
cannot clear a newer iterator. A cursor may continue across a transaction
boundary in its live invocation, reading the then-current image. After rollback,
subsequent steps read the surviving image rather than a retained private snapshot.
A cursor cannot enter a different invocation. Query cursors
remain bound to their selected committed image and cannot escape its callback.
Cursor positions are ephemeral and are not restartable durable work.

The public RF3 and RustFS Storage cases must check structured values, empty KV
schema, all range/limit directions, mutation between steps, invalidation, rollback,
expired views and exact cell/version isolation. The ordinary RPC case must hold
an iterator across an independently proven async root and observe the newly
inserted key. Preserve the ordinary native Storage conformance regression.
These checks qualify this API boundary; SQL cursors, other Storage methods,
named namespace KV bindings and the full family matrix remain separate work.

### Logical-name discovery

`RaftCells::resolve_cell_history` observes an existing name's current enrollment
through a fresh quorum read. It returns the selected incarnation or absence and
never initializes a name. Only linearizable consistency is admitted. The lookup
uses the existing namespace read authorization, including current-voter access,
and authenticated, purpose-specific bounded wire frames. A command grant or
version-token signing key is not required. Source and
replica routing retain the original admission, attempt budget and deadline,
including a future first polled after its deadline.

Inside an ordinary JSON RPC, lookup waits for the source root's exact requested
durability proof. The resulting exact-history handle may be constructed in the
current invocation, while its command/query still requires separate admission and
source proof. Synchronous commands, immutable queries and open atomic callbacks
refuse before dispatch. Discovery neither supplies invocation authority nor
resolves whether an earlier mutation committed; a returned absence cannot clear
an uncertain outcome. A caller persists and reuses the original child identity.

The native and isolated RustFS name-discovery cases must check read-only remote
resolution without constructing the application, absent names, source-proof
ordering, handle construction inside an ordinary RPC, exact replay, callback
refusal/rollback, original deadline expiry, follower resolution after leader
transfer and refusal without quorum. The wire tests must reject cross-purpose,
truncated, oversized and foreign-name/schema frames. These component cases do
not establish declared `Context` binding support, automatic enrollment, a typed
RPC port, automatic invocation recovery or complete CG-17 acceptance.

### Additional primitive-specific oracles

The twelve audit-driven cases supplement all twenty original cases:

| Contract | Additional oracle |
| --- | --- |
| Activation/Storage bridge | Actual shared-group histories and isolated private state; ordinary factory/storage handles cannot hide a Fleet implementation behind a Quorum response |
| Async transactions/output gates | Hold work across awaits and leader replacement, including nested children and partial multi-transaction invocations; verify original identities and precise output boundaries |
| External execution | Hold a provider request through timeout, movement and owner replacement; enforce or reconcile the original operation under its real provider contract |
| Due work | Recover outbox, stream/resource/filesystem maintenance and sandbox cleanup plus Queue/Workflow/cron obligations without client requests; completing one installation preserves other deadlines |
| Payload closure | Fault upload/reference/archive ordering; retain pending uploads, readers, checkpoints and older archive cuts; inspect actual restored bytes after newer values are replaced/deleted |
| SQL/KV bindings | Use ordinary Database/Session/KV APIs; preserve session/bookmark, conditional revision, TTL, list/cursor, bulk/streamed-value and independent-provider semantics |
| Filesystem | Reuse descriptor, inode, rename/unlink, offset, directory-cookie, sparse-chunk, transaction, sync/fsync and quota/GC conformance, including typed uncertain outcomes |
| Receipt horizons | Explicit pruning, retention, partial batches/DLQ transfer, purge and delete/recreate; preserve documented ended-deduplication behavior and required identity floors |
| Schema/deployment | Declare supported change/refusal protocols for primitive tables and application/Workflow definitions; incompatible deployments and stale executors cannot silently operate together |
| Clocks/timers | Forward/backward wall changes, suspension and different leader clocks around leases, TTL, sleeps and cron; preserve monotonic request deadlines and generation fences |
| Coverage rejection | Independently reject incomplete API/scenario/configuration reports, even if every top-level test name prints a pass |
| Backend regressions | Explicit conformance factories for ordinary fleet and bucket durability, feature-off and supported mixed deployments; no requested Raft operation silently falls back |

Keep SQLite-backed filesystem chunks separate from external Blob/KV references
and provider volumes. Descriptor tokens are ephemeral even when their file data
is durable. Workflow output chunks and EventStream records are durable state;
ordinary network byte pipes are not. Fully qualified API identities prevent the
word "stream" or "bucket" from collapsing these different contracts.

Derive a feasible capability profile from the intersection of public application
limits and actual envelope, result/receipt, private image, staging, worker,
scheduler, archive and provider limits. Bounded profiles must accept valid
applications and refuse intrinsically incompatible declarations before ingress.
Neither `CellLimits::unrestricted()` nor an adapter port can increase hard Raft or
deployment ceilings. Exhaustion must retain entered owners until joined completion.

For schemas and deployments, distinguish an application/primitive change contract
from conversion of existing Fleet data. Preserve normal supported SQL migration
operations; explicitly refuse incompatible class/definition changes before
activation when no reviewed transition exists. Do not infer a rolling upgrade
protocol, add old format readers or waive pending obligations after undeploy.

### Machine-readable inventory and acceptance evidence

CG17-01 owns the following outputs. The starter catalog, report schema and
independent gate are present. The catalog's `partial` status blocks acceptance
until the full export audit and real evidence producers are delivered:

- `primitive-inventory.json`: fully qualified public operations, built-in resource
  classes, symbols/examples, feature conditions, owners and schemas, persistent
  versus ephemeral state, permissions, provider/binding boundaries, acknowledgement
  proof, ordering, retry/retention and recovery contracts.
- `primitive-coverage-requirements.json`: the reviewed mandatory coverage cells
  for that inventory and feature contract. Each cell has a stable ID, operation
  set, family, scenario/fault cuts and configuration requirements. Applicability
  derives from the public contract, never from which providers happened to run.
- `primitive-coverage.schema.json`: strict format-1 report fields and bounded
  types, distinguishing acceptance results from failed development attempts.
- `tools/multi-raft/primitive_coverage.py` and its negative tests: the independent
  validator called by the actual acceptance and release verifiers and CI.

The report must bind the exact inventory/requirements digests, frozen source,
public application and voter/CLI executable manifests, feature/provider settings
and hashed command/ledger artifacts. Each required cell names the actual selected
case and result, image form, namespace mode, durability minimum, occupancy/group
topology, applicable public path and provider. Stateful cases also retain physical
process/fault observations, required versus achieved proof, original request/
effect identities and state/payload/resource reconciliation. Several cells may be
covered by one real run, but every claimed cell must have its actual oracle and
configuration evidence. A manually asserted coverage label is not enough.

Compare the public export inventory with the supported build/deployment surfaces,
including aliases, built-ins and applicable macro/feature-generated APIs. New or
unclassified operations fail inventory validation. Pre-existing documented
unsupported methods, such as an already unsupported utility operation, must be
classified and have refusal regressions; that exception cannot omit a supported
stateful family or excuse a missing new adapter.

Negative fixtures must remove one required API/family/scenario/image/namespace/
durability/feature/provider/path, duplicate or mislabel a cell, substitute an
ordinary backend, skip/select zero tests, omit physical fault/payload evidence,
corrupt hashes or mix source/binary revisions. All must refuse. Keep failed
attempts without letting them satisfy acceptance. No mandatory cell becomes
inapplicable because infrastructure is unavailable or a feature implementation is
missing. Feature-off regressions are separate required observations.

`plan.py check_evidence` invokes the independent verifier for CG-17 and CG-15/16
combined-source reruns. A `primitive_coverage` artifact in task evidence binds
the report to the contracts at its source revision. CG-15/16 additionally record
`primitive_test_executable_sha256`, separate from their campaign executable.
The [runner contract](../../../../tools/multi-raft/README.md#primitive-compatibility-evidence)
defines the test-emitted provenance and artifact shape. The
`primitives_complete_coverage_evidence` case must exercise that integration with
actual runtime artifacts; the current synthetic negative tests only qualify the
verifier's refusal behavior. Complete inventory, artifact producers and the
runtime matrix remain required before acceptance.

Use backend factories to reuse existing public conformance suites. Explicitly
exercise ordinary Fleet namespaces under both `fleet` and `bucket` durability,
alongside supported feature-off/mixed deployments. Keep these modes supported.
The forbidden fallback is acknowledging a requested Raft mutation through one of
those weaker proof paths. Co-located and cross-group bindings retain their own
transaction and provider boundaries in every mode.

## Task evidence

Store local raw evidence under an ignored repository-relative directory, for
example `.cellar/cell-groups-evidence/RUN/`; CI may download sanitized artifacts
there before validation. Do not commit credentials, cell data, opaque tokens or
the previously excluded MR-13/MR-14 progress JSON files. Add a relative record path
to a task's `evidence` only after its implementation and checks actually pass.

A task record uses this shape (example values are placeholders, not evidence):

```json
{
  "format": 1,
  "task_id": "CG-01",
  "source_revision": "FULL_40_HEX_COMMIT",
  "executable_sha256": "64_HEX",
  "qualified_executable_sha256": "DEPLOYED_CELLAR_BINARY_64_HEX_WHEN_PROFILES_ARE_PRESENT",
  "environment": "Actual compiler, features, OS and resource observations",
  "limitations": "Exact scope and untested boundaries",
  "source_inputs": [{"path": "crates/...", "sha256": "64_HEX"}],
  "commands": [{"argv": ["cargo", "+1.98.1", "test", "..."], "exit_code": 0,
                "output": ".cellar/.../stdout.log", "sha256": "64_HEX"}],
  "profiles": [{"path": ".cellar/.../profile.json", "sha256": "64_HEX"}]
}
```

`source_inputs` must include the task's pinned source anchor paths and its test
target; the validator verifies each hash against the named Git revision. Include
all runtime sources changed since the frozen `tasks.json` planned-at revision in
the record as well. Later source-baseline reviews do not rewrite this evidence
boundary or waive earlier implementation inputs. Test inventory and exact
one-test outputs must be hashed and retained. Every command in `plan.py show`
verification must appear with exit zero. Earlier failed runs stay in the run
directory and are described in the final report; do not aggregate passes across
different binaries into one task record.

For CG-16, one record must contain all new tasks' verification commands at the same
release revision, plus the repository checks listed below and all required profile
reports. Dependencies' earlier passes alone cannot qualify a changed release.
The task record must not claim it verified different executable inputs. Later
plan-only status/evidence commits are permitted; the release command compares all
other repository inputs against the qualified source and rejects dirty files,
including examples, vendor code, build configuration and newly added input paths.

Profile reports bind `profile_id`, `source_revision`, `executable_sha256`, the
SHA-256 of the entire frozen `profiles.json`, actual environment observations,
numeric `metrics`, and hashed artifacts for `ledger`, `measurements`, `topology`,
`fault_schedule` and `verification`. The profile must retain its controls,
repetitions and parameter matrix in those artifacts. `plan.py` checks every numeric
gate, required topology and artifact hash. A bare `passed: true` is insufficient.
Values must be finite and nonnegative; null, missing, NaN and boolean measurements
refuse. The report and every replacement cohort must match the task record's
`qualified_executable_sha256`. The primary `executable_sha256` identifies the native
test artifact; these separate fields avoid equating it with the deployed server.

The measurements artifact is JSON with `metrics` equal to the report summary and
`cohorts` containing exactly the declared occupancy × image mode × repetition
matrix. Each cohort records:

```text
variant: replacement | historical
occupancy, image_mode, repetition
source_revision, executable_sha256, ledger_sha256
cells, nodes, voters, offered_requests_per_second, measured_seconds
metrics: numeric observations for that cohort
```

Historical controls are required only where `historical_control` is true, at
occupancy one and the pinned historical revision. Replacement controls and
candidates use the same qualified binary. Every cohort must meet its frozen
population, resources, arrival rate and minimum duration. Correctness/error gates
apply to every cohort, including historical and occupancy-one controls. Performance
gates apply to `gate_occupancies`. Summaries use the worst eligible cohort: maximum
for a max gate, minimum for a min gate, and equality in every cohort for an eq gate.
Never average a slow or failed cohort into a passing result.

Ratio observations must equal the numerator divided by the matching control's
denominator, with the same image mode and repetition; `ratios` defines the exact
metric names and control occupancy. A missing/zero control refuses the ratio.
Hibernation's active and parked rates are separate phases of the same cohort.
The native harness derives the other measurements from actual ledgers/OS samples;
the plan validator does not replace the workload oracle or establish physical
infrastructure truth from a self-reported number.

The topology artifact is JSON with `kind` and `nodes`, each containing `id`,
`physical_host` and `failure_domain`. Independent-domain runs require a different
physical host for each node and at least three failure domains. The verification
artifact records `source_revision`, `complete: true`, `ledger_sha256` and
`measurements_sha256`, binding its full history oracle to the actual retained
artifacts. Retain the detailed checks and physical isolation observations there,
not just these mandatory header fields. The fault schedule records requested and
observed events, timing, exact owned resource IDs and phase boundaries for every
cohort. Infrastructure evidence must still be reviewed before advertising a tier.

## Repository release checks

Run from the repository root with its own writable workspace target directory:

```sh
cargo +1.98.1 format --check
cargo +1.98.1 check-layout
python3 tools/check_native_dependencies.py
python3 tools/docs/check.py
cargo +1.98.1 check --workspace --all-targets --all-features --locked
cargo +1.98.1 test --workspace --all-features --lib --tests --locked -- --test-threads=1
cargo +1.98.1 test --workspace --all-features --doc --locked
cargo +1.98.1 clippy --workspace --all-targets --all-features --locked -- -D warnings
python3 -m unittest discover -s tools/perf -p 'test_*.py'
```

Feature-specific and external consumer checks in the SDK CI workflow remain
required. The new package runner supplements CI; it is not a substitute for it.
