# Design 007: Single-writer cells with independent read replicas

Status: proposed. Baseline: `0efd8f4d871199542f97f9fe0b74086a38606d02`.
Date: 2026-10-19. Priority: P1. Implementation risk: high.

This is a feature design, not documentation of an existing capability. All
new Rust names, routes, records and defaults below are proposed. Existing
contracts are identified separately in the source map. The companion
[implementation plan](008-implementation-plan.md) is the execution contract;
[qualification](009-qualification.md) defines evidence required for release.
The [application integration guide](010-application-integration.md) shows the
proposed builder, handler and public-request integration end to end.

## 1. Problem, goals and release scope

Each cell currently has one authoritative node. Owner-routed reads share CPU,
database access, admission and output-proof dependencies with mutations. Moving
other cells to other nodes does not distribute one hot cell's reads.

Add independently placed, queryable copies of persisted cell state. Primary A
continues to execute mutations. D/E continue to protect A's node-log history.
B/C execute explicitly registered queries against safe SQLite snapshots.
These sets may overlap, but no overlap is required.

The first release delivers:

1. Continuous replication of proven LTX history for policy-selected existing cells.
2. Side-effect-free framework query capabilities and immutable snapshot reads.
3. Explicit command/query registration, stale reads and minimum-version reads.
4. Correct restart, catch-up, fencing and warm takeover through existing recovery.
5. Bounded transfer, disk, memory, admission and lifecycle accounting.
6. Observable serving node, actual snapshot version and actual proof source.
7. Automatic replica selection, repair after node loss and proactive safe writer
   recovery without requiring a new application request.

Excluded: concurrent writers, a new consensus/election protocol, transparent
rerouting of ordinary GET/RPC calls, cross-cell transactions, arbitrary query
side effects, streaming query outputs, automatic query caching, demand-based
replica-count autoscaling, point-in-time backup, strong/linearizable replica reads, time-bounded
staleness, and counting read-replica disks as recovery witnesses.

Queries address existing replicated state. A query never creates a cell or runs
its constructor. No snapshot means `SnapshotUnavailable`, not proof that a cell
does not exist. Existing owner-routed application reads remain available.

## 2. Baseline and source map

Paths are relative to a full checkout of the pinned revision. Implement against
that baseline or a reviewed successor, verifying the drift anchors first.

| Existing path and anchor | Observed responsibility | Design consequence |
| --- | --- | --- |
| `docs/adr/002-cell-authority-and-proof.md:7` | One conditional owner/epoch; output proof; predecessor recovery | Extend serving roles; retain one write authority |
| `crates/cellar-logic/types.rs:169`, `OwnerRecord` | One node, lineage, epoch and ETag | Do not add read nodes as additional owners |
| `crates/cellar-logic/types.rs:206`, `CapacityPeer`; `rebalance.rs:1` | Advisory capacity and existing guarded owner balancing | Reuse observations and guard patterns; add replica-specific planning and reservations |
| `crates/cellar-logic/output_gate.rs:84`, `output` | Read outputs can reveal another request's unproven writes | Replica publication must obey the same no-unproven-output rule |
| `crates/cellar-logic/output_gate.rs:283`, `durable_reached` | Bucket proof still needs ownership verification | Upload completion is not the publication seam |
| `crates/cellar-logic/output_gate.rs:342`, `settle_gate` | Successful core settlement records proven position | Authorize safe-cut publication here, with stale-op/epoch checks |
| `crates/cellar/actor/mod.rs:3945`, `AwaitDurable` | Adapts host proof to core completion | Carry exact capture receipts through this path |
| `crates/cellar/durability/replication/backend.rs:2986`, `await_durable` | Tickets avoid SQLite fingerprint versus LTX TXID mismatch | Never cast current output positions into replica versions |
| `crates/cellar/durability/replication/backend.rs:4153`, `sync_cell` | Samples tickets before capture; uploads resulting LTX | Bind ticket coverage to exact captured artifact cuts |
| `crates/cellar/durability/replication/backend.rs:5487`, `apply_round` | Credits ordered fleet rounds | Record completed fleet cuts without bypassing core authority |
| `crates/cellar/durability/log/mod.rs:3937`, `FleetShipper` | All installed followers fsync, or bucket fallback | Read nodes must not enlarge this ensemble implicitly |
| `crates/cellar/durability/log/mod.rs:5540` | Missing complete fleet witness blocks recovery | B/C cannot substitute by asserting a readable watermark |
| `crates/cellar/durability/replication/backend.rs:3937`, `restore_snapshot` | Private non-owning snapshot restore | Reuse mechanics, add exact proof bounds and artifact pinning |
| `crates/cellar-ltx/src/replica.rs:28` | Continuous follow/apply deferred | A materializer must be implemented, not merely wired up |
| `crates/cellar-ltx/src/client/epochs.rs:1`, `EpochChain` | Restore can span continuation epochs | A replica version includes verified history, not just a TXID |
| `crates/cellar/storage/sqlite/mod.rs:860`, `open_at_epoch` | Normal open is writable and initializes framework state | Add a separate read-only opener |
| `crates/cellar/application/mod.rs:125`, `Cell` | Factories/invocations receive normal mutable `Context` | Register distinct stateless query callbacks |
| `crates/cellar/node/network/peer_tunnel.rs:49` | Existing tunnel routes owner calls | Add an explicitly typed query/transfer path |
| `crates/cellar-logic/Cargo.toml:13`, `crates/cellar-ltx/Cargo.toml:21` | Library tests disabled | Add normally runnable integration tests |

The architectural decision remains in `cellar-logic`. Adapters perform I/O,
verification and file operations, then report versioned completion events.
The read-replica module hides bootstrap, apply, snapshot ownership and reseed
behind one interface; this gives locality to correctness rules and leverage to
all SDK and transport callers. No second ownership mechanism lives in adapters.

## 3. Roles and records

### 3.1 Independent sets

| Set | Scope | Authoritative for | Not authoritative for |
| --- | --- | --- | --- |
| Primary owner A | One cell/epoch | Mutation admission and existing cell behavior | Unproven external visibility |
| Durability ensemble D/E | A node-log generation/fragment | Existing fleet durability evidence | Query readiness or writer election |
| Read replicas B/C | Selected cell copies | Their verified snapshot availability | Write admission or complete predecessor recovery |
| Object store | Fleet | Existing conditional ownership and durable artifacts | Low-latency query execution |

### 3.2 Policy-based placement and discovery

Automatic selection and repair are required for the first release. Operators set
policy; Cellar selects nodes and reconciles health/readiness. This supersedes the
earlier draft's manual per-cell B/C assignments. Explicit node choices remain
fixture/debug overrides, not a prerequisite for normal recovery.

Proposed policy fields (illustrative, not an existing configuration schema):

```text
ReadReplicaPolicy = {
  existing_cell_selector, replica_count, eligible_node_pool,
  distinct_from_primary: true, one_copy_per_node: true,
  failure_domain_constraints, repair_delay, source_observation_ttl,
  placement_cooldown, minimum_residence, max_surge: 1,
  per_node_build_limits, per_source_transfer_limits, retry_backoff,
  automatic_primary_recovery: true
}
```

`replica_count` counts read-serving nodes in addition to the writer. Setting it
to zero drains read copies and stops their repair; it does not erase cell state
or alter write durability. Failure-domain rules explicitly distinguish required
separation from preferred spreading. Unsatisfied required constraints produce
UnderReplicated status rather than silent placement in an excluded domain.

Reuse existing node lease/process identity and capacity observations where valid.
`CapacityPeer`/`rebalance` provide current membership, draining, pressure and
weight patterns; they are not an existing replica scheduler. Add explicit read
capacity, compatible query digest, pool and operator-supplied failure-domain
metadata. Eligibility excludes draining, incompatible, stale-observation,
pressure-limited and admission-refusing nodes. Read capacity reservation is
checked at the destination; advisory fleet load alone cannot reserve disk.

Rank eligible candidates deterministically by a stable weighted rendezvous key
over cell identity, policy revision and node identity. Enforce hard domain/node
constraints before ranking. Retain healthy serving assignments by default;
small utilization changes do not rerank them on every heartbeat. Add missing
copies and replace unavailable/ineligible ones with bounded hysteresis. Optional
rebalancing occurs only after cooldown and a declared benefit threshold. Tests
must use the actual selected nodes, not assume that desired labels imply roles.

An advisory `read-replicas/<scope>/placement.json` record carries marker 1,
policy revision, plan revision, source identity, preparing/serving/draining node
assignments and repair operation generations. It describes serving work, never
write ownership or durability membership. The authoritative primary's core
coordinates normal per-cell changes; adapters return versioned CAS completions.
Reject epoch/plan regression and never blindly retry a failed CAS with a stale
plan. No cross-object transaction with the owner record is assumed: every
bootstrap, readiness and publication still validates source/history authority.

Replica readiness is a bounded, expiring peer observation, not a bucket write
after each apply. Routers discover current assignments and cache readiness only
within its TTL. Receivers recheck operation, source/history, placement generation
and query eligibility. Stale discovery can cause refusal, not unsafe execution.

### 3.3 Automatic repair and autonomous takeover

Every node runs an execution-domain-owned repair supervisor. Bounded paginated
inventory scans discover existing cell ownership records covered by policy;
an enrollment index accelerates discovery but cannot be the sole inventory if
its publication was lost during a crash. Reconcile after membership, policy,
readiness, drain or source changes and periodic jittered ticks. Persist enough
advisory progress to resume work after supervisor restart. Snapshot/replica
initialization never creates a new business cell. When pending repair lacks an
active source, the supervisor must activate the existing protected cell through
the normal owner lifecycle; this is independent of query callback execution and
does not make queries run constructors.

Handle three inventory states explicitly: active owner (send repair work to its
core); healthy sticky owner with evicted runtime (wake that owner through normal
guarded activation); deliberately released existing state (select a candidate
for normal guarded acquisition/restore). A release is not evidence that the cell
was deleted. Missing enrollment notices must not hide any of these cases from
the inventory scan. Retain a normal counted activity/source pin while admitted
bootstrap or repair work needs the runtime, preventing idle eviction mid-job.
After completion, release pins and respect minimum residence/cooldown; completed
repairs do not trigger repeated activation merely because the runtime idled.

A successful normal durable handoff may advertise a Quiescent source observation
for existing safe images. For released ownership, refresh it only by verifying
the same epoch's released owner record and its validated handoff cut. For Sticky
idle eviction, the healthy owning node's core can refresh read-source observations
without instantiating application code: verify the exact retained owner epoch,
current node/process authority and accepted clean-stop cut, and invalidate this
idle observation on new activation. Runtime absence alone is insufficient.
If no valid idle-cut evidence exists, wake through normal activation and retain
counted source work until a verifiable observation path exists; report capacity
limits instead of repeatedly dropping admission and waking the same cell.
Exercise both cases over several observation TTLs with default connected-only
read policy. This supports stale/covered-token reads without keeping every
application instance resident. New ownership/process/history invalidates the
observation and requires reconciliation. Missing-copy repair still wakes/acquires
an ordinary primary source when needed. Quiescence is descriptive read status,
not another write authority or an assertion of strong freshness.

The current primary coordinates live-cell repair. For absent or failed primaries,
supervisors rank eligible recovery candidates and stagger attempts using the
existing ownership/recovery interface. Ranking reduces duplicate work; it grants
no authority. A candidate uses the existing predecessor recovery interlock,
lease checks, ownership CAS and restore before admitting writes. There is no
new fleet leader or competing writer lease. Delayed supervisors may duplicate
attempts; existing guards and generations must reject conflicting completions.
A returned CAS or coordinator preference is never enough without full readiness.

Read-serving reachability and write authority are distinct. Suspect B can be
excluded from query routing and scheduled for replacement after `repair_delay`
even if B's node lease has not expired: extra read copies do not grant writes.
A timeout alone never allows replacing an authoritative writer. Short read-path
outages trigger backoff/observation first; node rejoin must not churn healthy
replacement copies back to a previously preferred node.

For read-node B failure with A alive:

1. Exclude unavailable B from new eligible routes; keep C serving compatible reads.
2. Select another admitted target F under policy. Reserve bounded capacity using
   a target-issued expiring handle bound to policy/plan/source/process generation.
3. Bootstrap F from an exact proven cut and catch up using the existing protocol.
4. Mark F serving only after authenticated, validated readiness. Preparing copies
   do not satisfy the count of ready replicas.
5. Retire the old assignment and release reservation/session resources. If B
   returns, reconcile its plan generation before it may advertise readiness.

For planned drain or rebalance, add/catch up a replacement before retiring a
healthy serving copy, subject to `max_surge`. Do not claim zero interruption for
an already failed copy. If capacity cannot be reserved, retain healthy copies,
report UnderReplicated with its reason and retry with bounded backoff. No
operator command is needed once an eligible destination becomes available.

For A failure, the supervisor schedules existing safe takeover even with no new
application traffic. After the winner recovers and activates, it reconciles
replica count again. If B is promoted, B no longer counts toward the read-copy
target; choose replacements such as C/F. Missing witness evidence or unavailable
required store history produces RecoveryBlocked, periodic bounded retries and
an alert; self-healing never declares missing acknowledged history expendable.

Repairs reserve target disk before copying, limit builds per destination and
transfers per source, stagger retry storms, and cap concurrent per-cell changes.
Hard per-node/per-source limits bound aggregate fleet work by their recorded
sum; do not claim a stricter globally atomic semaphore under partition. Pending
reservation expiry cannot delete a published serving image; it only releases
unused provisional capacity. Active build cancellation/join retains accounting
until actual work releases its resources. No repair work delays write proof.

Health states include Healthy, Suspect, Preparing, UnderReplicated, Repairing,
PolicyUnsatisfied and RecoveryBlocked with actual ready/desired counts and causes.
Progress events and metrics distinguish detection, queue, recovery, transfer,
apply and readiness times. The achievable repair time includes observation and
inventory-scan delay. Restoring the configured count is conditional on eligible
capacity, required durable history and eventually functioning control paths.
Cellar redistributes onto running eligible nodes; provisioning new machines is
outside the node runtime's first-release responsibility.

### 3.4 Proposed identity types

Keep data-only forms in `cellar-logic`; serialization and hashing live in adapters.

```text
SourceIdentity = (fleet_scope, cell_scope, owner_node, owner_lineage,
                  owner_process_generation, cell_epoch, activation_generation)
LtxCut         = (SourceIdentity, max_txid, post_apply_checksum, history_link_id)
CaptureReceipt = (SourceIdentity, ticket_high_water, LtxCut,
                  immutable_artifact_refs, proof_source)
SafeCut        = (LtxCut, core_publication_generation, proof_context_ref)
ReplicaStatus  = (replica_node, replica_process_generation, placement_revision,
                  replica_lifecycle_generation, published_cut, application_digest,
                  query_contract_version, admission_status)
```

`activation_generation` is a host/runtime identity, not a new persisted writer
epoch. It distinguishes delayed work across same-epoch runtime replacement.
Use existing identities where available; RR-01 must map their actual types.

TXID, node-log sequence, SQLite write fingerprint and durability ticket are
distinct types. Do not add a blanket ordering on versions from different source
epochs. Within one verified history, order by valid chain coverage. Reject
arithmetic overflow, missing identity, wrong cell and incompatible checksums.

`SafeCut` is a trusted-host proof result, not a Byzantine consensus certificate.
Peer authentication plus core-controlled construction is the trust model.
LTX checksums validate history integrity; they do not authenticate a sender.
Response authentication described in section 6 binds transferred proof metadata
to the intended source; ordinary request authentication alone does not do so.

## 4. Safety invariants

| ID | Invariant |
| --- | --- |
| I1 | Only the existing primary authority can admit a mutation |
| I2 | A query result reveals only a coherent snapshot covered by accepted durability and authority proof |
| I3 | No B/C receive, apply, readiness or failure event can count toward D/E proof or change ownership |
| I4 | A minimum-version read returns a snapshot covering the validated requested history or returns an error |
| I5 | Stale op, source activation, replica lifecycle, placement and process generations cannot publish readiness |
| I6 | A published immutable database file is never modified while any reader can reference it |
| I7 | Promotion preserves existing predecessor recovery and refuses missing evidence |
| I8 | Replica queues, history retention, files, memory and query slots have explicit limits |
| I9 | Cancelled/failed output proof never publishes its candidate cut; a later independent successful proof may publish a covering cut |
| I10 | Feature disabled leaves existing request routing, proof semantics and on-disk authority formats unchanged |

An ambiguous write may appear in a later proven snapshot. Proof establishes
recoverability, not whether a particular client received a response. Never
filter history by the set of client acknowledgments.

## 5. Write proof and safe publication

### 5.1 Bind proof to immutable artifacts

Today `await_durable` returns an output position and `ProofSource`. It does not
return a portable database version. Introduce immutable completed capture
receipts for both bucket and fleet paths:

1. Sample the ticket high-water before capture, as today.
2. Capture WAL and record the exact resulting LTX end/checksum and immutable
   artifact identity while holding the owning capture exclusion.
3. Retain this mapping through upload or ordered fleet credit. Failed rounds
   do not credit it. Later pipelined rounds cannot overtake an earlier failure.
4. A waiter receives a completed receipt covering its ticket. Do not sample a
   newer `Db::pos()` after waiting, or combine independent atomic maxima into
   a receipt that never existed.
5. The core accepts the receipt only for the live gate, cell, epoch and source
   activation. Bucket proof still passes `VerifyOwnership` first. Preserve
   current fleet fencing rules and node-authority checks.
6. Successful settlement emits a typed `PublishSafeCut` effect containing the
   exact accepted receipt identity. It may advance the publication head only
   within the same verified history.

A captured segment can include transactions beyond the waiting handler's own
mutation. The whole segment must have valid proof before its terminal cut can
be published. Never include a subsequent capture just because it now exists.
No-op capture and already-proven tickets must return a valid existing cut;
they cannot fabricate a TXID from the output position.

Maintain a bounded/coalesced receipt ledger. Keep mappings required by live
waiters and publication jobs; compact only after their references are released.
Do not add unbounded one-entry-per-request permanent retention.

### 5.2 Publication is asynchronous and best effort

Write success does not await B/C, snapshot construction, transfer, status
publication or read-placement updates. The actor enqueues a constant-size
safe-head observation and releases existing write outputs normally. If read
publication resources are exhausted, preserve write proof and report replica
lag/unavailability. Retain no raw LTX payload in the pure core.

The publication head is monotonic within its history and can coalesce skipped
notifications. Artifact pin acquisition is bounded and may fail; no write
failure follows from that failure. A subsequent snapshot request retries from
a new fully proven head. A cut notice whose artifacts are gone is not itself
a usable bootstrap plan.

When a newly enabled replica has no recent notice, the source can request a
typed snapshot barrier for the current owner activation through the same core
proof path. It proves the selected committed state without running a normal
query callback or allowing a private adapter to invent authority. If there is
no active owner/source, discovery returns `SnapshotUnavailable`; queries do not
activate cells to create initial state.

## 6. Bootstrap, catch-up and transfer protocol

### 6.1 Source selection

Steady state: A serves cell-specific proven LTX to B/C. D/E need not expose new
per-cell read subscriptions. Source bytes can come from pinned local LTX,
node bundles or existing bucket objects, but only through a manifest bounded
by an accepted `SafeCut`. Transport source is not a new authority.

Missing/expired retained tail: A constructs a newer exact-cut bootstrap snapshot.
After A fails: use existing predecessor recovery before a successor publishes a
new source. B/C do not independently inspect D/E and declare a new commit head.

### 6.2 Session and bootstrap

1. B requests a session for cell 1, expected primary/source identity, placement
   revision, application digest and protocol capability.
2. A validates scope, configured destination, fleet peer identity, current
   source activation and available budgets. Core authorization is versioned.
3. A selects a safe cut and pins a complete exact restore plan through that
   cut, including any predecessor epochs required by `EpochChain`.
4. Build a materialized snapshot from that plan. Do not call `snapshot_active`
   on the live mutable SQLite database: it can include later unproven commits.
   Do not call unbounded `restore_snapshot` and assume its newest result is the
   selected cut. Target-bounded restore must verify its actual resulting cut.
5. Return a session manifest with source identity, safe cut, database size/page
   size, chunk sizes, checksums/digests, exact artifact ranges and expiry.
6. B streams into an unpublished temporary image, validates it, fsyncs it and
   its publication metadata, then reports readiness through the core.
7. Tail resumes strictly after the bootstrap cut; concurrent writes are served
   from retained proven segments or cause `SnapshotRequired` on a gap.

An LTX file spanning beyond a target cannot be truncated by metadata. Select a
complete earlier plan or advance to a newer fully proven cut. If neither is
available, return `SnapshotRequired`/`SnapshotUnavailable`, never newer raw data.

### 6.3 Proposed private routes and framing

Use authenticated private node routes, with explicit capability
`read_replica_v1`; marker stays 1 per repository convention.

| Route | Purpose |
| --- | --- |
| `POST /peer/read-replica/open` | Bounded bootstrap/tail-session request and manifest |
| `GET /peer/read-replica/artifact/<session>/<artifact>` | Authorized bounded chunk/range transfer |
| `POST /peer/read-replica/tail` | Bounded long poll after an exact cut; returns complete proven segments |
| `POST /peer/read-replica/status` | Bounded readiness/capacity observation |
| `POST /peer/read-replica/query` | Query name, JSON arguments, options and version token |

Paths never contain a raw filesystem path. Session/artifact identifiers are
opaque server-assigned handles. Sign method, normalized path, target and body
using existing peer authentication rules; scope and source identity are also
inside the validated envelope. Existing tunnel control headers cannot be
supplied by application input to impersonate replica requests.

Authenticate responses as well as requests. Baseline transport can use plain
HTTP; do not assume a protocol-version response header authenticates the source.
Each manifest, tail, successful query, successful status and error response
envelope carries a domain-separated
HMAC-SHA-256 over the exact serialized body digest plus length-delimited request
method/path, operation/session ID, unpredictable request nonce, source and
destination process identities and HTTP status. Use the existing fleet key with
an explicit key ID and a dedicated `cellar-read-replica-response-v1` domain.
The requester verifies it before trusting any SafeCut or allocating artifact
storage. Bind artifact digests, ranges and chunk layout inside that authenticated
manifest; streamed chunks verify against it. Authenticate conditional/empty tail
responses too. Query authentication covers the complete returned value,
`served_at`, serving/source identity and fallback metadata; status authentication
covers readiness, admitted digest and cut observations. Replay with another
session, nonce, destination or process
generation must fail. A compromised trusted fleet node is outside this protocol's
fault model; these envelopes do not claim Byzantine write proof.

Each envelope contains marker, operation/session ID, source and destination
process identities, placement/lifecycle revision, byte count and bounded
payload metadata. Validate limits before allocation. Artifact SHA-256 detects
transfer corruption; LTX format and database checksums establish actual apply
continuity. Never log authentication secrets or full application data.

Duplicate artifact/frame with identical identity and digest is idempotent;
same identity with different digest is corruption. A reordered frame is held
only within a bounded window or refused for retry. Missing history triggers
reseed, not a TXID jump. Sessions are finite and cancellable; close releases all
pins. Expiry never ends retention required by the original durability protocol.

## 7. Materialization and snapshot lifecycle

### 7.1 First implementation

Implement verified physical LTX apply in `cellar-ltx`, using injected `LtxHost`
filesystem/executor services. It performs mechanics only: page-size validation,
TXID continuity, pre/post-apply checksums, growth/truncation handling and bounded
page copying. Fleet ownership remains outside this crate.

Use one private mutable builder per cell and immutable materialized serving
generations. The builder has no application SQLite connection. Apply only proven
segments. On a publication interval, quiesce the builder at a complete cut, copy
it to a fresh generation file, validate the image, and publish. Readers never
open the builder. Reflink is an optional optimization with a correct copy
fallback; it is not a platform requirement.

Do not copy a published file and patch its shared storage in place. Do not
truncate or modify a file referenced by SQLite `immutable=1` readers. The
[SQLite immutable URI contract](https://www.sqlite.org/uri.html) permits SQLite
to skip change detection; this design therefore uses distinct file identities.

First release supports materialized databases up to configured size limits;
paged/sparse VFS files are not treated as complete images. Existing owner
paging remains unchanged. Large-cell replicas require a separate measured
page-map/versioning design before lifting this restriction.

### 7.2 Crash-consistent publication

For generation G:

1. Write `generations/G/db.sqlite.tmp` from a complete proven builder cut.
2. Validate full LTX-derived checksum, size, SQLite structural checks and
   expected schema compatibility. No migrations or application constructors.
3. Fsync the completed file; rename to a unique final filename and fsync its
   parent. Never reuse a generation filename.
4. Write a manifest containing the exact cut, image digest and application/query
   contract; fsync it and its directory before atomically switching `CURRENT`.
5. Fsync the `CURRENT` directory. Report completion to the actor with lifecycle
   generation, then allow readiness publication and pin acquisition.
6. Keep the old generation until its readers finish. Delete only unreferenced
   generations; a pinned generation counts against the disk budget.

On restart, reject incomplete manifest/image pairs, verify the chosen checkpoint,
discard the builder and `.tmp` files, and reconstruct a new builder from the
last valid published generation. A recovered local image is not instantly
advertised: reconcile source/placement/deployment identity first. This local
checkpoint is a serving cache, not a substitute for node-log recovery evidence.

### 7.3 State machine

```text
Absent -> Discovering -> Bootstrapping -> CatchingUp -> Serving
                         |                 |           |
                         +---- failure ----+--------> Reseeding
Serving -> Lagging -> CatchingUp/Serving
Serving/Lagging -> SourceChanged -> ReconcileHistory -> Serving or Reseeding
Any active state -> Draining -> Closed
Any active state -> Retired (revoked placement, incompatible deployment, corruption)
```

Serving means at least one safe queryable generation exists. Lagging may still
serve a stale read or a satisfiable version-qualified read. A source change
does not retroactively invalidate a previously proven historical snapshot, but
it stops tail publication until ancestry is verified. First-release default
rejects disconnected reads once its configured source-observation deadline
expires; an explicit `allow_disconnected_snapshot` option permits historical
safe snapshots while clearly reporting disconnected status. Neither policy
claims bounded staleness or strong freshness.

## 8. Read-only query contract and authorization

### 8.0 Explicit command/query contract

Expose CQRS as the application-facing dispatch model. Every newly declared
operation belongs to a cell class contract with an immutable registration:

```text
OperationDefinition = { operation_id, class, name, kind: Command | Query,
                        request_codec, response_codec, contract_version,
                        query_policy_if_applicable }
CellContract = { class, primary_factory, operation_definitions, contract_digest }
```

The builder accepts an explicit primary factory or a reference to the existing
validated factory registration. Declared commands require one; building the
application fails if it is missing. Supplying factory dependencies through normal
Rust closures preserves application initialization. Query dispatch never invokes
that factory, even when its definition is present on a read-serving node.

`operation_id` is stable within a versioned compiled contract. Names/IDs are
unique per class; duplicate IDs or conflicting command/query registration fail
application configuration validation. The contract digest includes operation
kinds, codecs/versions, query policy and handler/application identity. Decode
limits and authorization apply before business execution.

Proposed Rust-like builder, illustrative rather than currently compiling code:

```rust
let accounts = CellContract::new::<Account>("Account")
    .command::<Deposit>("deposit", Account::deposit)
    .query::<Balance>("balance", balance_query, ReadPolicy::stale_allowed());
registry.contract(accounts);

// Ingress adapters: authenticate, resolve the cell, decode, dispatch.
let account = accounts_namespace.by_name_for(&principal, account_id)?;
let written = account.command(&context, Deposit { amount }).await?;
let read = account.query(&context, Balance {}, ReadOptions::at_least(written.version)).await?;
```

The typed request associates a response type with the registered operation. Use
owned JSON codecs in v1, matching the existing wire limits; initial implementation
needs no derive/procedural-macro generator. A typed builder can erase callbacks
and codecs into the core registration table. This gives compile-time assistance
to in-process callers and runtime validation for remote envelopes.

There are two distinct kinds of handler:

| Handler | Responsibility | Routing knowledge |
| --- | --- | --- |
| HTTP/RPC ingress adapter | Authenticate/authorize, derive tenant-scoped cell ID, decode request, dispatch typed operation, encode reply | Stable namespace/cell identity only |
| Primary command callback | Validate business input and perform the local mutation under ordinary primary authority | No peer addresses, owner lookup or forwarding |
| Read query callback | Compute a result from one pinned read-only snapshot | No placement lookup or forwarding |

Forwarding happens before the destination callback executes. A command callback
can use its primary `Account` instance and a command context backed by ordinary
owner storage/transactions. A query callback is stateless with respect to that
instance: it receives `ReadContext` and the decoded request, not `Arc<Account>`.
The caller and business handlers do not choose A, B or C.

The runtime command bus resolves current write authority, dispatches locally or
forwards, runs the registered command callback and completes the existing output
gate. The new typed command interface returns `CommandReply<T> { value, version }`
using the exact accepted cut rules in section 9. Initial state/idempotent/no-change
commands follow that section's version rules; they cannot invent a version.

The runtime query bus selects compatible, admitted snapshots using the registered
policy and request's optional stricter minimum, pins a generation, dispatches
the registered read callback and returns `QueryReply<T>` with actual served-at
and source metadata. Policies are declared once per query; callers can supply
a minimum-version token or a shorter deadline. They cannot weaken a mandatory
contract minimum, extend deadlines or enable forbidden disconnected/fallback
behavior. No strong-read policy is offered by this release.

The request envelope includes operation ID, expected kind and contract digest.
Both source and destination resolve the operation against compiled registration;
the expected kind must match. A request that labels `deposit` as Query is refused,
not executed as either a query or a fallback command. HTTP methods and
caller-provided headers cannot reclassify an operation. Legacy `fetch`/`rpc`
remain explicitly owner-routed and are never inferred to be query operations.
The registered operation ID identifies a handler definition. Each call also has
a distinct invocation/request ID and, when required by the business contract,
a caller-supplied command ID/idempotency key. Never deduplicate all invocations
of one command using its registered operation ID.

Provide a framework-owned generic ingress dispatcher so applications do not
repeat ownership logic. An optional HTTP route binding records method/path,
operation reference, decoder, response encoder and an application-supplied
authorized cell resolver. It delegates to the same command/query bus. For
example, POST `/accounts/{id}/deposit` binds to Deposit and GET
`/accounts/{id}/balance` binds to Balance because of registration, not the verb.
Transport-specific bindings are adapters and must not introduce another routing
policy. The initial native implementation can expose the shared dispatcher
without requiring a new HTTP routing framework or changing Axum integration.

Command request identity is separate from retry safety. Pass the stable business
command ID/idempotency key through forwarding when supplied. The command handler must
commit any business receipt with its mutation using existing transaction rules.
The bus preserves current safe-redispatch rules and does not automatically replay
ambiguous commands or claim exactly-once execution. Read-modify-write invariants
remain one command at the primary; a replica query plus a command is not atomic.

This is CQRS over cell state: write callbacks use owner SQLite, and read callbacks
use replicated snapshots of that state. It requires neither an event-sourced
business model nor independently designed read schemas. Later projections can
be added as a separate application/read-model feature.

### 8.0.1 Multiple operations and application-owned business logic

An application can register many cell classes, each with zero or more command
definitions and zero or more query definitions. Individual cells are instances
of those contracts. There is no singleton application command/query callback
and no requirement for every class to expose both kinds. A command-only class
still uses its existing primary state; a query-only contract must identify an
existing compatible replicated state source and cannot create primary state by
executing a query. General application/static endpoints can remain ordinary
application handlers alongside these cell operations.

Illustrative expanded registration:

```rust
let accounts = CellContract::new::<Account>("Account")
    .command::<OpenAccount>("open", Account::open)
    .command::<Deposit>("deposit", Account::deposit)
    .command::<Withdraw>("withdraw", Account::withdraw)
    .command::<FreezeAccount>("freeze", Account::freeze)
    .query::<GetBalance>("balance", account_balance, balance_policy)
    .query::<ListTransactions>("transactions", account_transactions, history_policy);

let inventory = CellContract::new::<Inventory>("Inventory")
    .command::<ReserveStock>("reserve", Inventory::reserve)
    .command::<ReleaseStock>("release", Inventory::release)
    .query::<GetAvailability>("availability", inventory_availability, stock_policy);

registry.contract(accounts).contract(inventory);
```

Each operation owns its request/response codec, domain validation, handler,
business error mapping and compatible contract version. Query read policy,
fallback permission, timeout and payload/result ceilings can differ per
operation within application/node limits. No operation can increase those outer
limits or change a query into a mutating capability. Business names, schemas,
rules and workflows belong to the application; Cellar supplies dispatch,
lifetimes, storage authority, proof and admission.

The effective operation key is `(existing application/deployment identity,
class, operation_id)`. The same operation name may be used in different classes
or applications. Duplicate definitions for the same key are an error, never
last-registration-wins. Preserve existing persisted namespace/cell addressing;
this scopes operation dispatch metadata, not a rewrite of cell identity.

Registration is additive: each `.command`/`.query` appends a definition.
Feature modules can contribute registration helpers over the same builder and
inject application-owned immutable configuration or services through ordinary
Rust closures/traits. Helpers are validated together when the application
generation is built. Do not require one giant switch statement or an exhaustive
global enum of every application's operation. Configuration validation reports
the conflicting application/class/operation before serving traffic.

Typed request shorthand is permitted when it identifies exactly one operation
in the selected contract. If an application reuses one payload type for multiple
operations, expose a typed operation handle or an explicit registered name:

```text
cell.command(context, deposit_operation_handle, amount_payload)
cell.query(context, transaction_query_handle, filter_payload, read_options)
```

These handles encode kind, contract and codecs; remote envelopes still undergo
the same registration validation. Never choose an operation by the Rust payload
type alone when more than one definition matches.

Applications can compose business behavior at several levels:

| Scope | Supported composition | Consistency and execution rule |
| --- | --- | --- |
| One command, one cell | Call shared domain functions; read and change several local records; publish local outbox intent | Application chooses a local transaction for its invariant; one command's writes share primary proof |
| One query, one cell | Combine several tables/KV lookups and pure business calculations using shared read helpers | Every read stays on the query's one pinned generation |
| Application service/ingress/workflow | Dispatch multiple commands/queries to several cells, sequentially or concurrently when independent | Each target routes separately; application controls partial success, idempotency, compensation and deadlines |
| Existing owner command | Use existing nested service/cell calls where its ordinary context permits | Preserve current admission/reentrancy rules; cross-cell calls do not join its local SQLite transaction |

Replica query callbacks remain bounded single-cell snapshot computations. An
application that needs a customer overview from Account, Orders and Inventory
can implement an application-level query orchestrator that calls their typed
queries, combines results and returns per-cell versions. This orchestration
uses existing application execution/context or workflow facilities; it does
not widen ReadContext to allow owner mutations or imply one cross-cell snapshot.
Its public business operation may be called a query, while its individual
cell reads retain their explicit snapshot/freshness contracts. Authentication
and side-effect freedom of application-level orchestration remain application
responsibilities; the automatic replica guarantee applies to registered cell
query callbacks, not arbitrary application code.

One incoming request may coordinate multiple operations, but "multiple commands"
does not implicitly mean a framework atomic batch. If several changes must be
atomic in one cell, expose one business command and use a local transaction.
Across cells, use the application's established workflow/outbox/receipt protocol
and explicitly handle partial outcomes. Multiple queries of the same cell in
separate dispatches may pin different generations; combine them into one query
callback when they require one snapshot. Do not invent batch atomicity or repeat
submitted commands automatically to hide a partial failure.

For multi-cell orchestration, return individual results and a bounded map of
cell identities to their version tokens when needed. A primary command's local
token covers its own cell history; it never certifies downstream cells. Carry
each token only to the corresponding target cell. The single-cell token/bridge
protocol in section 9 is unchanged.

### 8.1 Registration and proposed SDK surface

Implement separate per-class named query registration behind the contract
builder and shared dispatcher above. Do not call `Application::create_cell`
or reuse a normal `Cell::invoke` instance on a replica. Default `Application`
query methods report unsupported so existing applications remain source-compatible
where feasible. New registration is compiled into the application executable.

Illustrative proposed surface (not code that currently compiles):

```rust
registry.query("Account", "balance", |read, args| { /* read-only callback */ });
let result = account.query(&context, "balance", args, options).await?;
// result: value + served_at + serving_node + freshness/source observations
```

`ReadContext` exposes cell identity, explicit immutable query configuration,
the pinned `ReadStorage`, cancellation/deadline, and tracing. It does not expose
ordinary `Context`, `Storage`, namespaces, SQL/KV/Blob bindings, fetch, sockets,
queues, workflows, alarms, outbox/inbox, facets, `wait_until` or command calls.
Inputs/results are bounded owned JSON in v1; no capability-bearing structured
stubs, streams or cursors may escape a query event.

The public interface shape is proposed as:

```text
Registry::query(class, name, callback: Fn(ReadContext, JsonValue)
                                      -> HandlerFuture<JsonValue>) -> Registry
Stub::query(&Context, name, JsonValue, ReadOptions) -> HandlerFuture<QueryResult>
Stub::rpc_with_version(&Context, method, RpcData) -> HandlerFuture<Versioned<RpcData>>
QueryResult = { value: JsonValue, served_at: VersionToken, serving_node,
                source_identity, source_observation, used_fallback }
ReadOptions = { consistency, fallback, allow_disconnected_snapshot,
                optional_timeout_budget }
```

Callback registration is per `(class, query name)` and returns the builder like
existing registry registration. All query statements observe its one event-pinned
generation. ReadOptions can shorten the enclosing operation deadline but cannot
extend it; forward remaining duration and retain the origin's deadline across
all attempts. Transport does not compare monotonic timestamps from different
machines. Query results are not released until result-byte limits and response
authentication metadata are complete.

Use read-only SQLite open flags plus a read-only authorizer. Allow bounded
SELECTs over application tables and narrowly required schema metadata. Deny
DDL/DML, ATTACH/DETACH, writable PRAGMAs, file functions, extension loading,
application-registered side-effect functions and direct framework-table access.
Permit host-owned KV decoding behind its bounded read interface. Apply the
[SQLite authorizer contract](https://www.sqlite.org/c3ref/set_authorizer.html)
during statement preparation; a leading `SELECT` string check is insufficient.

Open existing schema without `finish_open`/`schema`, wake initialization or
migrations. All statements and KV reads in one query use one pinned generation.
Result row/byte limits, SQLite interrupt/progress checks and per-query admission
are mandatory. Native Rust is trusted application code, not a sandbox: these
capabilities prevent accidental framework effects, not arbitrary direct OS I/O
or a malicious native infinite loop.

### 8.2 Authorization and deployment compatibility

Replica routes accept authenticated fleet requests only; public HTTP ingress
still runs the application's normal authentication/authorization middleware.
Do not create an unauthenticated generic SQL endpoint. The destination checks
namespace/class/query registration, destination placement, application digest,
query contract, tenant/cell scope and current deployment admission before it
opens a snapshot. Caller-provided headers cannot grant internal query authority.

Replicas must run the exact selected application/query digest in v1. Snapshot
schema compatibility is validated without mutation. Deployment changes retire
old query admission and drain old callbacks, then reconcile or reseed. An old
generation cannot publish readiness under a new digest. Authorization state
stored inside a stale cell snapshot is itself stale; contracts requiring latest
revocation checks must continue using the primary path in this release.

### 8.3 Modes and fallback

```text
ReadConsistency = Stale | AtLeast(opaque_version_token)
FallbackPolicy  = ReturnUnavailable | WaitThenOwner
```

`Stale` means a coherent proven snapshot, with no promise of currentness.
`AtLeast` means the entire query snapshot covers the token's verified history.
Return `served_at` on every success so callers can propagate a session minimum.
Reads on different cells do not share a global snapshot or cross-cell ordering.

Default is `ReturnUnavailable`; fallback must be explicit. `WaitThenOwner`
uses the same registered read callback and a safe owner-local immutable snapshot
through the replica query module. It never invokes normal mutable RPC/GET as a
substitute. If no safe snapshot can be prepared within the remaining deadline,
return an error. Fallback has a separate bounded admission pool to avoid a
read-replica outage flooding A and degrading writes.

Route once to an eligible replica, allow at most one alternative replica attempt,
then one explicit fallback within the original deadline. Pure query retries are
safe only within this restricted contract. No changes to mutation retry rules.

Strong and bounded-staleness options return `UnsupportedConsistency`. A future
strong-read design must establish a fresh authoritative read point and pin a
safe covering snapshot; a cached lease or last-applied timestamp is insufficient.

## 9. Version tokens and epoch continuity

Typed CQRS commands return version-bearing replies. Also allow opt-in
version-bearing replies for the existing RPC path; preserve existing `rpc`
return shapes. Proposed `rpc_with_version` returns the
normal result plus the safe cut from its successful output gate. It does not
claim the cut is the exact transaction of that one command: batching can cover
additional proven transactions. A failed/lost reply remains ambiguous and may
not yield a token; use existing application command receipts to reconcile it.
For a successful versioned call with no mutation, return an already accepted
safe cut covering the result, or run the same core snapshot barrier. If there
is no provable LTX base, return a typed version-unavailable outcome without
inventing transaction zero. The versioned-call contract must make any failure
after submitted mutation ambiguous exactly as the existing reply path does.
This applies to idempotent receipt lookups and read-only outputs trailing another
gate as well: retain the exact accepted covering receipt in the core response
metadata, or prove a new covering barrier before attaching a version. Do not
sample the newest adapter head after the response releases. Extra waiting is
part of the opt-in versioned-call contract, not the legacy RPC path.

Tokens encode a bounded canonical payload: marker, fleet/cell scope, exact
source cut/TXID/checksum/`history_link_id`, issuer identity, issuance/expiry and
key ID. MAC with a
domain-separated existing fleet credential; never include credential values.
Tokens are read constraints, not authentication, ownership or mutation proof.
Reject tampering, wrong cell/fleet, unknown key, expiry, oversize and impossible
values before allocating waiters. Credential rotation may make an old token
unverifiable; return `VersionUnavailable`, never silently serve stale data.

For the same verified epoch/history, compare TXID and check cut ancestry.
Across epochs, use an `EpochBridge`: predecessor terminal cut, recovered restore
base, successor source identity and checksums, produced only after existing
recovery/restore succeeds. Continuation epochs can preserve transaction numbering
while snapshot epochs can establish a different base; raw numeric comparison is
not the rule.

The `covers(token_cut, snapshot_cut)` predicate must be executable, not an
informal comparison. Maintain authenticated immutable history links:

- An append link names its predecessor link, complete LTX transaction range,
  artifact digest, source identity and terminal checksum. Its content-derived
  link ID includes all these fields. Accept it only after continuity and full
  proof checks. Preserve cut vertices that can be issued as version tokens.
- An epoch bridge links the exact recovered predecessor cut to the verified
  successor base. It records the existing recovery/restore result identity and
  checksum correspondence; a higher epoch alone adds no edge.
- A same-epoch `ResumeBridge` is required when activation/process identity
  changes. Derive it from a verified existing clean-reload/resume/restore result
  and exact state correspondence, never from an unchanged epoch number.
- Starting at the snapshot cut, traverse validated predecessor/bridge links to
  the token's exact cut identity, including checksum and link ID. Accept only
  when that identity is reached without a gap, conflict or unsupported mapping.
  Bound traversal work and metadata bytes. No arbitrary cross-source `Ord`.
- LTX data compaction does not automatically delete ancestry metadata. Metadata
  can be pruned under its own bounded retention policy; if needed, reconstruct
  exact links from verified artifacts/recovery results. If reconstruction is
  impossible or the traversal budget is exceeded, return `HistoryUnavailable`
  or `VersionUnavailable`; do not manufacture a shortcut from numeric endpoints.

For a snapshot bootstrap, include the validated ancestry metadata needed for its
advertised supported token window. Tokens outside that retained window can fail
with a typed error even when their numerical TXID is below the snapshot's.
Multihop epoch/resume bridges are checked by the same predicate. History links
are descriptive proof metadata under trusted host control, not a writer-election
log or an alternative durability authority.

Persist immutable read-history bridge metadata under a separate read-replica
namespace after validating its source recovery result. This metadata describes
existing recovered authority; it neither elects a writer nor seals a node log.
Publication may lag write success. Lost bridges can be regenerated only from
verified recovered history. Conflicting bridge content is corruption.

Retention is bounded and configurable. Within supported token/history retention,
attempt reconstruction when required. If evidence is unavailable, return
`VersionUnavailable`/`HistoryUnavailable`; no correctness promise requires
unbounded availability. Token max age and bridge retention are explicit contract
fields. A later epoch alone never satisfies an earlier token.

## 10. Failover and disjoint-topology behavior

Example: A primary at epoch 10; D/E are its log followers; B/C are read replicas.
Bucket through version 95; B through 98; C through 99; D/E cover acknowledged
version 100. A crashes.

1. B/C stop source advancement. Depending on explicit read policy they serve
   previously safe snapshots or refuse once source-observation admission expires.
   An `AtLeast(100)` request cannot succeed from 98/99.
2. Existing lease/ownership rules establish that takeover is allowed. B/C do not
   vote or infer primary loss from a local timeout.
3. Existing node-log recovery accounts for A's required history, seals the
   appropriate witness evidence and publishes required recovery artifacts.
4. A candidate wins the existing owner CAS at epoch 11. Candidate B is a
   preference, not an exclusive authority; any valid candidate can win.
5. The new primary completes ordinary restore/activation. B may reuse a verified
   snapshot only through an explicit trusted-cache adapter that checks its exact
   cut against recovery output and applies all missing history. Otherwise use
   normal restore; warm reuse is an optimization that can be disabled.
6. Publish an epoch bridge, a safe source head and reconciled placement/readiness.
   C follows the new source only after verifying ancestry or reseeding.
7. New writes use the new primary's own installed durability path, which may be
   F/G or bucket proof. It need not retain D/E as its future ensemble.
8. The supervisor automatically restores the read-copy target without counting
   the new primary as a read replica. No incoming request is required to trigger
   steps 2–8 for an existing policy-protected cell.

An immutable serving image is never reopened writable for promotion. Construct a
new owner-managed database through the existing restore path, with optional
verified cache reuse. Keep pinned old read files independent.

| Failure | Required result |
| --- | --- |
| B or both B/C fail | Keep write proof unchanged; automatically choose/reseed admitted replacements; bounded read fallback/refusal while repairing |
| D/E fail while A survives | Existing bucket fallback may prove writes; read progress never substitutes for missing proof |
| A fails, valid witness/bucket history survives | Supervisor triggers recovery, new epoch and restore without traffic, then rebuilds read copies |
| A fails, B/C are behind | Catch up selected successor; never promote by largest reported TXID |
| A and complete required witnesses lost before bucket coverage | Existing recovery refuses; readable B/C are not new witness evidence |
| B/C isolated from A,D,E | Only explicitly permitted historical reads; no autonomous promotion |
| A partitioned but still authoritative | No competing writer; eventual reads may lag, version reads wait/refuse |
| Old A returns | Existing fencing rejects its writes/publication; delayed responses cannot establish new readiness |
| Bucket unavailable and A later fails | Preserve current authority/recovery limits; read replicas do not create store-independent election |

Durability claim is unchanged: acknowledged writes remain recoverable under the
existing posture's stated evidence/failure assumptions. This design adds no
claim that surviving B/C increases the number of tolerable witness losses.

## 11. Retention, resource isolation and shutdown

Source pins for serving reads have their own byte/time budgets. Never hold the
original D/E log tail forever for a slow B. If reader history falls out of the
allowed window, close its session and request a newer snapshot. Existing
durability retention and recovery pointers always take precedence over replica
cache eviction; a reader cleanup cannot delete required witness evidence.

Initial explicit experimental policy, for small-cell fixtures only:

| Policy | Initial fixture value | Behavior at limit |
| --- | --- | --- |
| Maximum materialized database | 256 MiB/cell | Refuse replica enrollment; leave primary intact |
| Input/result payload | 1 MiB each | Typed size refusal |
| Transfer frame | 4 MiB | Refuse before allocation; snapshot uses chunks |
| Query lifetime | 5 seconds, bounded by caller deadline | Interrupt SQLite/cancel callback cooperatively |
| Snapshot publication interval | 250 ms, coalesced | Do not imply 250 ms freshness guarantee |
| Maximum retained serving generations | 3/cell plus private builder | Delay/drop publication; enforce total disk cap |
| Source transfer pin lifetime | 30 seconds | Expire and retry/reseed |
| Active snapshot builds | 1/node | Bounded queue or refusal |
| Active queries | 32/node, 8/cell | Bounded admission; default zero queue |
| Queued tail bytes | 32 MiB/replica session | Disconnect/reseed slow consumer |
| Total replica disk | 2 GiB/node | Evict unpinned generations/replicas or refuse enrollment |

These values are starting test configuration, not production recommendations.
Require explicit node-wide memory, transfer rate, retention bytes, token age,
source-observation, fallback admission and shutdown budgets before enabling.
Reserve disk space for builder plus new generation before copying; pinned old
images can prevent publication and cause typed unavailability. Two roles on
one server still share hardware: isolate serving/transfer workers and prioritize
durability fsync rather than claiming complete physical isolation.

Shutdown: stop new sessions and queries; cancel/drain query callbacks and
transfers; release pins; join materializers; close SQLite handles; then release
files and report completion. Add explicit replica-task/pin fields to shutdown
evidence without weakening existing ingress, event, cell, log or local-durability
proofs. Arbitrary native blocking code is cooperative and can make shutdown
incomplete; do not mark a cancelled future as joined work.

## 12. Errors and observations

Use typed internal errors with stable SDK mappings:
`ReplicaUnavailable`, `SnapshotUnavailable`, `SnapshotRequired`,
`FreshnessDeadlineExceeded`, `VersionUnavailable`, `InvalidVersion`,
`HistoryUnavailable`, `UnsupportedConsistency`, `QueryNotRegistered`,
`IncompatibleDeployment`, `ReadOnlyViolation`, `ResourceExhausted`,
`SourceChanged`, `Cancelled`, and `CorruptReplica`.

Mutation errors remain unchanged. A query refusal does not report that a command
was rolled back. A replica transport failure must not poison an ordinary cell
stub as if the writer process were permanently broken.

Query metadata: serving node/process, actual cut, source identity, connected or
historical status, application/query digest and whether fallback was used.
Metrics: safe-head progress, replica publication lag in comparable history units,
last source observation, applied/published cut, reseeds, gaps, checksum failures,
queue/disk/pin budgets, query and fallback p50/p95/p99, write proof latency by
actual source, and incomplete drains. Do not label elapsed local time as proven
data staleness. Avoid cell IDs as unbounded metric labels; expose exact per-cell
status through bounded authenticated inspection.

## 13. Compatibility, rollout and future work

Add read-replica capabilities independently from durability membership and
existing owner peer protocol. Unsupported peers refuse new operations explicitly;
ordinary owner operations retain their current paths. Do not change persisted
owner/node-log records to make B/C owners. Additive read cache/bridge/placement
records use marker 1; there are no legacy readers or silent migrations.

Deploy capable binaries with the feature disabled, then enable explicit fixture
placement, stale reads, version-qualified reads, and finally a bounded production
canary after qualification. Disable by withdrawing query admission, draining
sessions, and routing future requests according to their explicit fallback policy.
Do not reroute already-issued queries into mutable handlers. Cache files can be
removed after drain; original durability evidence must remain untouched.

Future separate designs: strong read barriers; verifiable bounded staleness;
page-versioned large-cell replicas; demand-based replica-count autoscaling; recovery-certified
read checkpoints; and an optional read-visible write acknowledgment that waits
for a named read target. Each changes additional cost or guarantees and needs
its own evidence.

## 14. Acceptance and design risks

Release requires I1–I10, exact receipt mapping under racing captures, safe image
publication across crashes, no replica constructor/effect execution, successful
version-qualified reads across verified epochs, refusal under missing witness
evidence, bounded resources, complete shutdown reports, disjoint five-node fault
results and a six-node automatic-repair extension. Automatic selection, repair,
no-traffic primary recovery, rejoin stability and insufficient-capacity refusal
are release gates. See the qualification plan for executable tests and oracle.

Largest implementation risks are proof-receipt drift, checksum/continuity bugs,
full-image publication I/O, token ancestry after takeover, and accidental reuse
of mutable activation. Resolve these before optimizing throughput. If exact-cut
bootstrap or bounded publication cannot be implemented with the selected LTX
format, stop that work package and revise this design; do not replace it with
live-file copying or numeric watermark guesses.
