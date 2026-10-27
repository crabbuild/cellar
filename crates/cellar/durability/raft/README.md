<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Node Raft effects

This component owns the log engine and the raft-rs adapter. Business activation,
command output, query publication and archive proof belong to the actor and
`cellar-logic`; `GroupStatus::protocol_ready` is only a protocol observation.
Native command/query serving, committed archives and replica recovery are
implemented. An opt-in controller repairs committed membership and balances
eligible leaders. Immutable Blob bytes have an explicit bounded adapter;
committed alarms and receipt-checked outbox delivery share an owned scheduler.
Combined business-feature qualification and later operating/release work remain
under implementation.
The [technical design](../../../../plans/multi-raft/technical-design.md) defines
the complete contract.

`RaftClass::immutable_blobs(RaftBlobLimits)` enables bounded immutable values and
changes the class identity. Commands use `put_blob` and `blob`; queries use `blob`
against their pinned committed image. Actual bytes share the command transaction
and LTX history. Archive capture reserves memory before enumerating their exact
closure and supplies each value as a verified recovery dependency. A reference
alone proves no content or durability. Defaults retain at most 128 values,
64 KiB per value and 1 MiB total; there is no deletion/GC API. This initial adapter
duplicates content in the SQL image and dependency objects. The
[business-feature contract](../../../../plans/multi-raft/effects-design.md)
records this boundary and the alarm/outbox contracts.

`RaftClass::alarm(name, digest, callback)` declares a synchronous callback and
`RaftHostOptions::effects` enables its owned scheduler. Commands set/cancel declared
names through `CommandContext`. The generation, due time, payload and required
durability share the command transaction. Firing reads a committed image, checks
current leader readiness and uses a private replicated command that rechecks and
consumes the selected generation atomically with its callback. Public command
discovery and validation exclude these system operations and their client IDs.
Follower/learner roles cannot run these callbacks. One node timer bounds discovery
and retained metadata; physical SQL work stays joined and can delay that timer.
Existing receipt limits apply, with no automatic pruning in this component.
The [alarm contract](../../../../plans/multi-raft/effects-design.md#committed-alarms)
states the resource, clock, retry and shutdown boundaries.

`RaftClass::outbox(RaftOutbox)` declares immutable delivery lanes. `emit` persists
a signed request in the business transaction; a committed private claim precedes
sending. `RaftInbox` commits receiver deduplication and business changes together,
and releases a signed receipt through the normal durability gate. Only a verified
receipt and matching attempt generation can settle source progress. The concrete
`RaftNativeEffectSink` and [recovery example](../../../../examples/raft-effects/README.md)
exercise native destinations. The [outbox contract](../../../../plans/multi-raft/effects-design.md#committed-outbox-and-receiver)
records authentication, bounds, ambiguous outcomes and remaining retention work.

`RaftCellHandle.status()` reports a sampled replica role and local query
readiness, including coherent image, registered callbacks and exact RecentSource
age. These observations do not reserve capacity or prove a current quorum.
Protocol-only applied entries can advance the sampled position while retaining
the same verified image. Readiness uses the same complete marker and monotonic
position checks as query admission; changed markers, regressing positions and
uncommitted cuts remain ineligible.
`RaftHostOptions.max_query_pins` bounds physical SDK query-reader reservations;
the blocking owner retains its permit through callback/connection exit after
waiter cancellation. `RaftCells.resource_usage()` reads actual fixed-size host
business/output, control, archive-capture and query-reader usage/capacity without
I/O or group enumeration. The internal generation-bound, authenticated
`/peer/raft/metrics` endpoint exports these counters, actual SQL work reservations
and incoming/outgoing transport and snapshot/metadata banks, without identity
labels or callback execution. Available sources use 68 fixed sample identities;
unavailable sources omit their values rather than synthesizing zero. A weak
transport observer cannot retain the transport/engine after joined shutdown.
Image, lag, connection concurrency and controller views remain open. The [operations contract](../../../../plans/multi-raft/operations-design.md)
defines the boundary.

`ReplicaLog` owns one blocking execution-domain task and one Raft Engine.
Its bounded `GroupLog` caches become visible after sync. Storage lookups on the
consensus worker perform no filesystem I/O. The filesystem is always supplied
by the node; unsupported operations fail explicitly.

Every newly created engine persists a nonzero `EngineInstanceId` in both its
node marker and engine root. Resume checks the exact pair; another empty engine
cannot repeat this identity by reusing configured node/disk values. The required
marker-1 field is a hard local-storage cutover, without an older-record reader.
The ID is a persistent identity rather than protection against copying live
vote files to another disk. Before bootstrap, joined recovery or standby
registration, the catalog verifies an immutable cluster-wide S3 pin against
this exact live engine instance. A recreated engine cannot acquire the old node
identity, even before that node has claimed a cell member. Query-only routers
do not acquire a member storage pin.

The directory adapter also publishes bounded, conditional process views using a
synced node revision. It binds configured node/disk/label/server-name trust to
the live engine owner; exact retry reconciles an ambiguous publication, and old
closed owners cannot replace a newer process. These views grant no membership,
seeding or read authority. Opt-in host publication and atomic routing refresh
are implemented with focused verification; broader qualification is pending.
Authenticated peer inspection and the automatic controller use these views.

Set `RaftHostOptions.node_directory` to `Some(RaftNodeDirectoryOptions::default())`
to publish this boot's process and refresh explicitly configured peers from an
owned timer. Include a self endpoint matching the engine's node/disk/process and
host label. Every source uses the same advertised endpoint for a physical node;
the advertised data endpoint may be a shared node-wide proxy. Replica/standby
storage is required; query-only routers retain their separate registration.

Startup publishes once per cluster under one original deadline. The timer
observes only configured exact keys, with at most eight concurrent bounded
records and one active batch. Views refresh all group/query routes, query-origin
permissions and pools under one registry lock, retaining group/configuration
revisions and revocation tombstones. A full replacement task budget refuses the
whole refresh; old workers stay counted until physical exit. Old frames, queries,
snapshot sources and seed epochs are revoked. A seed grant retains its process
epoch through physical preparation and cannot authorize initialization after
revocation. Future route/grant installs also refuse a stale process view.

Equal revisions require the same complete observation, including boot attempt.
Storage-instance, disk, label or server-name rebinding is refused. A newer valid
self publication from another process closes local Raft admission and joins the
host; an older self revision cannot displace the current boot. Missing records,
read failures and S3 outage preserve cached routes and live quorum authority.
The host refreshes its own peer map before later membership route construction.
This timer performs no membership repair or automatic leadership balancing.

Set `RaftHostOptions.controller` to `Some(RaftControllerOptions::new(policy))`
to enable owned reconciliation. It requires snapshots, a node directory, a node
profile with the policy's byte reservation, and reserved control capacity.
Configure `inspection_origins` explicitly on every participating node for the
namespace. Discovery grants no inspection permission. Missing, refused, stale or
overloaded observations cannot establish a failed member or an eligible target.
Only repeated failed TCP dials or authenticated failed-state observations after
the configured grace period establish unavailability for placement.

The controller inventories durable plans before proposing new ones, examines
registered histories in bounded rotating turns, and probes all current bindings
plus a bounded rotating candidate set. One owned job and one unresolved new-plan
intent per node bound automatic work. Decoded observations reserve controller
memory before transport reservations are released. Defaults allow eight samples
per group, four concurrent probes, 8 MiB of retained metadata, a 500 ms probe
deadline and a five-second whole-observation deadline. These are admission bounds,
not measured deployment capacity. Candidate profiles and failure domains are
declared hints; committed configuration remains the authority.

Repair uses the ordinary revision-bound begin/advance APIs and fresh-quorum
learner activation. It preserves an exact plan across ambiguous replies, pause,
election and restart; current matched and published progress gate promotion.
An empty source has no published SQL image, so canonical empty-state catch-up
uses its verified durable position. Joint transitions retain both majorities
until the committed finish. A timed-out unpromoted learner can be aborted through
the same guarded API; its member identity remains consumed.

Optional `leadership` policy uses complete node-wide load samples, compatible
current voters, fresh matched/activity and published progress, cooldown and a
deadband. The node-wide attempt interval applies even when a transfer fails.
Sample ages begin before probes/scans; receipt never refreshes an old observation.
Transfer acceptance does not promise an election outcome.

`RaftCells::set_controller_enabled(false)` pauses new reconciliation and cancels
its observation/action waiters while quorum serving continues. Admitted mutations
and target-owned initialization can still complete. Resume reconstructs committed
plans and restarts observation/cooldown state. `controller_status()` exposes local
attempt counters, which reset on boot and supply no consensus proof. Public Node
shutdown owns cancellation and physical joining of controller and transport work.

`reserve_node_revision` syncs a nonempty root fence and returns the node-wide
monotonic sync sequence. It changes no group vote, configuration or applied
position and cannot settle a group persistence ticket. Storage close separates
admission closure from physical completion: even error results wait for engine
and directory-lock release. Dropping a close waiter leaves the worker owning
drain/fence/join; later waiters observe the same terminal result.

Host initialization errors also close and join the supplied Raft infrastructure,
including preflight failures before any SQL pool or cell exists. Retained
infrastructure handles observe closed admission before the error returns. The
public Node entry point covers failure to enter the host's execution domain
after acquiring that infrastructure. A gated lock-drop regression verifies the
error waits for physical release and permits immediate recovery reopening.

The private consensus storage view can finish mechanical lookups against the
last durable cache during engine closure. Public group/proof reads remain
closed. This avoids turning ordinary cancellation or an I/O failure into a
raft-rs storage panic; no successful write or application proof can bypass the
closed engine's persistence boundary.

`Catalog` selects immutable namespace mode/schema/protocol/storage-prefix records
with conditional object creation. Raft data uses a disjoint `raft/<cluster>/<ns>/`
prefix. Per-cell births fix their group/incarnation, initial placement and empty
state; concurrent creators follow the winning record. A permanent group-ID
reservation prevents reusing that identity for another cell/history. Every
create is reconciled by bounded exact-key reads, including a lost response;
there is no overwrite fallback, key listing or lease-based birth replacement.
Records have strict canonical marker-1 framing and a content digest. Root mode,
schema, cluster, minimum durability and voter policy cannot change in place.
Three initial voters are required unless the immutable root explicitly selects
the single-voter development policy.

Initial attachment requires a permanent member claim binding member, disk
lineage and birth digest. The adapter reads the claim first, syncs/verifies local
genesis through the engine, then conditionally creates and reads back the claim.
A claim with absent local stable state refuses blank initialization, including
when a caller supplies the old disk identity in another directory. Competing
disks cannot both acquire the member. The returned `BootstrapPermit` binds the
selected catalog and exact engine owner; an old permit cannot attach a reopened
engine. Committed configuration remains membership authority after bootstrap.

Committed configuration also selects `GroupMembership`: the immutable birth ID
set, last configuration index, monotonic member-ID high-water and active joined
member/node/disk bindings. `JoiningMember` enters only through an AddLearnerNode
entry with canonical revision-bound intent. New voters, missing/rebound identities
and stale intents are refused before append; promotion requires replicated
progress through the current commit. Joint outgoing bindings remain until leave.
Retirement prunes bindings while preserving the high-water, so compaction and
restart cannot make an old ID available again.

The engine stores this state atomically with ConfState/applied progress. Physical
snapshot references carry it into the same install batch; snapshots and S3
manifests preserve it at their exact cut. Recovery refuses inconsistent same-cut
snapshot/binding markers. S3 validation checks the immutable birth ID set, and
transport checks joined members against their committed node/disk even when
routing supplies a matching numeric ID. Process generations remain separately
fenced discovery values. A binding observation alone is not live-quorum join
authorization; native fresh-member seeding requires the separate fresh safe
ReadIndex grant described below.

This is an internal marker-1 hard cutover. The namespace protocol identity is
`cellar-raft-ltx-1/members-1/seeds-1/s3-1/repairs-1`; a root selected by an earlier binary
is refused before attachment. Use a fresh experimental namespace/storage prefix
and local Raft state. There is no old-format reader or migration path.

`Node::start_raft_service` starts its runtime parked, including recovered groups.
No RawNode exists or votes until verified attachment. `RaftRuntime::start` retains
its direct trusted-embedding/verification behavior; normal node integration uses
`start_parked` and catalog permits. This adapter currently rereads claims on
restart: an unavailable/unconfirmed catalog leaves attachment closed. Retaining
local confirmed catalog receipts and snapshot/join recovery metadata remains
recovery-controller work. Running groups do not consult the catalog for append,
vote or command authority. SDK registration/dispatch and full actor integration
remain in progress.

`RaftRuntime` owns one async execution-domain task for all attached groups.
Each `RawNode` is mutated only on that worker. It collects at most one Ready per
group, batches independent groups' writes, and polls the storage future alongside
peer requests and the owning domain's timer. Deferred requests retain their
byte/item admission permits. Ticks coalesce while a group is paused.

After Ready persistence, the worker transfers its messages into a bounded
outbox before calling `advance_append`. It separately persists a LightReady
commit update before dispatching that update's messages and committed entries.
No-op and configuration entries advance the durable applied marker without
changing the data image; configuration changes also persist ConfState. A new
leader admits proposals only after a committed and applied current-term entry.

An `Apply` event is an immutable committed-entry receipt. Only the ordered apply
head is issued for a group; other groups keep running while its image work waits.
The apply worker must verify, sync and rename an immutable image before returning
`ApplyCompletion`. This adapter validates the marker and persists it, then calls
`advance_apply_to`. Physical SQLite/LTX verification belongs to the image worker;
the runtime tests use an explicitly separate application stub.

`ImageApplier` supplies this physical verifier for committed data entries. It
checks the complete replica/ticket/entry identity against the durable committed
apply head, decodes the scoped application envelope and proves its exact LTX
base/result through `GenerationStore`, either by replay or by transferring the
matching sealed private preview described below. Preparation returns a runtime
completion, not a readable image pin. `publish` releases a `PublishedImage` only after the
engine's synced marker names that exact prepared generation. A delayed event
can be reconciled against the durable cache; later protocol-only entries may
cover the same image. Recovery adopts both the applied image and the independent
image selected by the durable Raft snapshot. It validates every reference before
retiring other inventory, with mandatory digest, length, LTX position and page
geometry validation. Newer applied state does not retire the only snapshot image.

The internal marker-1 image record now includes `page_size`. Recreate older
experimental Raft storage for this hard cutover; no compatibility reader is
provided. Recovery reclaims orphan images only after adopting the complete
authoritative reference set. The image owner alone does not authorize leader execution,
query consistency or a successful SDK command.

`PrivateExecutor` owns an exclusively locked writable directory. Opening or
rebuilding it copies the marker-selected immutable base, verifies that copy,
opens WAL capture in disposable mode and seeds its continuation at the exact
committed LTX position. Opening/control writes join the next proposed cut; a
zero-page continuation file is never replicated as data. No Fleet uploader,
maintenance timer or output ticket can operate on this private image.
Private SQLite connections use `synchronous=OFF`; local capture files, copied
bases and cleanup do not require disk syncs. Every reopen discards those files
before reading private history or admitting a callback. Captured bytes still
pass physical preview verification, durable Raft commitment and committed-image
publication before acknowledgement. The immutable image and log persistence
barriers are unchanged; disposable state never supplies a recovery proof.

A checked `CommandAuthority` observes a current-term, caught-up leader and binds
the replica, term, applied marker and command base. One synchronous callback
receives `CommandTransaction`, SQL and explicit host-captured time/random inputs.
It cannot access the connection, commit early, start a nested transaction, use
reserved tables, attach another database, write temporary state or invoke SQL's
ambient clock/random functions. Native application code remains trusted; this
facade is not an OS sandbox. Callback errors, panics and oversized results roll
back. Any uncertain private state requires rebuilding before another callback.

Archive admission exposes two fixed-size duration histograms. The
`archive_admission_check` timer covers each complete node-wide policy evaluation,
including grants, deferrals and refusals; its success means evaluation completed,
not that a callback was granted. Existing decision counters retain the reason.
`archive_admission_wait` starts only when a command joins the bounded deferred
FIFO and ends when admission grants or refuses it, before authority/SQL work.
A cancelled or retired waiter records abandonment when its owner is released.
The wait includes fairness behind existing waiters; it does not include the
subsequent SQL queue, command execution or archive publication.

Archive publication exposes an `archive_publication_batch` timer from scheduling
until every captured cut returns, including object I/O, replay and waiting for
the shared verifier. A successful batch means all its cuts verified; it does not
mean the host accepted every local progress observation. Fixed counters record
succeeded/failed cuts separately from coverage that advances, is unchanged,
waits for a coherent applied image, is rejected, or names a removed cell.
`archive_coverage_index_advance` sums advanced Raft index distances, including
protocol entries; it is not the admission count of retained data-bearing entries.
These observations do not change publication, retry or admission policy. Live
snapshots may cross a counter update and are not atomic multi-counter proofs.

A known archive-admission refusal still permits duplicate receipt lookup, then
refuses new work before opening a transaction or invalidating private capture
state. It does not require a private-image rebuild. Failures after mutation
begins retain the existing rebuild requirement.

The transaction stores the command ID, operation/contract/request digests,
bounded encoded result, minimum requested durability and retry deadline beside
the business mutation. Per-client sequence and expiry floors use canonical
big-endian u64 blobs. New sequences must be contiguous; conflicting retries and
expired sequences are refused. Client and receipt budgets refuse admission
instead of deleting promised retry state. A replicated pruning update can remove
only an expired contiguous prefix; its client floor remains after receipt removal.

After local commit, capture includes every intervening complete LTX file. Bounds
are checked before reading/decompressing; SQLite's frozen logical image supplies
the mandatory result digest. A retained preview generation verifies and seals
the entire physical apply before proposal submission. The SQL owner can transfer
that exact generation to committed apply after validating the durable ticket,
entry, schema and current base. Reuse additionally requires the original replica,
submitted proposal, complete encoded payload digest, exact base/result states and
the same open image-store owner. A mismatch keeps the preview private and uses
ordinary physical replay, as followers and recovery do. The transfer removes the
pin from private cleanup ownership; the durable applied marker and publication
still precede acknowledgement. Reusing a sealed preview removes one copy, replay
and seal on the leader path without relaxing a persistence barrier.

`mark_submitted` runs before awaiting the runtime. A lost response or cancelled
waiter keeps the single command slot occupied. Abandoning that submission needs
a new term/process with its applied barrier; an unsubmitted attempt can rebuild
under the same authority. Neither path continues from the surviving writable
file. The host must revalidate live runtime authority before submitting bytes.

`finish` accepts only the exact published group/proposal/image, then reads the
receipt from that immutable generation. It returns a covering position and the
receipt's minimum durability, not S3 archive proof or an SDK success gate. A fresh
caught-up authority observation permits reuse only when the frozen private state
still exactly matches that head. Otherwise rebuild. Preview cleanup and capture
history trimming stay bounded; orphan/public-image retention is reconciled by
the archive/recovery controllers. SDK registration and node-owned command
scheduling connect these mechanics to the pure output gate.

The compiled SDK now provides `Registry::raft_class` and restricted `RaftClass`
definitions. Mutable factories and workflows cannot share their class names;
Fleet configuration refuses Raft definitions. Class name, explicit schema
version, schema-definition digest and sorted required command contracts form
the immutable application identity. Missing or changed commands cannot join
under the same root. Query contracts remain independently checked so query-code
availability does not determine voting compatibility.
`stage_registered` checks that identity, operation kind/contract and normalized
input digest before private execution or duplicate receipt lookup. The registered
query bridge opens a verified image through the strict read-only SQLite opener,
checks compatible schema/query code and returns that image's actual version.
It establishes no freshness/linearizability claim by itself.

`cellar_logic::raft_command::CommandState` owns one pending command's admission
and output decisions. Sequenced runtime status cannot resurrect a revoked term;
progress and receipts bind the full replica/history and operation. Fresh commands
need exact ordered entry proofs plus their verified image receipt. Retries can
return a verified stored receipt at its actual covering cut, including a prior
term, without fabricating a new proposal. Namespace/request durability can only
strengthen. Cancellation retains a submitted producer; unsubmitted work stays
pinned until the callback joins and its private mutations are discarded. A new
term and applied startup barrier are required to reconcile an unresolved
submission. Closing records unfinished identity and refuses further output.
`Node::start_raft_cells` installs registered groups in the node's execution
domain. `host.rs` owns serial command producers and effect dispatch;
`host_sql/mod.rs` owns SQLite/image resources in a fixed node-wide blocking pool.
Its private `images` module selects physical command/read adapters beneath the
shared controller. The extent branch is exercised by the worker integration
fixtures; public node configuration retains the materialized backend until the
extent recovery and effect contracts are integrated.
`RaftCells` and `RaftCellHandle` expose bounded public commands, explicit queries,
status and joined local closure. Admission is synchronous, including when a
returned future is not polled. Attempt deadlines are checked by the dispatcher
and SQL owner; cancellation cannot release a slot while its callback still runs.

Checkpoint and source metadata capture freezes an exact durable cut on that SQL
owner, then releases it before physical snapshot encoding. A separate registered
blocking task retains the immutable image pin, source reservation, scratch owners
and any application work until encoding exits. The same source admission bounds
checkpoints and transfers. Queries and commands can continue while a snapshot is
encoded; an encoding completion cannot clear a newer SQL operation's busy flag.
The `snapshot_encode` performance timer records this work separately from SQL.
Source observations also distinguish seed, checkpoint and transfer encoders,
with completed encoded-byte counters and a separate blocking-queue timer.
Recovery observations separate authorization, peer receive/append, archive fetch,
physical verification/replay and durable installation. Source archive-wait timing
includes retries until coverage reaches the fresh ReadIndex; outcomes preserve
failure and abandonment. Duration buckets resolve recovery costs through one
hour. These are node-wide bounded observations, not publication proofs or exact
recovery percentiles. The [cold-recovery campaign](../../../../tools/perf/recovery-README.md)
measures owner-loss through the first local verified read and durable write;
its embedded pilots do not qualify hard OS-process crashes or an RTO.
Physical verification and runtime checkpoint/install authorization still precede
publication, and shutdown joins the encoder before releasing its owners.

Each SQL owner prioritizes queued publication, retirement, submission, discard
and archive capture over unrelated ordinary work. It never passes an earlier
request for the same cell, and after three priority selections it serves the
oldest request. This scheduling adds no collection delay or admission capacity:
queued and executing requests retain the same worker semaphore reservations,
including when the queue moves work into its bounded local batch.

Business commands/queries and administrative operations have independent host
admission. `max_control_requests` defaults to 16 and `max_control_bytes` to 16 MiB;
status, repair, seed activation and leadership requests use that reservation.
Both byte and item charges survive unpolled waiters. Each host turn continues
admitted producers before servicing at most four queued control requests, then
services normal business, consensus and completion work. A command can wait for
an in-flight status check; execution and submission still require fresh checks.
Saturating either admission budget leaves the other available.

An explicit `node_profile` enables `RaftCellHandle::inspect()` for registered
members and dormant standbys. It reports declared pool/region/zone/capacity,
physical engine identity, history occupancy and a coherent member observation.
Durable apply and published-image positions remain separate. Inspection uses
control response reservations, reads bounded cache metadata and counts engine
groups without scanning their contents. It performs no SQL callback or member
initialization. Existing, retired and initializing histories are ineligible as
blank standbys.

Inspection-enabled hosts collect aggregate leadership load independently of
incoming requests. `inspection_scan` configures the interval (250 ms), whole-scan
deadline (5 s) and groups per batch (four, bounded to 1–16). The existing host
owns each batch; it samples the actual runtime for every attached history using
a bounded ordered cursor. A result appears only after a complete scan with the
same attached/durable group population. Initialization or a changed population
hides the old count. A failed or partial scan retains the prior complete result
at its original age; it never substitutes a partial count or refreshes its time.
The age starts before the first batch, and remote collection adds the full RPC
duration. An empty attached set can report zero after its complete empty scan.
Load observations grant no membership, transfer or read authority.

`inspect_peer(node_id, timeout)` uses configured trusted endpoints and the node
execution domain. Remote collection requires a profile, node directory and
explicit `RaftHostOptions.inspection_origins` at the destination. Each grant
matches a registered namespace and trusted physical peer. Query permissions and
discovery create no inspection permission; revocation retains its revision
through process refresh. A dormant source or destination needs no member ID.

The purpose-specific TLS/HMAC RPC binds protocol, full history, schema, birth,
both node/disk/process/engine-instance identities, labels, nonce and original
deadline. Replies authenticate their request and body digest before bounded
strict decoding. One RPC reserves 1 MiB of metadata, with 16 node-wide slots,
two per physical peer and a 64 KiB body limit. Metadata has additional control
connection slots; ordinary consensus keeps its own connection-class bound.
Retained SDK responses remain charged to the host control budget. Cancellation,
process/grant changes and shutdown stop and join owned network tasks. Returned
sample ages include the full request duration. Only `InspectionError::Unreachable`
reports a failed direct TCP probe; missing discovery, local pressure, refused or
unusable replies cannot establish remote node failure. Observations grant no
membership, seed, query or write authority. Automatic reconciliation remains
MR-11 work.
After submission, the producer remains owned through consensus and publication.
A newer committed/published term barrier can retire old uncertainty even on a
follower, after disposing of its private shadow; that grants no write authority.

MR-08's local read controller holds each request's original deadline and byte/item
permit while waiting for an image. `AtLeast` verifies an opaque signed token
before comparing history positions. `Linearizable` uses fresh safe ReadIndex
contexts, waits for actual image publication, and never treats a runtime status
or cached commit position as a barrier. A checked role/term change or lost reply
can resubmit the invocation's barrier under the same deadline. Retries wait four
host polling intervals, clamped to 100–1000 ms, and still require a safe ReadIndex
result. Status and submission generations prevent late replies from regressing
an executing query.

The SQL owner captures the durable applied marker with its exact immutable pin.
Protocol-only entries can cover that pin; an unrelated newer image cannot relabel
it. Queries retain the pin through callback completion, and their result metadata
names that captured cut. The default Stale policy requires a recent observed
source; `DisconnectedHistorical` explicitly permits an older verified image
without a time bound. This policy observation is never used for strong reads.
SQL progress hooks observe an atomic cancellation flag raised by the node's
deadline controller, including for an unpolled waiter. Native Rust callbacks
remain trusted/cooperative and are joined before resource release.
Completed responses retain their byte admission until consumed or dropped;
unpolled response channels cannot reuse capacity merely because SQL finished.

`RaftCellsServices::versions` injects managed signing/verification keys. Ed25519
tokens use bounded canonical marker-1 framing and bind full history, schema and
log position. Namespace startup reconciles public-key retention metadata through
conditional updates and exact reads; configuration cannot silently drop a live
verifier on restart. The catalog supplies no new trust roots or consensus
authority. Key rotation and token lifetime are described in the
[application contract](../../../../plans/multi-raft/application-contract.md).
`QueryRouting::Replicas` tries up to eight distinct configured members in priority
order, with one optional untried leader hint. The caller supplies a bounded
per-attempt timeout; every attempt keeps the same contract, captured input,
consistency/token and stale policy under the invocation's original deadline.
Forwarding does not require query code on the origin voter. The destination
still requires a matching query definition before execution. `served_by` names
the actual responding member, independently of the history token.

`NodeRaftListeners::query` and `RaftPeer::query_address` provide the separate
query endpoint. A peer has one bounded query queue/connection alongside its
control and LTX connections. A shared pool owns the physical RaftPeerNode
endpoint; it retains no group or member identity. Each consensus request still
carries and validates its own full member route. Query MAC domains, exporter
label and marker-1 framing are independent of consensus frames. Requests/replies bind full route,
schema, sequence, request tag and payload digest. The receiver forces local
query execution; it cannot recursively route. The sender deducts queued and
handshake time before sending a remaining timeout, while the origin separately
enforces its deadline. Closing the connection cancels the destination waiter.
Request/response representations remain charged through delivery.

Query headers use a distinct route representation: the caller carries its
node/disk/process identity and label, with no Raft member ID. The target carries
its complete replica/history identity. The response reverses that exact route;
its orientation cannot be reused as a request. Consensus headers retain their
separate member-bearing routes and validators. This is a marker-1 hard cutover
for experimental query peers; incompatible headers are refused without an old
reader or translation path.

Each `RaftCellRegistration` selects `RaftCellPlacement::Replica` or
`QueryRouter { leader_hint }`. A query router reads an existing namespace and
birth record, verifies the expected history/initial placement, and installs
discovery without creating a local member or SQLite image. It cannot reinterpret
existing member storage as a query-only binding. Its SDK handle rejects commands,
local reads and local status; use explicit `QueryRouting::Replicas`. The optional
hint is discovery only and cannot supply a read proof.

`RaftCellsConfig::query_origins` supplies trusted `NamespaceQueryOrigin` grants
for exact cluster/namespace/node/disk/process endpoints. A receiving replica
accepts a current group member or an explicitly granted origin, validates its
own current membership and checks permission around body/execution/reply work.
Granting read access never expands the member-bearing consensus route.
`RaftTransport::grant_query_origin` and `revoke_query_origin` support live
revisioned updates; revocation cancels waiting query work and preserves a
tombstone, so an old update or regrant cannot revive an old request. Tombstones
share the explicit `max_query_origins` bound. Different namespaces may retain
the same physical pool without sharing permission. A physical process change
must agree with every remaining routing/grant reference.

Grant revisions fence updates during this transport's lifetime. The embedding
supplies current trusted grants on restart; durable control-plane distribution
belongs to the placement/operations controllers. Routing-only views and peer
pools are bounded and never initialize a voter or learner. The
[read audit](../../../../plans/multi-raft/read-audit.md) retains qualification
scope and remaining delivery work. The counter's native
and Axum HTTP paths expose consistency, tokens and bounded member options, and
their executable restart drill includes remote strong reads and cross-cell refusal.
These paths do not claim completion of MR-08 or the full delivery scope.

Fresh status checks precede staging, submission and successful output. Transient
protocol Ready work defers preflight or duplicate-receipt settlement under the
original deadline, keeping the frozen private result instead of rerunning the
callback. Exact
applied tickets and immutable receipts settle the core, and delayed proposal
responses cannot regress a completed/retiring producer. Node shutdown joins all
started callbacks before releasing directory ownership. A close report records
unfinished command IDs without proving S3 coverage or authorizing disk deletion.
See the [counter example](../../../../examples/raft-counter/README.md) for native
HTTP and Axum gateways over the same handles. Explicit follower reads, S3 archive
gates and native snapshot installation are described below. Membership controllers
and unsupported business capabilities remain separate work packages.

Incoming snapshots similarly require a `Snapshot` effect completion before the
Ready's snapshot metadata or dependent messages can advance. The host verifier
must bind the image and metadata to the exact snapshot. `SnapshotInstalled` is
emitted only after the engine has persisted that installation.

Tickets bind group/incarnation, node/member, disk/process, operation, position
and digest. An opaque process-local worker identity also prevents a callback
from a retired worker completing a reissued job in the same process. A committed
entry from an earlier leader term remains applicable through its issued ticket.
Proposal admission receipts do not establish commit or command success.

Normal close stops admission, awaits submitted log work and writes a final
nonempty storage fence before releasing consensus ownership. Concurrent and
repeated close callers share the final result; cancelling one waiter does not
abandon shutdown. Teardown serializes with synchronous request publication and
drops all queued/deferred replies and permits before reporting completion, even
while application handles survive. It is not a fleet drain or permission to
discard disks. Consensus-task cancellation or panic leaves ownership
poisoned for that log handle; close/reopen its storage before rebuilding the
worker. Execution-domain shutdown cancels the consensus task and joins the
log worker and its owned engine threads.

The elected timeout distribution is unchanged. An attributed constructor accepts
a seed drawn from the owning domain's labeled RNG; qualification fixtures can
provide a seed and explicit ticks. No consensus adapter uses a process-global
random source. Limits bound queued work, outbox events and retained apply work;
complete resident-memory and scale qualification remains in the resource and
qualification packages of the delivery plan.

`RaftTransport` runs on that runtime's execution domain. One persistent TLS
connection per peer and traffic class multiplexes every registered group;
separate control/data listeners, workers and byte/item banks reserve control
capacity. Connections, groups, physical peers, frame/header lengths and decoded
entry counts have finite limits. Incoming bodies are allocated only after a
bounded header has authenticated and matched an explicitly installed route,
local member identity and durable configuration. Unknown groups never create
storage or fetch a bucket object. Each physical peer has separate incoming and
outgoing banks, so full outbound queues cannot consume inbound response capacity.

Control transmission holds at most sixteen selected frames and refills a slot as
soon as its FIFO authenticated reply and physical write finish. A later held
reply cannot delay that refill. Each selected frame keeps its original I/O
deadline, node/peer reservations and route identity. The reader and writer make
progress together, retaining partially completed I/O across scheduling events;
failed connections close before pending reservations are released and never
blindly replay selected frames. Data transmission retains its separate 16-frame /
1-MiB bounded batches, with oversized frames alone. The control-exchange timer
measures individual selected frames; the data-exchange timer measures batches.
Queue timers remain per frame. Older control measurements used batch timers, so
their exchange counts and means are not directly comparable.

The [control-window measurements](../../../../plans/multi-raft/control-window-performance.md)
record the latency improvement, shared-host limits and remaining archive work.

The marker-1 wire uses TLS 1.3 with verified server certificates and no early
data. A distinct HMAC domain authenticates each request and response with the
existing fleet secret, a normal TLS exporter and length-framed inputs. Requests
bind the complete group/history, schema/protocol digest, both node/member/disk/
process identities, a per-connection increasing sequence and body SHA-256.
Responses additionally bind the original request tag and reverse its exact
source/target route. Reusing a response, changing the target or moving a captured
frame to another TLS connection fails authentication. Fleet's existing HTTP
request protocol is unchanged.

`submit` queues without awaiting a slow socket. Its returned `Delivery` proves
only authenticated message admission. The real Raft append response remains a
separate message emitted after the receiver's required persistence; a timeout
cannot manufacture it. Queued send failures report unreachable and snapshot
failure hints while their routing view is still current. The host dispatcher
must retain or report immediate queue refusals. Dropping a delivery does not
refund queued/in-flight work. Peer and node usage counters expose held bytes and
items; permits release on completion, refusal, close and domain cancellation.

Receiver frame admission validates the current member and bounded message before
queueing it, without waiting for that group's Ready or image installation. Raft
execution validates again against its then-current membership. Queued messages
retain the node request charge and an independent group reservation. With default
runtime options, a group holds at most 32 peer requests and 20 MiB plus 4 KiB of
charged bytes; options document the derived bounds. One held snapshot therefore
cannot fill the default node queue or block another group on the pooled control
connection. Exhausted group/node capacity returns an explicit overload refusal.
Sender unreachable/snapshot-status hints have reserved admission of 256 slots
and 1 MiB. The worker coalesces them to one small record per current member,
preserving failure until execution and keeping the latest ordered snapshot result.
Delivery workers never wait behind local snapshot work. Both hint admission and
execution validate the current member; retired peers are pruned. Queue contention
retries on the owning clock within the transport's existing cancellation/deadline.
Hints execute before deferred bodies once the group is ready. Native immediate
send refusals also retain snapshot-failure hints. Hints never become durable Raft
responses or install acknowledgements.

Eight extra runtime queue slots and 128 KiB admit physical apply/snapshot completion
markers independently of ordinary request saturation. They carry no encoded image.
This lets already owned work settle and release deferred requests even when eight
stalled groups fill all 256 ordinary slots. Ordinary public `step` still waits for
execution; this admission path is private to the authenticated transport. Snapshot
installation and persistence-dependent responses retain their durability barriers.

Group route installation is a trusted membership-controller operation. It must
refer to existing local storage and current ConfState, never initializes a group
and cannot change the persisted membership. Views have monotonic revisions and
immutable schema digests. A process/address replacement shared by several groups
uses the trusted process-view acceptance boundary to replace affected routes,
read grants and pools atomically. A plain group-route update cannot bypass that
physical identity check. Pending callbacks from retired views cannot credit the new
peer or report failures against it. The membership controller and durable remote
identity registry are delivered separately in MR-07/MR-11.

Transport close stops admission and joins its counted resource owners. Execution
domain shutdown also joins their task registry. A retained closed transport
handle does not retain its exclusive runtime claim. Registry poison closes
admission while cleanup still waits for the actual resource owners.
`Node::start_raft_service`
uses the embedded node's existing execution domain, filesystem and peer secret,
and the node's `raft` storage directory. It supplies infrastructure handles;
namespace/SDK command and query registration remain separate work. An embedded
node shutdown joins these resources even while application handles survive.

Transport qualification uses real TLS sockets and three OS child processes, each
with the production engine/runtime/transport. Their dispatcher supplies opaque
fixture images and controlled peer loss, duplication, reordering and delay. This
does not qualify SQLite, LTX, S3, the complete SDK, workload capacity or production
failure domains.

## Committed archives and S3 output

`host_archive.rs` owns a bounded window of archive producers and one exclusive
verification workspace per node. `RaftArchiveOptions::max_producers` permits
one to four concurrent publications across distinct cells (default two). A
slot stays occupied from capture through completion, preventing duplicate
publication of one cell. Object I/O overlaps; physical replay remains serialized
by the verifier owner. SQL workers capture immutable published image/engine cuts;
`archive/mod.rs` performs bounded object I/O and physical verification, and
`manifest.rs` describes complete snapshot/tail/dependency sets. A conditional
head advances only after referenced bytes are read back, validated and replayed.
Lost object/head responses are reconciled by exact reads. The supplied
`Bucket::raft_store` binds the bucket prefix and its conditional-write client.

Each admitted publication batch runs on an execution-domain blocking worker
that drives its asynchronous object I/O. Snapshot file disposal during retry,
failure and completion stays on that worker, keeping the host driver available
for request admission and Raft work. The existing producer limit bounds worker
count; capture reservations remain charged through physical cleanup, and the
driver joins publication workers before releasing their slots during shutdown.

Quorum commands settle after commit and durable image publication. S3 commands
also need an archive proof covering that exact command. The namespace minimum is
persisted in the receipt; stronger retries wait without executing or proposing
the business command again. Archive completion advances only its captured local
history prefix. It does not import another member's applied progress or create
membership/leadership authority. Snapshot installation and fresh-member recovery
remain separate operations.

`RaftHostOptions::archive` configures `RaftArchiveOptions` and `ArchiveLimits`.
Defaults allow two retained cell cuts across capture and publication, with up
to two compatible cells per immutable-object batch. Each cut reserves 4 MiB of
captured log bytes (8 MiB node-wide by default). Logical objects are bounded at
128 MiB and each cell recovery set at 256 MiB, 32 tails, 1,024 tail entries and
128 dependencies. `max_batch_groups = 1` selects independent publication;
`max_producers` still bounds all retained cuts. A completed capture waits at most
5 ms for a companion capture before it becomes eligible for publication.
`ArchiveLimits::max_tail_image_ratio` also bounds the projected encoded tail
bytes relative to the current image's uncompressed length (default 2; accepted
range 1–128). Crossing that ratio selects a new snapshot before the hard
tail-count or recovery-set limit is reached. It bounds repeated history reads
and replay for small images while allowing short tails for large images.
Higher ratios trade fewer snapshot writes for more retained-history work.
Size-triggered rotation physically verifies the complete eligible extension,
including intermediate updates, before replacing its tails with a snapshot.
It then validates the new recovery proof before advancing the head. Changing
this producer policy does not invalidate an existing archive.
New callbacks reserve against 256 MiB of unarchived log/image admission and 4,096
data-bearing entries. Protocol entries consume bytes too. Existing duplicate
lookup remains available under backpressure. Incoming replication also has the
transport/log/per-image hard bounds; this cap is not total process or disk usage.
The node capture bank reserves `max_producers * max_capture_bytes` across at
most `max_producers` cuts. Each producer retains its own bounded recovery sets;
their aggregate memory scales with this explicit concurrency limit. All
producers share cancellation and are joined before the single replay workspace
is released. Setting the limit to one retains serial publication.
Recovery reads overlap at most four objects. Each may temporarily retain an
8-MiB containing bundle while selecting its logical object, so archive seed
admission reserves an additional 32 MiB above the logical recovery-set bound.
The reservation remains held through physical extraction after cancellation.
This does not change the logical archive format or its accepted size limits.

Retained log/image charges are reused for the same group cache, durable cache
revision and archive position. Storage publishes the revision under its cache
write lock after a successful sync; append, replacement, apply and snapshot
changes therefore invalidate prior accounting. Each admission still checks
closure, cache poisoning and current pending reservations. This avoids walking
unchanged histories while preserving the existing admission decisions.

The producer gathers at most two due cuts from the same namespace. Small snapshot
and tail uploads can share a content-addressed bundle under the namespace archive
prefix. A bundle is at most 8 MiB including its bounded index, and also fits each
member's object limit. The coordinator uses a 5 ms coalescing deadline for a
second ready payload; lone or large objects use direct publication. Dependencies, manifests
and conditional heads remain separate. Successful cuts with newer backlog become
eligible immediately; failed attempts retain the configured retry delay.

The marker-1 index binds cluster, namespace, cell, group, incarnation, namespace
root, birth, schema, object kind, exact term/index range, offset, length and
payload checksum. Ordered, non-overlapping members partition the body exactly.
Recovery verifies the complete bounded bundle and selected member before replay;
a foreign member or corrupt byte cannot supply a successful recovery set. Each
cell physically replays its own complete snapshot/tail/dependency set before its
head CAS. Partial publication across members is allowed; no cross-cell atomic
commit or shared durability proof is claimed. Batching can retain two complete
recovery sets; resource reservations are not a physical RSS qualification.

This archive reference change is a marker-1 hard cutover with protocol identity
`cellar-raft-ltx-1/members-1/seeds-1/s3-1/repairs-1/nodes-1/bundles-1`. Use a fresh
experimental namespace/prefix and local state. Earlier measurement evidence keeps
its original executable and storage format; no migration reader is provided.

Archive captures have an independent budget from command/query output. Reads
have one 10-second deadline including their stream. Recovery material uses at
most four concurrent object reads, collected in manifest order. Each object's
declared length is checked before body allocation, preserving the validated
aggregate recovery-set budget. Error or cancellation drops all pending reads;
digest, coverage and physical replay checks still precede head publication.
Extending a tail uses validated manifest metadata for planning and downloads
retained payloads once during complete final verification. Reusing a covering
head still downloads and physically verifies its material. Snapshot rotation
also checks the prior payloads before replacing them, preserving refusal of
missing or corrupt prior history.

Fresh snapshot uploads retain their anonymous source after exact conditional
read-back confirmation of every part and index. The SHA-256 of the actual
uploaded stream must equal that source's encoded digest. This private proof can
supply the same bytes to full physical validation and independent destination
replay without downloading the producer's own parts again. It remains bound to
the exact snapshot reference and complete image state. Source scratch admission
stays charged until the recovery set's last physical owner drops; failed
confirmation or replay returns no new head proof.

Part publication overlaps at most two conditional writes/read-back confirmations
per producer. Source reads and the whole-stream hash remain ordered; the index
retains source order even when confirmations finish out of order. Each part's
owned buffer moves into its upload, avoiding additional complete-part copies.
The window's PUT/read buffers fit the existing physical working-memory
allowance, independently of logical image size. A failure stops new part
admission and drains entered confirmations before releasing the source. No
index/head is exposed on failure, and independent destination replay remains
required. `raft_archive_phase` logs distinguish encoding, confirmed upload,
material download, physical replay and exact head confirmation; they are
observations, not new publication proofs or recovery-time guarantees.

Native replicas coordinate publication for actual images of at least 64 MiB
through a bounded conditional `publication` record beside the cell archive's
`head`. It is an advisory scheduling hint, with no cell ownership or durability
authority. Each boot binds its publisher identity to node, disk and process.
Acquisition, renewal and release use exact conditional versions and reconcile
lost replies by reading back the complete desired record. A late publisher
cannot release a replacement's hint.

`RaftArchiveOptions.publication_stale_ms` defaults to 5,000 ms (100..=60,000).
Competing replicas age unchanged versions on their own monotonic clocks; no
shared wall clock or lease timestamp is required. Renewal requires completed
data I/O, so a live but stalled publisher cannot keep peers waiting forever.
The renewal future joins entered I/O before the producer returns. Released
records permit immediate acquisition. Missing, malformed or unavailable hints
fall back to ordinary publication; they cannot relax data validation. Small
images avoid this scheduling traffic. Covering heads still require independent
material reads and physical replay on every replica. This reduces duplicate
snapshot creation, but does not remove full-image scans or qualify an RTO.

A newer protocol/configuration cut may rebind an existing full snapshot index
when its complete image state, including epoch, TXID, checksum, geometry and
digest, is unchanged. The producer first downloads and validates the prior
recovery closure, then conditionally writes and reads back an index reference
for the new exact position. The manifest uses the newly captured durable
configuration/member bindings. No image parts are uploaded on this path.
Missing or corrupt prior material remains a refusal. Full physical validation,
independent destination sealing and head CAS still gate publication; ordinary
recovery continues to download and verify every selected part.

`archive/replay.rs` materializes bounded batches into a private image and seals
only each batch's final generation. Every intermediate image still passes its
full SHA-256 and checksum proof. Batches retain at most 64 decoded envelopes or
4 MiB of encoded entry bytes, splitting sooner at the destination's aggregate
file/page/byte limits. A larger valid envelope runs alone under its existing
per-entry limits. Snapshot bytes are borrowed from the recovery set. Image
shrink boundaries also end a batch, preserving the disk capacity required by
entry-by-entry replay before a later growth. No intermediate image becomes an
activation or acknowledgement proof.

Failed archive attempts retry after 100 ms. Shutdown cancels I/O and joins
blocking work, then releases the workspace before returning. Startup reclaims
verification copies under their
exclusive owner and preserves the authoritative engine image before retiring
unreferenced application generations. Live pins survive retirement. Capacity
pressure can defer an already committed apply while retaining its original
runtime admission; it cannot acknowledge before publication or discard its only
recovery path. Host configuration requires compatible execution/apply/archive
limits. Native snapshot recovery requires at least six generations and eight
maximum-image copies. The explicit non-compacting profile with
`enable_snapshots = false` retains the three-generation/four-copy admission floor;
it cannot recover through native snapshots.

The default logical image limit is 64 MiB. For a larger fully materialized
image, configure every replica with
`config.with_materialized_image_limit(256 * 1024 * 1024)?` before
`Node::start_raft_cells`. The opt-in maximum is decimal 10 GB
(10,000,000,000 bytes). This method coordinates
execution, apply/decoder geometry, snapshot and archive sizes, generation disk
admission, archive scratch space and transport working-memory reservations.
It preserves the independent per-command envelope limits. Larger encoded-body
allowances alone cannot admit images beyond `max_snapshot_image_bytes`; S3
recovery also checks `max_seed_recovery_bytes` before reading its complete set.
Defaults retain the previous memory and disk bounds.

This materialized path still copies and hashes complete images. Snapshot files
and object parts use bounded I/O and explicit scratch admission. The native
materialized RustFS and direct-peer drills pass at 9,999,998,976 SQLite bytes
with owner/local state loss, replacement-local new writes, restart and receipt
verification on the documented two-volume layout. The
[qualification record](../../../../plans/multi-raft/10gb-implementation.md#current-qualification)
states its scope, timings and remaining fault/capacity checks; it establishes no
recovery-time guarantee. The
[large-cell recovery plan](../../../../plans/multi-raft/large-cell-recovery.md)
specifies incremental immutable roots, bounded transfers, warm promotion and
verified demand loading for fast cold activation, targeting Cloudflare's
10 GB (10,000,000,000-byte) per-object SQLite storage quota. The owning
`native_raft_large_images` test crosses 64 MiB through command execution,
leader takeover, object-store seeding onto an empty worker and subsequent writes.

The [archive audit](../../../../plans/multi-raft/archive-audit.md) records the
proof boundary, resource contracts and qualification scope. Run the owning
`native_raft_archive` target for native SDK durability, store faults, isolated
restore, retention and three-voter failover. It does not qualify physical S3,
power loss, production scale or automatic member restoration.


## Native snapshots and member recovery

`snapshot.rs` supplies bounded source capture, full LTX encoding, ordered chunk
assembly and destination generation sealing. Marker-1 descriptors bind the full
cell history, immutable birth/schema, index/term, canonical ConfState, exact image
marker and encoded payload length/digest. Unknown fields and noncanonical metadata
are refused. A source package pins its generation through transfer/checkpoint
sync; a download creates no Raft authority. A prepared image produces a completion
only for the exact runtime ticket and becomes public only after the corresponding
engine snapshot and applied marker are observed durable.

`ImageApplier` retains the snapshot generation independently from the latest
applied generation, including across restart and newer data publications. A new
durable checkpoint replaces the old reference; reader/transfer pins continue to
protect retired bytes. Snapshot encode/download operations belong on the owning
blocking domain. `host_snapshot.rs` schedules native checkpoints, source captures,
downloads and publication; `snapshot_transport.rs` supplies member-only bounded
pulls over separate TLS data connections. Explicit fresh-learner peer seeding is
implemented below. Exact-reference S3 seeding and public joined-node restart are
also implemented. Seven installed-replica snapshot cuts and eight fresh-learner
cuts for each of peer and S3 recovery cover process-kill boundaries. Automatic
placement and repair remain MR-11. The owning verification target is
`native_raft_snapshot`; source-pinned qualification lives in the
[snapshot audit](../../../../plans/multi-raft/snapshot-audit.md).


`RaftRuntime::checkpoint` admits metadata and a verified pin into a dedicated
storage phase; encoded image bytes stay outside consensus queues. Busy Ready/apply
state is refused promptly. `Checkpointed` follows sync and the monotonic retained
reference handoff, including while closing. Prospective references preserve image
bytes across uncertain I/O while the previous durable reference remains protected.
Stale retirement cannot revoke either reference. Physical incoming completions
carry the same no-I/O handoff capability bound to their exact ticket and digest.

Empty/configuration-only snapshot descriptors carry no image or encoded payload.
They preserve configuration/index/term without allocating a SQLite database; the
first committed LTX update can subsequently establish the initial image.
`recovery.rs` validates coherent engine-selected reference sets before startup GC.
Native checkpoint scheduling defaults to one cut per 256 applied entries. When
the aggregate durable log cache reaches half its byte capacity, eligible groups
above an average share also checkpoint before reaching that entry count;
`checkpoint_entries = 0` disables automatic compaction. The host pins and registers
the exact requested source cut before sending `MsgSnapshot`. The receiver validates
the descriptor against its local ticket, fetches ordered chunks and seals on its
SQL owner. It waits for `SnapshotInstalled` before publishing the image and
releasing waiting reads. A snapshot arriving during older physical apply is
aborted so that owner can complete before a fresh transfer. Late completions cannot
authorize the new ticket. An authorized install must instead be joined.

Snapshot bodies have separate node admission: one source capture/lease and one
receiver, each charged for encoded bytes plus four maximum-image working copies.
Neither consumes command/query output budgets. Defaults allow 128 MiB encoded,
1 MiB chunks, a 30-second whole-transfer deadline and the existing five-second I/O
idle deadline. Source leases also bind the current member/process route; source
pins survive checkpoint replacement until the lease completes or expires. Empty
snapshots need no body lease or socket. Delivery failure only releases the matching
source lease; it never proves install. TLS framing authenticates both directions,
the descriptor digest, full physical route, transfer/offset and payload digest.
Revisions and membership are rechecked throughout a transfer. Public status
includes the durable checkpoint boundary without granting any authority.

The native recovery drills isolate a follower until its leader has compacted the
missing prefix, then require physical snapshot installation and an AtLeast query.
Other drills interrupt ciphertext after the handshake and initial chunks, require
retry without blocking a second group's writes, and join shutdown while a partial
transfer remains held. Followers execute no business callbacks. Startup refuses
inadequate restore admission before creating catalog objects or member storage.
Replacement identities after disk loss require the separate committed learner
binding and fresh-quorum seeding path below.

`SnapshotPackage::retained` captures the exact durable checkpoint requested by a
transfer, even after a newer image becomes applied. It verifies the selected
descriptor and re-encoded payload; a replaced checkpoint is refused. A captured
package pins its bytes through subsequent checkpoint replacement and reclamation.
Empty checkpoints retain no image or payload.

The 64-MiB database limit remains unchanged. Full LTX framing, index entries and
compression overhead can exceed that size, so snapshot, native image-restore and
archive object budgets allow 128 MiB of encoded bytes. Configuration refuses
budgets below a conservative full-snapshot bound before member storage opens.
The component fixture transfers an exact 64-MiB incompressible SQLite database
and restores its newer archived state after deleting both local node directories.
It uses the in-memory object-store adapter; it does not qualify physical S3 or
authorize replacement membership.


## Explicit fresh-learner peer seeding

`RaftCellHandle::add_learner` commits a revision-bound `JoiningMember` containing a
fresh member ID, node ID and disk lineage. The node endpoint must already exist
in the trusted peer configuration. This administrative API cannot introduce an
arbitrary endpoint or invoke a business callback. Repeating the same identity
reconciles a lost receipt; an error or cancellation may follow commitment, so
reuse the identity and expected revision. The receipt supplies no seeding proof.
Routes refresh from the synced membership; removed members stop local admission.

Start the target with `RaftCellPlacement::Joining { member, source, source_node }`.
The existing namespace and immutable birth must match. `seed_transport.rs` uses a
purpose-separated TLS exporter, a fresh request tag/nonce and exact physical
routes. `host_seed.rs` waits for an actual safe ReadIndex result and a published
cut covering it before capturing a source snapshot. A cached configuration or
available S3 objects cannot replace this live-quorum proof.

The target grant is opaque, bound to its exact local engine owner, process and
original deadline. One bounded metadata reservation and one receiver body may
exist per node. Ordered chunks bind the snapshot digest and source lease; transient
transport failures obtain a fresh grant under the same deadline. A new request
supersedes the earlier source lease to that learner, whose outstanding stream
retains its pins until bounded I/O exits. Identity/authentication failures remain
refusals.

`snapshot_seed.rs` validates the original birth, exact learner binding, included
configuration and barrier before sealing the physical image. The engine creates
the mapping, snapshot, current configuration, bindings and applied marker in one
synced batch, retaining the original birth ConfState. Its HardState starts with
vote zero; no source vote, leadership or process state is restored. The image
stays retained across an uncertain engine write.

Only then does the catalog conditionally create and read back the permanent
member claim, followed by owner-bound runtime attachment. An existing claim with
missing stable group state refuses startup. Stable joined state can reconcile a
lost claim response without resetting its log or votes. Normal follower proofs
still govern queries after attachment.

The internal namespace protocol identity is
`cellar-raft-ltx-1/members-1/seeds-1/s3-1/repairs-1/nodes-1/bundles-1`; format markers remain 1. Use a fresh
experimental namespace/prefix and local state after this hard cutover.


## Exact-reference S3 learner seeding and restart

`RaftCellPlacement::JoiningFromS3` obtains an authenticated grant from a live peer
under a fresh safe ReadIndex. The source selects bounded archive metadata for an
exact immutable `ArchiveReference`, covering the barrier and the target learner
binding. Metadata selection does not download image/tail/Blob payloads or wait
for a private business callback. The target downloads only that selected set;
missing or corrupt bytes refuse startup even if a newer complete head exists.

`seed_archive.rs` retains receiver reservations through object hash/decode work
and physical replay on the SQL owner. `snapshot_seed.rs` verifies birth, schema,
cut and exact learner binding, replays snapshot/tails, seals a local generation
and constructs its actual snapshot descriptor. The shared atomic seed/claim path
then attaches with vote zero. S3 supplies cell state and committed history; it
supplies no old votes, process identity, leadership or replacement quorum.

The receiver shares one body slot with peer snapshots, charged for configured
snapshot working memory plus `max_set_bytes` (at most 256 MiB). Default capacity
is 640 MiB. Seed metadata holds one 4-MiB reservation per node, sufficient for a
configured 1-MiB manifest and verification copies. Reservations are accounting
bounds and are not eager allocations.

Public Node restart resumes verified local joined state and reconciles an exact
permanent claim without reseeding or resetting votes. Tests start the learner
before voters, permit retained causal reads and refuse strong reads until quorum
returns. Three claim cuts cover failure before PUT, a lost successful response
and a lost confirmation. Fresh-learner process-kill matrices cover image fsync,
rename, directory fsync, atomic engine sync, engine reply, claim and attachment,
then verify recovered reads and subsequent replication without callback replay.
The process fixtures preserve successful conditional in-memory object mutations
across child exit; they qualify no physical S3 or power-loss behavior.


## Committed repair plans

`RaftCellHandle::begin_repair`, `advance_repair` and `repair_status` expose a
configuration-owned three-voter repair sequence. The immutable plan ID/payload,
policy revision, basis configuration, replacement binding and phase persist with
ConfState/applied markers. Phases are learner seeding, explicit joint promotion/
removal, completion, or abort while still an unpromoted learner. Outgoing members
remain through leave-joint; promotion checks actual leader matched progress.
Status is an observation, without S3 output or fresh activation proof.

Retries reconcile the same retained plan, including a lost/unpolled response.
Stale/rebound intents append nothing. Aborting consumes the fresh member ID;
retired IDs remain unavailable. Generic configuration changes cannot bypass an
active plan. Complete/aborted status remains as a bounded last-plan record while
later ordinary configuration changes proceed. Snapshots and archive manifests
carry the same state; active phase/configuration mismatch refuses recovery.

`cellar_logic::raft_placement` selects bounded deterministic replacements from
fresh compatible capacity, pools and failure-domain policy. It excludes occupied,
draining or full targets, accounts for the limiting group/storage utilization,
and never grants membership authority. Missing/stale samples alone establish no
failure. Existing plans and joint states must be reconciled first. The controller
and inspection/process-view integration remain MR-11 work. Explicit native
APIs and their tests do not establish automatic redundancy repair.

### Registered standbys

`RaftCellPlacement::Standby` registers an existing history/class without creating
a group or SQLite image. It can resume its previously initialized joined member
on restart. Retired or conflicting local state is retained without admission.
A standby is not an initial birth member or a query router.

`RaftCellHandle::activate_learner` accepts the intended member and a trusted source
endpoint as hints. Before initialization it checks local state and permanent
claims, then obtains the existing fresh-quorum peer/S3 seed grant. It seals the
image on the existing SQL owner, atomically initializes the existing node-wide
engine with vote zero, reconciles the permanent claim and attaches the runtime.
Only then does a shared one-way local member slot release retained handles.
The slot cannot rebind a handle to a different member identity.

One initializer per node bounds work in addition to the existing source/receiver
body reservations and SQL queues. Network/catalog attempts retain the caller's
original deadline, capped by the configured seed deadline. Dropped waiters do
not own submitted initialization or physical SQL work. An unavailable response
may follow initialization; retry the same target identity. Failed attachment
refuses admission and preserves stable bytes for restart. Available S3 and a
cached learner plan do not replace a fresh quorum grant.

Native tests cover a held transfer with a dropped waiter and retained handles,
pre-admission/no-claim refusal, S3 activation and target-first public restart,
quorumless S3 refusal, later promotion/replication and joined shutdown while a
transfer remains held. These are explicit local activation APIs; controller
scheduling and automatic repair remain open. Node-directory distribution and
authenticated inspection are implemented above.

`RaftCellHandle::replication_status` returns bounded leader-local matched
positions and recent activity. These are observations, without apply or quorum
proof. `transfer_leadership` requests movement to an existing voter at an exact
term and configuration revision. The serialized owner refuses lagging or
inactive targets, learners, joint configurations, active repairs and competing
transfers. Ordinary peer frames cannot request a transfer around these checks.
Success means the request was admitted; callers must observe the new leader
separately. An unavailable response does not establish whether movement occurred.

The dependency-free leadership planner checks fresh node load and applied/matched
progress, respects a minimum interval between attempts, and uses an exact
capacity-weighted deadband. Normal balancing must lower peak utilization without
reversing the nodes' load order. Draining bypasses the load threshold while
preserving target eligibility. Native tests cover refusal while a follower is
isolated, caught-up movement after healing, subsequent writes/reads and refusal
during an active repair. Complete node-load collection is available through
inspection; automatic balancing remains open.

This extends the internal protocol to
`cellar-raft-ltx-1/members-1/seeds-1/s3-1/repairs-1/nodes-1/bundles-1`; numeric format markers stay 1.
Earlier roots/state have no compatibility reader. Use a fresh experimental
namespace/prefix and local state. The earlier MR-10 evidence retains its exact
historical protocol and source scope.
