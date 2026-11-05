<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Cell-group state and execution

This component owns physical-group manifests, execution, checkpoints and local
recovery. Canonical SDK hosts bind declared namespaces and admitted groups to
these owners. A stored root or local attachment is not fresh quorum authority.
See the [delivery status](../../../../../docs/design/multi-raft/cell-groups/remaining-work.md)
for the remaining public fleet and backend qualification gates.

## Managed startup outcomes

`cellar::sdk::consensus` exports `CellGroupStartupError` and its nested
`CellGroupControllerError`. `Refused` means this invocation submitted no
preparation. `Unconfirmed` can follow durable installation or attachment; retain
the original history, permit and resource debit, and reconcile from fresh
observations. Neither result resolves a prior lost reply. A locally installed
learner must still catch up through Raft before promotion. Reopening a joined
member uses its retained seed receipt and permanent member claim; its old-boot
learner grant cannot initialize work on the new boot. The movement reconciler
also reports provider I/O during terminal settlement as `Unconfirmed`: a failed
confirmation read cannot undo an already committed root CAS. Reconcile the same
reservation and verify its permanent receipt. Corruption and authority failures
keep their typed errors. Settlement removes the active reservation while
retained source storage remains charged.

## Managed maintenance and retained return

Authenticated fleet observations sample leader load from actual attached group
owners. Each runtime turn visits at most sixteen owners; a roster revision,
complete population check and original deadline span the scan. A failed owner,
roster change, pending attachment or incomplete physical inventory supplies no
known headroom. Counts describe physical groups, independently of cell count.
This advisory sample supplies neither a transfer grant nor serving authority.

A catalog-authorized maintenance return starts paused. Its exact new boot may
reattach an existing voting member only after the directory verifies the current
Live group, settled membership, returned-boot lineage, retained state, permanent
member claim and complete image closure. Joined members also retain their seed
receipt. The directory rechecks the local quiescence guard after catalog I/O.
A paused return cannot create genesis or a missing claim, seed a learner or admit
business writes. Generic new-group startup continues to require the ordinary
open/resumed guard. Fresh catalog resume proof and capacity settlement remain
separate from local attachment.

A public command refused by the maintenance barrier reports temporary
`Unavailable` with no submission by that invocation. The cause is captured under
the registration lock, rather than inferred by rereading a flag after resume.
Permanent closure remains a distinct cause. Background effects defer a captured
temporary refusal, including when resume races with their next flag read.
Neither this refusal nor a lost lifecycle response resolves an earlier mutation.
A remote maintenance stop or resume returns `Unconfirmed` after enqueueing its
bounded Node-owned proof work. Exact rooted authority and payload are checked
before enqueue, preserving typed refusals. The worker rechecks them before
collecting fresh physical/quorum evidence and syncing the guard. Releasing the
transport waiter lets the survivor leader
renew its inspection within the existing two-slot peer metadata bank. A later
exact retry can observe the synced effect; queueing alone is never acceptance.
Reconcile an uncertain stop using the same rooted child; signed closure requires
actual owned shutdown and join before offline confirmation.

## Recovered public-node startup

An offline recovery operator persists the exact recovery intent, obtains the
infrastructure fence, verifies the complete archive closure and seeds every
replacement voter with a fresh disk/engine and disjoint member/node identities.
It publishes that same intent only after every required seed receipt is durable.
Archive setup has its own one-use intent, which must be synced before commit.
None of these steps creates a live quorum.

`RaftCellsServices::recovery` supplies the independent
`DisasterFenceAuthority<CellGroupDisasterFenceRequest>` for unmanaged public
startup in `LogOpenMode::Resume`. Each assignment must already have the exact
local recovery genesis, seed receipt and retained engine/disk pins. Startup
locates the original request through the selected birth and immutable binding,
then independently verifies the external fence. A missing authority, rejected
fence, missing image or blank disk refuses; startup cannot regenerate these
proofs from object-store metadata.

One node-owned control service requests activation from the actual shared Raft
owners. Its metadata uses the existing control bank, and queued permits retain
their original deadlines. Startup may return before a fresh quorum exists;
application admission stays closed until activation commits and applies. Only
that acknowledgement can complete the original catalog intent. Unknown storage
outcomes retain the same intent for reconciliation. An authority or corruption
failure quarantines that group. Shutdown signals and joins entered recovery
verification/publication before closing consensus and storage.

A retained restart uses the same disks and independently verifies the fence
again. `RangeDirectory::group_recovery_pending` observes the exact published
intent; a completed status requires its activation receipt and selected route.
This metadata is neither current quorum proof nor serving permission. An
unrelated recovery lock cannot substitute for this group's activation receipt.
Managed disaster recovery still requires its fleet recovery ownership contract;
an unmanaged authority cannot enable that path.

## Manifests

A manifest is a persistent AVL tree ordered by the namespace routing digest and
full cell ID. Each bounded page holds one cell leaf and at most two child
references. References bind SHA-256, height, subtree count and earliest due-effect time. Insertions and
updates copy bounded paths; rotations preserve balance. Unchanged cell images
are reused. The format uses `CLRCGM01`, the marker-1 page hash domain, canonical
JSON and a 16 KiB decode bound. Tree depth and cell count are explicit limits.
These limits are admission ceilings, not qualified fleet capacities.

The due-effect index stores each subtree's earliest alarm/outbox time in its
required nullable page reference. `ManifestRoot::next_due_unix_millis` is a cheap
hint after complete-root verification. `ManifestTree::next_due` visits bounded
paths to select one due cell after a routing-key cursor, wrapping once for fair
selection. Future-only subtrees need no SQLite hydration. A prepared physical root also
exposes `next_due` over its verified persistent pin tree, with no filesystem or
SQLite I/O. Its returned image pin is a data view and grants no execution rights.
The caller runs page
I/O on its blocking execution domain and retains the selected verified root;
returned metadata still requires fresh leader, route, image and private-command
admission before execution. Missing scheduling fields refuse at marker 1, with
no compatibility reader. The shared controller now integrates indexed discovery
and private system commands. Full CG-08 effect/cache qualification and the public
SDK/fleet cutover remain unfinished; qualification is scoped to its recorded source.

Leaves bind logical cell history, mutation sequence, image commitment and
geometry, last group installation/mutation position, schema, effect summary,
immutable original enrollment identity and the selected version's complete
`CommitWitness` (original group history, term/index and proposal ID). A page
without either the enrollment or witness field refuses at marker 1; no old-layout
decoder or inferred identity is used. The witness is part of the authenticated
page and survives unrelated group progress, restart and image collection. A local
installation position remains separate from original decision provenance for
future transfers. Shape validation of foreign-group provenance grants no transfer
authority; the transfer protocol must establish its lineage.

Ordinary `prepare_update` requires the witness to name the current group and
its exact mutation position. The Raft owner additionally matches enrollment and
mutation witnesses to the actual committed entry's proposal before publishing.
An otherwise correct image cannot supply a fabricated proposal witness. Repeated
enrollment and unchanged entries retain the existing leaf and witness exactly.
`CellManifestLeaf::observation` exposes version/witness data; only an authoritative
selected root establishes its commitment. Local image
filenames are excluded: replicas may store the same image under different local
names and still derive the same logical manifest. A physical image adapter must
resolve and pin each image by the selected logical leaf. Extent and materialized
commitments retain separate domains.

The page store uses an injected filesystem, a lifetime exclusive lock, bounded
inventory and byte/page quotas. A new page crosses file and directory sync before
its reference is returned. Any ambiguous write/sync quarantines the owner until
reopen. Reopened filenames are only inventory; reads verify length and digest,
and reuse verifies and syncs bytes again. Every prepared pin-tree node holds a
page lease and the directory lock. Dropping a store handle cannot let a replacement
owner collect pages still selected by old readers. The store explicitly bounds
retained root slots (2–4096), reserving current and pending slots for each group.

`verify` traverses the entire authenticated closure, checks ordering and the
selected applied cut, and delegates actual image verification/pinning to its
caller. The callback must release any partial pins if verification fails. A
successful page traversal alone is not an image seal, Raft commit or serving
capability. Updates compare the expected cell version and require increasing
sequence, LTX position and group mutation position.

`prepare_complete` is the full-closure path for bootstrap, recovery and
checkpoint preparation. Every leaf must match a real materialized generation
pin, sealed extent image, or canonical empty image. Extent roots bind the stable
logical cell-history digest and schema. The method verifies and syncs all pages,
retains physical image pins and binds the preparation to the exact engine owner.
This full scan establishes the capability used by ordinary writes.

`prepare_update` compares the prior cell version, requires the next group index,
verifies the changed image seal and synchronizes the changed manifest path. A
persistent in-memory AVL pin tree mirrors the manifest. Unchanged subtrees are
shared across prepared/published roots; changed paths and rotations reuse the
unchanged cell image pins. Preparation performs O(log cells) page work and node
allocation, with O(log² cells) bounded in-memory comparisons. Cloning a root is
constant-time. `pin_cell` selects a coherent leaf/image pair from that root; read
authority still belongs to the caller. Memory admission conservatively charges
each owner for its entire tree, including cell IDs and pin-node allocations.

Incremental capabilities bind the exact previous manifest and page-store owner.
The engine refuses a staged successor of a different branch at the same index;
an identical already-selected result remains retryable. Reopening the page store
requires full preparation again. A live old root or individual image reader
prevents generation reclamation after a newer root publishes. Stores still need
authoritative retention reconciliation before retiring inventory, especially
after an uncertain engine outcome; a disposable request pin alone is insufficient.

After validating an engine batch, storage reserves physical root pins before
starting the write. Unarmed reservations can be cancelled without persistence.
Once armed, dropping the reservation before cache publication finishes quarantines
the retention owner and keeps both old and candidate closures. Successful cache
publication selects the candidate and releases the old engine reference. Identical
retries use the same retention protocol. No retention mutex spans engine or file I/O.

`prepare_complete` automatically registers a recovered root only when it matches
the engine-selected marker. An existing group cannot publish through an unrecovered
retention owner. Before collection, the owning service must prepare **every**
engine-selected root assigned to this directory and call `reconcile_retained` with
the complete additional snapshot/archive/transition root set. This is a trusted
inventory handoff, not discovery from page filenames or proof supplied by clients.
The future shared host must obtain that inventory from its authoritative catalog.
Replacing the additional set never releases the engine's current roots. Pending
publication, an uncertain outcome or engine closure prevents reconciliation and
collection until restart. Reopening requires recovery and reconciliation again.

`reclaim` scans at most the explicit scan limit (1–4096) and resumes from its last
inventory key. Only pages without physical leases can be selected. Deletions and
the directory barrier finish before page/byte quotas are refunded; failure keeps
the prior charge and quarantines the owner. Reader, apply, snapshot and transition
pins independently protect their closures. Image-store collection still follows
its own complete durable-reference reconciliation contract; the retained manifest
pins prevent it from deleting selected or uncertain images in the live process.

The engine persists group root, applied position, membership and log/hard state
in one synced batch. It requires contiguous apply, matching group/birth/range,
matching membership revision and the current engine owner. Cached readers see
the old root until sync completes. Old single-image accessors and snapshot writes
refuse a group root; future group snapshot installation uses its own complete
closure protocol. Recovery first observes the selected metadata and then repeats
full closure verification before any serving admission. Missing images or pages
cannot become empty cells.

The local application record now has explicit image/group semantic domains at
marker 1. Old bare-image record shapes refuse; there is no old-layout decoder or
automatic migration. The staged image variant supports existing code while the
remaining production consumers are converted, and is removed at hard cutover.
Existing disks and archives must be retained, not reset by this component.

Further integration must wire complete catalog/checkpoint/transition inventory
handoff into the shared host. Native recovery fixtures exercise both physical
image modes with independent SQLite value and receipt rows. Process-kill cuts
cover image/page writes and sync, engine sync, publication and reclamation;
restart verifies every selected cell before collecting orphaned objects. Missing
or corrupt pages, images, extent roots and non-header extents refuse complete-root
preparation. These are storage-boundary checks, not quorum, SDK deduplication or
RustFS qualification. The full CG-02 source-bound evidence gate is recorded in the
delivery plan.

## Complete-group checkpoint storage

`CellGroupCheckpointPool` admits both sources and receivers under one node-wide
item/byte budget. A source captures the exact engine-selected root, membership
and configuration. It retains immutable dependencies while later writes proceed,
and streams bounded chunks without aggregating the group images.

A receiver binds the destination engine and its selected base. It validates
ordered page/image parts, route/history, cut, membership and complete tree closure.
Materialized images spool one at a time under a shared `ScratchPool`, then undergo
independent SQLite/checksum verification and destination sync. Extent manifests
and objects are verified against each leaf and independently sealed. Partial,
corrupt, foreign or over-budget transfers cannot publish. All physical work runs
on a joined blocking owner; reservations follow that owner through completion.

`PreparedCellGroupCheckpoint` owns the sealed destination closure and receive
reservation. `SnapshotChange::CellGroup` atomically publishes its snapshot,
configuration, membership and applied root through the shared engine fence.
Mixed single-cell/group writes, conflicting bases and stale cuts refuse. The
capability establishes physical storage only: its future Raft consumer must bind
it to the exact accepted snapshot effect before submitting the engine write.

The page owner retains the installed checkpoint separately from the advancing
current root. Checkpoint retention consumes a third root slot in addition to the
current/pending publication slots. Recovery reconstructs both complete closures;
a missing older checkpoint refuses startup and reclamation even when the current
applied root is complete. Physical recovery never renews quorum authority.

Canonical startup recovers every retained physical group's complete current and
checkpoint image graphs before releasing conservative filename retention in the
shared serving inventory. It refuses an incomplete assignment and publishes no
peer/public routes before this adoption. Complete manifest pins keep both
selections alive; uncertain publication additionally retains its pending closure.
Fresh applied materialized/extent images transfer their live pins into that graph
without adding a permanent host reference for every entry. An existing image's
retention is preserved. Disposable command/enrollment previews retire only after
exact terminal owner reconciliation, retaining their physical pins until work
finishes. Unknown outcomes keep the pending owner. Materialized preparation uses
its existing bounded collection; transient extent roots still need node-owned
collection. The offline complete-inventory reclamation API remains offline.

These are verification APIs. Ordinary peer ingress still rejects snapshot
messages. `step_checkpoint` on the owner/runtime accepts an authenticated envelope
only with its exact receiver-local closure, engine/page owner, selected base and
membership binding. It requires settled prior apply and delegates restoration to
RawNode. Admission returns `Restoring` or `Ignored`; neither is a persistence
receipt. A matching log can advance commit and replay entries without installing
the received image. A restored snapshot is attached to its exact Ready write;
only the matching engine completion advances the application view and releases
Raft responses and `CheckpointInstalled`. Queued work retains the receiver budget
through engine sync and runtime shutdown.

An unstreamed captured source can become a `PreparedLocalCellGroupCheckpoint`.
`publish_checkpoint` admits only the owner's exact current applied cut and runs
publication on its persistence lane. The distinct `CellGroupCheckpoint` storage
operation preserves hard state, applied state and every later log entry; it
compacts only the prefix represented by the complete checkpoint. Mixed writes
and stale cuts refuse. `CheckpointPublished` is emitted after the matching engine
completion. Source admission and pins survive accepted publication and shutdown.

Archive registration owns an activation intent across retries, closing later
checkpoint admission while earlier exact cut-bound leases drain. Owner and
storage share the same lease so entered work can finish under the preceding
policy. Rejected or unentered shutdown work releases its intent; entered metadata
I/O retains it after waiter cancellation and fsyncs the required retention bit
before object-store I/O. Only successful durable publication completes intent.
That bit is part of the required marker-1 group application record, remains
set through ordinary writes and snapshot installation, and is recovered on open.
The in-memory verified archive frontier is deliberately absent after restart.
Local checkpoint publication and the storage preflight refuse a cut beyond that
frontier. Only the controller's independently verified complete archive coverage
advances it; the public archive status cannot authorize compaction. Its journal
continues to own the current remote hold. The frontier retains bounded marker
metadata, without keeping a replay workspace per idle group. A behind-archive
checkpoint is skipped before capture so quorum apply and sibling checkpoint work
continue. This is a prerequisite for retained-log admission, not its byte/disk
bound or the physical Raft file reclamation gate.

`capture_snapshot` pins the retained checkpoint named by the exact Raft snapshot,
including its original membership/configuration, after ordinary apply has advanced
the current root. A concurrent checkpoint replacement may refuse capture; a
successful source keeps its immutable closure until streaming ends. Recovery
reconstructs this retained closure before it can become an outbound source.

Snapshot RPC transport, learner catch-up, joint membership and disk replacement
remain CG-05 work. Configuration entries remain closed. Neither checkpoint API
authorizes a new voter or establishes S3 durability.

## Enrollment admission and ordered preparation

`decide_enrollment` checks a bounded cell-addressed enrollment entry against the
current group route and committed lifecycle state. Sealed and retired ranges
refuse, including delayed entries prepared before the seal. Active admission
returns either a prospective creation record or the identical committed record.
A competing operation, cell incarnation, schema or initial image cannot replace
the winner. The same decision must run again at ordered log application; a
preparation-time check is not permission to bypass a later seal.

`EnrollmentRecord` binds logical cell history, original creation operation,
schema and initial image. It deliberately excludes physical group placement, so
normal transfer preserves the record while the request adopts the current route.
Its bounded canonical marker-1 codec verifies data shape, not authority. The
record is now required in each authenticated manifest leaf and published with
the selected image. Ordinary mutation refuses any changed creation record.
The record matches the cell history/schema; sequence one must select its initial
image, and later sequences must advance that image's LTX position. A normal
transfer carries the same record. It records an initial image commitment without
pinning that superseded image forever. Complete snapshots and archives must
retain this metadata as part of their selected manifest closure.

`prepare_enrollment` rechecks a bounded enrollment entry against the preceding
complete root and the caller's committed lifecycle phase. It verifies the next
contiguous group position before invoking the physical image producer. A new
creation prepares sequence one and its image seal together. An exact retry does
not invoke that producer or replace the latest image; `prepare_unchanged` advances
only the group position, sharing the same manifest and image pins without page
I/O. A quarantined page store refuses even this unchanged-root path. Competing creations refuse before physical preparation. The engine checks
the exact previous branch and committed position before publication. The future
shared owner must consume this boundary only for the actual ordered log entry;
preparation is not a quorum proof or command acknowledgement.

Current-term quorum and read authority remain required even for a recognized
retry. Shared RawNode ownership, real concurrent enrollment and business callback
settlement remain CG-04 work. The storage fixtures exercise repeated enrollment
after mutation, restart and reclamation of the initial image in both physical
image formats; they do not qualify a live shared host.

## Range directory

`RangeDirectory` stores a namespace-bound, immutable AVL range tree. Each page
binds its route, child hashes, subtree bounds, height, count and coverage. A lookup
descends one path and verifies each page; the cache has independent page and byte
limits. A pinned `DirectoryView` can remain readable after a topology change and
requires no head refresh for lookup. A fully cached path performs no S3 requests.
Views are hints; the destination must check its own current route and quorum
authority. Observation does not traverse a complete recovery inventory.

Initialization publishes at most 128 contiguous ranges covering the whole routing
domain. Every new route must match its permanently reserved group birth before
publication. A replacement removes up to 64 exact parent routes and inserts up to 64
fresh groups covering the identical contiguous union. Children advance the maximum
parent epoch by one. Paths are copied incrementally; incomplete intermediate trees
cannot become a selected root. Group IDs remain reserved after retirement.

`DirectoryMutation` contains canonical bounded bytes binding the namespace,
operation, expected head and exact routes. Persist these bytes before submission;
after an unknown result, decode and retry the identical request. One conditional
head update selects the tree, immutable journal and operation/group indexes.
Read-back confirms the selected operation even after another publisher advances
the head. Reusing an operation for different bytes refuses. The returned receipt
describes its historical decision, not the latest route or serving authority.

Genesis writes the head before a permanent pin. An interrupted initializer can
finish the same genesis, including after another initializer pins and advances it.
A missing head with a retained pin refuses reinitialization. Head observation
verifies the pin, selected journal and operation-index binding. Missing or corrupt
objects refuse instead of returning an empty directory. No namespace-wide listing
is used, and historical pages/journals are retained; remote metadata GC is not
implemented here.

Run async operations within the owning execution domain. Each operation has an
explicit timeout; a timed-out publication is unconfirmed and retains its original
identity for reconciliation. Cache locks never span I/O. Object and traversal
limits bound individual operations; defaults are not a qualified capacity claim.

Directory publication is metadata only. The future topology owner must obtain
the seal, installation, archive and activation proofs before exposing replacement
ownership. Birth/claim admission and invocation-bounded route confirmation are
described below. Managed admission and production client integration remain open. Native directory tests exercise real conditional
object-store operations in memory, including races, dropped replies, corruption,
restart and bounded split/merge churn. They do not qualify live Raft ownership or real RustFS. CG-03 combines these
checks with the separately retained physical-bootstrap and routing evidence.

## Cell-independent group birth

`CellGroupBirthSpec` binds physical group history, range, route epoch, original
creation operation and initial members. It has no single-cell ownership field;
range boundaries still contain full cell keys. The namespace binds schema,
storage representation and durability policy. Preparation sorts members
canonically and enforces the namespace voter policy and distinct member/node IDs.
The bounded marker-1 birth encoding has its own semantic record kind; its digest
is the route's birth binding. Persist the returned request bytes before submission.

Use `prepare_group_birth`, `reserve_group_birth`, directory publication, then
`materialize_group_birth`. Reservation conditionally creates the existing global
group-ID key with the complete canonical birth. Another namespace, incarnation,
placement or creation operation cannot reuse that ID. Existing records from the
old catalog also prevent reuse; they are not converted or reinterpreted. Losing
a directory CAS retains the reservation permanently. Directory publication checks
every child's exact reservation, but does not eagerly create its birth object.

Materialization verifies the currently selected route and its global reservation,
conditionally writes the exact birth, then checks selection again. Missing
confirmation remains unconfirmed; reopen and decode the original request before
retrying. A retired or replaced range cannot materialize again. A birth observation
is bound to its catalog owner; `require_current_group_birth` checks current route,
reservation and selected immutable bytes, and refuses missing or changed state.

These metadata methods expose no voter-start capability and write no permanent
member disk claim. The bootstrap path below connects the verified birth to real
engine genesis and permanent claims. Managed
namespaces refuse these unmanaged APIs until their rooted fleet authorization is
connected. This staged boundary does not permit blank-disk recovery or replace
current quorum membership.

## Fresh-history disaster recovery

Unmanaged recovery preserves one canonical `CellGroupRecoveryRequest` through
all unknown outcomes. `fence_group_recovery` verifies its exact held archive cut
and an independently verified permanent infrastructure fence. The authority must
stop every possible old writer and revoke restart; unreachable hosts and S3
markers cannot supply that proof. Retain the source hold while its provenance is
referenced.

`CellGroupRecoveryPool::prepare` restores complete data under fresh cell histories.
It grants no vote. `seed_group_recovery` binds the imported genesis to the actual
node, member, disk and engine, atomically syncs its mapping/root and then confirms
a permanent seed receipt. A lost waiter cannot release entered physical work.
`publish_group_recovery` requires every initial member to have the same complete
seed commitment and retains the topology lock after selecting the replacement.

Ordinary bootstrap refuses recovery births. `bootstrap_recovered_group_member`
requires the existing seed, immutable engine genesis, storage pin and selected
publication, and rechecks the external fence. Physical reopen must verify the
complete selected and checkpoint closures before attachment. Startup alone grants
no serving authority: pending roots reject application work, including reads.

`prepare_group_recovery_activation` issues an engine/process-bound permit for
`CellGroupRuntime::activate_recovery`. The owner requires a fresh-term quorum
barrier and admits one bounded activation proposal. Only committed, persisted
and applied activation opens application admission. An applied acknowledgement
allows `complete_group_recovery` to confirm the activation receipt and release
the topology lock. Repeated attempts preserve the first activation position.
Rare catalog operations own boxed futures to bound composed control polling
frames; they do not start per-cell tasks.

Recovery origin, seed and activation records use the internal marker-1 hard
cutover, with no reader for older intermediate shapes. These low-level contracts
do not complete the public SDK/fleet cutover; implementation and qualification
status remains in the [design progress](../../../../../docs/design/multi-raft/cell-groups/progress.md).

## Physical log identity cutover

The engine registry uses `GroupHistory` (cluster, namespace, group and incarnation),
independent of application cell names. Marker-1 mappings use an explicit
`cellar-group-mapping-1` domain and a separate physical history. Old bare `scope`
mapping records refuse during recovery; there is no compatibility reader.

The staged single-cell host still stores its real cell binding as application
adapter metadata. Its lookup, birth retries, pending slot accounting and physical
inventory must reject another cell binding for the same physical history. This
metadata is not part of the registry key and is removed with the remaining
single-cell runtime consumers. Cell-group genesis and claims use the separate
physical scope described below.

Cell-independent engine handles now use `GroupLog<GroupHistory>` and
`ReplicaIdentity<GroupHistory>`. `create_cell_group` atomically syncs the physical
mapping, initial membership, Raft genesis and an empty route-bound group manifest.
`persist_cells` uses the same owned worker, quotas, batch sync and publication
path as the staged adapter. The engine rejects mixed application domains; old
single-cell enumeration and inventory cannot silently omit cell-group histories.
The mapping's required application tag also refuses the earlier untagged adapter
record shape. No old disk layout is decoded or migrated.

`RangeDirectory::bootstrap_group_member` admits an initial unmanaged member only
after checking its current birth, permanent node-storage pin, actual synced
genesis and an exact immutable member claim. Claims bind namespace, history,
birth, member, node, disk and engine instance. Lost confirmation returns no permit;
retry uses those same identities. A claim with missing local group state, or a
replacement engine behind the same node identity, refuses. Retired routes and
foreign nodes/catalog owners refuse. Managed namespaces remain closed until their
rooted fleet authorization is integrated.

The resulting permit is catalog startup evidence, not complete image recovery,
quorum, or application-serving authority. A future shared runtime must verify the
selected manifest closure, revalidate the permit and obtain current-term quorum
before accepting application work. The production host is not yet converted.
Native storage recovery fixtures exercise both image representations through real
cell-independent genesis without a placeholder cell.

`ReplicaLog::cell_group_inventory` reads every physical group under the engine
catalog lock and short cache reads. It includes retained histories regardless of
current membership, plus an optional initializing history. Pending identities
already in the catalog count once. The existing bounded node-inventory wire carries
these rows without allocating cell names. A domain-separated fixed-width digest
binds cluster, namespace, group and incarnation; `group_history` performs exact
lookup. Legacy cell-scoped lookup cannot match that digest. The node observation
must still match the complete occupied count before encoding or accepting a
response. Inventory is an occupancy observation, not quorum or release proof.
The generic physical capture includes shared histories, while the group-native
accessor refuses a legacy application engine.


## Invocation-bounded route confirmation

`RangeDirectory::routing_session` captures an explicit execution domain, original
monotonic deadline and 1–16 total attempts before discovery. The immutable request
binds logical cell history, command/enrollment/query identity and the dispatcher's
canonical request fingerprint. Initial discovery, redirects and same-group retries
share this budget. A previously resolved route can begin an invocation without
object-store I/O, even during a catalog outage; it is a potentially stale hint.

A relocation hint is checked against the namespace's configured trusted catalog.
The session observes a pinned directory root and requires an exact lookup match
for the requested cell. It refuses foreign namespaces/schemas, wrong ranges,
non-advancing epochs, unpublished histories and stale attempt tickets. This
confirms the mapping through catalog trust; it does not authenticate a network
connection. The production transport must still authenticate peers, and every
destination must verify the complete route, logical cell incarnation and current
serving authority. No redirect URL or caller-supplied catalog is followed.

An originating public command may use its local group's observed leader as a
destination hint. An ingress process with a `QueryRouter` placement can probe
its configured trusted group peers for the currently ready leader. These
read-only probes use the namespace command grant, authenticate the exact peer,
and check the selected group, route epoch, birth and cell range. Discovery shares
the original deadline and keeps time for the command. It owns no SQL producer
and marks no mutation submitted.

The same controller invocation retains its admission, command identity, captured
inputs and original deadline when it sends the mutation to one peer. The
destination independently checks its current route and write authority. Voter
membership grants no write permission. An incoming command always executes locally.

SDK queries use the local replica if present. On an ingress process with a
trusted group route, automatic selection tries configured members within the
original attempt limit and deadline. All attempts preserve the requested read
consistency and version floor. Callers can choose explicit local execution or
supply member preferences.

Managed embedders can call `RaftCells::grant_query_origin` and
`RaftCells::grant_command_origin` after discovery has accepted the peer's actual
boot and endpoints. The host verifies that it owns the namespace. The transport
checks the current peer view and monotonic permission revision. Read and write
grants are independent. `revoke_command_origin` retains a revision tombstone and
seals the old grant while already admitted work remains owned. Reapply these
trusted host permissions when restarting the transport.
The lower-level controller `command` API retains local-only execution; SDK
`dispatch_command` owns the originating routed invocation.

`CellCommandResponse::result`, `observation`, `token` and `durability` describe
either a local result or authenticated remote output. `local_receipt` returns an
inspected SQL receipt only for the local variant. Remote results retain transport
admission and cannot authorize local effects, local archive settlement or another
forwarding hop. Any possible outbound transmission makes an error ambiguous;
the owner does not automatically retry it or create a new command identity.

SDK enrollment uses the same leader selection and command grant. One controller
invocation captures the creation ID, initializer inputs and preview before
discovery, then admits one local or remote creation. A destination rechecks
its route and leader authority before entering SQL. `CellEnrollmentResponse`
returns the exact committed creation record and achieved durability.
`local_enrollment` exposes the locally inspected result only when this node
owns that result. An S3 request still requires complete archive coverage at
the destination.

Attempts are consumed before awaiting reads. Cancellation cannot refund a budget
or make a delayed ticket current. Read timeouts use the original domain clock,
even when a waiter is polled elsewhere. Marking a command potentially submitted
makes subsequent routing failures carry its original ambiguous command ID; a
redirect never proves the earlier proposal uncommitted. Physical command producers
remain the dispatcher's responsibility and must outlive cancelled waiters.

The directory supplies routing observations, not serving authority. The shared
dispatcher and controller combine those observations with actual quorum, SQL and
transport ownership. The public host cutover remains unfinished.


## Dynamic controller registration

`CellGroupController::register` adds a prepared group to a running node, including
one whose initial roster is empty. The caller supplies a bootstrap permit,
verified selected root, the applier owning that root's manifest page store, and
an authenticated physical peer route. Pending, active and quarantined groups share the group
limit; attachment and unconsumed replies also use the shared item/byte budget.
Admission failure returns the prepared owners. After admission, dropping the
response does not cancel attachment.

The worker registers the applier before polling runtime attachment, revalidates
the catalog permit through the existing runtime, and activates roster membership
only after success. Current-term quorum is still required for serving. Physical
route registration refuses adopting an existing route; cleanup checks exact route
identity. Failed attachment releases its group slot before publishing a refusal.
Shutdown joins attachment before closing transport, SQL and consensus ownership.

Group-local apply/runtime failure or an unresolved command failure quarantines
its group. `group_failure` preserves the original cause and `usage` reports active
and quarantined groups separately. A reserved runtime control request revokes
leader authority even while apply is waiting. New group work refuses; pending
apply and ambiguous command owners remain charged through physical shutdown.
Quarantine cannot be cleared by registration. Recovery requires joined restart;
a late persistence completion cannot restore the revoked serving fence.

Concurrent apply can invalidate a reconciliation observation before SQL inspects
it. This returns `ExecutionError::Uncertain` with the original producer retained.
The owned controller obtains a fresh observation and retries receipt inspection
without executing the callback again. Identity validation precedes this check.

After receipt reconciliation retires SQL, S3 output fencing is a separate owned
phase. It buffers the exact committed receipt and verified archive proof under
their existing admission. Later group apply can temporarily refuse current
serving authority; the controller waits for progress within the original deadline
and rechecks route, leader and catalog binding. It neither restarts receipt
reconciliation nor executes another callback. A duplicate receipt uses the same
output phase. Cancellation keeps the admitted owner; terminal errors release
output resources without quarantining a group whose SQL producer already closed.

Shared log-engine I/O/corruption/closure and lost shared SQL worker replies still
close the node. Unknown events and controller ownership violations fail closed.
Initial-member provisioning and canonical startup use the same physical page
inventory under the configured node directory:
`groups/<cluster>/<namespace>/<group>/<incarnation>/pages`. Their common path
belongs to `ManifestPageStore`; both take its exclusive lock and recover the
engine-selected root. Independent archive replay uses its own node base with the
same layout. A restart must refuse a missing selected page rather than open a
second empty inventory. Provisioning fault/restart fixtures target this canonical
path for both image backends. No previous flat-path reader or migration is added.

Provisioning binds immutable `CellGroupProvisionNamespace` profiles to exact
`RangeDirectory` owners and the declared image representation. Canonical startup
clones its node image inventories into these profiles. Duplicate namespaces,
foreign directory owners and mismatched backends refuse; a request cannot select
another image store. Profile caches and fixed peer metadata count once against
the configured preparation-byte total. Every namespace then shares the remaining
preparation bank and parallel worker limit. `provision_configuration_bytes`
reports retained configuration separately from active preparations.

`activate_managed_initial_group` uses the rooted group birth and actual initial
voter identity. It refreshes catalog authorization and checks the exact synced
boot before submission and again after local attachment. The controller retains
preparation, selected pages, roster and transport ownership when a waiter leaves;
retries reuse prepared owners. Entered physical work is joined even after its
invocation expires. An attached retry revalidates under the same shared budget
without reopening pages or attaching consensus again; it does not adopt another
registration's physical lifecycle. Shared-engine uncertainty still closes the
node. An attachment receipt describes the persisted fleet guard; it supplies no
fresh quorum, seed, serving or physical-release authority. Quorum readiness is
checked independently by the existing Raft owner.

Managed group repair controls take both the rooted physical history and its
exact admitted permit. They inspect a fresh quorum, preserve the complete plan,
consume/advance only the matching permit and revision, and observe durable apply
behind a new quorum before reporting success. Beginning placement checks the
installed trust revision, original target boot and physical-host separation over
all current occupants, including members omitted from advisory routing. Promotion
still uses actual RawNode catch-up; seed receipts and route discovery cannot
substitute for progress. Lost configuration replies remain unconfirmed.

After committed configuration changes, managed routing can resolve newly joined
member/disk bindings through rooted node views. Missing discovery leaves known
voters usable; it grants no initialization, vote, quorum or release authority.
Ordinary cell writes do not scan routes. Ordinary frames retry discovery once
only for an unknown destination; checkpoint sends also reconcile before capture.
Advisory refresh preserves the physical registration and checkpoint factory,
while each transfer pins an exact route generation. Old stream generations still
refuse. Per-chunk validation allocates no new endpoint. Explicit replacement or
removal revokes the old factory; an old registration cannot remove its successor.
All work retains the existing peer queues, admission and physical join owners.

Managed learner activation and full managed canonical startup remain unfinished.
See the delivery package for qualification status.


### Retained entry admission

`ReplicaLogOptions.cell_group_retention` bounds node/group logical retained log
entries plus owned reservations. Its progress sublimits reserve room for empty
Raft election and automatic joint-leave entries; ordinary payloads cannot consume
that space. The charge includes 512 bytes of fixed bookkeeping per entry. These
are logical entry and admission ceilings, not physical engine file byte limits.
`log_retention_usage` reports debt and reservations without granting admission.

New SQL commands reserve their bounded encoded envelope after duplicate lookup
and before application callbacks. Enrollment reserves before initialization.
Prepared commands retain that lease through submission and shrink it to actual
encoded bytes. Raft owners reserve inbound appends and local proposals before
mutation, including prospective election and automatic configuration entries.
Existing completion and read-index work does not consume ordinary entry admission.
Sealed pre-append rejection or authoritative receipt reconciliation retires an
unused preparation independently of client clones.

Entered storage retains exact engine/group/entry-bound leases after waiter loss.
After sync and cache/root publication, one ledger operation transfers them into
actual selected entry debt without a release gap or double charge. Suffix
replacement and selected checkpoints update debt from actual retained entries;
archive observations cannot refund it. Reopen reconstructs debt from the selected
log, retaining entries even under a newly lowered admission limit. Mutations
refuse until a reservation fits. No serialized field or per-cell worker is added.

The source-scoped archive/reclamation and local disaster-recovery qualification
is recorded in the [implementation progress](../../../../../docs/design/multi-raft/cell-groups/progress.md).
Sustained-load fairness and final SDK/fleet cutover have separate task gates.


## Per-cell effects on shared owners

`CellGroupController::configure_effects` installs bounded compiled namespace/class
registrations once. Each `CellGroupEffectRegistration` supplies explicit
`RaftEffectOptions` and a synchronous SQL-options factory, executed on the owning
domain's blocking service. The factory derives private paths from the complete
cell history and shares bounded node resources; its captured configuration must
not grow with enrolled cells. Dispatcher, SQL, engine and namespace schema owners
must match. Configuration enforces private-input and metadata capacity, and each
selected SQL execution verifies class-specific image, receipt and retry bounds.

One owned turn per node rotates ready groups and due cells. Its group queue is
initialized from the selected in-memory markers and updated only after synced
publication. Empty and future-only groups require no runtime probe or SQLite open.
Outbox summaries retain the minimum time among lane fronts; a blocked later
sequence with an earlier timestamp cannot keep a sleeping lane busy. Hints remain
advisory: the actual ready leader issues cell execution authority and a fresh
linearizable image supplies each private client sequence and due generation.

Discovery runs on the existing SQL workers. A completed scan transfers its
payload into the effect metadata permit and releases SQL read admission before
staging a command, including when the node/group has one SQL slot. Alarm discovery
retains only its generation ID; its bounded payload stays in SQLite until the
private callback loads it. The private operation and reserved logical cell client
are checked before both new execution and duplicate lookup. Public and remote
commands cannot select the private execution path. The last committed private
receipt alternates alarm/outbox priority without a preference table for every cell.

An outbox claim crosses ordinary quorum and requested S3 output settlement before
send. After consuming the claim result, the turn releases its response admission
before a loopback receiver or source completion needs the same bounded lane.
Published durability survives that consumer release. The durable Dispatched row,
bounded request metadata and fenced image custody retain the unknown outcome.
Delivery custody requires the same active route, authorized member and leader
term, and a current same-history cell version covering the committed claim.
Later business writes and unrelated apply lag do not revoke that custody. A term,
route, activation or lineage change does. The source rechecks custody before send
and reply acceptance, verifies the configured receiver signature, then replicates
conditional generation-matched completion through the normal durability gate.

Cancellation does not retire admitted command producers. Closing stops new effect
admission, joins factories/scans/delivery waiters, then closes the existing physical
SQL/runtime owners. Pending callback work must finish before close succeeds;
cancelled external delivery preserves committed debt for retry. Controller usage
reports active turns, completed turns and retries; SQL usage separately reports
retained producers and operations. Root publication can precede those resources'
disposal, so inspection through a one-slot pool waits for their ownership to drain.
Settled materialized producers discard the private SQLite/WAL and LTX scratch
under their existing lock, retaining only its lock inode. Failed opens and revoked
post-open authority also discard scratch before releasing that lock. Cleanup is
bounded and its failure is reported. Sealed committed images, receipts and pending
proposal bytes remain independently owned. Extent workspaces retain their existing
bounded store ownership and joined release.

The [native effect cases](../../../tests/native_cell_groups_effects.rs) exercise
due discovery among cold cells, exact committed-base reopening after disposable
state is removed, retained SQL/delivery ownership, and joined shutdown. Two
independent physical group bindings compare the exact signed request and receiver
proof while each loses a reply and suppresses duplicate receiver execution. Live
transfer sealing/publication belongs to CG-09. The S3 case requires claim coverage
before send, retains the receiver's S3 policy, and independently restores the final
complete image/receipt closure. It has an explicit local RustFS variant without a
memory fallback. Current results and remaining qualification are recorded in the
[implementation progress](../../../../../docs/design/multi-raft/cell-groups/progress.md).

Managed peer learner activation enters the same retained provisioning manager as
initial group startup. An exact original-boot permit selects the joining member;
the authenticated seed exchange independently checks that same permit and obtains
a fresh source quorum grant. The manager saves the recovered page/applier owner
before starting installation and retains it after an unknown result. A durable
seed receipt resumes through the permanent joined claim and selected physical
closure; a staged mapping cannot bootstrap genesis. The parallel preparation
permit spans recovery and installation. Attachment remains separate from learner
promotion and application serving.

The SDK and movement receiver now use this whole-group peer path. S3 learner
activation currently refuses before submission: exact-reference archive transfer
has not yet been integrated. These new managed paths have no executed native or
RustFS qualification while the ordinary library still has legacy consumers to
remove.

The canonical group host now joins physical cleanup before an explicit startup
error returns and withholds physical routes until all local group roots have
attached. Its cancellation guard retains the same cleanup on the execution
domain. The two existing startup regressions now use real shared groups, engines
and TLS endpoints; their original lock-release and incomplete-inventory traffic
oracles remain. Their execution is not yet qualified.

Canonical archive startup creates and syncs its node-owned archive spool directory
on the execution domain before registering an archive job. Materialized checkpoint
receivers open anonymous files there under the existing shared scratch admission;
they do not create a missing caller-supplied parent. Missing image data and failed
scratch preparation still refuse S3 admission rather than changing durability.

A provisioning retry may reuse its retained, completely verified selected root
without scanning or syncing it again. Reuse checks the same live engine and page
owners, exact selected marker, settled publication and complete checkpoint pins.
Pending or quarantined retention refuses; marker equality cannot resolve an
uncertain write. Changed durable selection recovers on the same page owner.

A peer seed requires a live source grant through the engine's pre-fence admission
check. After actual engine persistence succeeds, local completion checks the
receiving engine/member/disk/process, managed boot, transport shutdown, original
deadline and fresh catalog bootstrap. A later source restart does not revoke that
completed installation. Neither its receipt nor bootstrap establishes fresh
quorum or serving authority; an expired completion is refused even when cold
recovery later finds the installed root.

Shared linearizable cell reads may be selected on followers after their own safe
ReadIndex round. Their private forwarded context binds the authenticated origin
member, process-local nonce and sequence, separately from membership metadata and
leader-local seed/execution contexts. Selection waits for complete local apply
through the correlated barrier. SQL rechecks the selected history, live member,
term and source generation; a later term/leader change or shutdown revokes the
retained proof. This changes read availability, not leader-only mutation or seed
admission. SQL results describe the actual selecting replica.

Recent-source reads use an explicit age bound for the observed current source.
A stale-term message or catalog refresh cannot renew that observation. The selected
proof retains its original source expiry through queued and entered SQL work;
fresh later traffic cannot extend it. SDK stale reads use the injected host source
age by default; disconnected historical reads require explicit selection. Neither
policy supplies a quorum proof. The default request deadline and physical query
retention still apply. Public replica preferences use the same shared controller;
counter/performance entrypoint conversion remains unfinished CG-07 work.

Public replica preferences now enter the same shared controller invocation as a
local query. One to eight distinct candidates have a bounded per-attempt timeout;
all attempts share the original request, route fingerprint, global deadline and
attempt bank. A final fallback may address one untried observed leader. Discovery
and the leader field inside an authenticated query reply remain advisory; a
receiving node independently checks the complete cell/group route and namespace
process grant. Incoming RPCs always execute locally.

An expired physical query retains both dispatcher and controller admission until
its SQL callback exits. Outbound work retains dispatcher admission through the
physical transport exchange; successful remote results retain their node/peer
reservations and exact authenticated replica identity. A later attempt cannot
renew an earlier read proof or create a private replacement admission bank.
Both image forms pass the focused routing cases and the complete execution
suite; repeated deadline cases retain the original capacities and timeouts.
Consumer ports and the combined public/managed/backend gates remain unfinished.
