<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Cell-group state and execution

CG-02's storage boundary is qualified. The component supplies immutable manifest pages, their filesystem owner, complete-root
preparation and atomic Raft Engine root publication. Production host admission
is not yet wired to these contracts; a stored root is not quorum authority.

A manifest is a persistent AVL tree ordered by the namespace routing digest and
full cell ID. Each bounded page holds one cell leaf and at most two child
references. References bind SHA-256, height and subtree count. Insertions and
updates copy bounded paths; rotations preserve balance. Unchanged cell images
are reused. The format uses `CLRCGM01`, the marker-1 page hash domain, canonical
JSON and a 16 KiB decode bound. Tree depth and cell count are explicit limits.
These limits are admission ceilings, not qualified fleet capacities.

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

Shared log-engine I/O/corruption/closure and lost shared SQL worker replies still
close the node. Unknown events and controller ownership violations fail closed.
Request-driven group provisioning, configuration lifecycle and public SDK
integration remain unfinished. See the delivery package for qualification status.
