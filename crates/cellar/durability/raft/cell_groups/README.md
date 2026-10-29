<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Cell-group durable state

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
geometry, last group mutation position, schema and effect summary. Local image
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

## Enrollment admission (CG-03 in progress)

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
Its bounded canonical marker-1 codec verifies data shape, not authority. CG-04
must publish this record atomically with enrollment, retain it through mutations,
and include it in complete snapshots/archives. An exact retry must not initialize
again or roll the current image back to the initial one. Current-term quorum and
read authority remain required from the host even for a recognized retry.

## Range directory (CG-03 in progress)

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
ownership. Group birth/permanent disk claims, managed admission and bounded client
redirects remain to be integrated. Native directory tests exercise real conditional
object-store operations in memory, including races, dropped replies, corruption,
restart and bounded split/merge churn. They do not qualify live Raft ownership,
real RustFS or the full CG-03 gate.

## Cell-independent group birth (CG-03 in progress)

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

These methods expose no voter-start capability and write no permanent member disk
claim. The physical log still needs cell-independent genesis and recovery identity
before the new birth can be integrated with actual disk-claim admission. Managed
namespaces refuse these unmanaged APIs until their rooted fleet authorization is
connected. This staged boundary does not permit blank-disk recovery, replace
current quorum membership, or complete CG-03 qualification.
