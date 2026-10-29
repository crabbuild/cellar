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

Paged range-directory publication, birth reconciliation and host integration are
still pending. Pure admission tests do not qualify those boundaries.
