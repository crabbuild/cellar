<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Cell-group durable state

This component is under CG-02 implementation. It supplies immutable manifest pages, their filesystem owner, complete-root
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
and reuse verifies and syncs bytes again. Pages are never deleted in this initial
slice, so old roots remain readable. Authoritative root retention, pin-aware
reclamation and recovery of partial orphan writes remain CG-02 work.

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

Further integration must add authoritative physical root retention/reclamation,
including page reclamation and uncertain publication reconciliation. Reader, apply,
snapshot and transfer pins must protect dependencies before deletion. Native
engine crash cuts and both physical image modes remain required before CG-02
can be marked DONE.
