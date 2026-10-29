<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Cell-group durable state

This component is under CG-02 implementation. It currently supplies immutable
manifest pages and their filesystem owner. It is not yet connected to a Raft
engine apply marker and does not admit requests or establish durability proofs.

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

Next integration must sync complete cell images and manifest paths before one
Raft Engine transaction selects the complete group root and contiguous applied
position. Recovery must refuse any missing page or image. Reader, apply,
snapshot and transfer pins must protect dependencies before deletion. Native
engine crash cuts and both physical image modes remain required before CG-02
can be marked DONE.
