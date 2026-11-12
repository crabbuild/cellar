<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Immutable manifest page packs

This candidate implements the persistence-amplification part of OPT-06. The
authenticated AVL tree and persistent root pins already make updates proportional
to the changed path. The remaining local amplification was one file barrier per
new AVL page. At native source `0c4e8c34`, the retained
`manifest-pack-negative-control` reproduces that cost: the existing 64-image,
changed-path storage test fails its new one-file-barrier assertion on the second
insertion. It builds with Rust 1.98.1, test opt-level 1, debug information disabled,
and assertions and overflow checks enabled. No request or resource limit changes.

## Publication

The [page store](../../../../crates/cellar/durability/raft/cell_groups/page_store/mod.rs)
streams one AVL update into one bounded immutable container. It retains at most
96 frame descriptors and streams each existing bounded page buffer. Reads during
the update use a separate injected file handle; the writer's cursor never moves.
The final footer precedes a single file sync, final rename and directory sync.
Only then may the root escape the update scope. Quota, I/O failure or unwind after
any physical change quarantines the owner and releases no physical quota.

The local marker-1 container begins with `CLRCMP01`. Each frame contains the
unchanged logical page digest, a big-endian u32 byte length and the unchanged page
bytes. A `CLRCMS01` footer binds record count, data end and a domain-separated
SHA-256 digest of ordered frame keys, offsets and lengths. Each container has
1–96 records, each at most 16 KiB. Strict lowercase digest filenames end in
`.pack`; partial writes use `.pending`.

Startup validates bounded framing, duplicate keys, exact lengths, filename and
index identity. Each logical use independently reads and authenticates its frame
and complete page payload. Completed sync observations belong to this open owner;
they never replace integrity checks. A changed-path read still performs one
bounded physical read per page. Reopened files must establish fresh barriers.

This is a local format hard cutover: old `.page` directories are refused, with no
legacy reader or migration. Use fresh matching local namespaces/binaries and the
existing authenticated archive recovery path. Replicated logical page bytes,
root digests and checkpoint/archive page streams do not change. An exclusively
locked opener may discard an incomplete `.pending` file because no selected
root can name one; it completes a directory barrier before returning.

## Physical reclamation

Usage counts all resident encoded record copies, full encoded file lengths and
container files, including obsolete and duplicate records. A pending write
conservatively includes its framing and reserved footer. These counters describe
encoded files, not filesystem allocation blocks or a new hard disk quota.

The [collector](../../../../crates/cellar/durability/raft/cell_groups/page_store/collection.rs)
requires the same reconciled authoritative root inventory as before. It deletes
wholly unpinned packs. For a partly live pack it admits a replacement alongside
the source within the unchanged page and byte caps, authenticates each live page,
and completes the new file and directory barriers before relocating its index.
Original logical leases survive relocation, including leases held by readers,
archive streams and snapshots. Only a subsequent source unlink and final shared
directory barrier release the source allocation. Failed copies and uncertain
deletions remain charged and quarantine the owner.

A scan counts physical containers, each with at most 96 resident descriptors.
The file deletion budget counts actual container unlinks. At most 96 live records
are read/relocated across one node-wide maintenance request; one page buffer and
bounded source/destination indices stay within its existing scratch reservation.
Whole orphan deletions share the final directory barrier. If a copy cannot fit,
the report records a blocked compaction, with no write, credit or quarantine;
other orphan files may still release headroom. No claim of completed physical
release follows from such a refusal.

An interrupted copy may leave two completed containers. Reopening charges both
and deterministically indexes each logical key. Reuse authenticates the actual
completed destination and establishes fresh barriers before deleting the source.
Cleanup completion checks container emptiness, rather than logical index size.

## Qualification scope

The existing changed-path, independent-image, old-reader, corruption, quota,
engine-publication and process-crash assertions remain required. Physical fault
locations use an independent fixture decoder. New owning storage tests cover
reader-preserving relocation, exact encoded-file accounting, refused copy
headroom, file/rename/directory/unlink failures, interrupted-copy duplicates,
pending cleanup and malformed framing. A successful focused check alone is not
OPT-04/06 acceptance.

The full native profile-1 storage/lifecycle matrix, matching real RustFS and
constrained Linux campaigns, original occupancy-128 gate, repeated measurements
and remaining held-owner/failure-domain/release gates still need current-source
evidence. The preceding occupancy-128 failure remains failed. No measured speedup,
full CG-13/CG-17 or OPT-01–08 acceptance is claimed.
