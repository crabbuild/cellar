<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Incremental image commitment contract

This is an implementation building block for
[large-cell recovery](large-cell-recovery.md), exposed through the unstable LTX
verification API. It provides verified demand reads and disposable writable
SQLite extent views. A native verification adapter also binds sealed image roots
to exact runtime tickets and durable engine markers. Public SDK command dispatch
and archive/peer recovery do not select this backend yet. Existing materialized
image digest checks remain unchanged.
Passing these component tests does not qualify fast native recovery.

## Identities and bounds

`ExtentId` commits to one nonempty extent of at most 4 MiB, including its exact
length. `ImageRoot` is a separate type and hash domain from a flat image SHA-256.
The image root binds its history and schema digests, LTX epoch/TXID/checksum,
SQLite page size, logical length and ordered extent tree. The authenticated Raft
record must still bind its own exact committed position and scope; these values
grant neither membership nor ownership.

The persistent tree has twelve binary levels. Updates share unchanged subtrees
with their immutable parent and hash only paths to changed leaves. Growth must
supply every new extent; a changed partial boundary must supply a new exact-length
identity. Truncation removes the unreachable suffix. Changing nonempty SQLite
page geometry requires constructing a new image. Transition authorization and
monotonic position checks remain the generation/apply owner's responsibility.

At the page-rounded decimal quota, 9,999,998,976 bytes occupy 2,385 extents. The
canonical manifest is 76,432 bytes. The bound is independent of the image's
compressibility and has no per-SQLite-page heap entry. Encoding/decoding visits
the bounded extent index; it does not read image bytes. Actual extent readers
must separately verify the requested length and `ExtentId` before exposing data.

## Canonical format 1

All integers are big-endian. The header is 112 bytes:

| Offset | Field | Bytes |
| --- | --- | ---: |
| 0 | `CLEXT001` | 8 |
| 8 | History digest | 32 |
| 40 | Schema digest | 32 |
| 72 | LTX epoch | 8 |
| 80 | LTX TXID | 8 |
| 88 | LTX checksum | 8 |
| 96 | SQLite page size | 4 |
| 100 | Logical length | 8 |
| 108 | Extent count | 4 |
| 112 | Ordered extent digests | 32 each |

The count must equal the ceiling of length / 4 MiB; extent lengths follow from
geometry. Extra bytes, missing entries, invalid geometry and oversized input
fail before tree construction. Decoding requires the expected authoritative
`ImageRoot` and compares the reconstructed root. No legacy reader is provided.

Every hash starts with `cellar.extent-image.v1\0`, a four-byte tag length and tag,
then each field preceded by its eight-byte length. The tags distinguish extent
bytes, leaves, branches, empty leaves and image roots. Branches include their
height. Empty subtrees use the same branch construction over canonical empty
children. Root fields use the header's binding and geometry, followed by the
tree hash; the magic and redundant extent count are verified by the codec.

## Evidence and next integration boundary

Eight ordinary integration tests cover an independently calculated Python
SHA-256 vector, every-byte corruption, truncation/extra input, forged counts,
extent-length mismatch, empty images, all SQLite page sizes, growth/shrink,
parent immutability, exact quota geometry and an update above the 4 GiB offset.
The length-mismatch regression failed before `ExtentId` retained its length and
passes with the corrected typed identity. Rust 1.98.1 Clippy passes for this
target. Run `cargo test --locked -p cellar-ltx --test extent_tree` to reproduce.

## Private verified extent files

`ExtentDownload` reserves its complete expected length and one file against the
shared node scratch pool before creating an anonymous file on the supplied
filesystem/volume. Ordered appends accept at most 64 KiB. Invalid or uncertain
appends poison the owner; they cannot be retried over potentially written bytes.
An incomplete owner exposes no reader.

Finishing requires the exact incoming length and content identity, a successful
file sync, then an independent destination length check and streamed hash. Only
then does it return an immutable `ExtentFile`. Its reads also use at most 64 KiB.
During independent destination verification it derives up to 64 block digests
(2 KiB per file), retaining them only after the complete extent identity matches.
Every later read verifies its containing blocks before returning any bytes;
unaligned requests need at most two 64 KiB reads, rather than an extent rescan.
These local read proofs are not a new wire format or publication capability.
The anonymous handle closes before its disk/file reservation is refunded; clones
share both owners until the last clone drops. Hash framing matches the typed
`ExtentId` domain, including exact length. There is no whole-extent allocation in
the receiver, file verifier or reader.

Seven file tests cover 1-byte through 4 MiB extents, 64 KiB boundaries, read and
append bounds, shared admission before I/O, clone/unfinished-owner lifetimes,
truncated/extra/incorrect content, poisoned partial writes, creation failure,
sync refusal, corrupted physical writes and short destination reads. These pass
alongside the eight root tests and seven ordinary streamed-snapshot regressions;
the physical 10 GB codec fixture remains an explicitly ignored qualification.
These APIs are still exposed only through the unstable verification surface.
Rust 1.98.1 Clippy passes for all three owning test targets with warnings denied.
Formatting and module layout cover 925 Rust files; native dependency and local
documentation checks also pass. Run `cargo test --locked -p cellar-ltx --test
extent_file --test extent_tree --test stream_snapshot` for this ordinary scope.

## Independent committed-image observations

`internal::db::observe_extents` constructs a manifest from the actual committed
main-file/WAL overlay. It consumes no proposed LTX bytes or caller-provided
dirty-page list. Ordinary VFS providers initially read the image with 64 KiB
buffers. Their subsequent observations reuse immutable subtrees only for the same
live database owner, history/schema/epoch and page geometry, after matching the
complete previous committed WAL prefix with SHA-256; WAL resets force full
observation. The extent workspace instead supplies its sealed initial root and
actual main-file mutation sequence through the live SQLite file handle. Across
a reset, it rereads prior/current WAL extents and actual main-file changes,
retaining unchanged subtrees. Reopening invalidates the prior database token; a
new extent workspace must start from its own independently sealed base.
New/changed partial extent boundaries and truncated suffixes remain explicit.

The live SQLite WAL index fixes the committed frame count, database geometry,
salts and last-frame checksum. Both native-endian header copies and their
checksums must match; unsupported formats fail. A short, nonwaiting VFS write
lock protects the 96-byte metadata copy from concurrent header mutation, and is
released before decoding or image I/O. Active writer transactions refuse this
sample. Callers must still keep all writers and checkpoints quiescent throughout
observation. The implementation reads no private SQLite struct through a Rust
layout cast and does not checkpoint or extend the shared region.

This closes a reproduced corruption gap: a damaged/truncated latest committed
WAL frame must not silently select an older transaction with the same logical
length. The regression fails before the committed-boundary check and passes
afterward. Rolled-back spilled frames beyond that boundary remain excluded.
Every committed WAL frame is checksum verified. The WAL byte budget still bounds
the per-page overlay index; it is not a per-page index for the complete image.
Prefix verification currently reads the complete bounded WAL on each call.

Five real SQLite tests compare roots and canonical manifests with SQLite's own
serialized pager view across 512/4,096/65,536-byte pages, small updates, unchanged
observations, growth/shrink, capture/checkpoint/reset, rollback, different/reopened
owners, invalid history/bounds and committed corruption. Small counter updates
rehash one 4 MiB extent; unchanged observations hash zero image bytes. These are
small ordinary fixtures, not native 10 GB first-write measurements. Three WAL
index tests exercise both copies, corruption of every byte, unsupported layouts,
page geometry, salts, byte order and final-frame checksum.

Observation is neither destination sealing nor publication authority. Physical
image preparation independently applies bounded LTX files into new extents,
verifies and syncs their local closure, validates SQLite geometry and canonical
lock-page bytes, and compares the exact result root. Native publication additionally
requires the exact engine-selected marker and runtime ticket. Public SDK writer
selection, checkpoint capture, aggregate private-file disk admission, authenticated
extent placement and remote installation still need integration. A Merkle root
must never be relabeled as a flat SHA-256.

## Disposable extent working images

`ExtentWorkspace` exclusively creates one disposable sparse main file and wraps
the injected SQLite VFS. An `Arc<SealedImageRoot>` retains its exact initial
source. Reads and partial writes fault only the necessary 4 MiB extents, through
the separately admitted verified cache. WAL, SHM, file locking and checkpoints
use the base VFS. Working main files are not recovery authorities. The caller
must admit private disk capacity, use this VFS for every connection, exclude
external writes and remove the main file, sidecars and capture metadata after
all connections close.

One fixed slot per possible extent records hydration and actual main-file
mutation sequence: 2,385 slots at the decimal 10 GB quota, independent of SQLite
page size. There is no per-page hydration set on this path. The tracker serializes
faults with checkpoint writes; it never retargets a live main file to a newer
logical WAL head. Truncation permanently bounds access to the old source, so
regrowth observes zeros rather than resurrecting removed bytes. Hydration hashes
the authenticated source prefix and independently reads the disposable destination
before exposing that extent to SQLite. Main-file and WAL writes also check their
destination bytes in at most 64 KiB reads; truncation verifies the resulting size.
Failed destination mutation or verification poisons the workspace, including failed size reads:
a caller cannot freeze or reuse an uncertain working image. Advisory sizing cannot
bypass tracked writes or the image quota.

A canonical sealed empty root can bootstrap SQLite without materializing an image.
Existing WAL, SHM or journal sidecars refuse admission before main-file creation.
Physical WAL writes and truncations have a separate configurable bound, at most
512 MiB, checked inside the VFS before I/O. Only the exact workspace WAL receives
that bound; foreign files opened through the registration retain ordinary behavior.

The private execution owner now has materialized and extent image adapters beneath
the same command validation, SQL policy, receipt reconciliation, uncertainty and
authority gates. Its extent writer captures committed WAL updates, then prepares
an independently verified immutable result before Raft publication. It requests
rebuilding at a 256 MiB WAL high-water mark, retaining the 512 MiB physical guard,
and disables close-time checkpoints on its disposable connections. Closing such a
writer must not backfill the whole working image before discarding it. The public
node dispatcher does not select this adapter yet; public-node admission wiring,
global extent references and remote placement remain required integration work.

The extent adapter now acquires a `NodeWorkspace` from its shared node extent
inventory before opening SQLite. Canonical `w-<id>` directories live inside that
inventory's exclusive lock. Their complete main/WAL/capture file-byte ceiling and
control-file headroom share the immutable extent/root byte budget. These are
conservative file-byte reservations, not measurements of filesystem blocks or a
kernel quota. The adapter validates the reservation's owner and exact limits;
its VFS enforces main/WAL limits before writes, and capture enforces an aggregate
encoded-byte ceiling before creating its output files.

Every VFS file retains the directory reservation. The last pin closes creation
and SQLite handles before bounded deletion, directory sync and refund. Partial
cleanup, uncertain creation or a failed deletion sync retains the whole charge
and quarantines further node admission. Restart first inventories all immutable
names, then discards bounded canonical private directories under the exclusive
lock. It never mutates a directory during its inventory enumeration, follows a
private-directory symlink, or restores a cell from disposable SQL bytes. This
admission is wired into the private extent executor; the public dispatcher and
materialized private writer still require integration with this policy.

The direct filesystem lock guard explicitly unlocks when its creating process
drops the final owner, then closes the file. Closing a descriptor alone can leave
the lock held by a forked child's inherited descriptor before exec/exit. A Unix
regression holds such a child live and checks immediate reacquisition; the
previous guard fails that case. The guard checks the current OS process ID at
this filesystem boundary so an inherited guard cannot unlock a live parent's
lock. This physical lock behavior supplies no Raft ownership or output proof.

The observation API obtains this capability from SQLite's actual main file,
not from a supplied VFS name or dirty-page list. It checks main sequence and the
WAL cut again after reading. Root histories, schema, epoch and page geometry
must match. On a discontinuous WAL, prior WAL extents are included even when
main-file writes were absent, so disappearing frames cannot leave an unexamined
old logical root. Ordinary VFS providers retain their conservative full fallback.

`extent_workspace` exercises actual 512/4,096/65,536-byte SQLite pages, 8/40 MiB
payloads, one-extent hot opens, partial physical writes, quota refusal,
truncate/regrow, retained reader snapshots and registered-file lifetime. Its
checkpoint fixture keeps at least five extents: opening auto-vacuum control
metadata can touch three extents, so a smaller image may have no unchanged subtree.
Oracle comparisons use SQLite's serialized pager only in these small tests.
These cases do not establish a 10 GB SDK first-write or recovery-time result.

## Verified demand reads through SQLite

`internal::ExtentImage` combines one immutable authenticated manifest with a
blocking extent source and a bounded, replaceable cache. The source retains the
selected root's dependency closure. It receives an already admitted
`ExtentDownload`, so source code cannot omit shared scratch reservation,
independent destination verification or sync. All append/read/hash buffers stay
within 64 KiB. Only a complete verified extent becomes a cached reader.

Cache byte/file limits are separate from the shared pool's physical bounds.
Eviction drops physical owners outside the cache mutex before trying to spend
their capacity on a new miss. Concurrent reads/downloads retain their actual
file charges until the last owner closes; logical eviction cannot refund those
charges. No cache lock crosses source I/O, verification or sync. Duplicate
concurrent misses may temporarily consume separate admitted files; the pool
refuses excess work instead of exceeding its physical bound. Content identities
deduplicate repeated extents without changing their manifest positions.

The first extent must match SQLite's page geometry. A fetched extent containing
the reserved lock page must carry canonical zeros, even when the requested range
does not overlap that page. Missing, corrupt, incomplete or failed data produces
an error rather than zero-filled database pages. Eviction leaves the selected
root unchanged and a later miss fetches the same identities.

`internal::open_extent_image` uses the same bounded read-only SQLite VFS as
materialized generation pins. Both paths retain their physical owners through
the connection and all open files; the existing slot, filename, sidecar and
read-only gates remain in place. The new source must run on the owning SQL/
blocking domain. It is not a global asynchronous network adapter or a native
serving/admission decision.

Ten component tests use real SQLite files and a one-extent cache. Opening and
reading the same counter from 8 and 40 MiB of payload fetches just one 4 MiB
extent. Cross-extent reads, broad SQLite integrity checking, cache loss/refetch,
read-only enforcement and owner lifetimes work across 512/4,096/65,536-byte
pages. Faults cover missing, truncated, corrupt and partially failed source I/O,
independent destination corruption, source panics, quota refusal before source
I/O, and held/cancelled source work through joined destruction. Separate
synthetic cases cover lock-page geometry and content deduplication; they do not
claim a physical GiB-scale image or full-size recovery qualification.

Native root inventory selection, archive/peer placement and retention,
authenticated cold writer bases, incremental checkpoint continuity and native
installation gates still require integration. Anonymous cache files must never become the only
copy of committed state. This component does not close R7 by itself.

At `eb459388`, the owning scope passes all 60 ordinary LTX integration cases and the
49 native LTX/read regressions after sharing the VFS boundary. Three child/
qualification entries remain ignored by these invocations. Rust 1.98.1 LTX
all-target Clippy with warnings denied, formatting/layout (1,047 Rust files),
the 465-package native dependency graph and local documentation checks pass.

The broader native pass at the same revision adds 603 host library, 60 archive, 21 execution, 32
host, four ordinary large-image and 44 snapshot cases: **873 passing cases**
in the combined scope, with ten child/qualification entries ignored by their
parent invocations. Native/LTX all-target Clippy and native-only/no-Python
libraries also pass. The full-size production experiment is a separate frozen
materialized-runtime build; these ordinary results do not qualify native
extent-based installation or full-size recovery.

## Post-seal read integrity

An injected read fault exposed a gap after the initial extent seal: `chunk`
could return an altered byte while still carrying the original content identity.
The new regression fails before per-block verification and passes afterward,
including a fault in the second block after the first portion was valid. An
actual SQLite open over a previously verified cache entry also refuses a later
faulty read. Clearing a transient test fault permits a subsequent correct read;
the selected root and physical charge remain unchanged throughout.

The follow-up changes only the extent-file data path and test fault adapter.
The materialized native runtime remains the separately qualified baseline above.
All 62 current LTX integration cases pass (one full-size qualification entry
ignored), including eight extent-file and eleven extent-image cases. LTX
all-target Rust 1.98.1 Clippy passes with warnings denied. This follow-up does
not claim a rerun of the 813 native cases or a full-size native extent restore.

## Persistent node extent owner

`NodeExtentStore` owns one flat, exclusively locked directory and one budget
across all groups on a node. Its default independent limits are 64 GiB of stored
bytes, 16,384 data objects, 4,096 root files, 262,144 manifest extent slots and
16,384 reference keys. These are inventory admission bounds, not a change to the
10,000,000,000-byte per-cell logical quota. Every data import reserves its full
length before file creation or source I/O. Existing objects are verified rather
than overwritten. Source identity, independent destination verification, file
sync and directory sync precede reuse. All body I/O is at most 64 KiB.

Startup inventories only canonical names and actual file lengths. Known partial
files remain charged; unrecognized entries, symlinks and exceeded limits refuse
opening. No filename or newest-root rule selects serving authority. The caller
supplies an exact root identity and metadata length. Root metadata is bounded
and checked against that identity before constructing a view. Recovered data
objects start unverified: a metadata-only `RootView` can fetch and verify a hot
extent for actual SQLite queries, but cannot become a `SealedRoot` until every
object in its closure is independently verified and synced locally. The seal
still grants no Raft vote, native admission or output acknowledgement.

Within that inventory lifetime, full verification retains up to 64 SHA-256
block hashes per object. Descriptor caches share this metadata, so opening a
new query does not re-read entire extents that recovery already verified.
Each returned block is independently read and checked against its retained
hash, and the opened descriptor must have the exact extent length. The
`max_objects` admission bound also bounds this retained hash data to 2 KiB per
object (32 MiB by default), apart from proofs held by admitted readers and
bounded in-flight verification metadata.
Restart discards every such proof; filenames never reconstruct it. Full-body
archive reads and closure verification still check the complete extent digest.

View and SQLite lifetimes retain the node lock and their root pins. Closing a
cell does not refund named data. Retained closed-root metadata also participates
in shared-object reachability. The host reconstructs scope-derived monotonic
reference keys from authenticated native markers and supplies the complete root
set before enabling reclamation. Stale/equal revisions cannot resurrect released
keys. Reclamation deletes unpinned, unreferenced root metadata and syncs those
deletions before deleting unreachable data. A successful deletion barrier
precedes each accounting refund. Failed cleanup or uncertain creation retains
conservative charges and quarantines mutation; recovery inventories actual files
again. Existing exact-root reads remain available when their data is intact.

The owner serializes mutations without holding its state mutex across source or
filesystem I/O. Entered work retains its owner, capacity and exclusive lock until
it returns or the owning execution domain joins it. It is synchronous library
code; the native adapter must schedule it on its owning blocking domain.

Fifteen owning cases cover reopen without body scans, real SQLite demand reads,
metadata-only roots, independent local seals, capacity refusal before source I/O,
ambiguous creation, independent destination corruption, failed file/directory
sync, failed deletion, retained/shared roots, monotonic references and joined
work and source panic with conservative accounting/quarantine. A parent launches
an abrupt-exit child at root-file sync before directory
sync; reopening keeps the explicitly selected old root and reclaims the orphan
candidate only after reference reconciliation. This is one small component
process-exit cut, not native marker atomicity, power-loss or full-size recovery
qualification. Public SDK COW writer selection, archive integration and
policy-specific remote durability closure still need implementation.

The combined component scope passes 77 ordinary LTX cases (the final owner
invocation adds the panic case to the preceding all-integration pass), with two
child/full-size entries ignored by parent invocations. All 49 native LTX/read
regressions also pass after the filesystem-interface addition. LTX/native
all-target Rust 1.98.1 Clippy with warnings denied, formatting/layout (1,052 Rust
files), the native dependency graph and documentation checks pass. The remaining
764 native baseline cases above are prior-revision evidence, not a fresh rerun.
