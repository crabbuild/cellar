<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Incremental image commitment contract

This is an implementation building block for
[large-cell recovery](large-cell-recovery.md), exposed through the unstable LTX
verification API. It is not connected to native command execution, generation
publication, archive storage or demand loading yet. Existing full-image digest
checks remain unchanged. Passing these tests does not qualify fast recovery.

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
dirty-page list. Initial observation reads the image with 64 KiB buffers.
Subsequent observations reuse immutable subtrees only for the same live database
owner, history/schema/epoch and page geometry, after matching the complete
previous committed WAL prefix with SHA-256. WAL resets or replaced prefixes
force full observation. Reopening or rebuilding a writer invalidates its token.
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

Observation is neither destination sealing nor publication authority. A future
native generation owner must provide an authenticated initial base for demand
reads without rescanning a new cold writer. Its checkpoint protocol must preserve
incremental continuity; the conservative reset fallback above still scans the
image. Resident extent admission/eviction, authenticated object-store extent
placement and demand reads remain required. Execution, envelopes, generation markers, snapshots and
archives must switch together. The current anonymous file owner is disposable
data material, not a persistent cell generation or admission proof. A Merkle root
must never be relabeled as the existing flat SHA-256 to avoid that coordinated
change. None of this prototype is connected to native recovery yet.
