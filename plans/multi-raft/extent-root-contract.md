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

The next boundary is owned immutable extent files and a verified reader, followed
by an independent committed SQLite/WAL observer. Execution, envelopes, generation
markers, snapshots and archives must then switch together. A Merkle root must
never be relabeled as the existing flat SHA-256 to avoid that coordinated change.
