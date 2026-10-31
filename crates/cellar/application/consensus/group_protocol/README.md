<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Cell-group entry encoding

This module encodes the replacement group runtime's cell-addressed log entries.
The existing host is not switched over by the codec. Storage and runtime owners
must verify quorum, physical images, membership and transition proofs separately.

## Frame and bounds

Every integer is unsigned big-endian. The marker stays at 1; the new magic and
semantic domain refuse old per-cell entries in both directions.

| Field | Bytes |
| --- | ---: |
| `CLRCGE01` | 8 |
| Marker, exactly 1 | 4 |
| Header length | 4 |
| Payload length | 8 |
| Header | Declared, bounded |
| Concatenated LTX files | Declared, bounded |
| SHA-256 of all preceding bytes | 32 |

The header starts with SHA-256 of
`cellar-cell-group-entry-1/cell-history-1/range-route-1/administration-1`, followed
by group history (four 16-byte identities), route epoch (8), birth digest (32),
schema digest (32), proposal ID (16) and command tag (1). The receiving owner
supplies its expected route; mismatched history, epoch, birth or schema refuses.

Cell addresses contain the cell incarnation (16), ID byte length (2), and exact
UTF-8 ID. Cluster and namespace occur once in the group binding and are inherited
by the cell address. A key is recomputed from its namespace and validated ID.
An encoded range boundary also carries its digest; it must match recomputation.
Ranges have explicit infinity tags and retain the full ID as a collision tie-break.

An image is epoch/txid/checksum (three 8-byte integers), commitment tag (1), digest
(32), and manifest length (4). Materialized commitments require length zero.
Extent commitments use the extent store's actual root-reference validation.

File tables begin with a 4-byte count. Each record is five 8-byte integers:
encoded length, minimum txid, maximum txid, page count and decoded bytes. Payload
spans are consecutive and exhaustive; overlapping offsets cannot be represented.
Counts, header/payload limits and accumulated page/decoded-byte limits are checked
before allocation or physical staging. The decoder borrows payload slices from
the original entry buffer. The LTX applier must independently enforce actual
page/decompression limits; envelope declarations are not proof of physical size.

Fixed-position fields are mandatory and occur once. Unknown tags/domains, extra
fields, trailing bytes, zero identities, overflow, truncated declarations and
noncanonical image commitments refuse. The frame digest detects corruption; it
is not authentication or a quorum certificate.

## Commands

| Tag | Command | Required owner behavior beyond decoding |
| --- | --- | --- |
| 1 | Mutation | Check exact current cell history/base image and next sequence before staging; verify every LTX file and publish only a contiguous group prefix. |
| 2 | Enroll | Sequence starts at one. A nonempty image carries its full LTX chain from txid one in the log payload. Verify and sync the initial image before publication; empty enrollment must verify the canonical empty root. |
| 3 | Membership | Bind previous/next membership revision and physical member identities. Use the actual Raft ConfChangeV2 path and membership authorization; never treat it as an ordinary data entry that implicitly changes voters. |
| 4 | Checkpoint | Verify the complete manifest and its referenced image closure at the exact committed position and membership revision. |
| 5 | Seal | Verify the immutable seal authorization and complete parent/child topology union before irrevocably sealing the parent. Child routes must be distinct, compatible and contiguous. |
| 6 | Install | Verify all sealed source cuts and the destination manifest; sync the whole seed closure on each installing replica. |
| 7 | Activate | Verify publication, seed and current-term quorum before admission. |
| 8 | PreparePark | Bind park generation, term and membership; collect durable acknowledgements and retain wake ownership. |
| 9 | Retire | Verify publication and persist the parent tombstone; reclamation still needs archive and pin proofs. |

A decoded mutation exposes `check_base` against the coherent manifest leaf. This
check refuses another cell/incarnation, a skipped or repeated mutation sequence,
or a different base commitment. It must run before image preparation, and remains
owned by the serial apply path so another completion cannot change the base.

Enrollment carries bytes rather than only a root reference: majority persistence
of a root alone cannot protect data that exists solely on the proposing host.
Oversized initialization must refuse the configured envelope budget; it cannot
fall back to an unproven local or S3 reference.

The native contract fixture uses SQLite-produced LTX and verifies replay of an
enrollment from the entry payload alone. It also checks a separately calculated
retirement byte vector, hostile declarations, duplicate identities, every
administrative command, old-domain rejection, and borrowed payload ownership.
These are codec tests, not qualification of a running shared Raft group.
