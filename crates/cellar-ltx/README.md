# cellar-ltx

Capture committed SQLite write-ahead log (WAL) data as LTX replication segments.
`cellar-ltx` writes and reads replicas, restores databases, compacts history and
supports page-on-demand access. It is an in-process library used by the
[`cellar` host](../cellar/README.md).

![SQLite WAL capture produces L0 LTX segments; replica storage supports restoration or page-on-demand access.](../../docs/diagrams/ltx-replication.svg)

Read the top row from source commits to captured segments, then follow storage
and restoration on the lower row. Capturing LTX alone does not prove a fleet
write. The host adds ownership, epoch fencing, replicated logs and output gates;
see [write durability](../../docs/06-storage-transactions.md#durability-and-external-side-effects).

## Find the replication API

| Task | Entry point | Source |
| --- | --- | --- |
| Capture WAL commits | `Db` | [Database capture](src/db.rs) |
| Drive replication | `Replica` | [Replication loop](src/replica.rs) |
| Implement a storage backend | `ReplicaClient` | [Client trait](src/client/mod.rs) |
| Store local replicas | `FileReplicaClient` | [Filesystem client](src/client/file.rs) |
| Restore a database | `restore` | [Restore implementation](src/replica.rs) |
| Read bundled L0 segments | `BundleOverlayClient` | [Bundle client](src/client/bundle.rs) |
| Read pages on demand | `paged`, `paged_vfs` | [Page map](src/paged.rs), [SQLite virtual filesystem](src/paged_vfs.rs) |
| Verify bounded consensus LTX | `ltx::decode_file_bounded` | [Strict decoder](src/bounded_ltx.rs) |
| Materialize and retain immutable images | `GenerationStore` | [Apply contracts](src/apply.rs), [Generation owner](src/generation.rs) |
| Query an immutable image | `generation::open_immutable` | [Read-only virtual filesystem](src/generation_vfs.rs) |
| Supply host tasks and files | `LtxHost`, `FileSystem` | [Host contract](src/host.rs) |

L0 segments capture commits before higher-level compaction. The paged SQLite
virtual filesystem loads each page on first use through indexes in retained
segments. A paged epoch continues the history from which it was opened.
Read the [LTX format notes](reference/ltx-format.md) before changing the codec.

The fleet host uses `Db::capture` for proof-bound work and schedules
`Db::checkpoint_if_due` separately, visiting one resident every 10 ms with one
maintenance job in flight. This spreads age-triggered checkpoints across cells.
Size-triggered checkpoints still run during capture. Both paths share exclusive
database ownership and the same WAL sealing and LTX staging rules; maintenance
does not acknowledge writes. Standalone `Db::sync` retains its combined policy.
A checkpoint captures its own sealing write before clearing maintenance state,
so a later capture cannot re-arm idle maintenance from that control write.

## Verified immutable images

`GenerationStore::prepare` copies an exact pinned base into a new private file,
then applies a contiguous LTX chain. Both the base and result require SHA-256
digests, including when LTX rolling database checksums are disabled. The bounded
decoder verifies file integrity, physical index coverage, page identities,
snapshot completeness and compressed-frame bounds before retaining page data.
Apply checks transaction ranges, declared page/byte counts, growth coverage and
truncation against the resulting SQLite header and full image digest. Published
images are never patched in place. This first materialized implementation uses
a full verified copy; it does not require filesystem reflinks.

File operations use the injected `FileSystem` synchronously on the host's owning
blocking/execution domain. Publication syncs the complete image, reserves its
unique final name with exclusive creation, renames over that owned unpublished
placeholder, then syncs the directory. Directory creation also syncs ancestor
entries. Unsupported operations fail. Failed creation with uncertain ownership
or failed cleanup quarantines the store until it is closed and recovered; it
does not refund unknown disk usage or overwrite an unexpected image.

A returned pin proves physical image sealing. The Raft host must still persist
the corresponding engine apply marker before making that pin queryable or
settling a command. Recovery inventories retained files within configured bounds
and `adopt` verifies the image selected by an authoritative marker. It never
selects the newest filename. The host calls `retire` only after all durable
marker, recovery and archive references are gone. Reader pins retain both the
image and directory lock; bounded reclamation syncs deletion before refunding
capacity. Default admission allows 64 generations and 512 MiB, reserving twice
the largest intermediate image while an update is in progress.

`generation::open_immutable` opens only the pin's virtual filename. Its SQLite
VFS reads through the injected filesystem, refuses writes, sidecars and native
path access, and receives clock/randomness callbacks through `ReadServices`.
Descriptors use a process-lifetime pool capped at 4,096 concurrent owners; idle
slots retain no pins, services or files and can be reused after their owner and
all files close. Permanent descriptors avoid dangling SQLite registry pointers.
The host's `cellar::storage::open_immutable_generation` adds its SQL limits and
read authorizer without initializing framework tables. Query registration and
Raft publication authority remain the host's responsibility.

Run `cargo test -p cellar --test native_raft_ltx --locked -- --test-threads=1`
for real SQLite images, checksum/chain corruption, retained readers, injected
I/O failures and process-kill publication cuts. Those cuts use a fixture marker;
engine marker atomicity is qualified by the later command/apply integration.

## Select backend features

The default feature is `s3`. The manifest also exposes `gcs` and `azure` backend
features; see [Cargo.toml](Cargo.toml) for their dependency mappings.
`ObjectStoreClient` and its configuration exports are gated by `s3` in the
current API. Feature selection alone does not configure credentials or qualify
a provider's durability behavior.

Check the core and complete feature builds from the repository root:

```sh
cargo check -p cellar-ltx --no-default-features --locked
cargo check -p cellar-ltx --all-features --locked
```

Use the checkout-specific target directory in
[build guidance](../../AGENTS.md#build-and-validation). The ordinary library test
target is disabled; replication changes also need the owning host's integration
and recovery tests. Internal tests require their external harness.

## Provenance and attribution

Seeded on 2026-08-03 from a read-only snapshot of
[rustyriver](https://github.com/mikenomitch/rustyriver), a Rust reimplementation
of Litestream v0.5 and the LTX format. Cellar maintains this source independently
of the upstream branch.

Attribution for the vendored and ported work:

- **rustyriver** — Copyright 2026 The rustyriver authors, licensed under the
  Apache License, Version 2.0.
- **Litestream** (https://github.com/benbjohnson/litestream) — Copyright (c)
  Ben Johnson and the Litestream authors, licensed under the Apache License,
  Version 2.0. The original replication behavior comes from tag v0.5.11. The
  current block format follows tag v0.5.16 because it includes LTX v0.5.2.
- **LTX file format and reference implementation**
  (https://github.com/superfly/ltx), tag v0.5.2 — Copyright (c) Superfly, Inc.,
  licensed under the Apache License, Version 2.0.
- **pierrec/lz4 block compressor**
  (https://github.com/pierrec/lz4), tag v4.1.23 — Copyright (c) 2015 Pierre
  Curto, licensed under the BSD 3-Clause License. The Rust port preserves the
  exact compressed bytes that the LTX v0.5.2 writer produces.

Cellar omits Litestream's object-storage leaser. The unused port was removed
on 2026-08-06. Cellar instead uses a conditional ownership record carrying an
epoch and stamps that epoch into the LTX prefix. A separate replica lease would
introduce a competing ownership mechanism.

The full Apache License, Version 2.0 text is in [LICENSE](LICENSE). The full
BSD 3-Clause License text is in
[LICENSE.pierrec-lz4](LICENSE.pierrec-lz4).

## File compatibility

Cellar's node-bundle envelope contains verbatim L0 files followed by row metadata,
a little-endian CRC64-ISO checksum, a little-endian u32 footer length and `CLB1`.
Integrity covers payload, footer, length and magic before row metadata can
authorize recovery or collection. Rows must partition the payload, have positive
epoch/TXID and agree with their L0 header transaction range. The development
layout hard-cutover retains marker 1: recreate earlier unchecksummed bundle state
and rebuild participants. There is no legacy bundle reader or migration. This
container change does not rewrite the upstream LTX files described below.

Cellar and Litestream v0.5.16 can read the new block files and the older frame
files. Litestream v0.5.11 can read only the older frame files. This is a reader
compatibility boundary because LTX keeps file version 3 for both layouts.

The ordinary Cellar L0 writer still emits the older frame representation. The
v0.5.2 encoder and compactor emit exact block files. Cellar contains a
node-wide scheduler that publishes additive L1 files, and the scheduler is on
by default. Every takeover target must have the dual decoder before the first
L1 publication, so a mixed fleet must set `CELLAR_LTX_COMPACTION=0` until all
nodes can read block files. The same reader-first requirement applies to a
later L0 writer switch.
