<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# 10 GB per-cell implementation and qualification

This tracks the active implementation of the user-requested 10,000,000,000-byte
per-cell storage limit. The preceding 256 MiB extension is not completion of this
work. The architecture and product policy are in
[large-cell recovery](large-cell-recovery.md).

## Completion requirements

- [ ] R1: Admit logical SQLite images through 10,000,000,000 bytes, with exact
  page rounding and configurable smaller quotas. Preserve independent value,
  command-delta, response, transfer-memory and concurrency limits.
- [ ] R2: Remove whole-image memory allocation from command freezing, snapshot
  encoding, transfer assembly, archive download/verification and recovery.
  Anonymous scratch files and blocking work retain their owners through failure,
  cancellation and shutdown. Resource admission covers actual buffers and disk.
- [ ] R3: Preserve SQLite lock-page holes, length, full-image identity, LTX file
  checksums, ordered frame/index coverage and checked offsets across 1 GiB and
  4 GiB. Invalid, corrupt or incomplete input cannot produce a publication pin.
- [ ] R4: Preserve exact Raft/epoch/membership/disk-lineage proofs. Qualify warm
  takeover, fresh authorized peer/S3 recovery, lost local state, worker restart
  and exactly-once durable receipt reconciliation at the requested size.
- [ ] R5: Quota exhaustion rejects growth without changing the committed image;
  reads, cleanup and durable retries remain usable at capacity. Reserve bounded
  framework housekeeping/receipt capacity without bypassing output proofs.
- [ ] R6: Run actual SQLite images near the decimal quota with compressible and
  incompressible data. Verify data, version, subsequent writes, oversize refusal,
  corruption, disk/I/O faults and interruption. Exercise the actual RustFS/S3
  backend, not only in-process storage or a small database with a large bound.
- [ ] R7: Address warm first-write and cold activation work proportional to the
  complete image using incremental/COW publication and bounded recovery. Measure
  peak memory, transferred bytes and first-read/first-write time. Do not claim a
  recovery-time objective or production capacity from configuration-only tests.
- [ ] R8: Pass owning unit/native/crash suites, affected feature-mode checks,
  Clippy, formatting/layout, native dependencies and documentation checks.
- [ ] R9: Open a follow-up PR from updated `main` after merged PR #119, and retain a
  reproducible validation record with explicit scope and remaining limitations.

## Work in progress

- Added a file-backed v0.5.2 physical snapshot codec with a disk-spooled page
  index. Source reads, index buffers, transfer appends and encoded-file writes
  are bounded at 64 KiB. The codec omits the reserved SQLite lock page from LTX
  while binding canonical zero bytes into the complete physical image digest.
- Added independent logical and encoded size limits using u64 byte offsets.
  Frame/index/image/encoded integrity remains mandatory before physical output
  can be handed to the generation owner for sealing.
- Command freezing now hashes the independently read committed main-file/WAL
  overlay rather than allocating SQLite's serialized image. Tests compare its
  result with SQLite's pager across rollback, growth, shrink and checkpoints.
  Capture checks decoded/page/image bounds before page collection, including
  compression-heavy writes and fallback snapshots.
- Native snapshot sources/downloads use the file-backed codec. Transport moves
  file reads and appends into registered blocking work, retaining each scratch
  owner through cancellation. Installation tickets and membership/epoch checks
  remain separate from physical verification. The generation owner independently
  re-reads the output and shares the existing reservation/cleanup/sync barriers.
- Archives now store a small authenticated physical index and immutable 4 MiB
  parts, each using conditional create and exact read-back confirmation. Small
  snapshots inline their encoded bytes in the index to retain upload coalescing.
  The index is a format-1 hard cutover; no legacy archive reader is added.
  Recovery overlaps bounded part reads and spools the complete stream to disk.
  Snapshot plus bounded tail still shares a single private generation/publication.
- The opt-in materialized quota accepts decimal 10 GB without raising per-command
  delta, decoded-page, response or object-part buffers. Generation and unarchived
  admission are explicit disk/retention bounds, separate from transfer memory.
- Generation import, private writers and delta publication use exclusive COW
  clones on supported filesystems, with bounded copying elsewhere. Destination
  verification remains mandatory before callbacks or publication. Clone tests
  cover source isolation, collisions, unsupported fallback, corrupt output and
  uncertain creation. Full-image SHA-256 scans remain on these paths.
- Scratch sources live outside generation inventories; native receive and archive
  spools use the configured replica volume. Large transfers may explicitly use
  finite deadlines up to one hour; defaults remain unchanged. Longer deadlines
  are qualification controls, not a recovery-time objective.
- Added shared node snapshot scratch admission. Encoders reserve their entire
  body/index bound before creating files and shrink the charge after closing
  the index; receivers reserve their declared complete file. Source clones,
  partial-write failures and blocking owners retain the charge until the actual
  last file closes. Native source, receive and archive paths share one log-owned
  pool, separate from immutable generation and memory admission. Pressure maps
  to overload refusal. The large-image profile supplies a conservative four-body
  scratch floor; deployments may configure the independent byte/file limits.
- Private `SQLITE_FULL` failures return `StorageFull` before proposal submission.
  The disposable writer still requires rebuilding; an error alone does not
  prove rollback or permit reuse. Reads and durable receipts retain their normal
  committed-image proof boundaries.

## Validation so far

- The release-mode physical codec drill passed at the exact page-rounded quota:
  9,999,998,976 SQLite bytes and 10,083,173,257 encoded bytes. It exercised
  incompressible overflow data, offsets above 4 GiB, the reserved lock page,
  quota refusal, deletion/page reuse, restored payload hashes and full SQLite
  integrity checking. Current 64 KiB batched output finished in 695.60 seconds
  on the development filesystem, with 25,378,816 bytes maximum process RSS.
  Stages completed at 78.51 seconds (filled), 215.10 (encoded), 360.67 (received),
  501.20 (restored/synced), and 693.33 (SQLite integrity/payload verification).
  This is codec evidence, not native 10 GB or production RTO qualification.
- Native archive/execution/LTX/batched-apply/snapshot suites passed 149 tests,
  including process-kill snapshot and peer/S3 seed matrices, exact retained
  checkpoint recovery, partial transfers, shutdown, lost responses, missing
  quorum, corrupted archive parts and independent bundle publication. These
  ordinary native fixtures are small; they do not qualify native 10 GB recovery.
- Fast codec/capture/committed-image tests passed, including uncertain partial
  append poisoning, scratch-handle release, short reads, corrupted destination
  write detection and rejection of highly compressible oversized command output.
- LTX Clippy passed with warnings denied after COW and scratch placement changes.
- A 12 MiB full-capacity native template drill passed owner takeover, duplicate
  receipt reconciliation, quota refusal, cleanup/page reuse, an empty authorized
  S3 learner and local historical reads after restarting that learner before its
  peers. This validates the full-size harness at small scale. Full native 10 GB
  peer/RustFS runs and final-revision checks remain pending.
- The first full-size native RustFS attempt reached the exact quota-filled
  template, then correctly refused initialization at a remaining 256 MiB
  committed-marker guard. It failed after 48.93 seconds (26,591,232-byte maximum
  RSS); no native 10 GB recovery is claimed from that run. The guard now uses
  the decimal quota, with a regression test for the rounded last page and the
  first page beyond it. A rebuilt full-size run remains required.
- Shared scratch ownership tests pass for atomic refusal before source I/O,
  source cloning, completed downloads, uncertain partial appends and encoder
  errors. After native pool integration, 537 host library tests, four ordinary
  native recovery drills and 18 focused LTX tests pass. Clippy for both crates'
  libraries/tests with warnings denied, formatting, layout, native dependencies
  and documentation checks pass. The rebuilt full-size RustFS run completed
  native initialization at 263.28 seconds; its serving/recovery checks are still
  running. This remains incomplete full-size qualification.

## Remaining work

The completion checklist remains open. Full native 10 GB warm takeover, exact
peer and RustFS recovery after local state loss, restart/retry reconciliation,
quota housekeeping headroom, full qualification of aggregate scratch admission, and incremental
identity/lazy restoration still require implementation or qualification. COW
avoids full copies on supported filesystems, but full-image SHA-256 scans and
portable materialized copies remain on important paths; no
large-cell recovery-time objective is claimed yet. PR #119 has merged; this
uncommitted implementation requires a new follow-up PR from updated `main`.
