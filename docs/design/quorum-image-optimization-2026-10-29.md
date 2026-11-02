<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Quorum image pipeline optimization — 2026-10-29

Purpose: Record image staging, preparation, and publication optimizations with preserved durability proof boundaries.

Status: implemented and locally validated. Extent publication improves
substantially; staging and average replica preparation do not show a reliable
wall-time improvement. Overall extent throughput is not consistently better
across controls. The existing materialized backend is faster for the tested
small-state workload, with a different image-size limit. This follows the
[Quorum throughput diagnosis](quorum-throughput-diagnosis-2026-10-29.md).
The benchmark base is `1be27d234d4674d4d014785c60cecbfe394bdab9`.

## Implementation and proof boundaries

Extent-backed committed apply now accepts an optional private executor, matching
an existing optimization for materialized images. It validates the committed
entry and native apply ticket before attempting transfer. Transfer requires a
submitted preview, the original replica and open inventory owner, an exact
encoded-payload SHA-256, exact base/result commitments, and result geometry. The
preview was independently replayed, checked and sealed before submission.
Followers, recovery and mismatches continue to replay and seal LTX normally.

Taking the preview transfers its live pin out of private cleanup ownership.
Publication still checks the durable applied marker against the pending ticket
and image. New extent bytes and their directory, new root metadata and its directory,
and the Raft engine log retain their persistence barriers.

The applier now retains applied/snapshot references and retires previous sealed
images through metadata-only operations. A live exact-owner seal pins the root
against GC while these operations execute. Active marker references continue to
protect it after readers drop; only durable deletion refunds disk charges.
Physical inventory mutations continue to serialize on their blocking owner.

`validate_image_seal` checks that a live capability belongs to this inventory,
its root token/length still match, every dependency remains verified, and the
inventory is healthy. A known failed local read revokes eligibility until a
fresh complete verification succeeds. This check performs no I/O and cannot
promote a reopened root view or grant native publication authority.

Root entries also remember completed metadata file/directory barriers within
one open inventory. Repeated seals still read and authenticate the root file
and validate image bytes, but do not repeat barriers for already durable
immutable metadata. Reopen initializes this proof to false; any failed root
metadata read revokes it. A plain reopened view cannot establish it, and a live
image cannot bypass its revocation. GC removes the entry with the deleted root.

## Regression evidence

Two real host-SQL tests stage and submit commands, obtain committed native apply
tickets, and block an unrelated physical extent write in the shared serialized
inventory. They require preparation/publication to finish while that writer
remains blocked. They also refuse publication before the native applied marker,
then verify receipts and ReadIndex reads across two successive commands.

Both tests failed on the original production implementation with
`committed extent progress waited for unrelated inventory I/O`; zero tests
passed, two failed. Both pass with the optimization, and the broader Raft
library selection passes 285 tests, with zero failures and two existing ignored
tests. The LTX integration selection passes 135 tests with zero failures and
five existing ignored tests across 18 binaries. The red snapshot and build
logs are retained independently. The initial Clippy invocation had a duplicated
CLI flag; the corrected command is run separately without changing source.

Additional tests cover exact transfer bindings, foreign inventories,
non-submitted previews, payload/base/geometry mismatches, one-time pin transfer,
known read corruption, quarantine, metadata progress during physical I/O, and
reclamation protection by both live pins and active marker references.

## Measurement protocol

Run the original and optimized immutable images sequentially after all build,
test and Clippy activity ends. Both runs use three Linux/arm64 containers,
each pinned to one vCPU with 3 GiB memory and no additional swap; ten fresh
cells without bulk padding; one-KiB writes; two SQL workers per node; a 25-ms
host poll; a 1000-ms archive checkpoint interval; and the existing 1152-MiB
extent image limit. RustFS receives two vCPUs and 4 GiB. Data is on the same
ext4 disk image backed by the workspace SSD. Write windows last ten seconds,
repeat twice at concurrency 1 and 10, and have before/after stage metrics.
Read windows last two seconds. No mutation is retried.

The baseline's complete source manifest is reconstructed from the base revision
for the changed core/document files and the unchanged benchmark checkout for
all other inputs. It matches all 2726 source files in the original image. The
optimized image must match the complete current checkout manifest. Executable
hashes and actual resource settings are checked independently.

These are short local measurements on one shared physical host, VM and disk.
Completed-operation wall times include blocking, and sums over replicas measure
worker occupancy rather than the latency of a majority acknowledgement.

## Intermediate measurement

The preview-transfer/metadata-retention build alone passed all selected tests
and independently reconciled its workload, but did not improve total throughput:
46.07 and 39.83 writes/s at concurrency 10 versus 46.14 and 48.09 in its fresh
control. Publication fell from 9.14–9.78 ms to 0.37–0.68 ms, while staging and
preparation absorbed more inventory wait. Preparation histograms show 469 of
1407 operations below 1 ms in the first loaded window, matching the 469 staged
leader commands: preview reuse worked, but did not remove the physical bottleneck.
This intermediate result is retained, not reported as a successful overall
optimization. Its image is
`sha256:05c52ecfe46a4de944143b93bc2b75ea308497109fcf8ab6d1eda48ea1977046`.

A second regression against that implementation reproduced redundant root
metadata barriers: sealing the same immutable root again repeated its file
fsync. It failed before the persistence-proof change and passes afterward. The
updated LTX selection passes 137 tests, with zero failures and five existing
ignored tests. The final Raft selection also passes 285 tests, with zero
failures and two existing ignored tests (422 passing tests in total). The final change addresses that remaining physical work. Restart,
failed metadata reads, file-sync failure and directory-sync failure must all
retain their normal proof requirements.

## Final extent results

The final immutable image is
`sha256:9d77e5281e512d643d179cd274bc9d849fdff79dac6aba5a73991b0e28462e6f`.
Its Raft executable SHA-256 is
`b47fc4cc726a60b08798778910a9e9d2866eafff2d0e5ec3704c39f668e86bb4`;
the full source-manifest digest is
`6206c76445b28e961d228bb2ac3a7c35b7db24cda4db800381914d2d42de1483`.

The two ten-second concurrency-10 windows have these operation-count-weighted
means (all three replicas contribute to preparation/publication):

| Operation | Original extents | Optimized extents |
| --- | ---: | ---: |
| SQL/image staging | 40.96 ms | 41.97 ms |
| Image preparation per replica | 20.77 ms | 23.91 ms |
| Image publication per replica | 10.06 ms | 0.428 ms |
| SQL queue | 33.45 ms | 32.67 ms |

Publication improves **95.7%**. Staging and preparation do not improve in this
loaded measurement. The new leader avoids physical preparation, but the replica
average still includes follower replay and waits in the shared physical
inventory. Removing publication waits can move waiting time into these stages;
completed-operation means are not an additive majority-latency breakdown.

Throughput is 40.94/46.78 writes/s before and 49.99/50.49 after (pooled 43.84
and 50.24/s, +14.6% in this pair). The p99 ranges overlap: 416.5–492.1 ms before
and 436.2–460.0 ms after. These short observations alone do not establish a
repeatable overall speedup.

## Equal-work control and small-state configuration

To remove differences in accumulated receipt state, an additional experiment
uses exactly 10 warmup operations per worker with three workers, then two
500-write windows with ten workers. Each worker performs 50 operations per
measured window. Reads use the same fixed operation budget. Every variant has
identical final per-cell command sequences and business state. The ledgers
independently reconcile; no mutation is retried.

| Metric | Original extents | Optimized extents | Materialized, same optimized binary |
| --- | ---: | ---: | ---: |
| SQL/image staging | 33.17 ms | 42.31 ms | 24.62 ms |
| Image preparation per replica | 17.94 ms | 23.45 ms | 16.61 ms |
| Image publication per replica | 8.684 ms | 0.423 ms | 0.104 ms |
| Pooled writes/s | 50.32 | 48.51 | 70.89 |
| Individual write windows/s | 55.01 / 46.36 | 51.51 / 45.84 | 76.20 / 66.27 |

This control confirms publication's improvement, but does **not** establish an
extent staging/preparation or overall throughput improvement. Do not advertise
the timed pair's +14.6% as a reliable general speedup.

For the current empty/small-cell workload, the existing materialized default
is the faster measured configuration: **40.9% more writes/s** than original
extents in this equal-work comparison. It avoids the node-wide physical extent
inventory. This is a configuration result, not an additional performance gain
attributed to the new extent code. Both options retain RF3/majority durability.

The materialized run uses the SDK's **default 64 MiB per-image quota**, while
the extent runs retain 1152 MiB. This is not a hard materialized-backend limit:
`RaftCellsConfig::with_materialized_image_limit` configures coherent image,
archive, transfer and scratch quotas up to 10,000,000,000 bytes. Larger configured
quotas do not qualify larger-state throughput or recovery on these nodes. `--image-mib 0` in the diagnostic harness selects that
materialized default; it does not configure a zero-byte limit. Three nodes
still have one vCPU and 3 GiB each. This option does not qualify larger cells
and must not be substituted silently for an existing extent namespace: image
storage is part of the immutable namespace contract. Removing the explicit
extent selection is suitable when creating a fresh small-cell namespace.

## Physical sync evidence and remaining limit

A separate one-cell trace uses the final extent image. During 33 acknowledged
writes in its measured window, the traced leader issues 32 extent-file syncs,
32 root-file syncs, 68 inventory-directory syncs and 95 other syncs (primarily
Raft log persistence). Every observed root file syncs once. One-command boundary
spill is possible; background work is included. The earlier trace observed
repeated root-file syncs, commonly five per file.

This verifies removal of redundant root barriers. It does not remove the
mandatory new extent/root and Raft log barriers. All three nodes share one
physical host, VM and disk, and the extent inventory serializes physical
mutations per node. The remaining staging/follower costs include those physical
operations and their waits. Further work would need a separately designed and
qualified way to overlap independent inventory publications or batch barriers;
these changes do not weaken publication proofs or claim that work is done.
Tracing perturbs throughput and is excluded from capacity comparisons.

## Validation and retained evidence

Passed: 285 Raft library tests (two existing ignored tests), 137 LTX integration
tests (five existing ignored tests), Clippy for both affected crates' libraries
and tests with `-D warnings`, `cargo format --check`, `cargo check-layout`, native
dependency and documentation checks, and `git diff --check`. No
`cellar_internal_tests` configuration was enabled. The ignored set includes
explicit Docker/10-GB campaigns and child tests invoked by their parents;
this is not full fleet failure/soak qualification.

One final-pair setup failed before traffic because the retained Docker bridge
networks exhausted the VM's default address pool. No commands or measured
windows ran. The failed setup is retained separately. Both successful final
variants use explicit, non-overlapping /24 subnets in the owned VM. No retained
containers, networks or data were deleted to work around this.

Private evidence root:
`/Volumes/Workspace/crabbuild-target/cellar-2446b3c0-dense/`.
`optimization-final-summary.json` and `optimization-v2-comparison.json` retain
the timed calculation; `optimization-fixed-summary.json` retains equal-work
results. `optimized-v2-strace-2026-10-29/quorum/sync-analysis.json` retains the
bounded trace summary. `optimize-red.log`, `optimize-barrier-red.log`,
`optimize-tests-and-clippy-flag.log`, `optimize-green.log` and `optimize-v2.log`
retain red/green and build evidence. The external `profile_fixed_work.py`
reuses the same checked client loop with a fixed operation count; it does not
change production code or disable source/executable verification.

All **8 successful histories** independently reconcile **104,605 ledger
records** and **426 replica state audits**. Archive coverage, actual resource
limits, zero OOM kills and clean process exits pass. Credential-bearing
`config/` directories remain private. The dedicated benchmark VM is stopped
after validation; images, containers and data are retained.

## Timing interpretation and further optimization

The SQL-worker queue timer ends before the stage/prepare/publish stopwatch
starts (`host_sql/mod.rs`). These operation timings therefore exclude the
separately reported worker queue, but include blocking inside the operation.
`NodeExtentStore::start` admits one physical operation per node inventory and
waits on a condition variable while another operation owns that admission. It
does not keep the state mutex held during I/O. Ten cells share that gate.

Staging includes the SQLite command and durable receipt changes, WAL-to-LTX
capture, the result observation, and independent LTX replay into a sealed
physical preview. A follower's committed preparation decodes/replays LTX,
constructs changed immutable extents, verifies destination bytes, and persists
the new dependencies and root. Even a small logical update replaces the touched
extent's bytes (up to 4 MiB, smaller for a short image). These are not isolated
SQL statement timings. File/directory sync, inventory-admission wait and CPU
scheduling all contribute. The current metrics do not separate their exact
fractions, so a claim that a particular fraction is CPU or lock wait would be
unsupported.

The final loaded preparation histograms contain 513/1538 and 516/1547 operations
at or below 1 ms, consistent with one fast leader transfer per command. The
reported per-replica average mixes this fast path with two follower replays.
Publication's roughly 96% reduction is a local result; removing its wait does
not eliminate the underlying new-object persistence or increase the inventory's
physical-operation capacity. Waiting can consequently appear in other stages.
The materialized comparison is independent of that result and is not evidence
that the publication fast path failed.

The next controlled changes should be:

1. Split stage/preparation telemetry into inventory wait, SQL, LTX decoding,
   extent read/hash/write, file sync, directory sync and Raft engine sync. Record
   leader/follower roles separately and compare identical operation counts.
2. Allow bounded overlap for independent physical publications using the
   existing worker budget, rather than increasing workers behind the same
   gate. Preserve per-extent/root exclusion, atomic quota reservations,
   quarantine behavior and GC exclusion/pins. This needs correctness and crash
   regression coverage; no parallel publication implementation is included here.
3. Batch compatible persistence barriers where one completed barrier covers
   several prepared objects/roots. Grant no sealed/publication proof before
   every dependency and required namespace change is durable. Profile actual
   bytes rewritten before considering a different extent granularity or delta
   representation.

Repeat performance checks on independent node storage as well as the shared-VM
fixture to distinguish code limits from coupling through one physical disk.

The [bounded parallel publication follow-up](quorum-parallel-publication-2026-10-29.md)
splits leader/follower timers and measures compatible persistence batching on
the same empty-start three-node setup.
