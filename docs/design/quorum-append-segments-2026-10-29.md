<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Append-segment image persistence

Purpose: Describe opt-in append-segment image persistence, group barriers, recovery and GC boundaries, and matched verification.

This follows the [parallel publication and latency investigation](quorum-parallel-publication-2026-10-29.md).
The experiment replaces separate extent/root files with bounded append segments
while preserving independent destination verification and native publication gates.
The existing file inventory remains available as the matched control.

## Implementation

`NodeExtentStore::open_append` and `RaftExtentOptions::append_segment_bytes`
select the new format explicitly. The default remains the existing inventory.
The node uses the same inventory path and refuses a different populated format;
no legacy reader, migration or newest-root selection is added. Format markers
remain at 1. Targets accept 4 KiB through 64 MiB; the benchmark selects 8 MiB.
Records larger than the target occupy their own segment.

Extent and root payloads carry typed identities, exact offsets and lengths,
authenticated framing and completion footers. One admitted append owner finishes
each record before another writer reserves a later position. Source buffers and
physical I/O retain their existing bounds. A successful barrier covers only the
complete-record watermark observed before the barrier began. Compatible callers
can share that proof. Later appends require the next barrier, even if a device
happens to flush them early. No deliberate coalescing delay or worker is added.

Image preparation selects the last pending record in each touched segment,
syncs through it once, independently verifies every destination extent/root after
sync, and only then enables the image seal. Segment creation syncs its header
and directory entry before use; ordinary appends do not add directory entries.
Native Raft log, marker, ownership, epoch, receipts and output gates remain in
place. Per-stage and acknowledgement latency must both improve; persistence is
not moved to a later timer.

Recovery authenticates bounded framing and indexes exact ranges without scanning
extent bodies. It clears process-local verification and durable watermarks.
Short or authenticated incomplete tails remain charged and are never overwritten;
corrupt complete framing refuses opening. Exact authenticated native references
still select roots. Reads and seals cannot turn append order into authority.
Uncertain writes, sync failures and source/sync panics quarantine admission and
wake pending waiters. New publication also independently verifies bytes after a
successful shared sync.

Physical accounting includes payloads, framing, segment headers, dead records,
and incomplete tails. Whole-segment GC preserves pins, active references and
all dependencies of roots in every retained segment. Bounded deletion cohorts
include referring root segments, allowing a small GC budget to remove roots
before their large data sets. Directory persistence precedes refunds. Mixed
live/dead segments retain their full byte and logical-entry charges. Relocating
compaction is not implemented; fragmentation and dependency cycles exceeding
the GC budget can cause conservative capacity refusal. This is an opt-in latency
optimization, not a complete general-purpose log-compaction design.

## Validation record

The initial actual-SQL regression fails against the file-backed delegate with
**two file syncs instead of one**. It passes with append storage and also requires
**zero directory syncs** for an existing segment. A second regression exposes a
small-budget GC stall, then requires progress in one-segment deletion batches.

Component tests cover restart proof loss, frame corruption, conservative torn
tails, identity exclusion, independent post-sync corruption, capacity refusal
before creation, physical accounting, mixed segments, dependency protection,
reader pins, failed deletion, source/sync panics and wakeups. A deterministic
three-writer test proves two compatible waiters share their next barrier and
cannot use the barrier that started before their appends. A parent process also
cuts actual SQL preparation immediately before and after the shared sync and
reopens the explicitly selected old root. These process-exit cuts do not simulate
power loss or qualify a distributed disk-loss recovery.

The native fixture exercises two cells, closed-group retention, receipt replay,
joined shutdown and three starts with append storage. The benchmark uses three
one-vCPU/3-GiB replicas, RustFS, ten initially empty cells and 1-KiB writes on the
same guest ext4 filesystem and workspace SSD as the previous investigation.


## Completed checks and artifact identity

Rust 1.98.1, locked `lab` builds pass **167 LTX integration tests**, **287 native
Raft library tests**, and **4 public native extent tests**: **458 passing Rust
cases**. Ten helper/qualification entries are ignored by these ordinary
invocations; the new process-exit helper is invoked by its passing parent.
Clippy passes for Cellar and LTX libraries/tests with warnings denied. Repository
format and layout checks cover 1,764 Rust files; native dependencies contain no
JavaScript engine; documentation and `git diff --check` pass. The Python tooling
suite runs 279 cases successfully, including one skipped case. Its metric
vocabulary now includes the existing 54 image-role/phase observations and 13
image counters; the exact native contract assertion remains intact and previous
44-operation observations remain readable.

The native public fixture verifies 49 committed callbacks across two cells,
closed-cell retention, restored receipt deduplication and joined inventory
release through three starts. These are ordinary component/native regressions,
not power-loss or distributed disk-loss qualification.

All accepted histories use one exact dirty-source image (base HEAD
`1be27d234d4674d4d014785c60cecbfe394bdab9`):

- image_id: `sha256:6d4172802fde6f99f17310a5676397138ffb6e3853629e89ee72f7e8d0b80bc5`
- binary_sha256: `bea898685751b3ce6359a770ec1742cda11e4b7a7967be652b9b94789a5be2f4`
- source_manifest_sha256: `aadea10cd11a868c5940e00f6c46eaea1fe9b205d43d3c7c0884106160b28415`
- build_profile: `lab`

The initial full build encountered an incremental-LTO undefined-symbol link
failure. Cleaning only this checkout's LTX package artifacts and rebuilding
produced the passing artifact above. Failed/interrupted build logs and the two
intentional failing regressions are retained separately; they are not counted
as successful qualification.

## Matched performance comparison

Three replicas each use one pinned vCPU, 3 GiB RAM and no extra swap. RustFS uses
two vCPUs and 4 GiB. All share the same guest ext4 disk and workspace SSD; these
are not three independent physical storage devices. Ten cells begin without
bulk payload, with their normal schema/business row initialized. Writes are
1 KiB. Both backends use two SQL workers, publication capacity two, 25-ms host
polling and 1,000-ms archive checkpoints. Ext4 `data=ordered`, barriers and the
15,000-µs maximum journal batching wait remain unchanged throughout.

The first four histories run files/append/append/files, each with three windows
of 500 successful writes at concurrency ten. Their measured write work and final
business states match exactly. All six accepted fixed-work histories, including
the later sharing controls, independently match the same final business states.
The primary pooled comparison below uses the original two histories per backend,
3,000 acknowledged writes each. It sums elapsed seconds and request timings,
rather than averaging rates or selecting the fastest windows.

| Fixed work, concurrency 10 | Files | Append + sharing |
| --- | ---: | ---: |
| Acknowledged writes/s | 67.10 | 73.93 |
| Leader staging mean, ms | 27.74 | 21.29 |
| Follower preparation mean, ms | 25.00 | 15.91 |

Matched-work throughput increases **10.2%** in this comparison. Individual accepted file histories deliver 64.78/69.58 writes/s; the first two append histories deliver 75.93/72.03 writes/s. This is a workload-specific observation, not a storage SLO.

Separate timed runs use two 10-second write windows per concurrency, interleaved
with two-second reads and reversed order. Their completed work differs, so these
rows do not substitute for the fixed-work comparison:

| Timed workload | Files | Append + sharing |
| --- | ---: | ---: |
| c1 acknowledged writes/s | 26.49 | 32.89 |
| c1 client p99, ms | 80.86 | 49.91 |
| c1 leader staging mean, ms | 14.26 | 9.30 |
| c1 follower preparation mean, ms | 13.65 | 6.15 |
| c10 acknowledged writes/s | 76.94 | 87.99 |
| c10 client p99, ms | 404.36 | 496.07 |
| c10 leader staging mean, ms | 24.38 | 19.47 |
| c10 follower preparation mean, ms | 21.75 | 14.12 |

Concurrency-one mean staging/preparation now fit below 10 ms when pooled. This
is not a consistent per-request or per-window guarantee: append staging means
are **6.77/12.93 ms**, and follower preparation **5.38/7.26 ms**, in its two
concurrency-one windows. The later window has more accumulated cell/receipt
state. Both backends slow during their histories. Timed concurrency-ten client
p99 **regresses from 404.36 to 496.07 ms**, despite higher throughput. The first
two fixed append histories also have worse p99 than their file controls. The
additional shared confirmation later improves its tail, so these experiments do
not establish a stable loaded-tail improvement. The backend remains opt-in.

Stage percentiles come from histogram boundaries, while client p99 above comes
from raw successful-response samples. The concurrency-one stage histograms are:

| Backend / stage | Requests | At most 10 ms | p50 upper bound | p95 upper bound | p99 upper bound |
| --- | ---: | ---: | ---: | ---: | ---: |
| files / stage leader | 530 | 20.4% | 25 ms | 25 ms | 50 ms |
| files / prepare follower | 1060 | 33.7% | 25 ms | 25 ms | 25 ms |
| append / stage leader | 659 | 65.3% | 10 ms | 25 ms | 25 ms |
| append / prepare follower | 1318 | 95.3% | 10 ms | 10 ms | 25 ms |

### Concurrency one: timed phase means

Exclusive wall milliseconds per completed stage; synchronization waits are not SQL or CPU execution.

| Phase | Leader files | Leader append | Follower files | Follower append |
| --- | ---: | ---: | ---: | ---: |
| inventory wait | 0.086 | 0.021 | 0.035 | 0.034 |
| sql | 0.233 | 0.235 | 0.000 | 0.000 |
| ltx decode | 0.015 | 0.015 | 0.018 | 0.018 |
| extent read | 0.170 | 0.099 | 0.100 | 0.097 |
| extent hash | 0.264 | 0.296 | 0.255 | 0.287 |
| extent write | 0.020 | 0.037 | 0.022 | 0.025 |
| file sync | 7.882 | 4.524 | 9.891 | 5.447 |
| directory sync | 2.299 | 0.032 | 3.063 | 0.019 |
| barrier wait | 0.156 | 0.421 | 0.026 | 0.001 |
| other | 3.140 | 3.617 | 0.241 | 0.221 |
| Whole stage | 14.265 | 9.299 | 13.653 | 6.151 |

### Concurrency ten: matched fixed-work phase means

Exclusive wall milliseconds per completed stage; synchronization waits are not SQL or CPU execution.

| Phase | Leader files | Leader append | Follower files | Follower append |
| --- | ---: | ---: | ---: | ---: |
| inventory wait | 1.884 | 0.207 | 1.512 | 0.212 |
| sql | 0.195 | 0.197 | 0.000 | 0.000 |
| ltx decode | 0.017 | 0.018 | 0.019 | 0.018 |
| extent read | 0.536 | 0.082 | 0.500 | 0.076 |
| extent hash | 0.256 | 0.241 | 0.236 | 0.222 |
| extent write | 0.029 | 0.038 | 0.030 | 0.035 |
| file sync | 16.316 | 10.770 | 15.589 | 10.422 |
| directory sync | 2.931 | 0.039 | 3.752 | 0.044 |
| barrier wait | 1.893 | 6.485 | 1.937 | 4.195 |
| other | 3.680 | 3.215 | 1.422 | 0.686 |
| Whole stage | 27.738 | 21.292 | 24.998 | 15.910 |

Directory persistence is amortized over segment creation/rotation, and fewer
file barriers reduce staging and follower work. Shared-segment barrier waiting
remains visible under load. Independent root/extent post-sync verification is
retained. Phase totals are request wall times; they cannot isolate the cause of
the client-tail regression by themselves.

Other roles remain separately observed in the same matched fixed-work histories:

| Whole role mean, ms | Files | Append + sharing |
| --- | ---: | ---: |
| prepare leader | 0.033 | 0.032 |
| publish leader | 0.545 | 0.574 |
| publish follower | 0.106 | 0.053 |

## Cross-commit sharing control and failed history

The accepted append run with cross-commit sharing disabled delivers **86.49
writes/s**. A later accepted shared confirmation delivers **86.91 writes/s**,
while earlier shared histories deliver 72.03/75.93 writes/s. The reversed
unshared confirmation fails in warmup with two ambiguous command outcomes and
one unavailable response; it reaches no measured windows. It is retained as a
failed history, with no mutation retries, and is excluded from all performance
pools. Containers subsequently exit cleanly without OOMs. A kernel snapshot
catches node 1 in ext4 fsync/journal-commit waits and its admin requests time out;
this observation alone does not isolate the timeout's cause.

The protocol regression establishes that compatible waiters can share a barrier
and that late records cannot use it. These application histories do **not**
establish a standalone throughput win from cross-commit sharing at two workers.
The measured format/persistence gain must not be relabeled as a proven gain
from every individual mechanism.

## Space, bounds and qualification limits

Matched file histories finish with allocated node directories of **36.9–39.2 MiB**; the first two append histories finish at **63.1–71.0 MiB**. These include logs and private files, not only immutable storage. Whole-segment retention keeps more dead records than per-file reclamation; the shared fixed histories retain roughly 574–700 objects per node versus approximately 20–55 in the file controls at the sampled end. File-byte admission includes those retained bytes, so they are not silently refunded. Relocating compaction remains future work.

Across accepted histories, maximum sampled replica working set is **25.1 MiB**, and replica cgroup peak (including charged cache) is **126.5 MiB**. RustFS peaks are **470.9/748.2 MiB**, respectively. No accepted container records an OOM. These small-state histories do not qualify a one-GiB cell, memory exhaustion, sustained fragmented-log capacity, power loss, or independent host/disk availability.

**Eight accepted histories and one failed warmup** are preserved. Independent checks reconcile **79,288 ledger records** and **480 replica-state audit observations** across accepted histories, plus receipt/version/archive coverage and clean shutdown. Six accepted fixed-work histories end in identical business states.


The external evidence directory is
`/Volumes/Workspace/crabbuild-target/cellar-2446b3c0-dense`. It retains
`append-final-build-v6.log`, intentional red-test logs, source-bound reports,
raw operation/resource ledgers, stage snapshots, independent audits,
`append-matrix-status.json`, `append-group-confirmation-status.json`,
`append-qualification.json`, `append-comparison.json`, `append-final-summary.json`,
runner/analysis sources and before/after ext4 options. Earlier experiments are
not overwritten. The final VM is stopped after reports are collected; its data,
images and diagnostic histories remain available.


[Read-performance verification](read-performance-verification-2026-10-29.md) records the subsequent warm follower read-only comparison with client read-evidence syncs outside the measurement windows.
