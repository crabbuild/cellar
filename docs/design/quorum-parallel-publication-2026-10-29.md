<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Quorum image publication: bounded concurrency and persistence batching

Purpose: Record bounded independent publication, compatible persistence barriers, detailed phase timers, and latency experiments.

This follows the [image optimization measurement](quorum-image-optimization-2026-10-29.md).
The experiment keeps three replicas at one vCPU and 3 GiB each, ten initially empty
cells, 1 KiB command payloads, two existing SQL workers per node, and the same
pinned RustFS object store. It changes physical extent publication admission and
compatible directory persistence barriers. It does not change the required
quorum, native marker, file sync, destination verification or archive proofs.

## Changes and proof boundaries

- `NodeExtentStore::open_parallel` bounds admitted physical work. The native host
  defaults to two operations; the existing workers execute all work. Object and
  root keys serialize same-name writers. Inventory quota reservations happen
  before creation and remain charged through failed or interrupted work.
- GC and reference-set replacement require exclusive admission, with waiting GC
  taking precedence over new shared work. Active operations, live pins and
  durable marker references protect dependency closures. Metadata-only retention
  of an already sealed image continues to avoid physical I/O.
- A preparing image writes a bounded cohort of new files, including root
  metadata, before starting persistence. It retains owned destination handles
  privately until every file is synced, independently reverified after sync,
  and covered by a directory barrier. Directory epochs are
  registered after creation; a sync covers only epochs registered before it
  starts. A peer's namespace proof never substitutes for unfinished file data.
  No prepared image escapes until every required dependency is eligible.
- A shared directory failure quarantines mutation and refuses pending image
  seals. Files stay charged. Restart reconstructs authoritative retention,
  verifies selected images, and durably deletes orphans before quota refunds.
- A cohort flushes its pending dependencies before blocking on another cohort's
  object key. This prevents deadlock when two images need the same objects in
  opposite order. Ordinary immutable reads remain concurrent; readers wait on
  a writer only while an object is being initially published.

## Instrumentation

Request-local exclusive wall-time phases split inventory wait, SQL, LTX decode,
extent read/hash/write, file sync, directory sync, shared-barrier wait and other
work. Stage is leader work. Preparation and publication have separate leader and
follower samples. Every request samples every phase, including zero work; separate
counters report primitive calls and extent bytes. Nested phases pause their
parent, so file sync is excluded from SQL time. Queue time is reported separately. SQL covers receipt preflight, the command
transaction and committed-result queries; connection setup, metadata operations,
manifest work and page-checksum work outside named primitives remain in other.
Role totals overlap the original aggregate SQL timers and must not be added again
when estimating worker occupancy. Metrics observe work; they grant no authority.

## Validation method

Regression tests first failed against serial admission and separate directory
barriers. The owning-boundary suite covers independent progress, capacity,
same-object exclusion, quota exhaustion, GC exclusion, panic wakeup, shared
barrier failure, late-created names, an unfinished peer file sync, reversed
object order, and abrupt exit before the final batched image namespace proof.
Existing corruption, verification race, restart and retention tests remain intact.
The host fixture exercises actual staging and commit while unrelated inventory
I/O is blocked. Public SDK extent tests cover lifecycle/restart behavior.

The benchmark uses one source-verified immutable binary for all four controls:
serial or parallel admission, each with directory batching disabled or enabled.
Fixed-operation windows perform identical command counts and produce identical
logical state sizes. Fresh stores isolate each case. The original timed
concurrency-one/ten matrix checks workload behavior separately. Raw command
ledgers, receipts, replica audits, archive coverage and cgroups are independently
reconciled before accepting a run.

## Intermediate directory-only batching measurement

The first implementation batched namespace barriers but still synced each data
file before writing root metadata. Its image was
`sha256:5594731e98527758718369465df3996acebb92bde24a19a3adddd21d2606d3ab`.
One fixed-work comparison measured 42.22 writes/s for serial publication,
58.10/s for parallel only, 52.8/s for batch only, and 45.33/s for both.
Parallel-only and batch-only reversed-order runs also completed. Two subsequent
combined runs failed under load, retaining seven ambiguous and three unavailable
outcomes in one run and eight failures in another. Neither had an OOM or a
quarantined inventory; they shut down cleanly. These histories remain failed,
are not retried, and do not qualify the combined implementation.

A separate traced diagnostic completed. It observed simultaneous kernel fsync
waits across worker and Raft-storage threads (up to 137 ms on the observed node);
a following untraced window reached 79.88 writes/s. Other checkout formatting and
host indexing activity were observed on the shared machine. Host contention is
a plausible contributor, not a proven explanation of the failed runs. The traced
history is excluded from throughput comparisons.

Code inspection exposed a separate avoidable persistence ordering: root metadata
was written after the first data-file sync. A regression requiring the complete
available cohort before the first file barrier failed on that implementation.
The revised path writes the available cohort first, then syncs and independently
reverifies every owned file before its namespace proof. It retains at most eight
pending object descriptors plus root metadata per cohort; large images and
competing dependencies flush partial cohorts safely. Post-sync data/root
corruption must still refuse sealing. The revised implementation requires fresh
qualification; earlier results are not evidence that it improves throughput.



## Final build and qualification

Final image: `sha256:5c3c445e0163fd5ec68ecac10098d913b2aca8847db55eefea731a4812e7e361`.
Raft executable SHA-256: `d9a64bca404782eed6cd6a75ef1f29ede5de9c1fce39c97ce542629621b27296`.
Canonical complete source-manifest digest: `a70a72b8aff864360b194cb185ac0fbe95ceb7e81d3c06d0fde6212f68496415`.

The image matches the complete checkout source manifest; every deployed executable
matches its recorded hash. The base revision remains
`1be27d234d4674d4d014785c60cecbfe394bdab9`; changes are uncommitted.

The final build passes **152 LTX tests**, **287 Raft library tests**, and **three
public SDK extent tests**: **442 passing tests**, zero failures. Seven LTX and two
Raft entries remain ignored; process-isolation helpers are invoked by their parent
tests. The physical 10-GB qualification is not run. Clippy passes for both
affected crates with library/tests selected and warnings denied for Cellar code.
Formatting, module layout, native dependencies, documentation links and diff
whitespace checks pass. `cellar_internal_tests` is not enabled.

The SDK test build omits unrelated binary target declarations in a temporary
Docker build manifest, leaving the library, dependencies, features and test source
unchanged. It restores the original manifest afterward. Benchmark executables and
source manifests are built from the complete original workspace manifests.

## Matched-work results

Each completed fixed-work history has 30 warmup writes, 1,500 measured writes and
1,500 measured reads. All six completed histories have exactly the same final
per-cell command sequences and business state. Configurations are repeated in
reversed order; failed attempts remain failed and are excluded from completed-
history averages. This is a conditional comparison, not an error-free SLO result.

| Publication capacity | Batching | Complete / attempted histories | Writes/s, completed histories | Loaded-window range |
| ---: | --- | ---: | ---: | ---: |
| 1 | off | 1 / 2 | 34.15 | 26.11–42.69 |
| 2 | off | 1 / 2 | 56.78 | 50.74–64.23 |
| 1 | on | 2 / 2 | 46.56 | 32.18–55.47 |
| 2 | on | 2 / 2 | 59.24 | 45.06–86.32 |

Both settings together observe 73.5% higher completed-history throughput than the serial
control. Only one of two serial histories completed, so this percentage has
limited precision. Parallelism and batching are not additive: parallel-only
reaches 56.78/s and adding batching reaches 59.24/s in these completed histories.

## Separate leader and follower timers

Values below are count-weighted exclusive wall milliseconds per request, from
loaded fixed-work windows. Each cell is **serial → combined**. Leader preparation
uses the matching locally submitted preview; follower preparation replays LTX.
These are worker durations, not an additive majority-acknowledgement timeline.
Whole-operation timers also include small role/metrics bookkeeping overhead.
Inventory wait includes admission/key acquisition and uncontended bookkeeping;
these are wall times, not CPU times.

| Phase | Leader staging | Leader preparation | Follower preparation |
| --- | ---: | ---: | ---: |
| Whole operation | 54.742 → 30.537 | 0.036 → 0.038 | 54.655 → 29.018 |
| Inventory wait | 26.251 → 2.729 | 0.000 → 0.000 | 28.354 → 1.846 |
| SQL | 0.212 → 0.223 | 0.000 → 0.000 | 0.000 → 0.000 |
| LTX decode | 0.023 → 0.022 | 0.000 → 0.000 | 0.022 → 0.021 |
| Extent read | 0.409 → 0.352 | 0.000 → 0.000 | 0.405 → 0.262 |
| Extent hash | 0.171 → 0.261 | 0.000 → 0.000 | 0.170 → 0.238 |
| Extent write | 0.038 → 0.045 | 0.000 → 0.000 | 0.037 → 0.044 |
| File sync | 12.268 → 17.845 | 0.000 → 0.000 | 12.587 → 18.825 |
| Directory sync | 11.061 → 3.298 | 0.000 → 0.000 | 11.055 → 4.309 |
| Shared-barrier wait | 0.000 → 2.081 | 0.000 → 0.000 | 0.000 → 2.378 |
| Other | 4.309 → 3.680 | 0.032 → 0.034 | 2.023 → 1.092 |

| Phase | Leader publication | Follower publication |
| --- | ---: | ---: |
| Whole operation | 0.828 → 0.610 | 0.367 → 0.120 |
| Inventory wait | 0.269 → 0.014 | 0.329 → 0.080 |
| SQL | 0.167 → 0.176 | 0.000 → 0.000 |
| LTX decode | 0.000 → 0.000 | 0.000 → 0.000 |
| Extent read | 0.128 → 0.142 | 0.000 → 0.001 |
| Extent hash | 0.232 → 0.246 | 0.000 → 0.001 |
| Extent write | 0.000 → 0.000 | 0.000 → 0.000 |
| File sync | 0.000 → 0.000 | 0.000 → 0.000 |
| Directory sync | 0.000 → 0.000 | 0.000 → 0.000 |
| Shared-barrier wait | 0.000 → 0.000 | 0.000 → 0.000 |
| Other | 0.031 → 0.031 | 0.037 → 0.037 |

SQL transaction/preflight/result work is about 0.2 ms in leader staging;
LTX decoding and extent hashing are also small. Inventory waiting and persistence
explain the tens of milliseconds. Parallel publication removes most inventory
waiting. Batching reduces directory work, although individual file-sync wall
times can grow under concurrent storage pressure. Extra post-sync hashing is
retained deliberately; preliminary read proofs cannot substitute for it.

Publication is already below one millisecond in both controls: both include the
previous preview-transfer/metadata-retention optimization. Its earlier reduction
from about 10 ms was real, but it left staging and follower preparation as the
larger bottlenecks. Zero publication file-sync time does not mean writes skip
barriers: image persistence happened during preparation, and the Raft durable
applied marker is checked before publication. Engine sync has separate timers.

| Primitive, summed across three replicas per acknowledged write | Serial | Combined |
| --- | ---: | ---: |
| File sync calls | 6.001 | 5.997 |
| Directory sync calls | 6.001 | 1.475 |

Small deviations from integral counts reflect snapshot boundaries and follower
work continuing after a majority acknowledgement. Every accepted history still
passes final state checks on every replica. Directory sync calls fall about
75.4%; required file syncs remain approximately six per write. Six is specific to
this workload: one new data extent plus root metadata on each replica; changing
more extents requires more file barriers.

## Original timed workload matrix

The original two ten-second write windows at concurrency one and ten, with
corresponding two-second read windows, also complete for serial and combined
configurations. Their faster variants perform more writes and accumulate more
history; the fixed-operation experiment above controls that difference.

| Concurrency | Serial writes/s | Combined writes/s | Change | Serial p99 range | Combined p99 range |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 19.00 | 28.48 | +49.9% | 204.1–206.9 ms | 53.5–91.9 ms |
| 10 | 38.90 | 65.05 | +67.2% | 493.7–832.2 ms | 476.1–531.1 ms |

The parallel-only timed attempt fails during its second loaded write window;
its partial throughput is not accepted as a complete comparison.

## Failed attempts and practical limits

The original final-image matrix has three failed attempts, separate from its
eight completed histories. No failed mutation is retried. Their unresolved identities and input
ledgers remain preserved:

| Attempt | Ambiguous outcomes | Definite refusals |
| --- | ---: | ---: |
| `cohort-fixed-parallel` | 8 | 1 |
| `cohort-fixed-serial-r2` | 4 | 0 |
| `cohort-timed-parallel` | 6 | 0 |

The failed parallel-only fixed-work attempt records approximately 45% full
guest I/O pressure over the preceding ten seconds. Subsequent read-only status
queries succeed on all 30 replicas; the ten leaders are command-ready, archive
readiness is current, and inventories are not quarantined. Nodes shut down with
zero exit codes and no OOM. This supports transient deadline/admission trouble
under storage pressure rather than a permanent lockup; it does not resolve the
ambiguous commands or prove the cause of each timeout. The failed serial control
also prevents attributing all such failures to parallel publication or batching.

The accepted serial/combined timed matrices and both final combined fixed-work
histories have no failed requests. Nevertheless, this shared-host benchmark is
**not an error-free capacity/SLO qualification**. It is not a production soak,
cross-machine failure-domain test, S3 recovery drill, or 1-GiB-state qualification.
It uses initially empty cells with growing receipt/business state. No conclusion
is drawn about large-state or materialized-backend performance.

The 8 accepted final-image histories in that matrix independently reconcile **73,245 ledger records**
and **480 replica state audits**, native receipts and archive coverage, with
clean shutdown and no OOM. The highest accepted node cgroup peak is **77.4 MiB**,
under the unchanged 3-GiB cap. This is measured peak usage, not a larger-state
memory forecast.

## Reproduction and retained evidence

All runs use Docker context `colima-dense-2446`, native guest ext4 data under
`/mnt/cellar-dense-native-2446`, three node containers pinned to virtual CPUs
0/1/2 with one CPU and 3 GiB each and no additional swap, and pinned RustFS
`ghcr.io/rustfs/rustfs@sha256:bffcab0c9d647aab0055d1c69d340b202d0909966b385932d4ead1aeb7602858`
on CPUs 3/4 with two CPUs and 4 GiB. Ten empty-start cells, 1-KiB writes, two SQL
workers per node, 25-ms host polling, 1000-ms archive checkpoints, and a 1152-MiB
extent image quota are unchanged. Actual container caps and options are asserted
by the harness; no worker count or durability acknowledgement contract changes.

The external experiment directory is:

`/Volumes/Workspace/crabbuild-target/cellar-2446b3c0-dense`

Its retained entry points are `run_parallel_matrix.py`,
`run_cohort_remaining.py`, `profile_fixed_work.py`, `profile_quorum.py`,
`analyze.py` and `analyze_parallel.py`. Controls set
`RAFT_EXTENT_PUBLICATIONS=1|2` and `RAFT_EXTENT_BATCH_BARRIERS=false|true`.
Every experiment uses a fresh data directory and explicit non-overlapping subnet.

Final machine-readable results are `cohort-pooled.json`,
`cohort-fixed-comparison.json`, `cohort-timed-comparison.json`,
`cohort-qualification.json` and `cohort-remaining-status.json`. Raw reports,
operation ledgers, phase snapshots, cgroups, audits, kernel failure diagnostics
and container logs remain in each `cohort-*` experiment directory.

Final build evidence is `cohort-final-build-v2.log` and
`parallel-final-v3.Dockerfile`; the initial ordering regression is
`cohort-red.log`. The older `parallel-*` artifacts retain all intermediate
directory-only-batching results, failures and the syscall diagnostic. Failed
histories are never replaced by later successful histories.

## Latency budget follow-up

The remaining question is whether leader staging and follower preparation can
fit below 10 ms. This follow-up changes no production code. It re-examines the
accepted application traces and calibrates persistence separately before making
another storage change.

In the existing final-image timed experiments, the two concurrency-one windows
have leader staging means of **12.00/15.70 ms** and follower preparation means of
**10.90/11.47 ms**. At concurrency ten in those same experiments, the means are
**27.62/29.54 ms** and **27.00/25.05 ms**, respectively. The earlier 30.54/29.02 ms
figures summarize separate fixed-operation loaded histories. These are different
load/state samples, not a fixed compute cost per image.

The concurrency-one staging windows spend 6.92/7.76 ms in file sync and
2.43/2.24 ms in directory sync. Follower file sync takes 7.43/7.77 ms and
directory sync 2.77/2.83 ms. Inventory waiting is almost absent at that load.
SQL work is approximately 0.2 ms; the current small-state per-stage bottleneck
is persistence and its coordination, not tens of milliseconds of SQL execution.

All three replica containers and RustFS share the same guest ext4 filesystem
and physical workspace SSD. Per-container CPU/memory limits do not reserve
independent disk bandwidth or latency. Concurrent Raft-log, image and object-
store persistence can therefore inflate each sync call's wall time.

### Storage-only probes

The probes use 64-KiB data plus 256-byte root metadata, Rust 1.98.1, one-vCPU/
3-GiB container caps and the same ext4 data disk. Every sample checks destination
bytes after persistence. SQL, LTX, native marker persistence, RPCs, real RustFS,
and Cellar recovery/GC are absent. The loaded ordering probe runs three containers
concurrently, each with two image-writing threads and one 4-KiB append/fsync log
writer. It approximates storage contention; it is not an application benchmark.

Paired tests alternate data-first and root-first file-sync ordering while keeping
both file barriers and the directory barrier. Results:

| Probe | Data-first median | Root-first median | Data-first p99 | Root-first p99 |
| --- | ---: | ---: | ---: | ---: |
| Single image writer | 2.34 ms | 2.37 ms | 6.96 ms | 7.79 ms |
| Three nodes with background log writers | 14.33 ms | 14.95 ms | 22.50 ms | 25.43 ms |

There are 80 samples per ordering for the single writer and 480 per ordering
for the loaded probe. Root-first ordering provides no measured latency gain and
is not adopted. The loaded current-pattern median exceeds 10 ms before Cellar
executes SQL, LTX processing or native apply/publication logic.

A separate layout/API calibration observed medians of 6.28 ms for two new files
with fsync+directory persistence, 7.11 ms using fdatasync for the files, 3.33 ms
for one combined file plus directory persistence, and 3.14 ms for an existing
append-only file with one fdatasync. Host linking activity and large I/O stalls
occurred during that calibration; its first two-file p99 exceeded three seconds.
These observations are not stable hardware floors or evidence of an implemented
packed backend. In particular, fdatasync did not establish a consistent median
improvement, so the production sync API is unchanged.

The [Linux fsync contract](https://man7.org/linux/man-pages/man2/fsync.2.html)
distinguishes file persistence from directory-entry persistence; fdatasync still
flushes metadata needed to retrieve data, including changed file size. Removing
barriers without replacing their proofs would weaken the current durability
contract.

### Next meaningful latency optimization

For small cells, below 10 ms is a plausible per-stage median target with healthy
storage. It is not an established mean or p99 guarantee at the tested load. The
next substantial opportunity is to put changed extent data and root commit
metadata into one durable record/container, and eventually group commits in an
already-created segment. This could reduce two per-image file barriers to one
and amortize namespace persistence over segment creation/rotation.

Such a backend must preserve exact object/root exclusion, byte/file quota
accounting, pinned-reader and GC protection, post-sync destination verification,
and recovery from torn/unacknowledged records. Durability proofs must be granted
only for completely written records covered by a successful barrier. Reusing the
existing Raft log instead would additionally require explicit checkpoint/log-
retention and recovery changes; removing per-write image syncs alone is not a
qualified replacement. End-to-end acknowledgement latency must be measured with
each stage so moving persistence later cannot masquerade as a latency win.

This follow-up has not implemented that storage format and does not claim
sub-10-ms durable stages. The current production implementation remains the
source-verified, 442-test build documented above.

Raw probe sources/results in the same external experiment directory are
`sync-calibration.rs`, `sync-order-calibration.rs`, `sync-unloaded.csv`,
`sync-order-unloaded.csv`, `sync-order-loaded-{0,1,2}.csv`,
`sync-order-loaded-containers.json`, `analyze_sync_latency.py`, and
`sync-latency-analysis.json`. No prior benchmark histories are overwritten.

### Ext4 journal batching experiment

The guest runs Linux `6.8.0-117-generic`. Its actual ext4 options report
`max_batch_time=15000`, `min_batch_time=0`, `data=ordered`, and `barrier`.
The [kernel ext4 documentation](https://www.kernel.org/doc/html/v6.6/admin-guide/ext4.html)
describes adaptive waiting before synchronous journal commits, capped by
`max_batch_time`; zero disables that waiting policy. This is a testable latency
candidate, not a reason to weaken file/directory persistence.

The same loaded storage probe is repeated with default/zero/default/zero
batching caps. All other ext4 options are asserted identical. Each row contains
960 checked image-write samples, including both file-sync orderings:

| Run | max_batch_time | Mean | Median | p95 | p99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| default-a | 15000 µs | 11.66 ms | 11.62 ms | 15.21 ms | 17.18 ms |
| nobatch-a | 0 µs | 12.84 ms | 12.71 ms | 17.21 ms | 19.52 ms |
| default-b | 15000 µs | 10.19 ms | 9.96 ms | 13.57 ms | 18.46 ms |
| nobatch-b | 0 µs | 11.96 ms | 11.13 ms | 18.30 ms | 34.47 ms |

Disabling the waiting policy makes this concurrent probe slower in both
repetitions. The intentional delay is therefore not established as the primary
cause of the observed latency; batching can reduce competing commit pressure.

A subsequent actual Cellar/RustFS comparison uses the same source-verified image,
CPU/memory caps, ten cells, payload sizes, two SQL workers, and two c1/c10 timed
windows. The new default-policy attempt fails during its first c1 write window
with one unresolved request outcome. It remains a failed history and is not
retried. The zero-delay attempt completes and independently reconciles all
receipts, replica state and archive coverage. Since the fresh baseline failed,
its throughput is not treated as a matched improvement claim. This follow-up adds
one accepted history and one failed baseline beyond the earlier matrix counts.

| Zero-delay application window | Leader staging mean | Follower preparation mean |
| --- | ---: | ---: |
| c1, repeat 0 | 11.89 ms | 11.28 ms |
| c1, repeat 1 | 11.72 ms | 6.71 ms |
| c10, repeat 0 | 26.28 ms | 23.68 ms |
| c10, repeat 1 | 24.15 ms | 19.78 ms |

In that trial, 41.7% of c1 staging requests and 70.1% of c1 follower preparations
fall at or below the histogram's 10-ms boundary. At c10 the proportions are 0.2%
and 1.7%. This does not establish a consistent sub-10-ms path for both stages.
Neither the sync API, file-sync ordering, nor the ext4 batching cap is changed
in the production implementation or recommended as a proven fix from these
experiments. The original ext4 options are restored and checked exactly.

The three-node data-first probe breaks its 14.77-ms mean into approximately
0.28 ms writing, 5.50 ms data-file sync, 5.58 ms root-file sync, and 3.42 ms
directory sync. These separate persistence rounds explain why a shared durable
record/append segment is the next substantial candidate. It remains an
architectural hypothesis requiring implementation and recovery/GC qualification,
not an achieved latency improvement. A per-stage latency target must also be
distinguished from client acknowledgement latency, which includes Raft and queueing.

Additional retained evidence: `run_ext4_batch_probe.py`, `ext4-batch-probe.json`,
`sync-order-{default-a,nobatch-a,default-b,nobatch-b}-{0,1,2}.csv`,
`run_native_ext4_latency.py`, `latency-native-ext4-results.json`, the
`latency-native-{default,no-batch-wait}` directories, and
`latency-native-ext4-restored.txt`. Failed and successful trials remain separate.
