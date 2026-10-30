<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Quorum versus Fleet throughput diagnosis — 2026-10-29

Purpose: Explain the measured Quorum versus Fleet write gap using leader and follower image-persistence stages.

Status: **diagnosis complete for the baseline**. The subsequent
[image-pipeline implementation and validation](quorum-image-optimization-2026-10-29.md)
records the publication improvement, remaining staging/preparation limits and
small-state configuration comparison. No production fix was present during the
diagnostic measurements below.

Quorum uses a majority and demonstrably continues accepting writes with its
third voter paused. The observed throughput gap is dominated by the local
SQL/image persistence pipeline, rather than waiting for the slowest replica.
The empty-state benchmark retained the extent-backed configuration originally
selected for one-GiB cells; that configuration adds substantial image work to
each small command. A materialized small-cell configuration performed materially
better, with the same Raft majority contract.

## Reproduction and controls

Production revision: `1be27d234d4674d4d014785c60cecbfe394bdab9`.
Three containers each retain one pinned vCPU, 3 GiB of memory and no extra swap;
RustFS retains two vCPUs and 4 GiB. They share the same Linux/arm64 VM and ext4
virtual disk on one workspace SSD. Ten fresh cells, zero bulk padding and one-KiB
writes reproduce the [empty-state workload](empty-state-durability-2026-10-29.md).
No mutation is retried. Both application state and durability acknowledgements
are checked; failed/ambiguous attempts remain separate.

The first profiles use the exact original executable/image. Subsequent runs use
a benchmark-only build exposing the existing SDK options `poll_ms` and
`archive_checkpoint_ms`. Their defaults remain 25 and 1000 ms. Actual node
resources and settings are checked before traffic. Production crates are
unchanged. Builds finish before measured traffic.

The repeated diagnostic runs measure two ten-second write windows at concurrency
1 and 10; their intervening read windows are two seconds. Each window has
before/after snapshots from the existing `/metrics` endpoint. Sources must match,
and unavailable/saturated metrics are refused. Timings are completed-operation
wall times and include blocking; they are not CPU samples. Sums across replicas
measure worker occupancy, not one command's critical-path latency.

## Majority and Fleet proofs

A one-cell experiment ran three equal three-second write windows. The third
voter was paused only during the middle window. Throughput was 26.95, **26.19**
and 28.21 writes/s, with zero request errors. New command identities and
acknowledged revisions advanced while that process could not respond. All six
before/after state audits passed across the three replicas after it resumed;
archive coverage and clean process exits also passed. Quorum is not secretly
waiting for all three voters.

An earlier ten-second pause exceeded the Raft fixture's five-second node lease.
Writes still continued on the other two voters, but node 3 correctly self-fenced.
The final audit succeeded on nodes 1 and 2 and failed on node 3. Its generic
`acknowledged state lost` harness message conflates a failed read with a state
mismatch; the retained evidence does not establish data loss. That experiment
is retained as incomplete, not a successful recovery qualification.

A separate Fleet run enabled diagnostic logs. Every node had a two-follower
ensemble at the start of each timed write window. Logged proof-wait events were:

| Write window | Fleet proof events | Bucket proof events |
| --- | ---: | ---: |
| Concurrency 1 | 457 | 0 |
| Concurrency 10 | 639 | 9 |

These events are not joined one-to-one with client command IDs and must not be
reported as exact per-response percentages. They show that follower proofs,
with both selected followers, dominate the sampled Fleet path. The gap cannot
be explained simply by Fleet using only one follower or always returning on S3.
The debug-logging run is evidence about proof selection, not a clean capacity
comparison.

## Measured bottleneck

In the repeated default extent-backed control, concurrency-10 writes achieved
44.07–47.27/s. The first window consumed **59.75 SQL-worker seconds**, using
98.2% of the six available worker slots across three nodes. The second used
97.3%. Per-operation means for the first window were:

| Operation | Mean wall time |
| --- | ---: |
| SQL stage, once per command | 41.24 ms |
| SQL prepare, on each replica | 21.09 ms |
| SQL publish, on each replica | 9.72 ms |
| SQL queue | 33.94 ms |
| Raft engine file sync | 4.27 ms |
| Transport control exchange | 0.21 ms |
| Transport data exchange | 0.24 ms |

Transport exchange timers measure RPC/admission work, not the complete interval
from proposal to majority commitment. Replica log persistence remains part of
Raft's commit path and is measured separately; no sub-millisecond end-to-end
quorum acknowledgement is claimed.

Stage + three prepares + three publishes total about **134 worker-ms per write**.
Six worker slots divided by that occupancy predict about 45 writes/s, matching
the observed 44.07/s. Queue time is separate and is not added to occupied-worker
time. Operation boundary effects and background work limit exact attribution,
but the capacity match and repeated saturation identify the dominant path.

Doubling the SQL pool to four workers per node reached 44.88–49.13 writes/s.
Stage/prepare/publish durations grew while queue time fell. Extra workers mostly
moved waiting into the operations, rather than increasing storage throughput.
This is consistent with the node-wide inventory's serialized physical-operation
lane and its shared filesystem barriers, rather than a simple thread-count
shortage. The test does not apportion every blocked millisecond between inventory
admission and filesystem/kernel wait.

## Code path responsible

The extent-backed leader performs additional work before and after consensus:

1. `PrivateExecutor::freeze` captures the SQL mutation and invokes
   `Images::preview`, which independently prepares and durably seals the new
   extent image before proposal submission.
2. After consensus commits, `RootImageApplier::prepare` independently replays
   the committed update through `prepare_image_root`, including on the leader
   that already holds a prepared preview. The materialized adapter has a
   `prepare_private` path that can reuse the private result.
3. Preparing a new extent/root writes and syncs immutable files and their
   directory entries. Re-staging an existing root also calls `read_root_synced`,
   syncing the root and directory again.
4. Physical inventory operations serialize node-wide. Root-reference retention
   and retirement also enter this lane, so SQL publication can wait behind
   another cell's physical work. A metadata-only `retain_image_reference` API
   already exists, but these publication paths still use `retain_reference`.

Relevant source: [private freeze](../../crates/cellar/durability/raft/execution/mod.rs),
[preview preparation](../../crates/cellar/durability/raft/execution/images.rs),
[apply dispatch](../../crates/cellar/durability/raft/host_sql/images.rs),
[rooted apply/publication](../../crates/cellar/durability/raft/rooted/mod.rs),
[inventory serialization](../../crates/cellar-ltx/src/extent/store/mod.rs),
[extent/root persistence](../../crates/cellar-ltx/src/extent/store/publication.rs),
and [reference APIs](../../crates/cellar-ltx/src/extent/store/reclaim.rs).

A separate syscall trace of one leader recorded approximately **469 fsync calls
while 31 writes were acknowledged**: 31 extent-data syncs, 151 root-manifest syncs,
192 extent-directory syncs and 95 Raft-log syncs. Most observed root files were
synced five times. Counts include background work and approximate clock/window
boundaries. Tracing substantially perturbed execution, so its throughput and
latency are excluded from performance comparisons. It confirms persistence
amplification; it does not authorize removing required durability barriers.

Fleet follows a different pipeline: an authoritative SQLite owner captures
changes and ships node-level log batches through pipelined follower lanes.
Followers persist log fragments; a Fleet acknowledgement does not require the
same per-command immutable SQLite image preparation/publication on every voter.
Although Raft waits for only a majority, it still replicates/applies to the third
voter in the background. In this experiment those writes contend on the same
physical SSD. Saving a sub-millisecond peer response does not remove this work.

## Controlled option results

All rows below completed both write repetitions, final state audits, archive
coverage and clean process shutdown. These are observations, not statistically
controlled tuning recommendations.

| Quorum configuration | Concurrency-10 writes/s | Finding |
| --- | ---: | --- |
| Extents, 1152-MiB allowance, defaults | 44.07–47.27 | Baseline |
| Same, host poll 25 → 1 ms | 35.66–45.03 | No improvement |
| Same, archived-cut checkpoint interval 1000 → 0 | 37.82–39.07 | No improvement |
| Same, SQL workers 2 → 4 per node | 44.88–49.13 | No material improvement |
| Default materialized storage, 64-MiB allowance | 64.69–84.32 | Lower image cost |

The materialized path used about **64–82 worker-ms per write**, compared with
123–134 for the default extent control. SQL publication averaged 0.11 ms instead
of roughly 9–10 ms. The earlier same-binary backend comparison also completed:
39.69/s extent-backed versus 70.80/s materialized. Logical quotas and capacity
reservation policies differ between these storage configurations, so their
entire numerical difference cannot be assigned to a single internal call.

A supplementary extent-backed 64-MiB trial did not complete the matched-quota
comparison: its concurrency-10 window stalled in SQL/image and log-sync work and
returned six ambiguous command deadlines. There were no OOM kills. Those six
outcomes remain unresolved and quarantined; no mutation was retried. Its partial
4.44 successful writes/s is not a qualified throughput result, and the test does
not establish that the quota itself caused the stalls.

## Optimization targets and limits

The measured target is the immutable-image persistence pipeline, especially
repeated preparation/sealing and serialized metadata/physical work. Candidate
changes should reuse exact-owner sealed previews after verifying the committed
proposal/base, use capability-based metadata retention where valid, and coalesce
necessary filesystem barriers while preserving their exact proof boundaries.
Any implementation needs regression tests for corrupt/reopened images, identity
and epoch changes, publication under unrelated inventory work, cancellation and
recovery. Increasing threads, shortening polls or removing fsyncs indiscriminately
would not follow from this evidence. No production optimization is implemented
in this investigation.

All runs share one host, VM and disk. These profiles do not establish the same
ordering on three independent hosts, an intrinsic Raft performance limit, or
production capacity. The small-cell default configuration is faster here but
still below the earlier Fleet throughput range; fewer required acknowledgements
alone do not determine total write cost.

## Retained evidence and validation

Private root: `/Volumes/Workspace/crabbuild-target/cellar-2446b3c0-dense/`.
Diagnostic directories use `profile-*-2026-10-29/`; each retains executable/source
identities, raw ledgers, stage snapshots, audits, resource samples and exit states.
`diagnosis-analysis.json` and `analyze_diagnosis.py` retain the independent
reconciliation and worker-occupancy calculations. `proof-analysis.json` under
`profile-fleet-proof-2026-10-29/fleet/` retains the Fleet proof observations.
Do not publish credential-bearing `config/` directories.

The original diagnostic image is
`sha256:e247a427094f14954d56c66c437002f84139235b299e9f1c2e221e4ea8cd71ed`.
The benchmark-controls image is
`sha256:95376d4ee1306b229b9f9e73b2a3c91c30d6bd1374a07fd6d5eb71736295d76e`.
No source/executable verification was disabled. All successful runs independently
reconciled. The two incomplete runs retain independently checked acknowledged
prefixes; the 64-MiB extent case retains six unresolved commands. In total,
300,631 ledger records were checked across twelve experiment histories, ten
complete and two incomplete.

Passed: Linux/arm64 build and Clippy for both executables with `-D warnings`,
36 Python benchmark tests, Python syntax checks, Rust formatting and module
layout, native dependency and documentation checks. The fixture controls retain
production defaults. Final cleanup stops only the dedicated benchmark VM;
all diagnostic data and original dense-state data remain available.
