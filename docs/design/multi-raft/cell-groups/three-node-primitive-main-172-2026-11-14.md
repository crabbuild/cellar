<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Main-172 three-node primitive verification

Purpose: Retain the complete frozen main-based control/candidate comparison and its failing correctness boundaries.

Machine-readable qualification and benchmark outputs are retained separately.
Keep generated run outputs outside this documentation tree; the prose summaries
preserve their source, scope and unresolved failures.

**Result: FAILED. No isolated throughput or latency improvement is established.** The candidate completes five of twelve API/mode cases. The control completes one of three quorum cases. None of the three APIs has two complete passing quorum cases for an optimization ratio.

Compiler/runtime source is `95244b42e679fa81bda0f00cbb59e55a89e3236a` (manifest `f8917ffe8b6760a04dfeb93b128988ac467315cb386fd3f1ff5c4e913391867e`), including main `0e925f6b` (#171/#172). The control reverses only the nine-file image-publication directory-barrier change `97b805f3`; its manifest is `354f48f25c264f23456f9862ee53a5870aec3aa0eed8f22abfd546474f3c7559`. Compiler profiles and load-client bytes match. Both images use locked Rust 1.98.1, size-optimized `lab`, thin LTO and line-table symbols.

The topology has three separate 1-vCPU / 2-GiB / no-extra-swap nodes, three RF3 groups, separately capped RustFS and client containers, 24 SQL/KV cells, and 24 queues plus 24 DLQs. Each case requests C1/4/16, two reversed repetitions, 5-second warmups and 30-second timed operations. Actor operation and lease bounds remain five seconds.

## Complete matrix

V/F/U counts verified, failed and unmeasured windows. Failed or unmeasured windows supply no qualified rate. SQL/KV have separate write/read windows; Queue has enqueue windows and bounded verification/drain.

| Campaign | API | Mode | Result | Timed V/F/U | Warmup V/F/U | Request errors |
| --- | --- | --- | --- | --- | --- | --- |
| control | sql | raft-quorum | PASSED | 12/0/0 | 6/0/0 | 0 |
| control | kv | raft-quorum | FAILED | 11/1/0 | 6/0/0 | 1 |
| control | queue | raft-quorum | FAILED | 0/0/6 | 0/1/5 | 0 |
| candidate | sql | raft-quorum | FAILED | 0/1/11 | 1/0/5 | 1 |
| candidate | sql | raft-s3 | FAILED | 7/1/4 | 4/0/2 | 5 |
| candidate | sql | fleet | PASSED | 12/0/0 | 6/0/0 | 0 |
| candidate | sql | bucket | PASSED | 12/0/0 | 6/0/0 | 0 |
| candidate | kv | raft-quorum | FAILED | 4/1/7 | 3/0/3 | 16 |
| candidate | kv | raft-s3 | PASSED | 12/0/0 | 6/0/0 | 0 |
| candidate | kv | fleet | PASSED | 12/0/0 | 6/0/0 | 0 |
| candidate | kv | bucket | PASSED | 12/0/0 | 6/0/0 | 0 |
| candidate | queue | raft-quorum | FAILED | 1/0/5 | 1/1/4 | 4 |
| candidate | queue | raft-s3 | FAILED | 2/1/3 | 3/0/3 | 12 |
| candidate | queue | fleet | FAILED | 4/1/1 | 5/0/1 | 4 |
| candidate | queue | bucket | FAILED | 0/1/5 | 1/0/5 | 1 |

The candidate has **78 verified, 6 failed and 36 unmeasured timed windows**, with 43 request errors. All four Queue cases fail. Passing small-value windows do not accept a complete primitive family, its operators, large values, lifecycle or recovery.

## Descriptive C1 observations

Only complete passing cases appear below. Rates combine successful requests divided by total elapsed time across the two repetitions; latency percentiles use their pooled raw samples. These are shared-host observations, not maximum capacity or equivalent-guarantee cross-mode speedups. The unbatched control SQL quorum case is separate from the failing candidate quorum case.

| Campaign | API | Mode | Operation | Requests/s | p50 ms | p95 ms | p99 ms |
| --- | --- | --- | --- | --- | --- | --- | --- |
| control | sql | raft-quorum | write | 14.02 | 62.39 | 116.38 | 285.27 |
| control | sql | raft-quorum | read | 13.69 | 62.52 | 110.86 | 249.91 |
| candidate | sql | fleet | write | 113.47 | 6.10 | 15.06 | 66.00 |
| candidate | sql | fleet | read | 1330.89 | 0.62 | 1.55 | 2.55 |
| candidate | sql | bucket | write | 71.00 | 5.99 | 38.36 | 84.63 |
| candidate | sql | bucket | read | 1440.27 | 0.61 | 0.89 | 2.43 |
| candidate | kv | raft-s3 | write | 5.27 | 107.46 | 631.65 | 1110.86 |
| candidate | kv | raft-s3 | read | 6.48 | 117.50 | 280.27 | 1263.15 |
| candidate | kv | fleet | write | 132.63 | 5.77 | 15.78 | 26.32 |
| candidate | kv | fleet | read | 1412.43 | 0.53 | 1.86 | 2.96 |
| candidate | kv | bucket | write | 121.31 | 4.63 | 26.27 | 59.48 |
| candidate | kv | bucket | read | 841.84 | 0.77 | 2.90 | 4.40 |

Fleet may acknowledge peer replication or a bucket proof; bucket, quorum and quorum-plus-S3 have different acknowledgement and read contracts. Closed-loop completion latency is measured. No Queue rate is reported from a failed case.

## Correctness and failure boundaries

The frozen native campaign records **1,508 passed, 15 failed and 107 explicitly ignored** across eight libtest targets; the separate production daemon oracle passes. The partial transition attempt interrupted by host restart is excluded. The remaining three targets use the same hash-verified executables and immutable compiler inputs; all owners join before timing starts.

Failures include Axum SQL/KV, activation/source-proof/name discovery and Queue deadline controls, three synchronous EventStream journal capability controls, removed-member counter and superseded-boot readiness controls, and two transition recovery controls. The removed-member assertion fails after its unchanged callback/SQL-stage guard; it does not establish unauthorized execution. The superseded-boot failure is in readiness setup.

The candidate SQL quorum attempt fails with an unconfirmed write and a readback mismatch. Its raw verifier does not identify the mismatched key/value. An unconfirmed write can supersede an earlier acknowledged value at the same key; the mismatch alone establishes neither corruption nor acknowledged data loss. No fresh-ID retry is issued.

The failed candidate SQL snapshot records no sampled CPU throttling, OOM, restart or image quarantine. Node CPU averages are about 0.18–0.21 cores during its sampled interval. Completed sync/preparation histograms contain long tails, including a node-1 file-sync sample above one second and at most five seconds. Aggregated counters do not identify the failed command's critical path or prove a cause.

The original Queue verifier can omit producer IDs when a delivery or metrics probe fails before its final update. A separate derived ledger recovers the 152 expected message IDs in the control's C1 warmup from nineteen acknowledgements, no request errors and the hash-bound deterministic client. It reconstructs no delivery or settlement observations. Missing concurrent-worker evidence remains missing; failed windows stay failed. The [verifier follow-up](../../../../tools/perf/primitive_compare.md) records producer and partial probe ledgers before those failure points.

Failed native disks from the pre-restart campaign are unavailable after host restart. The resumed automatic-transition fixture also drops temporary directories on its error path; its failure logs survive, but its disks do not. Failed-disk retention remains a separate harness gap.

## Measurement and cleanup limits

A host process snapshot observes Cargo/Rust compiler activity during candidate timing; the owner and working directory are unresolved. Earlier queued validation also repeatedly hashes host source before that guard is removed. These observations cannot attribute earlier failures or support an isolated causal speedup. A controlled repeat and command-specific progress diagnosis remain required.

All 75 measured containers stop; no OOM or restart is observed. Failed volumes and container identities remain retained. The original RustFS identity, limits and data are preserved and readiness returns HTTP 200. One Colima VM supplies one physical failure domain; this does not qualify cloud S3, partitions, all-voter disk loss or sustained production capacity.

Machine assessment and raw capsule retain source manifests/patches, all original window and load-log data, resources, failure snapshots and native log hashes. Runtime credentials/configuration and node/store log text are excluded; actual generated credentials are checked against the emitted capsule.

Full OPT-01–08, CG-17 and release acceptance remain open. See the [prioritized gaps](optimization-progress.md#priorities-for-completing-the-optimization).
