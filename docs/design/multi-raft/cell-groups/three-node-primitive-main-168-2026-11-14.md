<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Updated-main three-node primitive verification, 2026-11-14 UTC

Purpose: Record native checks and constrained SQL/KV/Queue comparisons after main #168, with failures and source bindings.

Machine-readable qualification and benchmark outputs are retained separately.
Keep generated run outputs outside this documentation tree; the prose summaries
preserve their source, scope and unresolved failures.

Origin/main `50b927d9` (#168) is integrated by merge `a16bbdc6`; the measured
source is published as `c5444fd2` on PR 167. The merged revision remains
**unqualified**: eight native primitive controls fail and ten of twelve local
comparison cases fail. KV Fleet and KV bucket complete their full matrices.

## Scope and result

All twelve SQL/KV/Queue × Raft quorum/Raft+S3/Fleet/bucket cases are attempted.
Of 120 planned timed windows, **49 verify, seven fail and 64 are unmeasured**.
Of 72 warmups, 34 verify, three fail and 35 are unmeasured. All 93 emitted
windows are retained, including 33 request errors.

A case stops at its first
failed window; later cases still run. No failed window contributes to the
verified-rate tables. These are local observations, not stable capacity or
isolated architecture speedup claims.

| API | Mode | Case | Verified timed / planned | Stop point |
| --- | --- | --- | ---: | --- |
| SQL | Raft quorum | FAILED | 4 / 12 | r1, c16, write |
| SQL | Raft + S3 | FAILED | 2 / 12 | r1, c4, warmup |
| SQL | Fleet | FAILED | 3 / 12 | r1, c4, read |
| SQL | Bucket | FAILED | 4 / 12 | r1, c16, warmup |
| KV | Raft quorum | FAILED | 5 / 12 | r1, c16, read |
| KV | Raft + S3 | FAILED | 0 / 12 | r1, c1, write |
| KV | Fleet | PASSED | 12 / 12 | complete |
| KV | Bucket | PASSED | 12 / 12 | complete |
| Queue | Raft quorum | FAILED | 1 / 6 | r1, c4, warmup |
| Queue | Raft + S3 | FAILED | 1 / 6 | r1, c4, enqueue |
| Queue | Fleet | FAILED | 3 / 6 | r2, c16, enqueue |
| Queue | Bucket | FAILED | 2 / 6 | r1, c16, enqueue |

Each case uses three separate containers at **1 vCPU / 2 GiB / no extra swap**,
plus separately capped one-vCPU/two-GiB RustFS and client containers. CPU sets
are nodes 0–2, store 3 and client 4. The owned Colima VM has six vCPUs and
about 14 GiB configured RAM; all participants share its physical host/storage.
This does not supply independent failure domains.

SQL/KV enroll 24 cells in three RF3 groups; Queue enrolls 24 queues plus 24
DLQs in three RF3 groups. Startup checks every replica population and balances
one serving leader per group across the three processes. Fleet/bucket receive
the same logical bindings and requests. Fleet acknowledges peer replication
**or bucket proof**; bucket acknowledges its bucket output gate. Public read
implementations and durability guarantees differ between modes.

All modes use concurrency 1/4/16, two repetitions in reverse concurrency order,
five-second warmups and 30-second timed operations. Values contain 256 ASCII
bytes; Queue batches contain eight messages. Five-second operation/lease/callback
budgets, one Raft SQL worker, eight private workspaces, image/manifest limits
and the bounded disk profile are unchanged. Lease tracing is disabled.

## Verified throughput and latency

The compact panel pools only verified one-client write/enqueue windows. Rates
use summed successful operations divided by summed actual elapsed time; p99
is recomputed from their concatenated raw samples.

KV Fleet/bucket have two
such windows each; other populated cells have one. “—” means no verified timed
window, not zero throughput. Each cell is **rate/s / p99 milliseconds**.

| Mode | SQL upserts | KV puts | Queue enqueued messages |
| --- | ---: | ---: | ---: |
| Raft quorum | 6.74 / 417 | 8.27 / 734 | 4.90 / 3322 |
| Raft + S3 | 5.06 / 537 | — | 12.69 / 1701 |
| Fleet | 54.00 / 76 | 66.85 / 77 | 59.15 / 586 |
| Bucket | 101.95 / 39 | 19.59 / 322 | 67.96 / 736 |

Queue p99 measures **batch enqueue acknowledgement**, not per-message delivery.
The complete verified windows follow. SQL/KV rates are successful requests/s;
Queue rates are enqueued messages/s. Latencies are successful HTTP operations
within each verified window. The JSON additionally retains Queue rates including
drain, attempted latencies, failures and resource observations.

| API | Mode | Rep | Clients | Operation | Rate/s | p50 ms | p95 ms | p99 ms |
| --- | --- | ---: | ---: | --- | ---: | ---: | ---: | ---: |
| SQL | Raft quorum | 1 | 1 | write | 6.74 | 129.61 | 301.43 | 416.72 |
| SQL | Raft quorum | 1 | 1 | read | 6.88 | 129.69 | 245.63 | 342.51 |
| SQL | Raft quorum | 1 | 4 | write | 11.61 | 267.51 | 909.32 | 1361.43 |
| SQL | Raft quorum | 1 | 4 | read | 9.51 | 305.90 | 791.10 | 3495.76 |
| SQL | Raft + S3 | 1 | 1 | write | 5.06 | 176.52 | 381.52 | 536.61 |
| SQL | Raft + S3 | 1 | 1 | read | 3.94 | 198.61 | 528.35 | 1244.67 |
| SQL | Fleet | 1 | 1 | write | 54.00 | 14.56 | 40.25 | 76.10 |
| SQL | Fleet | 1 | 1 | read | 411.28 | 1.10 | 6.87 | 14.66 |
| SQL | Fleet | 1 | 4 | write | 34.65 | 73.90 | 326.53 | 772.59 |
| SQL | Bucket | 1 | 1 | write | 101.95 | 7.70 | 23.12 | 39.10 |
| SQL | Bucket | 1 | 1 | read | 998.11 | 0.69 | 2.56 | 4.91 |
| SQL | Bucket | 1 | 4 | write | 168.77 | 15.89 | 55.01 | 216.81 |
| SQL | Bucket | 1 | 4 | read | 2491.92 | 1.03 | 4.27 | 8.52 |
| KV | Raft quorum | 1 | 1 | write | 8.27 | 93.16 | 246.96 | 733.81 |
| KV | Raft quorum | 1 | 1 | read | 6.83 | 107.30 | 322.45 | 770.88 |
| KV | Raft quorum | 1 | 4 | write | 6.80 | 336.67 | 1928.22 | 2663.09 |
| KV | Raft quorum | 1 | 4 | read | 11.31 | 296.87 | 774.61 | 1512.19 |
| KV | Raft quorum | 1 | 16 | write | 15.34 | 752.74 | 2704.61 | 4154.48 |
| KV | Fleet | 1 | 1 | write | 74.31 | 11.93 | 23.65 | 36.96 |
| KV | Fleet | 1 | 1 | read | 1078.73 | 0.76 | 1.76 | 3.91 |
| KV | Fleet | 1 | 4 | write | 130.50 | 27.42 | 54.26 | 80.77 |
| KV | Fleet | 1 | 4 | read | 2493.37 | 1.11 | 3.20 | 7.27 |
| KV | Fleet | 1 | 16 | write | 218.12 | 54.14 | 170.35 | 316.78 |
| KV | Fleet | 1 | 16 | read | 6790.70 | 1.86 | 4.90 | 9.66 |
| KV | Fleet | 2 | 16 | write | 188.66 | 57.61 | 210.60 | 482.13 |
| KV | Fleet | 2 | 16 | read | 7239.99 | 1.68 | 3.65 | 6.76 |
| KV | Fleet | 2 | 4 | write | 20.91 | 98.60 | 608.68 | 779.01 |
| KV | Fleet | 2 | 4 | read | 3745.64 | 0.79 | 1.52 | 4.18 |
| KV | Fleet | 2 | 1 | write | 59.40 | 12.15 | 32.62 | 155.47 |
| KV | Fleet | 2 | 1 | read | 1350.02 | 0.59 | 1.01 | 3.41 |
| KV | Bucket | 1 | 1 | write | 26.67 | 27.94 | 80.42 | 210.36 |
| KV | Bucket | 1 | 1 | read | 1297.95 | 0.62 | 1.03 | 2.66 |
| KV | Bucket | 1 | 4 | write | 75.87 | 41.95 | 112.17 | 354.33 |
| KV | Bucket | 1 | 4 | read | 4340.26 | 0.79 | 1.48 | 3.62 |
| KV | Bucket | 1 | 16 | write | 83.77 | 145.45 | 497.90 | 1181.54 |
| KV | Bucket | 1 | 16 | read | 7952.30 | 1.73 | 3.71 | 6.65 |
| KV | Bucket | 2 | 16 | write | 74.84 | 155.30 | 602.34 | 917.29 |
| KV | Bucket | 2 | 16 | read | 6006.55 | 2.22 | 5.68 | 9.74 |
| KV | Bucket | 2 | 4 | write | 72.46 | 44.13 | 122.86 | 444.92 |
| KV | Bucket | 2 | 4 | read | 3678.38 | 0.91 | 2.02 | 4.01 |
| KV | Bucket | 2 | 1 | write | 12.53 | 61.46 | 179.40 | 422.48 |
| KV | Bucket | 2 | 1 | read | 1172.05 | 0.72 | 1.30 | 3.21 |
| Queue | Raft quorum | 1 | 1 | enqueue | 4.90 | 1854.23 | 3308.61 | 3321.61 |
| Queue | Raft + S3 | 1 | 1 | enqueue | 12.69 | 472.97 | 1689.62 | 1701.17 |
| Queue | Fleet | 1 | 1 | enqueue | 59.15 | 105.04 | 387.24 | 585.66 |
| Queue | Fleet | 1 | 4 | enqueue | 70.82 | 359.90 | 1125.45 | 1308.06 |
| Queue | Fleet | 1 | 16 | enqueue | 102.34 | 908.11 | 3044.05 | 5151.06 |
| Queue | Bucket | 1 | 1 | enqueue | 67.96 | 89.23 | 271.96 | 735.70 |
| Queue | Bucket | 1 | 4 | enqueue | 171.82 | 135.21 | 422.85 | 1315.79 |

## Failures and uncertainty

SQL quorum verifies c1/c4 operations before its first c16 write fails with four
request errors. SQL S3 verifies c1 before its c4 warmup fails. KV quorum reaches
its first c16 read before failing; KV S3 fails its first c1 write with an
unconfirmed request and readback mismatch.

An unconfirmed mutation can later
commit, so a mismatch after that error does not establish corruption or lost
acknowledged data. Original attempt identities remain retained without fresh-ID
retries.

SQL Fleet and bucket also fail. Fleet node 3 and bucket nodes 2/3 eventually
exit with `NodeLeaseExpired`; the failed process exits are retained. Their
untraced logs do not establish the exact fence event time. A process finish can
follow shutdown joining, so comparison with load-exit timestamps alone cannot
attribute earlier request errors to these fences. No Raft node reports a lease
expiry fence in this run.

The failed Queue windows retain the following identity observations:

| Mode | Failed window | ACK IDs | Observed IDs | Missing ACK IDs | Extra IDs | Reported backlog |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Raft quorum | r1 c4 warmup | 72 | 80 | 0 | 8 | 0 |
| Raft + S3 | r1 c4 enqueue | 200 | 224 | 0 | 24 | 0 |
| Fleet | r2 c16 enqueue | 2720 | 2728 | 0 | 8 | 0 |
| Bucket | r1 c16 enqueue | 7648 | 7608 | 40 | 0 | 40 |

The extra 8/24/8 IDs in quorum/S3/Fleet exactly match their 1/3/1 unconfirmed
batches. Missing acknowledged IDs are zero and reported backlog/DLQ are zero
in those windows. Bucket has no producer request error in its failed window:
40 acknowledged IDs remain unobserved and a backlog of 40 is reported. A matching
count does not identify the pending messages or prove permanent loss. All four
failed rows lack a complete exported observed body-hash map; ID reconciliation
does not independently qualify bodies or settlement. Their failures remain.

Complete start/end snapshots are available for all six Raft cases. Selected
group-admission counters increment once for SQL quorum and nine times for KV
quorum, with overlapping staging counts for those same bank refusals. The
other four Raft cases have zero increments in these selected overload phases.
These are node-wide counters, including maintenance, not per-request outcomes.
A zero count does not exclude writer, readiness, routing or proof waits.

Across the whole SQL quorum case, there are 15,716 engine file-sync observations
with mean 11.30 ms; 5,079 SQLite preparation observations with mean 53.05 ms;
20,998 SQL-queue observations with mean 18.00 ms; and 3,386 staging observations
with mean 37.00 ms. ReadIndex cohorts start 1,707 times and record three joins.
These include warmup, timed, verification and background work, and the phases
overlap: the means must not be added or assigned to an individual HTTP request.
They identify preparation/persistence/queueing and limited observed coalescing
as profiling priorities, rather than proving an unavoidable quorum slowdown.

## Native verification

Locked affected-target compilation and strict Clippy pass. Formatting/layout
checks cover 2,306 Rust files; documentation and native dependencies pass
(496 reachable packages, no JavaScript engine). Python suites run 333 performance
tests, 183 multi-Raft-tool tests and 44 plan tests; the first two each retain
one existing skip. These checks do not replace complete native qualification.

There are 109 distinct passing libtest controls: 63 focused unit tests, 20
selected execution controls, 25 primitive controls and one Workflow compatibility
control. The complete primitive target has **25 passes, eight failures and 38
explicit ignores**. Failures are Axum KV, ordinary KV/SQL, interrupted SQL
recovery, ordinary alarm recovery, alarm retry exhaustion and Queue load/four
in-flight. Their original errors and retained disk paths are in the raw evidence.
The ignored explicit RustFS/timed cases are outside this native campaign; the
separate comparison above uses actual RustFS.

A targeted Axum KV rerun passes both image modes on unchanged inputs/budgets;
that does not erase its earlier failure. Targeted interrupted SQL recovery fails
again: seven configurations pass, while the successful S3 child in extent mode
times out after retained restart. The broad run instead fails two extent/S3
setup requests. These phases remain distinct; the exact readiness diagnosis is
pending. All three stopped disks from the targeted failure are copied and hash
verified locally.

The later [SQL diagnostic follow-up](main-168-sql-followup-2026-11-14.md) records
an isolated and eight-configuration recovery rerun that pass with test-only
probes, plus a separately traced constrained-load failure. Neither erases these
earlier failures. Its sync profile includes image/manifest and archive paths
outside the engine counters; tracing timings are not comparison results.

The real three-process TLS daemon runner exits zero with its explicit completion
marker. It uses `harness=false`. The wrapper incorrectly requires a libtest footer
and labels that stage failed; the original record is retained and a separate
assessment records the custom-runner pass. Eight real primitive failures still
fail combined verification. Full commands, output and executable hashes are in
the raw evidence; no `cellar_internal_tests` configuration is enabled.

The full plan CLI preflight still fails because the historical record
`.cellar/cell-groups-evidence/CG-01-06-gates-e32115aa-02/CG-01-record.json`
is missing. Passing plan unit tests do not restore that evidence or waive its gate.

## Source and lifecycle binding

Immutable image `630bbfa5`, source manifest `b417c200`, uses locked Rust 1.98.1
and size-optimized `lab`/thin LTO. The library and both node binaries rebuild;
load-client bytes remain unchanged. All 3,410 manifest entries match the image
before and after measurement. Of these, 3,350 tracked inputs match Git archive
bytes at `c5444fd2`. Sixty pre-existing ignored generated optional-JavaScript
bundles/sidecars are captured separately by exact hash; they are not claimed to
be present in the pure Git archive. Native dependencies contain no JS engine.

All 60 measured containers stop; no container OOM or restart is observed, and
sampled kernel OOM counters are zero. Failed loads exit nonzero; the three SQL
baseline nodes with lease fences exit one, while the other nodes and all case
stores exit zero. Failed containers, disks and logs remain retained. The
controller, supervisor and native/build handles are reaped. The original RustFS
service is restored under its unchanged identity/limits and readiness returns
HTTP 200. Actual generated credentials are checked absent from prepared evidence;
private config/credential files are excluded.

## Remaining priorities

1. Diagnose original-deadline progress across cold admission/routing, ReadIndex,
   private writers, SQLite preparation/apply and quorum/S3 proof waits. Preserve
   original identities and operation-specific uncertainty; do not replace unknown
   mutations with new IDs.
2. Diagnose constrained SQL-baseline node-lease renewal progress and its real
   fence timing. Current evidence does not attribute all earlier workload errors
   to lease expiry.
3. Close Queue callback/claim/settlement drain progress at the unchanged profile,
   including the 40 reported pending messages. No deadline, TTL or resource
   increase is used to turn these failures into passes.
4. Repeat native compatibility and the complete matrix after fixes. Sustainable
   complete-root manifest reclamation, authority expiry/revocation, physical
   retirement/once-only Fleet settlement, provider recovery, independent domains
   and the existing missing historical evidence remain open. No OPT/CG gate is
   promoted by this diagnostic.

Machine-readable observations retain the complete planned
matrix, individual windows, same-owner counter deltas and resource outcomes.
Compressed raw evidence retain all 93 windows and raw
latency samples/errors, native logs/records, owner observations, node/store logs,
source/build bindings and host activity. Rates and integer-rank percentiles are
independently recomputed; failed and unmeasured entries retain their statuses.
Earlier reports remain bound to their earlier sources and execution histories.
