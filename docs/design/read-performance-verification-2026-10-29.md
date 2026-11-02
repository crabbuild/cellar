<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Read performance verification

Purpose: Verify warm follower read throughput and latency for file and append inventories without per-response evidence syncs.

This verifies the read path after the [append-segment optimization](quorum-append-segments-2026-10-29.md).
No production source or binary changes are made for this verification.

The read-buffer policy is now implemented in the reusable dense runner; the
[client ledger follow-up](client-ledger-optimization-2026-10-29.md) compares it
against per-response syncs using the same service binary and reports both
traffic-only throughput and rates including the evidence flush.

## Workload and measurement

Three replicas each have one pinned vCPU, 3 GiB memory and no extra swap.
RustFS has two vCPUs and 4 GiB. All use the previous owned Colima profile and
native guest ext4 filesystem on the same workspace SSD. Ext4 barriers,
`data=ordered` and the 15,000-µs maximum journal batching wait stay unchanged.
Both backends use two SQL workers and publication capacity two. The append
backend uses 8-MiB segments with compatible commit sharing enabled.

Four fresh histories run files/append/append/files. Each creates ten cells
without bulk padding, initializes the ordinary schema/business row, and performs
one normal 1-KiB set per cell. Initialization and seed writes keep the original
per-event client-ledger fsyncs and normal service durability proofs. All four
histories end with identical business state and command sequences. The business
query is one point SELECT of revision, left/right values and payload digest;
it does not scan a payload or a large table.

Each history warms reads for two seconds, then runs two ten-second windows at
concurrency one and two at concurrency ten, reversing order in its second
repeat. No writes overlap the measured read windows. HTTP connections are
persistent within each window. Reads target the assigned follower, using
`AtLeast(last acknowledged version token)`. The serving member and returned
version/state are checked for every response. Full leadership snapshots before
and after traffic match the assigned leaders. These are warm token-bound
replica reads, not linearizable ReadIndex, cold-cache, concurrent-write, or
one-GiB-state measurements.

The earlier write-oriented runner fsyncs one serialized client ledger entry
after every read response. That work is excluded from response latency but
included in achieved throughput, limiting its usefulness as a read-capacity
measurement. This read-only harness admits up to 200,000 read events into a
bounded client-memory buffer per window, then serializes and fsyncs the evidence
after the window timer stops. Setup mutation evidence remains synchronous.
Every completed history has five verified read-evidence batch syncs: warmup
plus four measured windows. A lost/incomplete evidence batch cannot qualify a
history. The service uses the exact previously tested binary throughout.

Throughput is total successful reads divided by total elapsed window time.
Response latency runs from HTTP request submission through response-body
completion. Client parsing/model validation remains outside latency and inside
throughput. Individual latency samples are pooled for nearest-rank percentiles;
window percentiles and rates are not averaged. These are achieved client/service
rates, not a demonstrated intrinsic server saturation limit.

## Verified results

Each row pools four ten-second windows from two independent histories, about
40 seconds of measured traffic. TPS means one successful HTTP read/SQL query per
operation and is aggregate across all three nodes and ten cells.

| Backend | Concurrent clients | Reads | Reads/s | Mean ms | p50 ms | p95 ms | p99 ms | Max ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| files | 1 | 60,462 | 1,511.4 | 0.648 | 0.629 | 0.783 | 0.977 | 51.853 |
| files | 10 | 277,293 | 6,931.1 | 1.429 | 1.254 | 2.106 | 3.958 | 162.945 |
| append | 1 | 64,637 | 1,615.8 | 0.607 | 0.603 | 0.708 | 0.795 | 8.227 |
| append | 10 | 296,915 | 7,421.3 | 1.334 | 1.271 | 2.033 | 2.529 | 39.379 |

The append backend achieves approximately 1.62k reads/s at concurrency one and
7.42k reads/s at concurrency ten in this workload. Its c10 mean is 1.33 ms,
with 2.53-ms p99. The observed throughput advantage is approximately 6.9%/7.1%
at c1/c10. Concurrency-ten window ranges overlap, so this does not isolate a
causal read speedup from the write-persistence changes. The sharing mechanism
is not doing foreground write work in these read-only windows. Rare response
outliers remain; p99 is not a worst-case bound.

Per-history results make variation visible:

| History | Clients | Reads/s | Mean ms | p50 ms | p95 ms | p99 ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| files-a | 1 | 1,550.2 | 0.632 | 0.617 | 0.736 | 0.930 |
| files-a | 10 | 6,316.6 | 1.569 | 1.262 | 2.372 | 6.339 |
| append-a | 1 | 1,619.1 | 0.606 | 0.602 | 0.703 | 0.787 |
| append-a | 10 | 7,666.2 | 1.291 | 1.236 | 1.955 | 2.390 |
| append-b | 1 | 1,612.6 | 0.608 | 0.605 | 0.712 | 0.805 |
| append-b | 10 | 7,176.4 | 1.379 | 1.312 | 2.112 | 2.657 |
| files-b | 1 | 1,472.7 | 0.666 | 0.645 | 0.812 | 1.012 |
| files-b | 10 | 7,545.6 | 1.311 | 1.249 | 1.975 | 2.475 |

Independent server SQL-query observation counts also reconcile with successful
client reads. Their mean worker times are below; HTTP latency additionally
includes admission, queuing, transport and response handling.

| Backend | Clients | Server SQL-query mean ms |
| --- | ---: | ---: |
| files | 1 | 0.170 |
| files | 10 | 0.237 |
| append | 1 | 0.168 |
| append | 10 | 0.233 |

## Verification and retained evidence

All **4 histories pass**, with **0 failed histories**, **699,307 measured read responses**, and no failed or stale read. Independent replay reconciles **732,641 ledger records**, including warmup and setup, plus **240 pre/post replica-state audit observations**. The audit checks each read's state, last-write sequence, minimum applied index and exact serving member; it also verifies raw sample counts, window throughput/percentiles, archive coverage, resource limits and clean shutdown without OOMs. All four final business states and client sequences match; all build identities are identical.

- image_id: `sha256:6d4172802fde6f99f17310a5676397138ffb6e3853629e89ee72f7e8d0b80bc5`
- binary_sha256: `bea898685751b3ce6359a770ec1742cda11e4b7a7967be652b9b94789a5be2f4`
- source_manifest_sha256: `aadea10cd11a868c5940e00f6c46eaea1fe9b205d43d3c7c0884106160b28415`
- build_profile: `lab`

The already-qualified binary passed the earlier 458-test Rust scope; those
source tests are not claimed as a fresh rerun in this read-only follow-up.
All service/source identities are checked again by each benchmark startup.
The owned VM is stopped after collection; data and containers remain retained.

External evidence lives in
`/Volumes/Workspace/crabbuild-target/cellar-2446b3c0-dense`, including
`profile_reads.py`, `run_read_matrix.py`, `analyze_reads.py`,
`analyze_read_matrix.py`, `read-matrix-status.json`,
`read-verification-summary.json`, before/after ext4 options, raw reports,
per-request ledgers, resource samples, topology observations and independent
read audits under `read-verify-{files-a,append-a,append-b,files-b}`.
