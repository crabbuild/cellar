<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Verification at 32 and 64 readers

Purpose: Verify shared-cell read throughput, tail latency, evidence persistence, and reader participation at concurrency 32 and 64.

This extends the [client ledger verification](client-ledger-optimization-2026-10-29.md)
with 32 and 64 concurrent readers and a fresh ten-reader control. The native
service executable and storage settings are unchanged. Only the Python runner,
its documentation and regression tests change in this follow-up.

## Runner change

The old worker mapping assumed disjoint cell sets and refused concurrency above
the cell count. Read-only windows now allow up to 256 readers, mapping worker
IDs modulo the cell count so readers can share cells. Existing mappings below
the cell count retain their partitioning. Writes still require concurrency at
most the number of cells and preserve disjoint cell sets. Validation applies
both at the CLI boundary and in the window implementation used by the fixture.
Cell choices are computed once per worker. Each window reports successful
responses per worker so participation and totals can be checked explicitly.

The actual window regression runs 32 and 64 threads against ten cells with a
response rendezvous. It checks all workers participate, the exact distribution
over cells, response accounting, connection cleanup and one final evidence
barrier. It reproduced the former empty-cell-choice failure before the fix.
A second regression rejects excess writer concurrency before ledger admission.

## Matched setup and measurement

- Three Quorum nodes, each one pinned vCPU, 3 GiB memory and no extra swap;
  RustFS separately uses two CPUs and 4 GiB.
- Same eight-CPU/16-GiB owned Colima VM, workspace SSD and native guest ext4.
  Barriers, `data=ordered` and `max_batch_time=15000` remain unchanged.
- Ten cells without bulk padding, ordinary schema initialization and one
  normal 1-KiB seed write per cell. No concurrent writes during read traffic.
- Two SQL workers, publication capacity two, 8-MiB append segments and
  compatible service persistence barriers in both histories.
- Persistent HTTP connections and warm point reads on the assigned follower
  with `AtLeast(last acknowledged version token)`. Leadership is verified
  before and after each history. Every response is checked for exact business
  state, acknowledged command sequence, minimum applied index and serving member.
- Two fresh histories with concurrency orders 10/32/64 and 64/32/10. Each warms
  reads for two seconds, then runs two ten-second windows at every concurrency,
  reversing order for its second repeat. Each result therefore pools four
  windows and approximately 40 seconds of traffic per concurrency.

The runner uses bounded read evidence, one sync after each window and no sync
per read. Both the traffic-only rate and the rate including that evidence flush
are reported. The final flush must succeed before a window can qualify. Bounds
remain 200,000 records and 128 MiB of encoded evidence per window.

TPS is total successful reads divided by summed elapsed time, across all three
nodes and ten cells. HTTP latency runs from submission through body completion;
it includes connection establishment for each worker's first request. Parsing,
validation and evidence encoding affect throughput but are outside HTTP latency.
Percentiles pool individual response samples rather than averaging window
percentiles. These are closed-loop achieved rates, not an intrinsic server
saturation proof. They do not qualify cold caches, concurrent writes,
linearizable ReadIndex or one-GiB cell state.

## Verified results

| Readers | Successful reads | Reads/s during traffic | Reads/s including evidence flush | Mean HTTP ms | p50 ms | p95 ms | p99 ms | Max ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 10 | 271,284 | 6,780.7 | 6,357.4 | 1.444 | 1.344 | 2.337 | 3.287 | 30.008 |
| 32 | 311,067 | 7,773.1 | 7,131.4 | 4.081 | 3.656 | 7.815 | 10.811 | 51.301 |
| 64 | 320,858 | 8,010.1 | 7,546.4 | 7.917 | 6.890 | 16.660 | 23.500 | 150.333 |

Going from 32 to 64 readers adds 3.0% traffic throughput while mean latency
nearly doubles and p99 grows 2.17 times. These measurements favor 32 readers
over 64 when latency matters: doubling the clients produces little extra
throughput in this setup. At 32 readers, 98.49% of responses finish within
10 ms; at 64, 74.90% do. Neither pooled p99 meets a 10-ms target. Rare maximum
latencies exceed p99 substantially.

The fresh ten-reader control is used for this sweep. Its first window is
5,349 reads/s and its last is 7,596 reads/s; pooled performance differs from
the earlier approximately 7.46k read/s client-ledger result. Results below
retain variation between fresh histories rather than combining this sweep
with the previous experiment.

| History | Readers | Reads/s | Reads/s including flush | p99 ms |
| --- | ---: | ---: | ---: | ---: |
| a | 10 | 6,214.2 | 5,911.2 | 3.792 |
| b | 10 | 7,347.3 | 6,791.0 | 2.416 |
| a | 32 | 7,344.8 | 6,573.3 | 11.714 |
| b | 32 | 8,201.4 | 7,718.4 | 9.799 |
| a | 64 | 7,585.4 | 7,144.7 | 25.645 |
| b | 64 | 8,435.3 | 7,948.8 | 21.568 |

Every measured window uses one read-evidence sync: four per concurrency in
the pooled results. The four flushes total 2.664/3.601/2.461 seconds at
10/32/64 readers. The maximum encoded evidence buffers are 57.80/62.89/64.26
MiB respectively, all below the unchanged 128-MiB budget. Variation in these
flushes affects inclusive throughput and is not a change to service durability.

Independent server SQL-query count deltas exactly equal successful client read
counts in every history/concurrency pair. Mean server SQL-query worker times
are 0.242/0.228/0.222 ms at 10/32/64 readers. HTTP latency includes work and
waiting outside that timer. Sampled node CPU means at 32 readers span 82.5%
to 98.3% of a core across histories, with node two at 96.1% and 98.3%; at 64
readers the means span 74.3% to 85.7%. The controller, transport and host
runtime still contribute to achieved throughput and latency; these observations
do not isolate a single remaining capacity bottleneck.

Both histories pass independent replay: 903,209 measured reads, 917,911 total
ledger records including setup/warmup, and 120 pre/post replica-state audit
observations. Every requested reader receives successful responses in every
window, and per-worker totals reconcile with ledger and report counts. There
are no failed/stale read responses, ambiguous writes, lost evidence, buffer
overflows or OOMs. Final business states and command sequences match between
histories. All final ledgers are closed, successful and fully durable with
zero pending bytes. Archive coverage and original resource limits pass, and
all eight benchmark containers exit with code zero.

## Build provenance

The image passes the unmodified strict source-manifest freshness check against
the updated checkout. A guard proves every non-client source input unchanged
from the earlier qualified build; the temporary client-only recipe runs locked
Cargo against this checkout's cache and asserts the exact native output digest.
The general Dockerfile and freshness gate are unchanged.

- Image: `sha256:91aee7c7b8793ed31c864383a4b588935a2709503ebf7275c3119bcbd61b0170`
- Native executable SHA-256: `bea898685751b3ce6359a770ec1742cda11e4b7a7967be652b9b94789a5be2f4`
- Full updated source SHA-256: `0d43f6333c8180e77cdde0dc39bd992efd41910b45fca249a5a5bdfbbd4e5a78`
- Base revision: `1be27d234d4674d4d014785c60cecbfe394bdab9`; build profile: `lab`.

## Evidence and code checks

The Python suite runs 291 tests: 290 pass and one skips. Documentation validation
and `git diff --check` pass. No Rust source changes in this follow-up, so the
prior native qualification is not claimed as a fresh Rust test run.

Raw reports, request ledgers, per-worker counts, topology checks, resource
samples, stage counters, cgroups, replay results and logs are retained in
`/Volumes/Workspace/crabbuild-target/cellar-2446b3c0-dense`, under
`readers-32-64-{a,b}`. The external fixture is `profile_many_readers.py`, with
`run_many_readers.py`, `analyze_many_readers.py` and `analyze_many_matrix.py`.
`readers-32-64-comparison.json` holds pooled and per-history results;
`readers-32-64-image.json`, the source guard, build log and temporary recipe
retain provenance. Private credential-bearing configuration is not published.
The owned VM is stopped after collection, with data and images retained.
