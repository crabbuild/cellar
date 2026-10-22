# Native multi-Raft performance investigation

This checkout contains the fix and a reproducible three-node Colima fixture.
The baseline at `a849cc213f323788b68ccbe7f9334065e62cb24f` could register
1,500 groups but failed write-readiness qualification. The fixed implementation
has completed end-to-end writes, exact replica reads, and verified RustFS
archive catch-up for the same topology.

The preferred target is the highest observed error-free throughput with p99
strictly below 200 ms. A second analysis allows up to 500 ms when it buys
throughput. Measurements count complete successful HTTP responses, including drain
time. An SQL counter increment is one write transaction; it is not a batch of
application operations. The client waits for Quorum durability. RustFS is the
verified asynchronous archive and catalog; these latencies are not synchronous
S3 acknowledgement latencies.

## Configuration

- Three native SDK processes, each leading 500 of 1,500 three-voter groups.
- Each node follows the other 1,000 groups, with one pinned vCPU and 3 GiB memory.
- Colima: native arm64 VZ, eight vCPUs, 16 GiB; persistent volumes on ext4.
- RustFS: `ghcr.io/rustfs/rustfs:1.0.0-glibc`, two separate vCPUs and 4 GiB.
- Load generator: two separate vCPUs and 1 GiB, inside the same private network.
- Optimized `lab` profile, one Tokio worker, two SQL owners per Cellar node.
- 500-ms ticks, heartbeat every two ticks, nominal elections after 10–20 seconds.
- Runtime/event/transport queues: 4,096 items, bounded byte budgets unchanged.
- Only the disposable archive verifier scratch directory uses a 512-MiB tmpfs.
  This memory is charged to the same 3-GiB node limit. Raft logs, committed cell
  images and RustFS remain on persistent disk.
- All cells receive an initial committed write and verified archive coverage
  before timed traffic. Measured writes are never retried.

## Fixes and evidence

The production runtime now parks idle groups and keeps node-wide admission
accounting incrementally. The original idle-group regression test failed before
the fix and passed afterward. Inventory maintenance runs on its interval instead
of scanning every group for each peer frame. Scoped membership reconciliation
still precedes request admission, preserving idempotent repair behavior.

Heartbeat/pre-vote/read-only Ready records with no durable mutation skip an
unnecessary log sync. New terms, votes, log entries, commit changes and snapshots
retain the persistence barrier. The heartbeat regression test failed before the
fix. The runtime test holds an actual term/vote sync and checks that no vote
escapes it; pre-vote messages may flow without a disk mutation.

TLS frames use vectored writes, preserving authenticated bytes and bounded
payload ownership while avoiding separate records for each header fragment.
Tests cover exact wire bytes, partial writes, Pending, and write-zero failure.
Runtime-detected SHA acceleration retains the library's software fallback.
In seed-work profiles, software SHA-256 consumed about 20% of sampled CPU;
the hardware implementation consumed about 1.6%.

Disk traces showed individual fsync stalls above 100 ms. Moving archive replay
scratch to bounded RAM removed avoidable scratch-file sync traffic. Final
measurements exclude builds, crash tests and profiling. Other tasks on the
shared physical host are not controlled by this fixture.

## Validation

- Final accelerated build: 442 library, 15 native Raft runtime and 14 transport
  tests passed. Two process fixture entry points are intentionally ignored.
- Archive (29), execution (16), membership (27), reads (20), and snapshot (42)
  tests passed on the protocol changes. One snapshot crash-matrix attempt was
  refused with Overloaded during fixture setup; its isolated rerun passed.
- Clippy with warnings denied passed for the library and fixture binary.
- Formatting, module layout, native dependency and documentation checks passed.
- Benchmark evidence tests independently check counts, percentiles, resource
  limits, leader distribution, command identities and permitted scratch mounts.
  The Python suite passed 94 tests; its opt-in Docker build test was skipped.

These tests and local measurements do not establish independent failure zones,
power-loss behavior of the host, or a production availability/latency guarantee.
See [the runnable fixture](raft-README.md) for commands and evidence formats.

## Measurements

### Completed rate sweep

The `1500-scratch` run verified 12,877 acknowledged writes including the seed,
all 4,500 replica values and all 1,500 archives. Each setting ran for 60 seconds
with three clients, one per primary. Every window had zero errors.

| Offered requests/sec | Successful writes/sec | p50 ms | p95 ms | p99 ms |
| --- | ---: | ---: | ---: | ---: |
| 10 | 10.00 | 10.80 | 62.53 | 89.85 |
| 30 | 29.65 | 10.73 | 59.03 | 133.22 |
| 60 | 53.10 | 19.70 | 85.79 | 138.51 |
| Unpaced | 96.81 | 22.56 | 72.52 | 144.42 |

The unpaced result meets the preferred 200-ms target. It is one observed window,
not evidence that this rate and latency can be sustained on every run.

### Completed concurrency sweep

The `1500-concurrency` run used two 60-second windows per setting, reversing
their order on the second pass. It verified 22,208 acknowledged writes including
the seed against all 4,500 replica values and all 1,500 archives.

| Concurrent writers | Mean writes/sec | Range writes/sec | Worst window p99 ms | Admission refusals | Passes both repeats below 500 ms |
| ---: | ---: | ---: | ---: | ---: | --- |
| 1 | 24.04 | 18.13–29.95 | 449.21 | 0 | Yes |
| 2 | 42.93 | 35.98–49.88 | 429.70 | 0 | Yes |
| 4 | 48.70 | 42.41–55.00 | 588.07 | 486 | No |
| 6 | 56.76 | 46.10–67.43 | 653.22 | 0 | No |

None passed the preferred 200-ms target in both repeats. Admission refusals are
excluded from successful throughput and disqualify that setting from selection.
There were no ambiguous write outcomes. Peak cgroup memory was 567–576 MiB per
node, with no memory-limit or OOM events.

The worker partitions preserve one command in flight per cell. With three or
six workers, each worker's cell set belongs to one primary; other concurrency
levels distribute each worker across primaries. The three-writer setting keeps
one command in flight per primary, avoiding temporary concentration on one node.

### Balanced confirmation

The `1500-balanced-confirmation` run completed two 120-second windows at
concurrency three with the same image, resource limits, durability and archive
checks. Both windows had zero errors.

| Repeat | Successful writes | Writes/sec | p50 ms | p95 ms | p99 ms | Max ms |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 5,345 | 44.35 | 27.60 | 232.17 | 450.58 | 4,810.87 |
| 2 | 5,606 | 46.71 | 41.69 | 187.89 | 367.53 | 1,162.12 |

The mean was **45.53 writes/sec**, with **450.58 ms worst-window p99**. This is
the highest repeated error-free throughput observed below 500 ms in these runs.
It does not meet the preferred 200-ms limit. The earlier 96.81 writes/sec at
144.42 ms p99 remains the best single window, and did not reproduce in the
longer confirmation. A p99 limit also permits individual requests above that
limit; the observed maxima are retained in the table.

All **12,451 acknowledged writes** including the seed reconciled with exact
values on **4,500 replicas**. All **1,500 archives** covered their last
acknowledged index, and leadership remained 500 per node. The final archive
check took 5.93 seconds after replica verification. Peak cgroup memory was
466–469 MiB per node, with no memory-limit or OOM events. CPU throttling totaled
under 0.34 seconds per node across the run; storage and archive pressure remain
the main investigation candidates for further throughput improvement.

For this fixture, use three concurrent writers, one per primary, and the 500-ms
latency budget. The 200-ms target has one passing observation but lacks repeated
qualification. Do not extrapolate the results to different storage, workloads
or independent physical nodes.

The confirmed cluster remains running in Docker context `colima-cellar-raft-perf`
with prefix `cellar-raft-20261022-061508`. Its three Cellar endpoints are
`127.0.0.1:19801`, `127.0.0.1:19802` and `127.0.0.1:19803`; RustFS is on
`127.0.0.1:19800`. Private credentials remain in the run's `config/` directory.

## Reproduction and retained evidence

All completed measurements above used image
`sha256:f7d8781239d87d2876ee8c8e542cc94ee87dbe311377e1746ec0e3330c6bb9df`
and executable SHA-256
`926cb8c74c321b557f4fb6bf1266c55054c2dfa67f50e52f1dfe1aca2ab41158`.
The image tag is `cellar-raft-perf:accelerated-ios-7da4ddb5`. Source snapshots,
patches, raw receipts, latency samples, resource limits and independent analyses
remain under `.cellar/raft-perf/1500-scratch/` and
`.cellar/raft-perf/1500-concurrency/`, with the longer confirmation under
`.cellar/raft-perf/1500-balanced-confirmation/`. Failed exploratory runs remain alongside
them; they are not substituted for qualified measurements.

Recompute the two latency budgets independently:

```sh
python3 tools/perf/analyze_raft.py .cellar/raft-perf/1500-balanced-confirmation --p99-limit-ms 200
python3 tools/perf/analyze_raft.py .cellar/raft-perf/1500-balanced-confirmation --p99-limit-ms 500
```

Reproduce the confirmation in a fresh experiment after stopping the currently
named cluster and freeing its fixed ports/network subnet (retain its volumes):

```sh
python3 tools/perf/raft.py --image cellar-raft-perf:accelerated-ios-7da4ddb5 \
  --cells-per-primary 500 --tick-ms 500 --queue-items 4096 \
  --archive-tmpfs-mib 512 --seconds 120 --repeats 2 --concurrency 3
```

The analyzer recomputes quantiles from raw response times and reconciles all
acknowledged increments, including seed commands, with exact replica values and
verified archive indices. Selection requires every retained repeat for a setting
to be error-free and strictly below the chosen latency limit.
