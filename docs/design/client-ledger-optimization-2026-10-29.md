<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Benchmark client ledger optimization

Purpose: Record bounded client evidence, compatible ledger barriers, and matched read-throughput verification.

The durable client evidence ledger throttled the earlier read benchmark:
serialization, append and one fsync per response shared a global lock. HTTP
latency excluded this bookkeeping, but achieved throughput included it. This
change removes that barrier from read traffic in the reusable dense runner and
allows concurrent durable mutation evidence to share compatible barriers.
The service executable, persistence implementation and durability contracts
are unchanged in this comparison.

The [32/64-reader follow-up](readers-32-64-verification-2026-10-29.md) enables
shared-cell readers and measures higher concurrency with a fresh ten-reader
control using the same native executable.

## Implementation and evidence guarantees

`tools/perf/operation_ledger.py` owns the shared ledger. `raft_workload.py`
re-exports `Ledger`, retaining serial `sync` as the default for existing callers.
The dense runner explicitly defaults to `group` durable appends and `window`
read evidence. Encoding happens outside the shared lock; immutable encoded
records receive their monotonic order under the lock. Partial writes are
completed before any persistence barrier can cover a record.

Durable appends return only when a successful fsync covers their complete
record. Group mode captures its record/byte watermark before starting fsync;
records written during that syscall need a later barrier. Other callers can
append during an in-flight barrier and share the next one. There is no
background writer or deliberate batching delay. Mutation invocation evidence
still persists before HTTP send, and completion evidence persists before the
worker reuses the cell. Durable admission is bounded by 256 pending records,
32 MiB of pending bytes and 8 MiB per record.

Read-only windows retain encoded evidence in a buffer bounded by both 200,000
records and 128 MiB. The timer stops after all request workers join; the runner
then writes bounded chunks and syncs the evidence once. A window result is
published only after that barrier succeeds. Mutation or unclassified evidence
cannot enter a read batch. Buffer exhaustion and encoding/write/sync errors
poison the ledger and fail the history. A valid partial prefix can be retained
for diagnosis without qualifying the run. A client crash before the batch
flush loses that window's unpersisted evidence, so that window cannot qualify.
The bounds charge actual encoded bytes; Python record/list overhead and active
caller inputs require additional memory.

Sync failures wake all waiters and prevent further admission. Close joins
active appends and barriers, closes the file even after prior failure, and
reports the failed state. The dense runner still closes HTTP connections and
cleans up its exact containers when evidence persistence fails. Reports retain
durable/total record counts, pending bytes, sync counts/timing, buffer usage,
flush duration and failure/closed state.

Use `--operations read` for a read-only sweep after ordinary initialization.
The optimized dense defaults are `--ledger-mode group --read-ledger-policy
window`; the serial control is `--ledger-mode sync --read-ledger-policy
durable`. Buffer budgets are explicit through `--read-ledger-max-records` and
`--read-ledger-max-mib`. Longer or faster windows can exhaust these bounds;
shorten the window or explicitly increase its budget. See
[the runner guide](../../tools/perf/dense.md#client-evidence-and-read-throughput).

## Matched experiment

Four fresh histories ran legacy/optimized/optimized/legacy against the same
qualified service image. An external source guard required every non-client
source input to match the original build. The only changed source-manifest
paths were `dense.md`, `dense.py`, `raft_workload.py`, `operation_ledger.py` and
`test_operation_ledger.py`, all under `tools/perf/`. Both policies exercised
the actual updated `Experiment.window` implementation; the control selected
serial durable appends rather than a separate prototype workload.

The setup uses three nodes, each with one pinned vCPU, 3 GiB memory and no extra
swap, plus RustFS at two CPUs and 4 GiB. The owned Docker VM has eight CPUs and
16 GiB, with native guest ext4 on the workspace SSD. Ext4 barriers,
`data=ordered` and `max_batch_time=15000` remain enabled and unchanged.
Quorum uses two SQL workers, publication capacity two, 8-MiB append segments
and compatible service barrier sharing in both client-policy cases.

Each history has ten cells without bulk padding, normal schema initialization
and one ordinary 1-KiB seed write per cell. Reads target the assigned follower
with `AtLeast(last acknowledged version token)`, using persistent HTTP
connections. Leadership is checked before and after traffic. Each history
warms reads for two seconds, then runs two ten-second windows at concurrency
one and two at concurrency ten, reversing order for the second repeat. No
writes overlap the read windows. These are warm point queries, not scans,
cold reads, concurrent-write tests or one-GiB-state measurements.

Throughput divides successful responses by total window time; the inclusive
column also charges the final evidence flush. Latency runs from HTTP submission
through response-body completion. Parsing, validation and evidence encoding
remain inside throughput and outside HTTP latency. Percentiles pool individual
samples; rates divide total reads by summed elapsed time. Each row pools four
windows from two histories, approximately 40 seconds of traffic.

| Client policy | Clients | Reads | Reads/s during traffic | Reads/s including flush | Mean HTTP ms | p99 HTTP ms | Read evidence syncs |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| serial durable | 1 | 52,221 | 1,305.4 | 1,305.4 | 0.567 | 0.761 | 52,221 |
| buffered reads | 1 | 59,914 | 1,497.7 | 1,488.3 | 0.641 | 0.829 | 4 |
| serial durable | 10 | 154,965 | 3,873.3 | 3,873.3 | 0.813 | 1.476 | 154,965 |
| buffered reads | 10 | 298,561 | 7,462.7 | 7,163.7 | 1.312 | 2.358 | 4 |

At concurrency ten, traffic throughput improves 92.7%; including the evidence
flush, it improves 85.0%. At concurrency one, the corresponding gains are
14.7% and 14.0%. The largest read buffer is 58.98 MiB, below the 128-MiB limit.
The four pooled optimized c10 flushes take 1.670 seconds in total; c1 takes
0.254 seconds. Flush cost is explicit, not silently discarded.

HTTP p99 increases at the higher achieved rate. The former per-response sync
throttled actual offered traffic despite the same client concurrency. These
results establish removal of a client bottleneck, not lower service latency
or a new intrinsic server-capacity limit. Controller encoding, validation and
closed-loop scheduling still affect achieved throughput. The warm read rate
is also consistent with the earlier [read verification](read-performance-verification-2026-10-29.md),
which used a separate read-only evidence-buffer prototype.

## Durable append calibration

A separate client-only calibration used ten threads, each appending 100 durable
records, with real fsyncs and no artificial batching delay. It ran after the
service experiments and after the VM stopped to avoid contention.

| Case | Durable records/s | Completed syncs | Largest durable cohort |
| --- | ---: | ---: | ---: |
| serial a | 5,166.6 | 1,000 | 1 |
| group a | 14,783.6 | 248 | 8 |
| group b | 14,136.2 | 241 | 9 |
| serial b | 5,174.9 | 1,000 | 1 |

Every file independently reconciles all 1,000 unique worker/sequence pairs and
orders, with total records equal to durable records and zero pending bytes.
This demonstrates approximately 75% fewer client evidence syncs and 2.8 times
the durable-append primitive throughput. It does not establish an improvement
in service write TPS. Read-batch cohort sizes in the read reports are not used
as evidence of mutation group-commit effectiveness.

## Qualification and provenance

All four histories pass, reconciling 565,661 measured reads, 594,995 total
ledger records including setup/warmup, and 240 replica-state audit observations.
Independent replay checks every read's exact business state, client sequence,
minimum applied index and serving member, plus raw counts/percentiles,
evidence bounds/barriers, archive coverage and resource ceilings. Final
business states and sequences match across histories. Every final ledger is
closed, has no failure, has no pending bytes and has all records durable.
There are no failed/stale responses, ambiguous writes or OOMs; every benchmark
container exits cleanly with code zero.

The Python suite runs 289 tests: 288 pass and one skips. New regression tests
cover the actual dense read window, failed final flush, compatible watermark
sharing, late-record exclusion, waiter failure propagation, both read admission
bounds, mutation rejection, partial writes, capacity backpressure and joined
close. The initial regression reproduced five read syncs where one was required.
Documentation validation and `git diff --check` pass. No Rust source is changed
for this client optimization; the earlier 458-test Rust qualification belongs
to the unchanged service build and is not claimed as a fresh rerun here.

- Benchmarked image: `sha256:6d4172802fde6f99f17310a5676397138ffb6e3853629e89ee72f7e8d0b80bc5`
- Native executable SHA-256: `bea898685751b3ce6359a770ec1742cda11e4b7a7967be652b9b94789a5be2f4`
- Service source manifest SHA-256: `aadea10cd11a868c5940e00f6c46eaea1fe9b205d43d3c7c0884106160b28415`
- Updated client source manifest SHA-256: `f8ab6acbd542a935ab9446a6b5158e9f67b8d34c95e864833792a9bff557e417`
- Base revision: `1be27d234d4674d4d014785c60cecbfe394bdab9`; build profile: `lab`.

A client-matching image was subsequently rebuilt with locked Cargo against the
checkout's own build cache. The temporary recipe omits the general Dockerfile's
source-retouch step only after the guard proves all non-client inputs unchanged,
and asserts that the output executable has the exact benchmarked digest. The
result, `sha256:5c66021d5fd804fa56fb36e7477463e34f827f503d6941cf223c21d9d0c5bbee`,
passes the unmodified strict current-checkout `verify_image` check and carries
the updated full source manifest. Its native executable is byte-identical to
the benchmarked executable. This packaging check is not an additional traffic
run; neither the production Dockerfile nor the normal freshness gate is changed.

Raw evidence is retained outside source in
`/Volumes/Workspace/crabbuild-target/cellar-2446b3c0-dense`: paired
`ledger-{legacy-a,optimized-a,optimized-b,legacy-b}` histories,
`profile_ledger_reads.py`, matrix runner and independent replay scripts,
`ledger-comparison.json`, `ledger-matrix-status.json`, before/after ext4 options,
`ledger-write-probe.json`, `ledger-client-image.json`, the temporary build recipe,
source guards and qualification logs. Configuration
with credentials remains private. The owned VM is stopped after qualification;
data and images remain available for inspection.
