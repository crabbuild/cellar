# TiKV comparison and Cellar performance investigation

Status: investigation and implementation in progress. No new end-to-end
throughput improvement is qualified by this document yet.

## Scope and reproducible baseline

Compare three native Cellar nodes, each limited to one vCPU and 3 GiB, with
500 leaders per node and 1,500 three-voter groups. Quorum acknowledgements must
retain their existing durable-log and committed-image publication proofs.
RustFS archive coverage is checked separately for every acknowledged cell.
Prefer p99 below 200 ms; the accepted upper budget is 500 ms.

The [previous measurements](../../tools/perf/raft-results.md) record an
error-free two-window confirmation averaging 45.53 writes/sec, with a worst
window p99 of 450.58 ms. A shorter window reached 96.81 writes/sec at 144.42 ms
p99, but that result did not reproduce in the longer confirmation. During the
confirmation node CPU averaged approximately 39–42%, memory peaked below
470 MiB per node, and CPU throttling totaled under 0.34 seconds per node.
These observations favor investigating storage waits and concurrency limits;
they do not attribute the entire tail to a measured individual stage.

Cellar base: `fb660b164b3cb0c8f88afcb659473445928a92a2`.
TiKV source: clean local checkout at
`b3995dab28308f09cbb7c125bf32f789dbc203fe`.
Both projects use raft-rs and raft-engine, but their exact versions differ:
Cellar vendors raft 0.7.0 and raft-engine 0.4.2 with execution-domain changes;
the inspected TiKV lockfile pins raft-rs `ad13f3d90780f53aea2488c6a4b76c0d334bf136`
and raft-engine `55bccdea42718ab2b4f90cdd9068da53b6bf4c39`.

## Mechanisms in the inspected TiKV source

| Mechanism | What TiKV does | Cellar implication |
| --- | --- | --- |
| Batched FSM scheduling | Mailboxes schedule an idle FSM once; pollers collect several active FSMs before a batch ends. | Cellar already parks idle groups. Request draining can let multiple queued groups reach its existing shared log writer together. |
| Shared asynchronous log writer | Drains queued write tasks up to a byte limit, then synchronously persists their combined Raft batch. Adaptive bounded waiting may enlarge small batches. | Keep disk work outside the consensus event loop and amortize sync across groups. First test draining work already available, without adding an artificial delay. |
| Ordered outstanding Ready work | Tracks unpersisted Ready numbers and releases the corresponding messages only when persistence completes. | Cellar currently permits one outstanding Ready per group. Multiple outstanding Readies would require additional ticket, failure, and message-order qualification. |
| Batched apply | Accumulates state-machine changes in a shared engine write batch; flushes before reporting apply results. | Cellar's separate SQLite images do not share TiKV's KV batch. Its per-image copy, validation, sync, and marker work can dominate despite a shared Raft log. |
| Explicit recovery progress | Raftstore v2 disables tablet WAL while tracking flushed apply indices and retaining the history needed to replay missing state. | Cellar's durable applied marker currently names a durable immutable image. Delaying that image's persistence requires separate visible and durable progress, replay retention, and crash/snapshot/GC tests. |
| Coordinated hibernation | Peer agreement and quorum checks govern leader/group hibernation. | Increasing Cellar's fixture tick interval is a workload tradeoff, not an implementation of TiKV hibernation. |

Pinned primary sources:

- [Batch mailbox ownership](https://github.com/tikv/tikv/blob/b3995dab28308f09cbb7c125bf32f789dbc203fe/components/batch-system/src/mailbox.rs)
  and [poller batching](https://github.com/tikv/tikv/blob/b3995dab28308f09cbb7c125bf32f789dbc203fe/components/batch-system/src/batch.rs).
- [Async write task draining and durable engine writes](https://github.com/tikv/tikv/blob/b3995dab28308f09cbb7c125bf32f789dbc203fe/components/raftstore/src/store/async_io/write.rs).
- [Ready sequencing](https://github.com/tikv/tikv/blob/b3995dab28308f09cbb7c125bf32f789dbc203fe/components/raftstore-v2/src/operation/ready/async_writer.rs)
  and [Ready advancement](https://github.com/tikv/tikv/blob/b3995dab28308f09cbb7c125bf32f789dbc203fe/components/raftstore-v2/src/operation/ready/mod.rs).
- [Apply batching](https://github.com/tikv/tikv/blob/b3995dab28308f09cbb7c125bf32f789dbc203fe/components/raftstore/src/store/fsm/apply.rs)
  and [flushed apply progress and recovery](https://github.com/tikv/tikv/blob/b3995dab28308f09cbb7c125bf32f789dbc203fe/components/raftstore-v2/src/operation/ready/apply_trace.rs).
- [Hibernation state](https://github.com/tikv/tikv/blob/b3995dab28308f09cbb7c125bf32f789dbc203fe/components/raftstore/src/store/hibernate_state.rs).

## Ranked hypotheses and experiments

1. **Archive reservations prevent useful concurrency.** Each pending callback
   reserves the maximum image plus maximum envelope (about 80 MiB), even after
   its much smaller output is known. With a 256-MiB backlog limit, three such
   reservations can prevent the fourth command. Predict that replacing the
   reservation with a verified physical bound after preparation will admit
   more small commands while preserving refusal for unknown or excessive work.
2. **Private execution adds disk syncs ahead of Raft.** Working-copy creation,
   SQLite transactions, LTX capture, preview generation, committed publication,
   and durable markers have distinct I/O requirements. Instrument these stages
   before changing them. Disposable private state may have weaker persistence
   needs; committed images cannot be relaxed under the current marker contract.
3. **Mailbox dispatch fragments log batches.** The worker starts persistence
   after dispatching one request, even when other groups' requests are queued.
   Predict that bounded draining will share the first persistence batch across
   an already queued burst, without requiring a deliberately stalled disk.
4. **Archive verification limits sustained catch-up.** One producer and replay
   workspace serve each node. Even asynchronous archive work competes for CPU,
   storage and admission space. Measure lag and verifier occupancy before
   considering a bounded worker pool and its memory/shutdown obligations.

## Reservation change and safety boundary

The native SDK regression
`prepared_small_commands_release_worst_case_archive_reservations` opens eight
real groups and holds archive storage unavailable. Each small command must
apply while its S3 waiter remains pending. The unchanged implementation refused
the fourth command on two runs. After storage recovers, all admitted commands
must obtain S3 receipts with their exact values.

The proposed change keeps the maximum reservation until the private executor
has encoded the entry and physically verified its preview. It then retains
image length plus encoded-entry length plus framing allowance through proposal,
apply, and proof completion. Retained log entries and applied images continue
to count independently, so overlap is conservative. Unprepared callbacks,
including cancelled callbacks still running, retain the original bound.
Entry limits, duplicate receipt checks and ambiguity semantics remain in force.

The new native tests pass with this change: eight small S3 waiters fit the
default budget, and a deliberately smaller byte budget still refuses a second
callback. The complete archive binary passes all 31 cases.

## Queued request batching

The production runtime now drains at most 64 already queued requests in a
dispatch turn before collecting Ready work. It adds no waiting period and
retains the existing log byte/group bounds, one in-flight writer, per-group
ordering, and persistence receipts. Each bounded turn returns to event, timer,
I/O and shutdown polling.

`queued_groups_share_the_first_persistence_batch` queues eight independent
groups on a current-thread executor, with real raft-engine persistence and no
artificially blocked sync. Before the change, one group used sequence 33 and
the other seven used sequence 34. Afterward they share their initial sync.
All 16 runtime cases pass, including the held-sync message barrier, stalled
group isolation and shutdown with a full queue. The execution binary passes
all 16 cases serially; an initial parallel run encountered a directory-lock
refusal in its restart test, whose isolated repeat passed. The original failure
is retained in the diagnostic log and is not erased by the passing repeats.

## First comparison and follow-up

The first candidate (`sha256:f01a9a747600c29040a6eb3b5c01f75c27d6b9b6a635a13cf44afb79250177bb`)
contained physical-size reservation accounting and request batch draining.
The fresh control (`sha256:f7d8781239d87d2876ee8c8e542cc94ee87dbe311377e1746ec0e3330c6bb9df`)
used the unchanged baseline binary. Both used 60-second windows, two reversed
repeats at concurrency 3, 6 and 9, a 500-ms tick, 4,096 queue items, and 512-MiB
archive scratch tmpfs. Builds and profilers were absent during timed windows.

| Image | Concurrency | Mean successful writes/sec | Worst p99 (ms) | Admission refusals across both windows |
| --- | --- | --- | --- | --- |
| Control | 3 | 84.07 | 192.16 | 0 |
| Control | 6 | 92.56 | 403.11 | 0 |
| Control | 9 | 41.05 | 936.70 | 7,724 |
| First candidate | 3 | 69.56 | 440.75 | 1 |
| First candidate | 6 | 72.10 | 1,102.30 | 0 |
| First candidate | 9 | 79.82 | 458.88 | 328 |

All 4,500 replica values and 1,500 archive positions reconciled in each run.
However, no candidate setting passed both the zero-error and p99-under-500-ms
gates in every repeat. The candidate substantially reduced nine-writer
refusals but **did not establish an overall performance improvement**.
Raw evidence is in `.cellar/raft-perf/1500-tikv-batching` and
`.cellar/raft-perf/1500-tikv-control`, with independent `analysis-500.json` files.
These ignored local artifacts contain full samples; this table is not a soak
or crash qualification. A later `strace` experiment perturbed leadership and
produced ambiguous outcomes, so its timings cannot attribute production costs.

Follow-up regressions reproduced two remaining pre-proposal refusals: another
callback holding an unknown-size archive reservation, and a reader holding the
cell's SQL owner. The host now retains bounded pending requests for temporary
reservation pressure, retries them in FIFO order, and keeps the maximum charge
until preparation completes. A cell can retain one command behind non-command
SQL work and rechecks authority when SQL becomes available. It still refuses
a second pending command. Cancellation must remove waiting work without running
the callback; full retained archive backlog still permits duplicate lookup and
refuses new mutation. These changes require a fresh measured comparison.

The follow-up implementation passes 615 selected cases: 442 library, 33 archive,
16 execution, 27 membership, 20 reads, 16 runtime, 42 snapshot and 19 operations.
Six child-process entry points are intentionally ignored by their standalone
binaries and invoked by their parent scenarios. The integration suites were
run serially after an earlier parallel restart test encountered a directory
lock. Formatting (792 files), module layout, native dependency checks, docs,
and Clippy for the library, performance binary and changed integration targets
also pass. These checks retain the existing durable-log, image publication,
recovery and physical-shutdown assertions; they do not prove a performance gain.

The follow-up image (`sha256:200d020287cc61731206917afcf128c83fd31470c9b316780a4abc96c5f42be6`)
completed eight 60-second write windows at concurrency 3, 6, 9 and 12 in reversed
repeats, with 40,581 acknowledged writes including seed and zero write errors.
However, final replica verification failed: node 1, cell 193 returned
`Unavailable`. A separate read-only scan found 4,499 correct replica values and
the same unavailable replica. Its leader and other follower both returned the
expected value 35, with applied/archive index 38. The failed run is preserved at
`.cellar/raft-perf/1500-tikv-admission2`; it is **not qualified**, and no result
from it establishes a throughput gain. Independent host-process samples also
record concurrent builds/tests from other checkouts during later windows.
Failure-origin instrumentation and a reproduction are required before another
performance conclusion. The original report remains incomplete.

## Follower publication scheduling failure

A diagnostic-only repetition with failure logging passed all 4,500 replica and
1,500 archive checks after 49,129 acknowledgements. This does not explain the
earlier permanent refusal. Subsequent concentrated traffic across 30 of the
same 1,500 groups acknowledged another 14,704 writes without write errors, but
three of 90 active replica reads could not reach their acknowledged versions.
Their committed/applied indices were 449/230, 679/389 and 396/317. The other
replicas continued serving. These runs intentionally overlapped diagnostic
tests and are not performance comparisons.

The smaller real three-node SDK scenario
`follower_drains_committed_batch_after_held_reader` holds one follower's reader,
commits 32 writes through the other voters, then releases that SQL owner. It
reproduced a follower committed through index 35 but stuck applied/published
at index 6. There was no runtime or storage failure event in this case.

The host can receive the next committed apply while publishing the previous
image. Dispatching publication consumes the group's active scheduling slot.
Completion previously cleared `apply_inflight` without restoring the slot for
the queued entry, leaving that follower permanently idle. The proposed fix
restores scheduling after publication physically completes and keeps
`apply_inflight` true when an entry is queued. The reproducer now passes 40
consecutive fresh three-node fixtures. The final implementation passes 643
selected native/library cases, formatting, and affected-target Clippy. Full-cluster
revalidation of this fix remains incomplete. The earlier node-1/cell-193
`Unavailable` failure remains distinct
and unresolved until its origin is captured.

## Latest measurement and diagnostic limits

The publication-scheduling candidate
(`sha256:0a2380ae65b77c6f86547771307217df267e8dc8bf2fc3e5b8504a075c70876f`)
completed its first 60-second windows at concurrency 3, 6, 9 and 12 with zero
write errors: respectively 88.11, 82.82, 82.88 and 93.05 writes/sec, with p99
125.68, 227.52, 339.21 and 357.68 ms. The second 12-writer window fell to
48.72 writes/sec and 3,088.92-ms p99, with two ambiguous outcomes. The harness
stopped before final replica/archive verification. Both ambiguous commands
subsequently reconciled successfully using their original identities and
original retry deadlines, outside measurement. That reconciliation does not
turn the failed measurement into a passing one. The preserved run is
`.cellar/raft-perf/1500-tikv-publication`.

A separate bounded `perf trace` sample on node 1 recorded 5,406 completed
Raft-log syncs (median 1.202 ms, p99 5.509 ms) in approximately 24 seconds.
Their aggregate syscall duration was 7.79 seconds; this is summed thread time,
not a fraction of end-to-end latency. Another 28,877 syncs lacked reliable path
attribution, so the trace cannot rank private capture versus image publication.
The diagnostic load also encountered eight `NotLeader` responses. Its throughput
is not a qualifying performance result. This evidence supports further
storage-stage instrumentation, not a claim that a particular fsync stage has
already been proven dominant. RustFS archive verification remains asynchronous
for the measured quorum-acknowledged writes, but retained backlog can limit
admission.

## Disposable command workspace

Private command execution now explicitly opens disposable WAL capture. Its
SQLite connections omit local syncs, as do temporary LTX cuts, copied bases and
capture cleanup. Every reopen discards the entire workspace before using an
engine-selected committed image. The ordinary LTX constructors retain durable
capture, and disposable open refuses existing sidecars or metadata before
opening SQLite. Captured cuts still undergo bounded decoding and physical
preview verification. Raft logs, previews, committed images and applied markers
retain their existing durability barriers.

This applies the same distinction seen in TiKV between replayable local state
and authoritative recovery progress, but does not adopt TiKV's delayed tablet
flush contract. Cellar still publishes a durably sealed immutable image before
its applied marker can acknowledge a write.

A temporary real SQLite/raft-engine single-voter fixture measured five fresh
instances with 24 sequential counter increments each, excluding initial setup.
Private staging averaged 16.301 ms before and 9.536 ms after; its median changed
from 16.160 to 9.363 ms. Commit/apply averaged 17.133 and 15.770 ms respectively.
These are debug-build macOS stage measurements, not the container workload or
an end-to-end capacity claim. The same probe checked every committed counter;
its source and raw samples remain in the ignored diagnosis directory.

The capture regression first failed at the injected cut-file sync. Explicit
disposable capture now produces an independently restorable chain, including
checkpoint control writes, even when that sync operation is unavailable; normal
durable capture continues to fail. A second test refuses dirty-workspace reuse
before touching the database. The native execution suite passes 17 cases,
including process-kill cuts, committed-image marker barriers, replay/deduplication,
and a new corrupt-private-database/sidecar recovery scenario. Broader checks and
full-container measurement are still in progress.

Temporary timing probes have been removed from source. Permanent group failures
now use structured error events instead of the temporary diagnostic prints,
including their group, operation error where available, and admission-close
location. The measurements retain failed runs; no past timeout or replica
failure is reclassified by this change.

## Remaining qualification

- Run native archive, execution, runtime, recovery and resource regressions for
  the final implementation, including saturation and cancelled work.
- Record batch occupancy and stage-level I/O evidence on diagnostic runs.
- Compare fresh baseline and candidate clusters with identical resources,
  durability, cell counts, client partitioning and archive scratch placement.
  Avoid builds or profilers during timed windows; retain repeated raw samples.
- Require exact values on all 4,500 replicas, archive coverage for all 1,500
  cells, and zero ambiguous outcomes or admission errors in selected windows.
- Publish measured results separately from architectural expectations. TiKV
  mechanism reuse alone is not a performance or durability qualification.
