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

## Verified container runs for disposable execution

The candidate image
`sha256:6fdc0369dc06c3b5a1052d6cd2403d12c61706f71c97f7515d401009859c3c74`
contains the production sources committed at `47615ea`. All 96 recorded inputs
match its frozen build snapshot. Final validation passes 652 selected Rust
cases (644 Cellar library/native cases and eight LTX cases), affected-target
Clippy, formatting, layout, dependency, documentation and eight Python harness
checks. Six standalone crash helpers run through their parent scenarios.

The eight-window 1,500-group run reconciled 56,460 acknowledgements, including
seed, against all 4,500 replica values and all 1,500 verified RustFS archive
positions. The independent analyzer selected three concurrent writers:
**113.25 writes/sec mean, 106.84 minimum, 141.23-ms worst-window p99**, with
zero errors in both 60-second windows. Six writers also had zero errors,
but lower mean throughput and a worst p99 of 255.90 ms. Both 12-writer windows
and the second nine-writer window had admission refusals and are excluded.
The observed 179.37 writes/sec at 12 writers is not a qualifying operating point.
Final archive catch-up took 21.82 seconds after replica checks. No node exceeded
its 3-GiB limit or recorded an OOM event.

A subsequent 90-second, 12-writer workload concentrated traffic on 30 cells of
the same 1,500-group cluster. It acknowledged 19,054 additional writes with zero
errors, then again verified every one of the 4,500 replicas and 1,500 archive
positions. The previously observed follower scheduling stall did not recur.
No permanent group-failure event appeared in the sweep's node logs. This does
not retrospectively explain the distinct earlier permanent `Unavailable` case.

The [sanitized measurement record](evidence/tikv-disposable-progress.json)
includes image/binary identities, all window results, qualification exclusions
and hashes of ignored raw artifacts. Unrelated host builds overlapped 58 of
118 activity samples, limiting timing attribution. A fresh unchanged control
subsequently passed all replica/archive checks but had one six-writer admission
refusal; its three-writer windows varied from 98.81 to 37.79 writes/sec, with
worst p99 357.12 ms. Host builds overlapped 44 of 69 control samples. That control
used only three/six-writer windows. The matching candidate confirmation then
failed with one ambiguous response in its final three-writer window. Its first
three-writer window reached 100.76 writes/sec at 266.79-ms p99; the final one
reached 68.88 writes/sec at 211.01-ms p99 plus that timeout. Both six-writer
windows exceeded 500-ms p99 (569.10 and 1,117.60 ms). The original command
subsequently reconciled using its unchanged identity and original retry deadline;
all three replicas had already applied its index. The failed measurement remains
incomplete, and the timeout stage is not yet known.
These observations establish verified operating points, not a controlled
speedup or a soak/failure-zone qualification. All stopped clusters retain their
named durable volumes.

## Phase diagnostic and remaining tail

A temporary instrumented build repeated the same 1,500-group fixture. Its first
three/six/nine-writer windows had zero errors, but the second nine-writer window
failed with five ambiguous responses and leader changes. This is a diagnostic
run, not a qualifying throughput comparison. Instrumentation records every
completion at least 250 ms long plus one of every 64 completions; these records
are not an unbiased latency distribution.

All five ambiguous producers spent 5,028–5,539 ms between submission preflight
and delivery of their committed apply. Image preparation, applied-marker
completion and publication then took 7–24 ms combined. Their original command
identities reconciled within their original retry deadlines, and a separate
read-only audit verified 33,274 writes including reconciliation against all
4,500 replicas and 1,500 archive positions. The original timed report remains
failed. By that audit, 16 groups had changed from the requested leader placement.

Other slow completions appeared in different stages across nodes at similar
times. A VM pressure sampler remained scheduled throughout the five-second
failure interval; its longest observed sample gap was 342 ms. I/O pressure was
low during much of that particular interval, so it does not support attributing
those timeouts to a whole-VM pause or continuous global I/O stall. This narrows
the next probe to shared-log persistence and transport queue/RPC time. Each peer
lane currently waits for a frame admission response before sending its next
frame; whether that serialization causes these tails remains to be measured.
The failed measurement and tagged probe sources are retained locally under
`.cellar/raft-perf/1500-tikv-phases` and `tikv-phases-source`.

The standard Linux SDK CI job passed at `47615ea`. Its free-threaded Python
matrix job failed the native `management_stop_seals_raft_before_joined_receipt`
case: admission closure was not observed within its 500-ms bound. The original
assertion remains unchanged; this failure is unresolved and is an additional
qualification gate.

## Bounded transport pipelining

The second diagnostic build added shared-log persistence timing, sender queue
and RPC timing, and receiver admission timing. All six 60-second windows at
three/six/nine writers had zero errors; every replica and archive verified.
The five-second timeout did not recur. Shared-log persistence reached
761/1,514/1,505 ms on nodes 1/2/3. Control-frame queue delays reached
424/459/404 ms during measured write windows; the corresponding peer RPCs
completed in 1–3 ms. No receiver admission call reached 100 ms. These bounded
slow-event probes identify queueing separately from persistence stalls, without
qualifying the diagnostic image as a faster implementation.

TiKV's [message buffer and stream sender](https://github.com/tikv/tikv/blob/b3995dab28308f09cbb7c125bf32f789dbc203fe/src/server/raft_client.rs)
collect messages by count/bytes and flush them onto a client stream. Its
[stream receiver](https://github.com/tikv/tikv/blob/b3995dab28308f09cbb7c125bf32f789dbc203fe/src/server/service/kv.rs)
dispatches batch members without a separate response round trip for each one.
Cellar preserves its authenticated per-frame admission responses, but can avoid
waiting for each response before writing the next queued frame.

The proposed sender drains up to 16 already queued frames with a 1-MiB window;
one larger permitted frame occupies a window by itself. An overflow frame keeps
its FIFO position and original byte/item permits. Concurrent bounded reads and
writes avoid a full-duplex socket buffer deadlock. Every reply still binds the
channel, sequence, route and request tag. Error or cancellation discards the
connection; accepted transport frames remain distinct from durable Raft proofs.
No waiting interval or additional worker is introduced.

The real TLS regression queues two different groups and withholds the first
reply until the second authenticated frame arrives. The original sender times
out; the proposed sender passes. Additional tests reject a mismatched reply and
release all admission, and exchange 16 frames over 64-byte duplex buffers.
The final sender passes 590 Rust cases: 445 library, 27 host, 19 operations,
16 runtime, 14 transport, 27 membership and 42 snapshot cases. Five standalone
process helpers run through their parent scenarios. Affected-target strict
Clippy, module layout, dependency policy, documentation and eight Python harness
cases pass. The previously failing management-stop case passes locally; the
Linux CI failure remains unexplained. A fresh end-to-end comparison is pending.

## Remaining qualification

- Reproduce and resolve the Linux management-stop admission-seal failure, then
  run affected native regressions for the final implementation.
- Record batch occupancy and stage-level I/O evidence on diagnostic runs.
- Compare fresh baseline and candidate clusters with identical resources,
  durability, cell counts, client partitioning and archive scratch placement.
  Avoid builds or profilers during timed windows; retain repeated raw samples.
- Require exact values on all 4,500 replicas, archive coverage for all 1,500
  cells, and zero ambiguous outcomes or admission errors in selected windows.
- Publish measured results separately from architectural expectations. TiKV
  mechanism reuse alone is not a performance or durability qualification.
