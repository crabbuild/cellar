# Multi-Raft control-window performance

The selected change increases the per-peer control window from eight to sixteen
frames. Heartbeats and their responses accounted for 78.5–78.9% of admitted
control frames in a separate diagnostic run. Append responses waited 22–36 ms
on average, compared with 2.7–3.3 ms for exchange and 0.11–0.13 ms for admission.
That diagnostic used twenty writers and had archive refusals; it is explanatory
evidence, not an error-free throughput qualification.

The larger window preserves FIFO authenticated replies, per-reply refill,
physical-write completion, deadlines, route revocation, node/peer reservations,
receiver admission and quorum acknowledgement. The existing byte/item budgets
still bound selected and queued work. Data-lane batching is unchanged.

## Matched measurements

Control is main `5f92e2c72c774d14a0e26d453b48d25437de54ce`; candidate runtime is
`65edcc9e75b5172e6ed7b36b93145a3538b1f31d`. PR #98 had already merged.
Both images use Rust 1.98.1 and the same load-generator AST, RustFS image and
Python image. Only the control sender and its tests differ among the 118 frozen
source inputs. Each cohort starts with fresh data and a cold Colima VM.

Three Cellar nodes each have one pinned vCPU and 3 GiB RAM, with MemorySwap equal
to Memory. Each has 500 leaders and 1,000 followers: 1,500 three-voter groups in
total. RustFS has a separate two-vCPU pool and 4 GiB RAM; the load generator has
another two-vCPU pool. SQL uses four workers and 64 reservations per node.
Archive settings remain four producers, two groups per batch and a 256-MiB
unarchived-byte budget. Each build runs two 180-second windows with eight
closed-loop writers. Each transaction increments one cell's counter and waits
for quorum acknowledgement. TPS below is cluster-wide successful transactions
per second. There are no timed mutation retries.

| Order | Build | TPS, windows 1 / 2 | p99 ms, windows 1 / 2 | Errors |
| --- | --- | --- | --- | --- |
| Control first | Main | 261.58 / 224.14 | 76.57 / 98.92 | 0 / 0 |
| Control first | Window 16 | 274.15 / 235.46 | 65.18 / 79.99 | 0 / 0 |
| Candidate first | Window 16 | 267.21 / 237.76 | 73.38 / 77.48 | 0 / 0 |
| Candidate first | Main | 270.37 / 214.22 | 82.86 / 160.83 | 0 / 0 |

Pooled successful throughput improves 4.92% in the first order and 4.20% in
the reversed order. Across both orders it is **242.58 → 253.65 TPS (+4.56%)**.
Candidate p99 is lower in every matching window. The worst window p99 is
**160.83 → 79.99 ms**; this is not a pooled latency percentile. One matching
window has 1.17% lower candidate TPS, so throughput does not improve uniformly.

Mean control-queue time falls from 26.65 / 32.78 to 17.28 / 21.48 ms in the
first order, and from 34.04 / 45.89 to 16.90 / 20.76 ms in the reversed order.
All four cohorts verify exactly 4,500 replica values and 1,500 archive positions,
then stop cleanly without OOM. All timed windows are error-free and below the
200-ms preferred p99 bound.

These are shared-host observations. Compiler activity appears in 0/37 and
20/38 baseline samples versus 0/36 and 0/37 candidate samples in the first
order; reversed-order samples are 4/36 and 26/36 for main versus 13/36 and 0/36
for the candidate. Host swap usage is about 15 GiB. The repeated lower queue
time and p99 support retaining this small change; the exact throughput gain
is not an isolated causal estimate. This short, closed-loop counter workload
does not establish maximum sustainable TPS or larger-payload performance.
Twenty-writer baseline runs still had archive-admission refusals. This change
does not claim to resolve that overload.

## Validation and evidence

The real TLS test first fails with a sixteen-frame expectation against the
eight-frame sender: the ninth frame cannot arrive before a reply. It passes
after the change. Tests retain the full-window bound, immediate single-slot
refill, partial I/O, lost/reordered replies, timeout, revocation, cancellation,
joined shutdown and reservation-accounting assertions. Native validation passes
39 transport tests, 29 host tests and nine multi-group tests. The ignored crash
child is launched by its parent test, including cross-group crash recovery.
Formatting, module layout, native dependencies, documentation integrity and
`cargo clippy -p cellar --all-targets --all-features --locked -- -D warnings`
also pass. Full workspace and release qualification remain CI responsibilities.

Frozen images, source manifests, load records, host observations, replica/archive
checks and shutdown records remain local under `.cellar/raft-perf/`. The paired
summaries are `diagnosis/control-window16-c8-summary.json` and
`diagnosis/control-window16-c8-reversed-summary.json`; the combined decision is
`diagnosis/control-window16-decision.json`. Raw evidence is excluded from Git,
including every new file under `plans/multi-raft/evidence/`.

## Remaining work, in priority order

1. **Archive publication:** replay-workspace waiting measured only 0.05–0.06 ms
   and physical replay 1.04–1.16 ms in the earlier diagnostic. Object publication
   and repeated replica work deserve the next investigation. Head PUT failures
   were 36–42%, but their causes were not classified. Classify those failures and
   repeated publication before designing coordination or changing recovery.
   Reusing one confirmed manifest to avoid a duplicate GET failed its matched
   end-to-end acceptance gate and is excluded, along with all diagnostic probes.
2. **Control queueing:** retain only the measured window change. No election
   congestion was observed. Priority scheduling or heartbeat coalescing needs
   its own fairness, election and performance evidence.
3. **SQLite images:** separate copying, validation, file synchronization,
   directory synchronization and publication timings. Delayed checkpoints need
   distinct applied/checkpointed progress and crash-safe replay retention;
   removing synchronization without those contracts is not an optimization.
4. **Archive batch utilization:** measure flushes caused by the five-millisecond
   deadline, a full batch and compatibility boundaries before changing collection
   or batch size. Preserve current byte, memory and capture-permit bounds.
5. **Idle groups:** measure wakeup and scheduling cost before changing ticks.
   Heartbeats, elections, persistence/apply sequencing and external wakeups must
   remain correct. No activity-aware scheduler is included in this change.
