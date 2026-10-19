# Counter performance investigation: PR #87

## Finding

The Cloudflare comparison does not isolate PR #87. It compares `7d3beb9`,
before both #86 and #87, with `cf1dca7`, whose tree matches merged `c8555ec`.
The immediate predecessor of #87 is `17f6486`. Reverting #87 based on that
comparison would also revert correctness fixes without establishing causality.

The [counter implementation](../../../examples/counter/main.rs) is byte-identical at `17f6486` and `c8555ec`.
Single-key GET and POST do not use the changed bulk-KV or SQL-accounting paths.
A POST still uses `BEGIN IMMEDIATE`, a single-key lookup/update and COMMIT.
The host alarm lookup does exercise the prepared-query changes. Acquisition
budget propagation affects acquisition/recovery; the three read-only event
binding retries are not used by this counter's empty binding configuration.

## PR #87-only local comparison

The verified builds compare `17f6486` against `c8555ec`, with identical counter
source, lockfile and runtime base layers. Both use native Rust 1.98.1 release
executables on three 1-vCPU/3-GiB containers and local RustFS. The x86-64 binaries
run under QEMU on an ARM64 Colima VM. Both three-node fleets stay running; only
one receives timed load at a time. This isolates revisions more closely than
the Cloudflare comparison, but it is not a Cloudflare performance estimate.

There are 24 windows: GET/POST × hot/3,000 forwarded/3,000 direct-owner counters
× two builds × two repetitions. Each uses 64 clients, 2 seconds of warmup and
8 seconds of timed HTTP/1.1 traffic, waiting for outstanding responses. The
second repetition reverses build and case order. Rebalancing is disabled.
Every boundary checks all 3,000 values, exactly 1,000 owners/residents per node,
resident inventories and deployment versions. All timed responses are HTTP 200;
all strict count/proof checks pass. No POST is retried. All six native processes
subsequently exit zero after explicit preserving shutdowns.

Goodput combines successful responses over total client duration. The p99 below
is the worse per-run p99, not a pooled percentile. These short repetitions do
not establish statistical significance or production SLOs.

| Operation | Routing | Before req/s | After req/s | Change | Before p99 ms | After p99 ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| GET | hot | 1576.7 | 1617.2 | +2.6% | 80.4 | 89.2 |
| GET | many-ingress | 1365.7 | 1399.3 | +2.5% | 163.3 | 95.9 |
| GET | many-owner | 2635.3 | 2696.5 | +2.3% | 73.1 | 74.1 |
| POST | hot | 930.4 | 901.7 | -3.1% | 131.5 | 222.4 |
| POST | many-ingress | 437.8 | 360.5 | -17.7% | 365.2 | 509.2 |
| POST | many-owner | 533.2 | 476.0 | -10.7% | 279.7 | 440.9 |

**The local run also contains write regressions; it does not qualify #87 as a
performance improvement.** Forwarded writes decline in both pairs, by 33.3% and
5.8%. Direct-owner write changes reverse direction, from −39.7% to +23.5%.
This leaves a write-path concern and substantial timing variation. It does not
identify the responsible source hunk or establish a uniform regression across
all operations. Slight aggregate read gains do not compensate for worse writes.

Direct-owner requests consistently outperform forwarding through one ingress.
That difference measures the routing path, not a newly implemented optimization.
The normal-load checks above do not qualify disconnects or recovery; command
receipts are needed to audit individual ambiguous mutations under faults.

## Cloudflare evidence

At 64 clients over 3,000 counters, the measured goodput fell 4.3% for GET and
14.1% for POST. Write p50 and p99 increased. This symptom is real; its attribution
to #87 is unproven. See the [original comparison](2026-10-18-counter-cloudflare-comparison.md) and its rejected baseline window.

A reanalysis of the retained before/after snapshots gives the following mean
host proof waits. These are elapsed proof-counter deltas divided by started
proofs, not request-latency percentiles and not pure follower network RTT.

| Build | Repeat | Write req/s | Mean proof wait ms |
| --- | ---: | ---: | ---: |
| Before #86/#87 | 1 | 91.8 | 426.1 |
| Before #86/#87 | 2 | 88.5 | 442.8 |
| After #86/#87 | 1 | 77.9 | 490.6 |
| After #86/#87 | 2 | 77.1 | 511.0 |

Native CPU consumed 13.8–25.7% of one core across the twelve node intervals;
the Node transport consumed another 1.8–4.7%. These intervals include state
observation overhead. They suggest waiting rather than sustained CPU saturation;
they do not exclude brief scheduler stalls. Fleet proof served almost every
write in these four windows. Bucket fallback alone does not explain their rates.

All four intervals had zero peer-close counter deltas, so the separately
recorded connection closures cannot explain every slow write. The WARN-level
runs did not collect capture/append stage timings; the evidence cannot yet split
proof waits into capture, lane queueing, follower persistence and transport.

The health control also changed from 58–61 ms to 39–40 ms p50. The deployments
used different container cohorts. Their Worker selected a Durable Object with
`locationHint: 'wnam'`; container-region constraints were not configured and
container locations were not collected. Cloudflare documents independent
[container placement](https://developers.cloudflare.com/containers/concepts/architecture/),
[regional constraints](https://developers.cloudflare.com/containers/concepts/placement/),
and [`CLOUDFLARE_LOCATION` / `CLOUDFLARE_REGION` runtime variables](https://developers.cloudflare.com/containers/configuration/environment-variables/). A DO placement hint is insufficient evidence
that compared containers and followers occupied equivalent locations.

## Measurement integrity fix

The first attempted local revision comparison used different source trees but
identical native executables. A shared Cargo target cache reused the later
binary because `git archive` preserved earlier source timestamps. Its 13
completed windows are excluded from the revision comparison and retained as
incidental same-binary observations. The earlier Cloudflare run had distinct
verified baseline/candidate binary hashes; this newly caught build error does
not make those historical images identical.

A tiny Docker regression reproduced the cache error in all three checked-in
performance build recipes: compile current code, then build different historical
code using the same target cache, and inspect the executable's output. All three
returned the current marker before the fix. Refreshing source timestamps inside
the cache-mounted build step makes all three return the historical marker.
Dependencies remain cached; project sources rebuild when that Docker step runs.
No framework behavior, proof policy or retry rule changes.

The corrected local build logs show recompilation of Cellar and its workspace
dependencies. The executable hashes differ and are checked before deployment.
Both fleets have fresh buckets and local volumes; the failed image attempt and
stale control-connection preflight error remain in private evidence. No mutation
was replayed to repair an audit.

## Capture and follower diagnostics

A separate pass uses fresh fleets, with one three-node fleet active at a time,
3 seconds of warmup and one 15-second distributed-write window per build.
INFO is enabled only for the existing replication and log modules. Both passes
match counter increments to acknowledgments, retain 1,000 residents per node,
report zero proof errors and complete preserving shutdowns with exit zero.
These instrumented rates are not the primary WARN comparison.

| Observation | Before #87 | After #87 |
| --- | ---: | ---: |
| Checkpoint runs | 82 | 888 |
| Checkpoint worker time, seconds | 1.3 | 22.3 |
| Mean round capture, ms | 5.9 | 21.7 |
| Mean follower-lane wait, ms | 30.6 | 56.3 |
| Mean append round trip, ms | 16.3 | 37.1 |
| Capture share of sampled ship-loop wall time | 28.5% | 47.7% |
| Pipeline-stall share of sampled ship-loop wall time | 63.3% | 41.9% |

SQLite-busy checkpoint reports are zero in both passes. Checkpoint worker time
is summed across workers; it is not additive with capture wall time or a fraction
of request latency. Loop shares describe sampled node intervals. Append round
trip includes follower service and scheduling; round `ship_ms` additionally
includes ordered application delay. None is a pure network measurement.

The slower window does substantially more maintenance despite completing fewer
writes. `Db::sync` runs `checkpoint_if_needed` before capture returns. Below the
frame/size thresholds, its time policy uses the database file's age and a
60-second interval. Both revisions have identical [LTX checkpoint code](../../../crates/cellar-ltx/src/db.rs).
**Inference:** staggered database ages and clustered time-based checkpoints can
change the work inside a short window and amplify capture/follower waits.
This is a demonstrated comparison confounder and a performance-hardening target;
it does not establish why the original Cloudflare cohorts differed or identify
a defective #87 source hunk. The earlier 15-second windows do not span a full
60-second maintenance cycle.

The existing ordered-stream candidate was also attempted with the same #87
executable and resources. Initial seeding encountered `RemoteDisconnected`;
there was no timed stream window and no POST retry. Native logs include stream
head stalls and HTTP header timeouts. The exact initiating cause is unresolved.
The failed preflight is retained and the candidate is unqualified; switching
transport is not a demonstrated fix.

An earlier diagnostic attempt used a helper that dropped the preserving-shutdown
query and selected ordinary handoff. Its cleanup returned exit 3. That attempt
is excluded; the two diagnostic passes above use fresh buckets and explicit
preserving requests. No store or local volume was erased to repair a run.

## Recommended changes, in order

1. **Make deployment and measurement comparable.** Constrain containers to WNAM,
   record each container's location and boot identity, and measure client-to-node,
   node-to-node and node-to-R2 controls. Compare `17f6486` against `c8555ec` with
   the same application and lockfile. With checkpoint age controlled, isolate
   the storage and recovery hunks instead of reverting the entire PR.
   Preserve the required independent failure
   domains when improving locality. Keep network controls as observations;
   do not subtract their percentiles from request percentiles.
2. **Measure the Cloudflare proof path before changing it.** Use a diagnostic pass
   with existing `log_ship_loop`, `log_ship_round`, `log_append_rtt` and
   `log_append_serve` events. Distinguish capture, blocking-pool wait, per-member
   lane wait, transport/service time and ordered completion delay. Keep WARN
   runs for the primary comparison. Add bounded aggregate request-stage metrics
   only where these observations cannot distinguish routing from handler/proof
   waits; avoid allocating or logging for every request.
3. **Budget time-driven checkpoint maintenance at its owning boundary.** Separate
   mandatory committed-frame capture from deferrable maintenance scheduling in
   the managed Db/replication component. Retain WAL frame/byte limits, writer
   barriers, complete LTX capture and durable staging; threshold pressure must
   still trigger checkpointing or admission backpressure. Use bounded per-node
   maintenance concurrency and spread time-driven work across cells. Checkpoints
   must serialize with the same managed database and activation epoch; domain
   cancellation/shutdown must join entered I/O. This needs a tested design, not
   an asynchronous checkpoint bolted beside `Db::sync`. Reproduce a thousand
   cells becoming due together using a controlled clock before changing runtime
   code. Do not disable checkpoints to obtain a faster number.
4. **Qualify the existing ordered stream with unchanged proof policy.** The
   default HTTP `member_lane` awaits one append response before taking its next
   job. Four ship rounds in flight do not create four concurrent HTTP appends
   to one follower. `CELLAR_LOG_TRANSPORT=stream` already supports ordered
   frames and multiple outstanding replies. First resolve the failed preflight
   above, then compare HTTP and stream with the same pipeline and byte bounds; verify upgrade/backpressure through the real
   Cloudflare bridge. Promote it only if measured lane waits fall, throughput
   improves and p99 meets the chosen budget. A stream cannot remove physical
   network RTT or durable follower persistence. Do not simply raise queue depth.
5. **Reduce avoidable forwarding and metadata work.** Measure direct-owner and
   forwarded requests separately. An ingress owner hint can remove a peer hop,
   but the receiving node must still enforce ownership/epoch admission. Profile
   lease-expired route refreshes before considering coalescing. Keep wall and
   monotonic lease expiry, lineage validation and safe stale-route handling;
   never extend route authority from an old cache entry or replay a mutation
   after an ambiguous send.
6. **Make performance qualification a release gate.** Use repeated paired
   comparisons at fixed offered rates as well as closed-loop saturation tests.
   Match database/checkpoint age and cover several maintenance cycles; record
   checkpoint counts, captured frames and bytes in each build.
   Record goodput, p50/p95/p99, errors, CPU, resident balance and proof mode.
   Agree numerical throughput and tail-latency budgets from the qualified
   baseline. Reject a change that only moves the bottleneck into an unbounded
   queue or improves throughput by increasing tail latency. Keep cold activation,
   steady traffic and recovery results separate.

The runtime stages are proposals; neither the Cloudflare regression nor the
stream preflight has been fixed by a validated runtime change. Keep #87's
scoped authorization, transaction rollback and bounded-acquisition fixes while
isolating any measured regression.

## Correctness gates

A faster result is acceptable only with the same owner/epoch/activation checks,
atomic transactions, SQL authorization, complete WAL capture and required
output-proof policy. Ordered completion cannot credit a later round past an
unresolved/failed predecessor. Followers must persist the acknowledged range
and its recovery evidence before confirming; truncation must remain bounded by
the bucket-covered watermark. Old-ensemble completions cannot release new gates.

Before enabling a transport/default change, run the existing proof, stream,
missing-range, truncation, cancellation, settlement and predecessor-recovery
regressions. Exercise disconnects before/after append persistence, response loss,
slow followers, stream reconnect, epoch replacement, cancellation and supported
node/disk loss. Verify command IDs and retained receipts, not just aggregate
counter totals, for ambiguous operations. No implicit POST retries are allowed.
Missing predecessor witness proof must continue to refuse admission. Full-fleet
ephemeral-disk loss is a different durability contract from fleet acknowledgments.

## Delivered change and validation

The implemented change hardens the three performance Docker recipes against
stale historical builds. The [Docker regression](../test_build_freshness.py)
failed with the wrong executable on all three original recipes and passes all
three after the fix. The Python performance suite discovers 87 tests: 86 pass,
with this opt-in Docker test skipped in the ordinary run; its three recipe
cases pass when enabled. Documentation and whitespace guards pass.

Runtime source is unchanged. The [structured evidence](2026-10-18-pr87-investigation.json)
contains the 24 primary windows, diagnostics, source/binary identities and the
failed stream preflight. All normal-load checks passing is not fault qualification.
Complete raw output, vectors, deployment snapshots, failed attempts and node logs
remain in the checkout-private `pr87-investigation-20261018/` target directory.
