<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Main-173 three-node primitive verification

Purpose: Record the refreshed constrained SQL/KV/Queue comparison, complete verification ledgers and remaining progress failures.

**Result: FAILED. No isolated throughput or latency improvement is established.** Nine of twelve candidate cases pass. KV+S3, Queue+S3 and bucket Queue fail; the quorum control passes SQL/KV and fails Queue. SQL/KV have complete passing matched quorum pairs; Queue does not.

The source cut is `69654995d602d7e33bd8f0d1590574a5cd67a1b6`, including main `189edd9a` (#173 Blob providers and retained-payload recovery). Candidate manifest is `803d1d85640e31861c9beac6f7ede5eb36a72fd27a57cfff8af0adf95a7baf00`; control manifest is `67e4310de43e72eb614eb261bccd3be0e1acda5d460fbe46fc56688f0ec534ae`. The control reverses only `97b805f3`'s nine LTX image-publication inputs, with inverse patch SHA-256 `cd60a5972c519a7d92e688e4286a3c797c004ac7706d2a797edeb43717e03bb3`.

Both images use locked Rust 1.98.1 and matching size-optimized `lab`, thin-LTO, 16-codegen-unit, line-table profiles. This follow-up changes only the Python validator, its regression and guide. Node and load-client bytes are reused exactly from completed `e9509a2f` builds, with full old/new source manifests verified inside their materialized builders. New image metadata binds the frozen source cut; no Rust recompilation is claimed. Self-contained detached Git metadata prevents live branch changes from relabeling the run.

Each case has three distinct 1-vCPU/2-GiB/no-extra-swap nodes, three RF3 groups in Raft modes, separately capped RustFS and client containers, 24 SQL/KV cells or 24 queues plus 24 DLQs, and 256-byte values. C1/4/16 runs twice in reversed concurrency order, with 5-second warmups and 30-second timed windows. Five-second actor operation, callback and lease bounds remain. Leaders are balanced before load; later elections are retained.

## Complete matrix

V/F/U means verified, failed and unmeasured. A failed case supplies no complete comparison rate, even when earlier windows verify.

| Campaign | API | Mode | Result | Timed V/F/U | Warmup V/F/U | Request errors |
| --- | --- | --- | --- | --- | --- | --- |
| control | sql | raft-quorum | PASSED | 12/0/0 | 6/0/0 | 0 |
| control | kv | raft-quorum | PASSED | 12/0/0 | 6/0/0 | 0 |
| control | queue | raft-quorum | FAILED | 2/1/3 | 3/0/3 | 0 |
| candidate | sql | raft-quorum | PASSED | 12/0/0 | 6/0/0 | 0 |
| candidate | sql | raft-s3 | PASSED | 12/0/0 | 6/0/0 | 0 |
| candidate | sql | fleet | PASSED | 12/0/0 | 6/0/0 | 0 |
| candidate | sql | bucket | PASSED | 12/0/0 | 6/0/0 | 0 |
| candidate | kv | raft-quorum | PASSED | 12/0/0 | 6/0/0 | 0 |
| candidate | kv | raft-s3 | FAILED | 4/0/8 | 2/1/3 | 2 |
| candidate | kv | fleet | PASSED | 12/0/0 | 6/0/0 | 0 |
| candidate | kv | bucket | PASSED | 12/0/0 | 6/0/0 | 0 |
| candidate | queue | raft-quorum | PASSED | 6/0/0 | 6/0/0 | 0 |
| candidate | queue | raft-s3 | FAILED | 2/1/3 | 3/0/3 | 4 |
| candidate | queue | fleet | PASSED | 6/0/0 | 6/0/0 | 0 |
| candidate | queue | bucket | FAILED | 2/1/3 | 3/0/3 | 10 |

Candidate: **104 verified, two failed and 14 unmeasured timed windows**, one failed warmup and **16 request errors**. Control: 26 verified, one failed and three unmeasured timed windows. Failed windows remain failed; no fresh-ID mutation retry is issued.

## Observational rates and latency

Only complete passing cases appear below. Each cell is **requests/second / pooled p99 milliseconds**, combining both repetitions. These are contended-host observations, not isolated causal improvements or maximum capacity.

| Campaign | API | Mode | Operation | C1 | C4 | C16 |
| --- | --- | --- | --- | --- | --- | --- |
| control | sql | raft-quorum | write | 18.52 / 150.32 | 24.14 / 639.42 | 20.43 / 2381.48 |
| control | sql | raft-quorum | read | 18.70 / 144.23 | 24.74 / 565.38 | 24.12 / 2196.11 |
| control | kv | raft-quorum | write | 18.06 / 112.42 | 27.59 / 459.89 | 28.86 / 2439.28 |
| control | kv | raft-quorum | read | 17.50 / 199.19 | 26.57 / 488.50 | 24.58 / 2728.52 |
| candidate | sql | raft-quorum | write | 15.58 / 185.73 | 20.57 / 519.05 | 22.21 / 2166.90 |
| candidate | sql | raft-quorum | read | 16.11 / 165.27 | 20.67 / 543.70 | 21.08 / 2503.87 |
| candidate | sql | raft-s3 | write | 8.08 / 361.16 | 14.21 / 982.18 | 15.02 / 3537.94 |
| candidate | sql | raft-s3 | read | 8.52 / 315.46 | 13.22 / 1214.67 | 16.99 / 3050.07 |
| candidate | sql | fleet | write | 180.62 / 12.31 | 338.43 / 26.20 | 630.19 / 67.39 |
| candidate | sql | fleet | read | 1574.14 / 3.06 | 5208.15 / 1.70 | 8862.56 / 5.46 |
| candidate | sql | bucket | write | 211.54 / 13.40 | 385.47 / 40.37 | 602.36 / 193.95 |
| candidate | sql | bucket | read | 1684.84 / 1.15 | 5254.10 / 1.63 | 9870.16 / 3.81 |
| candidate | kv | raft-quorum | write | 15.19 / 383.97 | 16.50 / 2835.07 | 20.58 / 3145.70 |
| candidate | kv | raft-quorum | read | 11.88 / 553.01 | 15.72 / 1747.54 | 18.36 / 3152.93 |
| candidate | kv | fleet | write | 52.55 / 173.12 | 164.55 / 381.20 | 215.86 / 598.30 |
| candidate | kv | fleet | read | 1705.18 / 1.08 | 5261.59 / 1.51 | 9394.33 / 4.04 |
| candidate | kv | bucket | write | 194.94 / 16.69 | 389.68 / 40.90 | 549.00 / 138.60 |
| candidate | kv | bucket | read | 1798.92 / 1.08 | 5331.55 / 1.64 | 7994.76 / 6.45 |

Queue acknowledgement latency is per eight-message batch. Verified throughput below includes enqueue time, delivery verification and durable settlement.

| API/mode | Concurrency | Enqueue messages/s | Verified messages/s including drain | Batch p99 ms |
| --- | --- | --- | --- | --- |
| queue/raft-quorum | 1 | 49.04 | 44.41 | 555.98 |
| queue/raft-quorum | 4 | 74.88 | 46.04 | 1475.69 |
| queue/raft-quorum | 16 | 111.99 | 46.76 | 3829.39 |
| queue/fleet | 1 | 251.94 | 251.37 | 103.99 |
| queue/fleet | 4 | 527.78 | 525.35 | 196.40 |
| queue/fleet | 16 | 962.08 | 958.70 | 449.53 |

The matched SQL C16 write observation is +8.7% throughput with about 9% lower pooled p99; most other SQL bins are slower. KV throughput is 16–41% lower across matched bins, with worse p99. Neither direction is accepted as an isolated effect of the LTX change. Later reverse-order windows slow down and unchanged engine-sync timings also vary. A crossover repeat and timestamped I/O/profile diagnosis are required.

Fleet can acknowledge peer replication or a bucket proof; bucket, quorum and quorum+S3 have different durability contracts. Shared-group public SQL reads currently use the durable command path. Fleet/bucket read rates therefore do not establish equivalent read algorithms or justify bypassing admission for writable SQL. A public read-only route must preserve fresh ReadIndex authority, exact source/image pins and mutation refusal; KV reads can also mutate TTL/metrics state.

## Failure evidence

- **Control Queue quorum:** first C16 enqueue acknowledges 5,136 messages; 5,008 are observed at the drain bound. All producer requests succeed. Five queues account for 128 then-unobserved IDs; subsequent metrics leave one queue with backlog 16 and no dead letters. Leaders remain ready. Incomplete progress does not prove lost acknowledged data.
- **Candidate KV+S3:** C16 warmup makes 48 attempts, with 46 successes and two ambiguous five-second deadline errors. Readback later times out after eleven observations. Group 0 changes leader from node 1 to node 2, which already leads group 1. Tick admission continues. Several file-sync and image-preparation durations exceed one second; aggregate histograms do not establish the failed command's causal path. Both writes remain unconfirmed.
- **Candidate Queue+S3:** C16 enqueue makes 272 attempts, with 268 successes and four ambiguous deadline errors. All 2,144 acknowledged messages are observed, plus 24 extra IDs. Their IDs and body hashes match three uncertain attempts. The remaining uncertain batch is unobserved; zero backlog/dead letters at the metrics cut does not prove its noncommit. Observation supplies no missing S3 receipt, so the window remains failed.
- **Candidate bucket Queue:** C16 enqueue makes 1,175 attempts, with 1,165 successes and ten `DurabilityUnproven` errors. There are 9,320 acknowledged messages, eight acknowledged IDs still unobserved, 32 extra IDs and residual backlog across four queues. The load exits 1; nodes/store stop normally without OOM/restarts. The sampler detects the failed-load exit; terminal collection preserves all six emitted windows. No node/store crash is inferred from that sampler error.

## Verification and measurement limits

The first refreshed smoke passes SQL/KV but rejects Queue's valid `0.0` counts. `QueueMetrics` exposes `f64`; the Python validator incorrectly requires integers. The regression and saved windows reproduce this rejection before repair. The correction accepts finite, nonnegative whole-number JSON counts, excluding booleans/fractions/nonfinite values. Complete observer rounds, exact IDs/bodies/duplicates and zero backlog/dead letters remain mandatory. The 341-test performance suite succeeds with one existing skip. Saved-window replay and the fresh three-family quorum smoke pass; the original smoke remains failed.

Rust qualification remains scoped to unchanged production inputs at manifest `f4505de5`: six selected current-main primitive commands, owning checkpoint/archive controls, and native/RustFS retained-source and removed-member controls pass. A whole-library follow-up passes 1,192 tests with two existing ignores, after a preceding run fails one park/wake `Closed` case. That case passes 103 isolated attempts; the original failure remains unexplained. Full current-main primitive/backend/feature/crash qualification remains open.

At controller head `69654995`, Python examples and JavaScript/Node hosted workflows pass. Typed RPC's standard-Python full matrix fails during proof-wait warmup after `NodeLeaseMismatch` fences the host; the free-threaded full matrix passes. No isolated cause or suppression is claimed. Cellar SDK CI is pending at that report preparation.

A broader host snapshot at **2026-11-15 12:38:57 UTC** observes another checkout's primitive qualification plus two active VMs. The narrow compiler monitor misses the executable named `primitives`; its absence is not a quiet-host proof. This campaign cannot establish an isolated causal gain or regression. All observations/failures remain retained; no unrelated work is stopped.

All 75 full-run participant containers stop without observed OOM or restart. Failed disks and containers remain retained. All comparison owners join; frozen inputs and engine identity remain unchanged. The original RustFS identity, limits and mounts are restored, with independently confirmed HTTP 200 readiness. One Colima VM supplies one physical failure domain; cloud S3, partitions, crash recovery, all-voter disk loss and sustained capacity remain separate gates.

[Machine assessment](three-node-primitive-main-173-2026-11-15.json) and [raw capsule](three-node-primitive-main-173-2026-11-15.raw.json.gz) retain complete emitted samples/ledgers, resource curves, topology/performance observations, terminal states, build equivalence, both smokes and failure diagnostics. Capsule: 22,483,380 bytes, SHA-256 `8b81524d454b97552651646fb609c8f4506a59f121b8c49511723639460a2b68`. Runtime configuration, environment dumps, private keys and node/store log text are excluded. Thirteen actual runtime credential/private-key representations are checked privately against the emitted data.

Next priorities are the original-bound S3 and bucket Queue progress failures, controlled profiling of image preparation/sync and public read integration, then remaining authority, physical reclamation/once-only credit, placement/fairness and lifecycle/crash gates. No complete OPT-01–08, CG-17 or release acceptance is claimed. See the [prioritized gaps](optimization-progress.md#priorities-for-completing-the-optimization).

## Command-phase follow-up

Observer source `337abc29`, compiler manifest `cc89c945` and image `b49af6da`
add bounded aggregate timers without changing deadlines, admission, authority,
acknowledgement or Ready/sync ordering. Five metric-lifetime tests and three
owning S3 controls pass, including held publication during a later apply. Strict
all-target Cellar Clippy, formatting/layout, docs and native-dependency checks
pass. The Linux build verifies all 3,425 compiler inputs and rebuilds Cellar,
LTX and both node binaries with the same locked Rust 1.98.1 lab profile. The load
client is byte-identical to the completed comparison above.

The original C1/4/16, two-repetition S3 profile is repeated in fresh namespaces
under the same container and five-second limits:

| API | Verified windows | Failed | Unmeasured | Request errors |
| --- | --- | --- | --- | --- |
| KV | 18 | 0 | 0 | 0 |
| Queue | 5 | 1 | 6 | 1 |

A preceding uninstrumented KV rerun fails at the first C16 read with 538 of 547
requests successful and nine errors, despite unchanged leaders/terms. The
observer-only pass does not establish a deadline fix. Queue's C16 enqueue makes
340 attempts with 339 successes and one ambiguous deadline. All 2,712
acknowledged IDs/bodies are observed; eight extra IDs/bodies exactly match the
one uncertain attempted batch. That observation supplies no missing S3 receipt.
Reported backlog/dead-letter samples are zero, but delivery-set and settlement
qualification remain failed. No fresh-ID mutation retry is issued.

The new phase observations combine all three nodes over each case:

| API | Phase | Completed observations | Mean ms | Failed observations |
| --- | --- | --- | --- | --- |
| KV | Local command | 5,943 | 313.22 | 0 |
| KV | S3 coverage | 5,943 | 220.14 | 0 |
| KV | Proof freshness | 5,943 | 1.68 | 0 |
| KV | Resumed output wait | 230 | 26.47 | 0 |
| Queue | Local command | 1,728 | 695.95 | 1 |
| Queue | S3 coverage | 2,448 | 366.70 | 1 |
| Queue | Proof freshness | 2,447 | 5.26 | 0 |
| Queue | Resumed output wait | 60 | 46.06 | 0 |

These are inclusive, overlapping timers, including verification/background work;
they must not be added. Coverage also measures native RPC checkpoint commits,
so its count need not equal outer commands. An individual output check may
refuse while apply catches up and later succeed; such attempt failures are not
request failures. The Queue case records one coverage failure and no
proof-freshness/output-wait failure. This narrows the next investigation toward
archive coverage admission/publication/replay. Aggregate snapshots do not link
that counter to a particular request or identify the earlier deadline's cause.

All groups retain their initial leader/term in both observer runs. All six nodes
and two stores stop normally, without observed OOM/restarts; the failed Queue
load exits one as required. Owners join and the original RustFS identity,
limits and mounts are restored with HTTP 200 readiness. Host qualification
activity occurs during both cases; no isolated speedup is accepted. Queue uses
24 workload entities plus 24 DLQs, with 16 cells per group; the driver description
mentions 24 cells, while the retained runtime snapshots record the actual 48.

Read-only reconciliation also closes the earlier bucket Queue's extra-delivery
identity gap: all 32 extra IDs/bodies match four of its ten uncertain batches.
Eight acknowledged IDs remain unobserved at the original drain bound, and 48
uncertain message IDs are unobserved. Its durability/drain failure remains open.

The [phase assessment](three-node-primitive-main-173-2026-11-15.command-phases.json)
and [raw phase capsule](three-node-primitive-main-173-2026-11-15.command-phases.raw.json.gz)
retain full windows, ledgers, phase/resource observations, terminal states,
source/build manifests and the preceding failed KV reproduction. Runtime
configuration, environments, private keys and node/store log text are excluded;
actual owned runtime credentials/private-key representations are scanned before
publication. This follow-up accepts no complete primitive matrix or OPT/CG gate.

## Checkpoint object-window comparison

Source `8f674406`, compiler manifest `948c229d` and Linux image `0ccf1de9`
replace serialized checkpoint object I/O with two ordered futures inside the
existing reservation. The source-matched build verifies all 3,426 inputs,
rebuilds both owning libraries and binaries, and retains the baseline's locked
Rust 1.98.1 lab profile and byte-identical load client. The
[native qualification record](checkpoint-object-window-qualification-2026-11-15.json)
retains the failing-before overlap regressions, complete default archive target
(38 passed, ten explicit ignores), three S3 controls and six selected real
RustFS controls. Those passes establish bounded overlap and scoped correctness.

The six S3 cases use fresh namespaces and the original three 1-vCPU/2-GiB RF3
nodes, three groups, 24 workload entities (Queue plus 24 DLQs), separately capped
RustFS/client, C1/4/16, five-second warmups, 30-second timed windows, two reversed
concurrency repetitions and five-second operation/callback/lease bounds. The
baseline is observer source `337abc29` / manifest `cc89c945` / image `b49af6da`.
Image order alternates across APIs; this is not a per-API crossover.

| API | Image | Verified windows | Failed | Unmeasured | Request errors |
| --- | --- | --- | --- | --- | --- |
| SQL | baseline | 18 | 0 | 0 | 0 |
| SQL | window | 6 | 1 | 11 | 6 |
| KV | window | 6 | 1 | 11 | 8 |
| KV | baseline | 9 | 1 | 8 | 1 |
| Queue | baseline | 4 | 1 | 7 | 9 |
| Queue | window | 3 | 1 | 8 | 2 |

**The comparison fails, with no complete passing pair for any API.** The window
candidate records 16 request errors versus ten for the baseline. Candidate
timed windows are nine verified, one failed and 20 unmeasured; baseline timed
windows are 20 verified, none failed and ten unmeasured. Warmup failures stop
later measurement. Partial windows and different completed concurrency mixes
cannot establish a throughput/latency gain. Other-checkout qualification
activity is observed, so no isolated causal improvement or regression is
accepted.

SQL/window and KV/window fail the first C16 warmup, with 21/27 and 21/29
successful requests respectively. KV/baseline fails the second C16 warmup with
52/53 successful requests. Readback completes in these three cases, but request
failures remain unconfirmed and the windows remain failed. All six cases retain
their initial leaders and terms; an election is not necessary for the failures.
SQL/window records four S3 coverage failures, with no proof-freshness or resumed
output-wait failure. These inclusive aggregate counters do not link a failed
phase to an individual request. No individual engine-sync, SQL queue/preparation
or object get/create/update observation exceeds one second in that case; many
short operations and admission/retirement waits remain possible causes.

Queue/baseline fails its first C16 warmup with 39/48 successful enqueues. All
312 acknowledged messages are observed; 40 extra IDs/body hashes match five of
nine uncertain batches, leaving 32 uncertain IDs unobserved. Queue/window fails
at the first C4 enqueue with 70/72 successes. All 560 acknowledged messages are
observed; 16 extras match both uncertain batches. Reported backlog/dead-letter
samples are zero, but delivery-set and settlement qualification still fail.
Observation supplies no missing S3 receipt or noncommit proof. No fresh-ID
mutation retry is issued.

All 30 participant containers stop without observed OOM/restarts. Nodes and
stores exit zero; failed load clients exit one. The sampler's failed-load exit
does not establish a node/store crash. All owners join, frozen source/engine
checks pass, and original RustFS identity, limits, mounts and HTTP 200 readiness
are restored and independently checked. One Colima VM remains one physical
failure domain.

The [window assessment](three-node-primitive-main-173-2026-11-15.checkpoint-window.json)
and [raw window capsule](three-node-primitive-main-173-2026-11-15.checkpoint-window.raw.json.gz)
retain all emitted windows, identity/body ledgers, full phase histograms,
resource curves, source/build manifests and terminal/restoration evidence.
The capsule is 807,657 bytes, SHA-256
`b70dc862825faff9685aefbe7811bdd5d74b00a27953a5f08630550e9c185174`.
Twenty-three actual runtime credential/private-key representations are scanned
privately with zero matches. Runtime configuration, environment responses,
private keys and node/store log text are excluded. Existing archive-stage
logging is the next diagnostic; traced timings will not qualify performance.
The original deadlines, S3 coverage and broader OPT/CG gates remain open.

## Archive-stage diagnostic

The unchanged window image `0ccf1de9` repeats SQL/S3 with existing
`cellar::raft::group_archive` debug logging added by a separately hash-bound
controller hook to initialization and the three node commands. All 18 original
windows pass without request errors. Logging changes timing, so this diagnostic
does not explain the preceding failures, establish a deadline fix or qualify a
performance gain. The original source, client, topology, resource limits and
five-second bounds remain unchanged. Owners join and the original RustFS
identity/limits/mounts/readiness are restored and independently checked.

One publication job per node makes stage pairing possible between successive
launch events. The trace includes 3,308 published cuts, 5,794 other completed
coverage jobs, 2,421 maintenance completions and 4,062 attempts without a
terminal stage. The latter are unclassified attempts, including possible
retryable refusals; they cannot be counted as failed requests. Every logged
settlement leaves zero waiters in that selected batch.

| Paired stages | Count | Mean ms | p99 ms |
| --- | --- | --- | --- |
| Captured to uploaded | 3,309 | 31.22 | 95.29 |
| Uploaded to verified | 3,308 | 52.70 | 243.27 |
| Verified to published | 3,308 | 11.13 | 30.24 |
| Coverage to settled | 9,102 | 1.26 | 11.21 |

These timestamps include background work and lack request linkage. Stage gaps
overlap command metrics and are not additive request budgets. The largest
completed launch-to-settled interval is 1.75 seconds. There is no failing request
in this trace to attribute to a particular stage.

The [stage assessment](three-node-primitive-main-173-2026-11-15.archive-stages.json)
and [raw stage capsule](three-node-primitive-main-173-2026-11-15.archive-stages.raw.json.gz)
retain parsed events, full windows, aggregate metrics/resources and joined
source/build/cleanup evidence. The capsule is 692,604 bytes, SHA-256
`aa4ae9965c4e0c441b8aa8710ff5ec32601381c70ef2c4039b51945f1ace21aa`.
Eight actual runtime credential/key representations are scanned privately with
zero matches. Node/store log text, configuration, environments and private keys
are excluded. Code inspection and the differential controls below identify a
second image/manifest pass for empty tails. This trace does not attribute a
deadline failure to that extra work.

## Empty-tail recovery qualification

Compiler manifest `a37458ee` retains the exact fresh physical preparation from
the ordinary checkpoint receiver when the tail has no entries. Owner, bounds,
complete cut/configuration/membership, cancellation and liveness checks still
run. Every remote dependency and existing local image is independently
received/verified/synced. Nonempty tails retain full replay and final
verification. Required payload inspection and publication/response authority
remain; no deadline, admission bank or format changes.

The real RF3 differential control receives the same cut and independently
inspects payload-table inventory through the immutable SQL adapter. It validates
canonical empty cells and exact recovered bytes, rejects a rehashed wrong cut
and damaged warm image, and checks retained/returned quotas. Source `1b3f9a13`
without the optimization fails both final regressions; `a37458ee` passes them.

| Image mode | Original empty tail | Complete control | Candidate empty tail | Fewer calls |
| --- | --- | --- | --- | --- |
| Materialized | 47 | 32 | 32 | 31.9% |
| Extents | 62 | 44 | 44 | 29.0% |

These are filesystem-provider `read_exact_at` calls in a three-cell fixture.
Checkpoint reception alone accounts for 26/32 calls; the additional 6/12
payload-inventory reads remain required. They are not physical device reads or
SQL/KV/Queue throughput/latency measurements.

Strict all-target Cellar Clippy, formatting/layout (2,323 Rust files), native
dependencies and documentation checks pass. The default archive target passes
**40 / zero / ten explicit ignores**, plus three owning S3 output/result/deadline
controls. Six selected real RustFS controls pass for physical cancellation,
competing publication, exact results, deadlines, sibling dependencies and payload
recovery/GC. Three of the archive target's declared backend ignores are executed
separately; other ignored cases remain unrun. All owners join, input hashes stay
unchanged and original RustFS identity/limits/readiness remain intact.

The [qualification record](empty-tail-recovery-qualification-2026-11-15.json)
retains actual profiles, source/executable/log hashes, footers and counters.
It also retains the duplicate-helper Clippy rejection, the failed initial
checkpoint-only control (38 other archive cases and three S3 controls passed),
and the canonical-empty control setup failure. Required work is added to the
independent control; exact equality, cut, corruption and ownership assertions
remain. Failed fixtures retain their disks.

The matching Linux build succeeds; the original constrained comparison below
fails. These in-process node fixtures establish scoped correctness and fewer
provider read calls. They establish no deadline fix, end-to-end speedup or
complete primitive/OPT/CG acceptance. One Colima VM is still one physical failure
domain.

## Empty-tail constrained comparison

**All six S3 cases fail; no API has a complete passing pair.** Source `1d72a31d`
and compiler-input manifest `a37458ee` produce immutable image `9b98065f`.
The checkpoint-window control remains image `0ccf1de9` at source `8f674406`.
Both use the same locked Linux Rust 1.98.1 compiler and lab artifact profile
(`opt-level=s`, line-table debug information, debug assertions and overflow
checks disabled), and byte-identical load client. Source manifests and Git/OCI
revision checks pass; the changed owning libraries and binaries are rebuilt.

Fresh namespaces retain three 1-vCPU/2-GiB RF3 voters, three groups and 24
workload entities; Queue also retains 24 DLQs. RustFS and the client are each
separately capped at 1 vCPU/2 GiB. The original five-second operation/callback/
lease bounds, five-second warmup, thirty-second windows, C1/4/16 and two reversed
concurrency repetitions remain. The fixed image order alternates across APIs;
this is not an image-order crossover. Another checkout's qualification activity
is observed and no isolated causal gain is accepted.

Counts include warmup; unmeasured windows are retained explicitly:

| API | Image | Verified | Failed | Unmeasured | Request errors |
| --- | --- | --- | --- | --- | --- |
| SQL | Control | 5 | 1 | 12 | 4 |
| SQL | Candidate | 2 | 1 | 15 | 1 |
| KV | Control | 1 | 1 | 16 | 1 |
| KV | Candidate | 4 | 1 | 13 | 4 |
| Queue | Control | 0 | 0 | 12 | 0; load never starts |
| Queue | Candidate | 4 | 1 | 7 | 1 |

SQL/control fails C4 read and SQL/candidate fails C1 read. KV/control fails C1
write; KV/candidate fails C4 write and its later readback times out. The
candidate's node 3 exits with `NodeLeaseExpired` before the load finishes and is
unreachable in the before-shutdown snapshot. Incomplete phase observations are
unavailable, never zero. The Queue control's node 1 exits with a startup
`Runtime(DeadlineExceeded)` marker before load; the other nodes and RustFS exit
cleanly. These distinct failures remain uncharacterized.

Queue/candidate fails C16 warmup with one uncertain enqueue. All 568 acknowledged
messages are observed with exact bodies, plus eight extra IDs/bodies matching
that uncertain attempted batch. There are no duplicate deliveries or unmatched
extras. Observation establishes neither the missing S3 receipt nor a safe
fresh-ID retry; the window remains failed.

All 29 created node/store/client participants stop, with no observed OOM or
restart. The two failed node exits remain explicit rather than being classified
as healthy shutdowns. All owners join, source/engine identity remains unchanged,
and the original RustFS identity, mounts, limits and HTTP 200 readiness are
restored. Every case starts above the unchanged six-GiB free-disk preflight
reserve; no workload resource or admission limit changes.

The [assessment](three-node-primitive-main-173-2026-11-15.empty-tail.json) and
[raw capsule](three-node-primitive-main-173-2026-11-15.empty-tail.raw.json.gz)
retain failed/partial windows, phase histograms, resource samples, exact Queue
identity reconciliation and role-bound terminal evidence. Runtime configuration,
environments, private keys and raw node/store log text are excluded; a scan of
23 actual private representations finds zero matches. No failed case receives
an aggregate comparative throughput/p99 result. Original-bound lease, startup
and S3 coverage progress remain P0 work.

## Lease and archive diagnostics after empty-tail comparison

Two fresh KV/S3 diagnostics retain the exact `1d72a31d` / `a37458ee` / `9b98065f`
source/image, client, topology, resources and original bounds. The first enables
the existing lease/store trace; the second adds existing archive-stage logging.
Both fail. Logging changes timing, and neither run is a performance comparison.

| Diagnostic | Verified windows | Failed | Unmeasured | Request errors |
| --- | --- | --- | --- | --- |
| Lease trace | 2 | 1 | 15 | 1, C1 read |
| Lease plus archive trace | 11 | 1 | 6 | 8, second C16 read |

All 165 / 576 observed lease attempts apply, with no timeout-wait overrun or
lease-expiry marker. Renewals start with more than three seconds of prior
authority; maximum observed attempt durations are 166 / 387 ms. All six node
processes exit cleanly. These failed request windows therefore do not require a
node-lease expiry. They do not explain the earlier untraced expiry.

The C1 run records one inclusive local/S3-coverage failure and no output-check,
freshness or output-wait failure. The longer C16 run records eight local and six
coverage failures, plus 792 failed output-check attempts and 146 completed
output waits. Check attempts include retries: 792 is not a request-error count,
and inclusive timers cannot establish a per-request causal path.

The archive trace contains 2,402 publications, 4,123 other coverage jobs, 1,781
maintenance completions and 2,765 unclassified attempts without a terminal
stage. The longest completed launch-to-settlement interval is 2.93 seconds;
all 6,525 observed settlements report zero remaining waiters. Expired/canceled
waiters and unclassified attempts prevent that observation from proving every
request succeeded. Fair-queue delay, proof retirement, retry reasons and exact
request-to-coverage linkage remain unresolved; no deadline or proof is relaxed.

The [diagnostic assessment](three-node-primitive-main-173-2026-11-15.lease-diagnostics.json)
and [raw derived events](three-node-primitive-main-173-2026-11-15.lease-diagnostics.raw.json.gz)
retain both failures, all emitted windows, phase/resource evidence and numeric
lease/archive events. Four init/node log filters per run are verified. Raw
node/store log text and runtime secrets are excluded; eleven actual private
representations yield zero matches. Owners join and restore the exact original
RustFS identity/limits with HTTP 200 readiness after each run. This narrows the
diagnosis without establishing a production fix or end-to-end gain.
