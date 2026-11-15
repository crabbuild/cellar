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

Current-head Python examples and JavaScript/Node hosted workflows pass. Typed RPC's standard-Python full matrix fails during proof-wait warmup after `NodeLeaseMismatch` fences the host; the free-threaded full matrix passes. No isolated cause or suppression is claimed. Cellar SDK CI is pending at report preparation.

A broader host snapshot at **2026-11-15 12:38:57 UTC** observes another checkout's primitive qualification plus two active VMs. The narrow compiler monitor misses the executable named `primitives`; its absence is not a quiet-host proof. This campaign cannot establish an isolated causal gain or regression. All observations/failures remain retained; no unrelated work is stopped.

All 75 full-run participant containers stop without observed OOM or restart. Failed disks and containers remain retained. All comparison owners join; frozen inputs and engine identity remain unchanged. The original RustFS identity, limits and mounts are restored, with independently confirmed HTTP 200 readiness. One Colima VM supplies one physical failure domain; cloud S3, partitions, crash recovery, all-voter disk loss and sustained capacity remain separate gates.

[Machine assessment](three-node-primitive-main-173-2026-11-15.json) and [raw capsule](three-node-primitive-main-173-2026-11-15.raw.json.gz) retain complete emitted samples/ledgers, resource curves, topology/performance observations, terminal states, build equivalence, both smokes and failure diagnostics. Capsule: 22,483,380 bytes, SHA-256 `8b81524d454b97552651646fb609c8f4506a59f121b8c49511723639460a2b68`. Runtime configuration, environment dumps, private keys and node/store log text are excluded. Thirteen actual runtime credential/private-key representations are checked privately against the emitted data.

Next priorities are the original-bound S3 and bucket Queue progress failures, controlled profiling of image preparation/sync and public read integration, then remaining authority, physical reclamation/once-only credit, placement/fairness and lifecycle/crash gates. No complete OPT-01–08, CG-17 or release acceptance is claimed. See the [prioritized gaps](optimization-progress.md#priorities-for-completing-the-optimization).
