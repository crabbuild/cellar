<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Three-node SQL, KV and Queue durability comparison

Purpose: Record the complete constrained four-mode primitive matrix, failed correctness boundaries and descriptive passing results.

Frozen production source `b72a0075` contains main `16c45b26` and matches public commit
`1d663425` at compiler-input digest `362f78db`. Image `70b02055` rebuilds the owning
libraries and both node binaries. The later foreground-only capacity-cadence fix is not
in this image.

Every case uses three 1 vCPU / 2 GiB nodes and 24 entities (Queue adds 24
dead-letter cells). Raft modes use RF3 and three groups; Fleet and bucket retain
their own acknowledgement contracts. All modes use separately capped shared
RustFS and load, original five-second operation/callback/lease bounds,
five-second warmups, 30-second windows, C1/4/16 and two reversed repetitions.
Values are 256 B; enqueues carry eight messages. Cases run serially on one
Colima VM. Broader host activity was observed; this is not an isolated
before/after optimization experiment.

Four of 12 cases pass; eight fail. The 192 expected windows contain 109 verified, eight
failed and 75 unmeasured windows, with 51 request errors. Of 120 timed windows, 66
verify, seven fail and 47 remain unmeasured. Failed cases receive no aggregate rates.

| API | Mode | Result | Verified / failed / unmeasured windows | Request errors |
| --- | --- | --- | --- | --- |
| kv | raft-s3 | PASSED | 18 / 0 / 0 | 0 |
| queue | raft-s3 | FAILED | 5 / 1 / 6 | 5 |
| sql | raft-s3 | FAILED | 8 / 1 / 9 | 4 |
| sql | raft-quorum | FAILED | 8 / 1 / 9 | 16 |
| kv | raft-quorum | FAILED | 5 / 1 / 12 | 4 |
| queue | raft-quorum | FAILED | 4 / 1 / 7 | 1 |
| sql | fleet | PASSED | 18 / 0 / 0 | 0 |
| kv | fleet | FAILED | 4 / 1 / 13 | 4 |
| queue | fleet | FAILED | 1 / 1 / 10 | 1 |
| sql | bucket | PASSED | 18 / 0 / 0 | 0 |
| kv | bucket | FAILED | 8 / 1 / 9 | 16 |
| queue | bucket | PASSED | 12 / 0 / 0 | 0 |

## Descriptive results from complete passing cases

Each row pools both timed repetitions. Latency percentiles pool successful request
samples. Queue request rate counts eight-message enqueues; the extra message rate
includes final drain. These rows do not establish a four-mode or causal optimization
gain.

| API / mode | C | Operation | Requests/s | p50 / p95 / p99 ms | Verified messages/s including drain |
| --- | --- | --- | --- | --- | --- |
| kv / raft-s3 | 1 | write | 8.62 | 99.36 / 222.17 / 332.13 | — |
| kv / raft-s3 | 1 | read | 8.73 | 100.66 / 216.27 / 333.93 | — |
| kv / raft-s3 | 4 | write | 12.75 | 275.68 / 555.50 / 1081.24 | — |
| kv / raft-s3 | 4 | read | 12.99 | 279.65 / 553.92 / 864.13 | — |
| kv / raft-s3 | 16 | write | 18.75 | 630.29 / 1944.47 / 2754.73 | — |
| kv / raft-s3 | 16 | read | 17.23 | 695.10 / 2249.91 / 3351.09 | — |
| sql / fleet | 1 | write | 111.12 | 6.86 / 14.93 / 54.50 | — |
| sql / fleet | 1 | read | 999.36 | 0.63 / 2.92 / 7.01 | — |
| sql / fleet | 4 | write | 237.78 | 12.79 / 31.38 / 80.52 | — |
| sql / fleet | 4 | read | 4251.12 | 0.82 / 1.52 / 2.63 | — |
| sql / fleet | 16 | write | 466.90 | 25.18 / 60.59 / 206.21 | — |
| sql / fleet | 16 | read | 9089.78 | 1.62 / 3.08 / 4.37 | — |
| sql / bucket | 1 | write | 101.67 | 7.62 / 19.47 / 36.92 | — |
| sql / bucket | 1 | read | 962.81 | 0.79 / 2.31 / 4.51 | — |
| sql / bucket | 4 | write | 253.63 | 12.07 / 28.57 / 88.81 | — |
| sql / bucket | 4 | read | 2481.08 | 1.05 / 4.40 / 7.92 | — |
| sql / bucket | 16 | write | 389.99 | 28.96 / 81.68 / 257.82 | — |
| sql / bucket | 16 | read | 6344.76 | 2.15 / 5.09 / 7.98 | — |
| queue / bucket | 1 | enqueue | 36.86 | 19.52 / 54.61 / 140.35 | 294.12 |
| queue / bucket | 4 | enqueue | 49.37 | 61.01 / 186.24 / 417.80 | 390.98 |
| queue / bucket | 16 | enqueue | 55.82 | 188.35 / 1014.29 / 1894.51 | 440.21 |

## Failure and correctness limits

SQL/KV quorum runs record lease-expiry markers on all three nodes. Fleet KV/Queue and
bucket KV also record lease-expiry markers, so this is not exclusive to Raft. Recorded
Raft renewal warnings are ambiguous; observed waits overrun their bounds by 0–7 ms. This
distinguishes them from gross timer-delivery stalls without proving
storage-versus-caller-poll causality. Process exits occur after load completion and do
not establish when expiry began. Node samples show no CPU-throttled periods. RustFS records one throttled period in
each of KV+S3 and Queue+S3, and five in bucket Queue. No sampled memory-limit/OOM
events appear. These observations cannot exclude unsampled stalls or shared I/O
contention.

Queue+S3 has five unconfirmed requests. Its complete delivery probe observes all 2,256
acknowledged messages with exact bodies, plus 32 messages matching uncertain inputs,
with no duplicates/unmatched bodies; eight uncertain IDs remain unobserved. That
observation is not an exact receipt or S3 proof. Fleet Queue has an incomplete delivery
probe; its acknowledged-message retention remains unverified. Queue quorum has an
unconfirmed warmup request and therefore does not complete its timed matrix. The full
records retain every failed-window disposition.

The first owner stops before Fleet at the unchanged 6 GiB disk reserve. After pruning
exactly one unique unused private BuildKit target cache, the six remaining cases run on
the same source/image and limits. All failed data, containers, volumes, immutable images
and other-checkout caches are retained. Both owners join and restore the exact original
RustFS instance, ready HTTP 200. One VM is one physical failure domain.

The benchmark currently passes cloned data clients as the lease bucket, while normal
node construction creates a dedicated lease client. This is a configuration gap to test
independently; no pool change or causal attribution is included here. Full primitive,
recovery, crash/movement, authority, feature-mode and OPT/CG acceptance remain open.

See the [assessment](three-node-primitive-main-170-2026-11-15.json) and [compressed raw
capsule](three-node-primitive-main-170-2026-11-15.raw.json.gz). The capsule retains
windows, latency samples, phase/resource snapshots and terminal metadata. It excludes
runtime configuration, environment responses, private keys and raw node/store log text.
Both artifacts pass scanning against 41 actual private representations with zero
matches. Inclusive phase timings overlap; SQL publish counts producer completion checks,
not physical applier failures.

## Foreground-only cadence SQL quorum re-verification

Public commit `12eacc18`, compiler input `1d75a95f` and frozen source `bfa06775`
produce Linux image `aecb101f`. Both owning libraries and both node binaries
rebuild with the previous image's exact Rust compiler, lab profile and load-client
bytes. The complete native/archive suites and nine selected RustFS controls pass
at this source. These are separate from the earlier twelve-case image above.

SQL quorum now passes all 18 windows with zero request errors. All three nodes
exit cleanly without lease-expiry, panic, OOM or restart observations. Source and
engine identities remain unchanged. This completes the original C1/4/16 and two
reversed repetitions at five-second operation/callback/lease bounds, five-second
warmups and 30-second timed windows, with three 1 vCPU / 2 GiB RF3 voters, three
groups and 24 entities. It is not an isolated causal before/after speedup claim;
the earlier SQL quorum case is incomplete and receives no aggregate rate.

| C | Operation | Requests/s | p50 / p95 / p99 ms |
| --- | --- | --- | --- |
| 1 | write | 20.67 | 38.71 / 88.89 / 124.22 |
| 1 | read | 16.23 | 49.62 / 108.58 / 314.55 |
| 4 | write | 24.27 | 144.45 / 314.99 / 576.50 |
| 4 | read | 22.86 | 156.04 / 315.62 / 636.65 |
| 16 | write | 20.21 | 583.48 / 1904.30 / 2499.04 |
| 16 | read | 19.17 | 650.21 / 1940.54 / 2700.83 |

The owner stops before KV because free disk falls to 3.34 GiB, below the unchanged
6 GiB pre-case reserve. KV and Queue are unmeasured, not failed workload cases.
The exact original RustFS instance is restored and ready HTTP 200; all owners
join. The corrected cache postcheck reclaims only the unused private target cache
`8cz7yr40ln7gzdehcqorfod03` and verifies all 60 preceding participant containers,
60 named volumes, runtime images and original backend remain unchanged. The
first postcheck fails before any cache mutation because Docker refuses the
`--verbose`/`--format` flag combination; its empty comparison remains retained.
This launcher correction changes no framework or workload bytes.

See the [SQL assessment](three-node-primitive-main-170-2026-11-15.cadence-sql.json)
and [compressed raw capsule](three-node-primitive-main-170-2026-11-15.cadence-sql.raw.json.gz).
They exclude configuration, environments, private keys and raw node/store logs;
eight actual private representations scan with zero matches. Existing twelve-case
failures retain their original source. The VM remains 160 GiB throughout
this measurement; no expansion is included in its evidence. Complete mode comparison and
broader CG/OPT qualification remain open.
