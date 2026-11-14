<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Three-node SQL, KV and Queue comparison with bounded writer overlap

The corrected disk profile measured all six Raft workload/durability cases on three independent **1-vCPU, 2-GiB nodes** with shared local RustFS. 0 of six Raft cases completed their full matrix. Of 60 planned timed Raft windows, 21 verified, 3 failed and 36 were not reached.

The comparison below uses these Raft observations and the Fleet/bucket baselines from the [initial experiment](three-node-primitive-comparison-2026-11-14.md). Across the selected 12 cases, 5 completed their matrix; 75 of 120 timed windows verified, 4 failed and 41 were not reached. Partial verified windows remain observations, not stable capacity or a completed matrix.

The [readable follow-up observations](three-node-primitive-capacity-2026-11-14.json) identify every selected source/run, complete window summary, physical topology and resource observation. The [lossless raw follow-up](three-node-primitive-capacity-2026-11-14.raw.json.gz) contains every attempted/successful microsecond latency sample and error from this Raft run. The initial report links its separate raw baseline dataset. No failed observation is replaced or qualified by the follow-up.

## Matched workload and explicit disk change

All four modes use the same public SDK SQL, KV and Queue application and load-client executable. There are 24 service identities: SQL/KV have three groups of eight cells; Queue has 24 queues plus 24 DLQs in three groups of 16. Every group has the same three voters. Before load, every node reports complete enrollment/physical inventory and the three serving leaders are balanced across nodes. Requests rotate across all three frontends.

Each node, RustFS and the load client has a separate one-vCPU/2-GiB container with no additional swap. Node CPU sets are 0/1/2, RustFS 3 and client 4. Docker limits and Linux cgroups are checked. Each case has a fresh RustFS process, data volume and bucket, checked for storage readiness and seven signed S3 protocol operations. RustFS remains pinned to image `sha256:bffcab0c9d647aab0055d1c69d340b202d0909966b385932d4ead1aeb7602858`.

Both experiments use one host/core executor thread per node, one Raft SQL worker, five-second actor deadlines/leases and Raft callbacks, a 128-MiB image bank, 16-MiB per-cell ceiling and 32,768 physical manifest pages/64 MiB per group store. The full plan has concurrency 1/4/16, two repetitions in reverse concurrency order, five-second warmups and 30-second operation windows. Payload values contain 256 ASCII bytes; Queue batches have eight text messages. SQL/KV check exact readback. Queue checks acknowledged IDs/body hashes, callback duplicates and zero backlog/DLQ after settlement.

The first Raft fixture allowed a 784-MiB extent inventory, while one private workspace reserves 592 MiB. A real `NodeExtentStore` test reproduces second-workspace refusal. The follow-up explicitly changes disk admission to eight workspaces and a 4,928-MiB inventory ceiling, including the existing image/receive headroom. The ninth workspace is refused; closing a workspace returns its physical credit. CPU, RAM, deadlines, image/manifest banks and SQL-worker count are unchanged. The inventory ceiling is bounded disk admission, not an allocation of that much RAM or a kernel filesystem quota.

The owned Colima VM disk expands from 100 to 160 GiB to fit the six inventory ceilings. Its engine identity, six CPUs, nominal 14 GiB RAM and all stopped container/volume IDs are preserved. After reboot, reported kernel `MemTotal` differs by one 4-KiB page; configured VM RAM and every container memory limit remain unchanged. The strict first post-resize guard failure remains retained alongside independent configuration/inventory verification. These are separate disk-capacity profiles, not an isolated statistical architecture experiment.

| Mode | Required write acknowledgement |
| --- | --- |
| Raft quorum | RF3 quorum commitment and exact applied receipt |
| Raft S3 | Raft quorum plus shared-store publication proof |
| Fleet | Existing peer replication **or bucket proof** |
| Bucket | Existing shared-store bucket output gate |

The guarantees and public read implementations differ. Fleet peer-only acknowledgements are not isolated. These closed-loop local measurements include a one-vCPU RustFS and a shared host; CPU sets select guest virtual CPUs. Other checkouts' host compilers are recorded without causal attribution. The optimized repository `lab` profile uses `opt-level="s"` (size) and line-table debug information. Results apply to that build/profile and do not establish production capacity, isolated speedup, independent-domain/cloud-S3 behavior, crash recovery, partitions, soak or completion of CG/OPT release gates.

## Throughput and completion latency

Only complete error-free verified windows appear below. Rates use total successful requests divided by total window time. Percentiles pool raw successful request samples within the same workload/mode/concurrency/operation and source profile; percentiles are not averaged. The repetition range shows individual window rates. Missing and failed windows are not assigned zero throughput. Raft rows come from this corrected run; Fleet/bucket rows come from the initial experiment.

| Workload | Mode | Clients | Operation | Verified / planned | Requests/s | Repetition range | p50 ms | p95 ms | p99 ms |
| --- | --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| SQL | raft-quorum | 1 | read | 1 / 2 | 16.30 | 16.30–16.30 | 57.29 | 91.64 | 132.36 |
| SQL | raft-s3 | 1 | read | 1 / 2 | 4.06 | 4.06–4.06 | 233.00 | 360.97 | 656.28 |
| SQL | fleet | 1 | read | 2 / 2 | 977.00 | 663.10–1290.94 | 0.66 | 2.75 | 6.39 |
| SQL | bucket | 1 | read | 2 / 2 | 932.18 | 354.19–1510.17 | 0.62 | 3.06 | 8.93 |
| SQL | raft-quorum | 1 | write | 1 / 2 | 20.01 | 20.01–20.01 | 47.98 | 65.86 | 94.00 |
| SQL | raft-s3 | 1 | write | 1 / 2 | 5.63 | 5.63–5.63 | 171.71 | 278.05 | 384.70 |
| SQL | fleet | 1 | write | 2 / 2 | 71.16 | 67.57–74.75 | 12.05 | 26.54 | 42.97 |
| SQL | bucket | 1 | write | 2 / 2 | 95.95 | 70.12–121.79 | 7.52 | 22.17 | 51.44 |
| SQL | raft-quorum | 4 | read | 1 / 2 | 17.66 | 17.66–17.66 | 215.90 | 369.25 | 482.87 |
| SQL | raft-s3 | 4 | read | 1 / 2 | 7.39 | 7.39–7.39 | 495.04 | 915.12 | 1398.45 |
| SQL | fleet | 4 | read | 2 / 2 | 3638.85 | 3030.09–4253.08 | 0.83 | 1.86 | 4.12 |
| SQL | bucket | 4 | read | 2 / 2 | 2608.50 | 648.84–4569.62 | 0.82 | 5.43 | 11.98 |
| SQL | raft-quorum | 4 | write | 1 / 2 | 18.21 | 18.21–18.21 | 205.16 | 367.26 | 517.73 |
| SQL | raft-s3 | 4 | write | 1 / 2 | 8.46 | 8.46–8.46 | 428.05 | 872.57 | 1099.24 |
| SQL | fleet | 4 | write | 2 / 2 | 66.26 | 44.83–87.70 | 38.40 | 137.86 | 417.30 |
| SQL | bucket | 4 | write | 2 / 2 | 257.88 | 194.43–321.35 | 11.74 | 31.44 | 70.00 |
| SQL | raft-quorum | 16 | read | 1 / 2 | 17.97 | 17.97–17.97 | 688.91 | 2013.43 | 2661.84 |
| SQL | fleet | 16 | read | 2 / 2 | 2997.53 | 2553.65–3441.72 | 2.79 | 14.82 | 39.30 |
| SQL | bucket | 16 | read | 2 / 2 | 7500.76 | 7269.49–7732.07 | 1.82 | 3.98 | 6.99 |
| SQL | raft-quorum | 16 | write | 1 / 2 | 16.89 | 16.89–16.89 | 771.35 | 2155.45 | 2544.98 |
| SQL | fleet | 16 | write | 2 / 2 | 106.73 | 103.71–109.73 | 74.34 | 399.29 | 1411.09 |
| SQL | bucket | 16 | write | 2 / 2 | 425.82 | 420.40–431.24 | 28.66 | 74.49 | 149.24 |
| KV | raft-quorum | 1 | read | 1 / 2 | 6.19 | 6.19–6.19 | 107.67 | 473.84 | 1298.30 |
| KV | raft-s3 | 1 | read | 1 / 2 | 5.70 | 5.70–5.70 | 164.29 | 279.26 | 389.51 |
| KV | fleet | 1 | read | 2 / 2 | 1360.57 | 1350.77–1370.37 | 0.59 | 1.01 | 2.74 |
| KV | bucket | 1 | read | 2 / 2 | 1128.30 | 860.56–1396.03 | 0.67 | 1.97 | 4.00 |
| KV | raft-quorum | 1 | write | 1 / 2 | 6.44 | 6.44–6.44 | 111.83 | 400.00 | 1294.15 |
| KV | raft-s3 | 1 | write | 1 / 2 | 5.40 | 5.40–5.40 | 174.41 | 287.02 | 422.88 |
| KV | fleet | 1 | write | 2 / 2 | 65.71 | 48.05–83.37 | 9.45 | 28.85 | 132.12 |
| KV | bucket | 1 | write | 2 / 2 | 101.48 | 58.43–144.52 | 6.95 | 22.11 | 48.89 |
| KV | raft-quorum | 4 | read | 1 / 2 | 20.05 | 20.05–20.05 | 186.95 | 345.33 | 450.65 |
| KV | raft-s3 | 4 | read | 1 / 2 | 10.13 | 10.13–10.13 | 361.54 | 653.08 | 844.62 |
| KV | fleet | 4 | read | 2 / 2 | 4582.91 | 4351.83–4814.13 | 0.76 | 1.33 | 3.37 |
| KV | bucket | 4 | read | 2 / 2 | 3572.91 | 3349.10–3796.66 | 0.90 | 2.20 | 4.27 |
| KV | raft-quorum | 4 | write | 1 / 2 | 18.22 | 18.22–18.22 | 212.21 | 343.13 | 479.82 |
| KV | raft-s3 | 4 | write | 1 / 2 | 11.64 | 11.64–11.64 | 321.76 | 556.98 | 648.50 |
| KV | fleet | 4 | write | 2 / 2 | 94.53 | 33.42–155.62 | 22.86 | 183.86 | 444.09 |
| KV | bucket | 4 | write | 2 / 2 | 240.22 | 218.20–262.23 | 12.92 | 36.06 | 71.44 |
| KV | fleet | 16 | read | 2 / 2 | 5970.53 | 4935.46–7005.83 | 1.90 | 6.68 | 16.49 |
| KV | bucket | 16 | read | 2 / 2 | 6832.44 | 6395.86–7269.01 | 1.85 | 4.62 | 10.31 |
| KV | raft-s3 | 16 | write | 1 / 2 | 16.59 | 16.59–16.59 | 749.48 | 2043.51 | 2828.09 |
| KV | fleet | 16 | write | 2 / 2 | 296.65 | 285.45–307.83 | 42.29 | 113.71 | 207.42 |
| KV | bucket | 16 | write | 2 / 2 | 358.42 | 325.67–391.16 | 33.69 | 106.67 | 192.66 |
| QUEUE | raft-quorum | 1 | enqueue | 1 / 2 | 4.95 | 4.95–4.95 | 188.99 | 306.40 | 447.59 |
| QUEUE | raft-s3 | 1 | enqueue | 1 / 2 | 1.78 | 1.78–1.78 | 524.76 | 926.03 | 1199.36 |
| QUEUE | bucket | 1 | enqueue | 2 / 2 | 21.18 | 15.19–27.16 | 24.90 | 151.66 | 341.50 |
| QUEUE | bucket | 4 | enqueue | 2 / 2 | 27.48 | 26.67–28.29 | 99.14 | 381.61 | 1009.62 |
| QUEUE | bucket | 16 | enqueue | 2 / 2 | 39.53 | 38.24–40.81 | 260.44 | 1186.38 | 2803.75 |

Queue request latency is **batch enqueue acknowledgement**, not per-message delivery latency. Message throughput including drain includes final callback observation and durable settlement verification. The bounded in-memory callback observer makes no crash durability claim.

| Mode | Clients | Verified windows | Acknowledged / delivered messages | Enqueue messages/s | Messages/s including drain | Duplicate callbacks |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| raft-quorum | 1 | 1 | 1192 / 1192 | 39.63 | 33.27 | 0 |
| raft-s3 | 1 | 1 | 432 / 432 | 14.27 | 9.87 | 0 |
| bucket | 1 | 2 | 10176 / 10176 | 169.45 | 168.92 | 0 |
| bucket | 4 | 2 | 13232 / 13232 | 219.85 | 218.00 | 0 |
| bucket | 16 | 2 | 19328 / 19328 | 316.28 | 313.69 | 0 |

## Complete cases and retained failures

| Workload | Mode | Run | Case | Timed verified / failed / unmeasured | Request errors |
| --- | --- | --- | --- | --- | ---: |
| SQL | raft-quorum | corrected disk | FAILED | 6 / 0 / 6 | 1 |
| SQL | raft-s3 | corrected disk | FAILED | 4 / 1 / 7 | 6 |
| SQL | fleet | initial baseline | PASSED | 12 / 0 / 0 | 0 |
| SQL | bucket | initial baseline | PASSED | 12 / 0 / 0 | 0 |
| KV | raft-quorum | corrected disk | FAILED | 4 / 1 / 7 | 7 |
| KV | raft-s3 | corrected disk | FAILED | 5 / 1 / 6 | 1 |
| KV | fleet | initial baseline | PASSED | 12 / 0 / 0 | 0 |
| KV | bucket | initial baseline | PASSED | 12 / 0 / 0 | 0 |
| QUEUE | raft-quorum | corrected disk | FAILED | 1 / 0 / 5 | 1 |
| QUEUE | raft-s3 | corrected disk | FAILED | 1 / 0 / 5 | 1 |
| QUEUE | fleet | initial baseline | FAILED | 0 / 1 / 5 | 0 |
| QUEUE | bucket | initial baseline | PASSED | 6 / 0 / 0 | 0 |

Fleet Queue is not rerun or repaired by the disk change. In its initial timed window, 415 batches receive successful replies (3,320 messages) before a node fences with `NodeLeaseExpired` during verification. That window has no qualified delivery/settlement throughput; the failure does not establish lost acknowledged data. The initial report retains its full record.

The first failed windows in the corrected Raft cases are:

- SQL / raft-quorum: repetition 2, concurrency 16, warmup; `Execution(Refused(Overloaded))`.
- SQL / raft-s3: repetition 1, concurrency 16, write; ambiguous `DeadlineExceeded`.
- KV / raft-quorum: repetition 1, concurrency 16, write; `Execution(Refused(Overloaded))`.
- KV / raft-s3: repetition 1, concurrency 16, read; `Execution(Refused(Overloaded))`.
- QUEUE / raft-quorum: repetition 1, concurrency 4, warmup; `Execution(Refused(Overloaded))`.
- QUEUE / raft-s3: repetition 1, concurrency 4, warmup; `Execution(Refused(Overloaded))`.

SQL Raft S3 reports a write-readback mismatch after unconfirmed concurrency-16 updates. A later timed-out update may commit and replace an earlier acknowledged value; this observation does not establish corruption or loss of acknowledged data. Original-identity reconciliation remains required.

Small Raft windows, especially Queue batches, have limited tail sample counts. The artifacts retain the exact count and every sample; their percentiles are descriptive.

Failed mutations remain unconfirmed and stop their worker without fresh-identity retries. Original application/command identities, request errors, verification failures, terminal state and raw latency samples remain in the artifacts. Subsequent windows are unmeasured. No failed case is promoted after raw-log recovery.

## Corrected-run resources

These are case-wide sampled kernel memory peaks, including process lifetime/file cache; they are not per-window RSS or isolated CPU measurements. A successful load that exits between inspect and sample contributes verified terminal state, without an invented final cgroup sample.

| Workload | Mode | Highest node memory.peak MiB | Store peak MiB | Load peak MiB | Node/store exits | OOM/restart events |
| --- | --- | ---: | ---: | ---: | --- | ---: |
| SQL | raft-quorum | 556.0 | 967.5 | 15.1 | 0,0,0,0 | 0 |
| SQL | raft-s3 | 193.3 | 611.2 | 13.8 | 0,0,0,0 | 0 |
| KV | raft-quorum | 325.7 | 678.3 | 13.9 | 0,0,0,0 | 0 |
| KV | raft-s3 | 316.2 | 868.1 | 13.9 | 0,0,0,0 | 0 |
| QUEUE | raft-quorum | 198.2 | 340.7 | 14.3 | 0,0,0,0 | 0 |
| QUEUE | raft-s3 | 106.4 | 429.2 | 13.3 | 0,0,0,0 | 0 |

## Source and reproduction

- Corrected benchmark source `d09569dfceede92d1d8525f0500c6844f25373b2`; full source-input manifest `4495f279f656185a7309546d0e0e2aef5db4c10bbbf647a14d3017296bd752b3` over 3392 files. The unchanged 60 ignored/generated inputs remain separately bound in the frozen build context. Source inputs are checked against the immutable image before and after the run.
- Immutable image `sha256:d24223044f6a67bf8c0b8805866c9c70aec96dfcab29957bc47654f62df42f2f`; Rust 1.98.1/compiler `48a229ceaefd4985c50990b14116b6d856af0985`, locked optimized `lab` build. The production library is fresh/reused with identical production inputs; baseline/load executable bytes match the initial run. The Raft binary rebuild includes the disk policy and profile reporting.
- Run `159e0047d958`, controller/driver exits `1/1`, elapsed 1128.8 seconds. Engine identity and compiler input bytes remain unchanged throughout this run.
- [Harness policy and commands](../../../../tools/perf/primitive_compare.md), [build recipe](../../../../tools/perf/primitive_compare.Dockerfile), [controller](../../../../tools/perf/primitive_compare.py), [client](../../../../tools/perf/primitive_load.py), [real workspace admission regression](../../../../tools/perf/primitive_raft_tests.rs). Run the same command with `--modes raft-quorum raft-s3` and a new checkout-owned output directory.
- All measured case containers are stopped and retained. The original local RustFS service is restored with its one-vCPU/2-GiB limits and storage readiness verified.
- All six corrected-profile three-second smoke cases pass at concurrency one/four, including Queue callback/body/settlement verification. Smoke results are not counted as timed performance evidence.
- The [initial three-node report](three-node-primitive-comparison-2026-11-14.md) preserves the one-workspace profile and original baselines. The older [single-process report](primitive-performance-2026-11-14.md) remains separate and retains the independently reproduced 1,024-record manifest exhaustion. Neither failed experiment is retroactively qualified.

## Remaining priorities

1. Diagnose any retained execution/admission refusal under bounded concurrency with original identities and deadlines; qualified windows do not close fairness, backpressure or sustainable operating-envelope gates.
2. Fix Fleet Queue lease-renewal progress under sustained load while preserving fencing. Reconcile unconfirmed operations and rerun complete affected cases after a source-bound fix.
3. Implement safe sustainable manifest reclamation with a complete persistent-root inventory and joined ownership. Larger benchmark budgets do not repair `Apply(Storage(group manifest: Limit))` at the original 1,024-record ceiling.
4. Continue authority expiry/revocation, physical retirement/once-only Fleet settlement, provider recovery and independent-domain [CG/OPT qualification](optimization-progress.md). Historical failures and missing evidence remain required.
