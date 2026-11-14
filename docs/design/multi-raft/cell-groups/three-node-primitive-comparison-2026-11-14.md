<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Three-node primitive comparison, initial disk profile, 2026-11-14 UTC

The requested local comparison ran all 12 workload/durability cases on three separate **1-vCPU, 2-GiB node containers** with shared RustFS. 5 cases completed their full matrix; 7 failed. Of 120 planned timed windows, 62 verified, 1 failed and 57 were not reached after earlier failures. Of 72 warmups, 35 verified, 6 failed and 31 were not reached.

These are measured public SDK operations, including SQL/KV value readback and Queue callback/body/settlement verification. Failed and incomplete cases do not establish stable capacity. There is no production readiness or OPT/CG gate acceptance from this experiment.

The [readable observations](three-node-primitive-comparison-2026-11-14.json) contain every window summary, exact environment/source identities, terminal process states and resource summaries. The [lossless raw observations](three-node-primitive-comparison-2026-11-14.raw.json.gz) additionally contain every attempted and successful microsecond latency sample. Decompress with Python `gzip` or `gzip -dc`; the readable artifact records its SHA-256. Raw logs, full cgroup samples and all stopped data volumes remain in the checkout-owned qualification directory.

## Environment and acknowledgement contracts

- One aarch64 Colima VM, six virtual CPUs and 14 GiB; three independent node processes and three independent node data volumes.
- Each node: one-vCPU ceiling, 2 GiB RAM, no additional swap, CPU sets 0/1/2. The fresh RustFS process and separate load client each have the same CPU/memory ceilings on CPU sets 3/4. Docker settings and Linux cgroups are checked, including terminal exit/OOM/restart state.
- Each workload/mode receives a fresh RustFS process, data volume and bucket. RustFS image `sha256:bffcab0c9d647aab0055d1c69d340b202d0909966b385932d4ead1aeb7602858`. The runner waits for storage readiness and performs seven signed S3 protocol probes before launch.
- SQL/KV: 24 cells in three Raft groups of eight. Queue: 24 queues plus 24 declared DLQ cells, three groups of 16. Every group has the same three voters. The runner verifies complete enrollment and physical group inventory at all three endpoints and balances serving leaders 1/2/3 before load. Healthy followers are enrolled but do not report serving readiness.
- All modes receive the same public requests through rotating frontends: 256-byte ASCII values; SQL prepared upsert/read; KV text put/get; Queue batches of eight text messages delivered to a real consumer with explicit acknowledgement. Queue verification requires matching distinct message IDs/body hashes and zero source backlog/DLQ count; duplicate callbacks are counted.
- One host executor thread and one core executor thread per node, one Raft SQL worker, five-second actor operation deadlines and frontend leases. Raft native callbacks retain their five-second timeout. Raft uses extents, a 128-MiB image bank and a 16-MiB per-cell ceiling; the pre-existing production perf manifest budget is 32,768 physical pages/64 MiB per group store.
- Closed-loop concurrency 1, 4 and 16; two repetitions in opposite concurrency order; five-second warmups; 30-second write/read or Queue enqueue windows. Setup, warmup and verification are outside operation throughput timing. Queue including-drain throughput includes final verification overhead.
- No deadline, RAM/CPU ceiling, manifest budget or workspace budget was increased during this comparison. This five-second actor profile and 32,768-page budget differ from the earlier single-process fixture; they do not repair or retroactively qualify its failures.

| Mode | Acknowledgement requirement |
| --- | --- |
| Raft quorum | RF3 Raft quorum and the exact applied receipt |
| Raft S3 | Raft quorum plus shared-store publication proof |
| Fleet | Existing peer replication **or bucket proof** |
| Bucket | Existing shared-store bucket output gate |

These guarantees and read implementations differ. The experiment does not isolate Fleet peer-only acknowledgements or certify equivalent read algorithms. CPU sets select guest virtual CPUs; the host remains shared. Other checkouts' host compilers were observed and recorded without causal attribution. These local measurements do not qualify independent failure domains, cloud S3, crashes, partitions, recovery or soak.

## Verified timed observations

Only complete error-free windows appear below. Requests/s uses total successful requests divided by total window time; latency percentiles use pooled raw successful samples, not averages of percentiles. The range shows individual repetition rates. A single verified window in a failed case remains a partial observation. No zero rate is substituted for missing or failed windows.

| Workload | Mode | Clients | Operation | Verified / planned windows | Requests/s | Repetition range | p50 ms | p95 ms | p99 ms |
| --- | --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| SQL | raft-quorum | 1 | write | 1 / 2 | 13.01 | 13.01–13.01 | 65.11 | 138.06 | 276.35 |
| SQL | raft-quorum | 1 | read | 1 / 2 | 15.63 | 15.63–15.63 | 57.75 | 104.60 | 148.57 |
| SQL | raft-s3 | 1 | write | 1 / 2 | 5.59 | 5.59–5.59 | 166.34 | 269.01 | 421.63 |
| SQL | raft-s3 | 1 | read | 1 / 2 | 5.59 | 5.59–5.59 | 167.33 | 284.51 | 444.90 |
| SQL | fleet | 1 | write | 2 / 2 | 71.16 | 67.57–74.75 | 12.05 | 26.54 | 42.97 |
| SQL | fleet | 1 | read | 2 / 2 | 977.00 | 663.10–1290.94 | 0.66 | 2.75 | 6.39 |
| SQL | fleet | 4 | write | 2 / 2 | 66.26 | 44.83–87.70 | 38.40 | 137.86 | 417.30 |
| SQL | fleet | 4 | read | 2 / 2 | 3638.85 | 3030.09–4253.08 | 0.83 | 1.86 | 4.12 |
| SQL | fleet | 16 | write | 2 / 2 | 106.73 | 103.71–109.73 | 74.34 | 399.29 | 1411.09 |
| SQL | fleet | 16 | read | 2 / 2 | 2997.53 | 2553.65–3441.72 | 2.79 | 14.82 | 39.30 |
| SQL | bucket | 1 | write | 2 / 2 | 95.95 | 70.12–121.79 | 7.52 | 22.17 | 51.44 |
| SQL | bucket | 1 | read | 2 / 2 | 932.18 | 354.19–1510.17 | 0.62 | 3.06 | 8.93 |
| SQL | bucket | 4 | write | 2 / 2 | 257.88 | 194.43–321.35 | 11.74 | 31.44 | 70.00 |
| SQL | bucket | 4 | read | 2 / 2 | 2608.50 | 648.84–4569.62 | 0.82 | 5.43 | 11.98 |
| SQL | bucket | 16 | write | 2 / 2 | 425.82 | 420.40–431.24 | 28.66 | 74.49 | 149.24 |
| SQL | bucket | 16 | read | 2 / 2 | 7500.76 | 7269.49–7732.07 | 1.82 | 3.98 | 6.99 |
| KV | raft-quorum | 1 | write | 1 / 2 | 16.19 | 16.19–16.19 | 60.37 | 87.09 | 108.51 |
| KV | raft-quorum | 1 | read | 1 / 2 | 14.07 | 14.07–14.07 | 66.06 | 107.09 | 190.84 |
| KV | raft-s3 | 1 | write | 1 / 2 | 6.81 | 6.81–6.81 | 138.13 | 241.84 | 304.30 |
| KV | raft-s3 | 1 | read | 1 / 2 | 6.05 | 6.05–6.05 | 148.98 | 303.78 | 493.11 |
| KV | fleet | 1 | write | 2 / 2 | 65.71 | 48.05–83.37 | 9.45 | 28.85 | 132.12 |
| KV | fleet | 1 | read | 2 / 2 | 1360.57 | 1350.77–1370.37 | 0.59 | 1.01 | 2.74 |
| KV | fleet | 4 | write | 2 / 2 | 94.53 | 33.42–155.62 | 22.86 | 183.86 | 444.09 |
| KV | fleet | 4 | read | 2 / 2 | 4582.91 | 4351.83–4814.13 | 0.76 | 1.33 | 3.37 |
| KV | fleet | 16 | write | 2 / 2 | 296.65 | 285.45–307.83 | 42.29 | 113.71 | 207.42 |
| KV | fleet | 16 | read | 2 / 2 | 5970.53 | 4935.46–7005.83 | 1.90 | 6.68 | 16.49 |
| KV | bucket | 1 | write | 2 / 2 | 101.48 | 58.43–144.52 | 6.95 | 22.11 | 48.89 |
| KV | bucket | 1 | read | 2 / 2 | 1128.30 | 860.56–1396.03 | 0.67 | 1.97 | 4.00 |
| KV | bucket | 4 | write | 2 / 2 | 240.22 | 218.20–262.23 | 12.92 | 36.06 | 71.44 |
| KV | bucket | 4 | read | 2 / 2 | 3572.91 | 3349.10–3796.66 | 0.90 | 2.20 | 4.27 |
| KV | bucket | 16 | write | 2 / 2 | 358.42 | 325.67–391.16 | 33.69 | 106.67 | 192.66 |
| KV | bucket | 16 | read | 2 / 2 | 6832.44 | 6395.86–7269.01 | 1.85 | 4.62 | 10.31 |
| QUEUE | bucket | 1 | enqueue | 2 / 2 | 21.18 | 15.19–27.16 | 24.90 | 151.66 | 341.50 |
| QUEUE | bucket | 4 | enqueue | 2 / 2 | 27.48 | 26.67–28.29 | 99.14 | 381.61 | 1009.62 |
| QUEUE | bucket | 16 | enqueue | 2 / 2 | 39.53 | 38.24–40.81 | 260.44 | 1186.38 | 2803.75 |

Queue request latency above is **batch enqueue acknowledgement**, not per-message delivery latency. Its separate message rates follow; including-drain time includes callback observation and settlement checks. The delivery observer is bounded, in memory, and makes no crash durability claim.

| Mode | Clients | Verified windows | Acknowledged / delivered messages | Enqueue messages/s | Messages/s including drain | Duplicate callbacks |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| bucket | 1 | 2 | 10176 / 10176 | 169.45 | 168.92 | 0 |
| bucket | 4 | 2 | 13232 / 13232 | 219.85 | 218.00 | 0 |
| bucket | 16 | 2 | 19328 / 19328 | 316.28 | 313.69 | 0 |

## Failures and unmeasured scope

The runner retains 13 request errors across warmup and timed windows. A failed mutation is treated as unconfirmed; the load worker stops without fresh-identity retries. Later delivery of an uncertain Queue batch is not proof of lost acknowledged work. Subsequent windows in a failed case are unmeasured, not zero throughput.

| Workload | Mode | Case | Timed verified / failed / unmeasured | Warmups verified / failed / unmeasured | Request errors |
| --- | --- | --- | --- | --- | ---: |
| SQL | raft-quorum | FAILED | 2 / 0 / 10 | 1 / 1 / 4 | 3 |
| SQL | raft-s3 | FAILED | 2 / 0 / 10 | 1 / 1 / 4 | 3 |
| SQL | fleet | PASSED | 12 / 0 / 0 | 6 / 0 / 0 | 0 |
| SQL | bucket | PASSED | 12 / 0 / 0 | 6 / 0 / 0 | 0 |
| KV | raft-quorum | FAILED | 2 / 0 / 10 | 1 / 1 / 4 | 3 |
| KV | raft-s3 | FAILED | 2 / 0 / 10 | 1 / 1 / 4 | 2 |
| KV | fleet | PASSED | 12 / 0 / 0 | 6 / 0 / 0 | 0 |
| KV | bucket | PASSED | 12 / 0 / 0 | 6 / 0 / 0 | 0 |
| QUEUE | raft-quorum | FAILED | 0 / 0 / 6 | 0 / 1 / 5 | 1 |
| QUEUE | raft-s3 | FAILED | 0 / 0 / 6 | 0 / 1 / 5 | 1 |
| QUEUE | fleet | FAILED | 0 / 1 / 5 | 1 / 0 / 5 | 0 |
| QUEUE | bucket | PASSED | 6 / 0 / 0 | 6 / 0 / 0 | 0 |

The first Raft SQL/KV refusal occurs during concurrency-four warmup after
verified one-client writes and reads. Both Raft Queue cases refuse during the
one-client warmup. Their returned failures include
`Execution(Refused(Overloaded))`; the original failed identities remain retained.

Fleet Queue completes a verified 224-message warmup. Its timed window receives
415 successful batch replies (3,320 messages), then delivery verification fails
with a connection reset. Node 1 reports `NodeLeaseExpired`, fences itself and
exits 1; no OOM or restart occurs. This does not establish lost acknowledged
messages, but the timed window cannot qualify delivery/settlement or availability.
Bucket Queue completes all six measured windows with delivery and drain checks.

The KV Raft S3 controller notices the failed load during sampling before normal
window indexing. Its complete emitted windows were recovered from `load.log`
and independently checked; the original failed case record remains unchanged.
The readable artifact identifies this recovery explicitly.

Detailed errors, original application identities and any unconfirmed mutation outcomes remain in both observation artifacts. Node/store/load terminal statuses are distinct: a failing load exit does not imply a node crash or OOM.

## Resource observations

These are maxima observed through case-wide cgroup sampling. Kernel `memory.peak` includes process lifetime before a sample, file cache and helper work; these are not per-window RSS or isolated CPU measurements. No final resource sample is invented when a successful load exits between inspection and sampling.

| Workload | Mode | Highest node memory.peak MiB | Store memory.peak MiB | Load memory.peak MiB | Node/store exits | OOM/restart observations |
| --- | --- | ---: | ---: | ---: | --- | --- |
| SQL | raft-quorum | 148.1 | 372.6 | 15.8 | 0,0,0,0 | 0 |
| SQL | raft-s3 | 70.4 | 347.7 | 13.3 | 0,0,0,0 | 0 |
| SQL | fleet | 80.7 | 553.1 | 28.5 | 0,0,0,0 | 0 |
| SQL | bucket | 153.1 | 1184.8 | 38.1 | 0,0,0,0 | 0 |
| KV | raft-quorum | 147.2 | 455.8 | 14.1 | 0,0,0,0 | 0 |
| KV | raft-s3 | 75.3 | 389.6 | 13.0 | 0,0,0,0 | 0 |
| KV | fleet | 115.0 | 734.0 | 35.7 | 0,0,0,0 | 0 |
| KV | bucket | 149.4 | 1089.7 | 34.8 | 0,0,0,0 | 0 |
| QUEUE | raft-quorum | 32.1 | 172.2 | 13.6 | 0,0,0,0 | 0 |
| QUEUE | raft-s3 | 31.8 | 206.7 | 12.4 | 0,0,0,0 | 0 |
| QUEUE | fleet | 42.7 | 311.6 | 15.4 | 1,0,0,0 | 0 |
| QUEUE | bucket | 121.5 | 983.2 | 30.2 | 0,0,0,0 | 0 |

## Measured source and reproduction

- Parent revision: `da5a272a668f6dc26580da79be9d1fdec6592a8b` plus the new benchmark inputs. Main `3d6450d43cac8a83be63cae65513db8ac8626976` from merged PR #166 was already included when `git pull --ff-only origin main` reported up to date.
- Benchmark sources published at `fdbe69617690bde109eee6bdc1078a5217448d9b` with unchanged measured bytes; 60 unchanged ignored/generated inputs are separately bound and retained in the frozen build context.
- Complete compiler-input SHA-256: `31f011aaf3dce7ac3b428431406374b5f41aeb01fe5664f80f5705a194eb2197` over 3391 inputs; the runner verified the immutable image against the checkout before and after measurement.
- Immutable Linux image: `sha256:c464cb67aebb98c52f72032bc9500b663eed91137365d5df58c59123aaa9748d`. Rust 1.98.1, compiler `48a229ceaefd4985c50990b14116b6d856af0985`, optimized `lab` profile and locked dependencies. Both node binaries were built; the controller-only sampler correction reused byte-identical node/load executables with unchanged production inputs and Cargo freshness checks.
- Run identity: `05365809bc9e`; controller/driver exits `1/1` after 2360.0 seconds. Source inputs and engine identity remained unchanged.
- [Harness and exact workload policy](../../../../tools/perf/primitive_compare.md), [container build](../../../../tools/perf/primitive_compare.Dockerfile), [controller](../../../../tools/perf/primitive_compare.py), [load client](../../../../tools/perf/primitive_load.py). Reproduce this initial 784-MiB disk profile at source commit `fdbe69617690bde109eee6bdc1078a5217448d9b`, using a new checkout-owned output directory. Later harness revisions explicitly change the disk-capacity profile; their measurements must remain separate.
- The earlier [single-process experiment](primitive-performance-2026-11-14.md) remains separate, including its original manifest-limit diagnosis and failed records. Preliminary three-second smoke runs are not counted as timed performance evidence.

## Next priorities

1. Prove and address the first Raft execution/admission refusal while preserving original command ownership and deadlines. The measured extent inventory ceiling is 822,083,584 bytes (784 MiB); the runtime-reported private workspace reservation is 620,756,992 bytes (592 MiB). `NodeExtentStore::create_workspace` charges the full reservation before creation, so this fixture cannot hold two such workspaces even though its workspace-count ceiling is 256. Overlapping Queue callbacks/enqueues and concurrent SQL/KV requests make this a concrete capacity concern. This static bound is a strong hypothesis for the observed fast overloads; the exact refusal site still needs a targeted runtime probe. No disk budget was lifted in this experiment.
2. Diagnose Fleet Queue lease-renewal progress under sustained load; preserve fencing and the original five-second deadline/lease profile.
3. Resolve uncertain operations by their original identities and rerun the complete affected matrices after a source-bound fix. Keep failed outcomes and incomplete repetitions visible.
4. Add safe sustainable manifest-page reclamation: the older fixture independently reproduces `Apply(Storage(group manifest: Limit))` at its original 1,024-record ceiling. A larger benchmark budget is not a collector fix; complete persistent roots and joined ownership remain required.
5. Continue independent-domain durability/recovery, authority expiry/revocation, retirement settlement and the remaining [CG/OPT gates](optimization-progress.md). The local comparison does not waive historical failures.
