<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Local SQL, KV and Queue measurements, 2026-11-14 UTC

The real RustFS/Colima experiment failed qualification in all six primitive/image
cases. Nine of 40 planned timed windows completed for 30 seconds with zero request
errors and successful value/message verification. Seven timed windows failed;
24 were not reached after earlier failures. Thirteen warmups also failed.
Short native smoke checks had passed each API before this experiment.

These results show working API paths and unresolved sustained-load failures.
They do not establish stable throughput, an SLA, a speedup, independent-node
capacity or completion of the broader CG/OPT qualification gates.

The [machine-readable observations](primitive-performance-2026-11-14.json) retain every
window's raw microsecond latency samples, errors, verification result, source
identity, executable identity and resource summaries. Failed and incomplete
windows remain in that artifact.

## Measured source and environment

- `git pull --ff-only origin main` reported already up to date. The measured
  branch includes main `3d6450d43cac8a83be63cae65513db8ac8626976` from PR #166.
- Measured revision: `b101730f9ad3ef370afdc29495a2efbc014c89d7`; compiler-input
  manifest: `8edbdba5548f7c2debdba32a94fad922a806757eed62d6c77ea939c85eea8331`.
- Rust 1.98.1, compiler commit
  `48a229ceaefd4985c50990b14116b6d856af0985`, optimized `lab` test artifact.
- Executable SHA-256:
  `d6a5f6eab5625fd76432644eff87567ec4ef888a340853dbde3875f8cc2b897a`.
- Immutable image:
  `sha256:fa9f492652623f8f861569ecdaabba4f90a2cdcea6a8aee4b4ce58c3b89223a1`.
- One aarch64 Colima VM, six CPUs/14 GiB. Three native RF3 voters and the HTTP
  load generator share one process/container with a three-CPU quota, 9 GiB
  memory and no extra swap. They have separate local node directories and real
  Raft transport, SQLite state and RustFS-backed catalog/archive traffic.
- The existing owned RustFS container retains one CPU/2 GiB and no extra swap.
  Its image, container, start generation and resource settings remained unchanged.
  Seven signed S3 protocol probes passed before workload startup.
- Materialized and extent image modes use fresh named data volumes and separate
  remote prefixes. Quorum acknowledgements are measured; synchronous S3 durability
  is not implied. No production or live fleet data is used.
- Workloads use a 256-byte repeated text base plus sequence markers. SQL upserts
  and KV puts overwrite each worker's key; reads check exact returned values.
  Queue sends eight messages per batch and checks completed bodies, distinct
  message identities and empty queues. One database, one KV partition and two
  queues intentionally exercise a small, contended working set.
- The plan uses two-second warmups, 30-second closed-loop windows and two
  repetitions with concurrency orders `[1, 4]`, then `[4, 1]`.
- Existing fixture limits remain unchanged: 1,000-ms frontend node leases,
  3,000-ms frontend operation deadlines and 5,000-ms Queue callbacks. Admission
  and worker limits were not enlarged to obtain passing results.

The selected optimized test target compiled successfully. Its Cargo artifact
record, compiler identity, AArch64 ELF identity and matching before/after byte
hashes were captured. The remaining automatically selected application/example
builds were then interrupted before timing; their original failed/interrupted
Cargo command record is retained separately. The complete selected artifact was
packaged and checked against all 3,383 source inputs, including 60 unchanged
ignored assets. This is not a successful full Cargo all-binary build claim.

No owned build ran in the VM during measurement. Other checkouts' host compilers
were observed at four case starts; none were observed at the KV/Queue extent
case starts. Shared host/storage contention is uncontrolled, and no causal claim
or cross-mode performance comparison follows from these samples.

## Verified 30-second windows

Each row below represents **one** complete, verified window. No row has a
completed, error-free two-repetition matrix. Latency includes the complete HTTP
request and response. Percentiles use nearest rank over raw samples.

| Primitive / operation | Image | Concurrency | Requests/s | p50 ms | p95 ms | p99 ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| SQL upsert | materialized | 1 | 6.35 | 137.90 | 263.58 | 477.26 |
| SQL upsert | extents | 1 | 10.03 | 81.02 | 155.28 | 462.26 |
| SQL read | extents | 1 | 11.03 | 85.33 | 131.70 | 205.63 |
| SQL upsert | extents | 4 | 10.37 | 349.79 | 559.99 | 1,584.90 |
| KV put | materialized | 1 | 10.13 | 79.81 | 197.00 | 732.92 |
| KV get | materialized | 1 | 7.05 | 95.45 | 416.78 | 710.41 |
| KV put | extents | 1 | 15.54 | 61.18 | 94.14 | 138.97 |
| KV get | extents | 1 | 12.12 | 67.31 | 118.42 | 645.31 |
| Queue batch enqueue | extents | 1 | 3.68 | 253.35 | 453.83 | 648.34 |

The verified Queue window acknowledged and delivered 888 distinct messages in
111 batches. Its enqueue rate was 29.46 messages/s; including final drain and
verification, the rate was 26.79 messages/s. Its p99 is a **batch request**
latency, not a per-message delivery percentile. Materialized Queue failed during
warmup and supplies no qualified timed throughput measurement.

## Retained failures

| Case | Verified timed windows / planned | Request errors, including warmup | Observed failure |
| --- | ---: | ---: | --- |
| SQL, materialized | 1 / 8 | 10 | Read refusal followed by `NodeLeaseExpired` fencing and closed HTTP ingress |
| KV, materialized | 2 / 8 | 9 | Ambiguous command deadlines at concurrency four and later at concurrency one |
| Queue, materialized | 0 / 4 | 4 | Warmup enqueue deadlines; extra deliveries from uncertain sends; later drain refusal |
| SQL, extents | 3 / 8 | 9 | Read refusal from a quarantined group after a verified concurrency-four write window |
| KV, extents | 2 / 8 | 9 | Ambiguous refusal during concurrency-four writes, followed by quarantined-group refusals |
| Queue, extents | 1 / 4 | 9 | Ambiguous concurrency-four enqueue refusals, followed by quarantined-group refusals |

All six test processes exited 101, and both the runner and owning driver exited
one. Source inputs remained unchanged throughout measurement. None of the six
workload containers was OOM-killed; sampled voter and RustFS `oom_kill` deltas
were zero. RustFS did not restart. Those observations do not identify the root
cause of the fences, deadlines or quarantines.

An uncertain mutation can commit after its response deadline. For example,
materialized Queue observed 96 completed bodies against 88 acknowledged bodies
after failed enqueue attempts. No fresh-ID retries were issued to turn that into
a passing result. KV's final-value mismatches likewise occurred after ambiguous
writes; they are not sufficient evidence of lost acknowledged data or corruption.
The experiment does not yet reconcile every uncertain outcome independently.

## Follow-up: manifest exhaustion

A separate native diagnostic retained the original fixture quotas and deadlines,
and extended the offered KV window to reach the observed boundary. The first
typed quarantine was `Apply(Storage(group manifest: Limit))` (group-local,
`node_wide=false`), followed by an ambiguous sequence-1,024 command result. This
is diagnostic evidence, not a replacement performance measurement.

A read-only inspection of the retained SQL, KV and Queue extent-mode voter
volumes validated pack framing, record counts, bounds and identity hashes. Each
of the three affected serving stores in each workload held exactly 1,024
physical manifest records, matching the fixture's quota. Retained image-root
counts remained below their independent limits. The active-group manifest
reclamation path remains a priority; increasing a quota does not close it.
The temporary error probe was removed after the diagnostic exited with code 101
(the owning driver exited one). Evidence is retained locally under
`qualification/cg167-quarantine-evidence` and
`qualification/cg167-quarantine-probe-native` in this checkout's target directory.

## Resource observations

The following sampled CPU averages cover each whole case, including startup,
warmup, validation and failure handling. They are not pure timed-phase CPU costs.
Voter memory includes all three voters and their colocated HTTP client. Cgroup
memory includes file cache; the RustFS anonymous-memory maximum was 332–380 MiB.
Its retained-container lifetime peak is not a newly measured per-case peak.

| Case | Voter CPU cores | Max sampled voter memory MiB | RustFS CPU cores | Max sampled RustFS memory MiB |
| --- | ---: | ---: | ---: | ---: |
| SQL, materialized | 0.73 | 65.4 | 0.67 | 2,047.9 |
| KV, materialized | 0.41 | 80.7 | 0.43 | 2,048.0 |
| Queue, materialized | 0.39 | 48.2 | 0.43 | 2,048.0 |
| SQL, extents | 0.63 | 408.4 | 0.50 | 2,048.0 |
| KV, extents | 0.52 | 316.9 | 0.54 | 2,048.0 |
| Queue, extents | 0.46 | 298.4 | 0.47 | 2,048.0 |

Raw cgroup CPU, memory, anonymous/file-cache and block-I/O samples, complete logs,
source manifests, build/artifact records, all stopped workload containers and
their failed data volumes are retained locally. Credentials are excluded from
the published artifact. The raw artifact roots are:

```text
/Users/haipingfu/Workspace/crabbuild-target/cellar-tikv-cursor-refusal-6aff/qualification/primitives-perf-b101730f-real
/Users/haipingfu/Workspace/crabbuild-target/cellar-tikv-cursor-refusal-6aff/qualification/primitives-perf-b101730f-orchestration
/Users/haipingfu/Workspace/crabbuild-target/cellar-tikv-cursor-refusal-6aff/qualification/primitives-perf-b101730f-selected-artifact
```

## Next priorities

1. Add safe bounded active-group manifest reclamation for the reproduced page
   exhaustion, and independently diagnose the remaining lease and deadline failures.
   Identify the primary refusal before interpreting later connection/quarantine
   errors. Do not lengthen deadlines or leases to hide the failure.
2. Reconcile timed-out SQL/KV/Queue commands and Queue deliveries by original
   identity; distinguish delayed committed results from missing acknowledged work.
3. Re-run the unchanged complete matrix after a proven fix. Require every
   selected warmup, 30-second window, verification and joined shutdown to pass.
4. Extend the public primitive workload to separately constrained voter processes
   and a larger population before making node-capacity or scaling claims.

The follow-up harness changes correct the Queue measurement clock, clarify the
base-payload column, capture RustFS resource samples directly and independently
check Queue message-rate arithmetic. They do not change production Rust or
retroactively qualify the measured `b101730f` result.

Post-run harness validation passes strict Clippy for the affected integration
target, layout, documentation/native-dependency checks, 319 performance-tool
tests and 178 multi-Raft tool tests (one optional skip in each tool suite).
The corrected native test artifact builds successfully. Its short SQL and KV
smokes pass; Queue passes concurrency one but fails concurrency four with an
ambiguous command deadline, 823 ms remaining at target dispatch, and 56 completed
bodies versus 48 acknowledged. That failed smoke and its disks remain retained;
no repeated rerun was used to replace it with a passing result.
