<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Empty-state durability experiment — 2026-10-29

Purpose: Compare Quorum, Fleet, and Bucket durability using ten initially empty cells on the same constrained nodes.

Status: **complete**. All three modes passed the full read/write matrix, final
state audits and process shutdown, with zero request errors, overload refusals
or OOM kills. This repeats the experiment without the one-GiB payload per cell.

The [Quorum/Fleet path diagnosis](quorum-throughput-diagnosis-2026-10-29.md)
investigates the throughput difference using stage metrics, a paused voter,
syscall traces and controlled storage/configuration comparisons.

## Method

The production revision remains `1be27d234d4674d4d014785c60cecbfe394bdab9`.
All three modes start with new data directories, a fresh RustFS bucket and ten
new cells. Setup creates the SQLite workload schema and initial business rows
with `database_bytes: 0`; there are no bulk population commands, preloaded
images or retained cells from the dense-state run. Timed commands replace a
one-KiB hot-row payload. State and normal durability/receipt metadata grow during
the experiment; “empty” does not mean a zero-byte physical database.

Resources and workload match the [dense-state experiment](dense-durability-2026-10-28.md):

- Three Cellar containers, each pinned to one vCPU, with a 3-GiB memory limit
  and no extra swap. Quorum replicates all ten cells to three voters.
- RustFS runs separately on two pinned vCPUs with a 4-GiB memory limit and the
  same pinned image digest. The 8-vCPU/16-GiB Linux/arm64 VM uses the same native
  ext4 data filesystem on the workspace SSD.
- One-KiB writes and small reads, concurrency 1, 3 and 10, two ten-second windows
  each. The second sweep reverses case order. Five-second write warmup is separate.
- The same `lab` build profile, normal foreground budgets and policy settings.
  Quorum retains its extent-backed image configuration and 1,152-MiB maximum
  image allowance; the allowance does not preload or allocate a GiB of state.
- Persistent HTTP connections, disjoint worker cell sets, no mutation retries,
  fsynced operation ledger and independent business-state replay. Latency excludes
  the pre-request ledger sync; throughput includes client bookkeeping.
- Before/after business-state audits cover every cell, on all three Quorum
  replicas. Quorum checks archive coverage through the final acknowledged index.
  Dense-block size/hash checks do not apply because there are no dense blocks.
- Container CPU/memory/swap settings and deployed executable hashes are verified.
  Resource sampling, cgroup OOM counters and post-stop exit states are retained.
  Traffic completion and shutdown success are evaluated separately.

Quorum writes acknowledge durable Raft majority commitment, and its reads use a
follower with the latest acknowledged version token. Fleet acknowledges required
follower persistence or bucket publication, whichever completes first; Bucket
requires object-store durability. Fleet/Bucket reads use the owner. Rates compare
different guarantees, rather than equivalent durability or consistency.

## Results

All 36 measured windows passed. Values show the range across two ten-second
repetitions; p99 is the worst of the two windows, not a pooled quantile.

### One-KiB writes

| Durability | Concurrency | Successful ops/s | Worst p99 (ms) |
| --- | ---: | ---: | ---: |
| Quorum | 1 | 23.58–24.31 | 70.88 |
| Quorum | 3 | 31.17–40.05 | 180.83 |
| Quorum | 10 | 37.31–56.63 | 560.30 |
| Fleet | 1 | 93.88–97.31 | 29.15 |
| Fleet | 3 | 103.25–124.01 | 74.36 |
| Fleet | 10 | 103.35–158.06 | 192.87 |
| Bucket | 1 | 110.99–128.24 | 42.13 |
| Bucket | 3 | 258.90–303.72 | 77.34 |
| Bucket | 10 | 475.64–535.01 | 125.31 |

### Small reads

| Durability | Concurrency | Successful ops/s | Worst p99 (ms) |
| --- | ---: | ---: | ---: |
| Quorum | 1 | 1,070.60–1,081.26 | 0.97 |
| Quorum | 3 | 2,854.42–2,950.01 | 1.21 |
| Quorum | 10 | 3,762.20–3,911.51 | 2.06 |
| Fleet | 1 | 1,075.47–1,145.68 | 1.05 |
| Fleet | 3 | 2,562.85–2,724.59 | 1.13 |
| Fleet | 10 | 4,108.97–4,126.03 | 1.84 |
| Bucket | 1 | 1,103.13–1,141.48 | 1.14 |
| Bucket | 3 | 2,842.76–2,864.82 | 1.05 |
| Bucket | 10 | 4,082.33–4,186.34 | 1.93 |

### Memory and shutdown

These figures are the maximum across the three Cellar nodes, over all sampled
phases. Docker working set excludes reclaimable file cache; cgroup charge
includes cache and other charged memory. The recorded cgroup peaks are captured
before shutdown. All three Cellar nodes and RustFS exited zero in each mode;
post-stop inspection reported no OOM kills or cleanup errors.

| Durability | Maximum sampled node working set (MiB) | Maximum pre-stop cgroup peak (MiB) |
| --- | ---: | ---: |
| Quorum | 22.36 | 87.75 |
| Fleet | 49.93 | 87.76 |
| Bucket | 51.78 | 188.36 |

### Comparison with one-GiB cells

- **Quorum:** the dense existing-state run hit overload on unpaced writes.
  Empty-state writes completed every concurrency level, reaching 37.31–56.63/s
  at concurrency 10. Follower reads remained in a similar range: 3,762–3,912/s
  empty versus 3,826–4,214/s dense, at concurrency 10.
- **Fleet:** the dense run completed 21.31 writes/s at concurrency 1 and then
  OOM-failed at concurrency 3. Empty state completed 93.88–97.31 writes/s at
  concurrency 1 and passed the full matrix and shutdown.
- **Bucket:** empty-state concurrency-10 writes reached 475.64–535.01/s,
  overlapping the dense run's 348.10–663.32/s. These short runs do not establish
  a throughput gain. Empty-state shutdown passed; all three dense-state nodes
  were OOM-killed during handoff shutdown.

The main observed change is successful completion and much lower memory use.
Production code, node budgets and policies are unchanged. The Quorum executable
SHA-256 is identical to the preloaded dense-state run. The Fleet/Bucket fixture
also includes the previously recorded missing-audit-block error guard, outside
timed request paths. These are separate runs with timing/cache variation, rather
than a statistically controlled estimate of a state-size multiplier.

## Validation

An independent checker replayed all 534,981 ledger records, verified every
acknowledged transition and read, and recomputed every window's sample count,
throughput and latency quantiles. It reconciled all final states and the 100
before/after state audits (60 Quorum, 20 Fleet, 20 Bucket). All initial database
seed values were zero, with no bulk population. Quorum archive coverage reached
all final acknowledged positions. Every observed cgroup OOM counter was zero.

Passed: source/executable provenance checks, immutable Linux/arm64 image build,
Python syntax, 36 existing Python benchmark tests, documentation checks and
`git diff --check`. This follow-up changes the Python runner and its notes;
Rust sources remain as validated by the preceding formatting/layout, Rust tests
and Clippy checks.

## Reproduction and retained evidence

Build the current checkout as described in [the fixture notes](../../tools/perf/dense.md),
then run:

```sh
python3 tools/perf/dense.py --context colima-dense-2446 \
  --image IMMUTABLE_IMAGE_ID \
  --output /Volumes/Workspace/crabbuild-target/cellar-2446b3c0-dense/empty-2026-10-29 \
  --guest-data-root /mnt/cellar-dense-native-2446 \
  --cell-mib 0 --cells 10 --write-bytes 1024 \
  --seconds 10 --repeats 2 --concurrency 1 3 10
```

The private evidence root is
`/Volumes/Workspace/crabbuild-target/cellar-2446b3c0-dense/`.
Each mode retains a report, complete operation ledger, state audits, node logs,
resource samples and container outcomes. Do not publish its `config/` directories,
which contain credentials. The independent checker is retained as `analyze.py`;
reconciled results are in
`empty-2026-10-29/analysis.json`. The runtime image ID is
`sha256:e247a427094f14954d56c66c437002f84139235b299e9f1c2e221e4ea8cd71ed`;
its source-manifest SHA-256 is
`29b20417c932c694bddb52678093aa4b0c933001f3849e7d7e3ebd40679aeb78`.
Quorum executable SHA-256:
`32312180f0659de3836679e52af85e7b5d224988fae5699d7bd7ec7877fad4bc`.
Fleet/Bucket executable SHA-256:
`82ebbe172993f6d48c85373c322eb11510c76abb14f8a82c03f43806d68a79f6`.
The experiment containers are stopped and retained. The dedicated benchmark VM
is stopped after validation, with the prior dense-state data preserved.

These are short, closed-loop measurements on one physical host/VM/SSD and a
local S3 simulation. They do not qualify steady-state capacity, independent-host
availability, cloud S3 performance, crash recovery or a joined SDK shutdown drain.
