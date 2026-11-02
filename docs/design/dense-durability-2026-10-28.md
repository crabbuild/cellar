<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Dense cell state durability experiment — 2026-10-28–29

Purpose: Record the three-node RustFS durability experiment with ten one-GiB cells and its qualification limits.

Status: **measurements complete**. Fleet failed during traffic. Bucket passed
traffic and audits but failed shutdown. Quorum ingestion and unpaced writes
received overload refusals; its separate existing-state reads and archive-paced
writes passed. The requested configuration did not pass all workloads and
lifecycle checks. Short windows do not establish steady-state capacity.

The [empty-state follow-up](empty-state-durability-2026-10-29.md) repeats the
same node budgets without bulk padding; all three modes passed that short
read/write matrix and process shutdown.

The checkout was fast-forwarded from `origin/main` to
`1be27d234d4674d4d014785c60cecbfe394bdab9`. Production crates remain unchanged;
this experiment adds benchmark setup and auditing in `tools/perf/`.

## Requested workload

- Three native Cellar containers, each pinned to one vCPU with a 3-GiB memory
  ceiling and no extra swap. Quorum uses three voters for each of ten cells.
- Ten cells, each containing exactly 1,073,741,824 bytes of dense SHA-256-derived
  payload, plus SQLite, business-row and receipt overhead. This interprets
  “1 GB” as 1 GiB. Fleet/Bucket payloads are populated through bounded native transactions; the
  separate Quorum existing-state case uses verified initial images as described below.
- A separate RustFS container, pinned to two vCPUs with 4 GiB memory, using
  `ghcr.io/rustfs/rustfs@sha256:bffcab0c9d647aab0055d1c69d340b202d0909966b385932d4ead1aeb7602858`.
- One-KiB hot-row writes and small state reads, planned at concurrency 1, 3 and
  10, with two ten-second repetitions in reversed order. Warmup is separate.
  The client uses persistent HTTP connections, never retries writes, and
  quarantines ambiguous outcomes.
- Actual Docker quotas and deployed executable hashes are checked. Each cell's
  size and first/middle/last block hashes are audited before and after traffic;
  every business response is checked against an independent model. Quorum also
  checks every replica and requires archive coverage through the last receipt.

See [the fixture](../../tools/perf/dense.py) and [reproduction notes](../../tools/perf/dense.md).
The fixture uses the optimized `lab` profile, not the shipped release profile.
Fleet/Bucket use the fixture policy of 64 activations, four evictions and 32
releases, with 32 resident cells and 128 root requests. Quorum uses a 500-ms
tick, two SQL workers and two archive producers. These settings are part of
this experiment; other policy choices have not been qualified.
The build/source manifest and binary hashes are retained in each raw report.
The incremental Quorum setup overlapped lint/build-cache cleanup; its population
wall time is not a matched loading-throughput comparison. Builds and cleanup
finished before all timed traffic. Quorum has a 1,152-MiB extent-backed image quota to allow SQLite/receipt overhead.
Bulk population has a 120-second Quorum setup budget; timed commands retain
its normal five-second budget.

## Results

| Mode | Full dataset populated | Observed write window | Outcome |
| --- | --- | --- | --- |
| Fleet | Yes; initial size/hash audit passed | One 10-s concurrency-1 window: 21.31 writes/s, p50 15.74 ms, p99 334.64 ms | **Failed** during concurrency 3; a mutation became ambiguous and a node was OOM-killed. |
| Bucket | Yes; initial and final audits passed | Concurrency 10: 348.10–663.32 writes/s; p99 37.26–85.56 ms | **Traffic passed**, independently reconciled; all three nodes OOM-killed during shutdown. |
| Quorum, incremental load | No | No full-size timing | Explicit non-ambiguous `Overloaded` refusals during population. |
| Quorum, verified preloaded state | Yes; all 30 full image digests verified | Unpaced warmup: 6 writes, then timed refusal; separately 10/10 archive-paced writes passed | **Unpaced write sweep failed** with explicit non-ambiguous `Overloaded`; separate reads and paced writes passed, with clean shutdown. |

Fleet population took 946.9 seconds. Its first timed
concurrency-3 window returned only about 4 successful writes/s and lost a
connection. It is not an error-free throughput result. The full matrix,
post-traffic audit and repeated confirmation were not completed.

Docker and kernel evidence confirm that all three Fleet Cellar containers
were OOM-killed under their 3-GiB cgroup limits. Node 3 died during the timed
window; nodes 1 and 2 died while stopping. The killed processes had about
3,115,000–3,119,000 KiB anonymous RSS, so this was application memory pressure,
not merely a high page-cache observation. RustFS was not OOM-killed.

## Quorum initial state construction

Incremental population reached roughly 280–388 MiB of acknowledged payload per
cell before three workers received explicit `Overloaded` refusals. No full-size
window ran in that attempt. The raw refusals are marked non-ambiguous; no
mutation was retried. This is a load/admission failure, not a one-GiB TPS result.

The separate preloaded case uses the repository's full-capacity qualification
procedure: create real permanent initial-member claims, independently verify
complete template bytes into each voter's extent store, and persist identical
initial positions before any runtime starts or command is acknowledged.
The normal SDK then supplies live linearizable reads, ordinary majority-proven
writer initialization, all-replica data checks and verified archive coverage.
This simulates existing state and does not qualify a production bulk-import API
or ingestion throughput. Cold writer setup has its own recorded 120-second
budget; timed commands retain five seconds.

The VM data disk was expanded from 160 to 320 GiB before this separate case;
CPU and memory quotas stayed the same. The same workspace SSD backs both.
The source and executable identities for the two fixture revisions are recorded
separately. No production crate changes are included.

## Quorum existing-state observations

The initial preloaded run verified 1,075,163,136 SQLite image bytes per cell on
all three voters, then archived the initial committed state. Cold writer
initialization succeeded for two cells and was explicitly refused for the third.
A second attempt restarted the intact disks with fresh process identities and a
fresh client, drained archive coverage through each cell's actual applied index,
and initialized all ten writers with ordinary majority-proven commands. Every
replica passed the before-traffic size and sampled-hash checks. These setup
commands had their separate 120-second budget and took 269–923 ms each.

The five-second unpaced concurrency-1 warmup completed six writes, about
1.14 writes/s, with p99 1,866.59 ms. The first timed write then received HTTP 503
with `ambiguous: false` and `Overloaded`. That zero-success failed window remains
in the ledger and is not a throughput result. No mutation was retried and no
Quorum node was OOM-killed. The logs do not establish one root cause for all
admission refusals.

A final intact restart used the same dataset, executable and node quotas. It
verified the previous acknowledged checkpoint, drained applied archive positions,
and measured follower reads with the latest acknowledged version token. All six
ten-second read windows completed without request errors:

| Operation | Concurrency | Successful ops/s | Worst p99 (ms) |
| --- | ---: | ---: | ---: |
| Follower token read | 1 | 1,014.87–1,038.94 | 1.14 |
| Follower token read | 3 | 2,589.85–2,810.11 | 1.47 |
| Follower token read | 10 | 3,826.00–4,213.77 | 3.25 |

The separate ten-write probe used the ordinary five-second command budget,
with complete archive drain before and after each write. All ten writes passed,
one per cell, without retries. Raw wire latency was 336.34–1,207.91 ms (median
714.37 ms). Including archive waits and client bookkeeping, this deliberately
paced sequence took 112.37 seconds: **0.089 writes/s**. This is ten isolated
writes, not a sustained-rate or concurrency-capacity measurement.

All 180 before/after sampled-block audits passed across all thirty replicas;
archive coverage reached applied state. An independent checker replayed the
three-run checkpoint chain, recomputed all read-window samples and quantiles,
and independently regenerated the audited block hashes. It reconciled 154,994
ledger records in the final run, with zero unresolved mutations. The earlier
preload and resumed-run refusals remain failures in their own reports.

The three Cellar nodes and RustFS all exited zero after the final experiment,
with no OOM kills. Maximum sampled Cellar working set was about 147 MiB;
cgroup charge including cache reached the 3-GiB limit. A clean process exit
is not a joined SDK drain or crash-recovery qualification.

The restart scripts and process-identity proofs are retained under
`resumed-quorum/` and `read-paced-quorum/`; they do not modify state files or
bypass live SDK durability acknowledgements. An initial restart before RustFS
was ready failed startup; its logs are retained separately and excluded from
measurements.

## Bucket traffic measurements

All twelve ten-second windows completed without request errors. Values below
show the range across the two repetitions; the p99 column is the worst p99
observed in either repetition. The second write sweep was substantially faster,
so these are short-window observations, not demonstrated steady-state capacity.

| Operation | Concurrency | Successful ops/s | Worst p99 (ms) |
| --- | ---: | ---: | ---: |
| Write, 1 KiB | 1 | 99.87–131.29 | 69.44 |
| Write, 1 KiB | 3 | 164.27–294.85 | 92.20 |
| Write, 1 KiB | 10 | 348.10–663.32 | 85.56 |
| Owner read | 1 | 987.11–996.60 | 1.51 |
| Owner read | 3 | 2,639.26–2,821.54 | 1.19 |
| Owner read | 10 | 3,936.68–4,048.97 | 2.92 |

The independent checker replayed every mutation, checked all read results,
recomputed every window's quantiles from its raw ledger, and reconciled the
final state. Sixty retained pre/post sampled-block audits passed. The maximum
sampled Cellar working set before shutdown was about 306 MiB; cgroup charge,
which also includes file cache and other memory, reached the 3-GiB ceiling.

The default SIGTERM path in the embedding fixture uses `ShutdownMode::Handoff`.
All three Bucket nodes were OOM-killed during that sequential handoff shutdown,
after the final audit and timed traffic. This run does not qualify shutdown,
handoff or preserving restart. The runner's `completed: true` means its traffic
and data-audit gates finished; `post-stop-containers.json` and `kernel-oom.log`
retain the separate lifecycle failure. RustFS was not OOM-killed.

## Scope and retained evidence

All containers share one Mac, one 8-vCPU/16-GiB Linux VM, and its native ext4
virtual data disk. The disk backing file is on the workspace SSD and is sparse;
physical capacity is not pre-reserved. No independent-host failure domains,
network latency, Amazon S3 behavior, crash recovery or soak test is qualified.
Latency excludes the client's pre-request ledger sync; throughput includes
all client bookkeeping. These are closed-loop measurements.

Quorum acknowledges durable Raft majority commitment and archives separately.
Fleet acknowledges required follower persistence or bucket publication,
whichever completes first. Bucket requires object-store durability. Quorum
reads use a follower and the last acknowledged version token; Fleet/Bucket
reads route to the owner. These are different guarantees and read paths.

Evidence directory (private, contains a separate credential configuration):
`/Volumes/Workspace/crabbuild-target/cellar-2446b3c0-dense/`.
The main native-storage experiment is `native-full/`, with raw operation
ledgers, latency samples, audits, container outcomes and resource samples.
The verified initial-state case is `preloaded-full/quorum/`; intact-restart
follow-ups are `resumed-quorum/quorum/` and `read-paced-quorum/quorum/`.
The retained `check_quorum_chain.py` independently verifies the latter chain;
`analyze.py` and `check_failed_prefixes.py` reconcile the original runs.
The measured initial fixture image was
`sha256:d018dee2c441addcd5a2a754f05873e7f04a39d827d56e451ffbb2d548922e05`;
the preloaded/restarted Quorum image was
`sha256:60bc3a909d187898cd19eb201ba71c56244b35e30ae20155ce2774941daf49f0`.
Both image source manifests and deployed executable hashes were checked before
running. A subsequent benchmark-only guard makes missing audit blocks return
an error; that invalid-input path was not exercised in measurements.
`native-storage.json` and `vm-data-relocation.json` record the filesystem and
verified disk relocation. Do not publish the `config/` subdirectories.

The initial 4-MiB three-cell smoke tests passed all three modes and independently
reconciled, but overlapped build work and are functional evidence only. A direct
virtiofs Quorum setup and a nested loop-backed ext4 pilot hit the original
five-second population deadline before the target dataset; their failed
attempts and ambiguous commands remain separate. They provide no full-size
performance result.

## Validation

Passed: two shared Rust workload tests; two preload fixture tests (including
changed-template rejection); 36 existing Python benchmark tests; Python syntax
check; `cargo format --check`; module layout (1,753 Rust files); native dependency
check (496 reachable packages, no JavaScript engine); and documentation checks.
Final Linux/arm64 Clippy passed for both benchmark executables with `-D warnings`.
The first dependency-check invocation lacked Cargo on `PATH`; its corrected
rerun passed. These checks qualify the benchmark additions, not production
behavior under failures.

Bucket traffic independently reconciled. Fleet and incremental Quorum
acknowledged prefixes independently reconciled, with one Fleet ambiguous
mutation retained unresolved. The three Quorum existing-state runs independently
reconciled as a checkpoint chain, preserving both explicit overload refusals;
the final read/paced-write run passed all traffic and data-audit gates.

All experiment containers are stopped. Raw data, logs and executable images
are retained for inspection. The dedicated benchmark VM is stopped after
validation to release its CPU and memory allocation; the user's default VM is
not part of this experiment.
