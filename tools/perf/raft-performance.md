# MR-14 performance runner

`raft_run.py` runs a bounded reference workload against three native Cellar
processes in the separate `cellar-multi-raft` Colima profile. Each Cellar process
has one pinned vCPU, 3 GiB RAM, no swap and a persistent volume. RustFS has two
CPUs and 4 GiB. The initial generator runs on the host; its durable journal and
scheduling costs are included in arrival-to-response latency and are reported
separately from request time. This is a local functional/performance experiment,
not qualification of independent failure domains or the Q1 topology.

## Build and run

Use the checkout's own target-cache ID. Source and executable checks happen
before creating the run directory or workload resources. The image must be an
immutable digest. The builder saves hashes of its copied source inputs, including embedded SQL,
protobuf, C, assets and build recipes, excluding generated caches;
the runner compares those inputs with the checkout and checks each running
process's `/proc/1/exe` against the measured executable hash.

```sh
docker --context colima-cellar-multi-raft build -f tools/perf/raft.Dockerfile \
  --build-arg TARGET_CACHE_ID=cellar-my-checkout-mr14-target \
  --build-arg SOURCE_REVISION="$(git rev-parse HEAD)" \
  -t cellar-multi-raft:mr14 .

image=$(docker --context colima-cellar-multi-raft image inspect \
  cellar-multi-raft:mr14 --format '{{.Id}}')
python3 tools/perf/raft_run.py --image "$image" \
  --profile tools/perf/raft_profiles.json --profile-name local-q0 \
  --scenario healthy --output /absolute/new/run-directory
python3 tools/perf/raft_analyze.py /absolute/new/run-directory
```

Use `local-q0-s3` to request S3-durable acknowledgements. The native ingress sets
that durability on every command and returns the achieved durability from the
SDK receipt. Both the online and independent checkers reject a mismatch.
Quorum and S3 write latency measure different contracts.

Do not run two three-node experiments simultaneously in the same VM. Stop the
previous run's exact containers first, preserving its volumes and evidence.
Each new resource carries `io.cellar.mr14.run=<run-id>`; the report names the
run, context and prefix. The runner generates fresh private RustFS credentials
and a private bearer file. It accepts no external bucket or credential options.
It retains resources on both success and failure. Inspect only the selected
context and label when stopping a run; never use a global prune command.

## Workload and evidence

The JSON profile explicitly names topology, cell population, uniformly sampled
size distributions, read fraction/consistency, hot-cell skew, arrival rate,
concurrency limit, timings, durability, resource limits and proposed numeric
SLOs. Initialization allocates padding in SQLite. Conditional sets and transfers
change two balances, revision and payload in the same command transaction.
Transfers and conditional updates do not commute. Followers apply LTX images.

An arrival has a fixed scheduled time independent of response speed. Busy cells,
full generator admission and late scheduling produce recorded rejections;
requests are not silently shifted to a later arrival schedule. At most one
operation is in flight per cell and at most the configured number are in flight
across cells. Each cell uses one stable command client with sequential IDs.
An unknown mutation outcome quarantines that cell across phases. The generator
does not invent a new identity or retry a write.

`operations.jsonl` reaches disk before submission and after response. It records
global event order, input/digest, command identity, retry deadline, requested read
consistency, exact returned scope/version/token and achieved write durability.
Latency starts at the original arrival. The journal also records dispatch delay,
invocation-journal time and request time. The report separates offered,
submitted, completed, server-rejected, ambiguous, failed-query and generator-rejected work. Timeouts
are additionally counted across their command/query outcomes.
“Submitted” is generator admission; it does not assert server admission for an
ambiguous request.

The independent analyzer replays the serialized per-cell transitions and checks
command sequence, result, scope, durability and query minimums. It reconciles
every final replica and archive cut with the ledger and recomputes throughput
and command/query quantiles. This is narrower than the MR-15 concurrent history
checker. Ambiguous or overlapping per-cell histories refuse reconciliation.

`completed` means the experiment reached its replica/archive checks.
`traffic_objectives_met` separately records the proposed latency, throughput and
error objectives. Warmup is explicit but does not establish steady state by
itself. A successful functional run is not a capacity claim.

## Batching and Q1

The runtime retains one persistence-queue slot per group with an outstanding
write. Shared batches respect both the byte and group limits. The first write
that does not fit stays at the head for the next batch, so smaller writes cannot
continually bypass it. Transient log admission refusal restores the original
queue order. Sync completion still verifies the batch operation and every group
identity before advancing Ready; per-group persistence/application order is
unchanged.

The focused cases cover a large waiter, mixed leader/follower roles, independent
progress and election while a hot group is stalled, and process kills before
sync, after sync and after acknowledgement of a shared three-group batch.

```sh
python3 tools/perf/raft_run.py --profile tools/perf/raft_profiles.json \
  --profile-name q1-capacity-plan --capacity-only
cargo test -p cellar --test native_raft_multigroup --locked -- --test-threads=1
python3 -m unittest discover -s tools/perf -p test_raft_run.py
```

Q1 remains six processes and 10,000 three-voter cells: 5,000 voter replicas per
process on average, or 6,000 with a proposed 20% repair reserve. The current
4,096-group host bound does not admit that placement. The runner reports the
arithmetic and refuses the unsupported topology. No raised constant or count of
registered cells establishes measured capacity.

## Remaining milestone gates

The native `repair_budget_under_load` case holds real TLS snapshot transfers for
two lagging cells while a third completes 24 commands and 24 AtLeast follower
queries. It samples actual node-wide reservations, checks both distinct restored
histories and waits for physical reservation release. This is bounded catch-up
coverage on three embedded nodes, not replacement throughput or an SLO.

The native archive producer can coalesce snapshot/tail payloads for two cells in
one bounded object. The bundle index binds each exact history; recovery verifies
the object and physically replays the selected cell before its independent head
publication. Native tests cover lost PUT responses and isolated restores. This
requires fresh experimental roots under the `/bundles-1` protocol identity;
all measurements below retain their original source and format. No batching
speedup or physical memory improvement has been measured.

MR-14 remains in progress. Remaining work includes all required internal
latency/byte/controller measurements, steady-state and density measurements,
and executable Fleet/bucket adapters with matched durability/hardware/workloads.
`matched_comparison` refuses missing or unequal contract, hardware, workload and
build-mode records; this guard is not a measured baseline result.

Fault scenarios and soak are refused before resource creation until their
injection and reconciliation capabilities exist. The Q1 six-process run and
large-fleet gates remain unqualified within the agreed local five-node ceiling.
MR-13's remaining management/restore gates also remain open.

## Initial local measurements — October 22

Five healthy 12-cell runs retained their complete ledgers and checked every cell
on every voter. The final two runs use source `c1992ee` and fix offered-operation
randomness so generator rejection cannot change subsequent cell/operation
choices. The earlier three are diagnostic observations; they predate that fix.
All use the same native executable hash and the 1-vCPU/3-GiB node limits.

| Run | Durability | Completed / 200 offered | Generator rejections | Arrival p99 |
| --- | --- | ---: | ---: | ---: |
| Diagnostic 1 | Quorum | 190 | 10 | 535 ms |
| Diagnostic 2 | Quorum | 200 | 0 | 38 ms |
| Diagnostic 3 | S3 | 177 | 23 | 230 ms |
| Fixed arrivals 1 | Quorum | 168 | 32 | 456 ms |
| Fixed arrivals 2 | S3 | 133 | 67 | 807 ms |

Every submitted operation in these runs received a successful response and passed
reference reconciliation. Rejections count against offered load; they are not
removed from the error fraction. The final runs both miss their proposed traffic
objectives. These small measurements do not establish steady state, capacity or
a performance improvement. Quorum and S3 are different acknowledgement contracts.

Timing in the second diagnostic run did not reproduce the first run's long
client-side delay: invocation-journal p99 was below 1 ms. The first outlier is
unresolved. After the final experiments, the host reported 32 GiB physical RAM
and about 18 GiB of swap in use; its three configured VMs totalled 34 GiB. Those
are observations after the runs, not proof of their cause. Controlled host
memory conditions and internal fsync/apply/archive timing are required before
latency qualification. The owned test profile was stopped after evidence
capture; its disks and logs remain available.

The portable evidence capsule and source/test pins are recorded in
`plans/multi-raft/evidence/MR-14-performance-progress.json`. Earlier failed
objectives are retained alongside later results.
