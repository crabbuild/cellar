# MR-14 performance runner

`raft_run.py` runs a bounded reference workload against three to five native Cellar
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

Do not run two experiments simultaneously in the same VM. Stop the
previous run's exact containers first, preserving its volumes and evidence.
Each new resource carries `io.cellar.mr14.run=<run-id>`; the report names the
run, context and prefix. The runner generates fresh private RustFS credentials
and a private bearer file. It accepts no external bucket or credential options.
It retains resources on both success and failure. Inspect only the selected
context and label when stopping a run; never use a global prune command.
Retained empty networks consume Docker's finite default address pools. After
closing an experiment and saving its evidence, inspect the report's exact network
name in its recorded context. Verify its run label, that every owned container is
stopped and that the network's `Containers` map is empty. Save that inspection,
then remove only its immutable network ID. Keep data volumes and stopped
containers as inspection artifacts; use a fresh directory for the next run.

## Three-voter placement across up to five nodes

Use `local-five` or `local-five-s3` to run 150 cells with 30 active cells on five
nodes. Each cell has exactly three voters: its round-robin leader node and the
next two nodes, wrapping at the end. Each physical node registers only its own
replicas, and leads some cells while following others. This profile assigns 90
replicas and 30 leaders per node. It retains the same per-node CPU/memory limits
and offers 10 operations/second after 30 seconds of warmup for a 120-second
measurement. Those durations do not by themselves prove steady state.

The dedicated VM must expose at least `nodes + 3` CPUs and `(3 * nodes + 5)` GiB
of guest-reported total memory. Configure enough VM memory for the guest to expose that
amount (for example, 22 GiB for five nodes). Node CPU sets are distinct; RustFS
uses two separate CPUs. Profiles with empty hosts, more than 4,096 actual replicas
on any host, more than five nodes, or out-of-range published port ranges
are refused before workload resources are created.

`topology.json` retains complete before/after observations for every cell on
every node. Unassigned nodes must report no local registration. Leaders report
the actual runtime voter set; each assigned replica must independently serve its
own final state. Measurement and periodic samples cover all physical nodes and
verify the durable/attached group population. The analyzer refuses missing nodes,
extra replicas, changed placement, shared CPU sets or insufficient VM resources.
Fresh reports must include this evidence; use the recorded analyzer revision for
older evidence packages, or create a fresh experiment with the extended runner.

This tests distribution within the agreed local five-node ceiling. Q1 still
requires six processes and 10,000 three-voter cells; neither the host bound nor
the Q1 gate is reduced by these profiles. Fleet and bucket reference adapters
continue to use their separate three-node topology and contracts.

The [retained five-node runs](../../plans/multi-raft/evidence/MR-14-five-node-progress.json)
at source `bd7868f` use those unmodified profiles:

| Acknowledgement | Completed / offered | Overall p99 | Command p99 | Follower-query p99 | Generator rejections |
| --- | --- | --- | --- | --- | --- |
| Quorum | 1,200 / 1,200 | 64.7 ms | 147.2 ms | 22.9 ms | 0 |
| S3 | 1,158 / 1,200 | 232.7 ms | 260.2 ms | 35.7 ms | 42 `cell_busy` |

Latencies include generator scheduling. Quorum meets the proposed traffic
targets; S3 misses them. Both reconcile every submitted operation, all 450
replicas and final archive coverage. All six containers per run exited zero
without OOM; volumes and complete evidence remain, and the owned profile is
stopped. These are shared-Mac observations with host swap and other independent
Colima profiles. They do not establish steady state, a capacity ceiling or an
equivalent-guarantee speedup. The independent comparison refuses a ratio between
Quorum and S3 contracts.

The [archive wake-up runs](../../plans/multi-raft/evidence/MR-14-wakeup-progress.json)
repeat the same profiles after removing maintenance-timer waits. Subsequent runs
use the original cached image and then the fixed image again; all observations
remain visible:

| Source | Acknowledgement | Completed / offered | Overall p99 | Command p99 | Query p99 | Rejections |
| --- | --- | --- | --- | --- | --- | --- |
| Wake-up fix `8424f02` | Quorum | 1,170 / 1,200 | 432.9 ms | 651.2 ms | 219.5 ms | 30 |
| Wake-up fix `8424f02` | S3 | 1,142 / 1,200 | 662.6 ms | 721.5 ms | 113.2 ms | 58 |
| Original repeat `bd7868f` | Quorum | 1,200 / 1,200 | 86.4 ms | 145.5 ms | 13.7 ms | 0 |
| Original repeat `bd7868f` | S3 | 1,158 / 1,200 | 367.6 ms | 427.6 ms | 223.6 ms | 42 |
| Fix repeat `8424f02` | Quorum | 1,189 / 1,200 | 190.5 ms | 250.0 ms | 20.1 ms | 11 |
| Fix repeat `8424f02` | S3 | 1,194 / 1,200 | 160.8 ms | 205.6 ms | 15.0 ms | 6 |

The final fixed-image repeats meet both short-run traffic targets; the first
fixed-image pair misses both. All histories, 450 replicas and archive coverage
reconcile in each run. Shared-host load and swap vary, so these measurements
do not isolate the code change or
establish steady-state capacity. The evidence retains host observations and all
missed traffic targets. See the scheduling regression below for the narrower
behavior established by the fix.

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

## Controlled slow-follower experiments

`--scenario slow-follower --fault-profile tools/perf/raft_slow_follower.json`
periodically pauses the exact tagged node 3 during measurement. The checked-in
schedule holds it for 250 ms every second, starting after two seconds and ending
before the last two seconds. Observed pause duration, including Docker and probe
work, must remain below 750 ms. The runner proves Linux freezer support using a
disposable, network-isolated process before creating workload resources.
Pause/inspection calls use a persistent connection to the selected local Docker
Engine socket. The controller verifies the exact name and owner label, then binds
all requests to the immutable container ID. Ambiguous mutations are inspected,
never blindly retried. Resource creation, final cleanup and log capture use the
runner's CLI adapter. This avoids repeated CLI startup within each pause window;
the unchanged timing gate still includes all control and probe overhead.

Use the same profile with `--scenario healthy --fault-profile ...` for a control
with the same probe schedule and no workload-node pauses. Warmup stays healthy.
Both runs use the same image, resource limits, workload, acknowledgement mode and
follower-query policy. The comparison command includes the scenario and probe
configuration in its input contract, so different conditions cannot silently
produce an equivalent-speedup ratio.

```sh
python3 tools/perf/raft_run.py --image "$IMAGE_DIGEST" \
  --profile tools/perf/raft_profiles.json --profile-name local-five \
  --scenario slow-follower --fault-profile tools/perf/raft_slow_follower.json \
  --output /absolute/new/slow-follower-run
```

This pauses the whole process. Node 3 also leads some cells and serves follower
queries for others; `fault_result.cohort_summaries` separates those planned roles from
cells where it is the third voter and from unaffected cells. `faults.jsonl`
retains ownership checks, the scheduled and actual pause/release times and
leader-local replication samples. A command counts as healthy-majority progress
only if it starts after the pause is confirmed, completes before a matching
leader probe, and its exact history/term/index is covered by the two healthy
voters while the third remains behind. A recorded pause without this witness is
insufficient. These samples are diagnostic evidence, not acknowledgement proofs.

Final reads still verify every assigned replica and archive cut. The recovery
bound starts before requesting the last unpause and ends when all exact replica
reads complete, so it includes the release round trip and any remaining
measurement time and scanning overhead. The worker always inspects and releases
its owned target, including after an ambiguous pause response, and joins before
returning. An incomplete schedule, changed ownership, excess pause, missing lag,
unresolved history or failed recovery refuses qualification and retains evidence.
Use the source-pinned analyzer for earlier report packages; fresh reports must
declare their scenario and fault evidence explicitly. This experiment does not
qualify disk-only stalls, partitions, crashes, Fleet/bucket fault handling or soak.

The [retained controlled runs](../../plans/multi-raft/evidence/MR-14-slow-follower-progress.json)
at `97d00b4` use the unchanged five-node profiles and 116 observation cycles:

| Acknowledgement | Scenario | Completed / offered | Overall p99 | Traffic targets |
| --- | --- | --- | --- | --- |
| Quorum | Healthy control | 1,194 / 1,200 | 169.8 ms | Met |
| Quorum | Slow follower | 1,165 / 1,200 | 311.9 ms | Missed |
| S3 | Healthy control | 1,198 / 1,200 | 146.8 ms | Met |
| S3 | Slow follower | 1,176 / 1,200 | 332.5 ms | Missed |

All histories, 450 replicas per run and archive coverage reconcile. The two
injected runs retain 44/45 exact healthy-majority witnesses and 3.16/3.21-second
recovery upper bounds. Every pause stays below 750 ms. The initial CLI-controlled
five-node attempt exceeded that bound in eight cycles and remains rejected;
control-stage timings and an isolated alternating probe support the persistent
connection fix. A separate attempt failed before workload creation when Docker
address pools were exhausted; exact empty-network cleanup allowed a fresh retry.
Both failures remain in the raw capsule. The owned profile is stopped, volumes
remain, and other profiles were not mutated. These short shared-host observations
do not establish sustained capacity or an equivalent-guarantee speedup.

## Archive replay work

S3 acknowledgement still requires a physically verified recovery set and exact
conditional head publication. Recovery now replays a bounded chain into one
private image, preserving the physical check of every intermediate result and
syncing the final image once. Long histories use multiple batches constrained by
the destination's existing aggregate apply limits. This removes intermediate
image copies, syncs, renames and reclamations; it does not skip checksums, digest
scans, object readback, quorum commitment or the S3 proof gate.

The native `archive_tail_replay_seals_one_private_image` case checks exact restored
bytes and one publication for 32 tail entries. Separate cases exercise smaller
batch limits, corrupt intermediate identities, aggregate bounds, peak temporary
disk admission and process-kill publication cuts. These checks count physical
work; a deployment latency claim requires a fresh image and retained workload
measurement under the same resource and durability profile.

New committed images and protocol-only progress wake an idle archive scan.
Accepted archive progress covering a pending receipt also wakes the normal
completion path, which rechecks current authority, receipt identity and output
proof before responding. The single archive producer, fair batch cursor, capture
bounds and per-cell failure backoff remain in force.

The `archived_progress_wakes_commands_without_maintenance_poll` regression runs
eight sequential S3 commands through the public Node and real SQLite/LTX paths.
Only the host maintenance interval is stretched to one second. The original
implementation fails the four-second aggregate bound; both wakeups pass 20
consecutive repetitions. This amplified scheduling check is not a deployment
latency target. The [wake-up evidence](../../plans/multi-raft/evidence/MR-14-wakeup-progress.json)
retains the failed probes, passing regressions and fresh physical observations.

## Fleet and bucket reference adapters

The same image also builds `cellar-perf-baseline`. It uses the production native
`NodeFleet` host and the existing output gate. `reference_workload.rs` contains
the SQL schema, statements, payload bindings and transitions shared with Raft.
An application transaction stores the exact result and command identity beside
the mutation, including conditional no-ops. Duplicate identities return the
retained result; conflicting input/deadline, sequence gaps, expired deadlines,
additional clients and capacity overflow refuse execution. The baseline accepts
one client and retains at most 4,096 receipts per cell, without pruning. It is a
bounded benchmark application, not a new general SDK command API.

Use the original `local-q0` workload profile as input. The baseline records both
that source profile and an explicit effective profile: three native nodes, zero
Raft voters, owner reads, and either `fleet-or-bucket` or `bucket` output policy.
It preserves business operations, size distributions, random seed, offered
arrivals, limits and timings. Reads sent to another ingress still route to the
owner; the result must not be counted as a follower or replica read.

```sh
python3 tools/perf/baseline_run.py --mode fleet --image "$image" \
  --profile tools/perf/raft_profiles.json --profile-name local-q0 \
  --output /absolute/new/fleet-run
python3 tools/perf/baseline_analyze.py /absolute/new/fleet-run
# Stop this report's exact containers before running the bucket experiment.
python3 tools/perf/baseline_run.py --mode bucket --image "$image" \
  --profile tools/perf/raft_profiles.json --profile-name local-q0 \
  --output /absolute/new/bucket-run
python3 tools/perf/baseline_analyze.py /absolute/new/bucket-run
python3 tools/perf/compare_runs.py /absolute/new/fleet-run /absolute/new/raft-run
```

Both launchers refuse an active experiment with their ownership label. Resources
remain after each run, with no automatic deletion of voter or object-store data.
The baseline verifies its own executable hash against the same complete source
manifest, caps receipt demand before creating resources and treats every failed
mutation response as ambiguous. The independent baseline checker replays raw
history and reconciles exact state and receipt sequence through all three ingress
routes. `owner-reads.json` contains those observations; it is not replica evidence.
A fresh-owner/empty-directory native fixture also checks restoration of state,
exact successful and no-op receipts, and a subsequent noncommutative write.

| Path | Acknowledgement contract | Read path |
| --- | --- | --- |
| Raft Quorum | Durable majority plus committed-image publication | Declared Raft query consistency |
| Raft S3 | Quorum contract plus verified S3 archive coverage | Declared Raft query consistency |
| Fleet | All current ensemble follower fsyncs **or** a bucket proof, whichever wins; applicable authority checks still gate output | Authoritative owner |
| Bucket | Bucket proof and current ownership verification | Authoritative owner |

The native Fleet log recruits two followers in a healthy three-node fleet but
can retain service with one. It also races bucket publication on every write.
The baseline labels its response with this configured policy; it does not invent
a per-command proof source. Raw `/state` snapshots before and after each phase
retain aggregate output-proof counters. The analyzer checks monotonicity, errors
and source policy, and refuses a Fleet experiment that never exercises a Fleet
proof during measurement.

`compare_runs.py` reruns the appropriate independent analyzer for each directory.
It compares authority, retained-copy and object-store guarantees, receipt and
read semantics, exact workload, resource limits, VM, source manifest, executable
and build profile before emitting a throughput ratio. Different contracts produce
an explicit difference report with no ratio. Raft S3 and bucket both wait for
object storage, but their quorum and receipt guarantees still differ. These
adapters make that comparison executable; they do not establish an equivalent
contract, a steady-state result or an architecture speedup.

## Owner measurements

The benchmark's bearer-authenticated `GET /metrics` returns the same resource
and performance SDK snapshots as the production peer endpoint. The runner saves
`measurements.json` before and after each phase and `node-samples.jsonl` during
traffic, beside Docker resource observations in `resources.jsonl`. Each request
has host-monotonic start/end bounds. Node samples are sequential, not a
simultaneous distributed snapshot. Sampling overhead is part of this experiment.

The analyzer recomputes deltas from raw snapshots, verifies every node and phase,
and refuses missing/duplicate observations, source changes, counter resets,
unavailable clocks, saturated integers and impossible histogram populations or
sums. JSON counters use decimal strings to preserve integer precision. The
current analyzer requires these observations; historical runs must use their
pinned analyzer, and their missing measurements cannot become zero values.

The fixed engine/SQL, object API, transport and controller vocabulary and byte meanings are documented in the
[operations design](../../plans/multi-raft/operations-design.md#performance-observations).
Histograms include all measured outcomes with available timing. Their p50/p95/
p99/p99.9 values are inclusive bucket upper bounds; null means no samples or a
quantile in the unbounded final bucket, distinguished by count and buckets.
They are not the ledger's exact command/query latency quantiles. A completion
window may include work that started before its first sample; both boundary
in-flight counts remain in the report. No per-cell or per-peer metric labels are
allocated. There is no claim yet that this instrumentation has negligible cost.

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
publication. The [native evidence](../../plans/multi-raft/evidence/MR-14-archive-bundle-progress.json)
covers lost PUT responses, partial head failure, isolated restores and corruption
refusal; it retains the failing scheduling diagnostic and its repair. This
requires fresh experimental roots under the `/bundles-1` protocol identity;
all measurements below retain their original source and format. No batching
speedup or physical memory improvement has been measured.

MR-14 remains in progress. Remaining work includes full-device I/O attribution, instrumentation-overhead,
steady-state and density measurements,
and equivalent-guarantee Fleet/bucket comparisons. Executable baseline adapters
now share the SQL transition and open-loop scheduler; the contracts below remain
explicit. No cross-architecture throughput ratio is justified by merely running
the same business workload.

Bounded process-pause experiments have an explicit injection and reconciliation
path. Other fault scenarios and soak are refused before resource creation until
their capabilities exist. The Q1 six-process run and
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
