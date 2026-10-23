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

`local-density-500` and `local-density-1500` retain the `local-five` workload,
timings, 30 active cells, resource limits and Quorum acknowledgement contract.
They increase only the total seeded live cell population, to 300 and 900 voter
replicas per node respectively. Every cell still has actual consensus, SQLite
state and archive coverage; inactive application cells continue Raft ticks.
Compare these with a fresh same-image `local-five` healthy run without fault
probes. This ladder measures the cost of additional mostly idle replicas under
fixed active traffic. It does not test Q1's active fraction, six processes,
database distribution or 24-hour failure/recovery requirement. Admission and
the 4,096-group host bound are unchanged; a completed run is not a density claim.

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
checker. Overlapping per-cell histories refuse reconciliation. Ambiguous commands
remain quarantined; only the explicit recovery scenario below can resolve them
with additional evidence after measurement.

`completed` means the experiment reached its replica/archive checks.
`traffic_objectives_met` separately records the proposed latency, throughput and
error objectives. Warmup is explicit but does not establish steady state by
itself. A successful functional run is not a capacity claim.

`kernel-measurements.json` records cgroup v2 and PID 1 observations before and
after each phase for every Cellar container and RustFS. Raw source IDs, container
start time and process start ticks must remain stable. The analyzer rejects
missing nodes, changed limits, restarts, reset counters and intervals that do not
enclose the workload. It recomputes CPU use/throttling, cgroup I/O deltas, lifetime
peak memory, boundary RSS/thread/FD counts and the declared memory headroom gate.
`memory_objectives_met` is separate from traffic and history results.

CPU rates are reported as a range bounded by the observation request windows;
they cover the whole container, including observer commands. Memory peak includes
startup and warmup. Boundary FD/thread counts are not interval peaks. Cgroup I/O
is charged container I/O, not complete physical-device or host-volume attribution.
The report retains observation overhead instead of subtracting an assumed cost.
Use the recorded analyzer revision for older evidence; fresh reports require
these kernel observations, while the internal format marker remains 1.
Periodic resource and runtime sampling begins before cell initialization, after
full topology and executable verification. Seed ledger events include monotonic
timestamps. A refused seed additionally records bounded read-only views from its
three assigned peers and best-effort kernel observations before shutdown. These
diagnostics retain failures; they never retry a mutation or turn a refused seed
into a successful workload run.

## Current density qualification

The [paired 1,500-cell sequence](../../plans/multi-raft/evidence/MR-14-repeat-progress.json)
uses the same immutable image built at `f96a94b`, with compiled-source-equivalent
checkout `70c5695`. Five nodes retain one CPU and 3 GiB each, with 900 replicas
per node. Thirty cells are active at ten offered operations per second. Healthy
controls use the same probe schedule as the pause attempt.

| Run order | Recorded completions / offered | Overall p99 | Qualification |
| --- | --- | --- | --- |
| Healthy control | 1,200 / 1,200 | 95.426 ms | Short-run objectives and all 4,500 replicas verified |
| Slow follower | 1,168 / 1,200 | 341.527 ms | Unqualified: probe timeout, incomplete fault schedule |
| Healthy control | 1,199 / 1,200 | 79.254 ms | Short-run objectives and all 4,500 replicas verified |

The failed run records only 91 complete cycle shapes out of 116 scheduled cycles.
Its sixth leader probe in cycle 91 is missing; cleanup confirms unpause. The
measurement continues after fault scheduling stops, so its diagnostic p99 does
not represent the declared fault profile. Final replica and archive checks never
run. The source/event prefix infers cell 25 as the missing probe; its failed-call
start, exact budget and exception type were not recorded by that image. The
subsequent diagnostic records those fields directly. Neither the paired controls
nor this failed attempt establish steady-state capacity or a causal speedup.

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
Failed probe calls record `probe_failed` with the cycle, cell, requested timeout,
start time, elapsed time and exception type before owned-target cleanup. The
failed read is not retried. Successful cleanup does not qualify the attempt,
and a later successful probe cannot conceal the failed call. The 100 ms probe
cap and overall pause/recovery bounds remain unchanged.

Tagged fault probes also carry a `cycle:cell` identity in `x-cellar-probe` and
the fault ledger. The fixture retains at most 64 server observations, including
separate status and replication await boundaries. The authenticated
`/probe-timings` endpoint exposes source identity, sequence and eviction counts;
the sampler retains its responses and failures in `probe-timings.jsonl`.
Only tagged reads enter this history. Cancellation preserves missing end times,
and an evicted request cannot overwrite a newer request with the same tag.
These diagnostics use a fixture-owned monotonic clock with a separate origin;
compare stage durations, never subtract them from Python or performance clocks.
A server completion can occur after the client times out. It is diagnostic
evidence only and never replaces the original HTTP result, quorum witness or
replica/archive verification. Sampling gaps and evictions remain explicit.

Each probe's existing fault-ledger record also retains up to sixteen client
HTTP spans: connect, send, response headers and body. The urllib connection
methods, timeout, result and exceptions are preserved; the opener is created
before measurement and no retry or extra ledger sync is added. Send may include
connect, so nested spans overlap and must not be added. Missing/overflowed spans
remain explicit. Only phase names, monotonic times and exception types are
recorded: no credentials, URLs, headers or response bodies. These HTTP client
spans distinguish the location of a wait; they do not explain its cause or
override the original probe outcome. HTTP error response bodies are explicitly
closed, including when reading the error body times out.

The [first stage-timed pair](../../plans/multi-raft/evidence/MR-14-probe-progress.json)
at `17fd0b1` completes the slow-follower schedule and verifies 4,500 replicas and
archive coverage, while missing the unchanged traffic targets (304.107 ms p99,
24 rejected arrivals). Its 696 probes all correlate with server observations.
The matching healthy control reproduces a 100 ms timeout without a node pause:
probe `68:25` has no server-handler observation, while all other 413 attempts
correlate. The source-stable final sample, more than fifty seconds later, still
ends at `68:20`; no later requests could have evicted the missing probe.
This narrows further diagnosis to the path before recorded handler entry; it
does not establish an ingress, scheduling or shared-host cause. That control
stays unqualified, including its 638.467 ms diagnostic p99 and 92 rejections.
Periodic host/VM pressure observations remain raw evidence. No performance
correction, instrumentation-overhead qualification or speedup is claimed.

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

## Shared object-store outage experiments

Both runners accept `--scenario s3-outage --fault-profile
tools/perf/store_fault_profile.json`. The checked-in schedule pauses the exact
owned RustFS process for one second every five seconds, beginning five seconds
into measurement and leaving five seconds at the end. The complete pause and
control overhead must fit the unchanged two-second maximum. A missed timing
bound fails verification. This models a temporarily frozen backend, not a crash,
packet-loss partition or Amazon S3 outage.

```sh
python3 tools/perf/raft_run.py --image "$image" \
  --profile tools/perf/raft_profiles.json --profile-name local-q0-s3 \
  --scenario s3-outage --fault-profile tools/perf/store_fault_profile.json \
  --output /absolute/new/raft-store-outage
# Stop the preceding report's exact containers before the next experiment.
python3 tools/perf/baseline_run.py --mode bucket --image "$image" \
  --profile tools/perf/raft_profiles.json --profile-name local-q0-s3 \
  --scenario s3-outage --fault-profile tools/perf/store_fault_profile.json \
  --output /absolute/new/bucket-store-outage
```

Use the same fault profile with `--scenario healthy` for a control with the same
inspection schedule. Each fresh run still requires its independent analyzer.
The Linux freezer preflight precedes workload resource creation. The fault
owner binds the container's unpredictable name, run label and immutable ID;
lost pause or resume replies trigger inspection, and cancellation joins cleanup.
No cell node is paused in this scenario. Raw `faults.jsonl` retains every window,
including incomplete or failed attempts.

Every window must contain a fresh successful command dispatched after the
observed hold and before release starts. `sent_seconds` records the instant after
the invocation journal sync and immediately before calling the HTTP adapter. Scheduled
arrivals and time spent writing the journal cannot supply a witness. For S3 and
bucket acknowledgement policies, no such command may complete while RustFS is
observed paused. Quorum and Fleet may acknowledge within that window under their
different output contracts; the checker reports those observations separately.
It never infers a per-command Fleet proof from aggregate counters.

Raw histories must still reconcile. In `s3-outage` and healthy controls, an
ambiguous mutation remains quarantined and fails qualification. Raft requires
all assigned replica reads and final archive coverage;
the baseline requires matching owner-route state and sequence through all three
ingresses. Their final observation bounds recovery from the last release within
the original recovery SLO. This is state convergence, not recovery from lost
disks. Existing proof-error, resource and traffic gates remain unchanged.

The shared injection does not make the architectures' authority, retained-copy,
read or receipt guarantees equal. `compare_runs.py` includes the fault profile
for every adapter and continues to refuse a throughput ratio when any required
contract differs. The runner implementation is tested with local Python
fixtures. The
[source-pinned harness evidence](../../plans/multi-raft/evidence/MR-14-store-fault-progress.json)
retains those test results and explicitly records zero physical runs in that
implementation component. The subsequent physical matrix is recorded separately.

### Physical store-outage matrix

The [eight physical runs](../../plans/multi-raft/evidence/MR-14-store-physical-progress.json)
at source `1391e94` use one fresh image, three 1-vCPU/3-GiB/no-swap Cellar nodes,
RustFS, 12 active cells, five seconds of warmup and 20 seconds at 10 offered
operations per second. Each outage has two one-second store pauses within the
unchanged two-second maximum; healthy controls use the same inspection schedule.
All submitted histories reconcile, with every assigned Raft replica and final
archive cut checked, or all three baseline owner routes checked.

| Policy | Healthy completed / offered | Healthy p99 | Outage completed / offered | Outage p99 | New commands acknowledged / submitted during observed pauses |
| --- | --- | --- | --- | --- | --- |
| Raft Quorum | 200 / 200 | 29 ms | 200 / 200 | 41 ms | 10 / 10 |
| Raft S3 | 198 / 200 | 126 ms | 193 / 200 | 942 ms | 0 / 6 |
| Fleet | 200 / 200 | 67 ms | 196 / 200 | 754 ms | 6 / 6 |
| Bucket | 200 / 200 | 25 ms | 193 / 200 | 945 ms | 0 / 6 |

All healthy controls and the Quorum outage meet the original short-run traffic
targets. S3, Fleet and bucket outages miss them; their successful safety and
state checks do not override those misses. Missing completions are generator
rejections, with no server refusal, timeout or ambiguous mutation observed.
Final state checks provide recovery upper bounds of 9.03–9.40 seconds from the
last release start; those bounds include the rest of the measurement window.
They are not precise per-cell recovery times.

Fleet's 97 healthy and 93 outage output-proof completions all use its replication
path, with zero bucket proofs and zero proof errors. Its two long outage queries
overlap the first pause and account for its latency miss. Their request times
are about 751 and 842 ms, while invocation journal sync is below 1 ms. This
locates the delay after journal sync; it does not identify the causal server
operation. Per-command Fleet proof sources are not inferred from counters.

All eight independent analyzers pass again after extracting the hashed evidence
capsule. Both S3-versus-bucket comparisons refuse an equivalent-contract ratio.
All 32 experiment containers stop with exit code zero and no OOM. Their exact
empty networks are removed, volumes and containers retained, and the dedicated
profile is stopped. Unrelated profiles remain untouched.

These are short functional observations on a shared Mac, with swap use and
other active Colima profiles recorded in each run. They do not establish a
causal speedup, sustained capacity, independent failure domains or a common
cross-adapter service contract. The following separate drill covers selected
ambiguous outcomes; broader process loss, disk loss, real S3 and Q1 remain open.

### Recovering ambiguous commands after measurement

The separate `s3-recovery` scenario supports Raft S3 and bucket acknowledgements.
Use the same workload profile and `--fault-profile
tools/perf/store_recovery_profile.json`. It pauses RustFS for seven seconds,
starting five seconds into measurement, with a nine-second outer pause bound.
This exceeds the native five-second and HTTP six-second request budgets. The
scenario must observe an ambiguous command dispatched during the confirmed pause;
a run without one cannot qualify this recovery path.

The measured operation ledger remains immutable. A command with an unknown
outcome quarantines its cell for the rest of measurement. Once measurement ends
and the store release is observed, the runner submits the original command ID,
input and retry deadline once, then sends one identical confirming request.
Both responses must succeed with the same receipt and result, satisfy the
original durability contract and finish within the original recovery bound.
Raft observation tokens are renewed by the SDK on each response; their issue and
expiry times are excluded from receipt equality. Result, position, term, scope,
durability and every other returned field must remain identical. Both tokens
remain in the raw ledger, and final at-least reads authenticate the first token
through the SDK. The analyzer does not treat parsing a token as authentication.
The retry deadline is never renewed. Expiry, failed confirmation, changed scope,
or an inconsistent result stops recovery and retains the partial ledger.
There is no blind retry loop. The first retry may observe an earlier commit or
execute a command that was previously absent; the lost response cannot establish
which happened.

`recovery.jsonl` records both requests and their original identities separately.
The independent analyzer checks that evidence before appending logical resolutions
to its state model. It still requires every replica/archive or owner-route check.
Timeouts, ambiguous outcomes, quarantined-cell rejections and their latency remain
in the original performance summary. Recovery traffic contributes no measured
completion or throughput. Bucket output-barrier errors during this explicit
failure phase remain visible in the report; recovery-phase errors must be zero.
Healthy and short-outage proof-error requirements remain unchanged. These
serialized receipt checks do not replace MR-15's concurrent history checker.

The [physical recovery evidence](../../plans/multi-raft/evidence/MR-14-recovery-progress.json)
retains four attempts with the original three-node, 12-cell workload and SLOs:

| Source | Policy / attempt | Recovery result | Measurement completed / offered |
| --- | --- | --- | --- |
| `f3bf129` | Raft S3 / first | Rejected: harness compared renewed token bytes | 68 / 200 |
| `f3bf129` | Bucket / first | Four original commands recovered and confirmed | 61 / 200 |
| `1490457` | Raft S3 / repeat | Four original commands recovered and confirmed | 68 / 200 |
| `1490457` | Bucket / repeat | Rejected: node 1 fenced expired ownership | 66 / 200 |

The first Raft attempt compared whole responses even though the SDK issues a new
observation token on each response. Its two responses retain identical durable
result, position, term, scope and durability; only token issue/expiry times and
signature differ. A regression now distinguishes token renewal from changed
receipt fields. That incomplete attempt remains rejected; it retried only the
first of four unknown commands before stopping.

The completed Raft and bucket runs verify all 36 replica or owner-route reads.
Raft also verifies final archive coverage. No fresh observed write acknowledges
during the held store pause. Final state checks bound recovery at 8.41 and 7.75
seconds after release starts, including the remaining measurement time. Both
completed runs miss the original traffic targets; their four ambiguous command
outcomes and generator rejections remain in measurement. Bucket also records
one failed query. Recovery requests are excluded from those totals.

The bucket repeat records an ambiguous lease renewal, then a native
`NodeLeaseExpired` fence. Node 1 exits with code 1 and no OOM; the required
post-measurement node snapshot fails before recovery starts. The ten-second
lease and seven-second pause can overlap such that authority expires around
release. Approximate cross-clock alignment does not prove millisecond ordering.
The checker preserves this failure and refuses comparison with the incomplete
run. The separate all-owner restart drill below does not qualify this mixed-owner
failure; neither a longer lease nor repeated attempts until one passes establishes
that behavior.

Both accepted runs reanalyze after capsule extraction; both rejected runs stay
rejected. All 16 containers are stopped, all four exact empty networks removed,
and volumes and raw artifacts retained. The dedicated profile is stopped.
These observations do not establish repeatability, steady state, causal speedup,
independent failure domains or a common cross-adapter contract.

### Restart after bucket owners fence

The bucket adapter also accepts `--scenario s3-fenced-recovery --fault-profile
tools/perf/store_fenced_recovery_profile.json` with `local-q0-s3`. This distinct
drill pauses RustFS for twelve seconds, beyond the unchanged ten-second node
lease. It requires all three native owners to report `NodeLeaseExpired`, join
their owned work and exit before issuing one start operation against their
original immutable container IDs. Missing fences, OOM, changed images or data
volumes, reused lease generations and recovery beyond the original thirty-second
bound refuse qualification. A lost start response is inspected, never replayed.

Every cell is read through all three ingresses before any ambiguous mutation is
retried. Each result must contain the complete acknowledged prefix, with either
the original unknown command present or absent. Missing acknowledged state
cannot be repaired by replay. Original identities and deadlines then pass the
same recovery/confirmation checks. One new sequence per cell must subsequently
commit, followed by the final owner-route checks. `restart.jsonl`,
`before-recovery.jsonl` and `resumed.jsonl` retain these phases separately from
the immutable measured workload.

This exercises restart on the same persistent volumes; it does not establish
empty-disk or disaster restore. The old processes' final counters are explicitly
unavailable. Recovery and resumed-write counters belong to the new processes,
and are never subtracted from old-process samples. `measurement_proofs_complete`
and `traffic_objectives_met` remain false; `compare_runs.py` refuses performance
ratios for these drills even when all recovery checks pass. Fresh images include
the native public lease generation in startup evidence.

The [physical restart evidence](../../plans/multi-raft/evidence/MR-14-restart-progress.json)
pins two fresh runs to source `aaa83f8` and one image. Each run proves all three
fences, fresh lease generations, 36 pre-retry owner reads, eight original-identity
recoveries with confirmation, twelve new writes and 36 final owner reads. All
eight unknown commands are absent at the pre-retry audit; acknowledged state is
intact. Final recovery bounds are 8.22 and 7.69 seconds. New-process counters
record eight recovery and twelve resumed bucket proofs with no proof errors.

Each measured workload still records only 32 of 200 completions, eight ambiguous
commands and ten failed queries. None of the eight fresh held-window writes
acknowledges during the pause. The independent analyzers pass after extraction,
and comparison refuses both runs. All eight containers stop cleanly; their exact
empty networks are removed, volumes retained and the dedicated profile stopped.
The earlier mixed-owner failure remains rejected, and cold/disaster restore is
still unqualified.

## Comparison rules

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

Archive admission also reports fixed `archive_admission_*` counters: granted,
observation-refused, retained-bytes-refused, retained-entries-refused,
reserved-bytes-deferred and reserved-entries-deferred (names use underscores).
A retained-limit check includes the next command's worst-case reservation;
reserved-limit checks additionally include other admitted commands. One decision
records the first encountered reason, checking bytes before entries. Counts
include waiter rechecks and duplicate requests, so they are not HTTP error counts.
Compare their deltas with failed staging operations and archive lag; missing
counters in an older frozen build are unavailable, not zero. These observations
add no capacity and change no admission decision.

Control/data timing now separates sender queue residence, sender exchange windows
and receiver runtime admission. Use their phase deltas alongside queue depth and
CPU pressure to distinguish queue buildup from slow exchange or admission. An
exchange includes connection/TLS setup when needed and can contain eight control
frames; it is not a per-frame latency or a durable acknowledgement. The analyzer
requires all six new operations. Older evidence uses its pinned analyzer;
missing observations are never filled with zeros. These timings provide a
diagnostic boundary, not proof of the existing density slowdown's cause or fix.
The [timing validation](../../plans/multi-raft/evidence/MR-14-transport-timing-progress.json)
at `87ed5d6` includes 96 Raft unit cases, 20 native reads/replication cases and
20 native operations cases. Real TLS tests separate queued frames from held
exchanges and retain failure/cancellation outcomes. The operations fixture
verifies 551 fixed metric identities for both one and 32 groups. All 192 Python
cases pass with one existing opt-in Docker skip; Clippy and repository checks
pass. No physical workload or instrumentation-overhead result is added here.

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

Bounded node and object-store process-pause experiments have explicit injection
and reconciliation paths. Other fault scenarios and soak are refused before resource creation until
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
