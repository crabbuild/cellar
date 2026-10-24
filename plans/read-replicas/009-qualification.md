# Qualification 009: Read-replica correctness and release evidence

Status: required future qualification, not a passed test report.
Baseline: `0efd8f4d871199542f97f9fe0b74086a38606d02`, 2026-10-19.
Scope: primary A, durability ensemble D/E, read replicas B/C; independently
placed serving copies with unchanged existing write/recovery semantics.

Read the [technical design](007-technical-design.md) for I1–I10 and the
[implementation plan](008-implementation-plan.md) for RR-01–RR-12. All new
targets and the five-node runner in this document must be implemented first.

## 1. Correctness oracle

Use a fixture with a transactionally updated per-cell sequence, a business
command-ID/digest receipt and a deterministic business-state hash. Record its
catalog operation ID separately; it identifies the registered handler, not one
business invocation. Append enough retained
receipts to verify the whole experiment; do not let the current 4,096-receipt
fixture limit silently prune the correctness ledger during a soak.

Fsync a client operation plan before submission and outcomes after complete
responses. Record acknowledged, definitely refused and ambiguous operations
separately. Never replay ambiguous writes before saving the recovered pre-replay
state. Match command IDs/digests against recovered receipts; duplicate replay
must not apply another business effect. An ambiguous operation may have committed.
It is wrong to require recovered state to equal only the acknowledged count.

Each query result must record:

- Cell/query/operation ID and request mode; supplied token if any (redact secrets).
- Serving node/process and role; source epoch/activation; application digest.
- Exact `served_at` history/cut/checksum and source-observation status.
- Business value/hash from that pinned snapshot; fallback flag and route attempts.
- Request start/end, deadline, outcome and typed refusal for failed requests.

Keep an independent publication ledger: safe cuts accepted by the core, proof
source, immutable artifacts and epoch bridges. The audit must demonstrate that
every successful read matches a coherent proven history and that every AtLeast
read covers its requested token. Never accept a self-reported TXID alone as the
oracle. Sampled payload inspection is insufficient for a no-unproven-read claim.

For test-controlled fixtures, write a recognizable sentinel in an unproven
capture, pause/fail proof and attempt every query path. No response may expose
that sentinel until a later valid proof covers it. Test this across a later
captured file, a failed pipeline tail and a source epoch replacement.

## 2. Runnable integration targets

The following targets are proposed, ordinary Cargo integration tests. Existing
`cellar-logic`/`cellar-ltx` library test disabling must not hide their coverage.

| ID | Scenario and assertion | Owning proposed target | Invariants |
| --- | --- | --- | --- |
| T01 | Two racing captures map a waiter to exactly its completed receipt; never sample newer unproven state | `native_read_replica_proof` | I2, I9 |
| T02 | Bucket completion waits for ownership verification; rejected/fenced/stale gate has no publication | `read_replica_decisions`, `native_read_replica_proof` | I1, I2, I5 |
| T03 | Failed ordered fleet round prevents later queued credit/publication; bucket/fleet receipts cannot mix | `native_read_replica_proof` | I2, I3 |
| T04 | Real LTX apply equals exact-cut restore across page growth, truncation and checksum changes | `read_replica_apply` | I2, I6 |
| T05 | Gap, corrupted file, wrong page size/source/epoch and target-crossing segment cannot publish | `read_replica_apply`, transport | I2, I5 |
| T06 | Reader pinned to G sees G throughout publication of G+1; crash points yield coherent old/new image | `native_read_replica_materializer` plus `read_replica_apply` mechanics | I2, I6 |
| T07 | Many commands/queries/classes/modules retain distinct definitions, operation types and policies; contract kinds are validated; ingress and typed calls share dispatch; query registration never constructs normal cells; writes/effects/system-table access denied | `native_read_replica`, core decisions | I1, I2 |
| T08 | Validated token is satisfied or refused; no-change/idempotent/gate-follower version replies have exact receipts; multi-cell orchestration retains separate tokens; expiry/tamper/wrong cell/unknown ancestry never downgrade | `native_read_replica`, proof, core decisions | I4, I5 |
| T09 | Session duplicate/reorder/gap/expiry has bounded cost; slow consumer reseeds without pinning log GC; unauthenticated/replayed manifest/query/status responses and mismatched chunks are refused | `native_read_replica_transport` | I2, I5, I8 |
| T10 | Behind B catches up through recovered acknowledged history before promotion | `native_read_replica_failover` | I1, I7 |
| T11 | Prior-epoch and same-epoch-resume tokens require verified multihop ancestry; compaction does not invent coverage; old A cannot advance new source | `native_read_replica_failover`, core decisions | I4, I5, I7 |
| T12 | Lost complete witness evidence blocks takeover despite available B/C images | `native_read_replica_failover`, existing `native_log`/`native_node` | I3, I7 |
| T13 | Query, fallback, disk, frame, transfer and pin limits hold under saturation | Native replica/transport targets | I8 |
| T14 | Cancel, deployment retirement, disable and shutdown release handles and join owned work | Native replica/transport/failover targets | I5, I8 |
| T15 | Feature-off and ordinary fetch/RPC retain current behavior and mutation retry rules | Native replica plus existing node/counter/daemon targets | I1, I10 |
| T16 | Read traffic on a shared witness node cannot starve reserved proof execution; measured limit is reported | Native replica plus five-node load | I3, I8 |
| T17 | Policy selects/replaces nodes automatically; no-traffic repair and safe primary recovery, capacity reservations, constrained placement, restart/rejoin, stale plans and repair-storm budgets hold | `replica_placement`, `native_read_replica_repair` | I1, I3, I5, I7, I8 |

Run the exact commands in each RR package. List new tests before release with
`cargo test -p PACKAGE --test TARGET --locked -- --list`, record named cases and
nonzero counts, then run the target. A green exit with all tests filtered out is
not a pass. Keep process/fleet cases serial as baseline CI requires.

## 3. Reference topology and fixture controls

Allocate five independently addressed processes with separate local storage:

| Node | Initial role for cell 1 | May have unrelated roles? |
| --- | --- | --- |
| A | Primary, cell epoch E | Yes, recorded and bounded |
| D, E | Actual installed durability ensemble for A's active log generation | Yes, but no hidden substitution for B/C |
| B, C | Explicit read replicas of cell 1 | No durability membership for A in the disjoint scenario |

Use the current source's existing owner-placement mechanisms plus scoped fixture
controls to produce this topology. Verify the actual installed ensemble and
read source after warmup and each fault. Do not label a run disjoint based only
on desired configuration. A separate overlap experiment may put serving and
witness roles on the same server to measure residual hardware contention.

Normal placement is automatic. Fixture policies constrain eligible pools and
select a reproducible cell/ranking seed to establish B/C initially; they do not
replace the scheduler with manual reassignment. Q13–Q16 add a sixth running node
F with compatible code/capacity in the eligible read/recovery pool. D/E remain
outside the read pool in the disjoint repair case. A fixed five-node fleet cannot
replace B while preserving that separation, because it has no spare destination.

A deterministic fixture must control:

- Primary epoch/activation and actual ensemble identity observations.
- Pausing bucket upload and follower proof independently.
- Pausing B/C receive, apply and publication independently.
- Retaining source/session proof evidence and disconnecting selected peer edges.
- Crashing without graceful seal/drain, restarting with and without local cache.
- Dropping a command reply after submission to create a genuine ambiguous result.

Existing `tools/perf/run.py`, `faults.py`, and `peer_partition.py` assume three
nodes in important paths. RR-11 must add a genuinely configurable topology and
tested edge-graph partition installation. Probe both directions of every blocked
edge and intended surviving paths. Record bucket reachability separately; a peer
partition is not automatically an object-store outage.

One shared VM/disk/store can establish process/protocol evidence only. It cannot
qualify multi-zone loss tolerance. Record actual failure domains with every run.

## 4. Five-node scenarios

| ID | Fault/workload | Required evidence and result |
| --- | --- | --- |
| Q1 | Healthy disjoint topology; concurrent writes and stale/minimum-version queries | Actual D/E proof and B/C serving observed; every query matches proven history; tokens satisfied |
| Q2 | Pause B, then C apply while A continues writing | Write proofs continue independently; lagging reads satisfy mode or refuse; budgets remain bounded |
| Q3 | Lose one durability follower with A alive | Actual bucket fallback or valid existing recovery behavior observed; no B/C progress counted as proof |
| Q4 | Kill A after acknowledged 100 while B=98, C=99, bucket=95 and D/E hold tail | Recover complete history, acquire new epoch, restore to at least 100, then admit writes; old tokens resolve through bridge |
| Q5 | Lose A and all complete required witnesses before bucket coverage | No successor write admission; explicit missing-proof refusal despite readable B/C |
| Q6 | Partition {B,C} from {A,D,E}, preserving stated bucket paths | No independent B/C promotion; stale reads only under explicit disconnected policy; unsatisfied tokens refuse |
| Q7 | Delay old A and return it after successor starts | Old writes/read publication/session replies rejected; no two writers or epoch regression |
| Q8 | Corrupt a frame, omit a tail range, expire pins and race source GC | Old coherent snapshot remains usable according to policy; bounded retry/reseed; no leaked pin or false success |
| Q9 | Crash B at every manifest/file/CURRENT publication boundary | Restart chooses a verified coherent image or reseeds; partial builder never served |
| Q10 | Saturate queries, pin old generations and exhaust read disk/admission; test role overlap separately | Typed bounded refusals; no unbounded growth; write proof progress respects declared resource envelope |
| Q11 | Roll deployment digest; disable feature; cancel queries and stop nodes | No incompatible query admission, no mutable fallback replay; all required stop/join/pin proofs returned |
| Q12 | Independent-arrival mixed workload and soak, with matched owner-only baseline | Correctness ledger remains clean; latency/resource thresholds met or explicit performance rejection |
| Q13 | Kill B with target two replicas and spare F, without changing configuration or sending repair-triggering application traffic | Supervisor detects loss, reserves F, bootstraps/catches up and restores two ready copies; write proof unaffected |
| Q14 | Kill A with recoverable history, no new application requests, B/C plus F eligible | Proactive fenced recovery produces one ready primary, preserves acknowledged writes and restores two additional read copies |
| Q15 | Restart supervisors, flap/rejoin nodes, change policy, exhaust/restore capacity, and lose enrollment notices after Release/Sticky idle eviction | No-traffic inventory finds and repairs idle existing state; Quiescent status is validated; no stale-plan readiness or activation churn; UnderReplicated clears automatically when feasible |
| Q16 | Fail many replicas/control paths simultaneously, including a missing-witness subcase | Per-node/source repair limits hold, retry storms are bounded, required domains are honored and missing evidence never permits writer admission |

Q4 and Q5 must deliberately establish the specified fleet-proof window. If
uploads advance bucket coverage before the crash, the run does not exercise that
case and must be rerun. Count and inspect actual proof-source observations.
Missing exact observations mark the scenario incomplete, not passed by inference.

Q5 is a refusal test, not an instruction to discard real fleet data. Use isolated
test prefixes and disposable node disks. Restore blocked witnesses for cleanup
or record incomplete lifecycle honestly; never weaken recovery to finish a run.

### Scenario and proof-posture matrix

| Scenario | Fleet posture | Bucket-only posture |
| --- | --- | --- |
| Q1, Q2, Q6–Q12 | Required, with actual fleet acknowledgments recorded | Required where the scenario does not concern fleet-only evidence; record bucket proof |
| Q3 | Required follower-loss/bucket-fallback case | Control: D/E are outside the write path, so their failure must not change bucket proof |
| Q4 | Required exact setup: acknowledged 100, bucket 95, B 98, C 99, valid witness tail | Separate recovery control: bucket must already cover every acknowledged write before A is killed; restore all of them |
| Q5 | Required missing-complete-witness refusal before bucket covers acknowledged tail | Separate unavailable-history control: make required bucket history unavailable, require no admission until recovery is possible; do not claim a fleet-tail loss window |
| Q13–Q15 | Required with actual fleet proof and automatic policy unchanged across node failure | Required with bucket proof and safe existing recovery |
| Q16 | Required, including absent-complete-witness refusal | Required for repair storms/store unavailability; missing bucket evidence blocks recovery |

The runner must name the two bucket controls distinctly in evidence while keeping
their parent Q ID. An unavailable bucket is not proof of permanent data loss.
Restore access and verify recovery. In pure bucket posture a successful write
cannot legitimately be ahead of its required bucket proof; a fixture that creates
that condition is invalid rather than a useful crash experiment.

## 5. Future harness commands and audit contract

These commands become available only after RR-11 creates the runner, topology
fixture and application support. They are not existing Cellar CLI features.

```sh
python3 tools/perf/read_replicas.py --help
python3 tools/perf/read_replicas.py run \
  --image cellar-embedded-perf:read-replicas-candidate \
  --topology tools/perf/read_replica_topology.json \
  --scenario disjoint --arrival-mode independent \
  --output /absolute/new/path/read-replicas-run-001
python3 tools/perf/read_replicas.py audit \
  --output /absolute/new/path/read-replicas-run-001
```

`--help` must document prerequisite VM/resources and reject unsupported commands.
`run` accepts Q-scenario selection in the topology/run policy, validates a new
output directory and refuses production store prefixes or reused live identities.
Topology JSON includes five baseline nodes and the sixth spare for Q13–Q16,
desired roles, automatic placement policy and explicit budgets,
source-observation/token retention, arrival schedule, fault checkpoints, required
proof postures and predeclared acceptance thresholds. Never write credentials
into the topology/report. Document fixture controls and cleanup ownership.

`run` and `audit` both return nonzero for correctness failure. Incomplete evidence
also returns nonzero and produces `completed: false`. `audit` reprocesses saved
ledgers independently; it cannot trust the run's final success flag. Expected
summary fields:

```json
{
  "schema": 1,
  "source_revision": "<candidate SHA>",
  "executable_sha256": "<actual executable digest>",
  "topology_verified": true,
  "proof_postures_exercised": ["fleet", "bucket"],
  "completed": true,
  "correctness": {
    "acknowledged_writes_missing": 0,
    "unproven_read_results": 0,
    "minimum_version_violations": 0,
    "unauthorized_promotions": 0,
    "duplicate_business_effects": 0
  },
  "shutdown_complete_for_required_stops": true,
  "automatic_repair_verified": true,
  "scenarios": ["Q1", "Q2", "Q3", "Q4", "Q5", "Q6", "Q7", "Q8", "Q9", "Q10", "Q11", "Q12", "Q13", "Q14", "Q15", "Q16"]
}
```

Include per-scenario outcomes and exact missing evidence, not just these
aggregate counters. Q5's expected refusal is a correctness pass even though it
does not complete a normal successor startup. Report performance acceptance as
a separate field; protocol correctness alone does not establish a speedup.

## 6. Performance and availability acceptance

Declare numeric thresholds before running. Required policy fields are:

- Offered arrivals/s, read/write mix, hot-cell skew, query cost and database sizes.
- Query and write latency p95/p99 limits, timeout/rejection ceilings and target
  duration; actual warmup, repetitions and percentile method.
- Allowed write p99 regression versus matched owner-only baseline.
- CPU, RSS, disk, retained pins, transfer bytes, apply/publication lag and headroom.
- Required replica recovery and writer takeover readiness time.
- Automatic failure detection, inventory revisit, repair-queue and catch-up
  limits, desired/ready count recovery time and allowed movement/churn.
- Token-history retention and behavior when evidence expires.
- Fault posture, store durability assumptions and real failure domains.

Use at least three matched steady-state repetitions and a 60-minute initial soak
as a minimum experimental procedure, then extend to the intended deployment
envelope. These durations do not by themselves prove production readiness.
Reject qualification if the threshold fields are unset. Do not fabricate
universal milliseconds, RPO or throughput promises from counter fixtures.

Independent arrivals must record offered and completed work separately so
queueing does not disappear when the generator slows. Record fallback traffic;
otherwise a supposed replica speedup can actually be primary execution.
Compare identical builds, query results, proof postures and resource allocations.
Any additional read-node resources must be disclosed in cost comparisons.

## 7. Release ledger

For every candidate record code SHA, actual executable/image hashes, toolchain,
dependency lockfile, complete policy/topology, actual roles, tests/counts,
scenario raw artifacts, command/read/token ledgers, fault timestamps and verified
connectivity, proof sources, resource series, independent audit, stop reports,
failures, measured limits and unqualified cases.

Release requires all safety tests, complete expected fault evidence, supported
posture recovery, honest refusal outside that posture, bounded resources,
passing existing CI, automatic repair without operator intervention/application
traffic, and predeclared performance/availability thresholds. A
failed strong-read request must remain `UnsupportedConsistency`; stale success
does not qualify strong reads. Keep feature off by default and enable only a
bounded operator-selected canary after these gates pass.
