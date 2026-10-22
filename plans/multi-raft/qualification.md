# Cellar multi-Raft qualification plan

The release criterion is preservation of acknowledged state and advertised read
contracts through the declared failures, followed by measured performance. A fast
counter with no fault injection is insufficient. All first-release tasks MR-01
through MR-15 must be DONE with evidence before enabling production admission.

## Test ownership and execution

[tasks.json](tasks.json) contains exact proposed Rust test names, their owning
integration targets, expected observations and invariant IDs. Each target must
exist and expose every listed test through Cargo's `--list`; filters returning
zero tests do not qualify a task. Unit/model tests may supplement those public
contract tests but cannot replace process-crash and filesystem tests.

Use a checkout-specific Cargo target on the workspace volume, following
`AGENTS.md`. Run independent fleet fixtures serially. Do not enable
`cellar_internal_tests` without its separately supplied harness. MR-01 creates
ordinary checked-in fault-injection/test support so this package is executable
without that unavailable harness.

```sh
export CARGO_TARGET_DIR="$HOME/Workspace/crabbuild-target/cellar-multi-raft-design"
python3 plans/multi-raft/plan.py validate
python3 plans/multi-raft/plan.py show MR-01
# After its target is implemented:
cargo test -p cellar --test native_raft_contract --locked -- --list
cargo test -p cellar --test native_raft_contract --locked -- --test-threads=1
```

Before release, run repository checks as well as all new targets:

```sh
cargo format --check
cargo check-layout
python3 tools/check_native_dependencies.py
python3 tools/docs/check.py
cargo check --workspace --all-targets --all-features --locked
cargo test --workspace --all-features --lib --tests --locked -- --test-threads=1
cargo test --workspace --all-features --doc --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
python3 -m unittest discover -s tools/perf -p 'test_*.py'
```

## Correctness oracle

The client writes a durable operation ledger before submission and after response.
Record cell/incarnation, command identity and digest, invocation/completion order,
requested/achieved durability, response/result/version, and failure injection.
Use per-client monotonic event order plus harness synchronization for happens-before
constraints; do not infer real-time order from unsynchronized machine clocks.

Use a reference state machine per cell with noncommutative operations (conditional
updates, transfers between fields in the same cell and receipt/result checks).
Counts alone cannot detect reordered or wrong-result histories. Record query
invocation/completion and the exact served version. Build an offline history checker
in MR-15 that verifies linearizable-mode histories against the reference model.

For every acknowledged command, recovered state contains the mutation and matching
receipt exactly once within the promised retry window. For ambiguous commands,
both absent and present are allowed, but all replicas agree on the resulting
history and retries cannot duplicate the effect. Stale reads must correspond to
a real committed prefix; AtLeast reads must cover the supplied token; linearizable
reads must satisfy real-time constraints. Partitioned minority writes and fresh
linearizable reads must fail or wait. No mode silently changes its contract.

For S3-durable acknowledgements, erase all test replica disks and restore into a
new isolated incarnation from the archive. Every S3-acknowledged effect and its
immutable dependencies must survive. For Quorum mode, complete simultaneous disk
loss beyond the declared loss model may lose the unarchived tail; the report must
measure and disclose that tail rather than treat it as a safety pass for S3 mode.

## Failure matrix

Run every relevant cut with restart, leader change, and delayed duplicate
completion where applicable. Repeat with configuration changes and snapshots
overlapping application traffic.

| Injection | Required observation |
| --- | --- |
| Crash after private SQLite commit, before proposal | No query/effect sees it; working image rebuilt |
| Crash after append, before fsync | No dependent successful response; recovery uses durable prefix |
| Follower disk stall or full disk | No early acknowledgement; healthy majority may progress |
| Crash after quorum commit, before result | Retry returns committed receipt once |
| Crash after image sync, before apply marker | Orphan image ignored; safe replay |
| Crash after apply marker, before response | Published image and receipt recover together |
| Leader isolated from majority | Minority cannot commit; majority elects and serves |
| One-way partition, reordered/duplicate messages | No divergent committed history or unsafe query |
| Crash during snapshot download/install | Old or new coherent state selected; no mixed metadata |
| Remove/readd with stale member or disk identity | Old votes/messages rejected; new member joins through quorum |
| Crash in joint membership transition | Both-majority rules retained through restart |
| S3 unavailable or ambiguous upload/CAS | No fabricated archive cut; bounded backlog and correct durability mode |
| Corrupt LTX, snapshot, log or archive metadata | Quarantine/refusal; no index skipping or silent repair |
| Disk loss on one voter | New learner identity; catch-up before promotion |
| Loss of majority | No automatic bucket fallback or force-new quorum |
| Leader loss during alarm/outbox delivery | Committed intent recovered; receiver receipt prevents duplicate effects |
| Long query pins old generation | Stable view, bounded disk pressure and controlled refusal |
| Shutdown with pending proposal and delayed fsync | Complete task joins and truthful unresolved-work report |
| Incompatible query/schema deployment | Replica may remain voter but refuses incompatible serving |
| Old cluster returns after disaster restore | Infrastructure fencing prevents two writable incarnations |

## Harness delivery

MR-14 now provides `tools/perf/raft_run.py`, the native `raft_workload.rs`
reference application in `cellar-perf-raft`, and `raft_analyze.py` for independent
serialized-history and replica reconciliation. Controlled slow-follower runs add
bounded process pauses, observed majority progress during third-voter lag and
exact final replica recovery. The target's concurrent leader/query-follower roles
are reported separately. Both Raft and Fleet/bucket runners also implement the
shared `s3-outage` RustFS process-pause schedule and healthy controls. Independent
checks require fresh submitted command witnesses, preserve each acknowledgement
policy, refuse unresolved histories and bound final state recovery. The
[eight-run physical matrix](evidence/MR-14-store-physical-progress.json) reconciles
all four policies under bounded pauses, while retaining three outage traffic
misses. It does not make cross-adapter contracts equivalent or qualify longer
failures with ambiguous outcomes. A separate `s3-recovery` scenario now retains
timed-out commands in measurement and resolves their original identities after
measurement, requiring two matching receipts and the unchanged recovery bound.
The [local recovery drills](evidence/MR-14-recovery-progress.json) recover four
ambiguous commands in one Raft S3 run and one bucket run, while retaining a
rejected Raft harness attempt and a bucket repeat that fences an expired owner.
Both completed runs miss traffic targets. Fenced-owner restart/restore and
repeatable long-outage recovery remain open. The runners refuse other unimplemented fault scenarios
and soak before creating workload resources.
`tools/perf/raft-performance.md` gives its current executable interface and
qualification limits. MR-15's concurrent `raft_history.py` checker and complete
fault/result qualification remain future outputs.

Required runner interface:

```text
python3 tools/perf/raft_run.py
  --image <verified-image-digest>
  --profile <checked-in-workload.json>
  --scenario <healthy|slow-follower|leader-loss|minority-partition|disk-stall|s3-outage|s3-recovery|snapshot-crash>
  --fault-profile <checked-in-fault.json>  # required for slow-follower/s3-outage/s3-recovery; optional healthy control
  --output <new-run-directory>
```

The runner must refuse production bucket/credentials, stale executable digests,
missing fault capabilities, missing workload fields and existing output directories.
Own only its tagged disposable containers, network, bucket and disks. Cleanup
retains failed-run evidence and never deletes another run's resources.

A workload specifies node/domain topology; total and active cells; database-size
and write-size distributions; read/write ratio and consistency modes; skew;
open-loop offered rate; warmup, measurement and soak duration; failure timing;
durability; and resource limits. A benchmark waits for steady state and reports
offered, admitted, completed, rejected and timed-out operations separately.

Compare the same build mode/hardware/workload against the existing all-follower
fleet path and bucket path. Include healthy and slow-follower cases. A reduced
latency result with weaker durability or fewer retained copies must be labeled
as a contract change, not an equivalent speedup.

## Measurements and gates

Measure command/query p50/p95/p99/p99.9, throughput, error rate, commit/apply/query
lag, fsync latency, logical/LTX/network/disk/S3 bytes, CPU, memory, open files,
group count, controller work, archive backlog, and time to business readiness
after failure. Distinguish leader election from successful state recovery and
from restored redundancy. Report each cell's recovery tail, not only fleet means.

MR-14 must establish a versioned SLO profile for each intended deployment before
MR-15 can pass. It names numeric latency/error/throughput objectives, maximum
recovery time, maximum archive lag, supported failure domains and resource headroom.
No universal production numbers are invented in this design. Missing numeric
targets block performance qualification but do not block implementing correctness.

Hard safety gates are universal: zero acknowledged-state loss within the declared
loss model; zero speculative read exposure; zero wrong-incarnation admission;
zero duplicate business effects within the receipt contract; zero false successful
shutdown proofs; and no unbounded resource growth under a bounded workload.

Q0 and Q1 in [scaling.md](scaling.md) are required for the initial release, including
a 24-hour mixed-workload soak with scheduled failures. Q2 through Q4 require new
physical-infrastructure evidence before making corresponding scale claims.

## Evidence and release record

Each task evidence record names source SHA, dirty patch digest if applicable,
executable/image digests, toolchain, dependency lockfile digest, exact command
argument vectors, listed/passed/failed/ignored tests, raw output paths, host/store
versions and limitations. Keep logs immutable under `plans/multi-raft/evidence/`
or a durable linked evidence location; do not include credentials.

DONE records must cover every manifest case and include each relevant negative
case, not just a green Cargo exit. MR-15 additionally records the SLO profile,
fault matrix, client ledger reconciliation, exact supported topology, rollback
drill, residual limitations and a release decision. A design/checker test run
cannot be entered as runtime evidence.

Enable a new isolated namespace on a small canary only after those gates. Expand
traffic within measured capacity and check errors, apply/archive lag and repair
load. Rollback routes to a compatible Raft binary; it does not reinterpret the
namespace as Fleet data or destroy the durable replica directories.
