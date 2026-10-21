# Cellar multi-Raft delivery package

Status: MR-01 through MR-11 are complete with source-pinned local evidence.
MR-12 through MR-17 remain open. The complete delivery goal is not achieved.

The [MR-11 completion evidence](evidence/MR-11.json) pins source
`cc93ccf96bda3c48caf57d1f59d7d3bcd1a4a355`: all 248 unique Rust cases across
12 binaries and all 26 commands pass. This includes all seven original MR-11
requirements, 25 membership cases, seven pure leadership cases, 48 Raft library,
42 snapshot, 29 archive, 27 host, 16 directory and the affected public Node
lifecycle case. Five ignored child entry points execute through owning parents
and are excluded. Clippy, both LTX modes, format (678 Rust files), layout,
dependency policy, docs, 17 planning tests and 71-anchor preflight pass.

An opt-in owned controller inventories committed plans, probes bounded rotating
candidate sets and repairs without incoming business traffic. A six-Node fixture
covers failed-member replacement, canonical empty-state repair before the first
command, pause/resume, minority refusal with reachable object storage, and loss
of controller/leader while resuming the exact Seeding plan. Eight groups exercise
automatic leadership movement to caught-up voters while a control-reachable,
data-lagging voter remains ineligible; node-wide attempt limits bound movement.
Fresh-quorum seed grants, actual matched/published progress and explicit joint
consensus retain the existing authority boundaries. Declared domains/capacities
are placement inputs, not measured infrastructure qualification.

Public Node shutdown joins the controller's domain and closes retained Raft
handles. Its existing Fleet stop report does not yet supply a graceful Raft
close receipt or report Raft archive obligations; MR-13 retains that semantic
drain work. Existing explicit `RaftCells.close()` tests retain their separate
receipt/join contract. No physical S3, power-loss, real-zone resilience,
production capacity, larger tiers, federation or complete release is claimed.
MR-12 through MR-17 remain open.

Shared durable logs, multi-group consensus, authenticated transport, private
leader commands, explicit follower queries, committed S3 archives, snapshots,
authorized replica recovery and automatic redundancy repair are implemented.
A node can lead some groups and follow others. S3 restores cell images and
committed history; it grants no votes, process identity, leadership or quorum.

The next priority is MR-12 durable business features, followed by MR-13 node
operations, MR-14 batching/workloads, MR-15 release qualification, MR-16 measured
density/physical tiers and MR-17 independent-cluster federation. The first
release remains MR-01 through MR-15, including the required 24-hour soak.
Hundreds/thousands of physical nodes and millions of cells require measured
qualification. Millions of physical nodes are not a single-cluster promise.

The current internal identity is
`cellar-raft-ltx-1/members-1/seeds-1/s3-1/repairs-1/nodes-1`; numeric markers remain 1.
Use fresh namespace identities, storage prefix and local state. Earlier
[archive evidence](evidence/MR-09.json), [recovery evidence](evidence/MR-10.json),
[process regressions](evidence/MR-11-process-regression-progress.json) and
[component records](evidence/MR-11.json) retain their original qualification.
The process record includes all 88 public Node cases at its separate source.

## Deliverables

| File | Purpose |
| --- | --- |
| [Technical design](technical-design.md) | Authority, commit, storage, read, archive and recovery protocols |
| [Implementation plan](implementation-plan.md) | Ordered work packages, source ownership, deliverables and stop conditions |
| [Qualification plan](qualification.md) | Failure scenarios, correctness oracle, performance methodology and release gates |
| [Scaling plan](scaling.md) | Group density, hundreds/thousands of nodes, millions of cells and federation |
| [Application contract](application-contract.md) | Command/query registration, receipts, versions, effects and compatibility |
| [Task manifest](tasks.json) | Dependencies, test cases, command argument vectors and completion evidence |
| [Source baseline](baseline.json) | Source hashes for drift detection |
| [Capacity profiles](capacity-profiles.json) | Explicitly hypothetical scale scenarios |
| [Plan tool](plan.py) | Validate the package, inspect ready work and calculate placement arithmetic |

## Start implementation

Use a complete checkout at the baseline or a reviewed successor. Python 3.10+
and Git are sufficient for the package checks; runtime implementation uses the
repository toolchain and build instructions.

```sh
python3 plans/multi-raft/plan.py validate
python3 plans/multi-raft/plan.py preflight
python3 plans/multi-raft/plan.py ready
python3 plans/multi-raft/plan.py show MR-12
python3 -m unittest discover -s plans/multi-raft -p 'test_*.py'
```

`validate` checks document links, source paths, task dependencies, case ownership,
commands and evidence requirements. `preflight` compares the pinned source
hashes with the checkout and fails on drift. It does not rewrite the baseline.
Review every reported change, revise affected contracts and refresh the baseline
in the same reviewed change before starting dependent work.

`show` prints a task's implementation requirements and future verification
commands. It does not execute shell text, modify source, install dependencies or
mark work complete. The task commands become runnable as their targets are
implemented. The manifest records required test names, so an empty test target
or a filter that runs zero cases cannot count as completion.

Update task status manually after its evidence is recorded. DONE requires all
dependencies DONE, all named tests present and passing, and a linked evidence
record with source revision, commands, outcomes and limitations. BLOCKED describes
an implementation prerequisite, not an instruction to bypass safety.

## Decisions to implement

- One cell per group, three durable voters; optional learners for additional reads.
- `raft` 0.7.0 consensus core and `raft-engine` 0.4.2 as the selected log-engine
  integration baseline, subject to MR-02 compile, filesystem and crash gates.
- Shared node runtime, transport and log engine; no per-cell thread or log engine.
- Private leader execution state; only committed, crash-consistent SQLite
  generations can serve queries or establish successful command results.
- One outstanding data proposal per cell in the first release.
- Quorum durability by default in the new mode, with explicit S3-durable requests.
- Stale, minimum-version and safe ReadIndex linearizable queries.
- Raft owns leadership and membership. No bucket-only fallback after quorum loss.
- An isolated namespace and storage prefix with immutable mode selection.
  Cells in Fleet mode retain their current authority protocol.
- Automatic replica repair and balanced placement are first-release requirements.
- Small materialized databases first. Shared-cell groups, Raft hibernation,
  page-versioned execution and federation are separate scaling milestones.

## Delivery boundary

MR-01 through MR-15 deliver and qualify the first release. MR-16 and MR-17 are
later scaling work, with their own release evidence. Hundreds/thousands of nodes
and millions of logical cells are qualification targets, not established limits.
Millions of physical nodes require federation and are not a single-cluster promise.

The earlier `codex/read-replica-design` proposal preserves the Fleet ownership
protocol and treats read copies as disposable caches. This proposal instead
introduces consensus-backed authority and durable voting replicas. Reuse its
query isolation ideas where useful; it is not a prerequisite branch and this
package contains the required contracts independently.

The [storage integration audit](storage-integration.md) records the host filesystem
adaptation required by MR-02.

## Validation record

Package integrity and negative checker tests are recorded in
[validation](validation.md). These checks do not establish Raft safety, runtime
performance, or deployment qualification. Runtime evidence belongs to the work
packages and [qualification gates](qualification.md).
