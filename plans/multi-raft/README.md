# Cellar multi-Raft delivery package

Status: MR-01 through MR-11 are complete at their recorded native boundaries.
MR-12 is IN_PROGRESS with Blob closure and committed alarms implemented, two of
its six required feature gates. MR-13 through MR-17 remain open. The complete
delivery goal is not achieved.

Leaders execute private commands; serving replicas apply committed SQLite/LTX
history and expose explicit follower queries. S3 stores verified committed images,
tails and dependencies. A node can lead some groups and follow others. S3 restores
cell state, not old votes, process identity, leadership or quorum. An opt-in owned
controller repairs redundancy and balances eligible caught-up voters. Declared
alarms run through private replicated commands and an owned leader-only timer.

| Boundary | Qualification |
| --- | --- |
| [MR-11 automatic repair](evidence/MR-11.json) | Source `cc93ccf`: 248 Rust cases, 12 binaries, 26 checks; six-node repair and eight-group balancing |
| [MR-12 immutable Blobs](evidence/MR-12-blob-progress.json) | Source `8cfc4bd`: 354 cases, 16 binaries, 31 checks; includes all 88 public Node cases |
| [MR-12 committed alarms](evidence/MR-12-alarm-progress.json) | Source `5a6b606`: 322 cases, 19 binaries, 31 checks; five new native alarm cases and all 42 snapshot cases |

Each record retains its full source pin, executable/output hashes, failed initial
diagnostics and original scope. Counts from different sources are not added.
The [business-feature contract](effects-design.md) describes Blob, alarm and
private-system boundaries. Outbox dispatch, durable receiver receipts, failover
races, progress replication, full unsupported-binding validation and examples
remain MR-12 work. Existing receipt bounds still apply without automatic pruning.

Public Node shutdown joins domain cancellation. Its existing Fleet report does
not supply a graceful Raft close receipt or permission to discard unarchived
disks; MR-13 retains unified drain, readiness, resource and archive-obligation
work. MR-14 adds batching/workloads, MR-15 release qualification including the
24-hour soak, MR-16 measured density/physical tiers and MR-17 federation.

Local/in-memory stores, declared zones and small native fixtures do not qualify
physical S3, power loss, failure zones, production performance/capacity, larger
physical tiers or federation. Hundreds/thousands of physical nodes and millions
of cells require measured qualification. Millions of physical nodes are not a
single-cluster promise. All original packages remain in scope.

The internal namespace identity is
`cellar-raft-ltx-1/members-1/seeds-1/s3-1/repairs-1/nodes-1`; numeric markers remain 1.
Class capability/system contracts also enter the immutable application digest.
Use fresh compatible experimental namespaces/prefixes for hard cutovers; there
is no compatibility reader or state migration.

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
