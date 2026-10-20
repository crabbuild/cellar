# Cellar multi-Raft delivery package

Status: implementation in progress. MR-01 through MR-07 are complete with
recorded evidence. Shared log storage, multi-group Raft, authenticated peer
transport, verified SQLite/LTX images, restricted definitions and node-owned
command/query serving are implemented. All six planned MR-07 host cases pass;
its [evidence](evidence/MR-07.json) records 47 owning cases and the
[host audit](host-audit.md) covers mode and submitted-producer transitions.

Public handles serve Quorum commands and committed-image queries through
bounded, joined SQL workers. The application identity pins required command
contracts while query compatibility stays independent of voting. Native HTTP
and Axum share the counter's normalization and dispatcher. Its executable drill
checks six endpoints, late startup failure with disk reuse, joined shutdown,
retained receipts and new writes after restart. Real TLS partition fixtures cover
both a discarded proposal and a retained proposal committed by the majority.

MR-08 is in progress. Local Stale/AtLeast/ReadIndex queries and opaque tokens
retain [local evidence](evidence/MR-08-local-progress.json). Separately owned TLS
query RPCs now support bounded member selection, explicit leader fallback and
forwarding from voters without query code; [network evidence](evidence/MR-08-network-progress.json)
records 83 owning cases, 12 Raft unit cases and repeated network histories.
The [HTTP/context evidence](evidence/MR-08-http-progress.json) adds 34 owning
host/read cases, captured capabilities during a live ordinary invocation, and
explicit consistency/tokens through native/Axum HTTP. Its executable drill
verifies 24 remote strong reads, retained tokens and cross-cell refusal before
and after restart. [Peer-fault evidence](evidence/MR-08-peer-progress.json) adds
36 owning cases and repeated saturation, queued expiry and delayed TCP reply
histories. The [read audit](read-audit.md) retains directory/ingress routing
outside registered group membership and final requirements review. All seven
named cases exist; MR-08 is not closed.
MR-09 is
ready for implementation. S3 archive/restore, snapshot installation, membership
controllers, effects and later release/scaling work remain pending. Unsupported
SDK capabilities are refused. These tests do not establish production capacity
or completion of the full delivery scope. The reviewed source baseline is
`1ac7a3e11482274b536dc2389280969e8963887c`.

Build a storage mode in which each cell has a three-voter Raft group, leaders
execute commands, replicas serve explicit queries, LTX carries physical SQLite
changes, and S3 archives committed history. A node leads some groups and follows
others. Majority commits replace the current all-follower proof for this mode.

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
python3 plans/multi-raft/plan.py show MR-01
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
