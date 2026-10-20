# Cellar multi-Raft delivery package

Status: implementation in progress. MR-01 through MR-09 are complete with
source-pinned evidence. Shared durable log storage, multi-group Raft,
authenticated transport, verified SQLite/LTX images, restricted definitions and
node-owned command/query serving are implemented. The [MR-08 evidence](evidence/MR-08.json)
and [read audit](read-audit.md) cover the completed read-contract boundary.

Queries support Stale, authenticated AtLeast and safe ReadIndex consistency.
Bounded member attempts and one optional hint preserve contract/input/token,
stale policy and the original deadline. Query RPCs have independent capacity;
waiting reads cannot occupy their control/LTX catch-up connection. A query-only
ingress can read existing cell histories without query code or local
member/image state. Trusted namespace-origin grants authorize exact physical
processes separately from voting; live revocation cancels old work. Startup
receives the current trusted view; durable distribution and dynamic placement
remain controller work.

The [MR-09 evidence](evidence/MR-09.json) pins source
`2298fd442f0b867695b564527a66b61684aff2c2`: 251 Rust cases pass, including all
26 archive requirements/cases, 88 Node cases, 16 Raft library cases and 11 pure
command lifecycle cases. The remaining owning targets cover storage, physical
LTX, runtime, transport, host and read contracts. Six ignored process child
entry points are invoked by owning parents and excluded from those counts.
Axum feature runs pass 27/33 cases including the compile doctest. The external
counter drill covers six endpoints, 24 remote strong reads, late startup cleanup,
retained tokens/receipts and joined restart. All 23 recorded commands pass,
including Clippy for the affected crates/feature modes, formatting, layout,
dependency policy, docs, 17 planning tests and the reviewed 68-anchor preflight.
These local/loopback checks do not qualify physical S3, power loss, zones,
production capacity or a complete release.

MR-09 is complete at the archive/output boundary. Committed image cuts and
snapshot/LTX-tail/dependency sets are physically replayed before conditional
head publication. Node-owned scheduling settles exact S3 proofs, preserves
minimum durability and stronger retries, and backpressures new callbacks under
bounded retention. Deadline, cancellation, directory ownership and deferred
apply under image pressure are covered by the [archive audit](archive-audit.md).
The archive reconstructs cell images and receipts; atomic Raft snapshot
installation and authorized member recovery remain MR-10. Membership controllers,
effects and later operations/release/scaling work remain their original packages.
All 17 original packages remain in scope; the complete delivery goal is not
achieved. MR-10 is in progress at the recovery boundary.

The [feature priorities and milestones](implementation-plan.md#feature-priorities-and-delivery-milestones)
define the remaining delivery order: recovery, automatic repair, durable business
features, node operations, release qualification, then larger scale/federation.
Recovery through the public Node path is the current priority.

The [native recovery progress](evidence/MR-10-native-progress.json) pins source
`08b2bca3da32982795994d0240844324437ff0c7`: 304 Rust cases pass across 17 binaries,
including 26 snapshot, 18 read and all 88 public Node cases. All 21 commands pass,
including owning/integration Clippy, cellar-ltx backend feature modes,
format/layout, dependency policy, docs, 17 plan tests and the reviewed 68-anchor
preflight. Native nodes create checkpoints, serve exact retained cuts over bounded
authenticated TLS chunks, install durable images and resume follower reads.
Interrupted transfers retry while another group writes; shutdown joins partial
transfers. Startup rejects insufficient capacity before creating catalog/member
state. A forced lost ReadIndex reply also recovers under the original deadline
with the term and leader unchanged; successful strong reads still require quorum.

The [snapshot audit](snapshot-audit.md) records the implemented native path and
remaining authorized fresh-member/S3 admission, quorum-loss refusal and native
crash qualification. MR-10 remains IN_PROGRESS. The earlier
[cancellation record](evidence/MR-10-abort-progress.json) pins its 160-case scope at
`c6aaca08153d039717a07195d05a8b0248ac6fe5`; the
[peer-admission record](evidence/MR-10-inbound-progress.json) pins 157 cases at
`ca6f59aa37f5f6ecef6c1a7f97136fb3ee9a8a29`; the
[boundary record](evidence/MR-10-boundary-progress.json) pins 152 cases at
`a5619f3d522fd942372c520f02a52945fd0ec7b5`. The
[earlier physical record](evidence/MR-10-physical-progress.json) retains its
separate 218-case qualification at `24410fb754b055078255ccf5d595c0893f0d1b0e`.
The current record includes the complete 88-case Node target.

The [earlier storage progress evidence](evidence/MR-09-storage-progress.json)
retains its narrower 91-case qualification at source
`122a1c7b9a50af74bf9a8c6ab1efa781986661e3`. Completion uses MR-09.json, not that
historical component-only record.

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
