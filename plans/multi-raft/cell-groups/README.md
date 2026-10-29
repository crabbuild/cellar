<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Cell group delivery package

Status: executable design and implementation plan delivered on 2026-10-29.
CG-01 contracts and CG-02 storage have passed their local evidence gates.
CG-03 routing and enrollment is in progress.
The production host has not yet cut over to shared groups.
See [implementation progress](progress.md) and the generated task index.
Package validation is not implementation, benchmark or release qualification.

The current requested deliverable is the executable design and implementation
plan. The target remains an end-to-end hard cutover to the shared-group runtime;
executing all remaining tasks is a separate implementation phase. Existing code
checkpoints are preserved and do not mark the remaining tasks complete.

This package implements the direction selected in the
[density decision](../density-decision.md). The target is TiKV-style partitioned
multi-Raft: many cells in a group, many groups on a node, group replication across
nodes, dynamic split/merge, placement and coordinated hibernation. Cellar keeps
independent SQLite/LTX cell state and its Quorum/S3 durability contracts.
It does not implement TiKV's KV API, MVCC, transactions, wire compatibility or PD.

## Read and execute

The plan runner requires Python 3.11 or newer and Git; it has no Python package dependencies.

1. Read [design](design.md), [protocols](protocols.md) and
   [qualification](qualification.md). These define the target, including crash
   behavior and final release gates; they supersede fixed assignment as the final
   target. Fixed assignment is an intermediate milestone only.
2. Run the commands below from the repository root. `ready` reports dependency
   eligibility; `show CG-03` emits a complete executor brief, source excerpts,
   scope, steps, exact test commands and completion rules. It never runs them.
3. Implement each task in dependency order on the active PR branch or an explicitly
   assigned branch. Preserve working intermediate commits. Do not expose a second
   production mode or reinterpret existing data as the new format.
4. Record actual test outputs and source/artifact hashes. A task can become DONE
   only with verified evidence and completed dependencies. Final qualification
   repeats the full capability matrix at one release revision.

This checkout retains evidence under the ignored
`.cellar/cell-groups-evidence/CG-01-ee7f4a8d2dec/` and
`.cellar/cell-groups-evidence/CG-02-ec9d277e5041/` directories. A fresh checkout must
receive both retained directories at the same relative paths before validating
the DONE checkpoints. Missing evidence fails validation; it is never treated as a pass.
The design, task briefs and validator unit tests are committed in this package.

```sh
python3 plans/multi-raft/cell-groups/plan.py validate
python3 plans/multi-raft/cell-groups/plan.py preflight
python3 plans/multi-raft/cell-groups/plan.py ready
python3 plans/multi-raft/cell-groups/plan.py show CG-03
python3 -m unittest discover -s plans/multi-raft/cell-groups -p 'test_*.py'
```

`validate` checks the task graph, coverage, source inventory, links and evidence
for DONE tasks. `preflight` checks source drift against `baseline.json`; it does
not silently refresh hashes. After a dependency changes an input, review that
change and update the baseline with its new revision and review note before
executing the dependent task. `show` labels planned test targets that are absent;
an absent target is never a passing test. `release` exits nonzero until every
task and required deployment gate is evidenced at the final source revision.

The runner accepts no arbitrary shell commands and launches no processes or
containers apart from read-only Git source inspection. Commands displayed in
executor briefs are explicitly run by the implementation agent. Destructive
qualification belongs only to the harness's recorded, owned disposable resources.

### Current executor handoff

The latest user clarification selects an executable design and implementation
plan. This handoff supersedes earlier instructions in the progress history to
continue implementation immediately. No further runtime changes are part of this
plan handoff; all remaining implementation and qualification gates stay explicit.

When implementation is resumed, start with CG-03 after reading
[progress](progress.md) and inspecting the working diff.
CG-01 and CG-02 are DONE only at their named contract/storage boundaries; the
remaining tasks still require implementation. `ready` lists eligible TODO tasks;
an IN_PROGRESS task is resumed from its existing brief and working state. Each
task's complete source-bound evidence gate must pass before changing its status
to DONE. Continue in the generated dependency order afterward.

The next concrete boundary is physical group bootstrap: remove the cell binding
from replica/log identity, durable engine mappings, node inventory and permanent
member claims. Connect the verified birth to synced genesis and test restart,
unknown outcomes and refusal after a claimed voter disk is lost. A birth metadata
observation alone cannot authorize voter startup. A fabricated cell name is not
a substitute for a group identity. Then complete bounded redirects before CG-04
connects enrollment to the shared runtime.

The implementation executor should deliver code, updated component contracts and
the required retained evidence for each task on the active PR. The final handoff
requires CG-16 and the release gate, including the real RustFS and independent
failure-domain profiles. Successful plan validation alone satisfies this design
deliverable, not those implementation or deployment gates.

## Deliverables and source of truth

| Artifact | Responsibility |
| --- | --- |
| `design.md` | Identities, routing, storage, APIs, compatibility and bounded ownership |
| `protocols.md` | Enrollment, apply, snapshot/archive, transfer, hibernation and disaster state machines |
| `tasks.json` | Ordered implementation tasks, file ownership, steps, tests and invariant mapping |
| `implementation-plan.md` | Human index generated from the same tasks; `show` supplies complete briefs |
| `qualification.md`, `profiles.json` | Fault oracles, fixed workload contracts, numerical candidate gates and evidence format |
| `baseline.json` | Reviewed Cellar source hashes/excerpts and TiKV investigation pin |
| `plan.py`, `test_plan.py` | Executable package validator, drift checks, task briefs and negative validation tests |

The complete scope includes managed fleet integration, effect identity preservation
through split/merge, and disaster recovery while a transfer is incomplete. These
cannot be dropped to make a fixed-group demonstration count as full delivery.
One-cell groups remain available through the same implementation for hot cells.

The initial independent review corrected three delivery hazards: effects must be
adapted before public cutover; catalog admission tests precede real enrollment
integration in the host task; and remotely verified transfer closure is mandatory
before publishing split/merge ownership. The validator also binds benchmark cohorts
to exact binaries, controls and profiles instead of accepting aggregate pass flags.

No supported density, performance improvement or hibernation latency is claimed
until the corresponding profile passes. Local containers do not prove independent
host or zone resilience. Historical one-cell measurements remain baseline evidence.

## Execution conventions

Use Rust 1.98.1 and locked Cargo builds at this baseline. Read current root and
subtree AGENTS.md instructions. This checkout uses a private workspace-volume
target directory, for example:

```sh
export PATH="$HOME/.cargo/bin:$PATH"
export CARGO_TARGET_DIR="$HOME/Workspace/crabbuild-target/cellar-3617-cell-groups"
```

Keep `cellar-logic` free of I/O, async and dependencies. Use execution-domain
services and join owners on shutdown. Use a component `mod.rs` with named children,
or a leaf with sibling test files; never combine `feature.rs` and a Rust-filled
`feature/` directory. Match `native_raft_snapshot.rs` and `support/raft_cluster.rs`
for real-engine, native SQLite fixtures, rather than scheduler tests with stub
application markers. Do not enable `cellar_internal_tests` without its external
harness. Preserve license notices and keep credentials and raw cell data private.

The parent `plans/multi-raft/plan.py validate` already rejects a repository-relative
component link in `010-large-cell-recovery-time.md`. This package has its own
repository-aware link validation; it does not suppress or claim to fix that
unrelated parent failure. Run normal repository docs checks as well.
