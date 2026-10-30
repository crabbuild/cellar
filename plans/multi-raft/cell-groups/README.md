<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Cell group delivery package

Status: executable design and implementation plan delivered on 2026-10-29.
CG-01 contracts, CG-02 storage and CG-03 routing/admission preparation have
passed their local evidence gates. CG-04 shared execution is in progress.
The production host has not yet cut over to shared groups.
See [implementation progress](progress.md) and the generated task index.
Package validation is not implementation, benchmark or release qualification.

The active goal resumes end-to-end implementation and the hard cutover from the
delivered plan. See the [executor handoff](handoff.md) for the starting revision,
existing work, commands and completion boundary. Plan delivery or an intermediate
task gate does not qualify the complete shared-group runtime.

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
   eligibility; `show CG-04` emits a complete executor brief, source excerpts,
   scope, steps, exact test commands and completion rules. It never runs them.
3. Implement each task in dependency order on the active PR branch or an explicitly
   assigned branch. Preserve working intermediate commits. Do not expose a second
   production mode or reinterpret existing data as the new format.
4. Record actual test outputs and source/artifact hashes. A task can become DONE
   only with verified evidence and completed dependencies. Final qualification
   repeats the full capability matrix at one release revision.

This checkout retains evidence under the ignored
`.cellar/cell-groups-evidence/CG-01-ee7f4a8d2dec/`,
`.cellar/cell-groups-evidence/CG-02-ec9d277e5041/` and
`.cellar/cell-groups-evidence/CG-03-routing-final/` directories. A fresh checkout must
receive all three retained directories at the same relative paths before validating
the DONE checkpoints. Missing evidence fails validation; it is never treated as a pass.
The design, task briefs and validator unit tests are committed in this package.

```sh
python3 plans/multi-raft/cell-groups/plan.py validate
python3 plans/multi-raft/cell-groups/plan.py preflight
python3 plans/multi-raft/cell-groups/plan.py ready
python3 plans/multi-raft/cell-groups/plan.py show CG-04
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

The implementation executor should continue from the existing code and retained
evidence. Review the working diff before resuming CG-04; intermediate owner and
storage checkpoints do not replace its full shared-execution gate.

Continue CG-04 after reading
[progress](progress.md) and inspecting the working diff.
CG-01 through CG-03 are DONE only at their named contract/storage/admission
boundaries; the remaining tasks still require implementation. `ready` lists eligible TODO tasks;
an IN_PROGRESS task is resumed from its existing brief and working state. Each
task's complete source-bound evidence gate must pass before changing its status
to DONE. Continue in the generated dependency order afterward.

Physical group genesis, typed replica handles, permanent initial-member claims,
complete group inventory and invocation-bounded catalog-confirmed redirects are
implemented through `0df009a2`; see the latest [checkpoint](progress.md) for
evidence and limits. CG-03 has been audited at its preparation boundary. Its
record is at
`.cellar/cell-groups-evidence/CG-03-routing-final/record.json`; transfer that entire
directory along with the earlier DONE-task evidence to the implementation
executor. CG-04 now retains creation identity with the selected image and
prepares ordered enrollment and exact retries. Its physical group owner consumes
the catalog startup permit and verified selected image/manifest closure, and
obtains current-term quorum before protocol admission. Shared scheduler, SQL
execution, receipt settlement and serving integration remain required. Managed
fleet integration remains required by CG-07. A birth
metadata observation alone cannot authorize voter startup.

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
