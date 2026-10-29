<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Cell-group implementation handoff

This deliverable is an executable design and implementation plan. It does not
claim completion of the shared runtime, a production cutover or measured TiKV
performance parity. The target is the same partitioned multi-Raft pattern: many
whole cells per group, one Raft machine per group replica, and many groups sharing
node resources. A group spans voter nodes; it is not all cells on one node.

## Starting point

Use branch `codex/shared-cell-groups` and the existing
[draft PR 160](https://github.com/crabbuild/cellar/pull/160).
The committed handoff before this document is
`fde3e6f96a388c3f17566667a81b14df49febc00`; the reviewed source baseline is
`c187c47773fd22d4658de1d912b6096f92a8e26d`.
Fetched `origin/main` at `1be27d234d4674d4d014785c60cecbfe394bdab9` is already
included. Recheck origin before publishing later implementation changes.

The [TiKV investigation](../density-decision.md) pins the supplied checkout at
`b3995dab28308f09cbb7c125bf32f789dbc203fe`. Follow its Region ownership and shared
worker model, adapting storage to independent SQLite/LTX cells. Do not copy KV
transactions, split a cell's SQLite history, or assume TiKV sizing thresholds.

Read [design](design.md), [protocols](protocols.md),
[implementation order](implementation-plan.md) and
[qualification](qualification.md). `tasks.json` is the source for task ownership,
dependencies, steps, exact tests and acceptance oracles. `profiles.json` freezes
candidate workload and performance gates; those numbers are targets, not results.

## First executor session

Inspect `git status` and the existing diff before editing. This working checkout
contains an uncommitted physical owner prototype in
`crates/cellar/durability/raft/cell_groups/owner.rs` and its native execution
fixture, plus supporting edits in `cache.rs`, `storage.rs`, the group module and
directory bootstrap. It is not connected to the shared scheduler or public SDK.
Its test target has unresolved compilation failures documented in
[progress](progress.md). A clean checkout of the committed handoff does not
contain this prototype. Preserve or explicitly transfer it when assigning work;
do not mistake its presence for a passing task gate.

Run from the repository root with Python 3.11+ and Rust 1.98.1:

```sh
export PATH="$HOME/.cargo/bin:$PATH"
export CARGO_TARGET_DIR="$HOME/Workspace/crabbuild-target/cellar-3617-cell-groups"
python3 plans/multi-raft/cell-groups/plan.py validate
python3 plans/multi-raft/cell-groups/plan.py preflight
python3 plans/multi-raft/cell-groups/plan.py show CG-04
python3 -m unittest discover -s plans/multi-raft/cell-groups -p 'test_*.py'
```

Choose a different private target directory for a different checkout. The plan
runner validates and prints commands; it does not execute Rust tests or provision
infrastructure. `ready` currently returns no TODO task because CG-04 is already
IN_PROGRESS and its successors depend on it. Resume CG-04 rather than resetting
statuses. Global preflight intentionally rejects the unreviewed owner diff;
record a reviewed successor source only after reconciling that implementation.

The three DONE records require their retained evidence directories listed in
[README](README.md). Transfer those artifacts with the source when validating on
another machine. Missing evidence must refuse; do not replace it with a pass flag
or reset completed tasks merely to bypass validation.

## Delivery sequence

| Milestone | Tasks | Required outcome |
| --- | --- | --- |
| Contracts and storage | CG-01–03, scoped gates already passed | Separate cell/group history, complete durable roots, bounded range routing and startup preparation |
| Shared runtime | CG-04–06 | Real multi-cell consensus and execution, complete-group snapshots, verified S3 archive and safe reclamation |
| Public cutover | CG-08 then CG-07 | Preserve effects and idle caches; route SDK and managed fleet through the shared runtime |
| Adaptive groups | CG-09–12 | Fenced transfers, split/merge, placement and coordinated hibernation/wake |
| Recovery and qualification | CG-14, CG-13, CG-15 | Reproducible load harness, fenced fresh-incarnation recovery, real RustFS and independent-domain tests |
| Final delivery | CG-16 | Remove the superseded one-cell architecture and pass all release gates at one revision |

Use the generated dependency order for exact scheduling. A hot cell may occupy
one group through the same runtime. Do not introduce a permanent old/new mode or
an old-format migration reader; preserve marker 1 with explicit domain separation
and refuse unsupported persisted layouts. Never delete existing user data as
part of the hard cutover.

CG-04 needs all six named real-Raft cases: `three_cells_one_rawnode_per_voter`,
`concurrent_enrollment_one_cell`, `shared_group_fair_admission`,
`cell_receipt_not_group_progress`, `strong_read_at_group_barrier` and
`apply_prefix_never_skips_slow_cell`. Fixing a prototype's compilation or passing
a low-level owner test does not establish fair SQL execution, SDK dispatch,
receipt settlement or strong-read serving. Its complete brief prints the owning
paths and exact command/evidence gate.

## Release boundary

Preserve local quorum as the Quorum acknowledgement boundary. S3 protection
requires a verified complete checkpoint and contiguous tail; uploaded log bytes
alone do not authorize reclaiming local recovery dependencies. A failed complete
group restore refuses admission. Disaster recovery requires the chosen fencing
authority API, Docker qualification, permanent old-host restart prevention and
fresh group/cell incarnations, including incomplete topology transitions.

Measure occupancies 1/8/32/64/128, both image modes, resource use and request
latency under the frozen profiles. Report wake latency by its actual phases and
include timeouts and refusals. No fixed cost saving or immediate hibernation
response is promised before measurement. Local Docker/RustFS results and
independent failure-domain evidence are separate gates.

Retain source, binary and output hashes, including failed runs. Update task status
only after its full gate passes. CG-16 reruns the complete capability matrix at
one release revision; `plan.py release` must continue to refuse until then.
Keep the requested MR-13/MR-14 progress JSON files excluded from the PR. Reuse the
active PR, report qualification limits and leave merging to a separate action.
