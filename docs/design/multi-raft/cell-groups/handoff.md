<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Cell-group implementation handoff

The executable design and implementation plan is delivered. The active goal
continues through end-to-end implementation and hard cutover, including the full
task and release gates.
This handoff does not claim completion of the
shared runtime, a production cutover or measured TiKV
performance parity. The target is the same partitioned multi-Raft pattern: many
whole cells per group, one Raft machine per group replica, and many groups sharing
node resources. A group spans voter nodes; it is not all cells on one node.

## Starting point

Use branch `codex/shared-cell-groups` and the existing
[PR 160](https://github.com/crabbuild/cellar/pull/160).
The reviewed source baseline is `8a0cf640`, adding applied-barrier receipt
reconciliation after the fixed SQL pool at `51dff0e5`, following
the SQL producer at `f807b2f7` and shared-node runtime at `aec4029e`; see the latest checkpoint in
[progress](progress.md) for qualification and remaining work.
The branch integrates `origin/main` at `8a93ffd7` (documentation consolidation,
PR 161). The design package now lives in `docs/design/multi-raft/cell-groups/`;
use the commands below. Historical evidence keeps its original tested source,
paths and hashes. Recheck origin before publishing later changes.

The [TiKV investigation](../density-decision.md) pins the supplied checkout at
`b3995dab28308f09cbb7c125bf32f789dbc203fe`. Follow its Region ownership and shared
worker model, adapting storage to independent SQLite/LTX cells. Do not copy KV
transactions, split a cell's SQLite history, or assume TiKV sizing thresholds.

Read [design](design.md), [protocols](protocols.md),
[implementation order](implementation-plan.md) and
[qualification](qualification.md). `tasks.json` is the source for task ownership,
dependencies, steps, exact tests and acceptance oracles. `profiles.json` freezes
candidate workload and performance gates; those numbers are targets, not results.

## Current implementation checkpoint

The CG-04 node runtime is committed in
`crates/cellar/durability/raft/cell_groups/runtime/`. It multiplexes physical owners,
batches engine persistence across groups, retains event admission through apply,
and joins submitted physical work on close. Native cases use independent SQLite
images and actual node workers; see [progress](progress.md) for qualification.
The prior uncommitted runtime draft has been incorporated into this source.

The per-cell SQL producer is now committed at `f3d48bdc`. It adds
`cell_groups/execution_authority.rs`, `execution/cell.rs`, shared receipt
transaction helpers in `execution/command.rs`, and `tests/support/cell_group_sql.rs`,
with changes to physical image capture, owner/runtime authority and native tests.
It separates a selected cell image/version from group progress and requires exact
receipt settlement in both image modes. The partial gate passes at `f807b2f7`:
133 native tests and eight verification commands. It does not complete CG-04.

The fixed SQL pool now accepts cell-history jobs on the existing blocking workers.
Admission, pending proposals and retained outputs survive waiter cancellation;
node/group quotas and an exclusive engine lease bound their ownership. Extent
workspaces are created and closed on those workers. The partial gate passes at
`51dff0e5`: 184 selected Rust tests and ten verification commands. It does not
complete CG-04 or qualify the production dispatcher.

Per-cell reconciliation now runs on the pool's reserved progress path. A verified
current-term applied cell pin can be inspected on a follower. Matching original
receipts resolve from later images; absence requires a strictly newer applied
term before closing the old producer. Foreign engines, wrong proposals, stale
cell pins and an unapplied new-term barrier refuse without releasing ownership.
Missing producers report uncertainty. Production retry dispatch and real
snapshot/compaction integration remain required.

Next, separate reconciliation lookup from new-client capacity admission:
`receipts::preflight_identity` currently checks capacity for an absent new client,
so saturation can keep an older absent producer unresolved unnecessarily. Use a
read-only identity/receipt lookup while preserving all command admission limits,
and add saturated-client, expiry-floor and conflicting-identity cases. Then
integrate concurrent initialization, coherent reads and registered SDK dispatch.
Preview/cache lifecycle also remains required. Preserve physical completion and
cancellation boundaries. These cases alone do not complete the six-case CG-04 gate.

The remaining coupling is concrete: `execution::CommandAuthority` binds the
old `GroupLog`/`GroupStatus`; `execution::images::Published` exposes `GroupScope`;
and the old `host_sql::Cell` still keys its execution state by that scope. The
new pool path already keys by cell history and exact image pins; replace the
production dispatcher with that path while keeping group authority separate.
Reuse the existing receipt transaction and callback machinery; do not manufacture
a synthetic cell-scoped group to satisfy the old interfaces.
A later mutation of another cell must not settle the first cell's pending output.

The registered callback boundary also needs a real identity conversion:
`application/consensus/class.rs::CommandContext::identity` currently exposes
`GroupScope`, and command identity validation/invocation passes that same scope
to inbox and system-effect authentication. Carry logical `CellHistory` through
this boundary and keep physical consensus proof separate. Do not enable effects
with a fabricated group scope; preserve the CG-08-before-CG-07 dependency. Reuse
the new `host_sql::Pool` cell-history path and joined shutdown; do not create a
second node worker pool or a task per enrolled cell.
A cancelled caller cannot release physical callback ownership or allow a second
producer for that cell.


## First executor session

Inspect `git status` and the existing diff before editing. The physical owner in
`crates/cellar/durability/raft/cell_groups/owner.rs` and its native execution
fixture now use real three-voter Raft with complete group manifests and separate
SQLite images. The initial fixture compilation failures have been repaired.
The owner now runs on the shared node scheduler; public SDK and per-cell SQL
integration remain incomplete. Preserve its persistence, completion and physical
lease boundaries during integration;
do not mistake the low-level owner cases for the full CG-04 gate.

The SQL checkpoint includes `native_raft_execution` and `native_raft_rooted`
as well as the shared-group execution suite. Both cell receipt cases include
the final callback/owner-close extension. Preserve authority revocation, exact
committed entry matching and duplicate callback suppression during integration.
The pool now provides applied-barrier receipt reconciliation; production retry
dispatch, real compaction integration and preview/cache retention still need the
owning host. Reuse the old executor's current-term
applied-barrier rule when replacing it: a submitted producer cannot be discarded
from a sampled leader status, timeout or missing response. Resolve the original
command against the authoritative cell receipt before admitting another producer. These regressions supplement, and do not replace, the full task
gate printed by `show CG-04`. Registered SDK dispatch and strong-read serving
remain absent.

Run from the repository root with Python 3.11+ and Rust 1.98.1:

```sh
export PATH="$HOME/.cargo/bin:$PATH"
export CARGO_TARGET_DIR="$HOME/Workspace/crabbuild-target/cellar-3617-cell-groups"
python3 docs/design/multi-raft/cell-groups/plan.py validate
python3 docs/design/multi-raft/cell-groups/plan.py preflight
python3 docs/design/multi-raft/cell-groups/plan.py show CG-04
python3 -m unittest discover -s docs/design/multi-raft/cell-groups -p 'test_*.py'
```

Choose a different private target directory for a different checkout. The plan
runner validates and prints commands; it does not execute Rust tests or provision
infrastructure. `ready` currently returns no TODO task because CG-04 is already
IN_PROGRESS and its successors depend on it. Resume CG-04 rather than resetting
statuses. Global preflight rejects any further unreviewed source drift; record
a reviewed successor source after reconciling each implementation change.

The three DONE records require their retained evidence directories listed in
[README](README.md). Transfer those artifacts with the source when validating on
another machine. Missing evidence must refuse; do not replace it with a pass flag
or reset completed tasks merely to bypass validation.

The latest partial CG-04 record is
`.cellar/cell-groups-evidence/CG-04-reconciliation/checkpoint.json` at `8a0cf640`:
181 selected Rust cases, nine successful commands, 179 source inputs and eight
copied executables. Transfer its entire directory along with the prior records.
It qualifies the new pool reconciliation boundary in both image modes, not actual
compaction, production retry dispatch or the full task gate. See [progress](progress.md)
for tested behavior and remaining receipt lookup/refusal work.

The prior partial CG-04 evidence is retained at
`.cellar/cell-groups-evidence/CG-04-sql-pool/checkpoint.json`. Transfer that entire
directory, including copied binaries and intermediate/successful logs, with the
prior SQL-producer, runtime/owner and DONE-task evidence. The pool checkpoint
binds 178 source inputs, eight executables, 184 selected Rust cases and ten
verification commands. It cannot stand in for the complete six-case CG-04
contract. Three RustFS-specific cases remain explicitly unrun at this checkpoint.

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
