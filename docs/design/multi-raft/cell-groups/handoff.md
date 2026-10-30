<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Cell-group implementation handoff

The executable design and implementation plan is delivered. This handoff scopes
the remaining end-to-end implementation and hard cutover, including the full task
and release gates, for subsequent execution.
This handoff does not claim completion of the
shared runtime, a production cutover or measured TiKV
performance parity. The target is the same partitioned multi-Raft pattern: many
whole cells per group, one Raft machine per group replica, and many groups sharing
node resources. A group spans voter nodes; it is not all cells on one node.

## Starting point

Use branch `codex/shared-cell-groups` and the existing
[PR 160](https://github.com/crabbuild/cellar/pull/160).
The newest reviewed source baseline is `ee684c33`, adding a fixed-roster node
controller that drives existing shared TLS/apply owners without fixture pumping.
Its partial record is `.cellar/cell-groups-evidence/CG-04-event-controller-qualified/checkpoint.json`:
58 shared-execution cases, eight commands, 244 source inputs and
one executable. The preceding `61aa7752` gate passed 289 Rust regressions before
Clippy found a duplicated test fixture import; only that test import changed, and
the affected execution suite was rerun. Transfer both gates with prior evidence.
Six new cases
cover two groups on one SQL worker, both image modes, exact ownership, real
manifest sync failure, cancelled close and final-handle cleanup. Commands still
require caller-retained invocations; post-response resolution is not implemented.
Dynamic roster and per-group failure isolation remain gaps. CG-04 stays IN_PROGRESS.
The preceding reviewed source baseline is `eb99a242`, connecting physical groups
to the existing production TLS transport without constructing the legacy runtime.
Its partial record is `.cellar/cell-groups-evidence/CG-04-physical-transport/checkpoint.json`:
192 native cases, 91 library cases, nine commands, 241 source inputs and
eight selected executables. Transfer that directory with prior evidence. The
three new native cases qualify shared peer pools and routed command settlement
over TLS in both image modes. They still use fixture-driven runtime events, not
a production controller after ingress cancellation. Complete-group snapshots
explicitly refuse. CG-04 remains IN_PROGRESS.
The preceding reviewed source baseline is `1f014f42`, adding retained routed
command phases over the existing catalog, consensus and SQL owners. Its partial
record is `.cellar/cell-groups-evidence/CG-04-routed-commands-qualified/checkpoint.json`:
105 native cases, eight commands, 229 source inputs and four selected executables.
Transfer that directory and the retained failed `CG-04-routed-commands` and
`CG-04-routed-commands-final` gates with prior evidence. Cancellation tests retain the invocation; production controller
ownership after ingress cancellation remains unqualified. The command phases
preserve captured inputs across proven-non-commit retry and never equate proposal
acceptance with durability. S3 minima refuse pending real shared archive proof.
CG-04 remains IN_PROGRESS; follow the next execution priorities below.
The preceding source baseline is `8723db2d`, adding catalog-bound query
invocations over the existing runtime/SQL owners. Its partial record is
`.cellar/cell-groups-evidence/CG-04-routed-queries/checkpoint.json`: 103 native
cases across four affected suites, eight successful commands, 227 source inputs
and four selected executables. Transfer the complete directory with prior evidence.
This qualifies local routed queries, including authenticated cell tokens,
unchanged deadlines/attempt budgets and cancellation ownership. It does not
qualify live movement, command/enrollment routing, remote transports or the public
SDK/controller cutover. CG-04 remains IN_PROGRESS.
The preceding source baseline is `7a79511d`, adding registered queries with exact
cell observations on the existing workers. Its partial checkpoint is
`.cellar/cell-groups-evidence/CG-04-registered-queries/checkpoint.json`: 319 selected
Rust cases, seventeen successful commands, 225 source inputs and sixteen selected
executables. Transfer its entire directory with prior evidence. That checkpoint
preceded the local catalog-routed query integration; CG-04 remains IN_PROGRESS. The
explicit temporary `QueryVersion::GroupCut` and `invoke_group_query` must be
removed with the old host by CG-16.
The preceding baseline is `b28078af`, adding registered commands on shared
SQL workers and logical command/effect identities. Its partial checkpoint is
`.cellar/cell-groups-evidence/CG-04-registered/checkpoint.json`: 295 selected Rust
cases, seventeen successful commands, 221 source inputs and fifteen selected
executables. Transfer its entire directory with prior evidence. It does not
complete CG-04, qualify shared effect scheduling or implement the public cutover.
The preceding `1ec2eeaa`
checkpoint qualifies selected-image recovery and physical apply fault coverage.
Its retained record is `.cellar/cell-groups-evidence/CG-04-apply-recovery/checkpoint.json`:
260 selected Rust cases, fifteen commands, 205 source inputs and twelve executables.
Transfer that entire directory with prior evidence. It qualifies joined cold
restart, not whole-process kill or power loss. The preceding `c9100ec7` checkpoint
qualifies committed preparation on shared workers (`2085cc96`). The preceding `ed84df5d` checkpoint retains exact
cell-version witnesses in authenticated manifests and query/receipt observations. The preceding read
checkpoint at `3da4481e` decouples pin capacity from transport limits and repairs
the fixture macro. The initial read implementation is `88c4717d`. Earlier checkpoints cover prepared submission sealing
and exact rejection cleanup (`0cf89c40`) and bounded proposal acceptance and
enrollment capacity (`446dd905`). Earlier checkpoints cover shared-worker initialization
and term fencing (`6e09bbc7`, formatted at `82ae6877`), receipt lookup (`91c3ecfc`),
applied-barrier reconciliation (`8a0cf640`), the fixed SQL pool (`51dff0e5`),
the SQL producer (`f807b2f7`) and shared-node runtime (`aec4029e`).
See the latest checkpoint in [progress](progress.md) for qualification and limits.
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

Receipt lookup now uses `receipts::lookup_identity`, preserving identity, floor,
sequence and allocation checks independently from new-client admission. Command
admission retains both ceilings. Native fixtures now cover saturated identity
tables, replicated expiry floors and conflicting identities in both image modes,
including callback suppression and later successful command admission.

Initialization now runs through `CellGroupSqlPool::initialize` and
`CellPrivateEnrollment`, using `CellEnrollmentAuthority` from the real leader.
Logical name admission covers both commands and initialization, including competing
incarnations. `CellEnrollmentState` reads verified current-term applied creation
state on leaders or followers; `resolve_enrollment` preserves the original record,
requires a newer applied term for absent/conflicting old producers, and never
reexecutes callbacks. Prepared outputs carry their term; the guarded submission
path checks it on the actual owner before append, including after that node
regains leadership. Cached acceptance grants no new authority.
The four native cases exercise materialized/extents, concurrent creation, other
cell progress, three cells in one group, retries after mutation, cancelled close,
and absent/conflicting/committed enrollment recovery. All 191 selected Rust cases
and all-target Clippy pass at `6e09bbc7`. The retained partial checkpoint is
`.cellar/cell-groups-evidence/CG-04-initialization/checkpoint.json`; it includes the
initial formatting failure. Formatting-only successor verification is recorded in
`.cellar/cell-groups-evidence/CG-04-initialization-format/gate-commands.json`: all
six checks pass, including formatting, layout, dependencies, docs, preflight and
37 validator tests. These records do not mark CG-04 DONE.

The current admission change counts applied cells and accepted pending creations
against `max_cells_per_group` before append. Pending acceptance is keyed by logical
cell name, binds proposal identity to exact bytes, and returns the original
position on a retry. Conflicting bytes refuse; another proposal for the same
pending name waits. Independent cells remain eligible. Once applied, stale bases
and existing enrollments refuse another append and require receipt/creation
resolution. Acceptance metadata has a separate shared node budget so saturation
cannot prevent Ready or completion work. All 195 selected Rust cases and ten
verification commands pass at `446dd905`; the source-bound checkpoint is
`.cellar/cell-groups-evidence/CG-04-proposal-admission/checkpoint.json`, with 187
source inputs and eight copied executables. See [progress](progress.md) for the
retained intermediate failures and qualification limits. This does not complete
CG-04.

Prepared SQL submission now shares an irreversible decision across clones. Only
an actual owner's definitive pre-append capacity refusal seals the preparation;
a generic queue error leaves its outcome unresolved. The exact seal can be
recovered locally after a lost response, and the worker accepts it only for that
same preparation and engine. It closes the private producer before releasing
name admission. A fresh preparation with the same proposal ID cannot be closed by
the old proof. Accepted preparations retain their original position/digest even
after apply; this remains provisional acceptance, never commitment. Prepared
submission also closes byte-only proposal entry points on that owner.

Next connect catalog routing for SDK creation/commands and the production host
controller to the registered workers. Local catalog-routed query invocations now
consume exact cell-version observations. Use `CellSqlPrepared::submit` and its typed result;
never release a producer from generic overload or same-term absence. Preserve
the original request fingerprint, deadline and attempt budget across redirects. Do not synthesize a legacy `GroupScope` to
satisfy callback/effect identities. The arbitrary-callback `stage` API remains a verification boundary; use
`stage_registered` for compiled commands. Public SDK routing is still pending. Real compaction/production retries
and preview/cache lifecycle remain required. These cases do not complete CG-04.

The remaining coupling is concrete: `execution::CommandAuthority` binds the
old `GroupLog`/`GroupStatus`; `execution::images::Published` exposes `GroupScope`;
and the old `host_sql::Cell` still keys its execution state by that scope. The
new pool path already keys by cell history and exact image pins; replace the
production dispatcher with that path while keeping group authority separate.
Reuse the existing receipt transaction and callback machinery; do not manufacture
a synthetic cell-scoped group to satisfy the old interfaces.
A later mutation of another cell must not settle the first cell's pending output.

The registered command boundary now carries `CellHistory` through
`CommandContext`, inbox authentication, effect targets and system-client identity.
`CellGroupSqlPool::stage_registered` validates the operation, full class schema,
normalized input digest and authenticated identity before receipt lookup, then
uses the existing shared worker and exact proposal settlement path. Signed effect
wire and the class manifest use distinct marker-1 domains; there is no old-wire
reader. The temporary single-cell host projects its actual logical binding.
The shared command path derives identity directly from cell execution authority.

Registered query contexts still expose `CommittedVersion`/`GroupScope`. Replace
that boundary with the selected logical cell version and witness before the public
cutover; never infer a mutation sequence from group progress. The catalog/SDK
adapter must select the authorized class and preserve invocation deadline,
fingerprint and retry budget. Full shared effect scheduling remains CG-08 work
before CG-07. Reuse the existing SQL workers and joined shutdown.
A cancelled caller cannot release physical callback ownership or allow a second
producer for that cell.

Shared read selection and SQL execution now exist. Use
`CellGroupRuntime::read_authority` with the original absolute domain deadline,
then consume its pin through `CellGroupSqlPool::query`. Strong reads correlate
fresh quorum to that invocation and wait for contiguous apply; Stale/AtLeast
select logical cell versions. The immutable pin, node/group reservations and
worker-owned query survive publication and cancellation. A returned query result
borrows retained bytes and exposes its logical `CellVersion`.

Registered query integration must still replace
`application/consensus/class.rs::QueryContext`'s `CommittedVersion`/`GroupScope`
identity with the logical cell version. Preserve operation/schema validation,
read-only callback restrictions, version-token authentication and installed
transfer lineage. `CellExecutionAuthority` and `CellReconciliationAuthority`
still grant no strong-read authority; use the new request-owned read path.
Do not synthesize a cell-scoped legacy group or treat the group barrier index
as the target cell's mutation position. SDK token issuance should consume `CellSqlQueryResult::observation()` or
`CellCommittedReceipt::observation()`. These retain the selected version's actual
cell decision witness. Real compaction integration must preserve it; never use
the latest group progress or the retried command's proposal as a substitute.


## First executor session

### Design delivery check, 2026-10-30

The requested deliverable is the executable design and implementation plan.
The existing runtime checkpoints remain implementation evidence at their stated
boundaries; this delivery does not complete CG-04 or authorize a release claim.
Fetching origin confirmed that the branch contains `origin/main` at `8a93ffd7`.
The supplied TiKV checkout still matches the investigation pin above.

Package validation passes: 16 tasks, 95 required cases, 14 invariants and nine
profiles. Repository documentation validation also passes. In the current working
tree, preflight deliberately refuses source drift in
`crates/cellar/durability/raft/host_sql/mod.rs`; consequently 36 of the 37 plan
validator tests pass and the actual-source preflight test errors. Do not refresh
the baseline merely to silence this refusal.

At that design-delivery check, `cell_groups/apply/` and
`host_sql/cell_groups/apply.rs` were uncommitted drafts. Subsequent execution
completed that adapter at `2085cc96` and repaired the regression fixture at
`c9100ec7`; see the current checkpoint in [progress](progress.md). The untracked
`diagram/` directory remains separate existing work and is excluded from this
implementation checkpoint.

### Resume implementation

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
gate printed by `show CG-04`. Registered SDK dispatch remains absent; registered query callbacks now consume
the existing read-authority and immutable-query paths.

The selected version's complete commit witness now persists in the authenticated
cell leaf and flows through query and receipt observations. The source-bound gate
at `ed84df5d` qualifies exact entry matching, unchanged-cell and older-pin
provenance, restart/image-collection recovery and token round-trips in both image
modes. Actual log-compaction and group-snapshot integration remain unqualified.

Local query invocations now connect catalog sessions to registered query workers.
Next connect command/enrollment routing and the public host without synthesizing
legacy group identities. Preserve original invocation deadlines,
contracts and retry identities. Query callbacks must receive logical cell versions;
returned tokens must use the selected observation. `CellGroupApplier` now consumes the startup manifest-page owner, and
`CellGroupSqlPool::prepare_apply` prepares committed cell images and complete
manifests on the existing workers. Consume its retained output with
`CellGroupPreparedApply::complete`; that queues owner publication and is not a
commit acknowledgement. The new native fixture uses this adapter on all three
voters. Registered host dispatch must now consume these APIs instead of copying
fixture image preparation. Keep physical apply position separate from provenance
in later group transfers.

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

The latest fully passing partial CG-04 record is
`.cellar/cell-groups-evidence/CG-04-apply-final/checkpoint.json` at `c9100ec7`:
254 selected Rust cases, fifteen successful commands, 202 source inputs and
12 copied executables. Transfer the entire directory and its failed predecessor
`CG-04-apply` with the prior evidence. It qualifies committed image preparation
on the existing SQL workers and retention through held publication, not direct
image-I/O cancellation/crash cuts or registered host serving. See the priorities
below before continuing CG-04.

The preceding durable-witness record is
`.cellar/cell-groups-evidence/CG-04-witness/checkpoint.json` at `ed84df5d`:
214 selected Rust cases, thirteen successful commands, 194 source inputs and nine
copied executables. Transfer the full directory with prior evidence. It qualifies
durable selected-version witnesses and read/receipt observations; registered SDK
serving, real compaction, CG-04 and the release gate remain incomplete.

The preceding coherent-read record is
`.cellar/cell-groups-evidence/CG-04-reads-final/checkpoint.json` at `3da4481e`:
204 selected Rust cases, twelve successful commands, 194 source inputs and eight
copied executables. Transfer the entire directory, including intermediate failures,
with the prior evidence. This qualifies low-level coherent reads and immutable
SQL in both image modes; it does not complete registered SDK query serving,
production compaction, CG-04 or the release gate.

The preceding submission record is
`.cellar/cell-groups-evidence/CG-04-submission/checkpoint.json` at `0cf89c40`:
197 selected Rust cases, ten successful commands, 189 source inputs and eight
copied executables. Transfer the entire directory, including intermediate failures,
with the prior evidence. Both image modes exercise rejection cleanup for creation
and mutation, exact proof matching, remembered acceptance and delayed retries.
This remains a partial CG-04 checkpoint; it does not qualify production dispatch,
coherent reads, RustFS or the hard cutover.

The earlier receipt-lookup partial CG-04 record is
`.cellar/cell-groups-evidence/CG-04-receipt-lookup/checkpoint.json` at `91c3ecfc`:
187 selected Rust cases, ten successful commands, 181 source inputs and eight
copied executables. Transfer its entire directory with the earlier checkpoints.
It qualifies saturated-capacity lookup, replicated expiry/conflict and existing
reconciliation paths in both image modes; it does not complete CG-04.

The prior reconciliation record is
`.cellar/cell-groups-evidence/CG-04-reconciliation/checkpoint.json` at `8a0cf640`:
181 selected Rust cases, nine successful commands, 179 source inputs and eight
copied executables. Transfer its entire directory along with the prior records.
It qualifies the new pool reconciliation boundary in both image modes, not actual
compaction, production retry dispatch or the full task gate. See [progress](progress.md)
for the historical tested boundary and subsequent lookup/refusal qualification.

The prior partial CG-04 evidence is retained at
`.cellar/cell-groups-evidence/CG-04-sql-pool/checkpoint.json`. Transfer that entire
directory, including copied binaries and intermediate/successful logs, with the
prior SQL-producer, runtime/owner and DONE-task evidence. The pool checkpoint
binds 178 source inputs, eight executables, 184 selected Rust cases and ten
verification commands. It cannot stand in for the complete CG-04
contract. Three RustFS-specific cases remain explicitly unrun at this checkpoint.

## Next execution priorities

Deliver CG tasks in dependency order and keep the status at its actual qualified
boundary. At the end of each turn, record the completed behavior, evidence and
limits, then identify the most critical remaining gap and the concrete next step.
Do not let plan maintenance replace implementation.

1. Move fresh phased command invocations into `CellGroupController`, independently
   from ingress response waiters. The fixed-roster event controller now drives
   physical `GroupHistory` transport and SQL apply without fixture pumping; extend
   that owner rather than introducing cell-specific tasks. Keep the original
   response deadline while retaining bounded resolution ownership afterward.
   Qualify caller disconnect/timeout after SQL entry and provisional acceptance:
   resolve the exact receipt or proven non-commit without replaying uncertain
   callbacks. Existing routed command tests still retain their invocation manually.
   Before cancellation cleanup, extend `execution/submission.rs`: an attempted
   submission error currently resets to Open; that is not a pre-append proof.
   Seal only never-attempted preparations and retain an explicit uncertain state
   after attempted submission errors. Serialize the seal with delayed clones and
   prove physical cleanup with exact rejection capabilities.
   Missing origin producers on followers still need a safe leader receipt-lookup
   route; never infer non-commit from missing local state. Then add catalog-routed
   enrollment with stable creation identity, shared query RPC and the public
   `RaftCellHandle` binding. Requested and namespace S3 durability continue to
   refuse until CG-06 supplies real archive proofs. The event controller currently
   has a fixed roster and closes the node on fatal apply errors; add dynamic group
   registration and per-group failure isolation before claiming runtime completion.
2. Complete CG-04's production retry/cache lifecycle and configuration-entry
   handling, then its full gate before DONE. Use selected-root recovery at startup.
   Whole-process kill/power-loss remains distinct from the qualified directory-sync
   faults and joined cold restart. Full snapshots/membership follow in CG-05,
   archive/reclamation in CG-06, and shared effect scheduling/cache lifecycle in
   CG-08 before the CG-07 public cutover.
3. Remove `TransportRuntime::Legacy`, the typed cell-scoped frame arm and old
   transport entry points in CG-16. Also remove `QueryVersion::GroupCut` and
   `RaftClass::invoke_group_query` with the old host by CG-16. They explicitly preserve the old in-memory group observation
   during integration; they must not become a permanent alternate serving mode.
   Final `QueryContext` exposes the actual cell observation directly. Never derive
   a cell sequence or witness from a group log position.

The opportunity is to reuse the now-shared consensus, SQL, read and apply paths in
one dispatcher. Additional per-cell workers, a second production mode or a demo
that bypasses the registered API would increase integration work.

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

CG-04 needs its six core real-Raft cases, plus the admission and submission cases
listed in `tasks.json`: `three_cells_one_rawnode_per_voter`,
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
