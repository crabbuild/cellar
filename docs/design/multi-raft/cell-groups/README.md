<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Cell group delivery package

Status: executable design and implementation plan delivered on 2026-10-29.
CG-01 through CG-05 have passed their scoped native evidence gates. CG-05's
complete checkpoint, authenticated learner/rebuild, membership, restart and
resource-ownership requirements pass at `e0550acd`. Its current gate includes
all five exact cases and 205 Rust regressions across eighteen commands.
CG-06 verified S3 checkpoint/tail coverage and safe log reclamation is next.
The production SDK/fleet has not yet cut over; topology, hibernation, complete S3
recovery and deployment/release qualification remain tracked in later tasks.
See [implementation progress](progress.md) and the generated task index.
Package validation alone does not qualify implementation or a release.

This delivery is the executable design and implementation plan. End-to-end
implementation and hard cutover remain tracked as separate task and release gates. See the [executor handoff](handoff.md) for the current
source, qualification boundary, reviewed main integration and commands. Plan
delivery or an intermediate task gate does not qualify the complete runtime.

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
   eligibility; `show CG-06` emits a complete executor brief, source excerpts,
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
`.cellar/cell-groups-evidence/CG-03-routing-final/` directories. CG-04 adds
`.cellar/cell-groups-evidence/CG-04-qualified/` and its supporting
`.cellar/cell-groups-evidence/CG-04-local-provisioning-final/` directory. A fresh
checkout must receive these retained directories at the same relative paths
before validating the first four DONE checkpoints. CG-05 additionally requires
`.cellar/cell-groups-evidence/CG-05-completion-admission/qualification/` and its
retained predecessor/reproduction records. Missing evidence fails validation.
CG-05's earlier partial source checkpoint is retained in
`.cellar/cell-groups-evidence/CG-05-checkpoint-source/` and the receiver slice in
`.cellar/cell-groups-evidence/CG-05-checkpoint-receiver/`, followed by owner/runtime
acceptance in `.cellar/cell-groups-evidence/CG-05-snapshot-owner/`. These partial
checkpoints and local publication in
`.cellar/cell-groups-evidence/CG-05-local-checkpoint/`, authenticated transport in
`.cellar/cell-groups-evidence/CG-05-snapshot-transport/`, automatic policy in
`.cellar/cell-groups-evidence/CG-05-controller-checkpoints/` and membership in
`.cellar/cell-groups-evidence/CG-05-group-membership/` remain historical partial records;
the complete current-source gate above closes CG-05.
The design, task briefs and validator unit tests are committed in this package.

```sh
python3 docs/design/multi-raft/cell-groups/plan.py validate
python3 docs/design/multi-raft/cell-groups/plan.py preflight
python3 docs/design/multi-raft/cell-groups/plan.py ready
python3 docs/design/multi-raft/cell-groups/plan.py show CG-06
python3 -m unittest discover -s docs/design/multi-raft/cell-groups -p 'test_*.py'
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

Continue from the existing code and retained evidence. CG-01 through CG-05 are
DONE at their declared native boundaries; CG-06 is dependency-ready and requires
implementation. Read [progress](progress.md), then run `plan.py show CG-06` for the
complete archive/reclamation brief. Preserve every group's selected current and
checkpoint closure, exact cell identities and permanent physical claims. Only
verified complete remote coverage may settle S3 outputs or authorize archive-policy
reclamation. A task's current-source gate must pass before marking it DONE; final
release repeats the complete capability/profile matrix at one revision.

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
obtains current-term quorum before protocol admission. The shared node scheduler now multiplexes these physical owners. The per-cell SQL
producer and exact receipts now pass their partial regression gate, followed by
fixed-pool SQL ownership and admission at `51dff0e5`. Applied-barrier receipt
reconciliation is implemented at `8a0cf640`, followed by read-only receipt lookup
and saturated-capacity/expiry/conflict cases at `91c3ecfc`. Initialization and
concurrent enrollment now run on that pool at `6e09bbc7`, including original
creation retries, follower resolution and atomic preparation-term proposal checks.
Bounded proposal acceptance and configured enrollment capacity are implemented at
`446dd905`, including pending creations, exact acceptance retries, stale-base
refusal and a shared node metadata budget. Prepared submission sealing and exact
rejection cleanup are implemented at `0cf89c40`; see the latest checkpoint for
qualification. Coherent shared read selection and immutable queries now exist
at `88c4717d`, repaired and qualified at `3da4481e`: 204 selected Rust cases
and all twelve verification commands pass. Durable cell-version witnesses and
query/receipt observations are implemented and qualified at `ed84df5d`: 214
selected Rust cases and thirteen verification commands pass. See the handoff
for the exact scope and retained evidence.
Committed image preparation now runs through the shared SQL workers at `2085cc96`,
with its fixture repair and full partial gate at `c9100ec7`: 254 selected Rust
cases and fifteen commands pass. Transfer both
`.cellar/cell-groups-evidence/CG-04-apply-final/` and the retained failed
`.cellar/cell-groups-evidence/CG-04-apply/` directory with the prior evidence.
Selected-image recovery and physical apply fault coverage are implemented at
`1ec2eeaa`. Both image modes cover cancelled preparation/close while real image
or manifest directory sync is held, failed-sync old-root preservation, and cold
selected-root recovery with committed replay and no renewed serving authority.
All 260 selected Rust cases and fifteen commands pass; transfer the retained
`.cellar/cell-groups-evidence/CG-04-apply-recovery/` directory with prior evidence.
Whole-process kill and power-loss cuts remain unqualified. See the newest
[progress checkpoint](progress.md) for the committed-source gate and evidence.
Registered command callbacks now run on shared SQL workers at `b28078af` with
logical cell identity, pre-receipt validation and exact receipt settlement.
Command/effect identities exclude the physical group; old signed effect layouts
refuse under the marker-1 cutover. At that checkpoint shared query context, effect scheduling and
production catalog/SDK dispatch were incomplete. All 295 selected Rust cases
and seventeen commands pass; transfer
`.cellar/cell-groups-evidence/CG-04-registered/` with the previous evidence. Its
checkpoint binds 221 source inputs and fifteen selected executables. See the newest
progress record for scope and retained failures.
Registered shared query callbacks now receive exact logical cell observations at
`7a79511d`, preserving selected witnesses, read-only SQL and retained physical
ownership through cancellation and joined shutdown. The old adapters expose a
distinct temporary group cut that CG-16 must delete. See [progress](progress.md)
for this checkpoint's qualification: all 319 selected Rust cases and seventeen
commands pass. Transfer `.cellar/cell-groups-evidence/CG-04-registered-queries/`
with prior evidence; its checkpoint binds 225 source inputs and sixteen selected
executables. Registered SDK/catalog routing, production
retry/compaction integration and cache lifecycle remain required. Managed
fleet integration remains required by CG-07. A birth
metadata observation alone cannot authorize voter startup.

Local query invocations now connect the catalog to registered execution at
`8723db2d`: namespace/class bindings, immutable request fingerprints, original
deadlines/attempt budgets, authenticated AtLeast tokens and destination authority
checks share existing workers. All 103 native cases across four affected suites
and eight commands pass. Transfer
`.cellar/cell-groups-evidence/CG-04-routed-queries/` with prior evidence; it binds
227 source inputs and four selected executables. Command/enrollment routing and
production controller/SDK integration are the next critical gaps. Metadata-only
redirect tests do not qualify live transfer. See [progress](progress.md).

Catalog-bound command phases now retain original SQL ownership through cancelled
preparation, recover repeated acceptance and settle exact receipts at `1f014f42`.
A real leader-transfer retry preserves captured inputs; S3 minima explicitly refuse
until archive proof integration. All 105 native cases and eight commands pass.
Transfer `.cellar/cell-groups-evidence/CG-04-routed-commands-qualified/` and the failed
`CG-04-routed-commands/` and `CG-04-routed-commands-final/` gates with prior evidence. The newest record binds 229
source inputs and four executables. Production controller ownership after ingress
cancellation and physical-group transport integration are the next critical gaps,
followed by routed enrollment and the public SDK cutover. See [progress](progress.md).

Physical-group peer transport now starts directly on the shared runtime at
`eb99a242` and reuses existing TLS pools, pipelines, budgets and shutdown ownership.
Typed group histories refuse old untagged frames; unsupported group snapshots
refuse. All 192 native cases, 91 transport-filtered library cases and nine
commands pass. Transfer `.cellar/cell-groups-evidence/CG-04-physical-transport/`
with prior evidence (241 source inputs, eight executables). At that checkpoint,
production controller ownership and reconciliation after caller disconnect remained
required, followed by routed enrollment and SDK/shared-query RPC integration.
Those fixtures still drive runtime events. See [progress](progress.md) for exact limits.

At `f77d0d93`, registered initialization and catalog-routed local enrollment
run on the shared controller. Commands and creations share bounded admission and
response-independent ownership. Exact creation records survive later mutations;
conflicts do not replay initialization. The partial gate passes 90 Rust cases
and ten commands; transfer
`.cellar/cell-groups-evidence/CG-04-enrollment-controller-qualified/` (254 source inputs,
four executables) with prior evidence. Next connect public handles and remote
shared dispatch, including safe receipt routing. Lazy birth/dynamic roster,
per-group failure isolation and configuration/cache lifecycle remain gaps.
See [progress](progress.md); CG-04 is not complete.

At `da79a696`, attached-peer shared query RPC uses the existing bounded TLS lane
with physical group/logical cell identity and actual cell tokens. Host-owned
registration, original deadlines/retry budgets and physical SQL cleanup remain
bound. The partial gate passes 195 Rust cases and eleven commands; transfer
`.cellar/cell-groups-evidence/CG-04-remote-queries-final/` (260 source inputs,
4 executables). Nonmember shared discovery, bounded destination route caching
(the gateway currently resolves the catalog per RPC), write-authorized mutation
RPC and public SDK cutover remain gaps. See [progress](progress.md); CG-04 remains
IN_PROGRESS and no shared-group RustFS/load qualification is claimed.

At `c25f0d97`, explicit nonmember read discovery uses that same query lane without
local group storage or voting identity. Four-endpoint TLS fixtures cover read
permission/process binding, revocation during held SQL, stale discovery replies,
route bounds and cleanup in both image modes. The partial gate passes 198 Rust
cases and eleven commands; transfer
`.cellar/cell-groups-evidence/CG-04-nonmember-queries/` (262 source inputs,
4 executables). Bounded destination caching, write-authorized RPC and public-host
automatic discovery/SDK binding remain open. CG-04 remains IN_PROGRESS.

At `06652e36`, dispatcher clones share bounded range hints across reads, commands
and enrollment. Verified head changes invalidate hints; superseded lookups cannot
refill them. Three catalog cases and a both-image runtime case qualify eviction,
metadata-read reduction and catalog-outage behavior without weakening destination
admission. The partial gate passes 202 Rust cases and eleven commands; transfer
`.cellar/cell-groups-evidence/CG-04-dispatch-route-cache-final/` (265 source inputs,
4 executables). Write-authorized RPC and safe receipt lookup are next; removing
destination catalog reads requires replicated transfer seals. CG-04 stays IN_PROGRESS.

At `5fd44437`, remote commands use explicit namespace/process write grants and
an authenticated command purpose on the bounded TLS RPC lane. Existing controller
owners retain admitted work after disconnect/revocation, and committed duplicates
return the same receipt without rerunning SQL. The partial gate passes 209 Rust
cases and eleven commands; transfer `.cellar/cell-groups-evidence/CG-04-command-rpc/`
(273 source inputs, 4 executables). Remote enrollment, standalone receipt lookup
and the public SDK cutover remain open; CG-04 remains IN_PROGRESS.

At `0fe665d0`, remote enrollment shares the bounded command exchange, explicit
write grants and exact controller registration. Creation receipts survive later
mutations and retries; admitted initialization survives disconnect/revocation and
publication deadline. The partial gate passes 216 Rust cases and eleven commands;
transfer `.cellar/cell-groups-evidence/CG-04-enrollment-rpc/` (276 source inputs,
4 executables). Safe standalone receipt lookup and public SDK cutover remain;
CG-04 remains IN_PROGRESS.

At `fbc15053`, standalone receipt lookup uses a quorum image without invoking
callbacks or releasing unresolved producers. It requires explicit command grants,
returns signed observations and keeps absence ambiguous. The partial gate passes
227 Rust cases and twelve commands; transfer
`.cellar/cell-groups-evidence/CG-04-receipt-rpc/` (279 source inputs,
4 executables). Dynamic group lifecycle/lazy birth and per-group failure
isolation are next; CG-04 remains IN_PROGRESS and public cutover is still pending.

At `4326421a`, controllers admit bounded dynamic registrations and may start empty.
Catalog/root/page ownership, fresh quorum, response-independent attachment and
joined shutdown remain enforced. The partial gate passes 231 Rust cases and twelve
commands; transfer `.cellar/cell-groups-evidence/CG-04-dynamic-registration/`
(281 source inputs, 4 executables). Per-group failure isolation and
request-driven lazy birth are next; CG-04 and public cutover remain incomplete.

At `41121a7d`, group-local failures quarantine only the owning group while retaining
its capacity and physical work. Real shared-engine faults still stop the node.
The partial gate passes 233 Rust cases and twelve commands; transfer
`.cellar/cell-groups-evidence/CG-04-group-quarantine-final/` (283 source inputs,
4 executables). Bounded request-driven provisioning is next; CG-04 remains open.

At `69e41e86`, the controller provisions selected initial-member ranges with bounded
coalesced recovery, retained prepared owners and joined shutdown. Actual commands
and all-voter joined engine/page-store recovery pass in both image modes. The
partial gate passes 238 Rust cases and twelve commands; transfer
`.cellar/cell-groups-evidence/CG-04-local-provisioning-final/` (288 source inputs,
4 executables). The full scoped CG-04 gate below supplements this incremental
checkpoint. Production integration and shared RustFS/load remain unqualified.

CG-04 is now DONE at `69e41e86` after all 83 declared cases pass individually and
the plan runner validates every required command. Transfer
`.cellar/cell-groups-evidence/CG-04-qualified/` and its supporting evidence
(98 distinct command records, 288 source inputs). CG-05 complete-group
snapshots and membership recovery is next. Public SDK/fleet cutover belongs to
CG-07; complete release qualification still requires CG-16.

The implementation executor should deliver code, updated component contracts and
the required retained evidence for each task on the active PR. The final handoff
requires CG-16 and the release gate, including the real RustFS and independent
failure-domain profiles. Successful plan validation alone satisfies this design
deliverable, not those implementation or deployment gates.

## Deliverables and source of truth

| Artifact | Responsibility |
| --- | --- |
| `design.md` | Identities, routing, storage, APIs, compatibility and bounded ownership |
| `protocols.md` | Enrollment, read RPC, apply, snapshot/archive, transfer, hibernation and disaster contracts |
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

The parent `docs/design/multi-raft/plan.py validate` already rejects a repository-relative
component link in `010-large-cell-recovery-time.md`. This package has its own
repository-aware link validation; it does not suppress or claim to fix that
unrelated parent failure. Run normal repository docs checks as well.
