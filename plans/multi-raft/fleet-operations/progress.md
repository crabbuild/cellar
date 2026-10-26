# Fleet operations implementation progress

The full fleet operations goal remains active. **FO-01, FO-02 and FO-03 are
complete at their documented local adapter boundaries.** FO-04 and FO-07 are in
progress; FO-05, FO-06 and FO-08 through FO-10 remain pending. Automatic
redistribution, operator commands, managed maintenance and resize are not
delivered. The task manifest is the package-status source of truth. Historical
sections below record earlier states.

## Fenced unused-movement settlement delivery

`settle_fenced_movement` adds terminal cleanup for an admitted move that never
started. Each original voter must independently apply the exact committed
cancellation fence before physical samples are collected. Every admitted boot
must still match, seed workers and initializers must have exited, and complete
destination inventory must prove no physical history binding. The common
settlement CAS retains the permit/resource journals and monotonic physical floor,
records a distinct fenced outcome and removes only the exact active book row.
A successful-replacement call cannot reinterpret a fenced receipt.

Three new native cases pass at source
`f4ef708d5f808650374d2bfb88949d4652a3b62f`: lost CAS confirmation, adapter recreation,
rejection of delayed begin/activation and proof/payload rebinding, later capacity
admission, historical retry after destination shutdown, retained floor/resource
records and continued business writes; separate cases refuse seeding/aborted work
and an unavailable original owner. The integrated source passes **442 fleet/core
cases**, including all 141 required FO-03 cases, plus **6 native rooted-execution cases**.
The cohort comprises enrollment 132, Raft units 176, catalog 12, permits 25,
directory 16, host 32, execution 21, reads 22 and six targeted archive cases.
All-target host Clippy with warnings denied, formatting, layout, dependency and
documentation checks pass. Frozen inputs, immutable binaries, logs and the
source-specific audit remain outside runtime data in the checkout-private
`qualification/fleet-operations/fenced-movement-v2/` and the unchanged-source
`fenced-movement-snapshot-v3/`. The two seed cases run in the snapshot harness;
the earlier zero-match archive filter is retained and excluded from the count.
The first focused attempt is retained: its lost-reply fixture passed a snapshot from the
old adapter to its replacement and correctly received a snapshot-owner refusal.
The test now obtains its replacement adapter's own snapshot. No production
check, deadline or assertion was relaxed.

This source integrates main `9f1e7df19d77bb7cc77a44fbb02c9f7bcbc0b491`, including
extent-backed execution. Dynamic managed registration explicitly retains the
public materialized-image selector. Fleet publication proof refuses an extent
root where replay requires a full-file SHA-256; a focused regression covers that
hash-domain boundary. The protocol hard cut now ends in
`/isolated-removal-1/images-1/fenced-movement-1`; use fresh matching roots/engines.
The merge-format failure and the subsequent compiler failures from old image
fields/storage initialization remain retained separately. The new hash-domain
regression passes in the 176-case Raft unit cohort.

FO-06 now depends directly on FO-04. Explicit resumable execution precedes the
optional FO-05 workload policy. Successful moves and fenced unused reservations
have terminal adapter paths; aborted/changed-boot cleanup, owned execution,
assignment generation, operator workflows and deployed qualification remain.

## Terminal isolated-node removal delivery

Source `cfda0e7823c72f49a10b90967104af077be19dae` adds a distinct terminal path for nodes
with admitted deployment isolation. `begin_isolated_finalization` validates the
exact quarantined boot and journal after complete survivor/archive verification;
it requires no RPC to the unavailable node and produces no graceful stop selector.
`complete_isolated_evacuation` renews current inventory, survivor quorum and
process proofs before one CAS records the isolation receipt, preserves the
quarantined disk/engine tombstone, marks Removed and releases the exact shared
disruption slot. The reachable closure-based path retains its signed proof and
public API. Typed selector/journal checks prevent switching evidence kinds on a
retry.

Three native cases pass on the frozen implementation: actual resource-backed S3
replacement through movement settlement, physical replay and terminal removal;
a later survivor failure; and a later incomplete namespace. The composed case
also covers lost finalization/removal replies, adapter recreation, rejection of
graceful proof substitution, admission of the next node operation, retry of the
old receipt without releasing the new slot, retained storage and subsequent
business writes. Primary source/binary artifacts are outside runtime data in
`qualification/fleet-operations/isolated-removal-v1/`. The pre-merge source
passes all 429 affected core/host cases and all-target Clippy plus repository
checks in `isolated-removal-v2/`.

Main `80026d16e951ecea785a817cbbd8b305a8f2d63e` was then integrated through
merge source `7ca741d46894a8744a54efe3410eb0b44a5898f0`. The fleet Raft and consensus API files are unchanged by
the merge; shared application, node, storage and binding changes are included.
The unchanged merged binaries complete **429 core/host cases** across the
initial and resumed runs: enrollment 129, catalog 12,
permits 25, directory 16, host 32, execution 21, reads 22 and Raft units 172.
Integration checks then caught an ignored authorizer-installation result in a
new-main SQLite test and an old typed-RPC test-helper call. Successors
`d5705f39249fbf7bc312460daa2b33dc67df56b7` and `c5f40e8cb7e24f91b2cb5e9b0a4f71fb1680bd88`
correct those fixtures while preserving all production Rust and Cargo inputs.

The SQLite successor passes six targeted empty-archive/publication-origin cases
and four binding tests. The final RPC fixture successor passes all three native
typed-RPC cases, all-target Clippy with warnings denied, formatting, layout,
dependency and documentation checks. The combined **435 fleet/core cases
cover all 141 required FO-03 cases**. The broad cohort was not repeated for these
unrelated test-only corrections. Frozen inputs, immutable binaries, logs and audit
remain outside runtime data in `isolated-removal-main-v3/`,
`isolated-removal-main-resume-v6/`, `isolated-removal-archive-v4/`,
`isolated-removal-compat-v7/` and `isolated-removal-permit-repro-v5/`. The original integration-check failures are retained. Two initial permit cases failed with startup
readiness and no-leader outcomes. The same binaries pass 20 exact differential
repetitions and all 25 permit cases without changes; their cause remains
unestablished and the original failure is retained. No
assertion, timeout or stack bound was relaxed.

FO-07 is now in progress for this delivered evacuation boundary. Rolling restart,
resize, reversible admission gates, cancellation and complete operator/controller
orchestration remain unfinished. Deployment isolation still comes from trusted
platform evidence supplied by the caller; no independent platform integration or
RustFS/Colima performance qualification is claimed.

## Isolated-node repair delivery

Source `3834c560e1215f7555ec796a50265df7f1a497f6` adds `attest_node_isolation`: after rooted evacuation
exclusion, a trusted deployment assertion permanently quarantines the exact boot.
The caller establishes process/worker shutdown and prevents restart; Cellar checks
scope and journal linkage. Saved startup capabilities and generic quarantine
clearance refuse. Exact lost-response retries preserve the original acceptance.
This does not create a graceful-shutdown receipt or reclaim the retained engine.

Resource-backed admission pins an isolated obsolete voter while requiring fresh
survivor quorum. Completion substitutes its admitted isolation proof only after
new RF3 excludes that exact member; surviving voters and target still supply live
physical inventories. Receipts retain isolation provenance and physical floors.
Three actual peer/S3-seeded replacements exercise recycling of the two-slot book
while the old node remains offline, with business writes after every replacement.

This work exposed a route dependency: fresh repair admission required endpoints
for every current voter, so quarantine prevented repair from starting. Placement
now uses the complete durable node set; network routes omit explicitly isolated
endpoints. The original voter still counts toward Raft quorum and failure-domain
checks until committed removal. The failing reproduction remains retained.

All eight affected core/host suites pass **426 cases** on that behavioral source:
enrollment 126, catalog 12, permits 25, directory 16, host 32, execution 21,
reads 22 and Raft units 172. Clippy then caught a test's direct filesystem call.
Successor `bdb20e2bcaf7e2be4e5ba52d1bfc6ef7d5ede000` changes only that retained-directory
assertion to use the injected filesystem through the execution domain. All
production Rust and Cargo/configuration inputs are identical. Its three isolation
cases, all-target host Clippy with warnings denied, formatting, layout, dependency
and documentation checks pass; the broad cohort was not repeated for this
test-only correction.

Frozen sources, logs and immutable binaries remain outside runtime data in
checkout-private `qualification/fleet-operations/isolation-v5/` and
`isolation-fixture-v6/`. Initial route failures, the missing test-import build and
the Clippy failure are retained separately. No assertion, timeout or stack bound
was relaxed. No deployed RustFS, enforced Colima limits, throughput/latency or
soak is claimed.
At that source, terminal isolated-node removal, deployment fencing integration
and an automatic controller remained separate work. The terminal path above is
the newer delivery; platform integration and automatic orchestration remain pending.

## Merged-main host qualification

Source `7863043eb3f91c35e8e1e4b1d82099363af6d1bc` integrates main
`d3206c924031a3f2ca3387b542c02877bb91423d`, including file-backed streamed
snapshots and the RPC scope/pump fixes. Dynamic history admission shares the
node's snapshot scratch pool. Streamed replay preserves canonical empty archives,
exact publication-origin positions and independent per-history batch completion.

Eleven complete core/Raft suites pass **569 cases**: enrollment 123, catalog 12,
permits 25, archive 73, snapshot 46, directory 16, Raft units 172, host 32,
execution 21, reads 22 and LTX integration 27. Local-store and cell-limit suites
add nine; four streamed snapshot/LTX suites add 20. All 141 required FO-03 cases
and ten movement cases pass. Ignored helper cases are not counted; the snapshot
helpers also execute through their passing parent crash matrices.

All-target Clippy with warnings denied, formatting, layout, native dependencies
and documentation checks pass. The source manifest freezes 1,252 Rust files and
21 Cargo/configuration inputs. Logs, copied immutable binaries, hashes and audit
are retained outside runtime data in `qualification/fleet-operations/` under this
checkout's private target, generations `movement-completion-merge-v8/` and
`movement-completion-merge-v9/`. Ignored local mirrors and supplemental regression
records preserve the earlier qualification records.

The v8 directory deadline test timed out waiting for its fault gate. The same
unchanged binary passed 20 focused repetitions and all 16 directory cases in v9;
its root cause remains unestablished. The original failure is retained separately,
as are the v6 missing scratch-field build failure and v7 invalid harness target.
No assertion, deadline or stack bound was relaxed. Other successful v8 suites
were retained; v9 completes the remaining checks on identical frozen inputs.
No evidence-directory files are added. Deployed RustFS, enforced Colima resources,
fleet TPS/latency and soak remain unqualified.

## Previous completion qualification

Source `c7a1152b7366a799cc7fe41b1bbda6d63493cd0c` passes all 141 required FO-03 cases and
seven complete suites: enrollment 123, catalog 12, permits 25, archive 58,
snapshot 46, directory 16 and Raft units 166: **446 passed**. Local-store and
native cell-limit suites add **nine passes**. Two snapshot helper fixtures are
intentionally ignored directly and execute through passing parent crash matrices.
The ten movement cases pass both focused and within the complete enrollment suite.

All-target Clippy with warnings denied, formatting, layout, native dependencies
and documentation checks pass on that source. All 1,210 Rust files and 21
Cargo/configuration inputs are frozen. Primary logs, copied immutable executables,
source hashes and the qualification audit are retained outside runtime data in
`qualification/fleet-operations/movement-completion-v4/` under this checkout's
private target. Small ignored mirrors and supplemental FO-01–FO-03 regression
records preserve the original completion records; FO-04 retains an explicitly
partial movement-completion record.

The first build's missing test import and the later full-boot record size refusal
remain separately retained in generations v1 and v3. Generation v2 qualifies its
superseded selected-source version only. The compact all-provider implementation
uses the existing 4-KiB value bound and passes the complete v4 cohort. Failed and
superseded attempts are not counted as the passing qualification. No evidence
directory files are added. RustFS, enforced resources, TPS/latency and soak remain
unqualified.

## Previous admission qualification and artifact retention

Behavioral source `07315b2c16c6ddad33118bd0a2afdf82230b2cfa`, including latest
main's local-store WAL fix and resource-backed membership admission, passes all
141 required FO-03 cases and seven complete suites: enrollment 117, catalog 12,
permits 25, archive 58, snapshot 46, directory 16 and Raft units 166, **440 passed**.
The local-store suite adds four passed cases and native cell limits adds five.
Two snapshot helper fixtures are intentionally ignored directly and run through
passing parent crash matrices.

That run stopped at formatting in the new movement test. Formatting successor
`397c70c384d5777fffb94563748902cb39b87db0` changes only that fixture's layout;
its bytes exactly equal rustfmt output from the behavioral source. All production
Rust and Cargo/configuration inputs are identical. The four movement cases,
all-target Clippy with warnings denied, formatting, layout, native dependencies
and docs pass on the successor. The full 440-test cohort was not repeated for
this formatting-only change. A duplicate source-anchor/output declaration was
then corrected in plan metadata; validation, source preflight and all ten plan
validator tests pass in the published metadata successor.

All 1,206 Rust files and 21 Cargo/configuration inputs are frozen with their
respective source pins. Primary logs, immutable executables, hashes and the
formatting audit are retained outside runtime data at
`qualification/fleet-operations/movement-v4/`, `movement-format-v5/` and
`movement-package-v6/` under this checkout's unique target. Small logs and exact
completion records are mirrored into ignored `.cellar/raft-perf` paths. Failed
formatting, fixture and metadata attempts remain separately retained and are
not counted as passing checks. Earlier artifacts deleted by operator cleanup
are unavailable. No files under `plans/multi-raft/evidence/` are added to this PR.

## Completed FO-03 lifecycle

Trusted enrollment, complete managed-history inventory, guarded initial voters,
dynamic registration and evacuation candidates now extend through terminal
removal. A recreated adapter resumes a rooted Finalizing operation without
changing its accepted stop authority. It owns current RF3 quorum observations
and physical archive replay at the fixed publication prefix. After replay it
renews every live group's quorum proof and probes survivor processes again.

Only complete current inventory and exact boot-pinned signed closure after
physical owners join authorize one CAS that records the terminal receipt,
changes the node to Removed and releases the shared disruption slot. Physical
history, disk/engine identity and resource reservations remain. Exact retries
reconcile the permanent request journal even after lost terminal replies.
Incomplete import, a later failed survivor or unknown physical closure refuses
completion and retains the slot.

The native composed case performs an actual S3-seeded voter replacement, joint
completion, scoped stop during survivor writes, physical joins, Finalizing
resumption, terminal removal, admission of the next disruption and another
business write. These local tests qualify the adapter, not the future automatic
controller or unreachable-owner isolation workflow. Deployed RustFS, enforced
resource limits, density, throughput/latency and soak remain FO-10 gates.

## Archive publication progress

Requalification after merging main reproduced an existing cross-history progress
barrier. The second cell's object-store head had advanced to position 2 while
the host still reported position 1. Batch-level result delivery retained its
producer slot behind another cell's stalled read-back. The upload coordinator
also stopped polling member publications while it awaited that read-back.

Archive batches now report each verified history independently and keep polling
member futures during upload. A completed history drops its recovery material
and releases its capture reservation after its own publication finishes. Its
small bounded completion returns to the host independently; the physical batch
owner stays in the Node's joined job set. Producer, byte and replay-workspace
bounds are retained. Shared payload dependencies still require their own
successful verification before any referencing head can advance.

A scoped head/payload gate deterministically exercises an independent empty
history while a data-bearing history stalls in upload verification. It failed
before the coordinator fix and passes afterward. The original native S3 write
case retains its progress deadline and durability assertions. Forty consecutive
runs passed on the unchanged production implementation at `47c7f7a1`; the
successors change the regression's selection macro to Cellar's required
execution-domain macro and separate stop-reply fixtures from physical join.
The retained self-stop waiter deterministically returns `Closed` after its own
response domain shuts down; closure stays unconfirmed until real physical
owners join, then the exact signed closure and survivor writes succeed. The
final cohort repeats both cases, including three runs of the deterministic gate. Failed intermediate runs and the Clippy macro
failure are retained outside runtime data; no failed attempt is counted as a
passing cohort. Temporary diagnostic logs were removed. This establishes
independent publication progress, not a fleet TPS or latency qualification.

## FO-04 resource-backed membership admission

`reserve_movement` combines an exact RepairPlan and source with the resource
request. It owns target physical inventory and source safe ReadIndex under one
original deadline. Stable RF3, current boot/process/engine, exact history and
birth/schema, latest settled permit, member high water and staged voter failure
domains must agree. One CAS installs the target debit, bounded active book and
exact membership permit. The opaque result still requires actual group
consumption and learner initialization. Generic writes cannot mint a linked
permit or delete/replace one while its resource debit remains active. Exact
canonical journal reconciliation precedes current probes, including after target
shutdown and adapter recreation.

Four native cases cover S3 seed through joint completion and a following write,
lost CAS confirmation and exact retry, minority refusal, and two independent
writers racing for the last target slot. The loser installs neither debit nor
permit; another request refuses at the engine-slot capacity boundary. Each
writer owns its snapshot. Hosts use an independent control adapter and install
the winning cut before the next capacity decision. Fixture ownership,
backpressure and stale-cut failures are retained; no production admission check
was relaxed to make the cases pass.

## FO-04 successful movement completion

The all-reachable `complete_movement` path owns fresh terminal target quorum and
physical inventory of the target and every original voter. Admission pins all possible seed
providers, including providers different from the selected quorum source.
Canonical compact fingerprints bind every boot field within the existing 4-KiB
catalog value bound. Completion checks locally durable completed permit sequence,
stable RF3, unchanged boots/current trust, and physical seed/initializer exit.
Retired voters use their durable local cache; local observers use bounded host
control without a manufactured loopback transport route.

One CAS records the permanent completion, retains a separate monotonic physical
high-water floor and removes the exact reservation from the active book. The
original resource row, latest consumed permit and request journals remain.
Subsequent admission charges physical occupancy plus every unresolved movement.
Exact committed retries precede live probes, including after target shutdown.
Generic mutations cannot forge or delete the completion or retained floor.

Six new native cases pass for successful release with retained occupancy,
lost confirmation after peer seeding from a different voter, unstarted work,
unreachable selected and other seed providers, and an actual S3 metadata worker
that survives learner waiter loss and its deadline. The four admission cases
also pass on the same frozen source. The full regression cohort is recorded
above; these cases do not complete planner qualification.

## FO-04 explicit topology verifier

Final topology source `3af38b41a574b06e6df5f0f831cc7bdc959ca796` separates
current replica-receiving eligibility from final membership in the keep-set.
It passes **66 public core cases**, including **15 topology cases**, core
all-target Clippy with warnings denied, the downstream host-library build,
formatting, layout, dependency and documentation checks. `topology-v2/` retains
its frozen 1,256 Rust files and 21 configuration inputs, logs, immutable core
executables and audit outside runtime data. The native movement and host
all-target Clippy checks remain attributed to the parent below; they were not
repeated for this pure input-field addition. No broader host rerun is claimed.

Source `82221ec049e0da835363e14ab143d2585cc0212c` passes all seven ordinary
public core suites, **65 cases**, including 14 topology cases and the 400-pair
matrix. All ten native movement cases, core and host all-target Clippy with
warnings denied, formatting, layout, dependency and docs checks also pass.
Generation `topology-v1/` retains frozen inputs, logs and immutable executables
outside runtime data. This is additional targeted verification; the broader
host cohort remains pinned to its merged-main source above.

The pure `raft_fleet::preview_topology` API accepts a complete proposed RF3
assignment and final keep-set, then searches for a legal serial sequence of
direct replacements. It checks every intermediate stable triple and target seed
peak. Completion adds retained target occupancy; no source slot, byte or memory
is credited. The final keep-set and current replica-receiving eligibility are separate inputs;
a kept maintenance node cannot receive new copies. Exact boot/process/profile
scopes and the rooted input/policy
revisions remain in the witness. Serial dependencies require actual terminal
movement completion before the next real permit.

Each kept node has a separate loss scenario. All groups retain their two healthy
voters while bounded search assigns fresh eligible recovery bindings. The entire
restoration assignment and its staged path must fit before the verifier returns.
One work limit covers every search. Exhaustion returns no partial preview;
`RecoveryUnproven` means no direct witness was found, not global infeasibility
across arbitrary survivor reshuffles. Inputs must be stable and complete, with
fresh node ledgers and no unsettled reservations. Adapter-owned authentication
and current health remain required; this pure result grants no authority.

The verification fixtures cover the symmetric 500-cell five-to-six assignment
(250 replacements), five-to-four evacuation (300 replacements), retained source
slots, every final-node loss, staging deadlocks, alternate move orders, recovery
backtracking across disk/memory limits, retired bindings, exclusion, stale and
incomplete inputs, exhausted search and arithmetic overflow. A small exhaustive
matrix covers 400 old/new voter-triple pairs. These are executable mathematical
witnesses; they are not live movement, TPS or resource-envelope qualification.

## Next feature priorities

1. Deliver an owned resumable move executor around resource-backed admission,
   learner seeding, joint completion and terminal settlement. Persist effect
   identity/cursor, reconcile ambiguous replies and revalidate each next permit.
   Start with explicit verified layouts; optional workload balancing need not
   delay this execution path. Use the delivered fenced-unused settlement path;
   add distinct cleanup for aborted work and reconciliation of changed boots
   without freeing uncertain resources.
2. Collect complete authenticated fleet inputs and generate final assignments
   from topology intent and measured role costs. Preserve retained bindings,
   physical headroom, keep-sets, failure domains and bounded recovery witnesses.
   Add load-aware leader balancing and cooldown as an optional policy layer.
3. Deliver rolling maintenance/restart/resize and expose add/drain/remove plus
   capacity scaling through the operator API/CLI. Reconcile cancellation with
   committed moves and physical worker ownership before reporting completion.
4. Qualify RustFS/Colima at enforced resources with foreground TPS/p99, repeated
   node failures and soak. Automatic redistribution remains default-off.

FO-04 and FO-07 remain in progress. FO-05, FO-06 and FO-08 through FO-10 retain
their load policy, controller, operator, capacity-scaling and deployed qualification
scope. Repeated isolated-owner repair settlement and terminal node removal now
have explicit adapter paths; automatic fleet orchestration remains unfinished.

## Historical implementation checkpoints

The sections below preserve earlier source-specific results and then-current
status. Current package status and retained qualification are stated above.

## Implemented catalog foundation

Source `4832486df3eff29ab3b5ed61f572513e59b37828` adds the private
`crates/cellar/durability/raft/fleet_catalog/` component and an unstable verification
export. Production host/controller integration follows in the remaining packages.

- All root changes use the existing no-hidden-retry `Bucket::put_cas` adapter.
- A genesis pin prevents silently recreating an initialized fleet after root loss.
- Five immutable indexes hold policy, nodes, histories, operation projections and
  admission projections. A separate permanent mutation journal records exact
  request digests and original applied revisions.
- Lost responses can be reconciled by a fresh adapter after later revisions.
  Repeated mutation identity with different payload is refused.
- Hash-radix pages use at most 64 KiB, 128 leaf entries and a bounded depth of 16.
  Snapshot-bound cursors avoid bucket listing and preserve historical reads.
- Root, request, record, page, key and batch sizes are bounded. One admitted
  catalog turn per shared adapter supplies immediate backpressure and bounded
  index-path memory. Attempts use the execution-domain deadline and refuse a
  closed domain.

The adapter stores atomic metadata projections; it does not decide whether an
operator state transition is legal or authorize a membership change. Typed
membership/lifecycle decisions and proof checks belong to FO-02 through FO-09.
The raw projection tests establish CAS atomicity and referencing behavior, not
an end-user evacuation certificate. No business write-acknowledgement path calls
this component yet.

## Completed verification

On macOS arm64 with Rust 1.98.1 and a checkout-specific target:

| Check | Result |
| --- | --- |
| `native_raft_fleet_catalog` | 11 passed, including competing writers, lost replies, stale request identity, orphan metadata, invalid records, pagination, cancellation, backpressure and closed-domain refusal |
| `native_raft_directory` | 16 existing native cases passed, including node identity, process refresh, inspection, cached quorum routes and joined shutdown |
| Focused Clippy | Passed with `-D warnings`; existing third-party Raft warnings remain outside the changed code |
| Rust formatting and module layout | Passed |
| Native dependency check | 454 reachable packages, no JavaScript engine |
| Documentation checks | Passed |
| Plan validator tests | 10 passed |

Logs, executable digest, source identity and the FO-01 completion record are
retained under ignored `.cellar/raft-perf/fleet-operations/implementation/fo01/`.
Initial fixture and test-harness failures are retained separately. No raw evidence
was added under `plans/multi-raft/evidence/`. A fresh checkout must restore retained
artifacts before auditing a DONE record; missing logs are not treated as a pass.

These results qualify local conditional in-memory storage and native directory
regressions. Physical RustFS semantics, five/six-node operation, performance,
failure/recovery histories and the 24-hour soak remain FO-10 gates.

## Implemented membership guard

Source `cc470bb268f81bbc22f1f52e78c496e62ccb64af` adds rooted opaque catalog
capabilities and commits each group's enrollment, consumed sequence and exact
permit with configuration apply. Unused cancellation commits a same-configuration
fence; started learners abort through the existing repair state machine, and
joint transitions still finish under both majorities. Snapshots, archives, engine
reopen and successor validation retain and scope-check the guard.

Managed SDK begin/advance/add-learner calls refuse missing authorization, including
idempotent shortcuts. Fresh managed learners require registered standby activation
with their capability. Legacy fresh peer/archive `Joining` startup refuses them
before claiming a member; intact retained joined engines can resume normally.
Large control payloads and optional fleet state are boxed, and physical snapshot
completion reservations cover the expanded bounded ledger. At that checkpoint
the internal protocol ended in `/fleet-1`; older records refuse startup and require fresh experimental
namespaces, with numeric format markers retained at 1.

Read the [component contract](../../../crates/cellar/durability/raft/fleet_catalog/README.md)
for the distinction between generic catalog projections, opaque authorizations,
committed group state and fresh terminal quorum proof. Local fleet status is not
a reservation-release certificate; typed controller reconciliation comes later.

| Guard verification | Result |
| --- | --- |
| `native_raft_fleet_permits` | 7 passed: delayed cancellation replay, three retained-engine restart boundaries, joint cancellation/minority refusal, peer/archive seed roundtrip, quorumless fencing and guarded SDK/legacy startup ingress |
| Core member-state tests | 15 passed, including 3 new scope/codec/successor cases |
| Protocol cutover test | 1 passed; earlier namespace protocols refused |
| Existing native regressions | 187 passed: archive 53, directory 16, catalog 11, membership 27, runtime 19, snapshot 44 and storage 17 |
| Regression process fixtures | 5 helper cases remain ignored in direct listings and are invoked by their passing parent cases |
| Focused Clippy, formatting, layout, native dependencies | Passed; 901 Rust files, 454 reachable packages and no JavaScript engine |
| Documentation and plan validator tests | Passed; 10 validator cases |

FO-02 also names the legacy startup regression explicitly, bringing the overall
acceptance inventory to 98. Only FO-01 and FO-02 are complete at their documented
local boundaries; the remaining fleet packages and qualification gates are open.

The completion record, source/executable digests and exact log hashes are retained
at ignored `.cellar/raft-perf/fleet-operations/implementation/fo02/completion.json`;
its logs use the adjacent `fo02-*.log` paths. Initial argument-type and empty-engine
Resume fixture failures remain retained. No assertion was weakened. Managed
recovery cases use controlled retained-engine reopen; existing unmanaged abrupt
process-kill regressions also passed. Abrupt managed fleet faults, deployed RustFS,
resource tiers, performance and soak remain unqualified FO-10 work.

## FO-03 progress: conditional birth cancellation

Source `c40606d804037829f3e810b8da397b35ed192f77` adds the low-level cancellation
fence at the immutable cell-name birth key. Birth and cancellation use the same
conditional create; a cancelled winner refuses delayed creation and name reuse.
A live winner remains live and must use its ordinary quorum-backed lifecycle.
The returned `BirthDisposition` exposes the exact observed winner. Lost replies
and original-deadline expiry remain ambiguous until exact-key reconciliation,
including expiry after a successful tombstone write. Caller membership vectors
are bounded before synchronous sorting or storage access.

The birth codec requires explicit nullable `cancelled_by`; the experimental
namespace protocol now ends in `/fleet-1/enrollment-1`. Numeric markers stay at 1,
and earlier namespaces have no compatibility reader. Use fresh experimental
namespaces and matching binaries. No raw evidence was added to the tracked plans.

Five birth cases pass, including held late writers, both race winners, lost
confirmation, deadlines before/after commit, malformed identity and input bounds.
The final regression cohort also passed 63 existing cases: directory 16, catalog
11, membership permits 7 and host 29. The protocol cutover unit, focused Clippy,
formatting/layout, native dependencies, documentation and 10 validator tests
passed. Logs are retained under ignored `implementation/fo03-*.log` within the
usual `.cellar/raft-perf/fleet-operations/` directory.

`implementation/fo03/birth-boundary.json` is explicitly a partial progress record,
not completion evidence. FO-03 remains `IN_PROGRESS`: nine required native cases
are still absent, and `check-list FO-03` correctly refuses the incomplete listing.
Trusted dynamic nodes, route revocation, empty-host readiness, dormant registration
and complete inventory/birth admission are not delivered by this substep. The
acceptance inventory contained 103 named cases at that checkpoint; its remaining
scope is intact.

## FO-03 progress: rooted empty hosts

Source `3df4364c3d878d76b8e12ccca92c0e63daff7254` adds typed bounded node registration and opaque
host authorization, explicit compiled namespace/class declarations, permanent
fleet engine-instance pins and node controls independent of a first cell. The
declaration digest includes the current protocol, schema, durability and class.
Startup rechecks rooted enrollment and mandatory namespace storage identity before
publishing the endpoint. Intact engines retain their pin; changing the catalog
prefix cannot authorize a replacement engine under the old node/disk identity.

An enrolled host can start with zero business groups. Local `observe_node` reports
actual engine occupancy, unattached/retained histories, declared capabilities and
the explicitly identified startup enrollment cut. Registration remains ineligible
for placement. The observation is not remote capacity readiness, a fresh quorum
or removal proof. Existing participating histories must already be enrolled, and
the legacy automatic controller is disabled on these hosts.

Six additional native cases cover local empty-host startup/control bounds/owned
handoff, stale authorization, registration retries after lost replies, capability
mismatch, intact empty-engine restart and replacement-engine refusal. They extend
the acceptance inventory to 109 without replacing the nine still-required dynamic
enrollment/readiness cases. The source inventory now has 59 reviewed anchors.
FO-03 remains `IN_PROGRESS` and FO-04 through FO-10 retain their complete scope.

The final native cohort passed 124 cases: enrollment 11 (five birth plus six host
cases), catalog 11, permits 7, directory 16, host 29, membership 27 and operations
23. The membership process helper remains ignored in direct listings and runs
through its passing process-kill parent. Another 34 library cases passed: startup
2 and transport 32. Focused Clippy with warnings denied, format/layout checks over
908 Rust files, native dependencies (454 packages, no JavaScript engine), plan
validation, all 10 validator tests and documentation checks passed.

The cohort exposed a pre-existing metrics test expecting the former 23-operation,
20-counter vocabulary, while the reviewed baseline already exported 26 and 34.
Source `06651034` restores exact 26/34 and 634-series assertions and explicitly
checks archive instruments. The one-versus-32-group cardinality/response-bound
test passes; no telemetry behavior or assertion strength was reduced. The initial
noncanonical temporary-directory fixture failure and stale-assertion cohort logs
remain retained alongside the successful logs.

`implementation/fo03/host-enrollment-boundary.json` is a partial record, not task
completion. `check-list FO-03` still refuses the listing missing nine required
cases. These local runs do not qualify remote readiness, full inventory, RustFS,
deployed resource limits, fleet scaling, performance or the soak.

## FO-03 progress: dynamic dormant histories

Source `473e40ed114a5408e6adda78265327e6a11b2511` implements dynamic dormant
registration at its local SDK boundary. A host
prepares one bounded metadata read, rechecks current node/history enrollment and
the immutable birth, then publishes through the serialized actor. The shared
registry returns owned metadata under short locks and refuses name replacement.
Physical occupancy includes unattached engine histories; an occupied slot cannot
be reset by registration. No member or SQL image is created by this operation.

The six dynamic cases cover limits/idempotency/conflicting histories, cancelled
reads, close/join ownership, stale rooted history, undeclared classes and occupied
engine slots. The enrollment target now has 17 passing cases. The broader cohort
passed 166 native cases, including effects and query routing; the final focused
run adds the close case and reruns all 17 enrollment cases after preserving
temporary storage failure as unavailable rather than a permanent binding conflict.
All 126 Raft library units also passed. The total acceptance inventory is now
114, with eight required FO-03 cases still absent; this does not complete FO-03.

Focused Clippy (including query/effect/controller targets), formatting/layout over
910 Rust files, native dependency checks (454 packages, no JavaScript engine),
documentation checks and the 10 plan validator tests passed. The partial record
is retained at `implementation/fo03/dynamic-history-boundary.json`, alongside the
exact log hashes and the still-refused complete FO-03 test listing. It separates
the broader registry regression run from the final enrollment-only storage-error
classification rerun. No deployed RustFS, capacity-ready, TPS/p99 or soak claim is
made by this local SDK verification.

## FO-03 progress: rooted transport trust

Source `a155d22a7bbee57de3eccde135d8691e812068a6` installs a complete bounded
node-table cut before managed process discovery. The directory worker refreshes
the current root, reuses the validated immutable node cut when that exact root
is unchanged, and discovers enrolled nodes through exact namespace directory
keys without configured addresses. The fleet and namespace engine pins must
agree. Bucket objects and traffic cannot expand this trusted set.

Trust installation rejects older cuts, missing retained identities, changed pins
and removed-identity revival. Final `Removed` status cancels peer pools,
query/inspection grants and physical seed epochs. Restart, resize, draining and
evacuated states keep transport available while membership work resolves;
placement eligibility remains separate. This transport adapter does not prove
that publishing `Removed` is legal; the later lifecycle/removal workflow must
establish that proof first.

The one owned discovery batch holds a 4 MiB control reservation through metadata
and endpoint completion. Its read-only catalog adapter has a separate bounded
turn. A held background root read therefore cannot take the foreground history
admission turn. Managed hosts require that metadata capacity plus a 4 KiB request
and two administrative items. Local `fleet_trust_revision` acknowledges the last
installed complete cut, while the original startup enrollment remains distinct.
Neither field is authenticated remote readiness or a removal certificate.

| Verification | Result |
| --- | --- |
| Nine-target native cohort | 169 passed: directory 16, effects 16, catalog 11, enrollment 19, permits 7, host 29, membership 27, operations 23 and reads 21 |
| New enrollment boundaries | A live empty host observes a newer root without creating a group; a held discovery read permits foreground registration, then joins and releases its budget on close |
| Raft library tests | 128 passed, including new unknown-node/pin and revocation/grant/seed-owner cases |
| Focused Clippy | Passed with warnings denied; existing vendor warnings remain |
| Formatting, layout and native dependencies | Passed: 912 Rust files and 454 reachable packages, no JavaScript engine |
| Documentation, source preflight and plan validation | Passed; 116 required cases, 66 source anchors and all 10 validator tests |

The initial shared-turn cohort exposed two enrollment failures. Its failed log
is retained as `implementation/fo03-trust-native-shared-turn-failed.log` under the
usual ignored artifact directory. The final complete native and Raft unit runs
follow the isolated-reader fix; assertions were not weakened. The membership
process helper remains ignored in direct listings and is invoked by its passing
parent. Logs and executable hashes are recorded in the partial
`implementation/fo03/trust-boundary.json` record.

Eight required FO-03 cases are still absent, and its complete listing still
refuses qualification. Authenticated remote node observations and the required
trusted-revision acknowledgements by move participants remain to implement.
Complete import/birth admission, full retired-engine lifecycle, physical-host
policy and eligibility inspection are also pending. These local results make no
RustFS deployment, fleet-operation, TPS/p99 or soak claim.

## FO-03 progress: authenticated node observations

Source `164359427707f5bc80a64dd8d981debef7cd5d5e` includes the node RPC from
`c84540a701cf1474b7ae809d4fc3ec100c9bfc35`, the merge of `origin/main` at
`47f4a8924a54b25b579f17ad2fc5a46eb97a6ba6`, and conservative age through SDK
delivery. `RaftCells::inspect_peer_node` needs no cell scope or member. Both
physical processes must be rooted and discovered, and both installed trust cuts
must reach the requested minimum revision. Metadata grants no namespace query,
group membership, capacity activation or removal authority.

The response distinguishes startup enrollment from the installed current state
and epoch. Real TLS cases verify two empty hosts, reciprocal discovery, current
restart/resize/drain classification, unknown targets and insufficient revisions.
Reply age includes administrative queue residence and retention by an unpolled
caller; an old reply cannot appear fresh merely because its consumer waited.

Bodies are bounded at 16 KiB and share the existing independent metadata slots
and peer banks. Host response bytes remain charged through delivery. Managed
control capacity now covers one 4 MiB discovery read, a 64 KiB response and a
4 KiB request across three items. The exact-floor case serves cached metadata
with both catalog readers held; undersized startup refuses before engine pinning.
A blackholed TLS case verifies joined shutdown and the retained caller permit.

The protocol hard-cuts to `/fleet-1/enrollment-1/node-probes-1`, retaining numeric
markers at 1. The older enrollment protocol is explicitly refused. Use fresh
experimental namespaces and matching participants; no compatibility reader was
added. The main merge keeps upstream's complete archive-metric vocabulary checks
and snapshot failure diagnostics.

| Merged-tree verification | Result |
| --- | --- |
| Native cohort across 13 targets | 307 passed, including 24 enrollment cases, archive 53, snapshot 44, membership 27 and storage 17 |
| Raft library tests | 132 passed, including strict node-body and channel/purpose/request authentication cases |
| Upstream startup confirmation | 4 passed |
| Clippy, formatting and layout | Passed with warnings denied for affected targets; 921 Rust files |
| Native dependency, documentation and plan checks | Passed; no JavaScript engine, 119 required cases, 67 source anchors and 10 validator tests |

The post-merge cohort ran in two stages: archive/directory/effects/catalog passed
96 cases before the retained-age fixture constructed its sleep outside the
domain. After fixing only that test's domain entry, enrollment and the remaining
targets passed another 211. Production source did not change between these runs.
The 13-target Clippy run passed; focused lib/enrollment Clippy reran after that
fixture correction. The 132-unit run predates the fixture-only correction.

Earlier complete pre-merge native qualification (306 cases), the initial fixture
compile error, the Clippy fixture cleanup and the post-merge failed cohort remain
retained separately. The partial `implementation/fo03/node-observation-boundary.json`
record stores exact log and executable hashes and these scope distinctions.
Raw artifacts remain ignored, outside `plans/multi-raft/evidence/`.

The low-level node probe supplies required-revision observations. The full move
executor still must wait for its participants and validate current root policy;
this implementation does not complete that executor or the final quorum proof.
Complete inventory/import, birth/drain admission, retired binding lifecycle,
unknown-node ingress and strict physical-host policy remain FO-03 delivery work.
The complete FO-03 listing still refuses six missing cases; the task remains
`IN_PROGRESS`. RustFS deployment, full fleet workflows, TPS/p99, enforced Colima
resource tiers and the endurance soak remain unqualified.

## FO-03 progress: rooted managed birth admission

Fresh managed namespace roots now bind birth admission to the exact fleet
incarnation and scoped catalog key. Ordinary birth/cancellation cannot create
histories in those namespaces. A reservation CAS installs the complete initial
history descriptor and permanent name index before returning a birth capability.
The root maintains a bounded history count, without scanning every history for
each admission. Initial RF3 admission requires active, engine-pinned node records
on three distinct declared physical hosts.

The reservation and node-state update contend on the same root. An admitted
writer delayed past drain remains inventoried; a writer whose reservation CAS
loses cannot create a birth. Exact immutable-birth reconciliation changes the
history to Live or Cancelled. Timeouts and lost replies retain the reference,
and fresh adapters can reconcile the original admission after later root changes.
Pending/cancelled histories cannot mint membership authority. Name tombstones
remain permanent. These adapter tests do not certify a full node evacuation.

History descriptors now retain schema, namespace-root and live-birth digests,
initial node/member references and admission identity. Host startup, dormant
registration and group enrollment compare the actual birth and schema. Ordinary
bootstrap refuses fresh/bare managed genesis; it only resumes an already
enrolled managed group. Guarded initial-member initialization remains a required
integration step. Registering an observed import does not quiesce legacy creators,
convert its namespace or certify inventory completeness.

The protocol hard-cuts to `/birth-admission-1`; numeric markers remain 1 and there
is no compatibility reader for earlier namespace/history records. Qualification
and exact source/artifact identity are retained in the partial
`implementation/fo03/managed-birth-boundary.json` record under ignored
`.cellar/raft-perf/fleet-operations/`. This substep does not complete FO-03.

Source `dec09d328b0c8be6881a3d07f2effdd76e781ce6` is qualified locally on macOS arm64/Rust 1.98.1:

| Managed-birth verification | Result |
| --- | --- |
| Native regressions across 13 targets | 317 passed, including 34 enrollment cases |
| Raft library and startup confirmation | 132 and 4 passed |
| Affected-target Clippy | Passed with warnings denied |
| Formatting, module layout and native dependencies | Passed; 923 Rust files, 454 reachable packages, no JavaScript engine |
| Plan and documentation checks | Passed; 129 required cases, 67 reviewed source anchors and 10 validator tests |

The final native cohort includes corruption classification and ordinary managed
genesis refusal. The earlier 315-case cohort and 48-case focused cohort are retained
with their narrower source scope; the initial duplicate-helper compile failure is
retained separately. No assertions were weakened. Final source bytes were not
changed after qualification. The partial record retains log and executable hashes;
mutable target-cache paths do not imply that binary bytes were archived.

Six mandatory FO-03 cases remain absent. The new reservation race and declared
domain cases cover their adapter boundaries; they do not replace the full
lifecycle, import, participant-acknowledgement or movement-policy cases.
Guarded member initialization, complete import, group-identity conflict
reconciliation and actual lifecycle execution remain open. Initial admission
currently requires distinct declared physical hosts. Before same-host Colima
qualification, implement an explicit rooted domain policy; never relabel colocated
containers as independent physical failure domains. RustFS, resource enforcement,
TPS/p99 and soak remain unqualified. Keep fleet management default-off.

## FO-03 progress: guarded initial voters

Source `db751b13429db2750892510ae4c43410b796a256` connects confirmed births in fresh managed namespaces to
actual initial voters through both startup Replica registrations and
`activate_managed_initial_member` on running empty hosts. Genesis and fleet
enrollment share one engine sync before cache publication, permanent member-claim
confirmation and runtime attachment. The explicit Genesis origin starts at index
1; Committed enrollment still requires a later configuration entry. Missing
origin fields, bare-slot retrofit, rebound guards and origin changes are refused.

Dynamic initialization reuses the owned learner SQL/attachment pipeline, the
original deadline, a 4 MiB metadata reservation and one initializer per host.
Dropping a submitted caller can leave a successfully initialized member; same-capability
retry observes its actual state. Shutdown cancels held claims and joins ownership.
A matching declaration digest cannot bypass the namespace's coordination-key
owner. Initializer inspection now counts a slot once during transfer to the
synced engine catalog, under one catalog lock.

Cold managed startup resolves exact rooted process views before membership-route
construction, so a self-address hint is sufficient when required views exist.
Retained histories still require their startup recovery registrations; this is
address discovery, not automatic enrollment of unlisted or retired engine slots.
The protocol hard-cuts to `/managed-genesis-1`, with numeric markers at 1 and no
legacy reader for the new managed enrollment shape.

| Guarded-genesis verification | Result |
| --- | --- |
| Final native cohort across 13 targets | 323 passed, including all 40 enrollment cases |
| Raft library and startup confirmation | 136 and 4 passed |
| Clippy | Affected targets plus library passed with warnings denied |
| Formatting, layout and native dependencies | Passed; 926 Rust files, 454 reachable packages, no JavaScript engine |
| Plan and documentation checks | Passed; 135 required cases, 67 source anchors and 10 validator tests |

The six new native cases use actual empty SDK hosts, native SQLite/TLS, quorum
and archive acknowledgements, linearizable reads, data/guard recovery after
restart, held-claim caller cancellation, shutdown, stale history and foreign
coordination scope. Two new pure membership cases check origin/fence semantics;
two actual storage cases check sync visibility, reopen and non-retrofit.
Node activation and distinct physical-host labels are trusted fixture inputs;
these tests do not qualify physical capacity activation or independent machines.

Initial compilation failures remain retained. The first runtime cohort exposed
a real double count of an initializing slot after sync; the production counter
was fixed without weakening its count assertion. The next cohort exposed a test
waiting for SQL insertion rather than final member publication; that wait now
uses the actual installed membership. Both failed cohorts remain retained beside
the final successful run. Final source bytes were unchanged after qualification.

The partial `implementation/fo03/managed-genesis-boundary.json` record under
ignored `.cellar/raft-perf/fleet-operations/` pins source, log and executable hashes.
Cache paths are mutable and binary bytes were not archived. No raw artifacts were
added to `plans/multi-raft/evidence/`. FO-03 remains IN_PROGRESS with six mandatory
cases absent; FO-04 through FO-10 remain pending.

The next birth/drain integration must explicitly resume an already admitted birth
across exclusion: dynamic metadata registration currently refuses Draining nodes,
including this pending-birth case. Complete import cuts, retired-binding lifecycle,
unknown ingress, actual participant acknowledgements and an explicit rooted domain
policy remain open. Keep fleet management default-off. RustFS deployment, Colima
limits, throughput/latency and the soak remain unqualified.

## Next implementation boundary

FO-03 adds the trusted dynamic node set, empty managed host readiness, bounded
dormant registration and complete managed history/birth enrollment. It must keep
storage pins and retired bindings immutable and separate restart/resize exclusions
from evacuation. FO-04 through FO-10 retain their complete planning, telemetry,
reconciliation, lifecycle, API/CLI, scaling and qualification scope.

`node/raft_startup.rs`, `host.rs` and `host_directory.rs` now accept zero groups
only with explicit rooted enrollment and compiled namespace/class declarations;
node controls no longer borrow a first-cell binding. Remote node observations now
bind both physical processes and a minimum installed trust revision, with current
enrollment separate from startup state. The remaining work includes complete
import/birth admission, physical-host placement policy, unknown-node ingress and
binding acknowledgements to the actual movement executor. Current-root trust
refresh, transport revocation and metadata RPCs do not complete that movement
protocol. Dynamic binding mutations now belong to the
serialized host, with bounded reader access and no lock guard crossing an await.
Retained active histories still reopen through startup recovery registrations;
the new metadata path refuses resetting their physical slots. A local observation,
dummy business cell or successful TCP connect cannot substitute for the remaining
enrollment/readiness proof.

## FO-03 progress: admitted birth settlement

Source `deb67f903e230579593205dbedc47a6547e4a91e` fixes initialization retry through startup-recovered handles.
The host validates the actual birth and declared namespace, then observes the
retained original-voter guard without recreating the engine or SQLite image.
The restart case retries the same capability; another archived write and read
verify the accumulated data. An in-flight initializer still cannot return
an attachment receipt early.

A confirmed fresh managed birth can now register its original voters after
rooted drain, restart or resize exclusion. The actual SDK race case holds the
birth writer, excludes all three nodes, confirms the retained inventory and then
initializes the voters and acknowledges a quorum/archive write. An excluded
noninitial spare cannot use the exception. In the opposite root-CAS ordering,
both the delayed and fresh reservation refuse; no birth, dormant binding or
local voter appears. Trusted state changes remain fixture inputs; these cases
do not implement or certify evacuation, capacity activation or process stop.

The final affected native cohort passed **154 cases across seven targets**,
including all **41 enrollment cases**. All **136 Raft unit tests**, affected
Clippy with warnings denied, formatting/layout over 926 Rust files, native
dependency checks (454 packages, no JavaScript engine), plan validation,
10 validator tests and documentation checks passed. The two reproduced failures
remain retained. No Rust bytes changed after qualification.

The ignored partial record is `implementation/fo03/birth-continuation-boundary.json`
under `.cellar/raft-perf/fleet-operations/`; adjacent logs use
`fo03-birth-continuation-*.log`. It records exact source, log and observed executable
hashes; mutable cache paths do not archive the binaries. No raw evidence is
tracked. There is no new TPS/p99 or deployed RustFS/Colima qualification.

FO-03 remains `IN_PROGRESS`: five mandatory cases are still absent
(`fleet_enrollment_unknown_node`, `fleet_enrollment_route_revocation`,
`fleet_enrollment_retired_binding`, `fleet_enrollment_incomplete_import` and
`fleet_enrollment_physical_host`). `check-list FO-03` correctly refuses the
incomplete listing. The acceptance inventory stays at 135 cases and 67 source
anchors; FO-04 through FO-10 remain pending.

## FO-03 progress: group identity admission

Source `c4d2c0ec7b409a136aa4dd663464f20a7e1fc9ba` confirms the namespace's permanent group-ID claim before
fleet admission, then indexes the history, cell name and fleet-wide group ID in
one root CAS. A conflicting claim in another namespace refuses before a history
is admitted. A pre-admission claim left by cancellation or a losing root turn
grants no birth or membership authority; retain it and reconcile the original
request. The immutable namespace claim also prevents a competing catalog from
stealing the ID between confirmation and root admission.

The experimental protocol now ends in `/group-reservations-1`. Fleet heads
require its digest and the permanent genesis pin binds it. Earlier or missing
protocol state refuses initialization and reads. Use fresh fleet roots, fresh
namespaces and matching binaries; numeric markers remain 1. No legacy reader or
state migration was added.

Four new native cases cover aliases across names/incarnations/namespaces/data
prefixes, an existing unmanaged claim, cancellation before root CAS and lost
claim confirmation with exact retry after later revisions and adapter restart.
The catalog's malformed-record case also refuses missing/foreign head protocols.
The duplicate admission was reproduced before its fix; no assertion was weakened.

The final cohort passed **328 native cases across 13 targets**, including all
**45 enrollment cases**, plus **136 Raft unit tests and four startup tests**.
Affected Clippy with warnings denied, format/layout over 926 Rust files,
native dependencies (454 packages, no JavaScript engine), plan validation,
10 validator tests and documentation checks passed. No Rust bytes changed after
qualification. The initial manual native total omitted the preceding birth/drain
case; the retained correction records all 13 summaries totaling 328, zero failed.

The ignored partial record `implementation/fo03/group-reservations-boundary.json`
under `.cellar/raft-perf/fleet-operations/` stores exact source/log/executable
hashes. Adjacent logs use `fo03-group-reservations-*.log`; executable cache paths
do not archive binary bytes. No raw evidence is tracked. These runs do not
qualify RustFS, Colima resource enforcement, fleet automation, TPS/p99 or soak.

The acceptance inventory now contains 139 cases and 67 source anchors. FO-03
remains `IN_PROGRESS` with the same five mandatory cases absent: unknown-node
ingress, move-participant revision acknowledgements, actual retired-binding
lifecycle, complete imports and rooted physical-host policy. The complete FO-03
listing still correctly refuses qualification. FO-04 through FO-10 remain pending.

## FO-03 progress: authoritative namespace births and freeze

Source `2bb08a0f783e051cb6ad1510bbeb2979030db5ad` indexes canonical birth intents before publication in every
new-protocol Raft namespace. Birth decoding requires its indexed intent and
permanent identity claim; validly framed bucket objects alone cannot mint a
`GroupBirth`. Namespace and fleet adapters reuse bounded immutable radix pages
while retaining their own storage, errors and CAS roots. Logical record bounds
remain separate from the fixed 64-KiB index-page limit; paging returns at most
sixteen intents and no path uses bucket listing for authority.

`freeze_births` orders against admission through the namespace root CAS. If
admission won, pending/live/cancelled names remain enumerable from that exact
frozen root and admitted writers may settle. If freeze won, a delayed admission
cannot publish a new birth. Exact pending proposals can be conditionally
cancelled to fence late writers. Lost replies retain the same operation;
missing heads, pins, pages or intents refuse authority and cannot silently
recreate an empty inventory. The genesis metadata is prepared before namespace
publication. The protocol now ends in `/birth-inventory-1`; use fresh experimental
fleet/namespace roots and matching binaries, with markers remaining 1.

Seven new native cases cover both race orderings, delayed cancellation, lost
freeze confirmation/reopen, missing metadata and occupied-root refusal,
140-entry bounded scans under 1-KiB logical record limits, foreign cursors and
untracked forged decisions. The existing three-node/two-cell SDK case now
freezes new births before all its writes, queries and leader failover, which
continue successfully. This qualifies birth quiescence, not membership quiescence.

The final run passed **335 native cases across 13 targets**, including all
**52 enrollment cases**, **136 Raft unit tests and four startup tests**. Clippy
with warnings denied, formatting/layout over 930 Rust files, native dependency
checks (454 packages, no JavaScript engine), plan validation, all 10 validator
tests and documentation checks passed on the final source. No Rust bytes changed
after that qualification. Three existing native fixtures became reviewed source
anchors, bringing the inventory to **146 required cases and 70 anchors**.

Failed compilation, format/lint and native cohorts remain retained. The native
failures identified archive fixtures selecting the new inventory head by its
filename and a directory fixture counting namespace pins as node-storage pins.
The selectors now identify their owning key spaces and assert the separate pin
counts; all corruption, immutability and no-voting-authority assertions remain.
The initial missing-anchor validation was fixed by recording the reviewed input;
the validator was not weakened. The partial record is ignored
`implementation/fo03/import-inventory-boundary.json` under
`.cellar/raft-perf/fleet-operations/`, with adjacent `fo03-import-inventory-*.log`
files and exact source/log/executable hashes. Cache paths do not archive binaries.
No raw evidence is tracked and no new TPS/p99 or deployed capacity claim is made.

**Complete import is not delivered.** Next, integrate membership fencing, exact
fleet history/namespace coverage, ownership handoff and controlled cancellation;
a frozen birth cut grants no removal certificate. FO-03 remains `IN_PROGRESS`
with the same five mandatory cases missing: unknown ingress, movement revision
acknowledgements, retired bindings, complete import and rooted domain policy.
Its full listing correctly refuses qualification. FO-04 through FO-10 remain
pending and automatic fleet management stays default-off.

## FO-03 progress: quorum membership observation

Source `3fcffcf38bc7dc0899445bd35bb6fd5960d9f0bf` adds
`RaftCellHandle::observe_fleet_membership`. Each invocation uses a new safe
ReadIndex and waits for coherent durable apply and the local route revision. The
opaque result binds the actual birth/schema and physical node/disk/process,
retains both joint voter sets and learner roles, and exposes the committed fleet
sequence, joined bindings and repair phase. It needs no SQL image or application
query. An additional bounded 64-KiB/item administrative reservation survives
through SDK reply delivery; cancellation, deadlines and shutdown release it.
Business write acknowledgement adds no catalog or observation call.

All **340 native cases across 13 targets**, **136 Raft library units** and **four
startup-confirmation cases** passed on the existing local macOS arm64 setup.
The permit target now has 12 passing cases, including five new cases covering
empty images, missing enrollment and foreign births, cached-state refusal during
partition and successor recovery, seeding/joint/terminal roles, and bounded
cancellation/pressure/shutdown. Its existing joint-recovery case also refuses new
observations with only the new majority, including after physical reopen, then
observes terminal membership once both majorities recover. Five process helpers
remain ignored in direct listings and run through their passing parent cases.

Clippy with warnings denied, formatting/layout over 932 Rust files, dependency
checks (454 reachable packages and no JavaScript engine), documentation, plan
validation and all 10 validator tests passed. The inventory now has 151 required
cases and 71 reviewed source anchors. Initial test logs retain the incorrect
fixture process generation and skipped command sequence; both were corrected
without weakened assertions. Final qualified Rust hashes are unchanged.

`implementation/fo03/quorum-observation-boundary.json` is a retained **partial**
record under ignored `.cellar/raft-perf/fleet-operations/`. It records exact source,
log and executable hashes; mutable build paths do not archive binary bytes.
No raw evidence is tracked under `plans/multi-raft/evidence/`.

FO-03 remains `IN_PROGRESS`: 52 enrollment cases and five companion observation
cases pass, while its five mandatory complete-import/trust/lifecycle cases are
still missing. The combined listing correctly fails `check-list FO-03`.
A saved group observation supplies no lease, complete fleet inventory, archive
coverage or removal certificate. At that checkpoint, complete-import traversal,
remote/controller integration, ownership handoff/cancellation and typed lifecycle
gating remained required. The namespace-import substep below supplies the
per-namespace handoff. FO-04 through FO-10 stay pending; fleet management remains
default-off.
These results do not qualify RustFS deployment, enforced Colima limits, fleet
scaling, TPS/p99 or the soak, and claim no performance improvement.

## FO-03 progress: rooted namespace import

Source `900a677fb6481a259d1e24073d9dc1aacf785507` implements the per-namespace import
runtime over an exact frozen birth cut. Rooted Preparing, Inventory, Coverage, Handoff and Complete
phases settle every canonical intent, retain cancelled names and require each
live group's opaque safe-ReadIndex observation. Coverage verifies the actual
namespace storage pin, permanent member/disk claims and catalog-scoped committed
guard. Handoff rereads the complete retained witness set; missing metadata refuses
adoption. Bounded turns use no object listing or business SQL image.

Stop/resume retains the same cut, claims and physical references. A late namespace
claim cannot advance a stopped root. Original stored candidates reconcile lost
replies across adapter recreation, including a namespace adoption whose terminal
fleet-root reply is lost. Once Handoff is admitted, cancellation refuses. Only
Complete plus the exact adoption marker admits later managed births.

New births carry their managed owner in the canonical birth record. Three actual
managed hosts can import an empty immutable-unmanaged namespace, admit a later
birth, initialize its guard in the first engine sync, serve native SQL writes and
recover intact after physical restart. Old imported births retain their original
identity and replay their committed enrollment. Catalog-scoped group bindings
prevent another catalog prefix with the same fleet ID from reusing cached guards
or membership permits.

The experimental protocol now ends in `/namespace-import-1`. New required birth
owner, history-origin and catalog-binding fields hard-cut earlier records; use
fresh experimental coordination/data prefixes, namespaces and matching binaries.
`FleetGroupBinding::new` requires the catalog fingerprint; use
`FleetCatalog::group_binding` to obtain the descriptor. Descriptors grant no
authority. Numeric markers stay at 1, without a migration or compatibility reader.

All **349 native cases across 13 targets**, **136 Raft library units** and
**four startup-confirmation cases** passed on macOS arm64/Rust 1.98.1. Enrollment
has 53 passing cases and permits/import has 20, including nine new import/genesis
cases. Five direct-list process helpers run through passing parent cases. Clippy
with warnings denied, format/layout over 938 Rust files, native dependency checks
(454 reachable packages, no JavaScript engine), documentation, plan validation
and all 10 validator tests passed. The manifest retains 160 required cases and
71 reviewed source anchors; this substep advances no task status.

Initial import tests exposed a full-page continuation being mistaken for proof
of another entry and an oversized composed management future overflowing the
ordinary test stack. Two-entry lookahead retains exact count checks; boxing work
at domain adapters keeps ordinary stacks sufficient. A larger-stack diagnostic
is retained separately and is not qualification. No stack override or debug
instrumentation remains in source. Initial Clippy rejected an unnecessary unit
expression; its equivalent arm was corrected before the final snapshot.

An initial full native cohort had two readiness failures in existing enrollment
cases. Both passed in isolation and in the full rerun without source or timeout
changes. Their root cause remains unestablished; retain the failed cohort and
track readiness timing stability during broader qualification. Assertions were
not weakened and no qualified Rust bytes changed after the final snapshot.

The **partial** record `implementation/fo03/namespace-import-boundary.json` under
ignored `.cellar/raft-perf/fleet-operations/` retains source/log/executable hashes,
initial failures and the full rerun. Mutable build-cache paths do not archive
binary bytes. No raw evidence is tracked under `plans/multi-raft/evidence/`.

FO-03 remains **IN_PROGRESS**. The combined enrollment/permit listing still
correctly refuses five missing mandatory cases: `fleet_enrollment_unknown_node`,
`fleet_enrollment_route_revocation`, `fleet_enrollment_retired_binding`,
`fleet_enrollment_incomplete_import` and `fleet_enrollment_physical_host`.
Per-namespace Complete is not fleet-wide compiled-namespace coverage, capacity
activation, current quorum health, archive coverage or a node-removal receipt.
Remote/controller integration and lifecycle gates remain required. FO-04 through
FO-10 stay pending; automatic fleet management remains default-off. These local
checks make no RustFS/Colima/resource-limit, TPS/p99 or soak qualification claim.

## FO-03 progress: fleet namespace registry and inventory

Source `d98ca0fe98f0755006b565e197548c6092280341` adds an append-only paged registry of exact namespace,
class, schema and data-root identities. Managed startup checks actual prepared
catalogs and compiled classes against it before pinning the engine or publishing
an endpoint. History enrollment, import and new managed-birth admission require
the same declaration. Distinct hosts can support different declared subsets;
capability digests do not bypass the registry.

The fleet head permanently retains the namespace count. Missing registry/index
state, deletion and count regression refuse admission rather than creating an
empty replacement set. At most 64 namespaces are declared. Original immutable
candidates reconcile lost replies after adapter recreation and later additions;
changed identities, classes or data roots cannot replace a declaration.

`check_inventory` reads every rooted declaration and verifies the exact supplied
catalog handles. Each imported namespace must have completed handoff and retain
its witness pages and permanent claims. Historical node tombstones remain valid
references; this is not a new health observation. The checker then streams the
current canonical birth indexes, including post-import births, and all fleet
histories in both directions. It checks exact counts, identity indexes, initial
node references, permanent group claims and immutable decisions. Reservations
before intent publication and pending confirmations retain their references.
Offline endpoints and cancelled births cannot disappear from inventory.

The experimental protocol now ends in `/fleet-namespaces-1`; the fleet head's
namespace count is mandatory. Use fresh experimental fleet/data prefixes,
namespaces and matching binaries, without a compatibility reader or migration.
Numeric markers remain 1. Existing business write acknowledgement adds no registry
or inventory call. See the [registry contract](../../../crates/cellar/durability/raft/fleet_catalog/namespaces/README.md).

**356 distinct native cases passed across 13 targets**: the first eleven complete
targets passed 295 cases; a complete snapshot/storage rerun passed another 61
(snapshot 44, storage 17). Enrollment has 55 passing cases and permits/import has
25. Five process helpers remain ignored in direct listings and run through their
passing parent cases. **136 Raft library units** and **four startup-confirmation
cases** also passed. Clippy with warnings denied, format/layout over 944 Rust
files, native dependency checks (454 packages, no JavaScript engine), documentation,
plan validation and all 10 validator tests passed. The manifest retains 167
required cases and 71 reviewed anchors; this substep advances no task status.

Seven new cases cover complete namespace coverage, declaration retries/scope,
exactly full canonical pages with offline/cancelled histories, the 64-namespace
bound, registry no-reset behavior, compiled-host refusal/intact resume and pending
birth confirmations. Existing post-import genesis/restart and 141-intent cases
also use the global checker. Missing retained coverage is tested again after
import completion. Fixture node ownership and cloned-reference lint errors were
corrected without weakened assertions or suppression. A pre-fence run was
intentionally interrupted after Clippy so review could add the permanent count
and no-reset test; that native run is not counted as qualification.

The initial full native cohort had one failure: `missing_quorum_restore_refused`
returned `Ambiguous` on its first setup write, before quorum removal. It passed
isolated unchanged in 2.72 seconds and then in the full 44-case snapshot rerun.
The timing root cause remains unestablished. The failed cohort is retained; the
successful target scopes are reported separately rather than relabeling the
initial cohort as passed. Earlier enrollment readiness instability is not claimed
resolved by these checks. No qualified Rust bytes changed after the final snapshot.

The **partial** record `implementation/fo03/fleet-registry-boundary.json` under
ignored `.cellar/raft-perf/fleet-operations/` retains exact source, log and executable
hashes, run provenance, failures and interrupted attempts. Mutable build-cache
paths do not archive binary bytes. No raw evidence is tracked under
`plans/multi-raft/evidence/`.

FO-03 remains **IN_PROGRESS**. This checked inventory is not a capacity, current
quorum, archive, empty-node or removal receipt. Typed capacity and maintenance
operations must still consume a revalidated exact root and establish their own
proofs and physical-work joins. The same five mandatory FO-03 cases remain absent;
FO-04 through FO-10 remain pending and automatic fleet management stays default-off.
No RustFS/Colima/resource-enforcement, TPS/p99 or soak claim is made.

## FO-03 progress: current learner admission and retired bindings

Source `45b4924344028f2716d3064165c56339c764b23c` replaces startup-cached learner eligibility with a fresh
bounded host/history authorization read. It checks the actual engine pin,
namespace and history binding before initializing a fresh member. Registered to
Active can receive a replica without restarting; startup Active cannot bypass a
current Draining exclusion. The metadata read uses the original seed deadline
and an independent 4 MiB control reservation, released before bulk seeding.
An existing synced member still needs its exact committed permit to resume after
exclusion. Business-write acknowledgement adds no catalog read.

`fleet_enrollment_retired_binding` exercises actual peer seeding, joint consensus
and native SQL. It physically stops an outgoing voter before final removal,
finishes with the surviving joint majorities, then reopens that stale intact
engine without a cell registration. Both metadata registration and physical
bootstrap refuse a fresh member identity, retaining the original slot and guard.
The failed learner is aborted and its ID stays consumed. A distinct enrolled
engine receives a newer member ID through archive seeding and serves verified
native writes and reads. Retirement/reopen and replacement-host stops verify
physical Raft joins. Privileged catalog fixture mutations stand in for the pending
reconciler; this is not an evacuation/removal certificate.

**357 native cases passed across 13 complete targets**, including 56 enrollment
and 25 permit/import cases. Five direct-list process helpers run through their
passing parent cases. **136 Raft library units** and **four startup-confirmation
cases** also passed. Clippy with warnings denied, format/layout over 945 Rust
files, native dependency checks (454 packages, no JavaScript engine), documentation,
source preflight, plan validation and all 10 validator tests passed. The manifest
retains 167 required cases and 71 reviewed source anchors; no package status
advances in this substep.

The new test consistently reproduced learner admission refusal before seeding.
Targeted diagnostics showed startup Registered alongside current Active; refreshing
rooted authorization corrects that stale cache and tests the inverse exclusion.
Reopened-host discovery also exposed a temporary old-endpoint Unreachable result;
the existing bounded fixture retry now handles it while still requiring a valid
response at the requested trust revision. A now-redundant catch-all arm was removed
before qualification. Initial integration import/context errors and diagnostic
failures remain retained. No assertion, timeout or lint was weakened; no stack
override or debug instrumentation remains. Earlier readiness/setup timing failures
remain unresolved and are not claimed fixed by this qualification.

The **partial** record `implementation/fo03/retired-binding-boundary.json` under
ignored `.cellar/raft-perf/fleet-operations/` retains exact source, log and executable
hashes. Mutable build-cache paths do not archive executable bytes. No raw evidence
is tracked under `plans/multi-raft/evidence/`. This substep changes no wire format.

FO-03 remains **IN_PROGRESS**. The combined enrollment/permit listing still
correctly refuses four missing mandatory cases: `fleet_enrollment_unknown_node`,
`fleet_enrollment_route_revocation`, `fleet_enrollment_incomplete_import` and
`fleet_enrollment_physical_host`. Complete inventory consumption in typed
capacity/maintenance admission, move-participant trust acknowledgements and
explicit failure-domain policy remain required. FO-04 through FO-10 remain
pending. Automatic fleet management stays default-off; no deployed RustFS/Colima,
resource-enforcement, TPS/p99 or soak qualification is claimed.

## FO-03 progress: catalog-scoped move admission

Source `b5d25b4b0425d2db673f043fe68585ea990c29b5` gates a fresh managed repair on authenticated trust
acknowledgements from its actual source and target processes. The opaque
membership authorization retains its source root revision. The existing metadata
RPC requires both processes to install at least that cut; the source revalidates
the target process, disk, engine and epoch before running the configuration and
committed-guard checks again. Already committed exact plans continue without a
new acknowledgement, including when the target is unreachable.

The preflight uses the original deadline and a 64 KiB control reservation.
Retries follow directory polling, capped at 250 ms between attempts. Cancellation,
expiry, local drain and shutdown refuse an unsubmitted action. Physical RPC work
is joined before releasing its reservations; an unpolled SDK waiter retains its
separate 4 KiB request reservation until delivery. Refusal does not consume the
permit or release the root reservation. Business-write acknowledgement adds no
catalog or observation call.

A deterministic per-host catalog gate reproduced the missing admission boundary:
the prior path returned Seeding while the target still had the older trust cut.
The native regression checks no learner/sequence consumption while blocked,
continued native SQL, cancellation, expiry, rejection above the existing
60-second SDK bound, eventual admission, idempotent continuation and abort with
a stopped target, and joined shutdown of an unpolled
60-second attempt while its unresolved root reservation remains retained.

Further review found actual cross-catalog acknowledgement confusion. Two hosts
using distinct coordination prefixes could share a fleet ID, namespace storage,
capability digest, physical records, epochs and sufficient revision numbers.
A counted TCP proxy reproduced an accepted reply from the other catalog. Trust
snapshots, reuse/successor checks and authenticated node routes now bind the exact
catalog fingerprint. The regression requires an actual connection to the foreign
process and a signed refusal; missing discovery cannot satisfy it. Unit checks
also reject foreign trust installation/reuse and absent or zero route scope.

`fleet_enrollment_unknown_node` publishes a real unrooted storage pin/process
view, then sends correctly authenticated metadata and Control/Data frames for
unknown and dormant histories. Signed refusals arrive before body admission;
rooted nodes, engine groups, dormant metadata and application callbacks remain
unchanged. No listing is used to expand trust.

The experimental protocol hard-cuts to `/catalog-trust-1`. The catalog fingerprint
is mandatory in node metadata routes. Use fresh experimental fleet/data prefixes,
namespaces and matching binaries. Numeric format markers remain 1; no migration
or compatibility reader is provided.

**360 native cases passed across 13 complete targets**, including 59 enrollment
and 25 permit/import cases. Five direct-list process helpers run through their
passing parent cases. **136 Raft library units** and **four startup-confirmation
cases** passed. Clippy with warnings denied, format/layout over 949 Rust files,
native dependency checks (454 packages, no JavaScript engine), documentation,
source preflight, plan validation and all 10 validator tests passed. The manifest
retains 169 required cases and 71 reviewed source anchors; no package status
advances in this substep.

Initial fixture compile failures, the reproduced early-consumption and accepted
foreign-catalog replies, and intermediate failed fixture checks remain retained.
The shutdown fixture was corrected to distinguish the SDK's intentional 4 KiB
waiter retention from joined physical work; raw Raft probes now use the separate
transport protocol digest. No guard, membership, refusal or timeout assertion was
weakened. No stack override or debug instrumentation remains. The first cohort
was deliberately interrupted after Clippy and its two focused cases to add exact
catalog scoping; its partial native run is not counted as qualified. A second
attempt was stopped after Clippy to test a suspected deadline mismatch; that hypothesis was disproved
by the existing SDK rejection of managed timeouts above 60 seconds. The final
case retains that boundary check; no production timeout behavior was changed.
Earlier readiness/setup timing instability remains unresolved.

The **partial** record `implementation/fo03/catalog-trust-boundary.json` under
ignored `.cellar/raft-perf/fleet-operations/` retains exact source/log/executable
hashes and run provenance. Mutable build-cache paths do not archive executable
bytes. No raw evidence is tracked under `plans/multi-raft/evidence/`.

FO-03 remains **IN_PROGRESS**. Its three absent mandatory cases remain
`fleet_enrollment_route_revocation`, `fleet_enrollment_incomplete_import` and
`fleet_enrollment_physical_host`. Fresh-move trust acknowledgement is implemented;
complete route/process revocation with physical joins, typed inventory-consuming
lifecycle admission and rooted failure-domain policy still require delivery and
qualification. FO-04 through FO-10 remain pending. Automatic fleet management
stays default-off; no deployed RustFS/Colima, resource-enforcement, TPS/p99 or soak
qualification is claimed.

## FO-03 progress: physical revocation and successor completion

Source `1a6dc10e691fa10c292ff88e883692dfc2e03fec` adds the complete native route/process revocation
case. Its managed learner retains the same engine after a joined SDK shutdown,
resumes under a new process during RestartPending, catches up verified native SQL,
and completes joint promotion. All original peers acknowledge the new process.
A blocked request to the previous process closes while its server remains live;
the old publisher cannot replace the new directory view.

An empty peer preserves an already admitted transport call through RestartPending,
ResizePending, Draining and Evacuated. Removed closes and joins physical I/O and
releases metadata reservations before the SDK waiter is consumed. A valid late
advisory directory publication cannot restore rooted trust. The case also runs
the actual source/target trust-acknowledgement scenario and joins all owning SDK
nodes. Catalog state/permit edits remain privileged fixture inputs, not operator
evacuation, removal or capacity-activation certificates.

The first broader run found a repair-completion race. A new proposal had been
accepted while its waiter still observed the previous Complete repair. The
waiter returned CommandConflict; a subsequent safe quorum read proved the exact
new permit had committed Seeding at position 7. The conflict decision now waits
until the accepted position applies. The existing retirement case reproduced the
failure both in the cohort and alone; operation-specific error context identified
the second Begin. Tagged diagnostics were removed before final qualification.
The fix introduces no retry or reclassification of an error as success.

**361 native cases passed across 13 complete targets**, including 60 enrollment
and 25 permit/import cases. Five direct-list process helpers run through their
passing parents. **136 Raft library units** and **four startup-confirmation cases**
passed. Clippy with warnings denied, format/layout over 951 Rust files, native
dependency checks (454 packages, no JavaScript engine), documentation, source
preflight, plan validation and all 10 validator tests passed. The plan retains
169 required cases and 71 reviewed source anchors. This is a fresh complete
cohort; the prior source's 360-case record remains separately retained.

Initial compilation and ordinary-stack assembly failures remain retained. The
scenario boundaries use boxed futures; no stack override, timeout extension,
assertion weakening, lint suppression or debug instrumentation remains. The first
qualification attempt stopped after its enrollment failure and is not counted as
a passing cohort. Earlier readiness/setup timing instability remains unresolved;
the demonstrated successor-completion race is a separate finding.

The **partial** record `implementation/fo03/route-revocation-boundary.json` under
ignored `.cellar/raft-perf/fleet-operations/` retains exact source/log/executable
hashes and the failed-attempt provenance. Mutable build-cache paths do not archive
executable bytes. No raw evidence is tracked under `plans/multi-raft/evidence/`.
This substep changes no wire format; the experimental protocol still ends in
`/catalog-trust-1`.

FO-03 remains **IN_PROGRESS**. Its two absent mandatory cases are
`fleet_enrollment_incomplete_import` and `fleet_enrollment_physical_host`.
Typed lifecycle admission must consume a revalidated complete inventory and
operation-specific live proofs. Rooted node-versus-physical-host placement policy
must apply to later movement, beyond existing birth checks. FO-04 through FO-10
remain pending. Automatic fleet management stays default-off. No deployed
RustFS/Colima, resource-enforcement, TPS/p99 or soak qualification is claimed.

## FO-03 progress: rooted placement and evacuation admission

Source `67e3d6f4d2193f87ea7db122ac832b53c2c3d8b1` records explicit `PhysicalHosts` or `Nodes` replica
separation. The default requires distinct physical hosts. The head retains the
current choice and the genesis pin its initial choice; incompatible initialization,
missing policy fields and unknown values refuse. `Nodes` permits an explicitly
colocated fleet without claiming independence from loss of a shared host.

Managed birth admission uses that rooted policy. A fresh managed repair checks
the actual durable configuration plus its proposed learner after source/target
trust acknowledgements. Rooted physical-host identities supply separation;
advisory regions, zones and process labels do not. A refusal consumes no member
or permit sequence and leaves the root reservation retained. Exact committed
plans still reconcile after exclusion. Native coverage fences the unused permit,
then performs a legal replacement through peer seed, joint completion and S3 SQL;
the explicit colocated policy also completes real RF3 birth and replacement.

`begin_evacuation` now consumes complete inventory inside an exclusion-admission
turn. A single root CAS records the canonical operation projection, raises one
Active node's epoch to Draining and claims the disruption slot. The head retains
a digest of that slot, so missing data and generic deletion/reset cannot create
an apparently free slot. The immutable catalog-scoped operation-request object
binds operation ID, mutation identity, principal, original revision and node
before root CAS. Its binding survives a losing candidate. Admitted retries use
the journal and preserve the original applied revision, including after a later
incomplete namespace is declared. Admission reads verify canonical and journal
linkage; historical counts and node epochs grant no lease or removal authority.

Six native lifecycle cases cover authenticated observation of exclusion, refusal
of fresh target placement, continued S3 SQL, omitted/unadopted namespaces, missing
canonical history, a held CAS losing to a concurrent declaration, permanent
operation identity, lost replies through a recreated adapter, missing request
bytes, generic slot-deletion refusal, racing disruptions, cancellation and the
original deadline. Metadata and owning SDK nodes are joined. Catalog fixture
state inputs remain privileged; this is not capacity-activation qualification.

**369 native cases passed across 13 complete targets**, including 67 enrollment,
12 catalog and 25 permit/import cases. Five directly ignored process helpers run
through their passing parent cases. **136 Raft library units** and **four startup
confirmation cases** passed. Clippy with warnings denied, formatting/layout over
955 Rust files, native dependency checks (454 packages, no JavaScript engine),
documentation, source preflight, plan validation and all 10 validator tests
passed. The plan retains 176 required cases and 71 reviewed source anchors.

The first filtered two-target run passed its 12 catalog cases and 59 enrollment
cases, with seven enrollment cases excluded by the filter and one fixture
failure: it tried to register a new dormant history after exclusion. The fixture
now registers before exclusion and verifies that a subsequent call refuses.
No production guard was relaxed. A red regression also reproduced operation-ID
reuse reaching inventory checking after a losing CAS; the permanent request
binding corrects that gap. Initial and intermediate logs remain retained. No
assertion, timeout or lint was weakened, and no stack override or debug probe
remains. Earlier readiness/setup timing instability remains unresolved.

The **partial** record `implementation/fo03/lifecycle-placement-boundary.json`
under ignored `.cellar/raft-perf/fleet-operations/` retains exact source, log and
executable hashes. Build-cache paths do not archive executable bytes. No raw files
are tracked under `plans/multi-raft/evidence/`. The protocol hard-cuts to
`/fleet-admission-1`: use fresh experimental fleet/data prefixes, namespaces and
matching binaries. Numeric markers stay at 1, with no compatibility reader or
migration. New head/pin fields and canonical request linkage are mandatory.

FO-03 remains **IN_PROGRESS**. The remaining absent mandatory case is
`fleet_enrollment_incomplete_import`, whose oracle is terminal removal
certification, beyond exclusion admission. Capacity activation/readiness and
terminal lifecycle integration remain required. This increment moves no replicas
automatically, stops no process, certifies no node absence, marks no node
Evacuated/Removed, releases no disruption or membership reservation, and
reclaims no storage. FO-04 through FO-10 remain pending and fleet automation
stays default-off. RustFS/Colima limits, TPS/p99, full scaling workflows and soak
qualification remain unproven.

## FO-03 progress: remote quorum observations

Source `cc27382774f7acf4d589dfbe7de4fe3620019d43` adds
`RaftCells::observe_peer_fleet_membership` for an empty coordinator. The target
independently reads a rooted history at least as new as the requested trust cut,
checks its actual birth/schema, performs safe ReadIndex, and captures a coherent
configuration and publication sample. The opaque response binds the exact
original or joined member, node, disk, process and engine. Retained results expose
the complete invocation age through SDK delivery and supply no lease.

A distinct RPC purpose and TLS exporter authenticate catalog, scope, history,
nonce and both physical endpoints. Both peers retain metadata reservations;
both hosts retain response reservations. EOF, caller cancellation, the original
deadline, physical revocation and shutdown cancel and join nested SDK work.
No dormant registration, local replica, SQL image or business query is required
at the coordinator. Rooted node trust creates no seeding or membership-mutation
grant. The existing write acknowledgement path calls none of this metadata work.

Five focused native cases passed before final formatting. They cover image-free
original members, retained response age, dormant refusal, an injected older root,
actual held target catalog IO with continued native S3 SQL, minority refusal,
cancellation, shutdown, learner binding, joint roles and stable replacement.
The initial fixture used the unmanaged/colocated constructor and correctly failed
managed birth reservation before reaching the RPC; it now uses declared managed
births on distinct hosts. No assertion or timeout was weakened.

The first broad attempt failed an existing host cache-pressure case with
`node is closing` before Raft startup. A second attempt failed archive setup
with `node authority was not established before startup deadline`. Three exact
unchanged-binary cache replays passed. Temporary fixture tracing then produced
20 passing exact replays and a complete passing 29-case host target without
reproducing either startup failure. The fixture is restored byte-for-byte;
these diagnostic runs do not replace final source qualification. No production
logic, lease setting, assertion or deadline was changed, and the startup cause
remains unresolved. Both failed attempts and the probe logs are retained.

Final source qualification passed **374 native cases across 13 complete targets**,
including 72 enrollment, 12 catalog and 25 permit/import cases. Five process
helpers remain ignored in direct listings and run through passing parents.
Another **140 Raft library tests and four startup-confirmation cases passed**.
Affected-target/lib Clippy with warnings denied, formatting/layout over 961 Rust
files, native dependencies (454 reachable packages, no JavaScript engine),
documentation, source preflight and all 10 plan-validator tests passed.

Nineteen changed Rust files stayed identical from final formatting through the
successful qualification matrix and match the committed source. Temporary
fixture tracing was restored exactly before that matrix. The **partial** record
`implementation/fo03/peer-quorum-boundary.json` under ignored
`.cellar/raft-perf/fleet-operations/` retains source, log and executable hashes,
including both earlier failed attempts. Mutable build-cache paths do not archive
executable bytes. The repeated green matrix does not establish the cause of the
earlier startup failures.

The plan retains 181 required cases and 71 reviewed source anchors.
The protocol ends in `/fleet-quorum-1`;
use fresh experimental coordination/data prefixes, namespaces and matching
binaries. Numeric markers stay at 1, with no compatibility reader or migration.
No raw evidence is tracked under `plans/multi-raft/evidence/`.

FO-03 remains **IN_PROGRESS**. The missing mandatory
`fleet_enrollment_incomplete_import` case still requires the actual removal
certification boundary. Remote quorum observations are inputs to that verifier,
not complete inventory, archive coverage, physical-work joins or terminal
Evacuated/Removed receipts. Capacity activation, automatic placement, the
reconciler, operator workflows, scaling and deployed qualification remain open.

## FO-03 progress: coherent publication cuts

Source `ef6cca13d15864e25235f66c5f0fe04f84447c54` adds opaque `FleetPublishedCut` and
`FleetPublishedImage` values to authenticated remote quorum observations. A
single cache read captures the durable apply marker, exact published image,
configuration and physical member. A pending publication yields no cut; a cut
with no image proves canonical empty state. Ordinary status inspection cannot
transport this stronger statement.

Fixed-cut comparison covers configuration-only progress without requiring a
new SQL image. Intact restore may publish the same image at a later protocol-only
position. Equal image state can cover that alias; changed data must follow the
entire minimum applied cut. Different catalog/history or membership revisions
refuse comparison. A name-only snapshot handoff remains pending until the exact
durable and published markers match.

The final source matrix passed **376 native cases across 13 complete targets**,
including 74 enrollment, 12 catalog and 25 permit/import cases. Another **145 Raft
library tests and four startup-confirmation cases passed**. Five directly ignored
process helpers execute through passing parents. Affected-target/lib Clippy with
warnings denied, format/layout over 965 Rust files, native dependencies,
documentation, source preflight and all 10 plan-validator tests passed. The plan
retains 183 required cases and 71 reviewed source anchors.

Two new native cases verify configuration-only permit fencing, all three
survivors' coverage, full intact-cluster restart and later S3 writes, plus a held
real private-SQL callback whose uncommitted image never enters the cut. After
release, S3 acknowledgement, publication position and the actual native SQL
result agree. Four new cut units and one wire unit cover prefix conflicts,
canonical empty versus pending, exact markers, restore aliases, strict bounded
encoding and ordinary inspection's inability to supply a cut.

The first private-SQL fixture waited for sequence 1, but the existing gate holds
sequence 2. It timed out before reaching the observation. The corrected fixture
performs its initial sequence-1 write and then holds sequence 2. No production
guard, assertion or deadline was weakened. Earlier startup/readiness failures
remain unexplained; this increment claims no startup fix.

Sixteen changed Rust files stayed byte-identical through final qualification and
match the committed source. The **partial** record
`implementation/fo03/published-cut-boundary.json` under ignored
`.cellar/raft-perf/fleet-operations/` retains source/log/executable hashes and the
failed initial fixture log. Mutable build-cache hashes do not archive executable
bytes. No raw files are tracked under `plans/multi-raft/evidence/`.

The experimental protocol now ends in `/fleet-quorum-1/fleet-cuts-1`; use fresh
fleet/data prefixes, namespaces and matching binaries. Numeric markers remain 1,
with no compatibility reader or migration.

FO-03 remains **IN_PROGRESS**. These historical cuts certify neither current
health nor archive payload/replay coverage. Canonical-empty archive proof and
the actual terminal verifier remain required; the missing mandatory case is
still `fleet_enrollment_incomplete_import` at removal certification. Capacity
readiness, automatic placement/reconciliation, maintenance/operator commands,
scaling and deployed RustFS/Colima/performance/soak gates remain open. Fleet
automation stays default-off, with no performance improvement claim.

## FO-03 progress: canonical-empty archives

Source `886f85df887a904b402478b476a0745329761bc1` adds explicit canonical-empty archives for
cold groups. A cut captures the actual durable configuration, membership/fleet
guard, scope and birth/schema binding while proving the durable image is absent.
A missing publication of applied data refuses capture rather than becoming
empty. Empty captures retain bounded membership data without copying log tails.
The existing bounded producer scheduler now publishes cold resident groups.

The manifest requires explicit null snapshot-image, snapshot-object and final
image fields. Empty state forbids tails, dependencies and position divergence;
half-present state or zero-TXID images refuse. No SQLite image or payload object
is fabricated. `restore_state` returns `None` only for validated canonical empty
state and never deletes an existing generation. Image-only `restore` refuses it.
The first real SQL image establishes a complete snapshot; an older empty cut
cannot replace newer archived data. Mutable-head reconciliation and immutable
exact-reference reads retain their existing conditional-write contracts.

Authenticated S3 seeding uses the same live-quorum, learner, process, history,
engine and fleet-permit checks. An empty seed installs the existing empty snapshot
representation without an application callback. No archive value supplies voting,
activation, reservation-release or terminal-removal authority.

The final frozen-source matrix passed **382 native cases across 13 complete
targets**, including 56 archive, 75 enrollment and 46 snapshot cases. Another
**147 Raft library tests and four startup-confirmation cases passed**. Five ignored
process helpers execute through passing parents. Affected-target/lib Clippy with
warnings denied, formatting/layout over 968 Rust files, native dependencies,
documentation, source preflight and all 10 plan-validator tests passed. The plan
retains 189 required cases and 73 reviewed source anchors.

Six new native cases cover lost conditional-head replies, retained empty
references, first-image transition, refusal to erase live data, two-empty and
mixed batches, cold S3 seed/first write, actual minority refusal without target
group or member claim, and a managed cold repair through S3 seed, joint consensus
and the first native durable SQL command. Two new manifest units enforce strict
null-field presence and refusal of contradictory/payload-bearing empty states.
The managed test uses privileged rooted capacity/permit fixture inputs; it does
not qualify the pending controller or capacity-readiness adapter.

The first fixture compile passed borrowed paths to `GenerationStore::open`,
which requires owned paths. Both new paths were corrected. Six focused native
cases then passed before formatting; the cold-minority fixture was strengthened
before final qualification to inspect the actual peer attempt, physical engine
and absent member claim. No production guard, assertion, deadline or lint was
weakened. The first full frozen matrix passed 373 native cases and failed nine
across six targets. Three fixture assumptions accounted for four case executions:
producer/close checks observed archive IO before their actual SQL write, and the
bounded-report fixture assumed cold histories could never archive. All three
reproduced unchanged. Corrected fixtures require the actual published SQL cut
before close, or an explicit archive outage while preserving zero callbacks and
all four lagged histories. Their focused checks passed. Five other timing/readiness
cases passed unchanged individual replays; their causes remain unexplained. No
startup, timing or performance fix is claimed.

A second attempt passed 227 native cases and failed two, with six targets never
executed because their files were absent. All 13 expected native executable paths
were later confirmed missing while the target directory remained present with
ample free space. The cause is unknown. The managed cold test also repeated a
broad-only `NotLeader` result; permanent phase context was added without changing
source selection, retries or deadlines. A new task-isolated build retained both
executables before a passing full 75-case enrollment target and exact host-parity
case. Neither failure is claimed fixed by changing the build directory.

Fourteen changed Rust files stayed byte-identical through qualification and match
the committed source. The **partial** record
`implementation/fo03/empty-archive-boundary.json` under ignored
`.cellar/raft-perf/fleet-operations/` retains source/log/executable hashes and the
initial fixture failure and both broad attempts. The final run retains actual
executable bytes copied before execution, with verified hashes and build inputs,
outside the mutable target directory. Earlier attempts retain their stated
artifact limitations. No raw files are tracked under `plans/multi-raft/evidence/`.

The protocol ends in `/fleet-cuts-1/empty-archives-1`; use fresh experimental
fleet/data prefixes, namespaces and matching binaries. Numeric markers stay at
1 with no compatibility reader or migration.

FO-03 remains **IN_PROGRESS**. Next bind exact selected archive references and
physical verification into an owned terminal verifier alongside fresh survivor
cuts, complete inventory, settled permits and physical-work joins/fences. The
mandatory `fleet_enrollment_incomplete_import` removal-certification case remains
absent. Capacity readiness, placement/reconciliation, maintenance/operator APIs,
scaling and deployed RustFS/Colima/performance/soak qualification remain required.
Fleet automation stays default-off.

## Main integration: cold archive recovery

Source `181d2dba679459cfe4c4b869b4366db5c0b3de85` merges main
`906eb36a7a2ad2a8687310adcac880d8f4f0e0cc`. Archive manifest and replay conflicts
were resolved while retaining explicit canonical-empty state and main's configured
image/decode bounds: 64 MiB default, opt-in up to 256 MiB. These size profiles do
not qualify capacity or recovery latency at 1-vCPU/3-GiB limits.

The merged source passed **382 native cases across 13 complete Raft targets**,
**12 cases across three typed-RPC integration targets**, **151 Raft library
cases**, and **4 startup-confirmation cases**. Five directly ignored process
helpers run through passing parents. Affected-target/lib Clippy with warnings
denied, formatting and layout over 1,091 Rust files, native dependency checks,
documentation, plan validation and all 10 plan-validator tests passed. All
1,163 tracked Rust files and captured Cargo/configuration inputs stayed identical
through the run. All 35 compiled executable files were copied and hash-checked
before the test run; integration, unit and startup suites executed from retained
copies. The partial record is
`implementation/fo03/empty-archive-main-boundary.json` under ignored
`.cellar/raft-perf/fleet-operations/`.

Source anchors are refreshed to this merged source. Earlier failed attempts and
their limits remain recorded above; a passing integration run establishes no
startup fix, TPS improvement or deployed RustFS/Colima qualification. FO-03 stays
**IN_PROGRESS**, with `fleet_enrollment_incomplete_import` still missing at actual
removal certification. The next implementation boundary remains exact archive
verification, terminal survivor proof/receipt/slot release, then capacity readiness.

## FO-03 progress: physically verified archive coverage

Source `e083e6a2c58b245cd378a189351aff0c0776d488` adds
`CellArchive::verify_published_at(reference, minimum, verifier, timeout)` and the
opaque `VerifiedArchiveCoverage`. Exact immutable reads, payload hashes, complete
prefix coverage and physical replay must all pass before a proof is returned.
Canonical-empty archives use the same gate without fabricating SQL state.
Missing selected material never substitutes a newer head. Configuration changes
require a new attempt, and failed replay quarantines the scratch owner.

Manifests now require the producer's `image_position`, independently of the
archive applied/snapshot position. Full snapshots preserve that publication origin;
tails cannot place it before their last data entry. Cold state uses explicit null.
Legitimate producer/restart aliases remain comparable by full image state and
configuration. The request's original deadline spans object IO, workspace wait,
physical work and delayed delivery. A waiter that expires behind another replay
refuses before touching scratch state. Blocking work retains its inputs through
its execution owner. No business-write acknowledgement calls this verifier.

Four new native cases exercise cold/first/later data, retained references,
configuration changes, replay-capacity refusal/quarantine, full intact restart,
held payload IO, unpolled delivery, missing selected references and exact producer
origin preservation across protocol-only progress and recovered aliases. The
fleet fixture supplies privileged capacity/permit inputs; this is not operator
or controller qualification. A manifest unit adds required/bounded-origin cases.

The frozen source passed **386 native Raft cases across 13
complete targets**, **12 typed-RPC cases**, **152
Raft unit cases**, and **4 startup cases**. Five directly
ignored process helpers execute through passing parents. Affected-target/lib
Clippy, formatting/layout over 1,094 Rust files, native dependencies, docs, source
preflight, plan validation and all 10 plan-validator tests passed. The plan retains
193 required cases and 74 reviewed source anchors. All 1166 tracked
Rust files and captured build inputs stayed identical through qualification.
The 35 compiled executable files were retained before the
run; integration/unit/startup suites executed from those copies. The partial
record is `implementation/fo03/archive-coverage-boundary.json` under ignored
`.cellar/raft-perf/fleet-operations/`.

An initial unit build failed because its direct archive fixture omitted the new
deadline field; explicit `None` repaired that fixture. A new fleet assertion then
incorrectly equated different producers' publication origins: the observed image
was identical at origins 3 and 4. The failed binary and diagnostic replay are
retained. Exact-origin assertions moved to the real capture boundary; fleet
coverage retains strict image/configuration checks and now exercises intact
restart aliases. Earlier timing/readiness failures remain unresolved. No startup
fix, enforced-resource qualification, TPS gain or latency gain is claimed.

The experimental protocol ends in `/empty-archives-1/archive-cuts-1`; use fresh
fleet/data prefixes, namespaces and matching binaries. Markers stay at 1 with no
legacy reader or migration. FO-03 remains **IN_PROGRESS**. Consume these archive
proofs in terminal survivor verification, immutable receipts and disruption-slot
release; close `fleet_enrollment_incomplete_import` at actual removal certification.
Capacity readiness and FO-04 through FO-10 remain required. Automation stays off.

## FO-03 progress: inventory-consuming evacuation candidates

Source `f130ed3ef58f05d3423a00d88d850af9c3d47fe7` connects complete rooted inventory to
`FleetEvacuationVerification`. Preparation checks the current root, canonical
active operation/journal, exact Draining node and every declared namespace/history.
Each pending live history owns three authenticated safe-ReadIndex observations,
checks stable RF3, current survivor identities/engine pins, the exact settled
latest permit and rooted failure-domain separation, then physically verifies the
selected immutable archive. Voter comparisons use logical sets because raft-rs
ConfState vectors have no ordering guarantee.

`freeze_live` retains one fixed historical publication cut while ordinary writes
and archive catch-up continue. Repeated replay attempts keep that boundary.
Reserved births block; canonical cancellations are consumed explicitly. A bounded
immutable receipt index and digest-addressed candidate manifest cover every
rooted history. Rooted-input changes invalidate the attempt. Unrelated Operations
progress is allowed, while a missing or corrupted original operation refuses.
Candidate publication confirms the final index page and leaves node state, the
root and disruption ownership unchanged. Stored bytes cannot recreate proofs
or supply terminal removal/stop authority after a crash.

Five new native cases cover complete inventory and stable retries, failed
archive reads without progress, unconsumed permits, reserved/cancelled births,
missing imports, fixed cuts across later S3 writes, operation corruption, and
real S3 learner seeding through joint-to-stable voter replacement. The departing
original voter retains physical history; the three survivors continue writes
and queries. Capacity/permit metadata remains privileged fixture input.

Frozen-source qualification passed **223 native cases across five complete
targets**: enrollment 83, catalog 12, permits 25, archive 57 and snapshot 46,
plus **152 Raft unit cases**. Two snapshot process helpers remain ignored in
direct listings and execute through their passing parent crash matrices. Lib and
affected-target Clippy with warnings denied, formatting/layout over 1,098 Rust
files, native dependencies (465 packages, no JavaScript engine), documentation,
plan validation and all ten validator tests passed. All 1170 Rust files and
captured Cargo inputs stayed unchanged and match committed Git blobs. Six test
executables were retained and hash-checked. This is an incremental affected-scope
run; the earlier 386-case/13-target matrix remains qualified at its own source.
The plan now retains **198 required cases and 74 reviewed source anchors**.

Initial compilation and diagnostic failures remain retained. Concurrent survivor
calls reproduced typed `Observation(Overloaded)`; sequencing respects the bounded
metadata turn and one original deadline. Fixtures now wait for actual installed
trust after rooted changes. Publication verifies its final empty index page.
The replacement-specific teardown requires its actual one-replica state and
joined owners; existing empty-observer assertions remain intact. No proof check,
original deadline or durability assertion was weakened.

The ignored partial record is
`implementation/fo03/candidate-boundary.json` under
`.cellar/raft-perf/fleet-operations/`, with source/log/executable hashes and retained
failed attempts. No raw evidence is tracked under `plans/multi-raft/evidence/`.
The combined listing and passed-case audit still identify
`fleet_enrollment_incomplete_import` as absent at the required terminal boundary.
FO-03 remains **IN_PROGRESS**: fresh terminal survivor verification, physical
joins/fences, atomic terminal receipt/slot release and capacity readiness remain.
FO-04 through FO-10 are pending. This increment changes no existing protocol
suffix and claims no startup fix, RustFS deployment, enforced resource tier,
automation, TPS/latency gain or soak qualification.

## FO-03 progress: rooted boots and Finalizing fence

Source `f06b286edbeb6285001bc75b5ecc32edd68ba779` orders every managed boot, including intact restart,
against Finalizing through a root CAS before endpoint publication. A boot claim
binds its nonce, process, permanent engine pin and original node epoch to the
canonical request and permanent journal. Ordinary state changes retain that
claim; a newer admitted boot invalidates older candidates and running-host
credentials. Concurrent startup CAS conflicts fail cleanly; owned controller
reconciliation remains pending.

`FleetEvacuationCandidate::begin_finalization` rechecks complete inventory and
the active disruption, owns an authenticated probe of the exact departing
process, then atomically roots its canonical Finalizing projection and higher
node epoch. The original deadline is checked immediately before head CAS.
Exact committed retries reconcile without live inputs, including after adapter
recreation. Reusing an identity with different inputs refuses. The disruption
slot remains held and the process remains owned; this observation grants no
physical stop, terminal removal, capacity release or volume deletion authority.

Five new native cases cover retained boot identity across state changes, fresh
inventory/probe admission, lost replies and exact retries, actual intact-engine
boot refusal after Finalizing, invalidation by an admitted restart, and a held
finalization CAS losing to a real concurrent restart through a separate adapter.
The original owner joins before each retained engine is reopened. Survivor S3
writes and scratch/owner cleanup remain asserted.

Broader qualification exposed a restart liveness bug: a rooted boot refresh
removed old physical pools, while directory discovery recreated only pools still
present. The corrected path recreates pools for retained group/query obligations
and refreshes the same logical grants after verified discovery. It rejects old
views, preserves membership/grant revisions, avoids pools for empty discovery
and retains final Removed revocation. A new transport regression covers that
boundary. Controlled TLS sinks now obtain a real rooted boot and retain its exact
published view; attempting a second publication under one process correctly
refuses. Intact restart now asserts exactly one additional root revision and
node epoch, while retaining engine identity. No assertion or deadline was weakened.

Frozen-source verification passed **396 native Raft cases across 13 targets**,
**153 Raft unit cases**, **12 RPC cases** and **four startup-confirmation cases**.
The managed enrollment target passed all 88 cases. Five helper cases remain
ignored in direct listings and are not included in those passed-case totals.
Clippy for the library and selected targets passed with warnings denied, as did
formatting/layout over 1,101 Rust files, native dependencies (465 packages, no
JavaScript engine), documentation, plan validation and its ten validator tests.
The plan retains **204 required cases and 74 reviewed source anchors**.

A final test-import scope correction required rerunning enrollment. Rebuilding
all selected targets confirmed that every other executable was byte-identical
to the passing broad run. The qualification record retains 35 actual executables
(18 test executables), their hashes, both source manifests and the linked result
ledgers. All 1,173 Rust files and 21 Cargo/configuration inputs match the committed
Git blobs. The combined audit contains 46 passing stages and still identifies
only `fleet_enrollment_incomplete_import` as missing at the actual terminal
removal boundary. This is functional qualification against conditional in-memory
storage, native TLS and actual filesystem/SQLite, not deployed capacity proof.

Initial fixture/codec failures and the failed broad qualification attempts remain
retained. The first broad run stopped after its current native target joined;
the next run passed 86 enrollment cases but refused two reused-process fixture
publications. Those failures are not counted as passes. The final run uses the
corrected frozen source and independently retained actual executables.

The partial record is `implementation/fo03/finalization-fence-boundary.json`
under ignored `.cellar/raft-perf/fleet-operations/`. No raw evidence is tracked
under `plans/multi-raft/evidence/`. Node records require explicit nullable `boot`,
and the experimental protocol now ends in `/archive-cuts-1/fleet-finalization-1`.
The immediately preceding protocol is explicitly refused. Numeric markers remain
1; use fresh prefixes/namespaces and matching binaries, without a migration or
compatibility reader.

FO-03 remains **IN_PROGRESS**. Physical stop/join fencing, restart-safe resumption
while Finalizing, fresh terminal survivor checks, atomic terminal receipt/slot
release and capacity readiness remain required. The terminal
`fleet_enrollment_incomplete_import` case is still absent. FO-04 through FO-10
remain pending; this increment establishes no deployed RustFS/Colima capacity,
TPS/latency improvement, automation or soak qualification.

## FO-03 progress: exact-boot stops and signed joined closure

Source `024acc19fd70edcc4374280341140a8fabe8a964` pins the host's closure verification key in its rooted
boot claim and fsyncs a separate bounded local lifecycle record before endpoint
publication. The real engine worker persists exact operation/epoch/boot/process
stop acceptance before a Node-owned task signals shutdown. Caller loss does not
cancel that task. A synced local fence defeats delayed stops at its epoch;
accepted stops cannot be cancelled or rebound. Missing lifecycle metadata refuses
engine Resume. The guard adds no object-store call to business acknowledgement.

The outer host signs and conditionally publishes closure only after the Node
domain and physical Raft owners join. The CLI joins its core thread too. Closure
publication owns a separate bounded reporting domain and reconciles a lost PUT
response through exact bounded readback. Verification checks the exact selector
and the rooted boot key and returns an opaque proof. The signature assumes
trusted host code/private keys and coordination credentials. It supplies no
fresh survivor/archive health, application completion, terminal node transition,
volume deletion or disruption-slot release authority.

The local SDK and existing authenticated private management endpoint accept the
scoped stop/fence DTO. Managed hosts refuse generic management Stop. The admin
adapter enforces Operate permission, identical guard/audit request IDs and an
accepted, unconfirmed status. Legacy Actor-lease discovery remains a gap: a
standalone authenticated Raft-control-lane RPC is still needed for an empty
Raft-only node and a fleet controller.

New native tests hold the actual engine sync and engine-lock drop, discard the
caller, and wait for autonomous completion without another shutdown command.
They refuse closure before physical join, verify publication after a lost PUT
response, reject checksum-correct signature tampering, exercise local fence
retry/late-stop refusal, and reject new stop admission after inventory changes.
These focused stop fixtures use an empty managed departing host. Composing an
actual voter replacement under writes with scoped stop, joined closure and a
terminal removal receipt remains an end-to-end acceptance gap.
Engine tests cover retained fences/accepted stops, fresh boot nonces despite a
reused process hint, expired queued work, failed-sync ambiguity and missing
mandatory lifecycle state. The first broader cohort aborted on a deterministic
startup stack overflow in the unchanged retained-binding recovery test after
79 enrollment cases passed. The retained published control passes that exact
case; crash frames identify the nested startup polling path. The failed cohort
and isolated reproduction remain retained and are not counted as a passing run.
Boxing an inner helper was insufficient and was removed. The fix boxes the
composite startup future at the SDK/domain boundary, preserving its public async
API and cancellation ownership while preventing callers from embedding all
nested startup state. The unchanged recovery case passes with the original
stack configuration. In the retained arm64 debug executables, its main polling
frame falls from approximately 825 KiB to 497 KiB; this is a compiler-frame
measurement, not a steady-state memory or TPS claim. Temporary diagnostic output
is removed.

The audited pre-coalescing cohort passes **400 native Raft cases across 13
targets**, **157 Raft units**, **12 RPC cases**, **93 legacy Node cases**, **four
startup cases** and **19 admin cases**. Its 56 stages include warnings-denied
Clippy, formatting/layout, dependencies, documentation and plan checks. Forty
actual executables (22 test executables), the frozen 1,179 Rust files and 21
Cargo/configuration inputs, exact source tree and patch are retained. Five
process helper cases remain ignored in direct listings and are excluded from
passed totals.

Final review then reproduced duplicate shutdown-signal queueing with 10,000
accepted retries while the driver can still be joining physical I/O. The
production delivery helper remembers one successful or failed delivery outcome
per reporter, rather than accumulating signals. The regression fails with the
original repeated-send behavior; the real held-engine-lock case also exercises
32 exact accepted requests. Only the reporter and that native retry test change
in Rust after the audited broad cohort. Final-source verification passes **400 native Raft cases across 13 targets**,
**158 Raft units**, **12 RPC cases**, **four startup cases** and **19 admin cases**.
All 55 stages pass, including Clippy for the library/selected targets and admin
with warnings denied, formatting/layout over 1,107 Rust files, native dependencies
(465 packages, no JavaScript engine), documentation, plan validation and ten
validator cases. The final record retains 39 actual executables (21 test
executables); all 1,179 Rust files and 21 Cargo/configuration inputs match the
implementation commit. The 93 legacy Node cases remain attributed to the earlier
audited tree, without claiming a final-source rerun. Both source trees are
independently checked against their Git blobs, and the exact earlier patch is
retained. The plan has **213 required cases and 80 source anchors** and still
identifies only `fleet_enrollment_incomplete_import` as absent at its actual
terminal removal boundary.

The partial record is `implementation/fo03/physical-stop-boundary.json` under
ignored `.cellar/raft-perf/fleet-operations/`. It retains source/executable/log
hashes, both successful cohorts, the startup failure/control/fix evidence and the
expected failing duplicate-signal regression. These are native TLS, actual
filesystem/SQLite and conditional in-memory-store checks. No raw evidence is
tracked under `plans/multi-raft/evidence/`.

The experimental protocol now ends in `/fleet-finalization-1/fleet-stop-1`.
Use fresh coordination/data prefixes, namespaces, **engine directories** and
matching binaries. Numeric markers remain 1, with no compatibility reader or
migration. Automatic management remains default-off. FO-03 remains **IN_PROGRESS**;
this increment does not close the actual terminal
`fleet_enrollment_incomplete_import` case or claim deployed RustFS, resource,
performance or soak qualification.

## FO-03 progress: standalone Raft stop control

Source `43bcd17ab9c8d750fa2a2c6d76d8d18a102a6093` adds
`RaftCells::request_peer_fleet_stop` on authenticated Raft control. Both origin
and target may have zero business groups; no Actor lease or dummy cell is used.
A separate TLS exporter/MAC purpose binds the physical route, body digest,
request nonce and reply. New receiver work verifies the origin's current rooted
boot and permanent boot admission. The original origin budget covers queueing
and transport; the receiver includes prefix/header residence from TLS admission
and retains its absolute deadline through the bounded Node-owned worker.
Lost callers/replies remain unconfirmed and cannot undo admitted physical work.

The actual retired-voter case now composes guarded S3 learner seeding, joint
membership completion, rooted Finalizing, Raft-control stop during survivor
writes, joined retained physical history, exact signed closure and another
survivor write. Four new native RPC cases cover empty endpoints/fencing, caller
loss during real engine sync, expired metadata work and removed-origin refusal.
Four wire units cover authentication/purpose/channel/body binding, exact rooted
endpoints, bounded canonical inputs and receiver deadline residence.

All **58 qualification stages** pass: **404 native Raft cases across 13 targets**
(including 96 enrollment), **162 Raft units**, **12 RPC cases**, **four startup
cases** and **19 admin cases**. Warnings-denied Clippy for the library/selected
targets and admin, formatting/layout (1,111 authored Rust files), native
dependencies (465 packages, no JavaScript engine), documentation, plan validation
and ten validator cases pass. The audit retains 39 actual executables (21 test
executables); all 1,183 Rust files and 21 Cargo/configuration inputs match this
implementation commit. Five direct-listing process helpers remain ignored and
excluded from the passed totals. Earlier legacy Node results remain attributed
to their earlier audited source rather than a current-source rerun.

The plan now has 221 required cases and 80 source anchors. The remaining absent
FO-03 name is `fleet_enrollment_incomplete_import` at actual terminal removal;
that listing fact does not complete resumption, terminal proof or capacity
readiness. FO-03 remains **IN_PROGRESS**. The partial record at ignored
`implementation/fo03/stop-rpc-boundary.json` retains the final cohort, actual
executable/source/log hashes, earlier passing smoke runs and the initial test
fixture compile failure. No raw evidence is tracked under
`plans/multi-raft/evidence/`.

The experimental protocol now ends in
`/fleet-finalization-1/fleet-stop-1/fleet-stop-rpc-1`. Use fresh coordination/data
prefixes, namespaces, engine directories and matching binaries; markers remain
1, with no migration or compatibility reader. Terminal removal, slot release,
actual capacity readiness, deployed RustFS/Colima, performance and soak remain
unqualified. Automatic fleet management stays default-off.

## FO-03 progress: attested capacity readiness

Source `489a47cdc2f175e0ad874449b3cb552da2805524` implements typed Ready/Active
admission. The adapter owns complete current inventory and an authenticated node
probe, verifies the exact synced boot and installed host/consensus/engine bounds,
then commits the state transition and readiness record together. Ready remains
excluded. Platform CPU/memory/storage limits and retained usage are explicitly
trusted deployment attestation with an evidence reference/digest, not independently
measured kernel limits. Installed buffer totals are not process RSS.

Current capacity authorization checks the exact node epoch/boot and permanent
journal. A bare Active descriptor supplies no quantified capacity, and historical
acceptance after shutdown/exclusion supplies no current authority. Profile
revisions cannot change limits, an already qualified Active boot requires
maintenance before limit changes, and intact new boots must qualify again.
The directory's process-generation reuse refusal remains enforced; the new
restart case covers the refused attempt, joined cleanup and a valid fresh
process generation. Concurrent exclusion defeats an activation held at CAS.
Expired reads and incomplete/unfit evidence cause no activation.

Seven new native capacity cases pass. Current-source regressions pass **103
enrollment cases**, **16 directory cases** and **162 Raft units**, including the
focused metadata and four stop-RPC cases. Library/enrollment/directory Clippy with
warnings denied, formatting/layout, native dependencies, documentation, plan
validation and ten validator tests pass. All 1,186 Rust files and 21 Cargo/config
inputs match this implementation commit; retained actual executables and logs
are audited. The other native targets, legacy Node, admin and general RPC suites
remain attributed to their earlier-source cohorts rather than a current rerun.

The plan inventories 228 cases and 80 source anchors. The ignored partial record
`implementation/fo03/capacity-boundary.json` retains successful and failed attempts.
FO-03 remains IN_PROGRESS: terminal resumption, fresh final proofs, atomic
removal/slot release and the actual terminal incomplete-import case remain open.
FO-04 must combine current capacity authorization with fresh occupancy,
per-history eligibility and resource/movement reservations. Automatic management,
measured platform limits, deployed RustFS/Colima, TPS/latency and soak remain
unqualified. No raw evidence is tracked under `plans/multi-raft/evidence/`.

The current experimental protocol ends in
`/fleet-stop-rpc-1/capacity-readiness-1`. Use fresh coordination/data prefixes,
namespaces, engine directories and matching binaries; numeric markers remain 1.

## FO-04 progress: conservative resource ledger

Source `c79245d1c7308071e0d29e1964ad33a7c872be8c` begins FO-04 with the dependency-free `raft_fleet` component.
Its resource ledger charges complete physical high water, unresolved incremental
peak growth and explicit policy reserve independently for engine slots, durable
bytes and memory. It bounds the reservation inventory and active movement count.
Attribution is cumulative and exact-boot/process/profile scoped; older-scope debt
keeps its full charge. Stale, incomplete, conflicting, overflowing or partially
invalid inputs refuse without mutating the accepted ledger. Lower physical
readings do not free retained state, and no resource-release API is exposed.

All 51 ordinary decision-core integration cases pass, including 17 new accounting
cases and 34 existing cases. Crate/test Clippy passes with warnings denied;
formatting, layout and native dependency checks pass. The runner freezes source
and Cargo inputs, copies the actual executed binaries and records their hashes.
Logs and the partial `fo04/ledger-boundary.json` record remain under ignored
`.cellar/raft-perf/fleet-operations/implementation/`. No files were added under
`plans/multi-raft/evidence/`.

This is a pure accounting boundary, not an atomic catalog reservation, an
authenticated physical sample or executed fleet movement. The local last-slot
case is not the required two-catalog-writer race. All twelve native FO-04 cases
remain required, and none is claimed complete by these pure tests. Next integrate
current typed capacity, complete fresh physical observations and rooted movement
reservations in one admission CAS, then implement complete staged placement and
one-node-loss simulation. FO-03 terminal removal remains open and gates lifecycle
operations; it does not block this dependency-free accounting component.
FO-04 is `IN_PROGRESS`; FO-05 through FO-10 remain `TODO`. The fleet remains
default-off, with no new throughput, latency or deployed-capacity qualification.

## FO-04 progress: atomic resource debits

Source `c8e6815e50c70bd582619c5a63dbbf7a0820b67b` consumes current typed capacity
in `FleetCatalog::reserve_resources`. It owns complete inventory and an
authenticated exact-boot node probe, combines explicit deployment occupancy
attestation with installed runtime floors, then installs the journal-linked
resource row and active movement book through one expected-root CAS. Physical
high water and unresolved old-boot/profile growth remain charged. The initial
execution profile bounds active resource debits to two fleet-wide. Generic
metadata writes cannot delete or replace admitted rows or reset the book.

The six new native cases pass, including an actual last-slot race between
independent catalog adapters, an exclusion winning against a held resource CAS,
lost successful CAS response/confirmation with target shutdown and adapter
recreation, original-deadline cancellation without a detached local writer,
invalid/incomplete input refusal, and unmaterialized genesis debt. The seven
capacity cases and earlier-protocol refusal also pass. The full current native
cohort passes 137 cases: enrollment 109, catalog 12 and directory 16. All 162
selected Raft unit cases pass; focused checks overlap those totals. Clippy for
the library and these integration targets passes with warnings denied. The
runner freezes all 1,193 Rust files and 21 Cargo/configuration inputs and retains
the actual executed binaries and hashes. All inputs match this code commit. Formatting/layout, native dependencies (465 reachable packages, no JavaScript engine), documentation, plan validation, ten validator tests and source preflight pass. The partial record and logs are retained at ignored `implementation/fo04/resource-boundary.json` and adjacent `fo04-resources-v2-*` paths under `.cellar/raft-perf/fleet-operations/`. No raw evidence is tracked.

The resource cases currently run through the existing
`native_raft_fleet_enrollment` fixture. The complete
`native_raft_fleet_placement` target and all its required acceptance boundaries
remain deliverables; a passing resource case does not close FO-04.

This is resource-only admission. The historical receipt grants no learner or
membership authority. Complete physical history attribution, linking each debit
with its exact membership permit, source/target work ownership and terminal
release remain required. Unknown legacy permit attribution and noncancelled
genesis obligations refuse instead of counting them as free. Before releasing
an active debit, retain its physical high water independently of the completed
row so unused-growth release cannot also credit retained bytes or slots.
Complete final/staged assignment and one-node-loss restoration feasibility are
still pending. Deployed resource measurements, RustFS and performance are not
qualified; automatic fleet management remains default-off.

The experimental protocol now ends in `/capacity-readiness-1/resource-reservations-1`
so earlier writers cannot bypass the new append-only resource-book rules. Markers
stay at 1; use fresh coordination/data prefixes, namespaces, engine directories
and matching binaries. No compatibility reader or migration is added.

## FO-04 progress: authenticated physical history attribution

The resource consumer now owns a complete authenticated engine-history snapshot.
It matches original-voter references against full-scope digest, immutable birth
and physical member ID. A Reserved or unmaterialized Live birth remains blocked;
a durable voter is charged while another history can reserve unused capacity.
The prospective learner's occupied history refuses even when aggregate capacity
is available. Retired/unregistered engine histories remain visible after intact
restart, without a business registration or live quorum for that history.

The compact binary snapshot is bounded to 4,096 histories and 311,341 wire bytes,
within a 1-MiB response/metadata reservation. Capture validates the engine/node
count cut and preserves pending initialization. The request mode is MAC-bound;
ordinary node probes explicitly omit inventory. Response charges and original
age survive unpolled delivery and cancellation. Resource rows retain a canonical
inventory input digest scoped by their exact boot/profile and fleet root.

The physical-inventory source at `8b57107d55996763a6e9bdb2e80d1af8848189e6`
passed all-target Clippy and 138 native cases (enrollment 110, catalog 12 and
directory 16). Its Raft unit cohort passed 165 and failed one startup test with
temporary transport overload. The retained executable reproduced that failure
and also passed a subsequent focused run. The test now retries only explicit
heartbeat overload under its original bounded wait and still requires success;
this source change remains in qualification with the terminal-removal work.
The failed cohort is retained and is not reported as a passing unit run.

Earlier failed attempts remain retained: the
retention fixture constructed its timer outside the execution domain; a raw
unrooted-node frame lacked the new required request field; Clippy required a
shared observation type and fixed-size chunk API. All-target Clippy additionally
found the filesystem fault adapter loaded three times in library tests; these
consumers now share one owning test module. Assertions and fault behavior were
preserved. The strengthened warm-node case also refuses an extra debit against
an actual occupied slot plus an existing reservation despite a zero caller slot
attestation. No failed attempt is treated as a pass.

Typed membership-permit integration and terminal resource release remain open.
Keep retained physical high water independently of completed debit rows before
releasing movement slots. Complete staged assignment, one-node-loss restoration
feasibility, owned reconciliation and operator workflows remain required.
FO-03 and FO-04 stay `IN_PROGRESS`; this snapshot grants no membership, removal,
resource release, deletion or health lease. Platform occupancy remains separately
deployment-attested; RustFS/Colima performance and resource enforcement are not
qualified. The managed fleet remains default-off.

The experimental protocol ends in `/resource-reservations-1/physical-inventory-1`.
Resource records require the inventory digest and node requests require explicit
mode. Keep markers at 1 and use fresh coordination/data prefixes, namespaces,
engine directories and matching binaries; no compatibility reader or migration.

## Prioritized remaining delivery

| Priority | Boundary | Required result |
| --- | --- | --- |
| P0 | FO-04 placement | Atomically link resource debits and exact membership permits; execute and release real moves under fresh terminal proof while retaining physical high water. Complete staged feasibility and single-failure restoration headroom. |
| P1 | FO-05 load and FO-06 reconciliation | Fresh scoped load observations and a domain-owned resumable reconciler. Restart, cancellation and ambiguous replies converge on one exact operation. |
| P2 | FO-07 maintenance and FO-08 operator workflows | Shared-slot restart/resize/rejoin/cancel/remove, explicit unreachable-owner isolation, authenticated idempotent SDK/admin/CLI and typed outcomes. |
| P3 | FO-09 scaling | Desired/effective/observed horizontal and vertical capacity; fewer cells alone cannot establish retained-state shrink fit. |
| Release gate | FO-10 deployed qualification | Actual RustFS and five/six/four-node operations, original three-node/1,500-group density, enforced 1-vCPU/3-GiB limits, faults and 24-hour soak. Successful TPS, bytes/sec and p50/p95/p99; prefer p99 below 200 ms, hard ceiling 500 ms. |

Fleet management remains default-off. No local feature test establishes a TPS
or latency improvement. Optional performance changes require matched throughput
or latency gains. Preserve the investigation order: archive-head failures and
repeated replica work, control queueing by traffic class, SQLite copying versus
validation versus synchronization, archive batch flush reasons, then idle-group
scheduling cost. Earlier workspace contention was negligible; the sixteen-frame
control window is already implemented.

## Expanded operating design

The [lifecycle execution contract](lifecycle-execution.md) adds the authority and
linearization point of each effect, crash/lost-response resumption, bounded
catalog batches, empty-host activation acknowledgements and the separate desired,
effective-placement and observed-platform resource profiles. It specifies the
prospective shrink ceiling and the post-stop fit check before reducing platform
limits. Future departing nodes are excluded through the pinned target set while
only the current evacuation owns disruption work. Optional balancing retains
predictions and fresh outcome observations rather than assuming movement helps.

The planning examples now include exact symmetric placement tables. Five-to-six
moves 250 copies and produces 25 cells on each six-node triple; five-to-four
moves E's 300 copies and produces 125 cells on each remaining triple. A separate
arithmetic check parsed the documented tables, verified all resulting per-node
counts and six-node failed-pair classifications, and found feasible balanced
leader assignments through a bounded flow calculation. These are design checks,
not execution of the real seed/membership protocols or measured TPS gains.

The expanded contracts are assigned to the existing pending acceptance cases.
That design update retained 103 cases, with no package status advanced. Plan validation,
all 10 validator tests, local documentation links and diff whitespace checks pass
for this documentation update. Deployed capacity, lifecycle, performance and soak
qualification remain pending.

The [planning details](planning-details.md) specify exact resource accounting,
staged feasibility, worked five-to-six/five-to-four changes, failed-pair
classification, heterogeneous role costs, fewer-cell sizing and the removal
proof boundary. Maintenance now has a phase-by-phase decision table and measured
service-stage lower bounds for convergence. The operator workflow defines the
required preview fields and distinguishes a clean non-admission conflict from
an unknown submission outcome.

The earlier planning expansion added eleven future runtime cases across FO-04,
FO-07, FO-09 and FO-10, bringing the inventory to 97 at that checkpoint; subsequent
guard and birth work brought it to 103 before the six host-enrollment cases.
That expansion marked no additional package done.
The design also corrects the earlier 25% load statement: serving copies grow
from 300 to 375 after RF3 restoration on four survivors; that is not a prediction
of immediate follower work or throughput.

For this documentation expansion, plan validation and its 10 validator tests,
repository documentation checks and diff whitespace checks passed. A separate
arithmetic check enumerated all ten failed pairs and verified the copy counts,
fewer-cell count bounds and leader-transfer rate example. Those checks do not
execute the new native acceptance cases. No new benchmark or full runtime
qualification was claimed by that documentation expansion. FO-02 native guard
verification is recorded separately above; full fleet qualification is pending.
