# Fleet operations implementation progress

The full fleet operations goal is active. FO-01 implements the catalog adapter
and FO-02 implements the durable membership guard, with local native verification.
FO-03 is in progress; FO-04 through FO-10 remain pending. The operator commands,
automatic redistribution, managed maintenance and resize workflows are not yet
available. The task manifest is authoritative for package status and retained
completion records.

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

## Next implementation boundary

FO-03 adds the trusted dynamic node set, empty managed host readiness, bounded
dormant registration and complete managed history/birth enrollment. It must keep
storage pins and retired bindings immutable and separate restart/resize exclusions
from evacuation. FO-04 through FO-10 retain their complete planning, telemetry,
reconciliation, lifecycle, API/CLI, scaling and qualification scope.

`node/raft_startup.rs`, `host.rs` and `host_directory.rs` now accept zero groups
only with explicit rooted enrollment and compiled namespace/class declarations;
node controls no longer borrow a first-cell binding. The remaining work includes
authenticated remote node observations, required trusted-revision acknowledgements
and complete import/birth admission. Current-root trust refresh and transport
revocation are locally implemented; they do not complete the remote readiness
and movement protocol. Dynamic binding mutations now belong to the
serialized host, with bounded reader access and no lock guard crossing an await.
Retained active histories still reopen through startup recovery registrations;
the new metadata path refuses resetting their physical slots. A local observation,
dummy business cell or successful TCP connect cannot substitute for the remaining
enrollment/readiness proof.

## Prioritized remaining delivery

The next change should finish authenticated node observations and live trust
refresh/revocation in FO-03. This gives a coordinator an authoritative view of
new capacity without relying on the local startup cut. The following order keeps
safe admission ahead of automation; the manifest dependencies remain unchanged.

| Priority | Boundary | Required result before advancing |
| --- | --- | --- |
| P0, first | FO-03 node trust and readiness | Purpose-bound remote node controls work without a business group; current rooted trust rejects unknown or revoked nodes across inbound calls, routes and grants. Physical-host identity and eligibility modes are checked. Cover `fleet_enrollment_unknown_node`, `fleet_enrollment_route_revocation`, `fleet_enrollment_empty_host`, `fleet_enrollment_physical_host` and `fleet_enrollment_eligibility_modes`. |
| P0, next | FO-03 complete inventory and lifecycle exclusion | An incomplete import cannot authorize management; a concurrent birth cannot escape a drain cut; a retired physical binding cannot be reused. Cover `fleet_enrollment_incomplete_import`, `fleet_enrollment_birth_drain_race` and `fleet_enrollment_retired_binding`, then pass the complete FO-03 listing and behavior tests. |
| P1 | FO-04 placement, FO-05 fresh load, then FO-06 reconciliation | Validate final assignments and every staged move against retained/temporary costs, failure domains and one-node-loss restoration. Reconciliation must survive cancellation, restart and lost replies, with fresh terminal quorum proof before releasing reservations. Verify foreground overhead before enabling optional balancing. |
| P2 | FO-07 maintenance, FO-08 operator surfaces, then FO-09 scaling | Deliver the durable stop fence and one-disruption slot, complete evacuation/removal proof, authenticated idempotent API/CLI workflows and observed-limit checks for resource changes. Proposed commands are not available until these packages pass. |
| Release gate | FO-10 deployed qualification | Qualify RustFS conditional writes and real five/six/four-node workflows, faults and the 24-hour soak. At 1 vCPU/3 GiB per node, maximize successful TPS with p99 below 200 ms preferred and 500 ms hard ceiling. Measure enabled-idle controller cost and use matched comparisons for optional performance changes. |

The local implementation has no measured TPS or latency improvement claim. Keep
fleet management default-off until its release gates pass. Optional throughput
or latency changes must meet the existing qualification gates; additional code
alone is not evidence of an improvement.

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
