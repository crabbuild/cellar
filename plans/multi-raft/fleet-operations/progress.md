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

## Prioritized remaining delivery

The append-only namespace registry, complete inventory read and native
retired-binding refusal are implemented. Next, consume a revalidated exact root
inside typed capacity and lifecycle admissions; a saved count cannot authorize
placement or maintenance. The manifest dependencies remain unchanged.

| Priority | Boundary | Required result before advancing |
| --- | --- | --- |
| P0, first | FO-03 complete inventory admission | The rooted registry and full canonical/history checker supply the inventory input; native retired-binding refusal is covered. Connect an exact revalidated root to typed capacity and maintenance admission, including offline and pending references, operation-specific live proofs and physical-work joins. Checked inventory does not prove node absence. Close `fleet_enrollment_incomplete_import`. |
| P0, next | FO-03 trust and failure-domain policy | Qualify unsolicited-node ingress refusal, require installed trust-revision acknowledgements from actual move participants, and enforce rooted node-versus-physical-host placement policy. Pass all four remaining mandatory cases and the full FO-03 behavior suite. Close `fleet_enrollment_unknown_node`, `fleet_enrollment_route_revocation`, `fleet_enrollment_physical_host`. |
| P1 | FO-04 placement, FO-05 load, FO-06 reconciliation | Implement bounded staged feasibility for retained/temporary costs and one-node-loss headroom, fresh load observations and a domain-owned reconciler. Restart/cancellation/lost replies must converge; release reservations only with fresh terminal quorum proof and physical-work joins. |
| P2 | FO-07 maintenance and FO-08 operator API/CLI | Deliver exact-operation/epoch/process stop fencing, one disruption slot, complete evacuation/removal receipts and authenticated idempotent workflows with revision guards and typed outcomes. Cover add/remove and rolling restart. |
| P3 | FO-09 scaling and fewer-cell shrink | Reconcile desired, effective and observed resources for horizontal and vertical changes. Require observed platform-limit checks and complete retained-state fit before shrinking; fewer cells do not establish removability. |
| Release gate | FO-10 deployed qualification and performance | Qualify actual five/six/four-node RustFS/Colima operations and faults with enforced 1-vCPU/3-GiB limits and a 24-hour soak. Measure successful TPS, p50/p95/p99, archive catch-up and controller idle/movement overhead. Prefer p99 below 200 ms; 500 ms is the hard ceiling. Keep optional performance changes only with matched evidence of higher throughput or lower latency. |

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
