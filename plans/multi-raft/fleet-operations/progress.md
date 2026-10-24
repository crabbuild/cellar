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
acceptance inventory now contains 103 named cases; its remaining scope is intact.

## Next implementation boundary

FO-03 adds the trusted dynamic node set, empty managed host readiness, bounded
dormant registration and complete managed history/birth enrollment. It must keep
storage pins and retired bindings immutable and separate restart/resize exclusions
from evacuation. FO-04 through FO-10 retain their complete planning, telemetry,
reconciliation, lifecycle, API/CLI, scaling and qualification scope.

The current `node/raft_startup.rs` and `host.rs` reject zero registrations,
`host_directory.rs` derives namespace trust only from registered cells, and
`client.rs` routes node controls through a first-cell binding. Empty-host support
must remove those dependencies using explicit namespace/class trust and node-level
request identity. Dynamic binding mutations belong to the serialized host, with
bounded reader access and no lock guard crossing an await. A dummy business cell
or a successful TCP connect is not an enrollment/readiness proof.

## Expanded operating design

The [planning details](planning-details.md) specify exact resource accounting,
staged feasibility, worked five-to-six/five-to-four changes, failed-pair
classification, heterogeneous role costs, fewer-cell sizing and the removal
proof boundary. Maintenance now has a phase-by-phase decision table and measured
service-stage lower bounds for convergence. The operator workflow defines the
required preview fields and distinguishes a clean non-admission conflict from
an unknown submission outcome.

Eleven future runtime cases were added across FO-04, FO-07, FO-09 and FO-10,
bringing the acceptance inventory to 97. No additional package is marked done.
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
