# Fleet catalog and membership authorization

This component stores low-frequency fleet metadata through the existing
`Bucket::put_cas` adapter. It is not a consensus service. The native group owner
still commits configurations, grants fresh-quorum seeding and acknowledges
business writes. No business acknowledgement calls the fleet catalog.

The adapter and its verification export are available for integration work.
Complete inventory, typed capacity/lifecycle admission, automatic reconciliation
and operator workflows are separate packages in the
[fleet operations plan](../../../../../plans/multi-raft/fleet-operations/README.md).
Do not interpret successful generic metadata writes as proof that a placement
or removal is safe. Deployment against RustFS remains a qualification gate.

## Storage and ownership

`FleetCatalog::initialize` conditionally creates the fleet root and permanent
genesis pin. A lost initialized root cannot be silently recreated. Five immutable
indexes hold policy, nodes, histories, operations and admissions. A separate
permanent mutation journal retains exact request digests and original applied
revisions, including after later root updates.

`commit` writes immutable bounded pages before conditionally replacing the root.
Only the root update admits the metadata projection. Orphan pages and candidate
receipt bytes grant no authority. Retry an uncertain mutation with the original
identity, expected revision and payload; a new adapter can reconcile its journal
entry. A changed payload under the same admitted identity conflicts.

Snapshots and cursors belong to the originating adapter. Reads use exact keys
and bounded hash-radix traversal; listing is not part of the authority path.
Requests, records, pages, depth and decoded work are bounded in the implementation.
Each shared adapter permits one catalog turn at a time and returns backpressure
instead of accumulating waiters. The execution-domain deadline covers the entire
turn. Cancellation drops that turn; there is no detached metadata worker.

Use ordinary and conditional provider clients with the existing no-hidden-retry
CAS contract. Strong exact-key consistency and actual conditional-write behavior
must be verified on the deployed backend. Prefixes and fleet incarnation are
explicit constructor inputs; missing/corrupt state is never an empty inventory.

## Opaque capabilities and committed consumption

`FleetGroupBinding` describes fleet incarnation and exact cell history.
`FleetMembershipPermit` binds one sequence to a permit/operation identity,
configuration revision, complete repair-plan digest and replacement member,
node and disk. Constructing or serializing a descriptor grants no host authority.

The catalog mints opaque capabilities from rooted index entries:

| Method | Rooted prerequisite | Capability |
| --- | --- | --- |
| `authorize_group` | Exact encoded binding in `Histories` | `FleetGroupAuthorization` |
| `authorize_membership` | Exact history binding and encoded permit in `Admissions` | `FleetPermitAuthorization` |

These methods validate record identity and framing. They do not implement the
future typed planner's capacity policy or mutation legality. The owning controller
must atomically admit a legal reservation before asking for its capability.
Low-level metadata mutation is a privileged embedding/verification surface.

Capabilities can outlive their source snapshot. Safety therefore does not depend
on a TTL, a live controller, or rereading the root just before an RPC. Enrollment
commits a fleet binding in `GroupMembership`. Starting a managed repair commits
the exact next consumed sequence with the learner configuration. Continuation
requires the same active permit and repair plan. A delayed older sequence cannot
be reused after consumption or cancellation.

For an unused permit, `fence_fleet_permit` commits a same-configuration entry and
consumes its sequence. Its change explicitly re-adds an existing voter; an empty
`ConfChangeV2` means leave-joint and is not a safe no-op fence. A started seeding
plan instead uses guarded learner abort. Once joint consensus begins, finish
that exact joint transition under both majorities. A timeout never clears a
permit or physical reservation.

The group retains its enrollment, consumed sequence and last permit/phase through
engine reopen, snapshots and archives. Scope checks bind that ledger to the
actual history, and successor validation refuses dropped/regressed/rebound state.
Optional fleet state is boxed and allocated only for managed groups. Large
control request/response payloads and pending fleet goals are also boxed;
ordinary command carriers do not embed a full permit.
Physical snapshot completion reservations cover the expanded bounded ledger.

## Host API and fresh learners

The SDK handle exposes `enroll_fleet`, `begin_managed_repair`,
`advance_managed_repair`, `fence_fleet_permit` and
`activate_managed_learner`. They require the opaque capability. Managed groups
refuse ordinary begin/advance/add-learner calls, including idempotent shortcuts.
Unmanaged groups retain their existing APIs.

A fresh managed learner starts as a registered `Standby`, then activates with
its exact capability. Peer and archive seeding still require the existing live
quorum grant, verified image/dependencies, correct physical identity and synced
engine initialization. Legacy fresh `Joining`/`JoiningFromS3` startup refuses a
managed seed before claiming the member. An intact retained joined engine can
resume without creating another membership or rerunning application commands.

`fleet_membership_status` is a synced local observation. It does not establish
a fresh quorum or authorize root reservation release. The fleet reconciler must
obtain an authenticated quorum/configuration proof for the exact terminal permit,
join physical work, and finalize its catalog result. A successful or lost RPC
response alone is not a fleet completion certificate.

## Format cutover and qualification

Cellar-owned markers stay at 1. Membership and intent records require an explicit
nullable `fleet` field, and the namespace protocol identity ends in `/fleet-1`.
Older namespace protocols refuse startup; there is no legacy reader or migration.
Use a fresh experimental namespace and matching participants. Do not downgrade a
guarded engine to an older binary or remove the guard field to regain admission.

`native_raft_fleet_catalog` verifies the bounded conditional adapter.
`native_raft_fleet_permits` exercises native SQL hosts, real TLS, stale capability
replay, quorumless fences, both-majority joint recovery, retained-engine reopen,
peer/archive seed metadata and legacy ingress refusal. Core member-state tests
check strict decoding, scope and snapshot successor constraints. These tests do
not qualify full automatic fleet operations, physical RustFS semantics, the
resource-limited Colima topology, steady-state overhead or production capacity.
