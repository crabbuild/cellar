# Fleet catalog and membership authorization

This component stores low-frequency fleet metadata through the existing
`Bucket::put_cas` adapter. It is not a consensus service. The native group owner
still commits configurations, grants fresh-quorum seeding and acknowledges
business writes. No business acknowledgement calls the fleet catalog.

The adapter and its verification export are available for integration work.
The [namespace registry and inventory check](namespaces/README.md) enumerate every
declared namespace and retained birth/history. Typed capacity/lifecycle admission,
automatic reconciliation and operator workflows remain separate packages in the
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

## History and birth admission

Declare the exact namespace/class with `declare_namespace` before managed host
startup, history enrollment, import or new birth admission. Declarations are
append-only; their data-root and schema pins cannot be replaced. A complete
inventory read covers the whole rooted registry, rather than a caller-selected
subset or reachable-node scan. It does not certify node removal.

`bind_namespace` prepares a fresh namespace configuration with an immutable
birth owner. The owner binds the fleet incarnation and exact scoped catalog key;
changing catalog prefixes cannot issue an untracked competing birth. The existing
unmanaged root cannot be rewritten by opening it with this configuration.
Managed roots refuse ordinary `resolve_birth` and `cancel_birth` calls.

`reserve_birth` confirms the namespace's permanent group-ID claim, then atomically
indexes the exact history, permanent cell name and fleet-wide group ID before
returning an opaque `FleetBirthAuthorization`. The claim also fences other
namespace catalogs sharing the same data prefix. An existing conflicting claim
refuses admission before adding a history. A claim left by a cancelled or losing
root turn grants no birth or membership authority; retain it permanently and
reconcile the original request. Never delete it to retry or reclaim its ID.
Initial RF3 placement
requires three active, engine-pinned nodes on distinct declared physical hosts.
The same root CAS orders admission against node-state changes. A delayed writer
can finish after drain begins only if its history reference was already admitted.
Capacity feasibility, readiness acknowledgement and legal node-state transitions
still require their own controller boundaries; the reservation is not that proof.

History records retain schema, namespace-root and expected live-birth digests,
initial member/node references, admission identity and `Reserved`, `Live` or
`Cancelled` state. The root maintains the history count through every catalog
mutation, bounded to 10,000 entries without a per-birth full inventory scan.
Each record is at most 4 KiB. The initial references describe genesis, never
current membership or absence from a node.

`resolve_managed_birth` writes only the reserved decision and checks its exact
winner. `cancel_managed_birth` competes at that same immutable name key.
`confirm_birth` observes the exact winner and commits its disposition in the
fleet root. Missing replies, expired deadlines and missing birth keys preserve
the reservation; retry the original request identity/payload and reconcile it.
Cancelled histories and their names remain retained. Neither a pending nor a
cancelled history can mint group or membership authorization.

`enroll_history` registers one already observed immutable birth for import. It
does not assert that an import is complete, quiesce legacy creators, convert an
unmanaged namespace or authorize maintenance/removal. The
[namespace import runtime](imports/README.md) supplies the per-namespace claim,
complete traversal, quorum coverage, stop/resume, exact-request recovery and
birth-ownership handoff. Fleet-wide namespace coverage and lifecycle checks
remain pending. The [namespace birth inventory](../catalog_inventory/README.md)
now indexes every canonical intent before publication and can freeze new birth
admissions through CAS. Its frozen set includes pending and cancelled decisions;
it is an import input, not membership quiescence or a complete-import certificate.
Group enrollment, managed-host startup and dormant registration
also compare the recorded birth digest and schema with their actual birth.
Ordinary `bootstrap_member` can only resume an already enrolled managed group;
it refuses fresh or bare genesis before creating local group state or a member
claim. Managed host startup and `activate_managed_initial_member` use the guarded path described below.

This experimental protocol hard-cuts to `/fleet-namespaces-1`; use fresh fleet
roots, fresh namespaces and matching binaries. Fleet heads require the current
protocol digest, and their permanent genesis pin binds it. Missing or earlier
protocol state refuses initialization and reads; there is no compatibility reader.
Numeric format markers remain 1.

## Opaque capabilities and committed consumption

`FleetGroupBinding` describes fleet incarnation, exact catalog-scope fingerprint
and cell history. `FleetCatalog::group_binding` returns that scoped descriptor;
the low-level constructor requires the catalog fingerprint explicitly.
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

### Quorum-backed membership observation

`RaftCellHandle::observe_fleet_membership` requires a rooted group authorization
that matches the host's actual immutable birth and schema. Each invocation runs
a new safe ReadIndex through the existing bounded read dispatcher. The host waits
until its durable applied position covers that barrier and its route covers the
observed membership revision, then captures the configuration and fleet guard
under the same short cache read. The result binds the exact node, disk, process
and history. It includes incoming/outgoing voter sets, current/next learners,
joint auto-leave state, committed joined-member bindings and repair phase.

This metadata read needs neither a SQL image nor an application query. It shares
the existing administrative admission and reserves an additional 64 KiB/item
before cloning membership state. That reservation remains held through reply
delivery, including an unpolled SDK waiter. Pressure refuses immediately rather
than adding an unbounded queue. Cancellation, the original deadline and shutdown
release the pending read; barrier retries do not extend that deadline. Business
write acknowledgement does not call this API or the fleet catalog.

`fleet_membership_status` and idempotent enrollment replies remain local synced
observations. Neither supplies this invocation's quorum barrier. Likewise, saving
a `FleetMembershipObservation` does not establish current quorum later or grant
a lease. It is an input to complete-import and terminal-operation verification,
not a complete inventory, archive-coverage or node-removal certificate. Rooted
operation sequencing, complete history coverage and physical work joins remain
separate controller obligations.

## Host API and fresh learners

### Rooted node enrollment

`register_node` adds a bounded typed descriptor in the `Nodes` index with exact
request identity and CAS reconciliation. `authorize_host` returns an opaque
capability for that rooted node/disk, physical-host identity, endpoint name and
compiled-capability digest. Boot refreshes it against the current root and
refuses missing, evacuated, removed or incompatible records. Registration does
not activate capacity or certify a complete managed inventory.

An existing engine can be pinned in the registration request. A fresh engine is
permanently pinned by a conditional node-record update during startup, after the
ordinary namespace storage pin and before endpoint publication. Lost replies
reconcile through that monotonic pin. Changing namespace catalog prefixes cannot
make a replacement engine satisfy the fleet pin. The initial node-table bound is
64 retained records, including removed identity tombstones; exhaustion refuses
registration and does not collect identity history.

`RaftManagedHostOptions` declares a bounded namespace/class list whose canonical
digest must match enrollment. Classes resolve against the compiled application
and existing namespace/schema/version metadata. This lets a managed host boot
with zero registered groups. Imported participating histories must already have
their rooted/committed group enrollment. Fresh managed roots use guarded genesis;
this startup path cannot bootstrap an unguarded voter. The legacy group controller stays disabled on these hosts until
fleet-aware reconciliation is integrated.

`RaftCells::observe_node` uses bounded node control admission independently of
cell handles. It returns actual engine identity, occupied and unattached history
counts, configured profile and the explicitly named startup enrollment cut.
It also reports `fleet_trust_revision` and `current_enrollment`, the last complete
rooted node cut installed by the owned directory worker, including its node
epoch. The startup cut remains unchanged. These local observations grant no
capacity activation, quorum or removal proof.

`inspect_peer_node(node, minimum_revision, timeout)` reads an exact remote process
without a business cell or member identity. Both endpoints must have installed at
least the requested trust revision. The request and response bind fleet, cluster,
protocol, physical node/disk/process/engine identities, labels, nonce and channel.
Rooted transport trust permits only this node metadata operation; it does not
create namespace inspection/query grants or group authority.

The response distinguishes startup enrollment from the installed current state
and epoch. Restart/resize exclusion and full evacuation remain different modes.
The origin rechecks root identity/state and process discovery before publishing
its reply. A stale or missing view, pressure and unusable response
are not node-failure evidence; only a failed direct connection attempt is reported
as unreachable. Sample age includes origin administrative queue residence and
reply retention by an unpolled caller, measured through SDK delivery.
Admission must still compare the observation with its current rooted operation;
metadata is a sampled view, not a lease or a linearizable control-store read.

Bodies are at most 16 KiB. The RPC shares the existing independent metadata slots,
node and peer budgets; a 64 KiB host response reservation survives delivery to an
unpolled caller. Cancellation stops the physical transport owner, and shutdown
joins it before releasing physical ownership. This is a readiness observation,
not a placement activation, terminal operation receipt or complete inventory.

Managed discovery reads the current catalog root and bounded node table rather
than expanding trust from bucket objects or unsolicited traffic. An unchanged
exact root reuses the validated immutable node cut without replaying its pages.
New rooted nodes are discovered at exact directory keys, without configured
addresses. Their namespace storage pin must equal the fleet pin. Missing or
unavailable metadata preserves existing quorum transport and supplies no new
authority.

Trust installation rejects older cuts, identity/pin changes, missing retained
records and removed-identity revival. It fences route/grant installation and
seed work. Final `Removed` status cancels existing peer pools, query/inspection
grants and physical seed epochs; their tasks retain reservations until exit.
Restart, resize, draining and evacuated nodes retain transport while membership
work resolves; their placement eligibility remains separate. This adapter does
not establish the evacuation/removal proof required before publishing `Removed`.

One owned discovery batch holds a 4 MiB administrative metadata reservation
through completion. Its read-only catalog adapter has a separate bounded turn,
so a held background root read cannot reject foreground history operations by
owning their catalog turn. Managed hosts require at least that capacity plus a
64 KiB response and a 4 KiB request, with three administrative items. Cached node
probes can therefore progress while discovery waits on storage. The original batch deadline and
joined shutdown apply to both trust reads and endpoint observations.

`register_managed_history` lazily registers dormant metadata using an opaque
rooted group authorization. The owning host rechecks current host/history
enrollment, its compiled declaration, immutable birth and existing physical slots.
It admits one metadata preparation at a time with an independent 4 MiB control
reservation and original deadline. Cancellation/close joins that read before
releasing ownership; publication stays serialized with activation and shutdown.
The operation creates no member, SQLite image or voting authority. A timed-out
reply can race publication; retry the same authorization and inspect the result.

A confirmed fresh managed birth can finish registering its original voters after
rooted drain, restart or resize exclusion. Its initial node reference was admitted
before exclusion and stays inventoried throughout settlement. This exception
does not admit a noninitial destination or a new birth: reservations still require
active nodes at the root CAS. Local shutdown/drain admission remains closed;
maintenance must settle prior actions before accepting the stop barrier.

The shared binding registry returns owned metadata under short read locks. Only
the host publishes new entries; names are never replaced, and retained handles
keep their existing one-way member slot. Capacity includes dormant bindings and
unattached engine histories. Occupied or retired unregistered slots refuse
rebinding even when other aggregate slots are free. Reopening retained active
histories still uses the existing startup recovery registrations; dynamic
metadata registration cannot reset their engine state. Local observations also
report pending registration work so later readiness/removal checks cannot omit it.

Managed learner activation refreshes host and history authorization under the
original seed deadline before initializing a fresh member. It checks the actual
engine pin, namespace and history binding, and current replica-receiving
eligibility. Startup enrollment remains a historical observation: Registered to
Active can admit a learner without restarting, while Active to Draining refuses
a fresh slot. The read holds a separate 4 MiB control reservation and returns
overload when that reservation is unavailable; it releases the reservation before
bulk peer or archive seeding. An already synced matching member can resume its
exact committed permit after exclusion. This check grants no capacity activation
or stop/removal certificate.

### Guarded initial voters

A confirmed birth in a fresh managed namespace can initialize its original RF3
voters through startup `Replica` registrations or, on running empty hosts,
`register_managed_history` followed by `activate_managed_initial_member`.
Registering metadata grants no voting authority. Imported unmanaged namespace
roots cannot use the fresh-genesis path.

The host refreshes its rooted identity and exact history, checks the namespace's
coordination owner and actual birth/schema, and refuses unknown initial endpoints.
The engine stores genesis and fleet enrollment in one synced batch before
publishing its group cache. `Genesis` enrollment explicitly begins at index 1;
ordinary `Committed` enrollment still requires a later configuration entry.
Codecs require that origin, and successors cannot retag or retrofit a bare group.
Only the exact conditional permanent member claim permits runtime attachment.
Activation retries also observe startup-recovered original voters through their
actual birth, declared namespace and retained membership guard. They do not
reinitialize the engine or SQLite image. An initializer still awaiting runtime
attachment cannot supply a completed activation receipt.
A missing claimed engine, changed binding or retired local configuration refuses
admission. The genesis receipt observes local durable state; business writes
still require their normal Raft quorum and optional archive proof.

Dynamic initialization uses the existing owned SQL/attachment pipeline, one
initializer per host, the original deadline and a 4 MiB metadata reservation.
Cancellation after submission can leave a successfully initialized member; retry
the same capability. Shutdown cancels metadata waits and joins physical owners.
Existing initialized members return their actual guard without another genesis.
Inspection counts an admitted slot once as it becomes a durable engine group,
using one catalog lock across that handoff.

Cold startup reads bounded exact process-view keys from the rooted node set
before constructing membership routes. A self-address hint is sufficient when
the other required rooted views are available; periodic discovery refreshes later
boots. Missing metadata never invents an endpoint or voter. This does not complete
capacity activation, import completeness, movement acknowledgements or removal.

### Group membership and learner activation

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
nullable `fleet` field. Birth records also require an explicit nullable
`cancelled_by` field; the namespace protocol identity ends in
`/fleet-1/enrollment-1/node-probes-1`.
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
