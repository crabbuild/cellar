# Fleet catalog and membership authorization

This component stores low-frequency fleet metadata through the existing
`Bucket::put_cas` adapter. It is not a consensus service. The native group owner
still commits configurations, grants fresh-quorum seeding and acknowledges
business writes. No business acknowledgement calls the fleet catalog.

The adapter and its verification export are available for integration work.
The [namespace registry and inventory check](namespaces/README.md) enumerate every
declared namespace and retained birth/history. Typed evacuation exclusion now
consumes that complete inventory. Typed capacity activation and resource debit
admission are implemented at the boundaries below. Terminal lifecycle proof,
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

## Rooted replica separation

`initialize` selects `FleetReplicaSeparation::PhysicalHosts`. The explicit
`initialize_with_replica_separation` alternative accepts `Nodes` for a deliberately
colocated fleet. The head stores the current contract and the genesis pin retains
its initial choice. Reopening reads the persisted choice; initializing with a
different choice conflicts. There is no in-place policy-edit API in this increment.
Missing or unknown policy fields refuse decoding rather than choosing a default.

Both modes require distinct durable node identities. `PhysicalHosts` additionally
requires distinct rooted physical-host identities for birth voters and every
currently participating copy plus a fresh learner. The fresh managed repair path
checks the actual durable configuration after participant trust acknowledgements.
Advisory profile zones, process labels or node counts cannot supply physical-host
separation. Exact already-committed repairs can still reconcile after exclusion.

`Nodes` permits a multi-container development fleet on one host. It grants no
independence from loss of that host. This selection does not create capacity,
change RF3, loosen quorum requirements or qualify deployed resource limits.

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
Initial RF3 placement requires three active, engine-pinned nodes satisfying the
rooted separation contract; the default requires three distinct physical hosts.
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

This experimental protocol hard-cuts to `/fleet-quorum-1/fleet-cuts-1/empty-archives-1/archive-cuts-1/fleet-finalization-1/fleet-stop-1/fleet-stop-rpc-1/capacity-readiness-1/resource-reservations-1/physical-inventory-1/fleet-removal-1`; use fresh fleet
roots, fresh namespaces, fresh engine directories and matching binaries. Fleet heads require the current
protocol digest, and their permanent genesis pin binds it. Missing or earlier
protocol state refuses initialization and reads; there is no compatibility reader.
Numeric format markers remain 1.

## Inventory-consuming evacuation admission

`begin_evacuation` admits exclusion of one Active, engine-pinned node. It scans the
complete rooted namespace and history inventory within the same bounded catalog
turn. One root CAS then records the immutable operation projection, changes the
node to Draining with a higher enrollment epoch, and claims the fleet's single
disruption slot. A saved `FleetInventory` count does not authorize this transition.
Missing imports, omitted namespaces and missing canonical histories refuse before
publication. Native group serving and business-write acknowledgement continue.

The head retains the slot's digest. Missing slot data or a mismatched operation
projection refuses admission; generic deletion/reset cannot make another
disruption appear available. The operation, node and slot projections change
atomically within the ordinary sixteen-change and 4-KiB record bounds.

Persist the exact request identity, expected revision, principal and node. An
immutable, catalog-scoped `operation-requests/<id>` record binds those inputs
before the root CAS. A losing candidate retains that identity permanently and
cannot reuse its operation ID for another preview or request. Its existence
alone grants no exclusion authority. Admission reads also verify this record;
missing canonical request data refuses rather than appearing empty.
An unknown reply must reconcile that identity through the mutation journal. An
already admitted retry retains its original applied revision even after another
namespace is declared; it performs no second exclusion and grants no fresh
inventory assessment. An unadmitted stale candidate remains fenced by its old
root. Replanning uses a new canonical request rather than rewriting that revision.
The original deadline, execution-domain admission and cancellation cover the
scan and conditional write; an expired or cancelled remote PUT can be ambiguous.

`evacuation_admission` reads the rooted original admission and verifies its exact
request and journal linkage. Its inventory revision, counts and node epoch are
historical inputs, not a lease. This increment does not move memberships, stop a
process, declare a node empty/Evacuated/Removed, release a disruption or reclaim
any storage. Those transitions need fresh group/archive proofs, consumed-permit
fences, physical joins and terminal controller reconciliation. Automatic fleet
management remains disabled until the remaining delivery and deployment gates.

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

Membership authorizations also retain their source root revision in memory.
Before a fresh repair starts on a managed host, its actual source and target
processes must acknowledge at least that trust revision through the authenticated
node metadata RPC. The source revalidates the target's process, engine, disk and
rooted epoch immediately before proposing the configuration; the ordinary guard
and configuration checks then run again. This is an admission check, not a lease
or a capacity/removal receipt. Existing committed plans continue under their
exact permit without requiring a new participant acknowledgement.

The preflight holds a 64 KiB control reservation and shares the original request
deadline. Pressure refuses with overload, while stale or missing trust waits at
the directory polling cadence, capped at 250 ms between attempts. Caller
cancellation, local drain and shutdown refuse an unsubmitted fresh action;
shutdown joins its physical RPC owner before releasing resources. Refusal never
clears the root reservation or consumes a permit sequence. Business-write
acknowledgement adds no catalog read or observation call.

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

`RaftCells::observe_peer_fleet_membership(node, authorization, minimum_revision,
timeout)` obtains this barrier from an exact enrolled remote process. An empty
origin needs no dormant registration. A separate purpose and TLS exporter bind
the rooted history, catalog fingerprint, nonce and both physical endpoints. The
target independently reads its current catalog, authorizes the resident history,
performs safe ReadIndex and returns a coherent durable configuration plus local
publication progress. A signed status sample cannot satisfy this RPC. Dormant,
retired, unenrolled or mismatched histories refuse without activation or SQL.

`published_cut()` returns an opaque `FleetPublishedCut` only when the exact
published image marker matches the durable applied marker captured with that
configuration. A cut with `image() == None` proves the canonical empty state;
an absent cut means publication is still pending. Ordinary status inspection
cannot supply this statement. Configuration-only entries can advance the applied
cut without changing SQL; an intact restart can publish the same image at a
later configuration position. `covers()` compares complete prefix coverage at
the same exact catalog/history and membership revision, including these restore
aliases. A changed configuration requires a fresh verification attempt.

The cut is historical. It establishes no current health, archive payload/replay
coverage, reservation release, removal or process-stop authority. The terminal
verifier must independently check the surviving replicas, fully verify the
archive, settle permits and fence or join physical work. Cold canonical-empty
histories use explicit empty archives; the terminal verifier must select and
verify their exact history-bound set rather than waive the archive gate.

`CellArchive::verify_published_at(reference, minimum, verifier, timeout)` now
provides that archive sub-proof. It downloads the exact selected immutable set,
checks complete coverage of the fixed `FleetPublishedCut`, physically replays the
SQL image or verifies canonical-empty state, and reclaims the exclusive scratch
generations before returning an opaque `VerifiedArchiveCoverage`. Metadata-only
reads cannot mint it. Missing/corrupt selected material never falls back to a
newer mutable head. Failed replay quarantines the verifier until its storage
owner recovers it.

The proof binds the fleet/catalog/history, birth/schema, membership revision,
archive reference, applied position, original image-publication position and image state. It handles protocol-only
progress and intact-restore aliases, while a changed configuration requires a
new attempt. The manifest requires `image_position`: populated state has a
bounded non-null publication origin, canonical-empty state has explicit null,
and tail replay refuses an origin preceding its last data entry. A full snapshot
does not manufacture a later SQL publication position. An old archive cannot cover later data, and a later no-op cannot
make a conflicting earlier SQL image appear current. One original deadline
covers object reads, workspace waiting, replay and delivery; completed work
cannot issue a proof after expiry. Blocking work retains its inputs until its
execution owner joins it. The caller owns scratch storage and admission for the
configured recovery-memory bound.

This is historical archive coverage. Candidate preparation consumes this proof
with complete inventory, quorum publications and exact permit settlement as
described below. The terminal removal API combines that coverage with renewed
survivor proofs and exact signed closure before its atomic receipt/node/slot CAS.
Business-write acknowledgement calls none of this metadata work.

The opaque response includes the installed node enrollment and whole invocation
age through SDK delivery. Per-node and per-peer 1 MiB transport reservations cover
bounded decoding; both hosts reserve 128 KiB for retained control responses,
with separate bounded admission for nested quorum/status calls. The original
remaining deadline bounds catalog reads, ReadIndex and response IO. Disconnect,
caller cancellation, process revocation and host shutdown cancel and join owned
work. This grants no business-query, seed or membership-mutation permission.

`fleet_membership_status` and idempotent enrollment replies remain local synced
observations. Neither supplies this invocation's quorum barrier. Likewise, saving
a `FleetMembershipObservation` does not establish current quorum later or grant
a lease. It is an input to complete-import and terminal-operation verification,
not a complete inventory, archive-coverage or node-removal certificate. Rooted
operation sequencing, complete history coverage and physical work joins remain
separate controller obligations.

## Evacuation candidate preparation

`prepare_evacuation_verification(snapshot, operation, namespaces, timeout)` starts
only at the current root. It checks the active disruption's canonical operation,
original journal and exact Draining node record, then internally verifies the
entire declared namespace/history inventory. An incomplete import, missing
namespace handle or reserved reference remains an obligation. The returned
`FleetEvacuationVerification` retains the immutable inventory cut and one pending
history record; its `next_history` walk consumes every rooted key.

For each live history, `verify_live` owns three authenticated safe-ReadIndex calls
through the supplied `RaftCells` observer. Endpoint IDs select the calls; the
responses establish the proof. All three voters must have the same stable RF3
configuration, current Active enrollment, exact node/disk/process and permanent
engine identity, birth/schema, and settled latest rooted permit. Joint voters,
learners, active repairs and unconsumed permits refuse progress. The rooted
physical-host separation policy applies to all survivors. Three coherent
publications must cover the fixed first cut. `freeze_live` retains that exact
historical cut while archive catch-up and ordinary writes continue. Repeated
calls for the pending history return the same cut; a caller can select its
immutable archive afterward. `FleetEvacuationArchive` supplies that exact archive
and owned replay services. `verify_live` reuses the pending cut, or captures it
first if necessary, and physically verifies the selected material. Each method
keeps its original deadline across every awaited phase. Retaining a fixed cut
between calls supplies no freshness lease for terminal completion.

`verify_cancelled` consumes a rooted canonical cancellation already checked
against the immutable namespace decision. Reserved histories block until their
birth decision is reconciled into a new root. Each completed entry goes into a
bounded immutable hash-radix index. Failed, cancelled or expired turns retain
the pending history; immutable orphan pages grant no progress. The caller owns
scratch storage and recovery-memory admission, separately from these bounded
metadata pages and the existing transport/control reservations.

The verifier rechecks Policy, Nodes, Histories, Admissions and the disruption
slot around storage work. A change invalidates the attempt. Unrelated journal
or Operations progress can advance without changing that verification basis;
the original active operation and canonical journal linkage are revalidated at
each check. Missing or corrupted operation bytes refuse progress. Survivor calls
are sequenced through the bounded metadata authorization turn, with their
original deadline and typed observation failures retained.
After confirming inventory exhaustion with its final bounded page,
`publish_candidate` creates and confirms a digest-addressed
manifest binding the index, operation, excluded node/epoch and inventory cut.
Repeated publication yields the same candidate. It leaves the fleet root, node
state and disruption slot unchanged.

A candidate records historical verification. Its stored bytes cannot recreate
the runtime's proofs after a crash, authorize a stop, release reservations or
establish terminal removal. The terminal API below consumes renewed runtime
verification and signed joined-owner closure while retaining physical occupancy.
It updates the receipt, node and disruption slot atomically. Business writes continue
without calling candidate preparation or coordination storage.

## No-new-boot finalization boundary

`FleetEvacuationCandidate::begin_finalization` rereads complete inventory and the
active disruption, owns an authenticated probe of the departing process, then
atomically changes its exact Draining node to Finalizing and roots the canonical
finalization projection. The projection binds the historical candidate, engine,
disk, process, boot claim, operation and new enrollment epoch. The original
deadline is checked immediately before the head CAS. A restart admitted first
invalidates the candidate and defeats that CAS; a Finalizing winner refuses even
an intact engine boot with an older saved host authorization.

Exact committed retries reconcile through the permanent journal without a new
probe or namespace handles, including after adapter recreation. Reusing the
request with different inputs refuses. A legitimate Draining restart may prepare
a new candidate only after its newer boot claim is journal-confirmed. The old
candidate cannot authorize the newer process.

`FleetFinalizationAdmission` is an opaque historical observation of this boot
fence. It grants no physical stop, terminal removal, resource release or volume
deletion authority. The existing process remains owned and the disruption slot
stays claimed. Completion requires the terminal API below; this admission alone
cannot release that slot.

## Scoped physical stop and joined closure

`FleetFinalizationAdmission::stop_request` produces untrusted request data for
one operation, finalization request, stop request, node epoch and exact boot,
process, disk and engine. `Node::request_fleet_stop` submits it to the actual
Node's bounded lifecycle task. The receiving host verifies the canonical active
Finalizing projection, permanent journal, disruption slot and target row against
the current root. New stops also require unchanged policy, history, admission
and complete-inventory inputs. A changed inventory refuses new stop work.

Every managed boot pins its per-generation closure verification key at the root,
then fsyncs its exact boot binding in a separate bounded engine record before
endpoint publication. The same serialized engine worker fsyncs stop acceptance
before the Node-owned task signals Preserve shutdown. A disconnected caller or
lost acceptance response cannot cancel accepted work. Expired queued work cannot
start an effect; a started sync can still commit after its response deadline.
Reconcile the original request rather than assuming a timeout means no effect.
Accepted retries retain one remembered shutdown-signal delivery outcome. They
cannot accumulate duplicate signals while the driver waits for physical joins.

`stop_fence_request` narrows local authority at the same epoch. Its synced guard
defeats a delayed stop; an already accepted stop refuses cancellation, even with
a higher local epoch. Exact retries retain the original meaning. A newer rooted
boot requires a fresh nonce and a higher epoch; reusing a nonce with different
fields refuses. The local fence does not cancel the root operation, restore
eligibility or release its disruption slot. Finalizing resumption remains work
for the controller and terminal protocol.

The outer embedded/CLI host publishes an immutable signed closure only after
joining the full Node execution domain, Raft engine and physical owners. CLI
also joins its core thread first. A separate bounded reporting domain owns the
conditional publication and exact readback, including a lost PUT response.
The private signature domain differs from peer-probe responses; editing bucket
bytes or a deserialized completion flag cannot mint a verified closure.
`verify_physical_closure` binds the signature to the boot-pinned key and exact
rooted stop selector and returns an opaque `VerifiedFleetClosure`. This assumes
trusted Cellar host code and trusted coordination credentials; it is not an
attestation against a malicious host holding its private key.

This evidence establishes joined Raft/Node ownership for that boot. It does not
establish current survivor health, archive catch-up, application completion,
legacy graceful-reload proof, terminal removal or permission to delete a volume.
An unplanned shutdown or a fence without an accepted stop produces no closure.
Failed publication or an unreachable process remains unconfirmed and retains
occupancy. Only the terminal verifier below can consume this closure together
with fresh survivor and complete-inventory evidence to release the disruption.

The existing authenticated, generation-bound private management endpoint accepts
`NodeAction::FleetStop`; its acceptance is explicitly `physical_closure:
unconfirmed`. Generic management Stop refuses managed Raft hosts. The admin
adapter requires Operate permission and identical guard/audit request IDs, and
records acceptance rather than terminal success. Its discovery still depends on
the legacy Actor lease; complete operator workflows remain pending.

`RaftCells::request_peer_fleet_stop` supplies the standalone Raft control path.
Its origin and target may both have zero business groups. The origin resolves
only rooted exact node views at the requested minimum trust cut. A distinct TLS
exporter and MAC purpose bind the request/reply, body digest and exact physical
route. The origin boot comes from its synced engine guard; new receiver work
also verifies that boot against the current root and permanent boot journal.
This creates no cell, voting identity or query/seeding grant.

The original origin deadline covers queueing and transport. The receiver anchors
its advertised remaining budget at TLS admission, including prefix/header/body
residence, and passes that same absolute Node-domain deadline to the owned stop
worker. Its input queue, transport tasks and node/peer metadata reservations are
bounded. Dropping a sender or losing a reply cannot undo receiver-admitted work;
there is no hidden network retry. Responses preserve typed overload, conflict,
refusal and unconfirmed outcomes. A returned acceptance is historical local
admission only; use the signed closure verifier for physical joins. In-flight
work already admitted before caller revocation remains owned, and cached accepted
stops do not grant a new effect.

Native coverage includes empty origins/targets and an actual S3-seeded voter
replacement followed by rooted finalization, Raft-control stop concurrent with
survivor writes, retained-history owner join and signed closure. This still
supplies no terminal node transition or disruption-slot release.

## Terminal removal and recovery

`prepare_evacuation_verification` also resumes a rooted Finalizing operation.
It loads the original fence, rereads complete inventory and owns new quorum
observations and physical archive replay. For an unchanged history/configuration,
the original candidate supplies the fixed publication prefix; a current actual
quorum publication must cover it before that prefix is re-established. A changed
configuration or new history requires a new verification cut. This refresh
preserves the original stop request and stopped boot.

`complete_evacuation` requires that runtime verifier and a `FleetRemovalRequest`
identifying the exact signed stop closure. It refuses incomplete or changed
inventory. After archive work, a bounded pass renews safe-ReadIndex proofs from
all three replicas of every live history at the same fixed business cut. It
rechecks configuration, exact boots and settled permits without repeating image
replay. Only a complete successful pass refreshes the health clock. It then
reprobes every surviving process and checks proof age again immediately before
CAS. Health proofs older than 60 seconds, including queue and caller residence,
refuse. An attempt that cannot
finish within this freshness bound must repeat verification. Supported density
and convergence rates remain qualification work.

One root CAS writes the removal receipt, advances the exact Finalizing node to
Removed and clears the shared disruption slot. The disk/engine/boot tombstone,
physical histories and resource reservations remain. Public generic catalog
mutations cannot delete the disruption slot or manufacture runtime verification.
Exact committed retries reconcile the permanent request journal before requiring
live verification or namespace handles, including after controller recreation.
Changed request identity, stop or payload refuses. Orphan candidates grant no
completion, and uncertain physical closure cannot release the slot.

A controller that is itself one of a group's survivors uses the existing guarded
local safe-ReadIndex operation, current rooted history authorization and charged
reply retention. Locality grants no membership or removal authority.

## Capacity readiness and activation

`admit_node_capacity` owns a complete current inventory scan and an authenticated
node inspection. It atomically records the exact request and changes the target
to Ready or Active. Qualifying a spare keeps it excluded; activation repeats the
readiness checks. Neither operation creates a business cell. Their original
deadline covers the inventory, RPC, immutable request work and final CAS.

The probe reports the synced boot nonce/key and the installed host, consensus
and engine group bounds. Its buffer total covers the reported configured
host/consensus/log pools; it is not process RSS or a complete process-memory
estimate. The explicit deployment attestation supplies CPU, memory, storage,
engine-slot and no-swap limits, retained usage, a profile revision and a retained
evidence reference/digest. This is labeled operator attestation; there is no
independent kernel/container measurement in this adapter. Missing, conflicting
or unfit evidence refuses admission. One profile revision cannot change its
limits, and an already qualified Active boot requires the maintenance protocol
before changing limits.

Exact committed retries reconcile through the permanent journal before requiring
live inputs. A retry can therefore return historical Active acceptance after
exclusion or shutdown. It supplies no current placement authority.
`authorize_capacity` separately verifies the current projection, node epoch,
exact boot and original journal. A new boot invalidates old capacity even when
its process hint is reused; intact engines may qualify again. Generic metadata
saying Active without a matching readiness record is not quantified capacity.

The capacity authorization is bound to its current root and is not a health
lease or a resource reservation. The fleet planner must combine it with fresh
occupancy, running profile/startup bounds, per-history eligibility and existing
reservations. Actual host initialization still enforces its own bounds. The
reservation planner and complete operator workflows remain pending; this does
not enable automatic fleet management or certify deployed resource limits.

## Atomic resource debit admission

`reserve_resources` consumes current typed capacity, complete rooted inventory,
an explicit deployment occupancy attestation and an owned authenticated node
probe. The probe must name the exact qualified boot, engine, process, running
profile and current enrollment cut. Its original age is rechecked immediately
before the expected-root CAS, within the original operation deadline.

One root CAS installs the immutable journal-linked resource row and the bounded
active movement book. Independent adapters cannot both consume a target's last
slot. Engine capacity uses the smallest installed host/runtime/engine/profile
and attested bound; durable capacity uses the smaller profile/platform bound.
Memory and disk occupancy remain explicitly deployment-attested, with additional
floors from actual engine occupancy, configured byte reservations and installed
buffer bounds. These floors do not measure RSS or complete filesystem usage.
The submitting trusted operator retains the referenced attestation evidence.

The shared pure ledger charges existing physical high water and every unresolved
incremental peak before the candidate. Earlier boots and profiles retain their
full debt. The initial execution profile admits two active debits fleet-wide,
with at most two on one node. Queued plan entries are not active debits. Recovery
headroom and final/staged assignment feasibility remain planner prerequisites;
this resource-only boundary does not establish either.

Missing permit resource attribution refuses instead of inventing zero debt.
An initial voter reference may still initialize after its birth becomes Live.
Admission now owns a complete physical inventory probe and matches every target
genesis reference against its exact scope, birth digest and original member.
A Reserved birth or a Live birth without its durable voter still blocks new
work. A materialized voter remains charged while unused capacity can admit
another history. Any occupied binding for the prospective learner's exact
history refuses, including a retired or unregistered engine history.
An in-progress initializer also refuses. The accepted resource row retains the
canonical inventory digest alongside its exact boot/profile and input-root cut.
`reserve_movement` adds an exact `RepairPlan` and source node to the resource
request. It owns a fresh source safe-ReadIndex under the same original deadline
as the target probe. It checks stable RF3 configuration, the latest settled
permit, membership revision, member high water, exact history/birth/schema,
source boot/process/engine and all staged voter failure domains. One root CAS
installs the resource row, movement book and matching membership permit.
The returned opaque authorization still requires group consumption and actual
learner initialization. A minority source cannot authorize this admission.

Resource-linked permits cannot be deleted or replaced by generic mutations.
Every existing admission must have exact resource attribution; unknown costs
refuse further work. The canonical journal request binds both resource evidence
and the complete movement intent. Exact retries can return the original permit
after target shutdown or adapter recreation; they grant no current health lease.
The complete final/staged placement and failure-headroom planner remains pending.

Exact committed retries reconcile from the permanent request journal before
live probes, including after exclusion, target shutdown and adapter recreation.
Changed payloads refuse. A deadline drops the owned metadata/CAS waiter; an
ambiguous result still requires exact journal reconciliation. Generic metadata
mutations cannot delete or replace earlier resource rows or reset their book.

`FleetResourceAdmission` is historical debit acceptance, not a capacity lease,
membership permit or learner-initialization capability. No business
acknowledgement calls this component.

### Successful movement completion

`complete_movement` owns a new target safe ReadIndex and complete physical
observations of the target and all three original voters. Admission pins every
possible seed provider's exact boot and member, even when the learner selects a
different source from admission's quorum observer. Completion requires the exact latest permit, locally
durable Complete phase at its consumed sequence on all four owners, stable RF3 with
the replacement present and obsolete member absent, current enrollment and
failure-domain separation. The original deadline covers every probe and CAS;
the full probe cohort must remain within the five-second sample bound.

Seed archive jobs retain a separate work count until their actual futures exit,
including after their read waiter expires. Peer snapshot capture and transfer
retain their existing source reservation. All owners must report no outstanding
seed work, initializer or pending history registration. Locally completed
membership forbids a new seed grant for this already promoted learner. A retired
voter can prove local completion without obtaining a new source quorum. A local
physical inspection follows bounded host control and current trust validation,
so the controller may run on either endpoint.

One root CAS records a permanent completion receipt, advances an independent
physical high-water floor and removes only the exact original reservation from
the bounded active book. It retains the consumed permit and original debit and
request journals. Subsequent admission charges that floor plus all remaining
unresolved work; moving a replica never credits retained source slots or bytes.
Generic mutations cannot forge or erase a completion or floor. Exact committed
retries reconcile before live prerequisites, including after target shutdown;
changed request payloads refuse.

This boundary completes successful moves only. Aborted or unstarted moves,
unreachable/rebooted owners and uncertain physical work retain their debt. Their
terminal cleanup, complete assignment planner and automatic reconciliation remain
subsequent implementation work. Completion is not a deletion or capacity lease.

## Complete physical history observation

`RaftCells::inspect_peer_physical_inventory` uses the authenticated node-control
route and an explicit MAC-bound request mode. It does not need a business-cell
registration, Actor lease, SQL image or live Raft quorum. Its compact sorted
entries contain the full-scope digest, immutable birth digest, physical member
ID and locally completed permit sequence from the append-only engine catalog
and durable per-group cache. Retired and unattached histories remain
present. Unknown local histories still consume the reported physical slots.

The snapshot is bounded to 4,096 durable histories. Its maximum binary body is
344,109 bytes and fits the existing 1-MiB node/peer metadata reservation. Host
response ownership also reserves 1 MiB before capture and retains that charge
through SDK delivery or cancellation. Ordinary node observations retain their
smaller response reservation and omit inventory explicitly. Omission is not an
empty inventory. The inventory codec checks counts, exact length, ordering,
duplicate/zero identities and pending-initializer accounting before admission.

The driver captures entries under one engine-catalog lock with short per-group
cache reads, then checks the accompanying node counts. A racing sync that produces a mixed count cut
refuses. The pending initializer is explicit even after its slot becomes durable;
it cannot disappear as apparently unused capacity. No lock crosses asynchronous
work. The receiver's original budget includes header residence. Retained SDK
responses keep their original age; observations grant no membership, absence,
health lease, reservation release or data-deletion proof.

## Host API and fresh learners

### Rooted node enrollment

`register_node` adds a bounded typed descriptor in the `Nodes` index with exact
request identity and CAS reconciliation. `authorize_host` returns an opaque
capability for that rooted node/disk, physical-host identity, endpoint name and
compiled-capability digest. Boot refreshes it against the current root and
refuses missing, Finalizing, evacuated, removed or incompatible records. Registration does
not activate capacity or certify a complete managed inventory.

An existing engine can be pinned in the registration request. Every managed boot,
including an intact engine restart, claims a fresh boot nonce and process through
one rooted node CAS before endpoint publication or group startup. The claim pins
the engine and closure verification key, advances the enrollment epoch and retains its exact
request/journal binding. Unknown replies retain that nonce and payload; the
adapter does not hide CAS retries or rebind a failed request to a new revision.
Concurrent boot conflicts fail startup cleanly and require an explicit new startup
attempt after joined cleanup. Changing namespace catalog prefixes cannot make a
replacement engine satisfy the fleet pin. The initial node-table bound is 64
retained records, including removed identity tombstones; exhaustion refuses
registration and does not collect identity history.

Directory trust binds peers to the rooted boot process. Running-host refreshes
also require their own original boot claim; another process's later claim cannot
revive an old publisher. Ordinary lifecycle state changes retain the boot claim
and its original journal identity even while advancing the node epoch.

A boot replacement cancels old physical connections and grant owners. Verified
new discovery recreates connection pools for retained group/query obligations
and refreshes the same logical grants. Empty metadata-only discovery creates no
business pool. Final Removed still revokes the logical grants and cannot be
undone through a later advisory view.

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
The exact catalog fingerprint is mandatory on the authenticated metadata route.
An independently initialized catalog with the same fleet ID, physical records
and revision numbers cannot acknowledge this catalog's trust cut. Trust snapshot
reuse and successor installation also reject another catalog scope. Rooted
transport trust permits only this node metadata operation; it does not create
namespace inspection/query grants or group authority.

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
`/catalog-trust-1/fleet-admission-1/fleet-quorum-1/fleet-cuts-1/empty-archives-1/archive-cuts-1/fleet-finalization-1/fleet-stop-1/fleet-stop-rpc-1/capacity-readiness-1/resource-reservations-1/physical-inventory-1/fleet-removal-1/resource-movement-1`.
Node records require an explicit nullable `boot` field; Finalizing is a boot-refusing
state. The engine requires a separate `cellar-node-lifecycle` record, initialized
atomically with a new engine root. Resume refuses a missing record rather than
recreating it. Older namespace protocols refuse startup; there is no legacy reader or migration.
Use fresh coordination/data prefixes, namespaces and engine directories with matching participants. Do not downgrade a
guarded engine to an older binary or remove the guard field to regain admission.

`native_raft_fleet_catalog` verifies the bounded conditional adapter.
`native_raft_fleet_permits` exercises native SQL hosts, real TLS, stale capability
replay, quorumless fences, both-majority joint recovery, retained-engine reopen,
peer/archive seed metadata and legacy ingress refusal. Core member-state tests
check strict decoding, scope and snapshot successor constraints. These tests do
not qualify full automatic fleet operations, physical RustFS semantics, the
resource-limited Colima topology, steady-state overhead or production capacity.
