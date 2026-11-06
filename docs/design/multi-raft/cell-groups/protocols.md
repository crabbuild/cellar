<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Durable protocols and crash cuts

Purpose: Durable enrollment, apply, topology, hibernation and disaster-recovery protocols.

Status: required target behavior. Names below are contracts to implement.
The [design](design.md) defines identity, partitioning and storage. A catalog
record is never a vote, a majority proof or infrastructure fencing evidence.

## Invariants

| ID | Required invariant |
| --- | --- |
| I01 | Cell, group, incarnation, route and schema identities are checked independently. |
| I02 | Every acknowledged Quorum output has a real persisted majority and a published exact cell effect. |
| I03 | Receipts and cell versions cannot be borrowed across cells; retries preserve identity through movement. |
| I04 | Durable group apply is contiguous and references a complete synced image/manifest closure. |
| I05 | Strong reads use current-term quorum authority and a coherent target cell image. |
| I06 | S3 acknowledgements require a retained complete group checkpoint/tail closure. |
| I07 | Reclamation preserves every live group, snapshot, reader, archive and transition dependency. |
| I08 | Membership changes preserve quorum intersection, disk lineage and failure-domain constraints. |
| I09 | Split/merge admits at most one writable owner of every cell, including after lost replies. |
| I10 | Hibernation never manufactures authority or bypasses a fresh wake/read barrier. |
| I11 | Zero-quorum recovery fences all possible old writers and creates fresh incarnations. |
| I12 | Work, memory, file descriptors, catalog scans and shutdown remain bounded and owned. |
| I13 | Alarms/outbox/inbox preserve committed intent and stable effect identity across movement. |
| I14 | Release claims are tied to exact source, actual tests, workloads and infrastructure. |

## Physical group observations and handoff

`RaftCells::group(GroupHistory)` selects a local physical log; application
commands and queries continue to use independent `CellHistory` handles.
`RaftGroupHandle::observe(timeout)` samples the actual owner and its durable
cache in one serialized turn. It reports replica identity, role, membership
revision, persisted/committed/applied progress, installed checkpoint, retained
entry count and applied-manifest cell occupancy. A group containing two cells
still contributes one group replica. Retained entries are not a disk-byte
measurement. This metadata grants no quorum, serving or compaction authority.

`archive_status()` reports the archive controller's last verified publication
independently of the owner sample. Absence means no installed archive owner;
a publication is historical metadata, not a retained coverage proof. Local
compaction continues to require the existing physical dependency verification.

`transfer_leadership(LeadershipTransfer, timeout)` preserves the caller's exact
term, membership revision and target. The actual owner rechecks those guards,
incoming-voter membership, readiness and matched progress before admission.
Its deadline starts at invocation, including time before polling, and is checked
again after queuing before mutation. Success establishes admission only;
observe the actual election and obtain fresh application authority afterward.
Expiry or close after dispatch does not establish whether a handoff was admitted.
Observations and guarded transfer reject zero or greater-than-60-second budgets
and closed owners. They add no worker, per-cell Raft instance or admission bank.

## Enrollment

Resolve the current range, materialize its group birth if absent, and obtain
current-term leadership. Propose `Enroll(CellHistory, schema, initial_image)`
under that route epoch. The group checks the key is in its active range and no
other live incarnation has enrolled that ID. The command is idempotent by its
stable creation identity. Only committed/published enrollment becomes visible.

Persist the original creation operation, logical cell history, schema and initial
image commitment atomically with enrollment. Retain this creation record through
later mutations and normal transfers; comparing a retry to the latest image would
confuse a successful earlier creation with a conflict. The initial commitment is
provenance, not a requirement to retain the superseded initial image forever.
Snapshots and archives carry the record with the selected current cell state.
Initialization runs on the bounded node SQL workers under actual-leader authority.
The compiled initializer has an explicit contract digest in the class voting
schema. It receives the logical cell history and captured host inputs through its
private transaction. A host that cannot execute that contract must refuse the
registration instead of silently omitting initialization.
Reserve the logical cluster/namespace/cell name before entering the callback;
competing incarnations must not obtain independent producer leases. Capture initial
SQLite/LTX bytes once, carry the preparation term with the output, and check that
term on the RawNode owner atomically with proposal admission. A node that loses
leadership and later regains it must reject retained output from its older term.
Cancellation retains physical ownership until the callback and cleanup complete.

Before append, reserve group capacity against applied cells plus accepted pending
creations, keyed by logical name. Retrying one creation consumes no extra slot.
Retain bounded leader-local acceptance records independently from Ready/completion
budgets. An exact pending proposal retry returns its original acceptance, never a
second log entry; changed bytes under that identity refuse. After apply or leader
replacement, resolve the receipt/creation record rather than append an old base.
A stale-base refusal does not prove non-commitment. New leaders must apply their
current-term barrier before admitting creations against recovered capacity.

A definitive owner capacity refusal before append may retire a preparation only
with an exact preparation-bound rejection proof. Issuing that proof irreversibly
seals every clone against later submission, even after capacity returns. A generic
queue error, lost response or same-term receipt absence proves nothing about an
earlier attempt. Remember an accepted preparation's original position/digest;
returning that acceptance again grants no commit or current authority. Keep raw
byte submission closed once the owner receives prepared SQL work so queued raw
retries cannot bypass the seal. A proof for another engine, cell or replacement
preparation refuses even when the business proposal ID matches. Cleanup closes
the private writer on its worker before releasing logical-name admission; queued
submissions and retained outputs/proofs keep their byte reservations.

Admission decisions must be checked again in committed log order against the
current lifecycle state. Recognizing an exact retry grants neither current-term
authority nor permission to restore the initial image over newer mutations.

| Crash/collision | Recovery |
| --- | --- |
| Birth CAS result unknown | Read the canonical birth/mutation journal; never allocate competing voter history. |
| Schema image synced, enrollment uncommitted | Keep it unqueryable; replay/reconcile the same request or collect an orphan after all pins expire. |
| Enrollment committed, response lost | Return the recorded cell history/result on retry; do not run initialization twice. |
| Concurrent creators | One committed enrollment wins; mismatch refuses rather than changing class/incarnation. |
| Split seals during preparation | Revalidate before proposal; an accepted-before-seal entry belongs to the frozen prefix. |

## Routed read RPC

Use the existing node-owned query lane, with independent control/data progress,
bounded peer/node admission and joined physical shutdown. Authenticate both the
physical group and logical cell histories, schema, route epoch, birth identity,
destination member and node/disk/process identities. Query origins identify a
node process; a namespace read grant never authorizes a mutation or Raft message.
Grant revocation cancels its in-flight read waiters. A destination still requires
live membership and the actual cell's read authority after routing checks.
Separate host registration admission from data retained by accepted queries.
Dropping the final host registration must refuse new reads even while an earlier
request still owns its gateway data and physical SQL work.

Allocate authenticated wire sequence numbers in the single peer sender after
dequeueing. Concurrent callers can enqueue in a different order from admission;
that must not cause a valid request to be refused as a replay.
When the peer closes its bounded idle header read, the sender retires the
connection before sending the next queued request. An idle connection has
no valid response bytes. Once an exchange starts, a transport error still
preserves the original mutation's ambiguity.
Consume each catalog attempt once. Carry its remaining duration across hosts;
never compare unrelated monotonic clocks or replenish the original caller's
deadline/attempt budget. Confirm relocation hints through the trusted catalog.
Return the selected cell observation and authenticated cell token, never the
group's newer log cut. The caller independently enforces an AtLeast minimum.
Retain result admission until consumption, and retain entered SQL/image owners
through physical cleanup after disconnect, timeout or cancelled shutdown.

Invocation routing shares a bounded namespace range-hint cache across query,
command and enrollment dispatch. Configure both entry and reserved-byte bounds;
clones share the same cache, and cell count does not allocate one entry per cell.
Hints are learned only from verified directory lookups. An observed successor
head clears older hints; a lookup completing from an older head cannot refill
that cache. Same-revision/different-digest observations refuse. Explicit refresh
runs in the owning execution domain; failed refresh preserves only historical
hints, never a freshness or serving claim. Redirects still require exact catalog
confirmation under the original deadline and attempt budget.

The current shared gateway resolves the destination catalog route for each RPC.
Removing that destination read depends on replicated transfer sealing and
admission invalidation. The invocation cache does not relax the current check;
this intermediate behavior does not establish steady-state request latency.
Attached-peer and explicitly discovered nonmember reads share the same query
lane. Nonmember discovery creates no local replica and grants no voting or write
authority. It consumes the same bounded route/peer registries, participates in
node-process refresh and endpoint consistency, and releases unused pools when
removed. A replaced discovery view refuses captured requests and late replies;
removal does not revoke the destination's independently managed namespace grant.
Grant revocation cancels read waiters without releasing physically entered SQL.
The public host supplies trusted discovery and explicit permissions. Automatic
SDK reads use the local replica when present and otherwise try configured members
within the original budget. Dynamic membership refresh for ingress remains
integration work.

Remote commands use the same bounded application RPC lane with a mandatory,
MAC-bound command purpose and distinct payload/result markers. A read grant or
voter membership never grants write admission. `NamespaceCommandOrigin` supplies
an explicit namespace/process permission with bounded revision tombstones;
endpoint checks, rooted process refresh and fleet revocation include these grants.
A host-owned command gateway binds the exact node controller. Dropping its final
registration closes new admission without rolling back accepted work.

An ingress process can probe configured peers for a ready leader before
submitting a command. Probes use command permission and bind the complete route.
They open no SQL work. The origin retains time for a single mutation exchange
after destination selection.

Carry the caller's captured time/random inputs, command identity, request contract,
retry horizon and remaining deadline to the destination. The controller verifies
a fresh catalog route against the transmitted group, schema, epoch and birth,
then checks actual leader authority before entering SQL. It owns accepted work
independently from the connection. Disconnect, revocation and timeout can suppress
a reply but cannot prove non-commit or release a producer. Success contains the
exact committed command identity/result and an authenticated logical cell token;
conflict, expiration and sequence-gap refusals remain typed. Retain caller reply
byte admission through consumption. Retry the same identity and input under the
original budget; a committed duplicate must not re-execute application code.
Originating SDK enrollment uses the same bounded leader selection as commands.
Incoming enrollment always executes locally. The SDK retains a remote creation
record as authenticated transport output and preserves local SQL and archive
ownership separately. Remote enrollment uses the same bounded write exchange,
namespace and process grants, and exact controller registration. The mandatory authenticated purpose distinguishes
Query, Command and Enrollment under marker 1. A command payload cannot be decoded
as an initializer call. Capture creation identity, host inputs and preview identity
before discovery; retries retain them and the original route/deadline budget.
Receiver enrollment verifies a fresh catalog binding and actual leader authority
before initialization. Its admitted controller owner survives disconnect,
revocation and response deadline. Return only the exact committed creation record,
including the original enrollment image after later mutations. That record is not
a current-version token. Conflicting creation/incarnation refuses without replay.

Standalone receipt lookup has its own authenticated purpose and requires the
explicit command-origin grant. Read permission alone cannot inspect command
results. The exact registered controller supplies the runtime and SQL pool;
receiver admission checks fresh catalog binding and selects a correlated quorum
read. Registered contract, payload digest and authenticated command identity
validation precede receipt inspection. Immutable reads use the existing worker,
read admission, cancellation and deadline owners without a private writer,
initializer, command callback or receipt maintenance.

A retained receipt returns its exact result/retry horizon with a signed cell
observation. An absent receipt remains ambiguous, even after a quorum read:
original delayed or unresolved work may still commit. Lookup never releases a
producer or produces a NotCommitted proof. Preserve replicated expiry/conflict
and sequence-gap failures. Only a sequence-1 image can lack both receipt tables;
partial schema or later schema loss refuses as corruption. Missing and an empty
committed result have distinct wire encodings and API results.

The controller accepts bounded dynamic group registrations, including startup
with an empty roster. A registration supplies the exact bootstrap permit, verified
selected root, applier owning the same manifest page store, and physical peer
route. Pending and active groups share the group limit; pending work and unconsumed
replies also retain the existing item/byte budget. A pre-existing physical route
cannot be silently adopted. Cleanup removes only the exact registered route and
cannot erase a replacement view.

The worker installs the applier before polling runtime attachment so early
consensus events cannot escape ownership. Attachment revalidates catalog claims;
only successful completion publishes application roster membership, and serving
still requires fresh current-term quorum. Dropping a registration waiter cannot
cancel this work. Definite failures release their slot before responding. Shutdown
joins entered catalog/attachment futures before closing transport, SQL and the
runtime. A competing external runtime owner or runtime-wide failure still closes
the controller conservatively. A group-local apply/runtime failure quarantines
that group: admission refuses, execution authority is revoked through reserved
runtime control capacity, and unresolved producers/apply tickets remain owned.
Quarantined groups retain their roster capacity until joined restart. Late shared
persistence completion cannot restore their serving fence. Shared log-engine
I/O/corruption/closure and lost shared SQL worker replies still close the node.
Request-driven provisioning/lazy birth and configuration lifecycle remain open.

The current lane serializes RPC exchanges per peer; this is not load qualification.

## Whole-group membership

The shared owner accepts `CellGroupMembershipChange` through bounded runtime
admission. Its marker-1 domain binds the complete route and the observed membership
revision. Explicit changes use V2 entries; legacy V1 and unbound changes refuse.
Decoding bounds wire bytes, repeated changes and context before protobuf allocation.
New member IDs must exceed the permanent high-water mark and first enter as
learners with node/disk bindings. A removed ID cannot be reused. Promotion requires
the leader to observe that learner replicated through its durable committed cut.
These checks do not provision a fresh learner or grant it serving authority.

Configuration entries occupy the same ordered apply lane as cell mutations. Pure
replay computes the next Raft configuration and membership without changing the
live RawNode. The complete group root advances its applied position and membership
revision while preserving every cell leaf and version. Engine persistence validates
the committed entry and atomically syncs configuration, membership and root. Only
the matching persistence completion changes live Raft progress and advances apply.
Explicit and implicit joint changes are supported; Raft's context-free automatic
leave is accepted only while the persisted configuration requests auto-leave.

Admission returns a log position and digest, not a committed membership proof.
After a lost reply, inspect committed membership instead of rebinding a new revision
and blindly repeating the operation. A new binding may precede discovery of its
physical process route; missing peer routes are retryable stale-peer failures.
Authentication and active membership still gate transport admission.

`transfer_leader` admits a handoff only to another caught-up incoming voter after
local committed apply settles. It revokes captured producer authority during the
handoff; observe the new leader's current-term readiness for completion. Removed
or demoted leaders lose producer and leader-bound read authority at synced cache
publication, even before the owner consumes its completion. Membership alone never
allows a learner to produce writes.

`CellGroupRuntime::seed_authority` selects a complete applied root only after a
fresh correlated group ReadIndex. It works without an enrolled cell. The target
must be a committed new learner with the exact node/disk binding, outside both
voter sets. The selected cut covers the barrier and its membership revision.
Pending requests and retained roots share the bounded node/group read budgets.

The non-cloneable authority captures the exact selected root through the node's
checkpoint pool on a blocking owner. If apply advances before capture, the caller
must obtain a fresh authority. After capture, ordinary cell writes may continue
while the source streams its immutable cut. Membership changes, term changes,
leadership transfer, shutdown and deadline expiry revoke the source. Every sink
call has checks before and after it; the final completion also checks authority.
An incomplete or revoked stream cannot produce a successful End. The source API
binds the requested process identity; `prepare_group_seed` authenticates it through
a separate TLS-exporter-bound seed exchange. Requests, grants, readiness and chunks
have distinct marker-1 domains. The reply binds the request tag, nonce, complete
physical route, fresh barrier and canonical checkpoint descriptor. Chunk sequence,
digests and explicit End are authenticated within that exchange. Peer replacement,
transport shutdown and the original deadline revoke pending destination work.

The marker-1 request requires an explicit `from_s3` flag and the authenticated
reply requires a tagged `data` choice: peer bytes or an archive upload ticket plus
exact checkpoint reference. Missing fields and a choice different from the request
refuse before staging. The S3 mode opens the established namespace archive registry;
a peer cannot supply a backend or override its identity. It never falls back to
peer checkpoint bytes.

Managed startup validates its saved host capability and prepared namespace
declarations against one current catalog root before opening mutable engine
state. An already quarantined/finalized host refuses without reopening its log;
the later engine-bound boot CAS still orders admission against concurrent fences.
Managed physical route refresh discards a retained initial-voter endpoint only
with rooted permanent isolation, even when durable membership has not changed.
This permits an admitted learner's route to replace an unusable old endpoint
without removing votes or seed-owner/resource debt. Movement completion may use
that isolation proof only for the exact obsolete boot, after fresh stable RF3
excludes it. Surviving original voters still need live physical-inventory and
worker-exit proofs; settlement leaves the obsolete disk and disruption retained.

Managed live voting observations share one original deadline. Each read-only
member attempt receives a share of the remaining time, reserving time for other
members and final validation. A lost reply retains the unverified history and
allows the owning walk to retry fresh observations. No partial member set or
historical sample can satisfy the final proof; all three safe ReadIndex/durable
apply observations and current rooted inputs remain required. Mutation ambiguity
and retry rules are unchanged.

The explicitly started Node-owned failure service can drive exclusion, exact-boot
platform isolation, S3 replacement, fresh RF3 and terminal survivor/archive
verification. Its private verifier initializes durable scratch storage before
receiving image bytes; serving pins cannot satisfy this proof. Recovery/removal
phase futures remain owned under the original admission, stop and turn deadline.
Shutdown joins a pending platform callback. Restart reconciles the rooted
operation; archive-read failure blocks progression and retains the intent.
A later controller repeats physical S3 replay before
the terminal CAS. Removed records retain the old engine and disk; they do not
authorize reclamation or prove a production platform fence.

An S3 source captures the same fresh-quorum, exact-root checkpoint before catalog
I/O. A separate purpose-bound, exclusively locked local journal syncs the reserve
intent before allocating a generation and then syncs the returned upload ticket.
The pending generation and inherited parent graph protect every checkpoint object
through handoff. This transfer publishes neither a disaster-recovery head nor S3
command coverage. The destination checks that protection, independently fetches and
verifies all objects and feeds the existing physical checkpoint receiver. Only a
complete prepared checkpoint matching the authenticated snapshot permits READY1.
The source rechecks live authority before authenticated End. The ordinary engine
installation, fresh destination boot/catalog checks and promotion/catch-up fences
remain required.

The source retains its admitted transport, read, checkpoint and S3 resources until
physical upload and handoff cleanup finish. The destination's prepared stores own
their local pins before READY1. Caller cancellation cannot release entered work;
node close stops new source admission and joins transport with source cleanup.
An uncertain ticket retirement preserves its durable journal and observable cleanup
error. A later seed attempt reconciles that exact intent before allocating again.
A node-owned background sweep also discovers existing seed journals during idle
operation and retained restart. It streams one directory entry at a time and
checks the canonical history path, journal purpose, engine creation and exact
immutable namespace archive binding. Cleanup may outlive the selected birth;
reopening that binding grants no current membership or serving authority. Missing,
corrupt, foreign-engine or wrong-purpose state refuses without initialization.

Live sources and cleanup share a per-history lock. A sweep skips an entered
source, never expires its protection by age, and retains its lock through entered
physical I/O, including panic/error joins. It resolves the original reserve intent
or upload ticket idempotently, confirms abort, then syncs the empty local record.
An unavailable registry or unconfirmed abort preserves the intent and observable
debt for another pass. Bounded error tracking does not stop other journals from
being examined. Empty journals need no remote request. A lost journal or engine
cannot authorize retirement of an unknown remote owner.

The immutable provisioning configuration charges one 272-KiB journal workspace
to the existing node preparation budget. One sweep runs per node, with one
ordinary 8-KiB S3 admission per journal rather than for the entire inventory.
Configured ceilings remain unchanged. Complete passes rest for at least one
second, or the configured retry interval if longer. Close stops new sweeps and
joins entered work before releasing engine/configuration resources; cancelling a
close waiter cannot release a blocked archive read. Controller usage separately
reports active sources, the active sweep and retained cleanup/discovery errors.

Managed retries retain the exact destination boot, member, repair permit and data
mode. Source member/node are attempt hints, independently reauthorized against a
fresh source quorum; leader failure may change them without replacing the retained
destination owner. An entered attempt keeps its original deadline and hint. A mode
change cannot adopt a retained S3 preparation.

The destination reconciles the namespace node pin and permanent member claim
against the exact node/disk/engine. Its synced staging mapping retains the original
birth configuration and grants no startup permission. An existing remote claim
cannot recreate a missing mapping. `CellGroupSeedSession` exposes the staged group
only to construct its physical stores; dropping an uninstalled session abandons
the exchange and leaves a non-serving stage that needs a fresh authenticated retry.

The receiver verifies and syncs the entire physical closure before a single engine
fence installs configuration, membership, complete root, retained checkpoint and a
local authenticated-install receipt. That receipt is required in the marker-1
`cellar-group-apply-1` storage record (`manifest` plus `seed`); the old bare-manifest
shape refuses decoding. The receipt never travels in a snapshot and generic
snapshot installation cannot mint it. No old-format reader or user-data deletion
is introduced. Live grant and exact engine/process checks run before entering the
fence. Once physical persistence begins, cancellation and close retain the grant,
image owners, task reservation and consensus lease until actual completion.
A queued install rechecks the grant on the engine worker before its fence and
refuses a newly revoked source without publishing the prepared root or seed receipt.
Revocation after entering the fence cannot roll back already issued writes. A
successful fence may retain the complete authenticated seed despite that late
revocation; the local receipt still grants neither voting promotion nor current
quorum authority. A sync error leaves the live cache unpublished and closes the
engine to further mutation. Cold recovery must reconcile the selected complete
root and local receipt together; an error response alone does not prove rollback.

`CellGroupApplier::recover_selected` reconciles an unknown install outcome using
the existing physical store owner; selected pins prevent reopening that directory
while it is still owned. It verifies current and retained checkpoint closures
without introducing catalog or quorum authority.

`ReplicaLog::reclaim_cell_storage` performs idle-node orphan maintenance after
runtime and SQL shutdown have joined. It reserves both exclusive claims until
physical completion and serializes on the existing engine worker. Callers supply
one exact applier for every physical group, including unattached/retired groups,
and every additional durable archive/transition root. The image stores must belong
exclusively to this engine. Missing, duplicate, foreign, oversized or busy
inventories refuse. It verifies all selected current and checkpoint closures and
merges references by exact shared-store owner before any deletion. Live pins
remain independent protection; file and directory barriers precede quota refund.
Limits bound selected cells, reclaimed files and page scans. Cancellation retains
queued work and claims; engine close joins completion. This API grants neither
Raft log truncation nor S3 coverage and does not run during normal serving.

Normal shared-group startup separately adopts the serving extent inventory after
recovering every local group's current and checkpoint graph. Those complete graph
pins replace conservative filename retention before routes become reachable.
The independent archive verifier adopts its separately locked cache before
registering the first receive/replay job; cached filenames provide no recovery
proof. Disposable empty workspace roots are transient and pinned before sealing.
Existing retained roots and uncertain publications keep their protection.

An admitted private workspace retains its exact directory and locked extent-store
owner through all SQLite/VFS handles. The store's open established its ancestor
barriers; creating a workspace still syncs the child and parent before returning.
Rooted private opens validate that owner/directory binding and do not repeat the
ancestor walk. Failed child/parent barriers keep the full charge and quarantine;
reopen establishes new barriers before another workspace can be issued. Anonymous
read-cache directories remain disposable and supply no persistence, image or
recovery proof. Materialized private paths keep their full directory preparation.

The verifier keeps eligible files as an opportunistic, charged cache while each
inventory dimension remains below one quarter of its existing ceiling. Pressure
in roots, objects, root references or bytes enables ordinary collection; an
inventory without roots also collects orphan objects. Serving inventories collect
eligible files without this delay. Cache entries add no pins or capacity and
cannot replace archive GETs, source validation or independent local replay.
Buffered imports check the supplied extent digest even when a valid destination
already exists, so a warm cache cannot hide corrupt incoming bytes.

Only these adopted inventories enter the node-owned live extent collector. One
physical turn runs at a time across the two node roles, with at most sixteen
deleted files. Checked working memory and the service's fixed metadata charge
come from the existing control bank. Busy admission defers work. The idle hint
uses metadata and notices a last-reader release without another mutation; marking
preserves locally imported dependencies of pinned, incomplete receives. Physical
deletion and directory sync precede capacity refund. Stop and cancelled observers
cannot release the registry lease, inventories or working memory before an
entered physical call returns. Inventory or worker failure stops collection and
appears in advisory resource observations; those observations supply no durability
proof. This service does not collect manifest pages, invoke the offline engine
collector or authorize log truncation. Long-running replication and reclamation
still require their workload qualification.

After interrupted dependency writes, collect only unselected debris, then obtain
a fresh seed from a fresh quorum. The empty stage still has no startup permission.
Install its ordinary process route only after authenticated seed installation
produces the local receipt; a staged grant is not ordinary membership authority.
Register source checkpoint endpoints after installing their current physical
routes so the captured transport route matches the fresh source process.

`bootstrap_joined_group_member` resumes only a locally receipted install with the
same permanent remote claim and active node/disk-bound membership. Full physical
recovery still verifies both current and retained checkpoint closures before
attachment. Birth-member bootstrap continues to admit only original members.
Neither API grants current-term quorum authority. A new learner must catch up
through ordinary Raft replication before promotion; a lost install response is
reconciled from durable local state instead of assuming failure or resetting disk.

Native transfer/replacement qualification is recorded in progress.md. Managed
fleet orchestration, sustained-write capture fairness, the complete CG-05 fault
matrix and deployment qualification remain open.

## Apply, checkpoint and archive

For entry N, verify the cell base/sequence, stage the result, sync its immutable
image closure, sync changed manifest pages, then atomically persist the group root
and applied=N in the engine. Publish and acknowledge only after the marker succeeds.
Old roots remain pinned until no recovery/reader/transfer owner needs them.

| Crash cut | Required outcome |
| --- | --- |
| Before image sync | Recover prior root; no output credited. |
| Image durable, manifest incomplete | Prior root remains authoritative; staged image is not visible. |
| Manifest durable, engine marker absent | Prior root remains authoritative; replay the same log entry without callback execution. |
| Marker durable, response/publication lost | Recover the new complete root; receipt retry returns the committed effect. |
| Any referenced image/page missing | Refuse group admission; never substitute an empty cell. |
| Later cell staged before earlier cell finishes | No applied-prefix hole and no early output settlement. |

A checkpoint pins the whole root at K, including empty enrollments, membership,
range/transition state, receipt floors and effect summaries. Stream bounded chunks
and verify the receiver's own synced closure before atomic install. A successful
transfer is not proof that another replica has installed it. A new voter receives
fresh member/disk identity and joins only through committed membership changes.

The native checkpoint source now captures only an exact engine-selected complete
root. It snapshots configuration and membership under the same cache lock, retains
all immutable manifest/image pins, and streams canonical pages plus materialized
bytes or extent manifests/objects. Empty enrollment remains an authenticated leaf.
The source pool charges both retained metadata and a bounded streaming workspace;
its non-cloneable source and borrowed chunks prevent overlapping streams on one
reservation. Run capture and streaming on a joined blocking owner. A cancelled
waiter does not release the source's physical pins or quota. Flat-image integrity
is final only at EOF, so partial output must never be selected by a receiver.

This source contract currently represents active groups only: the shared owner
still refuses topology and park entries. Those owning tasks must extend the durable
root and checkpoint transition binding together. Receiver decoding/staging, atomic
engine installation, retained-checkpoint selection, transport admission and
membership catch-up are not implemented by this source API. Successful streaming
supplies no receiver-install, voter, serving, archive-coverage or reclamation proof.

The shared transport has a dedicated checkpoint stream on its TLS data listener.
Configure one node `CellGroupCheckpointPool` with `enable_group_checkpoints`, shared
with local publication. Controller registration retains the exact applier and
physical route; route replacement/removal or registration drop revokes subsequent
stream admission. Ordinary consensus frames still reject snapshot payloads.

Marker-1 start frames bind the physical group/incarnation, permanent member,
node/disk/process identities, schema and snapshot envelope to the TLS exporter.
Each chunk has a separate authenticated purpose, binds the start tag, and carries
a monotonic sequence, part identity, offset, length and body digest. Header and
payload limits are checked before allocation; chunks are at most 64 KiB. Extent
identities resolve against the receiver's already verified manifest. An explicit
authenticated End is required. EOF or cancellation before End cannot finish a
receive; mixed cuts and incomplete closures refuse. After verified admission,
connection loss does not establish whether installation completed. Source capture
pins the exact engine-retained snapshot even when the current applied root advances.

Both seed and checkpoint receivers observe the physical verifier while reading
network headers/bodies and feeding its bounded input channel. A rejected part
closes the exchange even if the peer sends no more bytes; it must not retain
network admission until the peer deadline. Successful verification still requires
the authenticated End and the matching closure. This does not cancel engine
persistence after it has been admitted or replace its actual durability fence.

Two bounded channel windows connect the network to blocking physical owners.
Node/peer data budgets and source/receive slots remain owned through actual I/O,
even after a connection or delivery waiter disappears. The checkpoint pool
separately charges physical closure metadata/workspace; flat images use the log's
shared scratch pool. Transport close joins these owners. Membership and exact
registration checks run throughout the stream and again before runtime admission.
The verified closure enters `step_checkpoint`; its stream reply proves admission
only. Raft releases its acknowledgement after the matching persistence completion.

Snapshot delivery feedback has 256 reserved node-wide slots and 1 MiB, separate
from ordinary calls and apply completions. The runtime coalesces one status per
known peer while retaining its reservation, then applies it after the in-flight
Ready has settled. A completed transfer remains owned until feedback admission or
transport shutdown; an I/O response timeout cannot silently discard required
failure feedback. Immediate snapshot queue refusals also report failure, allowing
Raft to leave its paused snapshot state and generate a retry. Whole-group
membership orchestration remains separate integration work. No transfer reply grants new voter, serving, archival
or reclamation authority.

The controller's optional `CellGroupCheckpointPolicy` selects a checkpoint before
starting a successor apply when the configured entry interval has elapsed. It
uses the transport's already configured pool and owns initial checkpoint endpoint
registrations. One node slot spans blocking capture, admission and durable
publication; each selected group retains its exact successor ticket. The current
root therefore cannot advance during capture. The reserved completion-lane request
checks that ticket and the current root, and can publish despite ordinary request
or apply pressure. Pending Raft work may still append a suffix. Only the matching
`CheckpointPublished` event releases the successor to physical apply; an admission
reply alone does not. Reply/event ordering is handled in either order.

Checkpoint selection precedes the SQL apply-capacity check in the controller's
bounded pending scan. An occupied SQL slot therefore does not block another
group's capture/publication. SQL work stays queued when its capacity is full;
checkpoint lifecycle, source and completion admission remain independently bounded.
Native tests hold actual image-directory persistence in both image forms, publish
an unrelated complete checkpoint, retain its unapplied suffix, then release and
join the original physical apply.

`CellGroupRuntime::checkpoint_before_apply` exposes this same exact-ticket
boundary for embedding owners. It waits for completion admission while retaining
the captured source; owner/engine validation still binds the current root and
ticket. Cancelling an unadmitted request queues no work. Admitted requests remain
runtime-owned through physical completion and joined shutdown. `usage()` reports
advisory ordinary/completion/feedback/read-pin charges and immutable limits without
request admission. Completion charges refund at owner admission, while physical
source/root/storage ownership continues. Gauge observations grant no authority.

The shared budget reserves and refunds byte/item dimensions in one short lock
scope, preventing rejected byte attempts from exposing temporary item charges.
Release/shrink notify all registered request sizes and retain a stored signal for
workers that register after checking capacity. Native budget tests cover mixed-
size waiters, late registration and concurrent rejected/fitting attempts.

Pool contention or transient admission overload releases the successor and backs
off until a later apply boundary using the controller's node timer. There is no
per-cell task or unbounded retry loop. Capture jobs are joined before releasing the
runtime's engine claim; accepted writes remain owned by the runtime through its
join. Hard checkpoint failures retain the ticket and quarantine the group, while
shared log uncertainty closes the node. Quarantined groups can retain checkpoint
capacity until joined restart, like their registration capacity.

Native controller tests hold publication at the real engine sync after lifecycle
and checkpoint-source admission. Cancelled close and wait retain both owners and
the engine/SQL claims through physical completion. The healthy two-voter quorum
continues another group's writes during the stalled node's engine sync. Release
joins shutdown, selects the captured checkpoint and permits a replacement runtime.
Both image forms run this cut. Further native cases fill all eight completion
slots with real completed images, retain a ninth checkpoint through admission and
a held engine sync, then complete its exact successor. Two concurrent 32-entry
writers compete for one shared source slot; both advance checkpoints before the
writers finish, and all selected current/checkpoint closures recover on every node.
These are bounded functional traces; broader deployment/fault/load schedules remain
in the later qualification gates.

This is an entry-count policy for active groups. It retains the final idle tail;
time/byte triggers and coordinated hibernation require their later explicit cut
reservation/wake contracts. It supplies no S3 archive coverage proof. Manual
publication remains available to lower-level owners; this policy does not introduce a second serving
runtime or weaken exact-cut publication checks.

An archive publishes a complete checkpoint plus a contiguous committed tail to K.
The tail addresses cells and validates each base chain; administrative entries
also replay. Do not rerun commands. Publish coverage only after all dependencies
are durable and independently verifiable. A dependency failure in one cell blocks
group coverage. Unchanged immutable cell objects can be reused between cuts.

The initial CG-06 checkpoint archive boundary is implemented in
`raft/cell_groups/archive/`. A node owns one `CellGroupArchivePool`; group handles
and their clones share its item/byte admission across uploads and downloads.
Group history has its own archive namespace and marker-1 domain, without a
synthetic cell identity. One admitted physical producer streams a pinned complete
checkpoint through a one-chunk channel. An immutable bounded index names each
cell-addressed chunk, offset, length and SHA-256. Unchanged chunks are GET-verified
and reused without PUT. New conditional creates require exact GET confirmation,
including an ambiguous PUT response. Corrupt existing objects refuse overwrite.

`CellGroupArchiveReference` is an untrusted persistent locator. Only
`verify_checkpoint` can produce `VerifiedCellGroupArchiveCheckpoint`: it downloads
and hashes every indexed object, checks continuity and limits, runs the shared
physical checkpoint decoder, and binds its complete recovered marker to the
index. Missing/corrupt cells or a substituted cut refuse the whole preparation.
`CellGroupArchiveStore` owns private pages/images for an explicit expected route;
open those stores on the execution domain, separately from serving storage.
`receive_archive` needs no GroupLog, member claim, selected applied cut or surviving
voter disk. Its checkpoint type is distinct from the engine-bound receiver output
accepted by SnapshotChange. Archive roots carry a privately minted owner token
that no Raft engine accepts for publication. No replacement genesis or voter
identity is fabricated. Serving still requires fencing, fresh-incarnation seeding
and a new quorum. Verifying older and newer archive cuts never changes an engine's
selected root or the eligibility of a later archive verification. The engine-bound
receiver preserves its selected-base and membership/install checks.

Source/receiver pools separately charge physical closures and scratch. Archive
admission charges bounded encoded/decoded metadata and I/O windows. Complete
individual PUT/GET operations, including body EOF, have domain-owned deadlines.
Ordinary failures join entered physical workers; caller cancellation closes their
bounded channels and retains permits until those workers exit. Domain shutdown
joins entered work. The pool must be shared by node orchestration when it is wired
in; constructing separate pools per group would defeat the node-wide bound.

The checkpoint capability has no conditionally published head or remote GC lease.
It grants no group S3 settlement, archive coverage, reclamation or disaster-recovery
authority. CG-06 still requires those owners, retention admission and fenced
fresh-incarnation seeding. Its executable scope records the production modules
and native archive qualification without changing any required case or oracle.

Quorum writes may proceed during an S3 outage within explicit disk/unarchived-byte
budgets. S3-mode outputs wait for confirmed coverage. Log reclamation requires the
covering local checkpoint and configured archive retention proofs, plus transfer
pins. Shared engine purge must preserve other groups' live ranges. Readers and
in-flight installs retain object pins until their physical owners finish.

CG-06 additionally implements committed-tail capture and independent data replay.
`upload_tail` copies the exact contiguous suffix to the engine-selected applied
root under the cache read lock, then encodes/uploads without that lock. Entries
have bounded canonical framing, including context and sync metadata. The complete
tail index names the original checkpoint, every immutable entry object, and the
final root/configuration/permanent membership. Missing ranges, unsupported types,
invalid cuts and configured byte/entry/cell limits refuse. Upload returns a typed
`CellGroupArchiveUploadedTail` retaining the exact committed-source observation.
Its persistent reference remains an untrusted locator. `into_reference` discards
the local source witness; deserializing a locator cannot recreate that witness.

`extend_tail` reuses an existing archived prefix and captures only the local
suffix after its cut. Its flattened index directly names the original checkpoint
and all entry objects, so the previous tail index is not a recovery dependency.
This permits continued archival after local prefix compaction. Combined limits
are checked before new uploads; a full bounded tail requires a new checkpoint.
Physical shared-engine file reclamation remains a separate unqualified boundary.

`verify_tail` recovers every checkpoint dependency and replays all entries through
the physical transition code shared with live apply. Live apply retains its
original ticket/commit/owner checks before and after preparation. Canonical joint
membership, leave and no-op entries advance group metadata without synthetic
cells or independent cell-sequence changes. Replayed final root, configuration
and membership must match the index; every final page/image is independently
verified and synced. No application callback or engine installation occurs.

Materialized replay uses `GenerationStore::prepare_verified`: an existing cached
result cannot replace validation of the incoming LTX. The delta is recomputed in
admitted anonymous scratch before the destination is independently verified and
synced. `GenerationPin::verify_and_sync` also preserves the receiver's existing
independent-destination checks. New private replay images become reclaimable after
their returned pins close; verifying an existing image preserves its retention.
Private-store quotas remain enforced and live proofs retain their physical data.
Extent replay applies every delta and rechecks
the complete final image. A valid but wrong LTX delta or warm destination damage
must refuse, even when metadata and object hashes are well formed.

Immutable page and extent-root stores remember completed file and directory
barriers only on their exact open owner. Warm preparation still reads and checks
the bytes; it may reuse those completed barriers for an unchanged immutable file.
Reopening clears the observation, and a failed read invalidates that file's
observation. Metadata reads and inventory filenames cannot restore it. Both
barriers must complete before reuse, and a page persistence failure keeps its
owner quarantined. This supplies neither a missing image nor remote coverage;
archive GET, digest, complete closure and replay validation remain required.

A node-wide `CellGroupArchiveTailPool` separately bounds copied suffixes, decoded
entries, image work and retained base/predecessor/successor metadata. Its limits
are explicit and do not change the ordinary archive/checkpoint pool limits. The
returned `VerifiedCellGroupArchiveRecovery` retains this admission with physical
pins. Cancellation stops further replay work and retains entered physical owners;
domain shutdown joins them. Typed contention and poisoned-owner failures differ.
Orchestration must keep the verified archive result alongside any derived raw
manifest roots: cloning `PreparedGroupManifest` does not clone its parent archive
admission. Controller ownership propagation and private-cache maintenance require
CG-08 qualification before managed integration.

The physical verifier remains data-only. CG-06's separate publication owner now
combines that proof with the exact committed source and persistent remote ownership.
S3 command settlement/outage admission, local Raft file reclamation and fenced
fresh-incarnation seeding remain unfinished; no required task oracle is changed.

### Protected publication and remote collection

A node shares one `CellGroupArchiveUploadPool` for upload maps, source witnesses
and collector marks, alongside the existing I/O, checkpoint and tail pools. Its
explicit byte/object/owner bounds include inherited-key lookup maps and compact
collection candidates. Clones retain their admission. Source-witness admission is
held until its final clone closes, independently of the asynchronous upload job.

Fresh registry initialization uses a one-use, non-deserializable
`PreparedCellGroupArchiveCreation`. Persist its entire `intent()` in the durable
authoritative owner before commit. Opening requires that exact expected creation
identity; never discover and adopt a replacement mutable identity on restart.
Established identity/state loss refuses admission. A lost identity-create reply
with no complete state requires authority recovery; it cannot reset counters.
`RangeDirectory::prepare_group_archive_binding` supplies that immutable namespace
binding. Persist its canonical `encode()` bytes before committing the one-use
initializer. It checks the selected birth before binding, before initialization
and before return. The marker-1 catalog record binds the namespace root and exact
creation route/identity; its key is specific to group/incarnation. The namespace
chooses the object store and group-archive data prefix. A competing initializer
cannot replace the binding or initialize a different registry beneath it.
`open_group_archive` requires a freshly verified birth observation belonging to
the same catalog owner, reads the immutable binding, checks its namespace/route,
and opens only that expected registry. Missing binding/identity/counters refuse
without repair. An incomplete initialization requires authority recovery; replaying
serialized bytes does not restore initialization authority. These APIs do not
make controller ownership durable or grant serving/disaster-recovery authority.

Persist each allocation intent before CAS and retry only that exact intent after
an uncertain outcome. Its registry, next revision, nonce and complete operation
form its identity. Definite conflicts can expire it. Live exact results reconcile
a lost reply even after unrelated registry changes; retired/aborted results never
reappear. Revision exhaustion refuses. Registry records are bounded to 64 KiB,
eight pending generations and 64 explicit holds. These are simultaneous bounds;
retirement permits continued lifetime churn without growing tombstone lists.

A reservation protects its never-reused upload generation and its exact selected
parent, atomically at allocation. New object keys include both the registry ID
and generation. Reuse is restricted to exact objects in that protected parent
closure, with GET verification; an arbitrary old content address cannot be
resurrected. Binding checks every inherited key even after successful physical
replay. Dropping a local handle never releases a remote reservation or hold. No
unknown owner expires by wall time. A replaced process must be fenced before its
persisted allocation can be shared or retired.

Tail capture seals a local committed-source witness with the real engine owner,
root, configuration and permanent membership before asynchronous upload. Source
writes and local checkpoint compaction may advance afterward. Publication
rechecks that sealed prefix against the live source, independently recovers the
entire immutable checkpoint/tail and matches its complete cut to the witness.
Neither a raw locator, a private archive root nor a serialized publication intent
can replace the source witness for a new publication.

`CellGroupRuntime::capture_checkpoint` asks the existing worker to capture its
owner's exact durable applied root. It performs bounded metadata capture only;
physical streaming and remote I/O remain outside the worker. Owner/cache mismatch
during concurrent persistence returns typed `NotReady`, to retry after apply.
`upload_cut` consumes that sealed checkpoint, retains its original configuration
and permanent membership under the upload-owner budget, uploads the checkpoint,
and writes a zero-entry tail for that same cut. It never recaptures a live root
after object-store I/O. Healthy quorum can advance while a dependency is held.
The source checkpoint slot is released before independent recovery starts, so a
single-source checkpoint pool can serve upload and verification sequentially.

`prepare_publication` writes an immutable root descriptor in the pending generation
and prepares one exact publication intent. Persist it before `commit`. One CAS
consumes the reservation, selects the non-regressing complete root and allocates
an explicit remote hold; independent readback confirms the exact result. Equal
indices require the same complete marker, and terms cannot regress. A later peer
mutation does not invalidate a still-live exact result. Lost readback/cancellation
leaves its intent for explicit reconciliation; it never allocates another hold
or silently substitutes a newer mutable head. Reconciliation only finds an
already-published hold and independently recovers that exact immutable root.

Readers acquire and persist their own independent hold before downloading a
selected root. `verify_hold` reads that exact closure and rechecks its hold before
returning `CellGroupArchiveCoverage`. Clones keep local pins/admission; remote
retirement is explicit, after the durable owner retires/fences all uses and joins
entered work. A predecessor's late release cannot revoke a replacement's distinct
hold. Coverage is historical data durability, without current quorum, serving,
membership or disaster-recovery authorization. S3 command output settlement
still needs its operation-specific receipt/version and current routing checks.

Remote collection snapshots one conditional registry revision, selected roots,
explicit holds, pending parents and pending generations. It expands every root's
exact dependency keys and downloads/hashes every marked object before issuing
any DELETE. Missing/corrupt protected data or metadata refuses the entire mark.
Collection excludes marked keys, complete pending generations and generations
newer than its watermark. New reservations can inherit only already-protected
parents; retired generations cannot be rebound. Thus concurrent publication
cannot resurrect an unmarked object, and a delayed DELETE cannot erase a new
copy of identical content under another generation. Unknown DELETE outcomes may
be retried against the same permanently obsolete key. Control identities,
registries, other groups and legacy namespaces are never collection candidates.
Scans/deletions and compact candidate inventories have explicit limits.

This remote object collection does not authorize local shared-engine Raft file
purge. Controller ownership reconciliation, private-cache maintenance, S3
settlement with bounded unarchived bytes, immutable source birth/fence inventory,
fresh group/cell incarnations and fresh-quorum serving remain required. The
fencing inventory must cover every possible old voting lineage, including joins
after the selected archived cut. Real RustFS, Docker fencing and measured-load
qualification remain later gates.

### Controller-owned archive publication

`CellGroupController::archive` installs archival for an already attached group.
The namespace binding must already be initialized. Registration checks the exact
birth/store route, then opens only the namespace's expected registry. One fair
archive lane on the existing node worker rotates a deduplicated dirty-group
queue, independently of SQL apply capacity. Clean groups require no roster scan. Apply/checkpoint-install events mark a group dirty. Transient
I/O/admission failures use the existing node timer; malformed metadata and lost
journal state refuse archival. Completions compare the covered cut with both the
owner applied index and consumed apply events; recovery of an older publication
cannot lose an idle final tail. This does not enable S3 command acknowledgements
or authorize local log reclamation.

Each group has an exclusively locked node journal bound to the engine creation
identity and expected archive creation. Its immutable identity guard precedes its
initial state; missing identity/state never reconstructs an interrupted journal.
Updates write an exclusive temporary, sync it, rename, sync the directory and
read back the exact bytes. Every attempt reloads the selected state, so an error
after rename cannot roll back to stale in-memory intent. Record size is bounded
to 32 KiB; the roster and single active lane bound retained control metadata.

| Selected journal phase | Permitted next action |
| --- | --- |
| None | Verify an already sufficient held cut, or prepare and persist one reservation intent. |
| Reserve | Reconcile/reserve that exact intent, then persist its ticket before any object upload. Definite expiry permits a newly persisted intent. |
| Upload | Capture and upload a sealed complete cut using that pending generation; physically verify it and persist the exact publication intent before CAS. |
| Publish | Reconcile only the exact existing publication. If absent, abort its pending ticket to fence a delayed CAS, then reconcile again before allocating anything new. |

Selecting a new head first persists its explicit hold and the predecessor to
retire. Only afterward may the old hold be released; both release and the journal
clear are idempotent. Source callbacks never run in this workflow. Physical
coverage/root admission stays scoped to the job, rather than retaining a replay
workspace for every registered group. `archive_status` exposes historical metadata
and errors only; it is neither a physical coverage capability nor an S3 receipt.

The sealed archive execution capability accepts either the full execution domain
or a controller's captured token, without acquiring shutdown rights. A controller
tracks its admitted blocking closures independently of their async waiters.
Shutdown joins logical archive work, drains those physical closures, closes private
verifier stores and releases journal locks before closing runtime/SQL ownership.
Fresh verifier stores are required on restart. No unrelated domain work is joined
through this archive-specific drain.

### Local command S3 response gate

`CellGroupDispatcher::with_s3` binds local command settlement to a controller in
the same execution domain. Execution also requires its exact runtime owner and
an active archive registration for the selected group/route. Effective durability
is the maximum of the request, namespace minimum and existing receipt minimum.
Remote execution uses the same receiver-side proof gate and retains its output
through physical delivery as specified below. An unbound provider still refuses.

A committed SQL receipt resolves its producer before any archive wait. The owner
acquires independent S3 byte/item admission before transferring output retention
from SQL; it releases the resolved preparation's duplicate SQL permit. The S3
lane uses the node's `max_commands` and `max_command_bytes` as separate bounds,
including fixed metadata and additional receipt-read bytes. It never recursively
acquires ordinary command slots. Held or uncertain S3 work cannot keep the SQL
producer unresolved or quarantine an otherwise healthy group.

The existing fair archive lane takes at most 32 pending command proofs per group
turn. It publishes or reopens a protected complete cut, then reads each exact
receipt from that independently recovered immutable image. Coverage must contain
the same cell history at or beyond the selected committed version/position. An
equal version must carry the same decision witness. All stored receipt fields,
including operation, contract/input digests, result, durability minimum and retry
expiry, must match. A later image can prove an earlier retained receipt; a group
index or sibling progress cannot replace that read. Missing/corrupt dependencies
anywhere in the selected group refuse the output. No callback runs during proof.

The response rechecks the selected catalog birth/route and live leader authority
before issuing its version token. Original deadlines and captured inputs remain
unchanged. Deadline/caller cancellation does not release an entered physical read;
its cancelled flag interrupts SQL, and the archive-specific owner joins the closure.
Pending requests and unconsumed responses retain bounded S3 admission.

A successful response retains the complete physical coverage, its pool admission
and the exclusive journal lock. Immutable route authority is shared per group,
so membership records are not copied into every response. The journal owner tracks weak uses and refuses
predecessor-hold retirement while a response still retains that coverage. A
replacement owner cannot acquire that journal lock before the response drops.
Idle registrations do not retain replay workspaces. Active consumers can occupy
those shared workspaces and backpressure subsequent archival until consumption;
this is explicit pool admission, without an unaccounted metadata-only proof cache.

Explicit unarchived-byte/disk admission, physical shared-engine Raft file purge, fenced fresh-incarnation serving,
load and process-kill qualification remain required gates. This local command
boundary is not the complete CG-06 or public SDK/fleet cutover gate.

### Local enrollment S3 response gate

The same explicit dispatcher binding and bounded archive lane also settle local
enrollments. Required durability is the maximum of the request and immutable
namespace minimum, captured in the invocation fingerprint. The exact runtime
owner and active archive registration are checked before entering initialization.
Remote enrollment carries the effective request requirement unchanged and returns
authenticated achieved durability with the original creation record.

SQL resolves and closes the initializer before archival. A separately admitted
S3 output takes retention of the bounded original creation record before SQL
releases its bytes and operation slot. The complete independently recovered root
must contain that exact history, creation ID, schema and initial image commitment.
A cut preceding creation waits; a different creation refuses. Later mutations do
not replace the original record or require retaining superseded initial bytes.
No initializer runs during proof or exact retry.

Before returning, recheck the current catalog birth, actual leader authority and
its exact committed creation record under the original deadline. The returned
`CellSqlEnrollment::durability` derives S3 only from its private retained proof.
That output retains the same physical coverage, pool admission and journal lock
as a command response. A deadline or dropped reply keeps commitment ambiguous,
without retaining an unresolved SQL producer or quarantining its healthy group.

### Local receipt lookup S3 response gate

`CellReceiptInvocation::select` binds an opaque quorum selection to the original
request, route and deadline. Required durability is the maximum of request,
namespace and stored receipt minimum. A configured S3 provider must own that exact
runtime and active group registration. S3 admission takes the selected bytes
before SQL read admission releases; no producer is created or retired. Selection
queues no archive work until its consuming `response` call. The existing `execute`
method runs both phases. Dropping an unsubmitted selection refunds admission.

A complete published cut must be independently recovered. Its cell history and
logical version must cover the quorum selection; an equal version must retain the
same decision and receipt outcome. A known receipt must match every stored field
and result, even in a later image. An initially absent receipt is read again from
the actual later archive: it may return a newly committed receipt or an ambiguous
absence, with that archived image's observation and signed token. No absence,
including an S3-protected one, proves non-commit or releases original work.

The shared receipt reader checks row length before fetching payload bytes. The
existing S3 reservation grows atomically for actual result size without another
item slot, within the unchanged node byte/item ceilings. Failure returns typed
Overloaded before payload allocation; expiry, conflict and sequence gaps remain
typed rather than becoming None. Receipt table loss remains corruption except
for legitimate initial sequence-1 images. Empty results remain distinct from
absence. No application callback or receipt maintenance runs during lookup.

Before output, recheck current catalog birth and a fresh correlated quorum read's
leader/route fence under the original deadline. The current cell cannot precede
the archived observation; equal versions must retain the same decision. This read
fence permits later concurrent mutations without retaining a private writer's
base. A successful response retains full physical coverage, S3 admission and the
journal lock, and reports achieved durability from that private proof.

### Remote S3 response delivery gate

Command, enrollment and receipt RPCs carry the effective request durability. Each
purpose retains its separate authenticated route and marker-1 payload. Successful
payloads also carry achieved durability, including an ambiguous receipt absence.
The exporter-bound MAC covers the entire response digest and the exact outstanding
request tag, purpose, node/disk/process, group/cell history and schema. Decoders
reject unknown durability codes and any achieved level below the original request.
Version tokens bind command/receipt observations; enrollment binds the original
creation/history/schema/image record and does not become a current-version token.

The receiver applies the same local proof gates above, including namespace and
stored receipt minima. A configured provider must own its exact runtime and active
archive registration. An unbound receiver refuses before mutation when S3 is
already required; a stored-minimum duplicate resolves SQL before refusing. The
sender retains its original invocation, captured inputs and deadline. Possible
transmission keeps mutation failures ambiguous; a lost or rejected reply never
proves non-commit and no transport retry invokes callbacks automatically.

Encoding returns a bounded owned reply. S3 replies retain the native output,
complete physical coverage, S3 admission and journal/remote-hold ownership alongside
the encoded bytes; the existing transport reservation admits those bytes. Quorum
replies release their resolved SQL output as before. The physical write stage owns
the TLS stream and reply together, with stream destruction ordered before proof
release. Flush completes before normal consumption; cancellation, revocation,
deadline or task destruction closes the stream before refunding its proof. No new
per-cell task, timer or proof cache is introduced. Client outputs retain their
existing node/peer byte admission through consumption.

The fixed `transport_query_reply_write` observation measures only response
header/body/flush, independently of archive settlement. Its bounded host-lifetime
histogram is advisory and grants no durability authority. Qualification fills a
bounded socket window with exact retries, sending each only after the preceding
physical flush/refund, without reading replies or changing node quotas.

Qualification must observe an actual blocked TLS write, retained S3/coverage slots,
SQL release, bounded overload, a same-cell Quorum successor and physical refund on
drain, disconnect, timeout and grant revocation. Exact retries must return the
original result or creation record without callback replay. This delivery gate
does not establish outage disk bounds, local Raft file purge, fresh-incarnation
serving, process-kill recovery, independent hosts or load performance.

## Split and merge

Use the same transition state machine for one parent to multiple child ranges
(split) and compatible adjacent parents to one child (merge). Create fresh child
group IDs and retire every parent; this avoids reusing a partially changed parent
as a second activation path. Initially seed children on the parents' qualified
voter set where possible; ordinary learner movement can rebalance afterward.
Pre-copy immutable images while parents are live, then send the final delta after
sealing. Hibernation and membership changes must be resolved before sealing.

Every transition pins its exact parents, range epochs, child births, final cell
partition and resource reservations in a durable catalog journal. There is a
namespace topology CAS, but no catalog I/O on an ordinary already-routed command.
The directory exposes explicit split and merge preparation over whole-cell
bounds. Both forms check the exact union and successor epoch before the fleet
admits a transition. A Live group created by an earlier transfer can reopen
its selected range as a sealed source during a later merge. Its synced seal
continues to fence application work across that restart.

Managed planning stages the canonical directory request before the fleet selects
its digest. A replacement controller can reopen the exact admitted plan by
operation ID. If the fleet selected the plan before the directory CAS, the
controller can replay the original request from the reopened record. A cancelled
operation retains its historical decision and plan.
The fleet can authorize a seal only for the exact active request and plan
after verified child preparation. Authorization and cancellation contend on
the same directory CAS. Once authorization wins, retrying the original request
returns the recorded decision and cancellation leaves the fleet slot owned.
An operator can submit that decision to the parent through
`RaftGroupHandle::seal_managed_source`. The handle verifies the running boot,
selected birth and applied voter membership before minting a local permit.
The Raft owner rechecks the permit and drains the accepted prefix. A proposal
acknowledgement is provisional. An exact retry can return the applied seal cut.
The managed archive owner can publish the applied cut while business admission
is sealed. The fleet verifies each held cut independently against the exact
authorized plan and rechecks the complete hold set before selecting Sealed.
After each parent seals, `stage_managed_sealed_source` independently verifies
its archive and records one exact hold in the plan's parent slot. A replacement
controller reads the slots in plan order with `staged_managed_sealed_sources`.
It retains staged holds through retirement and obtains the remaining seals.
`FleetCatalog::replay_managed_sealed_sources` verifies all staged cuts again
before selecting the complete sealed set and preparing non-serving child genesis
images. Once every child has an installed quorum proof,
`resume_managed_transfer_archive` rebuilds the child partition in the
controller's private verification stores and records the exact archive closure.
The verifier needs bounded capacity to retain one complete cut per parent.

| State | Durable action and admission rule |
| --- | --- |
| Planned | CAS a unique intent against the exact current range tree; reserve child/transfer resources. Parents still serve. |
| Prepared | Children are non-serving learners of transfer state; copy immutable bytes. No child has business admission. |
| SealAuthorized | CAS the journal from Prepared to an irreversible seal decision before issuing any parent Seal request. The exact retained decision grants permission to ask the parents to seal, not a quorum proof. Cancellation can no longer win. |
| Sealing | Every parent commits `Seal(transition, children, ranges)` after all earlier accepted entries. Seal covers enrollment, commands and strong reads. |
| Sealed | All parent quorums have durably applied their seal cuts. Capture and pin the exact cell/effect/receipt closure. The operation remains roll-forward only. |
| Installed | Every child's own quorum commits and applies its seed from the sealed cuts, with fresh Raft term/vote state. Children remain non-serving. |
| Archived | Publish and independently verify an immutable remote transfer closure containing every sealed parent cut, child partition/seed root and all cell dependencies. Pin it in the transition journal before range publication. |
| Published | CAS the namespace root from all exact parent ranges to all exact child ranges at once. Validate complete equal union and no gaps/overlap. Unknown CAS results reconcile by transition ID. |
| Active | Each child commits activation binding the retained publication certificate. Serving requires that activation plus current-term quorum authority. |
| Retired | Parents persist retirement/redirect tombstones. Release old data only after child archive coverage and all dependency pins are safe. |

The final sealed-source partition builds a non-serving child genesis marker.
It binds the exact transition, plan and sealed-source set. An active-source
pre-copy carries image pins without this genesis. Ordinary log apply and
checkpoint install preserve the pending marker and its exact image root.
The local install binds one child genesis across all its voters and persists a
member/node/disk/engine intent before syncing the mapping. Managed installs
also require the exact transition's reserved child, a current capacity proof
for the running destination boot, and the same local boot guard at the engine
write. The controller prepares only its assigned child in the retained node
stores. A retry can reconcile the exact synced mapping while the node retains
its pins. Startup parks an exact pending child after checking the rooted
transition and install intent. It verifies and pins the retained pages and
images under the new engine owner before allowing image collection. Its
physical mapping remains charged to the node without joining the Raft roster
or publishing a route. Receipt reconciliation reuses that closure and requires
capacity evidence from the current boot.
The child retains its fleet binding while pending. A matching receipt
follows the sync. A read-only check confirms a majority of reserved voters
from matching intents and receipts on distinct pinned disks and engines. For
a managed child it verifies the original active fleet plan and reserved route
and matches the physical receipts to the fleet's permanent storage claims.
A pending voter can attach after rechecking this majority and its own receipt.
The local applier uses the exact page and image owner handed to its engine.
Managed attachment also rechecks the current boot and admitted capacity and
installs only authenticated voter routes. The controller retains the same
physical page owner across an uncertain install reply and hands it to the
pending Raft roster. After a fresh-term barrier commits and applies, the
pending leader can issue a quorum witness while the child remains inactive.
Raft can elect a leader and commit in a fresh term while the pending manifest
fences cell commands and reads. Only the child leader can attest that its
stable initial voter configuration committed and durably applied a fresh-term
position with the exact pending manifest. The directory verifies the witness
against the installed genesis and the leader's physical receipt. It roots the
whole ordered child set in one CAS and can reopen the selected set after an
ambiguous response. The managed catalog rechecks each reserved child and its
voters' permanent fleet storage claims before that CAS. Installed children
remain fenced and the parent routes stay selected. Directory publication and
activation require their own proofs.

Child install receipts bind the source seals, own committed seed, membership and
manifest digest. Archive verification independently replays every selected parent
hold and rebuilds the child partition in dedicated stores. Every rebuilt root and
transfer origin must match its installed child. The directory rechecks the live
holds and selects an immutable closure that binds the two exact evidence sets.
For a managed namespace, the fleet catalog checks the active plan and the
reserved children against a single fleet snapshot before replay. The directory
binds the closure to the same installed set at its CAS. The closure can be
reopened from remote metadata after a lost response or voter disk loss. Recovery
replays the retained holds and reconstructs the same roots.
Publication requires this closure so losing every voter immediately afterward
still leaves a recovery path for prior S3-protected effects under the child
partition. A controller observation without
those proofs cannot publish. S3 outage may stall topology changes; it does not
change ordinary Quorum acknowledgement behavior.
Publication verifies that the original archive holds are still retained. A single
directory CAS selects all child ranges, the Published decision and the immutable
certificate binding the archive closure. An exact retry can recover that
certificate after a lost response. The same CAS roots a permanent birth fence
for every child. Ordinary initial bootstrap refuses those children even when a
separate install receipt is unavailable. Managed publication rechecks the
active fleet plan and the exact storage of the reserved child voters. Children
validate their exact transition and refuse replay after later retirement. The
managed activation permit binds the selected route and install receipt to the
current boot and initial voter configuration. The child Raft owner admits the
exact certificate and seed commitment only after its fresh-term barrier. Committed
apply retains the certificate, seed commitment and activation position in the
child manifest. Subsequent mutations and checkpoints preserve that evidence.
During an active managed transfer, restart reopens the exact activated voter
from its permanently claimed engine, revalidates the publication and current
boot, and uses its existing placement. Until the child archive is initialized,
the held source closure remains available for recovery. Explicit S3
acknowledgement requires child archive coverage. A selected child can serve
only after activation applies. The
directory selects Active after receiving one durable acknowledgement for every
child in plan order. It binds the set to the exact publication and installed
seed commitments and can reopen it after an ambiguous CAS. The topology lane
remains owned until parent retirement. After the route switch, a retained
parent voter reopens from its historical birth only if its synced seal matches
the exact selected transfer. This keeps the parent fenced and lets it
participate in the source quorum for retirement. The parent leader can commit
its tombstone only after the directory selects the complete Active set. The
entry binds the replacement publication to the original seal, and the manifest
and checkpoint preserve both. This tombstone leaves the topology lane and parent
archive holds pinned while child archive coverage is qualified. Managed setup prepares one archive identity per selected active child. The
operator persists the exact intent before committing it. A retained voter
opens the initialized registry and publishes coverage from its applied state.
The directory independently replays the exact held archive of every active
child. Each cut must retain the installed seed and the selected publication
and activation position. The directory rechecks all holds before rooting the complete set.
It can reopen the selected references after a lost response, with the parent
holds and topology lane still pinned for retirement settlement. The fleet
owner can root the Retired decision only after rechecking the live child holds
and binding every committed parent tombstone to the coverage and publication.
During cleanup, previously activated child voters may still reopen on their
claimed engines. New install and activation remain fenced. Child confirmation
preserves all parent references. The fleet owner confirms every
child Live in a single rooted commit after rechecking installed storage,
retirement and live child coverage. A Live child reopens using its fleet
membership, permanent engine install receipt, and the selected directory's
retirement and activation binding, independent of the transition slot. Archive
setup is required for that startup when archiving is configured. The owner
can reopen a retired parent disk in parked form after a joined process stop.
Startup checks the exact local tombstone against the immutable retirement
set and keeps that history out of the Raft and transport rosters. Authenticated
physical inventory identifies the parked source by history, birth, member and
boot. The disk and controller slot remain charged. The retirement record keeps
the final committed member and node bindings, including the disks assigned to
joined members. The fleet checks every member against its node's current boot
and parked inventory before releasing the selected preparation and sealed holds.
All parents on the same node must match the same authenticated inventory sample.
It rechecks that the children are Live and their archive holds remain valid.
The directory releases only the original transfer holds and confirms
each release before clearing topology ownership in a separate CAS. The fleet
then records every parent as retired and clears its disruption slot in one
root update. The retired row binds the operation and retirement set, keeps the
original birth identity, and stays in inventory. Retained engine slots still
consume capacity, while membership work and topology scheduling use Live
groups. An
interrupted cleanup resumes the same immutable decision. Later transitions
preserve historical publication certificates and reject stale child startup.
Source tombstones remain effective across restart, delayed messages and catalog
unavailability. The whole transition is one fenced owner operation; concurrent
controllers reconcile the same journal instead of selecting different children.

`cellar raft fleet transition status NAMESPACE_HEX --operation-id OP_HEX --catalog-id DIGEST`
reads the admitted plan digest, directory phase and Fleet slot at its observed
revision. Use the Fleet catalog ID from `cellar raft fleet status`. Directory
cleanup can finish while the Fleet slot is still held, so the phase and slot
are reported separately.

For a retired transition, submit
`cellar raft fleet transition resume NAMESPACE_HEX --plan REQUEST.json --catalog-id DIGEST`
as the principal that admitted it. The request binds the catalog, namespace,
operation, plan digest, expected Fleet revision and request ID to either
`confirm_children` or `complete`. Confirmation promotes the verified children
together. Completion claims the exact request, checks all parked parent replicas,
releases the archived holds and commits Fleet retirement. Retain the request
file and use the same request ID after an uncertain reply. An authenticated
node observer supplies the parked proofs while the holds are outstanding.

| Failure | Required behavior |
| --- | --- |
| Controller lost before seal authorization | Resume or CAS Prepared to Cancelled; seal authorization and cancellation compete on the same journal state. Revoke child activation permits before releasing reservations. |
| Seal reply lost | Treat outcome as unknown; reconcile committed log/receipts. Do not reopen a parent based on timeout. |
| One merge parent sealed, another unavailable | Sealed parent remains fenced; unsealed parent's earlier writes join its eventual seal cut. No destination admission. Resume when quorum returns or use the disaster protocol. |
| Child install incomplete | Keep parents sealed and all retained pins. Repair/finish the same child or use authorized disaster recovery. |
| Transfer closure upload/verification incomplete | Do not publish child ranges. Keep parent and child pins; retry the same immutable closure. |
| Directory CAS response lost | Read journal/root and confirm the exact winner. Never roll back a possibly published transition. |
| Publication succeeds, some children not active | Only activated children serve their disjoint ranges; finish activation. Parent never resumes. |
| All disks lost immediately after publication | Recover from the journal-pinned remote transfer closure plus any archived child tails; no newly acknowledged S3 effect may disappear. |
| Delayed old proposal/read/activation | Check history, epoch, transition and local sealed/retired state; refuse stale authority. |
| Parent disks fail after publication | Children serve from installed state. Parent deletion does not establish archive safety. |

After SealAuthorized, automatic cancellation is forbidden, even if no Seal reply
has arrived. Parent Seal admission requires that exact authorization certificate.
Availability can pause while completing a transition; preserving single-writer
ownership takes precedence. Pre-seal cancellation also needs a durable controller
decision in the same journal. Cancelled intents can never mint seal/activation
permits. Unknown authorization-versus-cancellation CAS results reconcile the
recorded winner before issuing work; timeout cannot establish that cancellation won.

Normal transfer preserves cell incarnation, mutation sequence, exact receipt
contents/client floors and effect identities. Seed provenance authenticates the
original commit witnesses. New group positions do not replace per-cell ordering.
In-flight outbox delivery is settled or journaled with the original ID; a lost
delivery response never authorizes a new identity. The target may retry that ID
under the receiver's existing dedupe contract.

## Coordinated hibernation

Closing an execution cache is independent from parking the Raft group. For the
first hibernation implementation require all configured voters to participate;
a healthy majority can continue active work if another voter is unavailable, but
cannot park. This is a conservative availability tradeoff, not a Raft safety rule.
Learners must have an explicit quiesced/catch-up state; no unbounded transfer may
be abandoned merely to park.

1. The current leader checks no unresolved proposal/read, transition, membership
   change, snapshot install or unsafe effect dispatch exists. Freeze new admission
   while negotiating. Publish a bounded next-due-effect summary to a node wake owner.
2. Commit `PreparePark(generation, term, membership_revision)` and apply through it.
   Every voter syncs a local acknowledgement for that exact generation and cut.
3. The leader assembles all acknowledgements, syncs its park certificate and sends
   authenticated park messages. Each recipient persists its parked generation
   before suppressing routine group ticks. Partial delivery is safe: an active
   peer's messages wake parked peers. Node-level transport remains available.
4. API request, peer message, due effect, topology operation or local restart
   wakes the group. Persist closure of that park generation before new admission;
   late park messages for a closed generation cannot re-park it.
5. Resume ordinary Raft using retained term/vote/log. Send wake hints to peers and
   obtain current-term commit/read authority before serving. Wake messages and
   retained leadership are not quorum proofs. Missing a quorum refuses/waits.

Use execution-domain timers and a bounded node-level due index; do not allocate a
thread per cell/group. A restart may conservatively wake all locally parked groups
in bounded batches. Never reset term/vote or initialize a missing disk. Include
park state/tombstones in local recovery and checkpoint contracts. The scheduler
must reconcile alarm changes after leader loss, and an overdue alarm wakes the
group immediately rather than waiting for a client.

Required fault cuts include lost prepare/ack/park/wake messages, delayed duplicate
park after a new command, leader loss while parked, minority wake, mismatched
membership revision, alarm changes during negotiation and all-voter restart.
Measure healthy retained-state wake separately from election, image-cache restore
and S3 disaster recovery. Do not claim wake percentiles from a few successful trials.

## Zero-quorum recovery and transition closure

Recovering a live range changes authority, so serialize it with namespace topology
and fleet lifecycle operations. Pin an exact recovery set; verify its full remote
closure before requesting infrastructure fencing. Never choose a newer head after
the operator request was admitted. Existing Quorum versus S3 recovery-point limits
continue to apply.

If the group participates in an incomplete split/merge, recover the connected
transition component, including every parent/child that could still activate.
Resolve the durable publication journal first. Fence the launch authority and all
possible voting lineages in that component, including removed/replacement hosts
whose restart could violate the selected boundary. Partial fencing cannot publish
new ownership. A list of current online nodes is not a complete fencing inventory.

For an unpublished transition, choose complete archived parent cuts and reconstruct
their non-overlapping domain under new groups. For a published transition, require
child recovery sets or a retained transfer closure plus each child's archived
tail; a parent checkpoint alone cannot cover child acknowledgements. Refuse if the
selected closure cannot account for every S3-acknowledged effect in the retained
recovery horizon. An intentionally older rollback needs explicit operator scope.

Create fresh group and cell incarnations, node/member/disk/process identities and
Raft genesis state. Restore SQLite, receipts and client floors without callbacks.
Publish replacement ranges through one fenced topology mutation, then require a
fresh quorum and committed activation. Managed fleet import/maintenance/recovery
must share that same mutation journal and resource debt; no unmanaged bypass.
Unknown fencing/publication outcomes resume the identical request. They never
unfence old hosts or select another cut.

The mandatory RustFS drill deletes all owned voter disks after independently
checking the ledger and archive cut. It verifies every cell, old-token rejection,
receipt retries, new writes and retained-disk restart. Repeat during split/merge
crash cuts and after physical log reclamation. This remains group/component
recovery, not a globally consistent snapshot of unrelated groups or external effects.

The unmanaged recovery fence barrier is implemented separately from replacement
installation. `RangeDirectory::prepare_group_recovery` binds the exact source
birth, dedicated archive hold, replacement birth and expected topology mutation
in canonical marker-1 bytes. Persist those bytes and retain the hold before
`fence_group_recovery`; resume the identical bytes after an unknown outcome.
The barrier independently verifies every held archive dependency before claiming
the namespace or asking infrastructure to fence it. It refuses joint, repair
and fleet membership records at this static unmanaged boundary.

A recovery claim shares the directory-head CAS with ordinary topology publication
and the namespace lifecycle CAS with fleet import. Once selected, its source
cannot bootstrap again and competing topology operations refuse. The existing
`DisasterFenceAuthority` contract accepts a typed `CellGroupDisasterFenceRequest`:
the deployment controller must permanently stop/revoke all possible old writers,
including later joins and removed hosts, and independently verify evidence bound
to the complete request digest. Cellar stores the winning bounded receipt and
independently verifies it again on resume. No permissive controller is supplied.

`FencedCellGroupRecovery` retains verified data under the original histories.
It grants no engine, voting or serving permission and does not release the
topology lock or publish a replacement. Fresh-history image/receipt rebinding,
fresh-genesis installation, activation and fresh-quorum admission must complete
the recovery protocol above. The contract fixture and real RustFS CAS tests do
not qualify Docker host fencing or the deleted-voter serving drill.

## Archive-aware local checkpoint compaction

An archive registration first makes the local group's retention requirement
irreversible through an engine-synced metadata write. The required marker-1
`archive_required` field belongs to the local group application record; a peer
snapshot neither grants a frontier nor clears this policy. Missing-field records
refuse; no legacy decoder, migration or marker increment is introduced.

After opening a retained disk, that requirement blocks local checkpoint
compaction until the journaled remote hold has been independently verified again.
The controller advances a private engine-local frontier only from complete archive
coverage. Public `archive_status` remains advisory. Equal-index coverage must
match the exact local root and term; newer coverage cannot be replaced with older
or conflicting evidence. An intent reconciled behind the retained checkpoint
keeps compaction closed until the next verified cut reaches local history.

An accepted registration owns a pending activation intent across retries. It
closes later checkpoint admission while earlier work drains. The Raft phase and
its storage batch share an exact engine/group/cut-bound lease; storage may finish
that earlier work while the intent is pending. Raw persistence cannot manufacture
or substitute the lease. The requirement is synced only after all older leases
drain, preserving the checkpoint's sole source slot without letting new captures
overtake activation. Capacity refusal is retried at this boundary.

An unentered intent releases its barrier when registration is rejected or closed.
An entered metadata write retains the intent independently of its waiter and
publishes the irreversible requirement after physical sync and cache/root
publication. Unknown I/O closes the engine rather than completing the intent.
An existing durable bit follows the same drain and frontier-reset boundary.
Frontier reset happens once before I/O; a delayed duplicate completion cannot
discard subsequently verified coverage. No mutex guard crosses an await or
physical I/O. These owners are local capabilities and add no serialized field.

The automatic checkpoint scheduler checks the frontier before capture, letting
quorum apply and another group's checkpoint lane progress during archive delay.
The actual owner checks the exact captured cut again; the storage worker repeats
the check before any snapshot compaction batch. An already prepared local
checkpoint cannot bypass the configured requirement through direct persistence.
The frontier retains only bounded marker metadata; the durable archive journal
owns the current remote hold, and no idle per-group replay workspace is kept.

This gate covers local checkpoint publication. Full-backend outage retention
admission, Raft file purge, peer-install integration of deployment archive policy,
fresh-quorum recovery and the public hard cutover remain separate requirements.


### Engine-local retained entry ledger

The shared-group storage engine owns one node ledger and one small group record
per physical history. `ReplicaLogOptions.cell_group_retention` supplies explicit
node/group byte and entry ceilings and bounded progress sublimits. Ordinary data
cannot consume the progress allowance for Raft's empty election/automatic leave
entries. Per-entry charges include fixed bookkeeping. Advisory debt observations
are not physical file measurements, durability proofs or admission capabilities.

Duplicate receipts resolve before new callback admission. A new command or
initializer must obtain an owned worst-envelope reservation before executing user
code. Encoded preparation shrinks that reservation; delayed submission, ambiguous
outcomes and entered physical work retain it. Definitively sealed pre-append
rejection and authoritative receipt reconciliation may release unused leases.

Before RawNode mutation, the owner admits local proposals, inherited peer entries
and possible election no-ops. Automatic joint leave receives promised capacity
before the configuration can require it; apply must not seek new admission after
its durable configuration is selected. Read-index envelopes are not log appends.
Heartbeat/read/completion work preserves its independent lanes at saturation.

Persistence validates exact engine, group and entry bindings. Its selected cache
and root state precede an atomic reservation-to-debt transfer. Failed preflight
cannot change durable debt. Unknown physical outcomes close the engine; reopen
reconstructs actual retained debt without trusting old in-memory reservations.
Conflicting suffixes and local checkpoints retire only actual selected entries.
Archive coverage/status alone never releases ledger debt. A smaller restart limit
preserves selected entries and closes new admission until a reservation fits.

This contract is being implemented and qualified. It does not by itself prove
physical disk bounds, full-backend S3 outage authority, fenced fresh-quorum serving
or the complete CG-06 gate. Those original requirements remain unchanged.
