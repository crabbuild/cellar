<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Durable protocols and crash cuts

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
The public host must supply trusted discovery and explicit permissions; automatic
discovery/SDK binding and dynamic group lifecycle remain
integration work.

Remote commands use the same bounded application RPC lane with a mandatory,
MAC-bound command purpose and distinct payload/result markers. A read grant or
voter membership never grants write admission. `NamespaceCommandOrigin` supplies
an explicit namespace/process permission with bounded revision tombstones;
endpoint checks, rooted process refresh and fleet revocation include these grants.
A host-owned command gateway binds the exact node controller. Dropping its final
registration closes new admission without rolling back accepted work.

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
Remote enrollment uses the same bounded write exchange, namespace/process grants
and exact controller registration. The mandatory authenticated purpose distinguishes
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
Controller/catalog integration must durably bind this expected identity before
archive use and make it recoverable without the voter disks. That integration is
still required before production activation or disaster recovery.

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

Child install receipts bind the source seals, own committed seed, membership and
manifest digest. Publication also requires the exact verified remote transfer
closure, so losing every voter immediately afterward can still recover all prior
S3-protected effects under the child partition. A controller observation without
those proofs cannot publish. S3 outage may stall topology changes; it does not
change ordinary Quorum acknowledgement behavior.
Publication certificates are immutable journal entries, not a mutable head URL.
Children validate their exact transition and refuse replay after later retirement.
Source tombstones remain effective across restart, delayed messages and catalog
unavailability. The whole transition is one fenced owner operation; concurrent
controllers reconcile the same journal instead of selecting different children.

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
