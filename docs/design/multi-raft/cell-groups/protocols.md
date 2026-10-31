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
root therefore cannot advance during capture. A private completion-lane request
checks that ticket and the current root, and can publish despite ordinary request
or apply pressure. Pending Raft work may still append a suffix. Only the matching
`CheckpointPublished` event releases the successor to physical apply; an admission
reply alone does not. Reply/event ordering is handled in either order.

Pool contention or transient admission overload releases the successor and backs
off until a later apply boundary using the controller's node timer. There is no
per-cell task or unbounded retry loop. Capture jobs are joined before releasing the
runtime's engine claim; accepted writes remain owned by the runtime through its
join. Hard checkpoint failures retain the ticket and quarantine the group, while
shared log uncertainty closes the node. Quarantined groups can retain checkpoint
capacity until joined restart, like their registration capacity.

This is an entry-count policy for active groups. It retains the final idle tail,
and time/byte triggers, fairness under continuously saturated checkpoint capacity,
controller checkpoint crash/held-sync cuts and complete root-pressure qualification
remain open. It supplies no S3 archive coverage proof. Manual publication remains
available to lower-level owners; this policy does not introduce a second serving
runtime or weaken exact-cut publication checks.

An archive publishes a complete checkpoint plus a contiguous committed tail to K.
The tail addresses cells and validates each base chain; administrative entries
also replay. Do not rerun commands. Publish coverage only after all dependencies
are durable and independently verifiable. A dependency failure in one cell blocks
group coverage. Unchanged immutable cell objects can be reused between cuts.

Quorum writes may proceed during an S3 outage within explicit disk/unarchived-byte
budgets. S3-mode outputs wait for confirmed coverage. Log reclamation requires the
covering local checkpoint and configured archive retention proofs, plus transfer
pins. Shared engine purge must preserve other groups' live ranges. Readers and
in-flight installs retain object pins until their physical owners finish.

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
