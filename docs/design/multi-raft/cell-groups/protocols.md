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
Reserve the logical cluster/namespace/cell name before entering the callback;
competing incarnations must not obtain independent producer leases. Capture initial
SQLite/LTX bytes once, carry the preparation term with the output, and check that
term on the RawNode owner atomically with proposal admission. A node that loses
leadership and later regains it must reject retained output from its older term.
Cancellation retains physical ownership until the callback and cleanup complete.

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
