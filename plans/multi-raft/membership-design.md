# Native placement and repair implementation contract

Status: MR-11 implementation in progress. The [plan foundation](evidence/MR-11-plans-progress.json),
[leadership/recovery](evidence/MR-11-leadership-progress.json) and
[registered standby](evidence/MR-11-standby-progress.json) evidence qualify explicit
repair APIs, guarded transfer and peer/S3 activation in an already-running host.
Automatic scheduling, authenticated inspection and the six-node
no-business-traffic gate remain open. The
[node-directory contract](node-directory-design.md) specifies that next boundary;
its local identity/fence foundation is [qualified](evidence/MR-11-node-storage-progress.json),
while the S3 pin/view adapter is [qualified locally](evidence/MR-11-directory-progress.json).
Process distribution and atomic routing refresh now have focused verification;
their broader qualification is pending. Inspection remains unimplemented.
MR-10 qualification remains separate.
These component results establish no additional capacity or release qualification.

## Authority and durable intent

Each group retains one active repair plan with its committed membership state.
The plan binds full group/incarnation scope, desired-policy revision, expected
configuration revision, fresh member/node/disk identity, obsolete member and a
scoped plan ID and canonical payload comparison. Its first change admits the target only as a learner.
Snapshots, compaction and archives must preserve that same plan cut. The catalog
can describe desired placement; it cannot change a group's voters or replace a
live quorum. A plan error or lost response may follow commitment.

The enclosing group log and authenticated operation bind the full history scope;
plan IDs do not supply authority.

A controller recovers the group's actual plan and configuration before deciding
its next action. Same-plan retries reconcile the committed phase. A competing
plan with an old configuration revision cannot append a different assignment.
Member IDs remain monotonic above the durable high-water; retired IDs are never
reused. A bounded last-completed plan record permits response reconciliation
without retaining an unbounded history of controller requests.

## Repair sequence

1. Select a healthy, compatible, non-draining target with capacity, outside the
   group's current physical nodes and within the configured pool/failure-domain
   policy. Deterministic ordering resolves equivalent candidates. Refuse a strict
   domain policy when insufficient domains exist.
2. Commit the revision-bound plan and exact learner binding. Reserve bounded
   controller work; create no claim or voting state merely from a directory edit.
3. Start the target's registered dormant cell through the existing authenticated
   peer/S3 seed path. Its fresh grant, sealed image, atomic engine state and
   permanent claim retain the MR-10 proof boundary. Existing synced state resumes
   rather than resetting the identity. Dropping a controller waiter never drops
   target-owned initialization or proves it did not commit.
4. Observe target readiness and actual leader replication progress. The consensus
   owner rechecks `matched >= committed` when accepting promotion. Directory
   readiness, archive availability and an admission receipt cannot substitute.
5. Propose explicit joint consensus that promotes the caught-up learner and
   removes the obsolete voter from the incoming set. The outgoing set and its
   bindings remain active until leave-joint commits under both required
   majorities. A lost leader/controller resumes from the committed joint state.
6. If a target fails while still an unpromoted learner, commit removal of that
   learner and an aborted-plan tombstone before choosing another fresh identity.
   A joint transition cannot use this abort path. Existing local state with a
   different member ID makes that node ineligible for a new blank attachment;
   normal repair never clears or rewrites it.
7. Commit leave-joint, reconcile the final configuration and only then report
   repair completion. Retired members close local admission and retain their
   stable history for the separately defined operational retention policy.

An unavailable original voter is a repair trigger, not permission to remove it
without quorum. No special S3 fallback or force-membership path exists.

## Native service integration

A preconfigured standby node must accept a bounded, authenticated activation for
an already registered class/history without application traffic or a second
node-wide Raft engine. Dynamic attachment belongs to the existing host/SQL pool
and the node execution domain. Client handles must observe their current local
placement; an immutable query-router binding cannot masquerade as an activated
replica. Incoming group traffic alone still creates no group.

Trusted node registration pins node ID to disk lineage. A process-view revision
may refresh the process generation and addresses for that same registration; it
cannot rebind an existing node to another disk. A lost disk uses a fresh node
registration and fresh member IDs. This preserves the initially trusted disk
mapping for birth members as well as explicit joined bindings. Discovery cannot
silently make an old claimed voter appear on a blank replacement disk.

The control protocol separates inspection, revision-bound plan proposals,
activation and leadership transfer from business commands and query payloads.
Every request/reply binds full history, current physical process routes, purpose,
nonce, TLS exporter and original timeout. It admits only configured trusted node
endpoints. Process-view updates have monotonic revisions and cancel stale work;
the durable node/disk binding continues to come from committed configuration.

Group-leader hosts run reconciliation from an owned timer, independent of
incoming cell traffic. Leadership partitions control work naturally; a new leader
recovers the committed plan before issuing another step. Standby hosts pull
bounded plan observations for their pre-registered histories, then perform the
existing fresh-quorum seed flow for an exact binding addressed to their node/disk.
A plan observation is only a hint and cannot authorize initialization. This avoids
a remote controller owning a target SQL worker or constructing a second engine.

The controller runs from that owned timer, independent of incoming cell traffic.
It bounds groups scanned per turn, concurrent repairs, metadata bytes, per-node
seed work and retry deadlines. Existing snapshot source/receiver reservations
remain enforced. Controller closure cancels its attempts and joins owned work;
ongoing data-plane quorum authority does not depend on controller liveness.

## Placement and leadership

Candidate inputs include stable node/disk identities, current process view,
namespace/pool, region/zone, schema/protocol compatibility, health sample age,
capacity and draining state. Selection is pure and bounded; host adapters supply
observations and execute effects. Fleet ownership balancing keeps its own
contracts and does not become a Raft membership oracle.

Leadership movement selects only a current non-draining compatible voter with
actual caught-up progress. Bound transfers per interval and apply a load
threshold/deadband so stale samples cannot cause oscillation. Transfer requests
are requests to the consensus owner, never changes to external routing authority.

The implemented native transfer primitive binds expected term/configuration and
rechecks current-voter status, recent activity and actual matched progress at the
serialized Raft owner. It refuses an active repair, joint state, pending
configuration or transfer. A success acknowledges admission, without promising
election. Peer `MsgTransferLeader` frames are refused so they cannot bypass this
boundary. Native verification isolates a follower, commits newer state, refuses
movement, heals/catches up, then elects the requested voter and resumes writes
and causal follower reads.

The implemented pure leadership planner requires fresh compatible target
observations and both matched/applied progress at the inspected commit cut. It
compares exact leader-count/capacity ratios, applies a configurable utilization
deadband and consumes a minimum interval even for failed attempts. Normal moves
strictly reduce the two nodes' peak utilization without reversing their relative
load. Draining bypasses only the pressure threshold. The future controller must
provide fresh receiver-local samples and revalidate at the owning runtime;
planner output supplies no authority or automatic scheduling by itself.

## Completion gates

The seven MR-11 manifest cases remain required. Native qualification must include
six actual Node instances, replacement after a failed member without business
traffic, repeated controller restart/lost responses, joint leader/process loss,
strict domain refusal, minority refusal while S3 remains reachable, and bounded
leadership movement with a lagging voter. Planner-only tests do not establish
automatic repair. Physical zones, production capacity and larger tiers need their
own later qualification evidence.
