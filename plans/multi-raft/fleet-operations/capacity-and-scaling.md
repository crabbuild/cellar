# Capacity admission and scaling protocols

Status: implementation contract for FO-01 through FO-10, extending the
[technical design](design.md). All APIs and commands remain proposed. This
document defines the observable steps and failure handling for adding nodes,
changing node size, withdrawing capacity and changing the fleet size.
The [planning details](planning-details.md) specify the resource ledger, legal
intermediate states, worked capacity changes and fewer-cell sizing bounds.
The [lifecycle execution contract](lifecycle-execution.md) fixes the authority
and retry boundary for each effect, route/readiness acknowledgements, bounded
operation persistence and the order of profile/platform changes.

## Distinguish four capacity changes

| Operation | Change | What remains invariant |
| --- | --- | --- |
| Scale out | Add eligible physical nodes and distribute existing/new groups | RF3 and each cell's history identity |
| Scale in | Evacuate selected nodes and reduce the eligible node set | RF3, declared failure headroom and retained data |
| Scale up | Increase CPU, memory or usable storage on an existing node | Its node/disk/engine identity if the original disk remains intact |
| Scale down | Lower an existing node's resource limits after reducing its obligations | Every existing durability and physical resource obligation must still fit |

Changing an integer in a node descriptor does not create capacity. V1 uses a
controlled restart to change Cellar runtime limits: `RaftHostOptions` and image,
SQL, transport and log budgets are constructed at startup. Live budget resizing
is not an implemented contract. Replacing a disk or restoring a copied engine
does not qualify as an intact resize.

Changing fleet size does not change the number of logical cells. Cold cells stay
durable; cell retirement is a separate authorized application operation. The
operator owns VM/container provisioning, resource-limit changes and destruction.
Cellar owns placement, catch-up, admission and the proof permitting removal.

## Durable control records

Extend the node table with these fields, carried in immutable pages referenced by
the CAS root. Optional observations may be absent; unknown never means zero.

| Record | Required fields |
| --- | --- |
| Node descriptor | Node/disk/storage-instance identities, immutable physical-host identity, authentication identity, supported protocol/classes, region/zone/pool, stable display label |
| Desired resources | Revision, CPU millicores, memory bytes, usable disk budget, group-slot and image limits, fixed/shared pool budgets, configuration digest |
| Observed resources | Process generation, boot/configuration digest, effective limits, actual occupancy, observation age and measurement provenance |
| Eligibility | `registered`, `ready`, `active`, `restart_pending`, `resize_pending`, `draining`, `evacuated`, or `removed`; reason and owning operation |
| Lifecycle fence | Monotonic node-operation epoch, exact operation ID, target exclusion, stop authorization state, pending profile revision |
| Capacity receipt | Accepted profile and process revision, readiness cut, verified resource bounds, capabilities and timestamp for display |

Physical-host identity prevents two Cellar processes on the same machine from
counting as distinct physical replicas in a deployment that requires host failure
isolation. It is trusted deployment metadata, not something inferred from IPs.
The Colima fixture deliberately records shared-host topology and does not claim
physical-host resilience. Failure-domain policy explicitly selects which level
is enforced; the local process-failure fixture uses node separation only.

Profiles are proposals; they cannot relax cluster schema/image bounds or mutate
immutable node pins. Capacity becomes effective only after observed runtime
configuration matches the accepted profile and existing host validation passes.
If the kernel/orchestrator limit cannot be observed directly, require an explicit
deployment attestation and identify that limitation in qualification. Never
claim a descriptor is proof of the applied container limit.

Persist these operation fields in the operation index:

```text
operation_id, request_digest, principal, expected_fleet_revision,
kind, desired_node_count or target_profile, selected_nodes, keep_nodes,
policy_revision, node_operation_epochs, input_inventory_digest,
plan_digest, current_phase, blocked_reason, child_permits,
scan_cursor, observed_effects, cancellation_state, receipt_digest
```

The immutable plan pages contain per-group proposed source/target bindings and
expected configuration revisions. They are a feasibility plan, not permission
to execute every move. Actual permits reserve capacity in bounded batches and
revalidate the current group before starting. Report a revised plan digest when
failure or changed workload requires replanning; retain the original request.

## Admission and operation conflicts

Use one fleet-wide disruption slot shared by restart, resize, evacuation and
scale-in. A scale-in operation owns the slot while evacuating one node at a time.
No second operation can independently schedule another node to go offline.
Required repairs can proceed within the remaining move/seed budgets. Registering
or qualifying a new node is non-disruptive and may proceed concurrently.

| Concurrent requests | Rule |
| --- | --- |
| Two lifecycle requests for the same node | Reject the second as `OperationInProgress`; repeat ID/payload returns the first |
| Restart and evacuation on different nodes | Serialize through the disruption slot |
| Add node while evacuation is blocked for capacity | Allow registration/qualification; revise the existing plan before reserving new moves |
| Two different desired active-node counts | Reject while the first is active; cancel/reconcile it before submitting a new target |
| Optional balancing while scale-in owns the slot | Pause overlapping moves; permitted independent moves cannot consume capacity reserved for the operation |
| Health repair while maintenance is pending | Repair has priority; it can invalidate maintenance readiness and force revalidation |
| Cancel after stop or joint transition was admitted | Reconcile actual effects; cancellation does not retract the accepted stop or committed membership |

Store explicit lifecycle phases, not an expiring owner lock. A new controller
resumes the same operation. A controller crash does not free the disruption slot.
Releasing it requires a completed/cancelled operation with all child effects
settled. With a stuck quorumless action, report blocked and permit non-disruptive
capacity addition; do not issue another independent maintenance permission.

## Eligibility is separate from shutdown

Do not overload the current `RaftNodeProfile.draining` boolean. Introduce explicit
managed eligibility in the planner and authenticated inspection:

| State | Receive new replicas | Receive leadership | Evacuate current memberships |
| --- | --- | --- | --- |
| Active | Yes, within bounds | Yes, when caught up | No |
| Ready spare | No until activation | No | No |
| Restart pending | No | No | No; failed-member replacement follows the bounded grace |
| Resize pending | No | No | Only selected partial moves in the admitted resize plan |
| Draining | No | No | Yes |
| Evacuated or removed | No | No | None should remain; otherwise block completion |

The existing repair selector treats `draining` as an evacuation trigger. Passing
that flag unchanged for a planned restart would replace voters immediately and
defeat intact maintenance. FO-03/FO-06 must consume the managed desired state and
active operation before selecting a victim. A separate maintenance admission
gate can pause new application work before handoff; the permanent Node shutdown
gate stays one-way once stop is admitted.

## Resource feasibility and failure headroom

Keep hard resource admission separate from performance estimates. For each node
and resource dimension, require:

```text
physical_used
  + remaining_growth_reserved_for_admitted_actions
  + candidate_peak_increment
  + required_recovery_reserve
  <= verified_effective_capacity
```

`physical_used` includes retained histories and pinned images. Once an admitted
action materializes bytes, its reservation is reduced only using an observation
that identifies that same action/process, so bytes are not counted twice or
released early. If attribution is unavailable, double-count conservatively until
reconciled. Never subtract a source copy before its storage is actually reclaimed.

Account for these dimensions independently:

- Live voting/learner groups, dormant registrations and retained engine histories.
  The current inspection uses `log.group_count()`, not only serving replicas.
  Expose both logical role counts and physically occupied slots.
- Durable storage: images, retained generations, shared-log tails, seed staging,
  archive backlog, retired copies and operational reserve.
- Memory: fixed/shared pools, real queued/executing SQL work, snapshot buffers,
  decoder bounds, readers and controller metadata. RSS alone is insufficient.
- CPU, disk I/O and network demand: measured service cost and throttling are
  estimates for the SLO; they do not replace the hard memory/byte checks.
- Source and destination movement slots and shared RustFS throughput. Empty
  target nodes still compete for a shared object-store backend.

Run feasibility against the desired final node set and again with each supported
single-node failure removed. Require an explicit legal assignment of all three
replicas after repair, including policy domains and staging capacity. Also verify
that current RF3 placements retain quorum immediately after that failure. These
are different checks: spare disk cannot repair a group whose quorum is gone.

For equal nodes, `3 * logical_cells / surviving_nodes` is a useful lower bound,
not a placement proof. At 500 cells, five nodes average 300 copies; after losing
one, repaired load averages 375. Scaling in to four raises normal load to 375;
after a further failure, the remaining three need room for 500 copies each.
With 500 slots/node, the initial five-node fleet uses 60% of those slots, whereas
four nodes already use 75%. Temporary state, retained histories, bytes and CPU
can require more headroom than these counts imply.

Make the policy distinction explicit: `failure_headroom=rebuild_one_node` is the
default and requires both immediate quorum survival and feasible RF3 restoration
after one node fails. An operator-authorized `quorum_only` policy can permit a
three-node final fleet, checking steady-state fit and immediate quorum survival
but reporting that RF3 restoration needs added capacity. It does not pass the
default repair-headroom gate. A scale request cannot silently change that policy
or describe those two availability contracts as equivalent.

Count failure domains too. A five-node topology with zones distributed 2/2/1
cannot rebuild RF3 across three distinct zones after losing the sole node in the
third zone. It may preserve quorum, but lacks eligible capacity for restoring the
strict placement policy. Report `InsufficientDomains` even when total free bytes
are adequate. Do not claim four nodes always provide a usable repair spare.

Use a deterministic bounded assignment search: place groups with the fewest
eligible destinations first, then largest required reservation, then scope ID;
rank candidate nodes by projected pressure and domain diversity. Stream plan
pages and persist a resumable scan cursor. Bound search expansions explicitly
(initially 4,096 per planning job) and decoded memory to the existing controller
limit. If the search limit prevents finding a complete plan, return
`PlannerSearchExhausted`, not a mathematical claim that placement is impossible.
V1 never starts scale-in from a partial feasibility plan.

The final assignment is only half the proof. Simulate a legal bounded execution
path with temporary learners, occupied source histories and actual host slots.
If every first move exceeds a staging bound or targets a retired history slot,
the operation needs additional fresh capacity even when the proposed final live
counts fit. Report that limiting resource and history in the preview. See
[complete planning](planning-details.md#a-complete-plan-includes-a-legal-execution-path).

## Add a node and make capacity usable

Adding a node has three distinct results: `Registered`, `CapacityReady`, and
`Active`. None implies that existing cells have been redistributed.

| Step | Durable transition | Work and completion check |
| --- | --- | --- |
| A1 Register | CAS adds descriptor as `registered` under the request ID | Validate operator trust, identity uniqueness, schema/protocol and non-removed pin |
| A2 Prepare storage | Existing pin/process publication protocol | Exact immutable pin matches the newly opened engine; no borrowed member identity |
| A3 Install trust | Node-table revision acknowledged by participating hosts | Bound route refresh; actual source leader/target must acknowledge this revision before a move |
| A4 Qualify | Receipt references observed profile/configuration | Verify hard limits, writable/syncable storage through existing storage probes, peer authentication, snapshot support, object-store access and a complete idle/load sample |
| A5 Activate | CAS changes eligibility to `active` | Recheck current profile, process and capacity; make it an eligible placement candidate |
| A6 Distribute | Separate rebalance/scale operation admits group permits | Register dormant histories lazily, seed learners, catch up, promote and retire source replicas |
| A7 Verify convergence | Root references a completed operation receipt | All moves terminal; current placement meets the requested policy/deadband and observation window |

A node with zero cells needs a node-level readiness/inspection endpoint. Do not
require creation of a dummy business cell just to prove it is empty. The current
directory preparation requires a replica or standby registration and the host
requires nonempty registrations; FO-03 must explicitly support a bounded empty
managed host attached to known namespace/class descriptors before scale-out can
work. Dormant metadata must not eagerly register all 10,000 histories on every
node or bypass the existing 4,096 per-host registration bound.

Registration errors retain the operation and typed reason. A failed readiness
probe keeps the node ineligible; it does not create learners. If a source has not
installed the required trust revision, that move waits rather than disabling
authentication. If the node fails after receiving a learner, the ordinary guarded
abort/repair sequence settles that permit. Cancelled registration leaves a
retained identity record; returning a pin with different engine bytes conflicts.

For an equally loaded five-to-six expansion, approximately 250 replicas move to
the new node and each old node relinquishes about 50 serving replicas. Current
live copies remain 1,500, but up to 250 retired source copies may remain locally
after convergence. Disk occupancy can therefore approach 1,750 copy-equivalents
before accounting for staging, logs and retention. The design must report that
temporary/persistent cost rather than promising immediate source disk relief.

## Scale out as a fleet operation

`SetDesiredCapacity(active_nodes=N, convergence=balanced)` defaults to waiting for
placement convergence, not just counting running processes. `capacity_only` is
an explicit alternative for operators reserving unused spare capacity. The status
reports `registered`, `ready`, `active`, `healthy`, and `eligible` counts separately.

1. Admit one desired-capacity operation and pin its policy/desired count.
2. Identify already qualified eligible nodes. Report `WaitingForCapacity` and
   the required domains/resources when external provisioning is still needed.
3. Activate newly qualified nodes through A1–A5. Activation receipts belong to
   this operation or an explicitly referenced completed add operation.
4. Plan redistribution under current load, including leader-first moves where
   possible. New nodes with no replicas require replica movement first.
5. Execute bounded permits and replan stale group configurations. Progress is
   terminal moves/bytes, not attempted RPCs or a percentage based only on count.
6. For `balanced`, verify the constraints over three fresh 10-second windows,
   no unresolved child permits, and required archive cuts. For `capacity_only`,
   complete after eligibility is established and report undistributed capacity.

The operation may complete with nonuniform counts when weighted resources or
domain constraints require them. Its receipt records the actual objective and
residual skew. It cannot silently change a requested balanced result to
capacity-only. Persistent workload churn or infeasible constraints leave a
specific blocked/pending result; timeouts do not declare convergence.

## Remove capacity through evacuation and removal

Evacuation excludes a node from future target placement while retaining the
transport and local state needed to finish its existing groups. Removal revokes
the registration after evacuation. Infrastructure termination is a third action.

| Step | Action | Required check before advancing |
| --- | --- | --- |
| R1 Preview | Build complete candidate and failure-headroom plan | All live/pending histories and existing reservations included; no partial solution |
| R2 Exclude | CAS `draining`, increment node-operation epoch and claim disruption slot | All future birth/move admissions reject the node; older permits remain discoverable |
| R3 Settle | Reconcile incoming learners and already reserved actions | Abort unpromoted work or finish it; never drop a reservation on timeout |
| R4 Replace | Move one group at a time within configured concurrency | Verified target, current quorum, caught-up learner and explicit joint completion |
| R5 Verify | Collect complete inventory/configuration and archive cuts | No incoming/outgoing voters, learners or unresolved action can refer to the node |
| R6 Certify | Commit certificate through the root finalization protocol | Exact operation epoch, inventory cut and zero-reservation condition hold |
| R7 Remove | CAS retained removed tombstone and revocation intent | Evacuation certificate is still rooted and current; no subsequent resume |
| R8 Stop infrastructure | Operator invokes supported shutdown then platform removal | Terminal removal receipt identifies the exact node/disk/process scope; preserve volume/archive retention |

Stop new application commands on a draining source only through the normal
handoff/admission path; continue accepted work and consensus. Route clients to
the observed new leader after handoff. A routing hint never grants leadership,
and a retry must retain command identity when the original result is ambiguous.
Do not wait for every last leader transfer to succeed before moving unrelated
follower replicas, but do not declare an admitted transfer complete.

For a dead node R4–R6 use surviving quorums. Observing zero responses from the
dead node cannot prove zero memberships. The complete index and committed
configuration provide that proof. A healthy but slow node is not erased or
excluded from quorum merely because the operator wants removal to finish.

## Scale in without repeated or unsafe movements

Choose nodes using the complete feasibility plan: first exclude candidates with
unsettled operations or indispensable failure domains, then minimize bytes to
move, hot leaders to hand off and predicted foreground disruption. Use stable
node ID as the final tie break. An operator may name explicit nodes; apply the
same checks rather than substituting more convenient victims silently.

Pin the final `keep_nodes` set. Mark all proposed departing nodes ineligible as
new placement targets, but evacuate and stop only one at a time. Future departing
nodes continue serving existing groups. This prevents moving replicas onto a
node that the next stage will immediately evacuate. If the keep-set changes,
publish a revised plan and recheck reservations and failure headroom.

For five-to-four, at least the departing node's approximately 300 replicas move;
the survivors approach 375 serving copies and 125 leaders each in the equal-load
case. New cell births during scaling use the keep-set and current reservations.
If those births or workload growth invalidate capacity, pause further removals
and replan. Do not freeze business traffic to preserve a stale capacity estimate.

After each evacuation, verify the reduced fleet before beginning the next stage.
Scale-in completion returns the per-node removal receipts and actual surviving
placement. If a node cannot be safely removed, the operation remains partially
completed with exact effects. Cancellation does not restore retired memberships
or reverse data movement automatically.

## Scale up an intact node

Use `ResizeNode` with a full target profile and configuration digest. V1 resizes
one node at a time under the shared disruption slot:

1. Check restart feasibility and existing runtime/profile compatibility. An
   increased CPU count does not permit larger cells or different schema bounds.
2. Set `resize_pending`, exclude new target placements, settle earlier incoming
   actions and obtain the maintenance stop authorization described in
   [maintenance and balancing](maintenance-and-rebalancing.md).
3. Stop the exact process through the owned Node lifecycle. The operator increases
   the platform limits or grows the volume using its supported mechanism.
4. Restart the same intact engine with the target configuration. Compare immutable
   pin and new process generation; inspect actual effective limits and budgets.
5. Catch up retained groups and verify survivor progress/archive cuts. Only then
   CAS the new profile as effective and resume placement/leadership eligibility.
6. Rebalance according to measured capacity. Doubling CPU does not automatically
   double leader count or throughput when SQLite/storage/RustFS remains limiting.

A crash between platform change and profile publication is recoverable: compare
observed configuration with the operation's original/target digests and keep the
node excluded until one is explicitly accepted. Do not assume the platform edit
succeeded. Cancellation after physical resize requires observation and may retain
the larger safe profile; it cannot silently command the platform back down.

## Scale down resources without losing retained state

Validate the desired profile before stopping the node. Predictive CPU pressure
may require transferring leaders; group/memory/storage pressure may require
moving replicas. Partial evacuation leaves the node registered but excludes new
incoming work until resize completes. It may leave retired bindings that cannot
host those same histories again under v1.

The target must cover actual retained engine group count, fixed pools, image and
snapshot ownership, local retained bytes and staging/recovery reserves. The
current source uses `log.group_count()` for inspection and validates image-copy
and decoder budgets at startup. A lower live replica count alone does not make
a smaller `max_groups` or memory profile valid. Obtain a local physical-fit
receipt after moving work; if retained state still exceeds the target, return
`RetainedStateCapacity` and keep the original limits.

For v1, shrinking a filesystem/block device or discarding retained engine state
is unsupported. Reducing a configured storage budget is permitted only when the
existing physical data/reservations fit. To obtain a smaller disk or replace the
volume, add a fresh smaller node, evacuate the old one and remove its registration.
Keep the old volume for retention. Do not treat moving a replica as permission
to delete its old durable bytes.

Existing resource counters are independently sampled atomics, not a coherent
capacity proof. Bind the fit assessment to a node-operation epoch, retained-state
cut and complete target boot configuration. Use conservative reservation maxima
while consensus continues. After stop, confirm old physical workers/readers have
joined before lowering external limits; retained owners cannot survive outside
the accounting model. Revalidate persistent occupancy at the new startup.

Once fit is proved, follow the controlled restart/profile-verification sequence.
The effective lower placement ceiling is installed before new work can arrive;
the higher physical limit stays until shutdown. If the reduced profile fails
startup or validation, keep the node excluded and return to the previous known
configuration while preserving engine identity. Do not repeatedly restart under
an invalid memory bound or call an OOM a completed resize.

## Progress and completion receipts

Status includes counts of planned/reserved/seeding/joint/terminal/blocked groups,
copied and remaining estimated bytes, observed bandwidth, archive lag, per-node
effective limits and excluded capacity. Estimates are ranges and carry sample
ages. Report ETA only when recent measured progress supports it; repeated blocked
quorums produce an unknown ETA, not a declining percentage.

For every terminal receipt, use a three-step commit:

1. CAS the operation to `Finalizing` with the canonical receipt digest, relevant
   input revisions and the exclusion/fencing conditions still held.
2. Conditionally create the immutable receipt and verify its exact bytes.
3. CAS the operation index to `Completed` or `Cancelled`, reference that receipt,
   and atomically apply the final eligibility/tombstone/reservation changes.

Store candidate receipts at `receipts/<operation-id>/<sha256>`. If proof-relevant
inputs change during finalization, recompute and publish a new digest-addressed
candidate under the same operation; the old object remains unreferenced. A
terminal operation selects exactly one receipt and can never be reopened.

An unreferenced receipt object is not completion. The API must resolve the rooted
operation index and its terminal state before returning success or authorizing
infrastructure removal. If a receipt PUT succeeds but the last CAS is lost,
reconcile the same finalization record. The receipt binds the operation epoch and
proof digest, not a guessed future root revision. Unrelated root writes require
CAS retry; changes to proof-relevant inputs require verification again. Invalidated
finalization publishes no new authority and cannot release the disruption slot.
