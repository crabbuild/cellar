# Fleet operator workflows

## Command and API contract

All commands below are proposed FO-08 deliverables. They must not be presented
as usable commands until their implementation and tests pass. The management
server exposes the same behavior under `/api/fleets/{fleet}/raft/operations` and
read-only `/raft/nodes`, `/raft/groups`, `/raft/policy` and `/raft/operations/{id}`.
Integrate existing admin authentication and auditing, with distinct permissions
for read, node registration, policy change, maintenance, removal and cancellation.
Resource resizing and guarded maintenance stop also require their own explicit
permissions. Detailed phase/rollback rules are in [capacity and scaling](capacity-and-scaling.md)
and [maintenance and rebalancing](maintenance-and-rebalancing.md).

`--operation-id` is caller-generated and retained across retries. Every mutation
also requires `--expected-revision`; a 409-style revision conflict requires a
fresh preview. Unknown outcome means inspect/retry the same operation, not issue
a new mutation. `--dry-run` returns the proposed diff, capacity/domain assessment,
estimated movement and input revision without reserving work. Preview is advisory;
admission always revalidates. API admission returns an operation handle, not a
completion claim.

```text
cellar raft fleet status --fleet F --json
cellar raft fleet nodes add --fleet F --descriptor node.json --dry-run
cellar raft fleet nodes add --fleet F --descriptor node.json --operation-id OP --expected-revision R
cellar raft fleet nodes activate NODE --fleet F --operation-id OP --expected-revision R
cellar raft fleet rebalance --fleet F --dry-run
cellar raft fleet rebalance --fleet F --operation-id OP --expected-revision R
cellar raft fleet nodes maintenance NODE --mode restart --max-unavailable-seconds 120 --operation-id OP --expected-revision R
cellar raft fleet nodes stop NODE --maintenance-operation PREPARE_OP --receipt-digest HASH --expected-process PROCESS --operation-id STOP_OP --expected-revision R
cellar raft fleet nodes resize NODE --profile target-profile.json --dry-run
cellar raft fleet nodes resize NODE --profile target-profile.json --operation-id OP --expected-revision R
cellar raft fleet nodes evacuate NODE --operation-id OP --expected-revision R
cellar raft fleet nodes resume NODE --operation-id OP --expected-revision R
cellar raft fleet nodes remove NODE --evacuation-receipt RECEIPT --operation-id OP --expected-revision R
cellar raft fleet scale --active-nodes 4 --convergence balanced --operation-id OP --expected-revision R
cellar raft fleet operation status OP --json
cellar raft fleet operation wait OP --timeout-seconds 600
cellar raft fleet operation cancel OP --operation-id CANCEL_OP --expected-revision R
```

Commands with omitted `--fleet` use an explicitly configured fleet, never guess
from an arbitrary bucket listing. Credentials stay in existing secure provider
configuration and never appear in request files or evidence. `wait` timeout is
an observation timeout and leaves the operation running. CLI exit codes: 0 for
the requested admitted/read result (or completed `wait`), 2 for invalid input,
3 for revision/payload conflict, 4 for blocked operation, 5 for unavailable/unknown
outcome and 6 for wait timeout. JSON always includes phase and completion status.
`--wait` on mutation uses the same wait semantics and cannot turn admission into
completion. There is no `--force` removal or quorum bypass.

## Add capacity

1. Provision a persistent volume and a compatible node with explicit resource
   bounds and trusted credentials. Starting the process does not grant membership.
2. Preview and register its descriptor. Verify immutable node/disk/engine pin,
   current process identity, domain labels, compiled classes and capacity.
3. Wait for `Ready`, then activate placement through the node operation API.
   `nodes activate NODE` uses the same idempotency and revision guards.
4. Submit or enable rebalancing. Watch reservations, replica transfers, leader
   changes, archive lag and foreground p99. New node capacity is usable only
   after actual target activation/catch-up.
5. Verify final placement and workload. For 500 equal cells, six nodes approach
   250 copies and 83/84 leaders each, subject to load and domain constraints.

`scale --active-nodes 6` reports `WaitingForCapacity` with required descriptors
if only five eligible nodes exist. It never creates a VM, container or disk.
For `--convergence balanced`, wait for the placement objective and verification
window. With explicit `--convergence capacity-only`, completion means eligible
capacity exists and reports that existing cells may remain undistributed.

Do not mark a node ready just because it has no cells or an open TCP port. Inspect
its effective resource limits, complete empty-host inventory, storage pin, boot
configuration, authenticated routes and required namespace/class capabilities.

## Restart maintenance on an intact node

Use this for a short process/OS update while preserving its volume. Submit
`maintenance --mode restart` and wait for the `RestartReady` maintenance receipt.
It proves the checked groups have two eligible surviving voters and that handoff
was observed. Submit `nodes stop` with that maintenance operation, readiness digest
and exact process generation. It revalidates readiness and invokes the guarded
owned Node shutdown path. Observe physical stop, then restart the same registration
and volume. Verify catch-up and resume placement before finishing the operation.
The old stop request cannot apply to a new process, and cancelling an already
admitted shutdown cannot reopen its permanent admission gate.

Restart, resize and evacuation share one disruption slot. The grace temporarily
postpones replacement, with a maximum of five minutes. If another failure occurs,
or the deadline expires, reconcile actual group availability and permit repair
where quorum exists. Do not restart a second node simply because the first
process exited successfully. If repairs retired some local bindings, the returning
node cannot blindly reattach them; use the fresh-registration path below.

A rolling request performs this sequence on one node at a time. Before the next
node, verify restored redundancy, current workload headroom and the previous
resume receipt. A partial rolling operation reports exactly which nodes finished;
it never takes the whole requested list offline together.

## Increase or decrease the size of a node

Submit `nodes resize` with a complete target profile, using dry-run first. The
preview reports required leader transfers, any replica evacuation, actual retained
state, and whether the reduced profile can satisfy startup and recovery budgets.

For CPU/memory growth, prepare guarded restart, change platform limits externally,
restart the intact engine with the requested configuration, and verify effective
limits before the fleet uses the added capacity. An edited profile alone does
not increase placement capacity. A 1-vCPU/3-GiB to 2-vCPU/6-GiB test is a separate
resource tier; it cannot be counted as an optimization at unchanged resources.

For reduction, wait for required workload movement and a physical-fit receipt
before lowering platform limits. Retired replicas, occupied log slots, fixed
pools and pinned images remain charged. If they cannot fit, the operation reports
`RetainedStateCapacity`; retain the old profile or use a fresh smaller replacement
node. V1 does not shrink filesystems or discard old state to satisfy a memory/disk
target. A failed restart stays excluded and may return to the previous known
profile; status never reports a failed/OOM boot as a completed resize.

Each resize holds the shared disruption slot through verified rejoin. If the
operator changes limits outside this protocol, report the observed mismatch,
exclude new placements and protect admission; external mutation cannot grant
capacity or create a successful operation receipt.

## Evacuate and permanently remove a node

Preview capacity and domains, then submit evacuation. The fleet excludes the node
from new placement while allowing existing consensus and admitted work to finish.
It transfers/replaces all memberships and verifies the complete inventory cut.
Wait for the `Evacuated` certificate. A report with remaining learners, joint
states, unknown histories, archive obligations or reserved work is incomplete.

Submit removal with that certificate and a current expected revision. Wait for
the terminal removal receipt, then stop/remove infrastructure. The receipt binds
the node/disk/storage identity and exact inventory/configuration proof; a newer
conflicting operation invalidates it. Stopping first is crash recovery, not
successful evacuation. Local volumes and archives remain retained.

If a node is already dead, the operation uses survivors to perform the same
membership repair and verification. A quorumless group blocks final removal.
The operator can restore its original durable members or follow a separately
qualified disaster procedure; this API does not force progress.

## Return hardware after evacuation

The physical machine may return, but evacuated member bindings stay retired.
Provision a new volume/engine and new node/disk registration; preserve the old
volume according to retention policy. Follow the normal add-capacity workflow.
Reusing the machine's display name is allowed; reusing its durable identity is
not. Selective reseeding into retired slots is outside v1.

## Reduce fleet size after demand falls

Submit a dry-run desired active-node count. The planner reports projected group,
storage and workload pressure, temporary seed storage, domain constraints,
one-node-loss headroom and estimated movement. Refuse a plan that only fits
healthy steady state but violates its configured failure-headroom policy.

The planner evacuates one node at a time and waits for proof before selecting the
next from its pinned final keep-set. All departing nodes are excluded as new
targets so data is not copied onto the next node scheduled for removal.
Default RF3 policy keeps at least four active nodes; a deliberately chosen
three-node policy reports that it cannot rebuild a third replica after a failure
without new capacity. Two nodes are refused. Reduced traffic alone never deletes
cell state. Committed cell retirement updates the inventory, after which capacity
can be recalculated while archive retention remains separate.

Check one-node-loss capacity on the final fleet, not only on the starting fleet.
For 500 cells, a four-node final fleet averages 375 copies each and needs room
for 500 each after losing another node. Zone constraints may block the plan even
when those counts fit. New births or workload growth can invalidate a pending
plan; status reports replanning or blocked capacity without freezing business
writes or quietly lowering the replica/domain policy.

## Run or pause rebalancing

Use `rebalance --dry-run` to inspect the trigger, objective, candidate moves,
expected peak/imbalance change, bytes to copy, retained source bytes and limiting
budget. A manual request uses the same safety and movement limits as automation.
Specify whether the objective is an explicit topology redistribution or measured
load improvement; an idle new node may need placement without any source being
overloaded.

The future CLI also exposes `balancing pause` and `balancing resume`, both with
operation ID and expected revision. Pause stops new optional actions. It does not
interrupt committed joint transitions, forget existing learners, disable required
repair or release their physical reservations. Resume waits for healthy telemetry
windows before admitting more optional work.

Inspect role movement and physical resource relief separately. A leader transfer
can reduce command work; it does not move follower SQLite apply cost or reclaim
disk. A replica move can leave a retained source copy. Shared RustFS saturation
may require backend capacity rather than more Cellar nodes.

## Read operation progress

The following JSON is an illustrative future response, not a measured run. Group
phase counts are mutually exclusive and sum to the current plan's 300 moves:

```json
{
  "kind": "scale_in",
  "phase": "Executing",
  "subphase": "Replacing",
  "completed": false,
  "desired_active_nodes": 4,
  "departing_nodes": ["node-e"],
  "groups": {
    "unreserved": 178,
    "reserved": 1,
    "seeding": 1,
    "joint": 0,
    "terminal": 120,
    "blocked": 0
  },
  "eta_seconds": null,
  "removal_receipt": null
}
```

Detailed responses additionally include operation/request identity, current root
and policy revisions, node-operation epochs, plan digest, paginated group IDs,
profile/pressure samples, bytes moved and retained, cancellation state and typed
blocked reasons. `Finalizing` is not completion. A receipt file existing in the
bucket is insufficient until the terminal root references it. `Evacuated` grants
no data-deletion permission; removal still requires its separate terminal receipt.

## Diagnose stalled operations

| Reported reason | Operator action |
| --- | --- |
| `InsufficientCapacity` | Add eligible capacity or reduce requested scale-in; inspect bytes and temporary reservations |
| `InsufficientDomains` | Add capacity in a permitted domain; do not silently relax policy |
| `QuorumUnavailable` | Restore original durable voters; inspect affected cells before any disaster procedure |
| `InventoryIncomplete` | Finish managed enrollment or resolve pending births/retirements |
| `ArchiveBehind` | Restore archive progress and retained-log headroom; do not erase the last copy |
| `StoreUnavailable` | Restore the catalog service; quorum data traffic may continue within retention bounds |
| `UnknownOutcome` | Inspect the same operation and retry its exact ID/payload |
| `RetiredBinding` | Preserve old storage and register a fresh engine for returned capacity |
| `RetainedStateCapacity` | Keep existing limits, wait for separately authorized reclamation, or use a fresh replacement node |
| `ProfileMismatch` | Restore the intended runtime/platform configuration and requalify the node |
| `PlannerSearchExhausted` | Narrow the requested change or increase a reviewed search budget; no unsafe partial plan is admitted |
| `StaleMaintenanceReceipt` | Revalidate the original maintenance operation against the current process and group state |

Pause optional balancing to reduce movement pressure. Cancellation prevents
further unadmitted steps, reconciles existing actions and may finish with partial
committed effects. Never kill background workers or delete CAS records to cancel.
