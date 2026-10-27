# Fleet operator workflows

## Delivered keep-set workflow

The dedicated `cellar` binary implements the commands in this section. They use
the authenticated admin API; bucket credentials are not supplied to the CLI.
Configure the server's existing-root adapter as described in the
[admin guide](../../../crates/cellar-admin/README.md#managed-raft-placement-operations).
The SDK equivalent is `cellar::sdk::fleet_operations::RaftFleetControl`.

The fleet and all namespaces must already be enrolled, target nodes must already
have eligible capacity, and an explicitly started node-owned topology service
must have trusted deployment resource inputs. The API can accept durable intent
while that service is unavailable; admission does not promise immediate progress.
Node registration/activation uses the separate workflow below; stop/remove and
infrastructure provisioners are not hidden in these placement commands. Keep sets contain 4–64 unique nodes under the current RF3 policy.

Set `CELLAR_ADMIN_URL` to the dashboard origin and provide a bearer token through
`CELLAR_ADMIN_TOKEN` or `--token-file`. Keep credentials out of command history.
Read the current catalog ID and root revision before preparing a request:

```sh
cellar raft fleet status --fleet F --json
```

Retain its 64-digit `catalog_id` as `CATALOG`, the current `observed_revision` as
`REVISION`, and a new nonzero 32-digit lowercase hexadecimal request ID as `OP`.
Use the actual registered node IDs in `KEEP`, comma separated. A policy file has
this shape; choose domain constraints and bounded solver work for the deployment:

```json
{"revision":1,"distinct_zones":false,"distinct_regions":false,"max_work":1000000}
```

```sh
cellar raft fleet rebalance --fleet F --catalog-id "$CATALOG" \
  --keep "$KEEP" --policy policy.json --operation-id "$OP" \
  --expected-revision "$REVISION" --json
cellar raft fleet operation status "$OP" --fleet F --catalog-id "$CATALOG" --json
cellar raft fleet operation progress "$OP" --fleet F --catalog-id "$CATALOG" --limit 64 --json
cellar raft fleet operation wait "$OP" --fleet F --catalog-id "$CATALOG" --timeout-seconds 600 --json
```

Progress lists admitted movements with full history incarnation and the observed
root revision. Pass its `next_cursor` using `--cursor`; a changed movement index
returns a conflict and requires restarting pagination. It is not a transfer-byte
meter. Fleet status distinguishes the active operation from the operation whose
retained placement policy governs future admission.

For cancellation, inspect the current root revision and generate a distinct
request ID, retaining the original catalog and operation IDs:

```sh
cellar raft fleet operation cancel "$OP" --fleet F --catalog-id "$CATALOG" \
  --operation-id "$CANCEL_REQUEST" --expected-revision "$CURRENT_REVISION" --wait --json
```

Cancellation stops new admissions, settles previously admitted work and restores
the earlier placement policy. Completed moves remain visible. `cancel --wait`
succeeds at `cancelled`; ordinary `operation wait` succeeds at `complete` and
reports a blocked result if the operation was cancelled. Wait timeout leaves
durable work unchanged. `converged: true` is historical placement completion at
`applied_revision`, not a fresh health lease, leader-balance result or permission
to delete nodes or retained storage.

All operation commands require the catalog-generation pin. A replacement fleet
must use a new incarnation. Mutations reject a different pin before catalog
dispatch; reads refuse to display a response from a different generation as the
retained operation's success. Canonical retries retain the exact catalog ID,
request ID, expected revision, keep set, policy and authenticated principal,
including after a lost response. Keep-set order is canonicalized. Do not refresh
the expected revision on an ambiguous retry. `not_submitted` describes rejection
of this attempt; it does not certify that an earlier attempt with that identity
was absent. Likewise, operation-not-found is only an observed root lookup.

The API routes use `/api/v1/fleets/{fleet}/raft`:

| Method and suffix | Permission and response |
| --- | --- |
| `GET /status` | `metadata_read`; catalog ID, root revision, active and governing operations |
| `POST /operations` | `fleet_topology`; begin request, HTTP 202 admission/reconciliation |
| `GET /operations/{id}` | `metadata_read`; rooted operation status |
| `GET /operations/{id}/movements?limit=64&cursor=...` | `metadata_read`; bounded page, 1–128 entries |
| `POST /operations/{id}/cancel` | `fleet_cancel`; cancellation request, HTTP 202 admission/reconciliation |

Begin JSON carries `catalog_id`, `operation_id`, `expected_revision`, `keep` and
`policy`; cancel carries `catalog_id`, `request_id` and `expected_revision`.
Mutation bodies are capped at 16 KiB and reject unknown fields. Success carries
the observed phase and terminal/converged flags; errors carry a typed `code`,
`outcome`, and null phase/terminal. CLI exit codes are 0 for the requested result,
2 invalid input, 3 conflict, 4 blocked/cancelled/authentication/authorization,
5 unavailable or unknown result, and 6 wait timeout. Mutations are never retried
automatically; exact caller retries are reconciled against the catalog.

## Delivered add-capacity workflow

The SDK, admin API and CLI expose explicit registration, qualification and
activation. `fleet_register` authorizes a deployment descriptor;
`fleet_capacity` separately trusts the operator to attest actual platform limits.
Neither `operate`, `configure`, `fleet_topology` nor either new grant implies the
other. Use a separately granted `fleet_topology` principal for redistribution.
Human authentication supplies the principal; JSON cannot choose it.

The embedding server configures a running enrolled observer explicitly:

```rust,ignore
let control = control.with_node_observer(running_raft_cells.clone());
let client = fleet_client.with_raft_fleet(control);
```

This starts no node, transport or controller. The standalone dashboard's
existing-root configuration has no observer: registration and status work,
while qualification/activation return `unsupported` without a catalog mutation.
A platform provider remains a separate integration. Capacity evidence is an
explicit trusted deployment attestation, not a measured claim by this adapter.
The catalog owns its independent authenticated live probe and full inventory
checks; caller-supplied observation JSON cannot replace them.

1. Read `fleet status` and retain `CATALOG` and `REVISION`. Write `node.json` with
   nonzero lowercase hexadecimal `node_id`, `disk_id`, `physical_host` (32 digits),
   a `label`, TLS `server_name`, `capability_digest` (64 digits) and
   `storage_instance` (`null` for a new engine, or an explicitly known engine ID).
   Use the original application's declared namespace capabilities. Registration
   reserves identity but grants neither votes nor quantified placement capacity.

   ```sh
   cellar raft fleet nodes add --fleet F --catalog-id "$CATALOG" \
     --descriptor node.json --operation-id "$REGISTER_REQUEST" \
     --expected-revision "$REVISION" --json
   cellar raft fleet nodes status "$NODE" --fleet F --catalog-id "$CATALOG" --json
   ```

2. Start the managed node through the deployment/SDK using this registration.
   Startup pins its physical engine and fresh boot before endpoint publication.
   This explicit infrastructure step is not performed by `nodes add`.

3. The deployment authority serializes `RaftFleetCapacityAttestation` into
   `capacity.json`: exact catalog/node/disk/engine/boot/process binding, profile
   revision/digest, CPU, memory, durable bytes, engine slots, swap-disabled claim,
   conservative used memory/disk and a retained evidence digest/reference.
   `RaftFleetCapacityAttestation` aliases the existing SDK capacity type. Use
   evidence for this boot and running profile; no limits are inferred or silently
   supplied. The operator is responsible for its platform claim.

   Read the current root revision for each **new** request. Qualification is an
   optional explicit Ready phase; activation can directly perform the same checks.

   ```sh
   cellar raft fleet nodes qualify "$NODE" --fleet F --catalog-id "$CATALOG" \
     --attestation capacity.json --operation-id "$QUALIFY_REQUEST" \
     --expected-revision "$CURRENT_REVISION" --json
   cellar raft fleet nodes activate "$NODE" --fleet F --catalog-id "$CATALOG" \
     --attestation capacity.json --operation-id "$ACTIVATE_REQUEST" \
     --expected-revision "$NEXT_REVISION" --json
   ```

4. Inspect status. Ready has qualified capacity but receives no new replica.
   Active requires a valid current `capacity` proof for placement. A later reboot
   can leave an Active descriptor with `capacity: null`; stale evidence cannot
   requalify it. A historical receipt does not reactivate an old boot. Use fresh
   deployment evidence and a distinct request to qualify the new boot.

5. Extend the existing keep set with the new Active node for add-capacity work.
   Dropping other nodes in the same request also performs scale-in and can refuse
   the future-recovery proof even when today's RF3 copies appear to fit. Retained
   old bindings still consume capacity and cannot serve as fresh recovery targets.
   Capacity activation alone moves no cells. The separately started owned controller must
   finish the generated movements and verify RF3; inspect that operation's receipt.
   None of these results authorizes node or storage deletion.

Every mutation retains the exact catalog pin, request ID (`--operation-id`),
expected revision, descriptor/attestation and authenticated principal across an
unknown result. Do not replace the original descriptor with a later status
response: startup may have pinned its previously-null engine. Exact retries
return the original `admission.applied_revision` and `admitted_state`, while
`observed_revision` can advance. These are historical admissions, never a live
health lease or balanced-fleet certificate. Use node status for current metadata
and a new operation's fresh proofs for new placement.

| Method and suffix under `/api/v1/fleets/{fleet}/raft` | Grant and result |
| --- | --- |
| `POST /nodes` | `fleet_register`; registration request, HTTP 202 historical admission |
| `GET /nodes/{node}` | `metadata_read`; current-root descriptor/state and optional exact-boot capacity |
| `POST /nodes/{node}/qualify` | `fleet_capacity`; capacity request, HTTP 202 historical Ready admission |
| `POST /nodes/{node}/activate` | `fleet_capacity`; capacity request, HTTP 202 historical Active admission |

Registration JSON carries `catalog_id`, `request_id`, `expected_revision` and
`descriptor`. Capacity JSON carries the same identity fields and `attestation`.
Both reject unknown fields and have the existing 16 KiB body bound. Success is
`{admission, reconciled}`; the admission includes authenticated principal, node,
action, original/observed revisions and historical admitted state. CLI validates
that the response matches the requested catalog, node, action and exact identity.
Mutations have no implicit wait or retry. Accepted audit entries cannot be
downgraded by late failed attempts.

## Delivered isolated-node repair SDK workflow

The explicit SDK service can now select and execute replacements after trusted
node isolation. It is separate from the keep-set CLI above and the graceful
departure commands below; production isolation remains a deployment integration.

1. Use `FleetCatalog::begin_evacuation` to root exclusion of the affected node.
   Retain the operation identity and exact request for ambiguous retries.
2. A trusted deployment authority must establish that the exact boot is stopped
   and cannot restart, then submit `attest_node_isolation`. Failure to contact a
   process is insufficient. The native fixtures use joined shutdown and a test
   attestation; they do not implement a production infrastructure driver.
3. On a surviving enrolled node, explicitly start
   `start_isolated_repair_service` for that evacuation operation. Supply movement
   evidence and `FleetIsolatedRepairEvidence` implementations with conservative
   resource bounds and complete namespace handles. The controller proves quorum
   and chooses fresh compatible capacity; the callback cannot nominate a plan.
4. Inspect `FleetMovementServiceStatus::isolated_repair` for checked/blocked
   groups and use the ordinary durable movement book/settlement APIs for admitted
   effects. Groups without quorum remain blocked. During an admitted move's
   retry/backoff window, another majority group may use remaining movement
   capacity while all original debt stays charged. A full book still blocks new
   admission. Service shutdown joins its callbacks; admitted physical work
   retains its node owner and durable debt.
5. Perform complete evacuation/removal verification through the existing guarded
   lifecycle APIs before any storage release. A completed scan or three-voter
   replacement alone is insufficient. The service does not remove the node.

The [component contract](../../../crates/cellar/durability/raft/fleet_catalog/README.md#owned-repair-after-trusted-node-isolation)
describes ownership, freshness, capability and resource bounds. Run the nine
delivered native scenarios from the repository root, using the checkout-private
target directory required by `AGENTS.md`:

```sh
cargo test -p cellar --test native_raft_fleet_enrollment --locked \
  fleet_isolated_repair_service_ -- --test-threads=1
```

## Remaining command and API contract

The broader commands below are proposed FO-08 deliverables, except for the
explicit subset above. They must not be presented as usable commands until their
implementation and tests pass. The future management server extends
`/api/v1/fleets/{fleet}/raft/operations` and
read-only `/raft/nodes`, `/raft/groups`, `/raft/policy` and `/raft/operations/{id}`.
Integrate existing admin authentication and auditing, with distinct permissions
for read, node registration, policy change, maintenance, removal and cancellation.
Resource resizing and guarded maintenance stop also require their own explicit
permissions. Detailed phase/rollback rules are in [capacity and scaling](capacity-and-scaling.md)
and [maintenance and rebalancing](maintenance-and-rebalancing.md).
Use the [worked planning examples](planning-details.md) to interpret the copy,
retained-state and failure-headroom numbers in a preview.
The [lifecycle execution contract](lifecycle-execution.md) defines effect
authority, crash recovery, profile sequencing and the evidence behind each
readiness, completion or removal result.

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

| Submission result | Retry rule |
| --- | --- |
| Admitted with handle | Inspect/wait for that operation; repeating its exact request returns the same identity |
| Clean revision conflict with verified non-admission | Refresh the preview; a changed expected revision is a changed canonical request and uses a new operation ID |
| Payload conflict | Inspect the original ID; do not overwrite its stored request |
| Timeout, connection loss or unknown outcome | Retain the exact ID, expected revision and payload; reconcile before creating any replacement request |
| Blocked after partial execution | Inspect committed and unresolved effects; cancel/reconcile the original operation before submitting an incompatible target |

An immutable request can exist without root admission. Its existence alone is
neither success nor proof that execution started. A negative reconciliation must
check the current rooted request history, including later revisions, before
reporting that the original admission did not occur.

## Add capacity

The future management preview must make the operator's requested outcome
reviewable before submission. It includes these fields; omitted or unknown
values are displayed explicitly rather than replaced by zero:

| Preview section | Required content |
| --- | --- |
| Identity and inputs | Fleet incarnation, input root/policy/inventory revisions, node-operation epochs and canonical request digest |
| Desired result | Active keep-set, excluded/departing nodes, convergence mode, placement tolerance and recovery contract |
| Resource fit | Per-node verified profile, physical usage, unmaterialized reservations, candidate peak, policy reserve and limiting dimension |
| History fit | Serving, learner, dormant and retired counts; incompatible/retired target bindings; pending births/retirements |
| Plan | Complete final assignment digest, executable move order, bytes/copies to move and peak temporary/retained occupancy |
| Failure cases | Immediate quorum classification and RF3 restoration fit for every declared node/domain failure scenario |
| Foreground impact | Current workload/p99, measured limiting service stage, movement budgets and uncertainty/unknown ETA |
| Decision | Ready to submit, still planning or a typed blocked result with exact prerequisites |

A preview is not a reservation. If its inputs change before admission, the API
returns a conflict/revised preview instead of executing the old placement list.
An inability to stage the first move is reported even when final copy counts fit.

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

The graceful workflow has separate redistribution, exclusion, finalization,
process stop and terminal-removal requests. Use the catalog-generation pin from
`raft fleet status`. Each mutation has an exact request ID, authenticated
principal and expected revision. Retain those values after an unknown outcome;
changing a field under the same ID conflicts. Never supply serialized proof,
closure or certificate fields to bypass verification.

First run the existing keep-set rebalance excluding the departing node. Wait for
`Complete`; inspect refusals, movement pages and retained debt. New placement is
already fenced by that governing keep set. The controller still needs spare
capacity, failure-restoration headroom and permitted domains. A completed
redistribution is a prerequisite for the graceful `evacuate` command. Topology
and evacuation cannot own the disruption slot concurrently.

Then use the node lifecycle commands, supplying the common `--fleet`,
`--admin-url`, `--catalog-id`, `--token-file` and optionally `--json` options:

```sh
cellar raft fleet nodes evacuate NODE --operation-id OP --request-id EXCLUDE --expected-revision R
cellar raft fleet nodes evacuation-status OP
cellar raft fleet nodes prepare-removal NODE --operation-id OP --request-id PREPARE --expected-revision R2 --wait
cellar raft fleet nodes stop NODE --operation-id OP --request-id STOP --expected-revision R3 --expected-process PROCESS
cellar raft fleet nodes remove NODE --operation-id OP --request-id REMOVE --expected-revision R3 --stop-id STOP --wait
```

The uppercase values above are placeholders. IDs are canonical nonzero
32-character lowercase hex; revisions are numeric. `PROCESS` is the exact process
reported by the finalization receipt. `R2` is the root after exclusion; `R3` is
the root after finalization. Stop does not mutate that root. Reconcile the exact
request after any unknown outcome. A policy, inventory or permit change after
finalization can refuse a fresh stop even at a newer expected revision. A new
stop ID cannot repair stale proof. Use the cancellation/restart workflow below
when the process has not accepted a stop. An already signed exact joined closure
remains a physical fact; terminal removal still needs fresh complete verification
against the current inventory.

`evacuate` uses the independent `fleet_evacuate` grant. It roots `Draining` and
claims the single disruption slot after checking the completed keep set.
`prepare-removal` and `remove` use `fleet_remove`; `stop` independently requires
`fleet_stop`. Broad `operate`, capacity and topology grants do not imply these
permissions. All four mutation routes use bounded requests and canonical audit.
`evacuation-status` requires `metadata_read` and exposes historical admissions,
current node state and every unresolved reservation, including resource-only
debits. Those debits remain charged and can block progress. Its nullable
`verification` field reports the controller's local walk only when the operation,
node and current root revision still match. A controller restart discards that
progress; a later root hides it until a fresh request repeats verification.

Preparation checks one complete-inventory history per successful request. Live
histories require three authenticated survivors, stable RF3 without the departing
binding, and actual replay of the selected immutable archive. Cancelled births
require their checked immutable decision; pending or unknown histories block.
The management server must explicitly configure a surviving live observer and
bounded replay services. Restarting it discards the local walk and repeats
proofs; the CLI never treats its serialized counters as authority. A read-only
quorum attempt that reports `unavailable`, `unreachable`, `overloaded` or
`stale_peer` can return nonterminal progress with a typed `blocker`. The
`history_quorum` stage includes the pending history key and does not consume it.
After signed joined shutdown, `terminal_quorum` describes fresh survivor renewal
over the completed inventory; its history is null and the disruption slot stays
held. Waiting carries no admission, process or release authority. The controller
rechecks the exact root and original deadline before acknowledging it.

`--wait` advances successfully acknowledged verification under its existing
deadline, including these explicit blockers. Refusal, conflict, storage failure
and unknown mutation outcomes remain errors and end waiting immediately. A lost
finalization/removal confirmation cannot become proof progress; reconcile the
exact request. Progress JSON always includes nullable `blocker`, and status JSON
always includes nullable `verification`. Deploy the matching SDK, admin and CLI
together for this internal format cutover.

After the walk, finalization fences new boots of the exact departing process.
It neither stops that process nor releases the disruption slot. Stop is a
separate human instruction. Its immutable consent binds principal, request,
revision, finalization and process; the receiving host rechecks its current
root/boot guard before acting. A `stop_requested` reply is not joined proof.
Response-domain closure may make the reply unknown; query or retry the exact
request. Only the signed post-Node/domain-join closure yields `joined`.

Removal checks the post-fence inventory again and requires that exact signed
closure. Its final CAS independently renews survivor and physical checks,
roots `Removed` with its tombstone, and releases the slot atomically. Exact
committed finalization/removal retries recover historical admissions without
replaying physical work. These receipts do not authorize storage deletion.
Local volumes, retired bindings and archives remain retained. Provisioning or
removing infrastructure remains a separate operator action.

An unreachable node uses the separate trusted-isolation repair primitives;
this graceful CLI cannot turn a timeout into isolation or force a quorumless
group through removal. Restore original durable voters or use a separately
qualified disaster procedure when quorum is lost. Maintenance/rejoin of the
same retained identity and resize are separate unfinished workflows.

## Cancel and restart a departure

Cancellation ends the old operation and returns the node to **Ready**. It grants
no capacity or new membership, revives no retired binding, and retains every
resource debit, data directory and archive. It releases only that operation's
disruption slot. The independent `fleet_cancel` grant authorizes the SDK method
`RaftFleetControl::cancel_node_evacuation`, the bounded authenticated POST route
`/api/v1/fleets/{fleet}/raft/nodes/{node}/cancel-evacuation`, and the CLI command.
The audit action is `raft_node_cancel`.

Before finalization, no scoped stop is admissible for this operation. Cancel
without process/fence fields, including through a metadata-only management server:

```sh
cellar raft fleet nodes cancel-evacuation NODE --operation-id OLD_OP --request-id CANCEL --expected-revision CURRENT
```

After finalization, supply its exact process and a separately retained child
fence ID. The catalog owns an authenticated RPC to the actual process. The
receiver must sync a fence denying every old stop at that epoch before the
catalog may atomically root Ready, the cancellation receipt and slot release:

```sh
cellar raft fleet nodes cancel-evacuation NODE --operation-id OLD_OP --request-id CANCEL --expected-revision CURRENT --expected-process PROCESS --fence-id FENCE
cellar raft fleet nodes evacuation-status OLD_OP
```

A synced fence cannot undo an accepted stop. If the target accepted stop or its
physical outcome is unknown, cancellation refuses and retains the rooted
operation. Reconcile the original stop ID and signed joined closure, then use
fresh complete removal verification. Choosing another fence ID cannot override
an accepted stop. Permanent exact-boot quarantine also refuses cancellation.

Retain **both** cancellation and child fence identities. An unknown root outcome
requires the same CANCEL/revision/payload retry; the permanent journal determines
whether it committed. If a concurrent catalog change definitively prevents that
CAS, a new CANCEL/revision may use the **same FENCE** for the unchanged finalized
process/epoch. A different fence ID at the same already-fenced epoch conflicts.
If a lower-level caller previously fenced that epoch, use its known exact fence
ID. The API never guesses an unknown selector. Process and fence fields must be
supplied together; callers cannot submit a purported fence acknowledgement.

A disconnected waiter or expired deadline cannot turn unconfirmed work into a
successful root transition. Receiver-owned narrowing may still complete; repeat
its exact child fence. Committed cancellation retries require neither observer
nor replay services and do not rewrite current node state or release a later
operation's slot. Historical finalization and cancellation receipts can coexist
in status; read the separate current node state. `cancelled` is terminal for the
old operation, including if that node is later Active or Removed by another one.
There is no `--wait` retry loop for cancellation.

Ready invalidates the prior node-epoch capacity. Obtain a fresh trusted platform
attestation and live capacity qualification/activation through the existing
node-capacity commands. Retained physical high water and reservations still
count. Then start evacuation with a **new operation ID and new request IDs**;
its preparation must verify the entire current inventory again, including births
that appeared during the old operation. The old operation remains cancelled.
This is explicit cancel-and-restart recovery, not an in-place upgrade of an old
verification certificate. The owning application may keep serving ordinary
writes throughout cancellation; a later stop still requires separate consent.

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
