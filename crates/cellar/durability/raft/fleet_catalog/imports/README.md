# Namespace import runtime

This component imports one authoritative frozen namespace admission set. Fleet
capacity activation, global namespace coverage, maintenance/removal certificates,
automatic reconciliation and operator surfaces remain separate delivery work.
Keep automatic fleet management default-off until its qualification gates pass.

The namespace's immutable configuration and schema stay fixed. The mutable birth
inventory is claimed by one exact fleet/catalog scope, then adopted only after
every frozen intent is settled and every live group has committed enrollment.
Business writes continue through their existing Raft acknowledgement path.

## Durable steps

| Root phase | Effect and admission boundary |
| --- | --- |
| `Preparing` | `begin_namespace_import` roots the exact cut, operation and catalog owner. The next step conditionally claims that same frozen namespace. A competing catalog can retain a harmless Preparing intent but cannot replace the winner's claim. |
| `Inventory` | Traverse the rooted cursor, one canonical intent at a time. Reconcile the immutable birth decision; conditionally cancel a still-pending decision. Root the history and permanent name/group references with progress in one CAS. Existing exact histories retain their original admission identity. |
| `Coverage` | Traverse the same cut again. Cancelled histories retain their tombstones. Each live history requires its own opaque safe-ReadIndex observation with the exact catalog binding, birth/schema, stable RF3 configuration, committed guard and no active repair. Verify current voter enrollment, engine pins and permanent member/disk claims. |
| `Handoff` | The last coverage CAS admits irreversible handoff. Before changing namespace ownership, read and validate the complete retained witness set against the canonical cut, actual histories and immutable decisions/claims. Unresolved namespace admissions block handoff. |
| `Complete` | After namespace adoption is confirmed, CAS the terminal root state. Only this state plus the exact namespace adoption marker permits new managed birth reservations in the imported namespace. |

Progress comes from the fleet root, never a caller-provided manifest, cursor or
reachable-node scan. Every turn is deadline-bound and owns one bounded catalog
admission. Inventory steps change at most four root projections. Index pages
remain bounded at 64 KiB and records/witnesses at 4 KiB. The 10,000-history fleet
bound also applies to imported histories; it is not a deployed capacity claim.
The handoff verification streams the complete witness set under the same turn
and deadline. Missing/corrupt metadata refuses completion and retains progress.

## Stop, resume and uncertain outcomes

`set_namespace_import_stopped` stops or explicitly resumes the same operation
during Preparing, Inventory or Coverage. Stopping retains its cut, namespace
claim, histories and all physical references. Already-started conditional effects
can still settle; their stale root CAS cannot advance stopped progress. There is
no automatic unfreeze, un-enrollment, capacity release or timeout-based cleanup.
Once Handoff wins, cancellation refuses and that exact handoff must finish.

Persist each `FleetRequestIdentity` before invoking a step. After a lost reply or
adapter restart, `retry_namespace_import_request` reconciles/replays its exact
stored candidate and permanent journal entry. It does not reconstruct or change
the original quorum observation. Request ID, operation, principal and expected
revision remain bound. A missing candidate is ambiguous, never successful
admission. An unadmitted stale candidate cannot overwrite a later root revision.

## Birth and group fencing

Imported original births retain their original unmanaged birth identity. Their
enrollment is a committed Raft configuration entry, which intact original engines
can replay during recovery. New births after adoption carry an explicit managed
owner in their canonical birth record. They require an inventoried reservation
and initialize with their fleet guard in the first engine sync, even though the
namespace's immutable `managed_fleet` configuration remains absent.

Group bindings now include the catalog-scope fingerprint as well as fleet and
cell-history identity. Use `FleetCatalog::group_binding` for that descriptor.
Constructing a descriptor grants no authority. Changing catalog prefixes cannot
reuse a cached authorization or permit to mutate the already-enrolled group.
The experimental protocol hard-cuts to `/fleet-namespaces-1`; earlier namespace,
fleet and group records require fresh experimental prefixes and matching binaries.
Numeric format markers remain 1, with no migration or compatibility reader.

A Complete import is evidence that this frozen namespace entered managed birth
and membership admission. It is not current quorum health, archive coverage,
complete fleet/compiled-namespace inventory, a capacity-ready receipt or proof
that a physical node is empty. Those decisions need their own current rooted
operations, full inventories, quorum observations and physical-work joins.
