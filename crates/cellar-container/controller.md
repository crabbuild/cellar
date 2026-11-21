<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Durable execution controller

Purpose: Define independent provider ownership, original-operation receipts and recovery fencing.

The host API `cellar::sdk::containers::ExecutionController` implements
`ExecutionFenceAuthority` with an independently owned, durable journal. It is the
controller core for a trusted host integration. An authenticated transport and a
provider implementing `ExecutionControllerBackend` are required; the core does
not install a native Container or sandbox adapter.

## Storage ownership

Create with a new absolute directory and a zero controller ID. Creation generates
a fresh ID and refuses an existing directory. Save the returned `options()` in
independent operator configuration and use `open` with those exact options after
restart. Keep the directory and recovery credential outside voter disks.

The authority's configuration digest covers the generated ID, backend identity
and all admission limits. Declare that digest in the Raft class's provider binding.
A new empty controller gets another identity; it cannot accept the old namespace's
claims. Opening missing, corrupt or incompatible state refuses admission. An
exclusive directory lock prevents overlapping owners. Atomic file publication,
file/directory sync, checksums and a registered cell inventory preserve high-water
marks; a registered missing cell file cannot become a fresh cell.

Limits bound cells, incarnations, receipts, payloads, results and admitted work.
Before provider entry, reserve capacity for every pending operation's largest
result. Capacity refusal never silently prunes receipts or permanently revoked
histories. A separate safe reclamation protocol is required before those records
can be removed.

## Request and response ownership

Obtain a grant through `BindingLifecycleGuard::fence_execution` with the exact
executing Context. Use `grant.operation(context, ordinal, kind, payload)` to bind
the operation to the original Raft command identity and deadline. An ordinal
identifies one operation within that command. Complete `grant.before_effect`
before submitting the claim, operation and exact payload to the controller.
Recheck invocation authority before publishing the result to the application.

Claims have a versioned metadata codec for authenticated host/controller messages.
Deserialization validates logical history, binding, checkpoint and resource key;
it supplies neither a quorum proof nor caller authentication. The embedding
service must authenticate and authorize callers and bound messages before decoding.
Never expose the backend through a second, unfenced endpoint.

For a higher ownership sequence the controller first persists its high-water
mark, then requires backend revocation of old execution, pending submissions and
open handles. Only a durable activation completion admits business operations.
An interrupted transition rejects old and new submission until the same
transition is reconciled. Startup completes pending revocations before admission.

The first operation ID, kind, payload digest and deadline remain immutable.
Submission is recorded before backend entry, and the outcome is durable before
acknowledgement. A lost reply, timeout or cancellation leaves the original
operation unresolved. Reusing its ID never re-executes it; a changed payload or
generation conflicts. A later request cannot extend its original execution
deadline. Successful receipt lookup may return an already retained outcome.
For a live request, that deadline also bounds waiting for admission and durable
file publication. Already started filesystem work retains admission until it
settles, even when the caller times out. An already expired request can only read
a receipt or persist a no-submission tombstone within the configured metadata
budget; it cannot enter provider execution.

`pending` and `reconcile` observe the original operation under the current claim.
A new generation or incarnation can reconcile older work explicitly, while the
result remains associated with its original history and generation. Old pending
mutations and their receipt reservations survive permanent revocation. Reusing
an old operation ID, or another Create ID for its original binding request,
cannot bypass that obligation. Reconciliation never replays a create,
command or write. Unknown mutations block new mutations. An authoritative
`Refused` outcome proves that the original operation cannot execute in the future;
a missing response or missing receipt does not prove refusal. Read/Inspect kinds
must be enforced as observations by the backend.

## Disaster recovery and shutdown

`recover_history` authorizes one exact old-to-new incarnation transition with an
independent recovery credential. It durably revokes the old history before
calling `revoke_history`. That backend method must permanently fence the old
Cellar hosts as well as external provider execution using the infrastructure
fencing authority. The credential authorizes the request; it is not a fencing
proof. There is no permissive default implementation.

Both histories refuse submission while that transition is pending. Restart
repeats the same revocation idempotently. After completion, the old history remains
revoked even if it presents a larger sequence. Only then can the new history
activate restored binding state. Provider volumes and their recovery cuts retain
their separate durability contracts.

Call `shutdown` to stop admission, join admitted calls and already started file
publication, and release the journal lock. Then shut down the controller's
execution domain. Backend fencing must survive its own restart or refuse service
until independently restored.
Opening the controller reasserts every current provider fence before admission,
including activations that were already durable before restart.

The owning integration target is
[`native_execution_controller.rs`](../cellar/tests/native_execution_controller.rs).
It exercises the persistence protocol with trusted metadata and a controlled
provider. Actual Raft admission, authenticated transport, independent controller
and provider process faults, real provider fencing and volume recovery still need
their end-to-end qualification. These tests do not close CG-17.
