<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Durable execution controller

Purpose: Define independent provider ownership, original-operation receipts and recovery fencing.

The host API `cellar::sdk::containers::ExecutionController` implements
`ExecutionFenceAuthority` with an independently owned, durable journal. It is the
controller core for a trusted host integration. An authenticated transport and a
provider implementing `ExecutionControllerBackend` are required. The
`ControllerProvider` adapter connects ordinary named SDK sessions to this core;
built-in `Context.container` and sandbox activation still need their adapters.

## Named SDK adapter

Construct `ControllerProvider` from the selected controller, declare its exact
identity in `RaftClass::provider_binding`, then register it with `Bindings`.
Use the ordinary `bindings.sessions().get(name)?.execute(...)` entry point.
Every session remains bound to its original Context. The adapter does not retain
or borrow grants from a later invocation, even after controller restart.

The adapter advertises Start, Destroy, Reconcile, Running, Monitor, Signal,
inactivity configuration, buffered ExecOutput and file read/write/stat/list.
It does not advertise interactive exec, ports or sockets. The host backend must
implement the version-1 `controller::Request`/`Reply` protocol, validate requests
before effects, enforce immutable resource IDs and independently fence every
operation. A native execution driver remains required for actual compute.

## Authenticated controller endpoint

`controller::remote::{ControllerServer, RemoteController}` carries the same
ordinary protocol over TLS 1.3 with mandatory client certificates. Use
`ControllerProvider::from_endpoint` to select it. The client verifies the CA,
server name and exact server certificate pin. The server verifies the client CA
and authorizes each exact leaf certificate for a cluster and namespace on its
binding. Configure keys and trust roots explicitly; claims are metadata and do
not authenticate their sender. Only trusted Cellar hosts may hold these keys.

Each request pins the complete independently durable controller identity and
limits before dispatch. One connection carries one request, with no early data,
redirects, connection reuse or automatic retries. Frame sizes, connections and
handshake/request time are bounded. The transport budget never extends the
original effect deadline. Lost replies remain typed uncertainty; receipt lookup
retains the original claim, operation ID, digest and expiry. Ordinary hosts have
no remote recovery-history or shutdown method. Recovery administration and its
infrastructure fencing authority remain separately owned.

Both client and listener reserve a slot from Read/Inspect observations for
lifecycle control. The listener bounds combined observations across all trusted
hosts before entering the controller. Local capacity exhaustion refuses before
network entry; listener observation exhaustion returns typed Busy. Handshakes
and all other requests retain their connection and time bounds. Backend mutation
debt still requires explicit reconciliation; spare transport capacity does not
authorize new execution.

The listener and client run in supplied execution domains. Listener shutdown
stops admission, cancels and joins connections, and leaves the controller journal
owned by its caller. Reopen the exact controller store after listener/process
restart; never create a fresh store to clear uncertain work. TLS authentication
does not supply native provider fencing, volume recovery or quorum proof. The
SDK retains its original source-admission and result-publication checks.

The library supplies authenticated client/server building blocks. A packaged
daemon and native compute backend remain required. Deployment credential rotation
and recovery ownership must preserve the configured identity and trust scope.

## Source admission and recovery

Bounded data/control operations acquire binding admission before the SDK's
ordinary output proof, then pass that same guard into the execution grant.
Busy shared-group maintenance declines before publishing a competing checkpoint.
Running/Monitor observations retain their concurrent lifecycle behavior. The
adapter never reacquires an already held guard or borrows a newer invocation.

Start proves its intent and execution claim together before provider dispatch;
confirmation shares the final output proof. Controller I/O parks the writer and
can reuse only the exact invocation's unchanged unfinished-receipt proof.
Automatic operation ordinals are invocation-wide, so separate sessions cannot
reuse an operation identity accidentally. Original source deadlines remain in
force. Observations release lifecycle admission, and the controller reserves
control capacity so a held observation cannot prevent revocation. A completing
observation reloads current controller state before storing its result.

Lifecycle recovery finds both unresolved and completed original create/delete
receipts across incarnations. If an original create has no controller entry,
recovery atomically seals that original ID before retiring its source intent;
an absence check alone cannot fence a queued create. Reconciliation never
replays an uncertain mutation. The same adapter implements fenced background
maintenance, including cleanup when the business constructor is unavailable.

Live maintenance, Reconcile and Destroy also look up every original pending
mutation, including exec and writes, before further provider mutations. They do
not require a controller restart and never execute the original call again.
Each lookup uses the current invocation's fence and retains the operation's
original history, generation, ID, payload digest and deadline. An unavailable
or unknown receipt still refuses progress; a later maintenance attempt continues
from durable outcomes already found. Read/Inspect observations are excluded so
a held read cannot block lifecycle control. Recovery does not publish the old
call's result as the result of a newer application command.

An inspection batches controller pending-receipt lookup, original reconciliation
and Running observation inside one admitted I/O closure. Each controller call
still checks the same immutable execution claim. The closure parks the writer
and rechecks the original invocation before publication; it cannot use SQLite.
Physical create/delete/data requests retain their separate required proofs.

Contended lifecycle admission can reuse only the exact invocation's unchanged
unfinished-receipt proof. Dirty work still checkpoints before parking, and the
original authority is checked after the wait. Entry/read authority cannot make
a new writer resumable. Shared maintenance proves its task claim with the
execution fence before I/O and task settlement with the final native-work result.
Cancellation before that first proof leaves the previous committed obligation
available; after it, the reserved next attempt remains durable.

The [current qualification](../../docs/design/multi-raft/cell-groups/progress.md#current-integration-checkpoint)
records missing-Delete regressions and original uncertain-operation recovery with
native RF3 and real RustFS. Focused passes do not establish full native
Container/Sandbox, independent backend faults or CG-17 compatibility; complete
release gates remain open.

After proving a delete intent and its claim, one admitted closure reconciles old
mutation debt, looks up the original deletion and submits only when permitted.
A completed deletion is never repeated. An unknown receipt blocks submission;
a new attempt requires authoritative no-effect proof. Lookup absence uses the
original stable deletion ID, so a late copy cannot become a second operation.

A deletion intent can become durable before the controller reserves its first
Delete. Maintenance and public Reconcile drive that intent through the same
fenced deletion protocol as Destroy. `Deleted/Pending` does not prove submission,
and an absent controller receipt does not count as completed maintenance. The
stable ID closes the late-request race; unresolved earlier mutations still block
submission, and a completed Delete is observed without executing it again.

Supervision captures its fencing authority when registered. If an embedding
reopens an in-process controller independently, rebuild that registration or use
a host-owned endpoint forwarder for both sessions and fencing. Replacing only
foreground sessions leaves maintenance using the closed handle. A forwarder
must preserve the pinned controller identity and pass the original claim through;
it must never substitute a newer invocation's grant.

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
