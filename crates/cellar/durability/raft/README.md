# Node Raft effects

This component owns the log engine and the raft-rs adapter. Business activation,
command output, query publication and archive proof belong to the actor and
`cellar-logic`; `GroupStatus::protocol_ready` is only a protocol observation.
The application-facing Raft SDK integration is still under implementation.
The [technical design](../../../../plans/multi-raft/technical-design.md) defines
the complete contract.

`ReplicaLog` owns one blocking execution-domain task and one Raft Engine.
Its bounded `GroupLog` caches become visible after sync. Storage lookups on the
consensus worker perform no filesystem I/O. The filesystem is always supplied
by the node; unsupported operations fail explicitly.

The private consensus storage view can finish mechanical lookups against the
last durable cache during engine closure. Public group/proof reads remain
closed. This avoids turning ordinary cancellation or an I/O failure into a
raft-rs storage panic; no successful write or application proof can bypass the
closed engine's persistence boundary.

`RaftRuntime` owns one async execution-domain task for all attached groups.
Each `RawNode` is mutated only on that worker. It collects at most one Ready per
group, batches independent groups' writes, and polls the storage future alongside
peer requests and the owning domain's timer. Deferred requests retain their
byte/item admission permits. Ticks coalesce while a group is paused.

After Ready persistence, the worker transfers its messages into a bounded
outbox before calling `advance_append`. It separately persists a LightReady
commit update before dispatching that update's messages and committed entries.
No-op and configuration entries advance the durable applied marker without
changing the data image; configuration changes also persist ConfState. A new
leader admits proposals only after a committed and applied current-term entry.

An `Apply` event is an immutable committed-entry receipt. Only the ordered apply
head is issued for a group; other groups keep running while its image work waits.
The apply worker must verify, sync and rename an immutable image before returning
`ApplyCompletion`. This adapter validates the marker and persists it, then calls
`advance_apply_to`. Physical SQLite/LTX verification belongs to the image worker;
the runtime tests use an explicitly separate application stub.

`ImageApplier` supplies this physical verifier for committed data entries. It
checks the complete replica/ticket/entry identity against the durable committed
apply head, decodes the scoped application envelope and applies its exact LTX
base/result through `GenerationStore`. Preparation returns a runtime completion,
not a readable image pin. `publish` releases a `PublishedImage` only after the
engine's synced marker names that exact prepared generation. A delayed event
can be reconciled against the durable cache; later protocol-only entries may
cover the same image. Recovery adopts only the image selected by that engine
marker, with mandatory digest, length, LTX position and page geometry validation.

The internal marker-1 image record now includes `page_size`. Recreate older
experimental Raft storage for this hard cutover; no compatibility reader is
provided. Orphan images remain retained pending explicit recovery/retention
reconciliation. The image owner alone does not authorize leader execution,
query consistency or a successful SDK command.

`PrivateExecutor` owns an exclusively locked writable directory. Opening or
rebuilding it copies the marker-selected immutable base, verifies that copy,
opens the ordinary WAL capture engine and seeds its continuation at the exact
committed LTX position. Opening/control writes join the next proposed cut; a
zero-page continuation file is never replicated as data. No Fleet uploader,
maintenance timer or output ticket can operate on this private image.

A checked `CommandAuthority` observes a current-term, caught-up leader and binds
the replica, term, applied marker and command base. One synchronous callback
receives `CommandTransaction`, SQL and explicit host-captured time/random inputs.
It cannot access the connection, commit early, start a nested transaction, use
reserved tables, attach another database, write temporary state or invoke SQL's
ambient clock/random functions. Native application code remains trusted; this
facade is not an OS sandbox. Callback errors, panics and oversized results roll
back. Any uncertain private state requires rebuilding before another callback.

The transaction stores the command ID, operation/contract/request digests,
bounded encoded result, minimum requested durability and retry deadline beside
the business mutation. Per-client sequence and expiry floors use canonical
big-endian u64 blobs. New sequences must be contiguous; conflicting retries and
expired sequences are refused. Client and receipt budgets refuse admission
instead of deleting promised retry state. A replicated pruning update can remove
only an expired contiguous prefix; its client floor remains after receipt removal.

After local commit, capture includes every intervening complete LTX file. Bounds
are checked before reading/decompressing; SQLite's frozen logical image supplies
the mandatory result digest. A separate retained preview generation verifies the
entire physical apply before proposal submission. This correctness-first path
materializes both a preview and the eventual committed generation; its cost is
part of release qualification, not a performance claim.

`mark_submitted` runs before awaiting the runtime. A lost response or cancelled
waiter keeps the single command slot occupied. Abandoning that submission needs
a new term/process with its applied barrier; an unsubmitted attempt can rebuild
under the same authority. Neither path continues from the surviving writable
file. The host must revalidate live runtime authority before submitting bytes.

`finish` accepts only the exact published group/proposal/image, then reads the
receipt from that immutable generation. It returns a covering position and the
receipt's minimum durability, not S3 archive proof or an SDK success gate. A fresh
caught-up authority observation permits reuse only when the frozen private state
still exactly matches that head. Otherwise rebuild. Preview cleanup and capture
history trimming stay bounded; orphan/public-image retention is reconciled by
the archive/recovery controllers. SDK registration, ingress, output gating and
node-owned command scheduling remain MR-07 work.

Incoming snapshots similarly require a `Snapshot` effect completion before the
Ready's snapshot metadata or dependent messages can advance. The host verifier
must bind the image and metadata to the exact snapshot. `SnapshotInstalled` is
emitted only after the engine has persisted that installation.

Tickets bind group/incarnation, node/member, disk/process, operation, position
and digest. An opaque process-local worker identity also prevents a callback
from a retired worker completing a reissued job in the same process. A committed
entry from an earlier leader term remains applicable through its issued ticket.
Proposal admission receipts do not establish commit or command success.

Normal close stops admission, awaits submitted log work and writes a final
nonempty storage fence before releasing consensus ownership. It is not a fleet
drain or permission to discard disks. Cancellation or panic leaves ownership
poisoned for that log handle; close/reopen its storage before rebuilding the
worker. Execution-domain shutdown cancels the consensus task and joins the
log worker and its owned engine threads.

The elected timeout distribution is unchanged. An attributed constructor accepts
a seed drawn from the owning domain's labeled RNG; qualification fixtures can
provide a seed and explicit ticks. No consensus adapter uses a process-global
random source. Limits bound queued work, outbox events and retained apply work;
complete resident-memory and scale qualification remains in the resource and
qualification packages of the delivery plan.

`RaftTransport` runs on that runtime's execution domain. One persistent TLS
connection per peer and traffic class multiplexes every registered group;
separate control/data listeners, workers and byte/item banks reserve control
capacity. Connections, groups, physical peers, frame/header lengths and decoded
entry counts have finite limits. Incoming bodies are allocated only after a
bounded header has authenticated and matched an explicitly installed route,
local member identity and durable configuration. Unknown groups never create
storage or fetch a bucket object. Each physical peer has separate incoming and
outgoing banks, so full outbound queues cannot consume inbound response capacity.

The marker-1 wire uses TLS 1.3 with verified server certificates and no early
data. A distinct HMAC domain authenticates each request and response with the
existing fleet secret, a normal TLS exporter and length-framed inputs. Requests
bind the complete group/history, schema/protocol digest, both node/member/disk/
process identities, a per-connection increasing sequence and body SHA-256.
Responses additionally bind the original request tag and reverse its exact
source/target route. Reusing a response, changing the target or moving a captured
frame to another TLS connection fails authentication. Fleet's existing HTTP
request protocol is unchanged.

`submit` queues without awaiting a slow socket. Its returned `Delivery` proves
only authenticated message admission. The real Raft append response remains a
separate message emitted after the receiver's required persistence; a timeout
cannot manufacture it. Queued send failures report unreachable and snapshot
failure hints while their routing view is still current. The host dispatcher
must retain or report immediate queue refusals. Dropping a delivery does not
refund queued/in-flight work. Peer and node usage counters expose held bytes and
items; permits release on completion, refusal, close and domain cancellation.

Group route installation is a trusted membership-controller operation. It must
refer to existing local storage and current ConfState, never initializes a group
and cannot change the persisted membership. Views have monotonic revisions and
immutable schema digests. A process/address replacement shared by several groups
requires retiring their conflicting routing views before installing the new
physical-peer view. Pending callbacks from retired views cannot credit the new
peer or report failures against it. The membership controller and durable remote
identity registry are delivered separately in MR-07/MR-11.

Transport close stops admission and joins its counted resource owners. Execution
domain shutdown also joins their task registry. A retained closed transport
handle does not retain its exclusive runtime claim. Registry poison closes
admission while cleanup still waits for the actual resource owners.
`Node::start_raft_service`
uses the embedded node's existing execution domain, filesystem and peer secret,
and the node's `raft` storage directory. It supplies infrastructure handles;
namespace/SDK command and query registration remain separate work. An embedded
node shutdown joins these resources even while application handles survive.

Transport qualification uses real TLS sockets and three OS child processes, each
with the production engine/runtime/transport. Their dispatcher supplies opaque
fixture images and controlled peer loss, duplication, reordering and delay. This
does not qualify SQLite, LTX, S3, the complete SDK, workload capacity or production
failure domains.
