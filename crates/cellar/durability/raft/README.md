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

`Catalog` selects immutable namespace mode/schema/protocol/storage-prefix records
with conditional object creation. Raft data uses a disjoint `raft/<cluster>/<ns>/`
prefix. Per-cell births fix their group/incarnation, initial placement and empty
state; concurrent creators follow the winning record. A permanent group-ID
reservation prevents reusing that identity for another cell/history. Every
create is reconciled by bounded exact-key reads, including a lost response;
there is no overwrite fallback, key listing or lease-based birth replacement.
Records have strict canonical marker-1 framing and a content digest. Root mode,
schema, cluster, minimum durability and voter policy cannot change in place.
Three initial voters are required unless the immutable root explicitly selects
the single-voter development policy.

Initial attachment requires a permanent member claim binding member, disk
lineage and birth digest. The adapter reads the claim first, syncs/verifies local
genesis through the engine, then conditionally creates and reads back the claim.
A claim with absent local stable state refuses blank initialization, including
when a caller supplies the old disk identity in another directory. Competing
disks cannot both acquire the member. The returned `BootstrapPermit` binds the
selected catalog and exact engine owner; an old permit cannot attach a reopened
engine. Committed configuration remains membership authority after bootstrap.

`Node::start_raft_service` starts its runtime parked, including recovered groups.
No RawNode exists or votes until verified attachment. `RaftRuntime::start` retains
its direct trusted-embedding/verification behavior; normal node integration uses
`start_parked` and catalog permits. This adapter currently rereads claims on
restart: an unavailable/unconfirmed catalog leaves attachment closed. Retaining
local confirmed catalog receipts and snapshot/join recovery metadata remains
recovery-controller work. Running groups do not consult the catalog for append,
vote or command authority. SDK registration/dispatch and full actor integration
remain in progress.

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
the archive/recovery controllers. SDK registration and node-owned command
scheduling connect these mechanics to the pure output gate.

The compiled SDK now provides `Registry::raft_class` and restricted `RaftClass`
definitions. Mutable factories and workflows cannot share their class names;
Fleet configuration refuses Raft definitions. Class name, explicit schema
version, schema-definition digest and sorted required command contracts form
the immutable application identity. Missing or changed commands cannot join
under the same root. Query contracts remain independently checked so query-code
availability does not determine voting compatibility.
`stage_registered` checks that identity, operation kind/contract and normalized
input digest before private execution or duplicate receipt lookup. The registered
query bridge opens a verified image through the strict read-only SQLite opener,
checks compatible schema/query code and returns that image's actual version.
It establishes no freshness/linearizability claim by itself.

`cellar_logic::raft_command::CommandState` owns one pending command's admission
and output decisions. Sequenced runtime status cannot resurrect a revoked term;
progress and receipts bind the full replica/history and operation. Fresh commands
need exact ordered entry proofs plus their verified image receipt. Retries can
return a verified stored receipt at its actual covering cut, including a prior
term, without fabricating a new proposal. Namespace/request durability can only
strengthen. Cancellation retains a submitted producer; unsubmitted work stays
pinned until the callback joins and its private mutations are discarded. A new
term and applied startup barrier are required to reconcile an unresolved
submission. Closing records unfinished identity and refuses further output.
`Node::start_raft_cells` installs registered groups in the node's execution
domain. `host.rs` owns serial command producers and effect dispatch;
`host_sql.rs` owns SQLite/image resources in a fixed node-wide blocking pool.
`RaftCells` and `RaftCellHandle` expose bounded public commands, explicit queries,
status and joined local closure. Admission is synchronous, including when a
returned future is not polled. Attempt deadlines are checked by the dispatcher
and SQL owner; cancellation cannot release a slot while its callback still runs.
After submission, the producer remains owned through consensus and publication.
A newer committed/published term barrier can retire old uncertainty even on a
follower, after disposing of its private shadow; that grants no write authority.

MR-08's local read controller holds each request's original deadline and byte/item
permit while waiting for an image. `AtLeast` verifies an opaque signed token
before comparing history positions. `Linearizable` uses fresh safe ReadIndex
contexts, waits for actual image publication, and never treats a runtime status
or cached commit position as a barrier. A checked role/term change can resubmit
the invocation's dropped barrier under the same deadline. Status and submission
generations prevent late replies from regressing an executing query.

The SQL owner captures the durable applied marker with its exact immutable pin.
Protocol-only entries can cover that pin; an unrelated newer image cannot relabel
it. Queries retain the pin through callback completion, and their result metadata
names that captured cut. The default Stale policy requires a recent observed
source; `DisconnectedHistorical` explicitly permits an older verified image
without a time bound. This policy observation is never used for strong reads.
SQL progress hooks observe an atomic cancellation flag raised by the node's
deadline controller, including for an unpolled waiter. Native Rust callbacks
remain trusted/cooperative and are joined before resource release.
Completed responses retain their byte admission until consumed or dropped;
unpolled response channels cannot reuse capacity merely because SQL finished.

`RaftCellsServices::versions` injects managed signing/verification keys. Ed25519
tokens use bounded canonical marker-1 framing and bind full history, schema and
log position. Namespace startup reconciles public-key retention metadata through
conditional updates and exact reads; configuration cannot silently drop a live
verifier on restart. The catalog supplies no new trust roots or consensus
authority. Key rotation and token lifetime are described in the
[application contract](../../../../plans/multi-raft/application-contract.md).
`QueryRouting::Replicas` tries up to eight distinct configured members in priority
order, with one optional untried leader hint. The caller supplies a bounded
per-attempt timeout; every attempt keeps the same contract, captured input,
consistency/token and stale policy under the invocation's original deadline.
Forwarding does not require query code on the origin voter. The destination
still requires a matching query definition before execution. `served_by` names
the actual responding member, independently of the history token.

`NodeRaftListeners::query` and `RaftPeer::query_address` provide the separate
query endpoint. A peer has one bounded query queue/connection alongside its
control and LTX connections. Query MAC domains, exporter label and marker-1
framing are independent of consensus frames. Requests/replies bind full route,
schema, sequence, request tag and payload digest. The receiver forces local
query execution; it cannot recursively route. The sender deducts queued and
handshake time before sending a remaining timeout, while the origin separately
enforces its deadline. Closing the connection cancels the destination waiter.
Request/response representations remain charged through delivery.

This entry point currently routes from a registered local replica. General
directory/ingress routing, counter HTTP options and the remaining capability
audit remain open in the [read audit](../../../../plans/multi-raft/read-audit.md).
These paths do not claim completion of MR-08 or the full delivery scope.

Fresh status checks precede staging, submission and successful output. Transient
protocol Ready work defers preflight or duplicate-receipt settlement under the
original deadline, keeping the frozen private result instead of rerunning the
callback. Exact
applied tickets and immutable receipts settle the core, and delayed proposal
responses cannot regress a completed/retiring producer. Node shutdown joins all
started callbacks before releasing directory ownership. A close report records
unfinished command IDs without proving S3 coverage or authorizing disk deletion.
See the [counter example](../../../../examples/raft-counter/README.md) for native
HTTP and Axum gateways over the same handles. Minimum-version/linearizable reads,
S3 archive gates, snapshot installation and membership controllers remain later
work packages; unsupported capabilities are explicitly refused.

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
