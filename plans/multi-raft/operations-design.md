<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Multi-Raft node operations

MR-13 is under implementation. This contract records implemented local node
operations; it does not establish the complete milestone or a release envelope.
The [implementation plan](implementation-plan.md#mr-13-bound-resources-and-complete-node-lifecycle-operations)
retains the resource, readiness, disaster-restore and qualification gates.

## Admission and ownership

A Node owns one production Raft cell host for its exclusive `data_dir/raft`
directory. Retained SDK handles do not own its lifetime. Starting a second host
in the same Node lifetime is refused; restart the complete Node to reopen that
directory. A normal failed startup joins its acquired infrastructure before
releasing the startup slot. Cancellation poisons the slot until Node restart.
The hidden raw infrastructure interface remains caller managed and supplies no
cell-host semantic stop receipt.

During startup, the host keeps replica routes private until every local group
has attached and the archive owner is ready. Peers receive `UnknownGroup` during
that interval. Early peer traffic must not fill the runtime request budget while
the host is still unable to consume events. Route publication does not replace
the existing per-cell readiness and durability checks.

When requested shutdown begins, the shared Node/CLI driver seals Raft admission before the
ordinary application-work drain. New user commands, queries, administration,
alarms, outbox discovery and controller actions stop. Already admitted work keeps
its producer and physical SQL ownership. Private outbox settlement may finish
after a receiver request has already been sent. A whole outgoing delivery is
tracked as application work from selection through claim and settlement; the
remote transaction may commit even if its source waiter is cancelled.

A short admission barrier covers the final request check, work registration and
nonblocking queue insertion. Sealing first refuses new callers, then waits for
earlier registrations before the Node samples application work. Validation and
user callbacks run outside the barrier; no guard crosses an await or I/O. A
poisoned barrier refuses new work and leaves an error after physical joining.
Selected effect jobs register their whole lifetime through the same barrier;
their private cleanup calls may continue during the drain.

Native effect routing stores weak endpoint hints. Actual delivery upgrades a
live endpoint for the bounded invocation. Self and mutual delivery must not
create a permanent class/outbox/SDK-handle reference cycle after joined shutdown.

Namespace roots bind the selected `Fleet` or `Raft` mode, schema and storage
prefix immutably. A conflicting mode under that same namespace is refused.
Raft classes cannot register as mutable Fleet factories; ordinary Cell/workflow
factories cannot open a Raft registration. Disabling registration supplies no
authority over retained Raft files. Re-enabling a compatible Raft definition
uses the retained engine and a fresh process generation through normal resume;
existing logical-history tokens remain scoped to that same history.

## Local query readiness

`RaftCellHandle.status()` reports the local replica role and query observations
beside its exact published version. A leader is distinguished from other voters
and learners. Outgoing voters remain voters during joint configuration; a
configuration or term mismatch yields `Unknown`. Removed membership supplies no
query readiness. A nonleader voter may be a follower, candidate or pre-candidate.

`queries.coherent_image` requires agreement between the published image, durable
apply marker and sampled runtime position. An empty history without an image is
not query ready. `registered_queries` records local callback capability; a voter
without query code can still replicate and route to a compatible serving node.
`historical` additionally requires an unblocked local execution path, without
drain, failure, retirement, incoming seed or unfinished apply/publication work.
`recent_source` uses the same monotonic source-observation deadline as actual
RecentSource query admission. Missing/future observations, overflow and the exact
expiry boundary refuse freshness. The observation includes its source age and
configured timeout.

These fields are local hints at capture time. They do not reserve capacity,
validate a future minimum-version token or prove a current quorum. Explicit
disconnected historical reads can remain available after source freshness
expires. Every linearizable query, including one served by a learner, still
obtains a fresh ReadIndex barrier. `command_ready` retains the existing authority,
pending-work, persistence and apply checks and additionally requires leader role.
No node-wide healthy boolean hides unavailable cells; nonmember/query-router
occupancy retains its separate typed inspection contract.

## Command admission during background SQL

One pending business command per local group can wait behind an archive,
checkpoint or other existing SQL action. It retains the request's existing
node-wide byte/work reservation and original deadline. Background SQL cannot
consume that command slot; a second producer still receives `Overloaded`.
The host waits for physical SQL completion before starting the authority and
staging checks. A cancelled, unsubmitted waiter is discarded without invoking
its callback. Queue admission is not a commit or durability acknowledgement.

## Physical query-reader admission

`RaftHostOptions.max_query_pins` bounds simultaneous SDK query-reader
reservations across the node's SQL workers (1 through 16, default 16). Each
blocking SQL owner acquires a permit before opening its immutable query database.
Saturation returns typed `Overloaded` without invoking the query callback. The
permit stays on that physical stack until the callback and connection exit,
including error/unwind and cancellation of the async caller. A cancelled waiter
does not free a reader slot. Shutdown still joins the held reader before its
permit is released. Other groups retain their independent command/consensus
path and reserved control admission.

`RaftCells.resource_usage()` reads bounded, actual host business/output,
control and archive-capture budgets, query-reader reservations, SQL work and
transport banks. It performs no I/O, callback or group enumeration, and remains readable during
drain and after closure. Independent atomic loads do not form a transactional
snapshot. SDK futures/results can retain request/output reservations after host
closure; the sampler does not replace those values with zero. Reader permits
reach zero only after the physical readers exit.

Query-reader reservations do not count every `GenerationPin` clone or an image
writer reservation. Archive, seed, checkpoint and effect readers use their own
existing bounds. SQL work reservations use the actual per-worker semaphores,
covering queued and executing requests. At most 16 are sampled. Running-worker
counts are owned by physical blocking stacks until their connections and private
state finish closing; they are not CPU-utilization or connection counts.
Internal queue closure is distinct from application admission closure.

Transport observations distinguish incoming/outgoing control, data and query
banks, plus snapshot source/receive, seed-metadata and inspection-metadata budgets.
Usage/capacity comes from the actual bank, without summing per-peer duplicates.
Registry task, peer and route counts require only bounded scalar reads; no route
enumeration occurs. Admission/failure counters use decimal strings in JSON to
preserve every `u64` value. None of these counters supplies acknowledgement proof.

Connection observations include accepted TCP/TLS handshakes and idle pooled
connections, not just frames with byte reservations. Incoming control, data and
query counts read their actual independent semaphores and expose each lane's
capacity. Control includes its separately reserved inspection slots after those
are enabled. Outgoing counts follow physical TCP/TLS owners through pooling, errors,
cancellation, snapshot/seed downloads and peer inspection. Their limits remain
the existing peer/operation admission bounds; the exporter adds no connection
quota or new admission policy.

The transport observer is weak and cannot keep its transport/engine alive after
Node joining. Missing/poisoned sources are `None` in the SDK and `null` in JSON;
Prometheus emits an availability gauge and omits their unavailable values. A
retained SQL observer owns only bounded semaphore/counter metadata; joined worker
counts and work reservations reach measured zero with its queue closed.

Image accounting reports stores, generations, charged bytes, retained images,
busy image operations and the aggregate capacities of currently open stores,
including the shared archive verification workspace and incoming seed stores.
The store updates scalar totals while releasing its state lock; physical pins
retain the registration with their directory owner. Reclamation refunds bytes
only after durable deletion. Failed/quarantined accounting stays unavailable
for that observer's lifetime, including after the uncertain owner closes.
Observers retain no directory, image or pin. Measured zero open stores after
joined closure does not mean that closed directories contain no durable files.
This is reservation accounting, not a disk inventory or deletion authority.

Every host uses the existing bounded `inspection_scan` policy for aggregate
observations, independently of peer inspection permissions. A batch requests at
most `groups_per_batch` runtime statuses (1 through 16); one complete cycle
retains its original deadline. Only a full, ordered, identity-checked population
publishes role, readiness, activity and apply/archive lag totals. An invalid
contribution or overflow discards the cycle. Partial or failed cycles never
refresh an earlier complete sample. Its age starts at cycle start; population
changes or activation invalidate the cached view. Parked durable histories have
an explicit count and unknown archive coverage. No partial zero hides them.
Size the batch/timeout policy for the configured group count: an unfinished
cycle cannot provide totals even if individual groups are serving.

The group view is a sequence of local observations, not a simultaneous healthy
snapshot. Readiness can change after capture. Apply lag measures committed
minus applied Raft entries; archive lag uses the last locally verified archive
coverage and can overstate lag if another publisher advanced. No scrape performs
runtime requests or group enumeration. The configured controller separately
publishes enabled/recovering/busy state, its actual metadata bank and attempt
counters with a monotonic source age; unavailable/unconfigured sources are null.

`RaftResourceUsage.prometheus()` emits 110 samples when SQL, image, complete group
and transport sources are available, plus 13 with a configured controller,
using fixed budget, lane and direction vocabularies. Cell, group,
member, process, peer and node identities never enter a label. Samples of each
metric family are contiguous, with a single preceding type declaration. Each
budget reports actual bytes/items and capacities; unavailable sources never
become fabricated zero budgets.

The internal HTTP listener exposes the same live source at
`GET /peer/raft/metrics?generation=<NodeInfo.lease_generation>`. Requests require
the Node's existing peer authentication and its exact Node process generation.
The default response is Prometheus text; `&format=json` selects the fixed typed
resource object. Invalid/duplicate options and stale generations are refused.
An unregistered host returns 404. An uninstalled/cancelled host, including a
still-owned opening worker, returns 503 rather than a fabricated zero sample.
This endpoint performs no Raft request, callback, group enumeration or archive
I/O. A sample reports resource reservations, not readiness or graceful closure.

Group detail remains separate through explicitly enabled SDK inspection and
authenticated peer inspection grants. Enabling resource sampling grants no
peer-inspection, membership, seeding or query capability. Production CLI
discovery/management, receipt-maintenance qualification and fenced disaster restore
remain open. The [bounded maintenance contract](receipt-maintenance-design.md)
describes the implemented command-coupled path and its remaining oracles.

## Performance observations

`RaftCells.performance()` returns bounded cumulative observations from one host
startup or standalone engine open. `GET /peer/raft/metrics?generation=<generation>&view=performance`
exports the same observation through existing generation-bound peer authentication;
append `&format=json` for typed JSON. The default resource view remains unchanged.
The performance view has 20 counters and 23 operations with 16 duration buckets,
producing 551 Prometheus samples independent of cell/peer population. Registry
storage and scrape work are fixed; no group enumeration, callback or I/O is used.

Each operation has started, succeeded, failed and abandoned counters, plus a
microsecond duration histogram and unavailable-timing count. Abandonment means a
discarded request, cancellation or unwind; it asserts no rollback. Queue timers
end on physical dequeue/admission or discard. Physical SQL/engine timers stay
with their worker when an async waiter cancels. File-sync timers surround the
actual engine filesystem `sync_all`, excluding lock acquisition; directory sync
is separate. SQL stage, prepare, publish, query, archive capture and other actions
have separate histograms. Outcomes describe the measured operation, not a command
acknowledgement. Successful dequeue is not successful execution. Output-budget
refusal remains a failed SQL action even if its callback produced a value.

Transport timings separate three boundaries for each control/data lane:
`transport_{control,data}_queue` measures from the bounded mailbox submission
attempt to sender-window selection; refused submissions fail, while discarded
queued work is abandoned. Earlier route, validation and budget refusals do not
enter this timer. `transport_{control,data}_exchange` measures one sender window,
including any connection/TLS setup and authenticated replies under the existing
deadline. Control windows contain one to eight frames; data windows contain one.
A partial window fails even if earlier frame admissions succeeded. These
window counts are not frame or command counts. `transport_{control,data}_admit`
measures the receiver's runtime admission after body verification and route
authorization, excluding reply transmission. Admission is not persistence,
application or command acknowledgement. Query, inspection, seed and physical
snapshot streams are outside these six timers and retain their byte accounting.
The queue timer follows physical ownership when a caller drops its Delivery;
timers never authorize or retry work. Quantiles from different boundaries cannot
be subtracted or added to derive a causal per-command breakdown.

| Counter | Meaning |
| --- | --- |
| `engine_file_read_bytes` | Bytes returned by successful engine filesystem reads |
| `engine_file_write_requested_bytes` | Bytes attempted at the engine write-all API |
| `engine_file_write_completed_bytes` | Bytes from successful write-all calls; failed partial prefixes are unknown |
| `engine_persist_groups`, `engine_persist_entries`, `engine_persist_entry_bytes` | Groups, entries and encoded entry bytes in successful shared persistence requests |
| `sql_stage_input_bytes` | Input bytes for executed stage attempts, including duplicate checks |
| `sql_prepared_proposal_bytes`, `sql_prepared_ltx_bytes` | Encoded proposal and contained LTX bytes produced by successful staging, before output-budget admission |
| `sql_query_input_bytes`, `sql_query_output_bytes` | Inputs to executed query attempts and successful query outputs before output-budget admission |
| `object_get_body_bytes` | Bytes delivered to the caller by object GET bodies or range reads |
| `object_put_requested_bytes`, `object_put_confirmed_bytes` | Payload bytes submitted to object PUT calls and payload bytes with successful responses |
| `transport_{control,data,query}_{read,write}_bytes` | Successful socket I/O below TLS, separately for each Raft lane and direction |

Engine filesystem bytes exclude SQLite image/private work, node metadata outside
that adapter and transport. They are API bytes, not physical device bytes; neither
requested nor completed writes prove fsync. Prepared bytes can include proposals
that never commit. Request ledgers separately count acknowledged business work.
Object GET timing includes the response body until EOF, an error or drop; HEAD
and range calls use the same operation. EOF is not an integrity proof. PUT timers
separate create, conditional update and overwrite. A lost PUT reply counts as
failed without proving the write did not commit. GET bytes count actual delivered
chunks; requested PUT bytes include failed attempts, while confirmed bytes require
a successful response. SDK-internal retries, HTTP overhead and billing are outside
this boundary. Backend method overrides and preconditions are preserved. Currently
unused multipart, list, delete, copy, rename and multi-range methods delegate to
the backend and mark observations unavailable until instrumented.

Transport bytes include TLS handshakes, inspections, snapshots, seeds, queries and
refused traffic. Socket writes count kernel-accepted bytes, not proven delivery.
Summing both endpoint directions double-counts transferred data. TCP/IP headers,
retransmissions, S3 HTTP sockets and public application ingress are excluded.
Controller probe and action timers cover asynchronous attempts, including failures
and cancellation. A completed attempt does not establish a completed repair or
physical backend quiescence. Whole-device attribution and instrumentation overhead
remain qualification work.

Each operation's outcomes and histogram share a short lock. Cross-operation and
byte snapshots are not transactional. Poisoned observations set `available=false`;
overflow saturates and sets `saturated=true`. Observation failures never change a
business result. `source` is a random nonce created before startup catalog I/O
(or at standalone engine open) and changes on reopen,
even with a reused configured process identity. JSON u64 values are decimal
strings, not floating-point numbers. Retained handles can sample after closure;
a dead owner clock is null and pending durations become unavailable. These
samples cannot support benchmark deltas. Prometheus uses only fixed names and
bucket labels; the nonce is never a metric label. Compare only source-stable raw
JSON samples and retain both in-flight boundary counts when deriving deltas.

## Drain and leadership handoff

The existing `ShutdownPolicy.total_ms` supplies one original Node deadline. The
application drain retains its existing no-progress and absolute limits. After
that phase, remaining time can support one best-effort handoff turn per attached
local replica. Each turn has at most one second and inspects at most eight
configured voters, serially. A single node-wide turn owns reserved control
memory; it creates no per-cell thread or detached task.

Failed or retired replicas cannot begin handoff. Incoming, pending-producer,
configuration-changing or physically busy replicas wait for their admitted work
within the same one-second turn. Completion events and the host poll resume the
turn without renewing its deadline; waiting consumes the inspection and transfer
budget. A group still busy at expiry is reported unavailable. The source obtains
current runtime status and replication progress. Candidate checks require current matched progress,
voter role and recent activity, then authenticated inspection of the exact
scope, schema, birth, member, node/disk/process, term and membership revision.
The destination must have durably applied and published the source's committed
cut; canonical empty histories retain their explicit no-image representation.
A draining node is ineligible. Existing inspection permissions remain required.

The serialized Raft owner rechecks the term, revision, current voter progress,
readiness and absence of an incompatible membership transition when accepting
the request. An accepted transfer does not prove the target won an election.
Its explicit temporary `NotReady` refusal maps to `Overloaded`, so callers can
retry under their original deadline after taking fresh observations. A lost
response remains `Unavailable` and can follow an accepted transfer.
The drain separately counts observed release when the old leader steps down.
A deadline can leave an accepted request's outcome unknown. Counters record
observed acceptance, never inferred success from cancellation.

Explicit `RaftCells.close()` retains its immediate close semantics. A prior
explicit close cannot acquire a later graceful handoff proof from Node shutdown.

## Final observations and operator action

Physical closure stops admission, cancels owned background work, closes consensus
and transport, drains jobs, joins SQL workers and closes the log engine. Running
blocking work remains owned until it actually returns, even beyond a semantic
deadline. Successful return records the actual node, disk, process and engine
instance, final synced local commit/apply positions, observed archive coverage,
retired/failed flags and submitted commands still ambiguous at close.

The shared stop report keeps its original absolute deadline through Raft closure.
If physical fsync/join work finishes after that deadline, `deadline_elapsed`
remains true and the final stop receipt is incomplete. A timeout never detaches
the storage worker. Cancelled startup likewise retains the opening worker and
reports incomplete startup, rather than interpreting a missing installed host
as `not_registered`.

Group detail is bounded by `RaftCloseReportLimits`; omitted detail has an explicit
count. The total includes every durable history in the engine. Known archive lag
includes omitted attached histories; parked histories without a registration
have an explicit unknown-archive count. Archive
coverage is conservative: another publisher may have advanced beyond the last
verified local observation. CLI output contains aggregate counts and errors.
Detailed scopes are available in the returned typed report, not metric labels.
Parked histories supply no invented per-history archive observation and cannot
become voters or serve queries merely because their durable files exist. Zero
known lag with unknown histories cannot certify full-engine S3 coverage.
The shared driver closes the registered Raft host before publishing its
`joining_domain` phase. The surrounding host publishes the final durable receipt
only after joining the whole Node domain. Authenticated management stops and
caller/signal stops use this same path. Forced cancellation publishes `failed`
without graceful proof, after physical joining.

`StopProof.raft` is required, including for Fleet-only nodes. `not_registered`
means no production cell host was registered; the hidden raw infrastructure API
does not acquire semantic cell-host proof. `failed` records unavailable close
proof and whether startup was incomplete. `closed` binds the actual node, disk,
process and engine identity and complete aggregate counts, including ambiguous
commands, archive lag and unknown parked histories. It contains no per-cell
payload. Handoff counts preserve missing/failed observations and distinguish
accepted requests from old-leader release.

A complete local stop requires successful Raft closure, completed startup, no
submitted unfinished commands and an observed handoff turn without an elapsed
deadline, in addition to the existing Fleet drain phases. Best-effort lack of a
handoff candidate does not invalidate joined local closure. Known archive lag
and unknown parked coverage remain visible obligations; neither a complete stop
nor the receipt grants S3 recovery, deployment fencing or disk deletion.
Malformed engine identities, inconsistent aggregate counts and missing Raft
evidence are refused by the receipt reader. The internal receipt marker stays
1 with a hard cutover; no old receipt reader or migration is provided.

The Node outcome formatter includes Raft results. Production daemon startup now
uses the same catalog, registration, transport, storage and host validation as
`Node.start_raft_cells`. See [production CLI startup](#production-cli-startup).
Remaining management surfaces and complete operating qualification remain MR-13 work.

After a stop:

1. Retain replica disks and identity markers. Even full archive coverage grants
   no permission to delete active voting state or reuse a claimed identity.
2. Reconcile unfinished commands with the same command ID, input and promised
   retry window. A lost response does not establish commit or non-commit.
3. Treat archive lag as retained local durability obligations. Quorum durability
   can survive a local restart without proving archive-only disaster recovery.
4. Reopen joined local state with a fresh process generation and the same durable
   engine instance. Missing/corrupt state refuses old-voter resume.
5. Require normal quorum for strong reads, commands and learner activation.
   Available object storage does not replace that authority.

## Production CLI startup

A compiled application opts in through `Host::raft_cells(script, prepare, bind)`
before `run` or `run_until`. One node owns one registered cell host. The named
script must exist in the active deployment; Raft uses that exact application
instance and its compiled class definitions. It does not invoke the application
factory a second time. The preparation future receives `NativeRaftNode` with
the actual process node name and selected data directory, and returns
`NativeRaftCells`: explicit cell/catalog/host/runtime/log/transport policies,
caller-bound control/data/query listeners, TLS, object storage and version keys.
The binding callback installs the resulting `RaftCells` handles in application
ingress. No public listener accepts requests before both stages succeed.

Run that executable with `--data-dir /absolute/persistent/node-directory`.
The flag rejects relative paths and duplicate values. Raft startup rejects the
`CELLAR_TEST_DATA_DIR` and `CELLAR_WATCH` test/development overrides. A temporary
process directory is never selected for a Raft daemon. Configure stable node and
disk identities, fresh process generations and trusted peer endpoints in the
application's preparation adapter. TLS and signing secrets belong in its
credential provider; command-line node flags do not supply consensus authority.
Remote consensus, query-origin or inspection routes require a configured fleet
bucket (`--bucket` or the corresponding fleet environment setting). The daemon
loads its shared peer-authentication key through that existing node boundary.
The independently supplied Raft catalog store does not configure this key.
Startup refuses remote routes with a process-local random peer key. TLS remains
mandatory, and discovering a peer does not grant membership or query authority.

The preparation adapter must choose `LogOpenMode::Create` or `Resume` explicitly.
For a retained node, use `Resume` with the original node/disk identities, a fresh
process generation and compatible namespace/schema/definitions. Missing or
corrupt claimed state refuses old-member admission. Starting with `Create` on a
retained history does not grant a replacement identity. Keep the original disks
when startup fails. An available catalog/archive is not a substitute for voting
state, and opening S3 objects does not grant permission to restore an old voter.

The current daemon pins its entire initial deployment when Raft is configured.
It does not start the deployment pointer watcher; `/reload` refuses adoption.
Use a supervised process restart to install a compatible executable or deployment
change. Live definition/ingress rebinding requires a separate qualified protocol.
Ordinary Fleet-only daemons retain their existing pointer/reload behavior.

Preparation, registration and binding failure refuse public admission. TERM,
INT or the supplied host-stop future cancels Raft startup. The lifecycle then
closes any installed host and joins the process execution domain, including
physical opening/storage work, before returning failure. Cancellation leaves
incomplete startup rather than a graceful stop proof. After admission, the usual
shared driver seals work, attempts bounded handoff and closes the registered
host before whole-domain joining and the durable stop receipt.

The `crates/cellar/tests/native_raft_daemon.rs` process fixture exercises
this public builder and the actual daemon argument parser. Its temporary local
catalog and development keys are qualification fixtures, not deployment defaults.
It does not qualify physical S3, deployment fencing, scale or live upgrades.

The [new-incarnation disaster protocol](disaster-recovery.md) exposes an explicit
S3 operator API and infrastructure fencing-authority contract. A verified
permanent fence and exact archive reference select a new birth; fresh voters
replay the image and committed LTX tail before attaching. Pending old-origin
outbox rows refuse admission; reconciliation remains a separate unfinished
operation. A caller boolean or object-store flag cannot fence an old live quorum.
Platform-specific production fencing and fleet-wide consistent restore cuts
remain outside this component's qualification.
