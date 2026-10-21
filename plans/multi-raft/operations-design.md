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

`RaftCells.resource_usage()` reads fixed-size, actual host business/output,
control and archive-capture budget usage/capacity plus query-reader reservations.
It performs no I/O, callback or group enumeration, and remains readable during
drain and after closure. Independent atomic loads do not form a transactional
snapshot. SDK futures/results can retain request/output reservations after host
closure; the sampler does not replace those values with zero. Reader permits
reach zero only after the physical readers exit.

Query-reader reservations do not count every `GenerationPin` clone or an image
writer reservation. Archive, seed, checkpoint and effect readers use their own
existing bounds. This API does not yet provide complete transport, queue, image
retention, lag or controller metrics. Aggregate export and sampled authenticated
group inspection remain separate operational work.

## Drain and leadership handoff

The existing `ShutdownPolicy.total_ms` supplies one original Node deadline. The
application drain retains its existing no-progress and absolute limits. After
that phase, remaining time can support one best-effort handoff turn per attached
local replica. Each turn has at most one second and inspects at most eight
configured voters, serially. A single node-wide turn owns reserved control
memory; it creates no per-cell thread or detached task.

Failed, retired, incoming, pending-producer, configuration-changing or physically
busy replicas cannot begin handoff. The source obtains current runtime status
and replication progress. Candidate checks require current matched progress,
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

The Node outcome formatter includes Raft results. Production CLI configuration
and startup still require separate end-to-end Raft integration; the shared drain
and formatter do not qualify that missing bootstrap path. Aggregate resource
inspection and other management surfaces remain MR-13 work.

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

New-incarnation disaster restore remains a separate unfinished MR-13 protocol.
No force-new-group command exists here. Restore must bind an exact selected
archive recovery point, verifiable old-deployment fencing and a distinct new
history. Pending old-origin outbox identities require explicit reconciliation;
they cannot be rewritten and redispatched as if they were new effects. A caller
boolean or object-store flag alone cannot fence an old live quorum.
