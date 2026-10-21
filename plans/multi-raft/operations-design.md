# Multi-Raft node operations

MR-13 is under implementation. This contract records the Node-owned shutdown
component; it does not establish the complete milestone or a release envelope.
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

When requested shutdown begins, the supervisor seals Raft admission before the
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
Durable management stop receipts still contain Fleet phases. Propagating the
final Raft result into those records remains an MR-13 integration requirement;
a complete Fleet stop receipt supplies no Raft archive or disk-deletion proof.
The Node outcome formatter includes Raft results. Production CLI configuration,
startup and management surfaces still require their separate end-to-end Raft
integration; a formatter unit test is not that operating qualification.

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
