<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Workflow on shared-group Quorum durability

Purpose: Define ordinary Workflow execution, journal proofs and remaining qualification boundaries.

## Registration and routing

Register the existing handler with `Registry::raft_workflow(class, code_digest,
handler)`, then declare the normal Workflow bindings. The nonzero digest identifies
compiled behavior and must change with code or replay semantics. Each script's
reserved Workflow class hosts its instance cells in shared Raft groups. A Workflow
instance is a logical cell, not a separate physical Raft group.

The namespace identity binds the handler digests, script, binding aliases,
Workflow names, retention and definition settings. Conflicting declarations,
missing handlers, mismatched namespaces and duplicate ownership refuse admission.
Changing a definition requires a fresh compatible namespace until an explicit
deployment transition protocol exists. Format markers stay at 1.

Mutating SDK calls from a stateful cell retain source intents and independent
child receipts. Stateless native/Axum mutations use the same managed enrollment
and nonce-bearing persistent command stream as other named cells. The authenticated
operator entrance retains its private operation contract and checks the exact
instance and expected Workflow generation. Quorum failure never selects Fleet.

## Journal and callback ownership

The shared scanner discovers committed alarm installations. Its exact claim must
satisfy the namespace's required durability before the Workflow handler runs.
Followers apply committed images and do not invoke application callbacks.

The ordinary native replay engine owns creation, retries, named step results,
sleeps, event waits, pause/resume, termination, selected restart, compensation,
retention, history and byte streams. A running step record is checkpointed before
callback entry. Completed results are checkpointed before returning to dependent
steps; compensation follows the same ordering. A storage/proof failure escapes
the replay engine rather than becoming a successful business result.

Sleep, event and retry deadlines become new committed alarm generations. They
release the physical callback instead of retaining a per-instance idle task.
Late completion cannot clear a replacement installation or write another
Workflow generation's journal. Retained startup rediscovers work without client
traffic. Raft alarm visibility hides only the active claimed installation.

A timed-out callback remains physically owned through its retained continuation.
The driver checkpoints the logical outcome while retaining the old callback,
then joins all continuations before releasing its node-wide callback slot.
Queued and running continuations share a 256-item event bound. Work retains the
existing two node-wide callback slots, one retained callback per cell and the
original byte admission. Shutdown or fencing drops the owned turn and its work.

Byte streams keep their manifest and chunk contents in the replicated image.
Raft sizes chunks against the complete encoded RPC response, up to 64 KiB of
bytes, so framing also fits the executing class's result limit. Capture batches
up to 256 KiB per proof, reduced by the admitted envelope
and metadata headroom; every source read is flushed before waiting for more
producer bytes. The producer still enforces the full encoded command limit and
retains the same admission. A committed manifest exposes only completed stream
capture. Readers remain invocation- and Workflow-generation-bound. Competing public commands join a bounded FIFO writer queue before application
entry. A clean, proven callback root yields its writer when that queue is
contended, even if the callback is immediately ready. Cancelled requests leave
the queue, preserve their original deadline and refund only their own admission.
After winning the writer, a request refreshes its selected image under the
original activation fence. Retained callbacks and outputs keep their byte/item
reservations while parked. History
reads return bounded pages against fresh commands instead of waiting on a parked
private root that cannot observe later commits.

Status, history pages, stream opening and chunk reads use private compiled queries over pinned committed
images with explicit linearizable consistency. They do not enroll cells, invoke
constructors or append command receipts. Both stateless and stateful callers
retain the same binding/name/configuration authorization and original deadline.
Public cold discovery uses the ordinary name resolver; a source binding uses
the resolver protected by its source-proof permit. Typed read-only `NotReady`
and `Overloaded` admission failures wait with bounded backoff within the original
deadline. Fencing, terminal and untyped failures are returned.
Automatic routing prefers an active local replica and reserves time for trusted
peers within the same deadline and attempt bank. A voter uses its physical peer
view; ingress-only nodes use their router view. Destinations independently enforce
linearizability, version minima and current membership. Expired physical SQL
retains its admission until joined; explicit local reads remain local. Cached
peers are tried before rooted discovery, so a local stall alone does not require
object-store access. The blocked-local regression and public status-pressure
campaign are recorded in progress.md. Deferred name/membership ReadIndex
requests and forwarded retries enqueue their actual Raft owner immediately,
under the existing read bank; they do not depend on a later heartbeat or
unrelated write to transmit their quorum request.
Stateful reads prove preceding source writes using the existing owned I/O driver.
After the host applies a Quorum checkpoint, pure reads may reuse that proof only
while the same invocation's private image has no subsequent writes, application
temporary objects, unfinished write cursors or open storage transactions. Writer
resume checks the exact unfinished receipt and refreshes this host-only marker;
every I/O poll still checks the original authority and deadline. New or dirty
roots and S3 turns take the ordinary checkpoint path. This avoids one replicated
source mutation per downloaded chunk without bypassing proof of preceding writes.
A read has no destination mutation requiring an intent/receipt. Reader generations
are validated against each selected image. Missing quorum refuses these queries.
While proven pure query I/O waits, an uncontended source may keep its admitted
writer instead of reopening SQLite for every chunk. The SQL owner checks writer
contention even without a query wake and parks only a clean, proven root before
polling the application continuation. Proof-pending or mixed work retains the
ordinary park/resume policy; deadlines and physical admission remain unchanged.
Prepared low-level target command, query, enrollment and receipt calls poll
their owned target I/O while the source is parked. A same-cell child may reach
the writer queue asynchronously; its source resumes only after the response is
ready. These target futures cannot access source SQLite or poll application
continuations. Their initial source proof and ambiguity rules remain required.
Status projects logical expiration; the committed retention alarm owns physical
cleanup. Raft history pages are bounded below the ordinary reply ceiling. The opening
operation consumes a subrequest; chunk continuations retain bounded
outbound/query byte admission without consuming another subrequest per chunk.
Deletion clears only the reserved Workflow key range through the fenced writable
scope and cancels its alarm. It retains framework receipts and sequence floors;
committing deletion retires the old activation before a new instance can run.

Delivery and callback execution are at least once after interruption. Completed
step replay reuses its durable result. External effects still require their own
idempotency or provider fence; a Workflow generation alone cannot stop an old
physical host or make an external effect exactly once.

## Qualification and remaining gates

Implementation and test sources alone establish no passing qualification. The
new fixture reuses the native Workflow behavior/assertions and covers native/Axum
SDK calls, retained RF3 restart, retries, timeout continuations, compensation,
stream reads, controls, archive PUT refusal and quorum refusal in both image
modes. See [progress](progress.md) for source-pinned observed results.

The initial fixture uses one SQL worker per native voter, a 30-second callback
budget and the existing shared admission bounds. RF3 voters share one process;
real RustFS resource limits do not imply independent voter resource limits.
Quorum acknowledgements precede asynchronous archival and therefore have an S3
disaster-recovery RPO. Failed controls and unknown outcomes must remain recorded.

Workflow binding schedules share the reserved cron owner. Its initializer arms
the combined declared expressions even when there is no ordinary cron handler.
The cron occurrence and history are proved before deterministic, idempotent
scheduled instance creation; retained startup resumes the same owed occurrence.
Schedule declarations are part of the immutable admitted namespace contract.
At `88f48cd4`, autonomous scheduled creation and retained-step reuse pass both
native image modes and all four real RustFS native/Axum configurations. The
combined campaign remains failed by an ordinary native status deadline; these
schedule passes do not qualify full Workflow support.

At `54a8379d`, the expanded clean RustFS campaign passes five cases in both
image modes: ordinary native/Axum execution, autonomous native/Axum schedules,
and repeated concurrent status/delete/recreate plus streamed completion. Twelve
selected routing, membership, SDK source-proof, stream-control and Fleet checks
also pass. Voter temp directories use the recorded checkout-owned workspace
volume; the original request/callback budgets and quotas remain. RF3 voters
share one process, with one SQL worker per voter; RustFS retains one CPU/two GiB.

Earlier default-system-temp campaigns still fail creation/enrollment deadlines.
Their source, binaries, logs and disks are retained. The storage-volume comparison
does not prove the cause or qualify a release across arbitrary storage profiles.
See [the read-deadline checkpoint](progress.md#cg-17-workflow-native-read-progress)
for the exact passing profile and failed controls. This closes the scoped MVP
read-liveness qualification, not the complete Workflow family matrix.

At `c6e9f7e95`, a dedicated lifecycle fixture passes all eight native/Axum and
real-RustFS native/Axum image configurations. It preserves completed steps and
exact stream bytes through S3-seeded voter replacement, verifies replacement-local
image coverage and current RF3 membership, parks the whole group, wakes it with
a public read, restarts retained disks and completes the owed event once. The
repository's `workflow-lifecycle` command passes its four RustFS configurations
at the same source and executable hash. Cold creation waits only for the initial
physical group's quorum readiness; it does not pre-enroll or prewarm the cell.

The removed-voter discovery regression selects the actual replaced member after
restart. Its retained log cannot establish serving authority or suppress current
router discovery. The first public name lookup returns the original history;
the subsequent fresh query is served by a current member without local reattach.
Native and real-RustFS selectors pass both images after a causal materialized
baseline failure. Fully joined removed owners also classify late metadata as
retired; the fatal retained-apply guard remains.

The original RustFS/Axum ordinary Workflow case still fails at this source:
materialized `delete/delete-held` refuses ambiguously with
`Execution(Refused(Unavailable))` after 281 ms, before its three-second deadline.
Its logs and voter disks remain. Do not retry this mutation as if it were known
uncommitted or infer complete Workflow acceptance from the lifecycle passes.
Close delete/cancellation and recreation settlement, then repeat the combined
public matrix. See [the current checkpoint](progress.md#current-integration-checkpoint)
for exact failures, red/green pins and resource limits.

Full-size params/results, longer
continuous callbacks, provider I/O, clock faults, deployment changes,
split/merge, placement and fenced disk-loss recovery require their own qualifications.
The common callback budget is bounded to 300 seconds; long sleeps and event
waits release that owner, but continuous callback work must fit its admitted
budget. Full Workflow/CG-17 acceptance remains open until its complete public
contract and independent scenario matrix pass.
