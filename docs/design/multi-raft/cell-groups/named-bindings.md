<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Named-cell and SQL binding delivery

Purpose: Specify the ordinary binding adapter and its remaining qualification boundary.

This CG-17 implementation is in progress. Full public primitive compatibility at
Quorum remains the required product contract. A focused binding checkpoint does
not complete that contract or waive recovery, movement, wake and release gates.

Register source and target classes with `Registry::raft_cell`. Declare the
ordinary exported classes and named cell bindings in the deployment. Application
code obtains `Context::namespace(binding)` or `Context::exported_namespace`, then
uses `Namespace::get_by_name` and `Stub::rpc`, `Stub::rpc_with_signal` or
`Stub::call`. JSON methods registered with `RaftClass::json_cell_rpc` retain their
existing direct SDK contract and gain an internal binding wire contract when
bindings are declared. `RaftClass::cell_rpc` registers owned JSON or structured
values. Cycles and UTF-16 values remain structured values. Leased capabilities
require their session adapter.

Owned RPC replies retain well-formed remote error envelopes as durable results,
including their original graph and causes. Requests still accept clone values
only; malformed or capability-bearing replies refuse admission. The owned RPC
schema digest binds these reply semantics. Use compatible fresh namespaces and
matching voters for this prototype cutover; format markers remain at 1.

The host validates every declared target before ingress. A binding selects a
class namespace in the same cluster and immutable application configuration.
A cross-class target must have exactly one matching namespace; missing and
ambiguous targets refuse startup. The current class uses its source namespace.
Binding dispatch has no Fleet or weaker-durability fallback.

## SQL database bindings

Shared native cells may declare `sql_bindings`, obtain a database with
`Context::sql`, and use the ordinary prepared statement, batch, exec, session and
migration APIs. Provision and load a separate `__SQLDatabase` namespace alongside
the source namespaces, using the existing managed namespace provisioning API or
`--provision-sql-namespace`. Include the source deployment's configuration in the
provisioning plan, for example:

```json
{
  "do_classes": ["Orders", "__SQLDatabase"],
  "sql_bindings": [["DB", "shared/orders"]]
}
```

The runtime supplies the reserved class factory and operation contracts; an
application cannot register its own replacement. The service's canonical schema
is independent of caller script, variables and binding aliases. Database names
retain the existing global SQL identity, so two applications declaring the same
database reach the same cell. A source may call only its declared SQL identities;
exporting the reserved class does not grant access to arbitrary databases. The
host refuses missing or ambiguous SQL namespaces before ingress.

Remote calls also require the existing explicit `NamespaceCommandOrigin` and
`NamespaceQueryOrigin` host grants. Voter membership does not confer write
permission. Trusted startup must install the configured grants after verifying
current peer boots and endpoints, and restore them after restart. The SQL drill
forces different caller and database leaders to cover this requirement.

The ordinary runtime refuses activation of Raft-declared classes. Native callers
also refuse legacy SQL HTTP/RPC egress at the source, so another co-hosted script
cannot supply a weaker SQL service through the global identity. Stateless native
handlers in a Raft application use the node-owned SQL group service directly.
Only declared database identities and compiled SQL methods are admitted. The host
requires exactly one loaded SQL namespace. Direct SQL operator/CLI HTTP routing
still requires its group adapter; ordinary SQL HTTP activation remains refused.

SQL calls from Raft cells use the durable binding protocol below, including the
target namespace's independent minimum durability, original caller deadline,
interrupted-call recovery and private sequence retirement. Queries use durable
commands too; a SQL binding query does not use the stale-query default. Sessions
retain the existing primary bookmark behavior.

The service runs each request in an owned savepoint inside its Raft command.
Batch, exec and migration failures roll back their partial changes before the
SQL error response is encoded. User SQL retains the command authorizer; only
fixed migration bookkeeping receives framework authority. No service request may
commit the framework's outer command transaction. Existing SQL limits and the
unsupported `Database::dump` behavior remain unchanged. KV binding admission
still refuses until its expiry, alarm and payload adapters are delivered.

This implementation checkpoint does not accept the full SQL/KV primitive family.
See [remaining work](remaining-work.md#primitive-delivery-priority) for the public
recovery and release gates, including the independent S3-source deadline blocker.

## Stateless SQL ownership boundary

Stateless native calls have no source cell transaction. Name lookup, one creation
attempt and one independently durable SQL command share the original absolute
node/caller deadline. When another first creator wins, reconciliation waits for
the committed canonical name without repeating enrollment or SQL. After business
submission, the selected history, input and command identity remain fixed.
Cancellation or timeout does not establish non-commit, and a new application
request is a separate mutation rather than an automatic retry.

Stateless SQL uses one reserved client stream per database history. A fresh
128-bit nonce in the owned request envelope distinguishes independent calls even
when their SQL and parameters are identical. A linearizable metadata query reads
the next contiguous sequence from existing receipt floors. A live applied-voter
history can supply the name locator. An acknowledged empty enrollment can start
with sequence 1 as a hint; another caller may still consume it first. The command
always validates current ownership and receipt identity under quorum. Node restart and
movement reuse that replicated state; no node-local client map or new lifetime
client identity is allocated per call.

Concurrent callers can select the same sequence. A conflict error alone does not
authorize another mutation. The adapter may advance only after a fresh quorum
query of the selected history finds a different immutable request fingerprint at
that exact sequence. Missing, pruned and matching receipts provide no such proof;
timeout, cancellation and other failures retain their original ambiguity. Every
attempt keeps the same nonce, input, target history, expiry and absolute deadline.
A proven competing request permits a new sequence, not a retry of an unknown
mutation. Partial committed receipts also fix their request fingerprint.

Only the reserved SQL service's native RPC contracts admit the nonce envelope
with the fixed stream identity. Ordinary clients and classes cannot use it. The
SQL service schema digest changes; use fresh compatible SQL namespaces and
matching voters for this prototype hard cutover. Format markers remain at 1.

The adapter uses normal outbound RPC admission and dense request normalization,
plus the existing Raft control budget. The SDK ticket transfers with the pending
command into the controller-owned invocation, including unresolved producers.
Dropping its response waiter does not release that ticket. Reply limits and the
original cancellation/deadline remain enforced.

At `b233dfbb`, the owning receipt test reuses one client floor across 32 commands
under a one-client ceiling. The public native/RustFS SQL drill passes all four
image/backend configurations with its ceiling lowered to four. Identical racing
inserts remain distinct, and retained restart preserves sequence continuity.
Original deadlines and production quotas remain unchanged. This is a component
checkpoint; SQL-specific recovery, sustained load and full SQL-family acceptance
remain open. See the [qualification record](progress.md#cg-17-bounded-stateless-sql-ownership).

## Source intent and target proof

Each call records its source intent before proving the source transaction. It
fixes the target name, candidate incarnation, enrollment identity, operation,
input, child identity, source/target durability and original absolute expiry.
It also fixes the originating physical `GroupHistory` and leader term. A term
number alone cannot fence recovery after movement to another physical group.
Group-local applied metadata, the existing charged target namespace range cache
and an invocation-local verified history cache may supply a locator. None grants
target execution authority or durability proof.
The invocation cache retains one charged entry and does not survive the invocation.
A cached target range may select a live local voter whose exact active applied
manifest contains the same schema/history. Sealed, closed, reclaimed, non-voting
or changed roots supply no hint. These metadata lookups retain no image pins and
never grant destination execution, reads or durability authority.

An unknown name uses its proven candidate and enrollment identity. A concurrent
creator may win; dispatch to that different history requires a new durable source
selection first. Once business dispatch is possible, the target history, input
and command identity cannot change, including when its outcome becomes unknown.
The target checks its current incarnation and independent minimum durability.
Every live lookup, enrollment, proof, retry and dispatch consumes the original
monotonic request deadline.

A successful call verifies the target receipt's identity, history and durability
before marking its private source intent resolved. The next source effect or final
parent acknowledgement proves that resolution; it needs no separate redundant
checkpoint. Only an explicit overload refusal before target admission retries
automatically, with identical input/history/identity and the original deadline.
Application failures and ambiguous outcomes are not automatically retried.

## Sequences and retained obligations

The source has 64 persistent exclusive outgoing lanes. A global lane serial
allocates enrollment identities and guards lane release. Child command sequences
are contiguous separately for each destination history, so switching cells does
not skip a target's next sequence. Destination counters are indexed, capped at
65,536 entries and subject to the normal SQL image budget. New source incarnations
have a separate lane epoch. Retained restart preserves names and counters.
The binding configuration digest changes with these internal semantics; old
prototype namespaces refuse schema admission rather than interpreting the old
counter layout or the earlier intent body without its physical origin. The
committed effect summary now includes binding due time; all classes use a new
schema digest seed. Use fresh compatible namespaces and matching voter binaries
for this internal hard cutover, including classes without bindings.
Cellar-owned format markers remain at 1.

`cells::CallError` preserves the actual child identity, selected history when
known, original cause and ambiguity. Cancellation after admission may leave owned
work running; cancellation does not prove non-commit. A known target refusal is
reported as known only after the source proves that refusal. Non-admission has
not consumed the target sequence, so its intent and busy lane remain reserved.
Unknown outcomes likewise retain their exact identity and lane. The recovery
owner must reconcile or seal these sequences before releasing them; it must never
replay known-refused business input.

Completed calls release lanes and retain sequence counters; parent completion
removes only resolved intents. Business-state erasure preserves protocol counters,
names and receipts and refuses unresolved calls. Authenticated manifest summaries
count unresolved calls and prevent hibernation while obligations remain. Node/group
SQL admission charges retained call/reply buffers through response handoff.

## Private target sequence retirement

A declared binding class has a private `__sys_raft_binding_seal` contract, absent
from public operation discovery and ordinary command admission. The node's
maintenance client invokes it through the existing command owner. It validates
the exact registered target operation, input digest, child identity, retry horizon
and minimum durability. The current cell incarnation and ordinary Raft admission
still apply. Maintenance may use its own deadline after the original business
expiry; it never dispatches the business input.

The SQL worker refuses sealing while a same-cell RPC continuation remains live
or parked, including a writer-free parked frame. An unresolved producer retains
its existing cell writer admission. An unused contiguous sequence is recorded as
an unfinished receipt with no business result. A later ordinary retry therefore
remains typed unknown and cannot execute the callback; the next sequence can be
admitted. An existing complete or partial receipt is preserved. An expired-through
floor reports consumption without claiming a retained input/result proof.

The seal and maintenance receipt share one transaction and publication. Admission
reserves both receipt rows and both client slots when needed; outer failure rolls
back the seal. An S3 child minimum requires S3 maintenance acknowledgement. Target
retirement does not itself release the source lane or establish a parent result.

## Autonomous claim, retirement and settlement

The shared node effect owner now discovers binding deadlines through the
committed manifest index. Discovery reads bounded identity/digest metadata and
never allocates the stored business arguments. Binding classes require this
owner even when the application has not enabled optional alarm/outbox work;
startup validates its bounds against the host execution limits. There is no
per-cell task, timer or idle SQLite scan.

After the original request deadline, a private source command claims the exact
intent while the SQL worker excludes every same-cell live or parked callback.
The executor supplies the current physical group and leader term. Logical source
incarnation and original source minimum must match; terms are compared only
within the same physical history. The committed claim is proven at the source
minimum before target maintenance begins. A claimed intent rejects late ordinary
selection or completion.

Recovery may finish the exact originally authorized enrollment, with independent
maintenance admission. It never invents a new candidate or dispatches expired
business input. An unselected losing candidate can release only its provisional
sequence after a different creation wins the name. Selected/retired histories
never redirect unknown business work to another incarnation.

Target retirement uses a separate authenticated command purpose on the existing
transport. Namespace/process command grants, revocation, current route, birth,
schema and incarnation still apply. The target allocates its own private system
sequence; its response binds the immutable retirement request and proves the
original target minimum. Source and target response admission are released before
loopback work needs the same SQL/archive slot.

A final source command settles only the claimed metadata digest and exact lane
serial at the original source minimum. It removes the obligation and preserves
the parent outcome as unknown. A self-call rereads the private system allocator
before settlement. The allocator reads only its fixed-size counter through the
existing SQL query owner, under a linearizable barrier and exact route/history.
It takes no command-byte reservation and decodes no pending effect payloads.
Transient read contention retries within the original maintenance deadline;
it does not publish another source claim or retry business input. SQL response
admission releases before the maintenance command. Retirement and allocator
errors retain their typed causes in the reconciliation log.

Orphaned resolved intents are removed without releasing a
lane that a newer call has reused. Every failed turn rediscovers committed state;
it never resumes an arbitrary Rust callback. The connected runtime qualification fails its combined gate; see the
[current checkpoint](progress.md#cg-17-connected-binding-recovery-qualification).
These contracts do not constitute a primitive-family pass.

## Archive publication scheduling

Managed archive registration uses `CellGroupArchivePublisher::GroupLeader`.
New upload allocations prefer the local Raft leader; follower registrations
acquire and independently verify advancing published prefixes. A role sample
schedules work and grants no serving, quorum or archive authority. Journaled
reservations/publications reconcile after role loss. Explicitly selected
single-replica publishers use `LocalReplica`, including the Docker qualification
service. Every leader-eligible managed voter needs an archive configuration.

Managed registrations use a 100 ms minimum archive retry/coalescing interval,
validated within 10–60,000 ms and driven by the existing controller timer. The
first newly waiting S3 proof initiates one immediate attempt; subsequent waiters
share its work and bounded schedule. Requests keep their original deadlines.
This scheduling interval is not a disaster-recovery RPO or response-time promise.
Quorum acknowledgements remain independent of asynchronous archival.

Followers may acquire an older published prefix while later local entries are
unarchived. Its route, complete physical closure and local term/index match must
verify before it can advance the historical reclamation frontier. It cannot
prove a newer command result. S3 output still checks the exact archived receipt
and current birth; local compaction still requires its own admitted checkpoint.

After selecting a verified publication, the owner durably journals its new head
and previous-hold retirement debt before settling output proofs. Remote release
of the previous hold runs as maintenance; it cannot delay an already covered
response. Another allocation still requires that retirement, preserving the
current-plus-retiring bound across cancellation, shutdown and retained restart.
One node-wide proof batch can reuse an actually live admitted verification while
publication waits on remote cleanup. The existing weak verification index retains
no idle image; receipt identity, the journal hold and current birth still verify.
Each batch holds at most 32 requests under their original S3 admission and
deadlines, without allocating another physical verification. An uncovered output
requires a newer cut; transient busy reads use the existing shared retry timer.
Group retirement closes proof admission on the controller before joining async
attempts and escaped physical work.
The one temporary completed verification drops after cleanup before a new cut
needs physical verification, preserving the original pool bounds.
Shutdown also attempts the joined publication's unretained predecessor cleanup
before closing the verifier. A live output keeps its hold journaled; a failed
cleanup remains durable and visible in archive status for retained restart.
At `d1f8535f`, the held-retirement regression passes in both native and real
RustFS image forms. Ordinary bindings and interrupted recovery each pass 8/8
backend/image/durability configurations under the original request budgets.
Full primitive-family and combined lifecycle qualification remain open; see the
[checkpoint](progress.md#cg-17-s3-proof-scheduling-and-joined-retirement).

## Remaining qualification and delivery

The focused public regression exercises ordinary named calls, repeated calls,
multiple destinations, owned structured values, recursive self-calls with live
SQL cursors, transaction refusal, cancellation, exact receipt replay and retained
RF3 restart. It uses both image forms and source Quorum/S3 requests against a target
whose minimum is Quorum. It does not qualify a target namespace with an S3 minimum,
cross-class namespace selection or concurrent creation.

Autonomous reconciliation and sequence sealing now have a connected implementation;
process-loss qualification, deadline/refusal exhaustion, capacity, leader-loss
and movement/wake/disaster qualification remain required. SQL/KV bindings, committed due work and the remaining public/provider
primitive adapters are separate unfinished deliveries. Do not claim the full
HTTP/RPC primitive-family case from a focused binding pass.

The current per-configuration report at `9e02f3f6` passes Quorum source requests
in both image forms on native storage and real RustFS. S3 source requests fail
in both forms under the original five-second deadlines. The target namespace
minimum in these cases is Quorum. The combined binding case remains failed;
these results do not establish complete HTTP/RPC-family support.

The later native checkpoint at `5323c59e` passes the extents Quorum configuration
but fails materialized Quorum and both S3 configurations at the first parent
command deadline. Five private sequence-retirement unit tests pass. Strict
all-target Clippy passes at the subsequent handler-only refactor `2f168bb4`.
No new real RustFS run or complete family case is accepted. See the
[current checkpoint](progress.md#cg-17-private-target-sequence-retirement-checkpoint).


At `bb02bae5`, 34 owning component selections pass. Interrupted restart passes
only native materialized Quorum; every real RustFS restart configuration fails.
The S3 retained image proves claim and target sealing but no source settlement.
Ordinary bindings pass only RustFS extents Quorum in the same cohort. Preserve
all original deadlines and shutdown assertions. A fixed-size system allocator
read and a safe joined shutdown proof are the next recovery priorities, followed
by the ordinary binding deadline gate and SQL/KV delivery. No full family is
accepted by these component results.

At `90e19b34`, fixed allocator reads and joined closure observation pass all
eight native/RustFS interrupted-binding configurations, 36 component tests and
four managed shutdown/restart regressions. The drill preserves unknown parent
outcomes, releases source obligations autonomously, avoids duplicate target
mutation and advances beyond the sealed child sequence. Ordinary bindings pass
6/8 configurations; native extents S3 and RustFS materialized S3 still miss the
first Relay deadline. The five-second budgets, quotas and strict closure
assertions are unchanged. Full family and lifecycle acceptance remains open.
See the [current checkpoint](progress.md#cg-17-binding-liveness-and-joined-closure)
and [next priorities](handoff.md#immediate-feature-priorities).

At `2aca4c6b`, the archive owner hands one live verification to unexamined proof
requests from the preceding background turn. The existing node-wide admission
remains charged; exact birth/journal and receipt checks still run. The handoff
releases before another group, uncovered successor, idle queue, retirement or
shutdown can need that workspace. It introduces no per-group idle cache.
The regression passes in both image forms on native storage and real RustFS.
Interrupted recovery remains 8/8; ordinary bindings pass 5/8, with three S3-source
deadline failures still open. All Quorum-source controls pass. These results do
not accept a full primitive family or establish a workload speedup. See the
[current checkpoint](progress.md#cg-17-live-archive-proof-handoff).


## Grouped SQL qualification checkpoint

At `b82970a5`, the public SQL drill passes in both image forms on native storage
and real RustFS, including different caller/database leaders, explicit process
grants, intended SQL errors, atomic rollback, retained restart, duplicate replay
and quorum refusal. Native conformance and the source/target Fleet refusal test
pass. This does not complete the SQL/KV family: direct stateless ingress and the
combined SQL recovery/scenario matrix remain required. See the
[current results](progress.md#cg-17-grouped-sql-bindings-and-durable-rpc-errors).
