<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# EventStream durable work on shared groups

Purpose: Describe the shared-group adapter for publication, subscriptions and transactional outbox delivery.

## Resource ownership and admission

`Registry::raft_event_stream(definition)` compiles a revision-one resource owner.
Validate the application configuration before resolving its reserved
`__EventStream` and `__EventCatalog` Raft classes. Register those classes as managed
namespaces alongside the application's ordinary Raft classes. Each resource
scope must select exactly one owner in the cluster. Missing or ambiguous owners
refuse admission. A definition must grant its owning application administration;
an application's proposed grant cannot create an undeclared resource.

Declare UUID or name-based stream bindings in the ordinary worker configuration.
Bindings, principal identity and compiled definitions participate in admission
and namespace identity. One generation activation owner configures resources and
checks current catalog/stream state before pinning explicit and named bindings.
Named adoption uses its fresh authorized name-resolution response as the catalog
proof: it already validates active lifecycle, complete resource identity and all
captured grant capabilities. Explicit UUIDs still need a separate current catalog
read. Both forms independently check the current broker lifecycle and identity;
no proof is reused across declarations or inferred from a name/configuration hint.
It selects the shared host for Raft resources and never materializes another
LTX-backed catalog or broker for the same declaration. Embedded hosts activate
after provisioning the required groups; the daemon uses its startup hook.

The merged candidate uses one private authenticated event command/query path.
Ordinary RPC bytes and public command discovery cannot supply event provenance.
Pure metadata reads use pinned linearizable images for activation, subscription
status and stateful callers, with separate archive coverage at an S3 minimum. Stateful callers prove
preceding source writes before dispatch; metadata reads do not allocate child
mutation identities or initialize target cells. Typed temporary reader admission
(`NotReady`/`Overloaded`) retries within the same absolute deadline, with fresh
target authorization and coverage checks. Fencing, malformed input, quota and
mutation failures remain refusals. Retained shared-group activation
skips configuration only after a fresh authorized catalog read proves the exact
active definition. Fresh owners with no local history hint use ordinary creation
without an extra strong absence lookup. Resumed owners try the strong read even
while their range cache is empty, avoiding a configuration replay solely because
the locator is cold. Neither startup mode nor the history hint grants serving
or configuration authority. Missing configuration follows the normal creation path;
unavailability, revocation and unknown outcomes refuse activation. Catalog and
partition resolution still check their current resource identities and grants.
Subscription status reports the selected image's identity, lease and lag under
its current consume/admin grant. Observing an expired lease does not expire it,
advance the stored clock or create a child intent/receipt. Clocks, leases and mutations retain their command
receipts. Configuration checks the actual compiled envelope against the host
profile; append batching intersects that profile with policy limits and image
capture still gates source output. No capacity limit is raised automatically.

Shared-group subscription fetches select their initial identity in the same
authorized target transaction as lease acquisition. The committed result carries
that identity even when the batch is empty. The caller validates identity, batch
and receipt before pinning the handle, then supplies the pinned identity on every
later poll; reset or recreation refuses instead of retargeting the wait. Exact
child-command receipts and fetch IDs retain replay authority. First acquisition
needs no separate subscription-status read, while target minimum durability,
source checkpoints and the original request deadline still gate completion.
Ordinary Fleet polling retains its existing status/read path. This protocol
change still requires the complete public lifecycle/recovery qualification.

A verified binding child completes only protected intent/receipt and lane
bookkeeping in its source. If the source was already clean at Quorum, that
successful completion preserves its application-state read watermark. A following
pure query, including a repeated acknowledgment probe, does not need another
checkpoint solely for those rows. Dirty application/schema state, failed
completion, S3 sources, cursor/local-state restrictions and current source/target
fences retain their checks. Later mutations and final output still prove the
complete physical source, including bookkeeping. No mutation authority follows
from the read watermark. The physical version oracle passes native and isolated
RustFS in both images at `df36b88c`; combined S3 output deadlines and the complete
public cohort remain unqualified. See the current integration checkpoint.

Fresh catalog creation can acknowledge its compiled provisioning intent from
the request's owned enrollment response. The exact nonce-independent wire binds
scope, resource, caller, operation and definition. History, creation, schema and
the requested durability minimum must match the retained proof. The result is
creating, with no accepted revision or digest; it cannot establish current
readiness. Activation still reads current catalog status and resolves both
catalog and broker bindings. Existing histories retain the authorized immutable
receipt join, and genuine updates follow normal commands. The adapter releases
enrollment output before admitting any ordinary producer. Shared per-resource
descriptors are charged to the namespace bank and support several resources in
one catalog. At `c5f59695e`, all 16 owning units and eight focused bootstrap
configurations pass, including real RustFS and Quorum/S3 minima. The original
public cohort is still 7/16; cold/retained readiness, Consume and Queue deadlines
remain open. This acceptance of an intent does not qualify the primitive family.

Retained-record reads also use the private immutable query contract. They check
the current read grant and lifecycle, then paginate the selected image's stored
retention frontier without advancing its clock or running maintenance. A read
allocates no per-cell command identity, mutation or receipt. A normal leader
election may still persist its Raft term barrier before the read. Source writes are still
proved before a bound read, and an S3 target still requires independently verified
archive coverage of the selected version. Cursor expiry and byte/count limits
match ordinary Fleet reads; lease acquisition and checkpoint advancement remain commands.

A repeated acknowledgment on the same shared-group subscription handle may
join its completed outcome through a private immutable read. One bounded local
receipt hint selects the probe; only fresh Consume authorization, the current
subscription incarnation, a linearizable selected image and the target's
independent durability proof establish `AlreadyAcknowledged` or `Obsolete`.
Pending outcomes use the ordinary acknowledgment command. Read or proof failures
propagate, and the observation cannot report a new acknowledgment, advance a
clock/checkpoint or synthesize a child command receipt. First acknowledgments and
ordinary Fleet calls retain their command path. Combined-source public deadline
and interruption qualification remains required.

Retained groups do not require an idle election timeout before starting their
first strong read. A current voter whose last durable vote was for itself may
attempt one ordinary startup election when no leader is known. This vote is a
liveness hint, not evidence of prior or current leadership. PreVote, quorum votes,
durable log admission and the new term's applied read barrier remain required.
Observing a leader or starting an election consumes the hint; repeated reads
cannot create a campaign loop. Recovery, membership and park fences still refuse
admission, and ordinary election timers remain the fallback. Stale reads do not
trigger this optimization. Name-discovery session expiry keeps the same typed
deadline failure as its original outer timer, without changing mutation ambiguity.
Automatic name discovery may contact the candidate named by a local durable vote
before an idle follower. This reorders only the existing trusted candidate set,
keeps the hint fixed through the route's attempt bank and preserves local/explicit
routing selectors. It grants no leadership, read, absence or enrollment proof;
each selected destination still performs the ordinary fresh quorum read under
the original deadline. Stale or missing vote hints retain normal fallbacks.

After a proven root, a parked callback reacquires its cell writer before
refreshing its applied cell image. This continuation retains the original ready
barrier and live owner/route/history/leader fence; unrelated committed apply work
does not require another whole-group barrier. Reconciliation alone grants no
execution permission. New application entry keeps full group readiness, and
checkpoint/final settlement keeps its original durability proofs and deadlines.
The continuation regression passes native and real RustFS in both image modes.
At `cd50b272`, a parked RPC waiting for its writer no longer ends the suspended
scan before another ready cell can run. The owner checks the rest of that bounded
pass, then serves queued work or idles when none can run. A physical single-worker
regression queues an immutable observer, wakes the blocked parent and another
cell's dirty callback, and requires the ready callback to run before the observer
while the first writer remains held. It fails before the fix and passes both
image forms on native storage and real RustFS, preserving final serialized
results/receipts. Original public limits, deadlines and all execution proofs
remain. The complete public EventStream/Queue cohort still fails; this is a
shared-worker progress fix, not full primitive acceptance.
At `701b6d6f1`, an invocation that still owns its writer uses the same original
fence to advance through its exact applied checkpoint. A later unrelated
committed entry no longer forces a second whole-group readiness barrier at this
boundary. A physical worker/apply regression fails before the fix and passes
both images on native storage and real RustFS; new-entry readiness, original
deadline and SDK fencing controls pass. S3 archival remains required. Both
selected public named EventStream cases still fail initial activation, so this
checkpoint liveness fix does not qualify cold binding activation or the family.
At runtime `50d9e58fc`, an exact own current-term applied checkpoint reads its
receipt from the frozen private writer that produced the proven image. Current
owner/route/history/term, exact version and RPC-step checks precede settlement;
preview cleanup and log reservation release still join. Other images and terms
retain immutable committed-image reconciliation. This avoids a redundant reader,
and does not change S3 archival requirements. A physical regression rejects
materialized image opens on the original source SQL thread during settlement;
native and real-RustFS cases pass both image forms. The final guard cohort still
fails an S3-source nested SDK deadline, so full public progress remains open.
Earlier named Quorum native/Axum paths pass scoped image/backend checks. The
latest uninstrumented public cohort fails activation; the complete public matrix
remains unqualified. Consult the current integration checkpoint before treating
historical passes as current-source guarantees.

After S3 enrollment settles, the adapter retains its exact creation record and
proof while waiting for full current serving authority. Unrelated committed
apply work does not immediately discard that output with an ambiguous readiness
failure. Progress waits share the original absolute request deadline. Current
route/creation identity, ownership, full group readiness and archive-proof checks
remain required; cancellation or expiry does not rerun the initializer. Native
and real-RustFS physical apply-gate regressions cover progress, original expiry
and exact replay in both image forms. Independent-target-S3 activation and a
later explicit RustFS Consume metadata-read deadline remain open.

Foreground S3 batches own one publication wake across an older job's completion;
publication launch consumes it, and proof-lane requeues cannot renew it. Busy
proof backoff applies to the exact physical group/cut. An uncovered read remembers
its attempted cut, so newer independently verified coverage is eligible without
inheriting that delay. This scheduling state supplies no authority or output proof
and creates no task or timer per cell. Current request-pipeline qualification is
reported in the [integration checkpoint](progress.md#current-integration-checkpoint).

An unchanged Quorum source may reuse its committed entry image for the first
immutable dependent read. The SQL owner records its baseline before maintenance
and constructors; private writes, schema changes, connection-local objects and
pending storage work still require a checkpoint. S3 sources retain their
checkpoint/archive path. Ordinary SDK queries use this same read-only dispatch.
Target read consistency and independent S3 minima, current source/target fences
and final parent receipt settlement are unchanged. Native and real-RustFS
regressions cover clean Quorum, dirty Quorum and clean S3 sources in both images.
This does not qualify the complete multi-step public pipeline.

The archive controller may retain one completed physical verification across
idle gaps and quiet cleanup. It keeps the exact journaled head and original
checkpoint/tail reservations. Admission for new capture or replay evicts that
idle owner first; independently retained active output proofs keep their own
custody and may still refuse admission. Cache reuse still requires current
birth, route, journal, serving authority and exact output checks. An advisory
archive status cannot substitute for this proof. Native and real-RustFS controls
cover lookup reuse without another chunk download and subsequent fresh S3 work.
This bounded optimization does not establish public activation or complete
primitive/recovery qualification.

This reconciliation requires its own combined-source qualification. Earlier
branch results below do not establish the merged candidate's runtime behavior.

The catalog factory records a trusted creation intent and durable reconciliation
deadline. Its active receipt waits for a separate broker configuration command
and that target namespace's required durability proof. Existing configuration,
retirement and receipts survive reactivation; factory construction does not
overwrite them. Authorized callers may observe a pending initial configuration
without acquiring creation authority.

Shared-group catalog ingress and autonomous maintenance share a bounded live
reconciliation reservation for each catalog resource. The reservation covers the
exact logical cell history, including cluster, namespace and incarnation; names
in another history stay independent. An overlapping caller keeps the durable pending operation and re-arms its retry without allocating a
second broker command. Rearming cannot pull a retry earlier, alter a newer
operation or revive a completed one. Activation polls the same pending operation
through current metadata reads under its original deadline; it does not resubmit
configuration or accept a historical receipt as current readiness. The live
reservation is an optimization. SQLite obligations, original operation identities,
current execution fences and independent destination proof remain authoritative
after cancellation, ownership loss or restart.

Catalog enrollment installs its schema, trusted desired definition, operation
receipt and reconciliation work in the same creation transaction. Failed creation
rolls them back together; later factory activation preserves the existing intent
and work deadline. Its initializer digest distinguishes this contract from the
previous empty enrollment; development namespaces must be recreated rather than
migrated.

For an existing catalog history, deployment ingress first joins the exact
configuration operation through a private immutable read. Fresh successful
enrollment instead uses the initializer's empty command-stream cut and takes the
ordinary configuration command path. A concurrent first command still requires
fresh proof of a different fingerprint before conflict recovery can advance its
sequence. Enrollment is never an exclusive sequence reservation.
The existing-history read checks the full canonical definition digest,
operation owner, current administration grant and lifecycle. It retains the
original deadline and the target’s independent minimum durability proof. A
verified `OperationUnknown` or `NotInitialized` response permits the ordinary
configuration command; transport, proof, authorization and malformed-response
failures do not. Retirement/deletion is checked before reporting operation
absence. The original command wire and nonce remain unchanged for that fallback.
The probe copies only framing and payload bytes, keeping two owned request wires
without decoding another full definition body in ingress.

An existing operation allocates no command identity or second reconciler. A
pending result is only an accepted intent: activation still waits for fresh
active catalog and broker proofs. Genuine updates retain their normal command
validation and receipt semantics.

Trusted catalog reconciliation prepares its claim and binding intent before one
shared checkpoint proves both at the source minimum. The binding bridge permits
no child enrollment or dispatch before that proof. This framework path enters no
application code; ordinary application callbacks still prove claims before entry.
Final work completion retains its required proof and original execution deadline.

## Publication and independent proof

Applications use `Context::stream`, `Storage::tx` and `tx.emit` unchanged. Business
updates, a source outbox row and its durable deadline share the private command
transaction. Rollback removes all three. The source command acknowledges only
after its Quorum or S3 minimum is proved. This proves a recoverable obligation;
destination acceptance remains asynchronous.

Source incarnation hashes the logical cluster, namespace, cell and incarnation.
It excludes physical group placement. Two logical sources cannot reuse a
producer lane merely because they share a physical group. Persisted bindings
retain the delivery principal and grant. The receiver checks its current
definition independently on every operation.

The adapter carries framework-minted authority outside application RPC arguments.
Ordinary `Context::rpc_cell` calls cannot supply it. Child calls use the existing
durable source journal: prove the exact source intent before dispatch, prove the
target command using its own namespace minimum, then settle the source. Lost
responses retain ambiguity and stable producer sequence identities. Known
non-admission overload, leadership and readiness refusals wait within the
original deadline, preserving candidate, creation ID, child ID, input and
minimum. Ambiguous attempts never take that retry path. Source
cleanup accepts only the validated contiguous destination watermark.

Retained child retirement can discover the current incarnation when another
creator won the logical name. That joined recovery job uses host name-read
admission after its source claim is proved. It has no live application command's
dispatch permit and never replays the original application frame or target RPC.

Emission checks the actual owned command encoding, including resource authority,
structured-value framing and base64 expansion, against target command and reply
capacity. An oversized event refuses inside the source savepoint. Dispatch
partitions retained rows by that same encoding bound. Read and subscription
budgets reserve response framing capacity.

## Work and subscription lifecycle

The bounded shared scheduler discovers outbox, stream maintenance and catalog
reconciliation through their fixed native work reasons. Claiming a reason commits
a generation-fenced lease before entering the callback. Completion removes only
its claimed generation and lease. Replaced deadlines survive completion and
retained restart. Idle cells do not acquire individual background tasks.
The SQL owner omits retirement from discovery while a same-cell private frame
is still live or parked, because its claim would refuse `Busy`. Runnable native
callbacks remain discoverable. This hint grants no retirement authority; the
claim keeps its mandatory joined-frame check. The
[source journal contract](event-stream-durability.md) describes logical source
ownership and per-lane incarnation handling.

Leased subscription fetch, acknowledgement, retry, absolute extension and
administrative changes execute as independently durable broker commands. A
fetch ID replays its original retained lease. A fetch never advances the
checkpoint; only a validated acknowledgement does. Consumers commit inbox
receipts and local effects together, call `Storage::sync`, then acknowledge.
Retained inbox receipts suppress repeated local effects after redelivery.

For shared groups, empty long polling runs in the consumer adapter. Each broker
fetch has zero wait and releases its logical writer after its required proof.
The consumer waits outside that writer and repeats with the same fetch ID until
its original polling deadline. Native HTTP and Axum ingress use bounded,
independently retained target command identities and the same authority checks.

## Qualification boundary

Qualification keeps the original command deadlines. An ambiguous response may
be resolved only through the original ID, contract, input and durability minimum
using receipt-only lookup. Unfinished publication and administrative mutations
still fail. A pull consumer may replay its fixed fetch ID with the same
transactional inbox; effects must remain unchanged. Expired leases require a
new fetch and receipt before acknowledgement. Observational handlers may issue
a fresh read within their bounded recovery gate. The campaign
logs these recoveries explicitly; passing does not establish a first-response
latency bound.

The executable cases live in
[the shared-group fixture](../../../../crates/cellar/tests/support/cell_group_event_stream.rs).
Run the source-pinned real-object-store campaign with
`tools/multi-raft/cell_groups.py --suite ordinary-event-stream` against an owned
RustFS backend. The campaign records exact native and Axum cases and both image
forms. Its three TLS voters share one host-native process; this does not prove
independent voter cgroup limits or host failure domains.

Name-only selectors, operator catalog inventory/retirement proof routing,
cross-deployment grant changes, movement/wake, sustained capacity and all-voter
disk loss retain separate implementation or qualification gates. The
[progress record](progress.md) owns executed results and source pins. These
component cases do not complete the full CG-17 compatibility or release gates.
