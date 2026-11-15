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

After a new catalog enrollment, deployment ingress joins the initializer’s exact
configuration operation through a private immutable read. The read checks the
full canonical definition digest, operation owner, current administration grant
and lifecycle. It retains the original deadline and the target’s independent
minimum durability proof. It allocates no command identity or second reconciler.
A pending result is only an accepted intent: activation still waits for fresh
active catalog and broker proofs. Existing catalogs and configuration changes
keep their ordinary command path.

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
