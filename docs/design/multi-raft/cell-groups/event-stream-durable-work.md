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

Declare explicit UUID stream bindings in the ordinary worker configuration.
Bindings, principal identity and compiled definitions participate in admission
and namespace identity. Name-only selectors currently refuse. The legacy
generation resource activator excludes Raft-owned declarations; it must not
materialize a second LTX-backed catalog or broker for the same declaration.

The catalog factory records a trusted creation intent and durable reconciliation
deadline. Its active receipt waits for a separate broker configuration command
and that target namespace's required durability proof. Existing configuration,
retirement and receipts survive reactivation; factory construction does not
overwrite them. Authorized callers may observe a pending initial configuration
without acquiring creation authority.

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
responses retain ambiguity and stable producer sequence identities. Source
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
retained restart. Idle cells do not acquire individual background tasks. The
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
