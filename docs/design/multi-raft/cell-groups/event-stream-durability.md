<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# EventStream on shared groups

Purpose: Define the source journal, callback ownership and remaining public EventStream adapters.

This is part of CG17-04 through CG17-08. Public shared-group EventStream
registration remains refused until authenticated target RPC, service registration
and resource activation are connected. The source implementation alone does not
establish end-to-end publication or subscription.

## Source state and identity

Business changes, event bytes, sequence allocation and delivery discovery share
one SQLite transaction. A source command proves a recoverable obligation at its
Quorum/S3 minimum; destination acceptance and consumer processing have separate
proofs. Savepoints retain the command's SQL authorizer.

The source owner binds cluster, namespace and logical cell, excluding physical
group and incarnation. A separate domain-separated digest binds that owner and
cell incarnation to a 128-bit event producer incarnation. Caller-chosen equal
incarnation bytes in different namespaces cannot borrow an event identity.
History comes from the executing command, not application arguments. The private
SQLite scope alias never enters an event envelope.

Each lane persists its producer incarnation. Fresh-history recovery retains old
event bytes, IDs, sequences and pending seals; new writes use a new lane.
Retained restart and group movement preserve lanes. A foreign owner refuses
before changing source metadata. Capacity/SQL failures roll back identity changes
with the business operation.

Outbox pages bind pagination to the current producer incarnation but retain each
lane's original incarnation in its revision. Retrying an old pending lane uses
that exact revision; seal dispatch also selects the lane's incarnation. An old
cursor cannot cross a producer incarnation. Required source-owner and per-lane
columns retain marker 1 and refuse earlier development layouts before mutation;
there is no legacy reader, identity reset or automatic migration.

## Shared durable work

`__sys_work` retains the ordinary installation generation and deadline. Its
`__sys_raft_native_work` row projects discovery into the manifest and carries the
callback generation, attempt and claimed lease. Both change in one transaction.
An old completion cannot remove replacement work at the same deadline;
generation overflow refuses without partial discovery changes.

Outbox, stream-maintenance and resource-reconciliation use the bounded shared
callback owner. The exact claim commits at the required durability before
callback dispatch. Completion matches that claimed lease, and rescheduled work
survives it. Retained state supplies discovery without client requests or a task
per idle cell.

Publication permissions use a host-installed immutable script/binding/readiness
view, retaining no provider, application or node execution owner. Arguments do
not install bindings. Shared contexts refuse the Fleet event RPC path until the
shared adapter exists; pending source obligations remain recoverable.

## Remaining target and public adapters

Use a private authenticated query-route purpose, following binding retirement.
Its origin, target, purpose and body must be covered by existing transport
authentication and independent namespace write grants. Normal command/RPC input
cannot select it or supply trusted event authority. Preserve boot/membership
fences and the receiver's current stream grants.

Retain the host-minted authority and canonical RPC in the request fingerprint.
Populate `Context.event_authority` only from private admission; validate the
reserved global stream/catalog scope and canonical resource name. Public command
discovery and validation cannot expose private EventStream operations. Keep
source child IDs, original deadlines, intents, uncertainty and independent target
proofs. Existing maintenance retains its derived system-client checks; do not
relax the generic system-command check. Local and remote event requests require
the same private admission and receipt binding.

Normal deployment definitions must register unambiguous resource-owned stream
and catalog namespaces; a producer binding does not own a foreign resource.
Provisioning/reconciliation must reach destination Quorum/S3 proof before catalog
configuration becomes active. Named bindings need actual catalog resolution and
generation-local readiness; handles cannot silently retarget recreated resources.

Reuse existing stream, subscription and catalog state machines. Qualify
publication/read, lease fetch, inbox/sync/ack, retry/extend, configuration,
retirement and operators. Long polling releases the writer between proven roots.
Test interruption, leader replacement, retention, clocks, movement/wake, archive
outage and fenced fresh-incarnation recovery with original event/receipt IDs.
External effects still require receiver idempotency or reconciliation.

The [remaining priorities](remaining-work.md#primitive-delivery-priority) retain
all primitive and CG-13/15/16 gates. This source checkpoint accepts no complete
family or hard cutover.
